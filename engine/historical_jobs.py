from __future__ import annotations

import base64
import io
import logging
import mimetypes
import threading
from time import perf_counter
from typing import Any, Dict, List

import pandas as pd

from engine.blob_storage import BlobStorage, INPUTS_CONTAINER
from engine.database import (
    append_events,
    remove_excluded_historical_keys,
    get_batch_master_df,
    get_customer_history_df,
    get_event_summaries,
    get_history_summaries,
    heartbeat_historical_build_job,
    historical_build_job_is_active,
    get_supplier_history_df,
    get_sto_incoming_history_df,
    get_sto_return_history_df,
    replace_batch_master,
    replace_customer_history,
    replace_supplier_history,
    replace_latest_sfda_snapshot,
    refresh_accept_history_incremental,
    refresh_dispatch_history_incremental,
    reconcile_affected_batch_master_event_totals,
    reset_history,
    update_historical_build_job,
)
from engine.exporter import Exporter
from engine.full_reconciliation import FullReconciliationEngine
from engine.warehouse_context import warehouse_scope


logger = logging.getLogger("SFDA-Reconciliation.HistoricalJobs")


class HistoricalBuildCancelled(RuntimeError):
    """Raised when a job is cancelled/failed while its worker is still alive."""


def _start_job_heartbeat(
    job_id: str,
    stop_event: threading.Event,
    cancel_event: threading.Event,
    warehouse_id: int,
    warehouse_name: str,
) -> threading.Thread:
    """Keep UpdatedAt fresh while the worker is alive and detect cancellation.

    ``contextvars`` do not automatically propagate to a new ``threading.Thread``.
    The heartbeat therefore establishes the same warehouse scope explicitly so
    SQL SESSION_CONTEXT/RLS never falls back to Warehouse 1 while processing a
    different warehouse.
    """

    def _run() -> None:
        with warehouse_scope(warehouse_id, warehouse_name):
            while not stop_event.wait(30):
                try:
                    if not heartbeat_historical_build_job(job_id, warehouse_id=warehouse_id):
                        logger.warning(
                            "Historical heartbeat no longer sees an active job. job_id=%s warehouse_id=%s",
                            job_id,
                            warehouse_id,
                        )
                        cancel_event.set()
                        return
                except Exception:
                    # A transient heartbeat failure must not kill a healthy worker;
                    # the normal stage updates still provide recovery evidence.
                    logger.exception(
                        "Historical heartbeat failed. job_id=%s warehouse_id=%s",
                        job_id,
                        warehouse_id,
                    )

    thread = threading.Thread(
        target=_run,
        name=f"historical-heartbeat-{job_id[-8:]}",
        daemon=True,
    )
    thread.start()
    return thread


def _read_excel_bytes(file_name: str, file_bytes: bytes) -> pd.DataFrame:
    if not file_bytes:
        raise ValueError(f"Historical input is empty: {file_name}")

    engine = "xlrd" if str(file_name).lower().endswith(".xls") else "openpyxl"
    return pd.read_excel(
        io.BytesIO(file_bytes),
        engine=engine,
        dtype=object,
    )


def _read_input_group(
    storage: BlobStorage,
    items: List[Dict[str, Any]],
) -> pd.DataFrame:
    frames: List[pd.DataFrame] = []

    for item in items or []:
        blob_name = str(item.get("blob_name", "")).strip()
        file_name = str(item.get("file_name", "")).strip() or "uploaded.xlsx"

        if not blob_name:
            continue

        downloaded = storage.download_blob(
            INPUTS_CONTAINER,
            blob_name,
        )
        frame = _read_excel_bytes(
            file_name,
            downloaded["data"],
        )
        frame["_Source File"] = file_name
        frames.append(frame)

    return (
        pd.concat(frames, ignore_index=True)
        if frames
        else pd.DataFrame()
    )


def _decode_exported_file(
    exported: Dict[str, Dict[str, Any]],
) -> tuple[str, bytes, str]:
    if not exported:
        raise ValueError("Exporter returned no file.")

    file_name, payload = next(iter(exported.items()))
    content = payload.get("content", "")
    encoding = str(payload.get("encoding", "base64")).lower()
    mime_type = (
        payload.get("mime_type")
        or mimetypes.guess_type(file_name)[0]
        or "application/octet-stream"
    )

    if encoding != "base64":
        raise ValueError(
            f"Unsupported exported-file encoding for {file_name}: {encoding}"
        )

    return file_name, base64.b64decode(content), mime_type


def process_historical_build_job(
    job_id: str,
    input_manifest: Dict[str, Any],
    operation: str,
    *,
    warehouse_id: int,
    warehouse_name: str = "",
) -> None:
    """Execute a historical build outside the initiating HTTP request."""

    storage = BlobStorage()
    storage.initialize_containers()

    heartbeat_stop = threading.Event()
    cancel_event = threading.Event()
    heartbeat_thread: threading.Thread | None = None

    job_started_at = perf_counter()
    stage_started_at = job_started_at
    timings: Dict[str, float] = {}

    def mark_stage(name: str) -> None:
        nonlocal stage_started_at
        now = perf_counter()
        elapsed = now - stage_started_at
        timings[name] = round(elapsed, 3)
        stage_started_at = now
        logger.info(
            "HISTORICAL_PERF job_id=%s stage=%s seconds=%.3f total_seconds=%.3f",
            job_id,
            name,
            elapsed,
            now - job_started_at,
        )

    def ensure_active() -> None:
        if cancel_event.is_set() or not historical_build_job_is_active(
            job_id, warehouse_id=warehouse_id
        ):
            raise HistoricalBuildCancelled(
                f"Historical Build is no longer active: {job_id}"
            )

    try:
        update_historical_build_job(
            job_id,
            status="Running",
            progress=5,
            current_stage="Reading uploaded historical files",
            mark_started=True,
            error_message="",
        )
        heartbeat_thread = _start_job_heartbeat(
            job_id,
            heartbeat_stop,
            cancel_event,
            warehouse_id,
            warehouse_name or f"Warehouse {warehouse_id}",
        )
        ensure_active()

        asn_df = _read_input_group(
            storage,
            list(input_manifest.get("asn_files", [])),
        )
        dispatch_df = _read_input_group(
            storage,
            list(input_manifest.get("dispatch_files", [])),
        )

        sfda_items = list(input_manifest.get("sfda_files", []))
        if not sfda_items:
            raise ValueError("SFDA file is missing from the historical job.")

        sfda_df = _read_input_group(storage, sfda_items)
        sfda_source_name = str(sfda_items[0].get("file_name", "") or "")
        replace_latest_sfda_snapshot(sfda_df, sfda_source_name)
        mark_stage("read_inputs_and_sfda_snapshot")
        ensure_active()

        update_historical_build_job(
            job_id,
            progress=20,
            current_stage="Validating and normalizing historical data",
        )

        engine = FullReconciliationEngine(
            asn_df,
            dispatch_df,
            sfda_df,
        )
        prepared = engine.prepare_incremental()
        mark_stage("validate_normalize_prepare_events")
        ensure_active()

        # Self-clean legacy out-of-scope data when an Append re-encounters it.
        # This prevents old LAB/Biochemicals rows from returning in Supplier/
        # Customer History after they were introduced by earlier versions.
        if operation == "append":
            scope_cleanup = remove_excluded_historical_keys(
                prepared.get("excluded_receipt_keys") or [],
                prepared.get("excluded_dispatch_keys") or [],
            )
            logger.info(
                "Historical append scope cleanup: %s",
                scope_cleanup,
            )
            mark_stage("remove_excluded_historical_scope")

        if operation == "rebuild":
            update_historical_build_job(
                job_id,
                progress=32,
                current_stage="Resetting previous historical data",
            )
            reset_history()
            mark_stage("reset_previous_history")
            ensure_active()

        update_historical_build_job(
            job_id,
            progress=40,
            current_stage="Saving receipt and dispatch events",
        )
        inserted = append_events(
            prepared["receipt_records"],
            prepared["dispatch_records"],
            assume_empty=(operation == "rebuild"),
        )

        mark_stage("save_receipt_dispatch_events")
        ensure_active()

        has_new_events = (
            inserted.get("receipt_events", 0) > 0
            or inserted.get("dispatch_events", 0) > 0
        )

        if operation == "append":
            # Append remains deduplicated at the event level, but Batch Master
            # refresh must be self-healing. A previous job may have successfully
            # saved ReceiptEvents / DispatchEvents and then failed before the
            # derived Batch Master was refreshed. In that case, re-uploading the
            # same historical file produces duplicate events (correctly skipped)
            # but MUST still recalculate every BN + Expiry Month + Generic key
            # represented by the uploaded file from the durable SQL event tables.
            #
            # Therefore refresh is driven by PREPARED INPUT KEYS, not only by
            # newly inserted-event counts.
            update_historical_build_job(
                job_id,
                progress=68,
                current_stage=(
                    "Reconciling affected historical batches"
                    if has_new_events
                    else "Rechecking existing historical batches"
                ),
            )

            receipt_records = prepared.get("receipt_records") or []
            if hasattr(receipt_records, "to_dict"):
                receipt_records = receipt_records.to_dict(orient="records")
            if receipt_records:
                refresh_accept_history_incremental(receipt_records, sfda_df)

            dispatch_records = prepared.get("dispatch_records") or []
            if hasattr(dispatch_records, "to_dict"):
                dispatch_records = dispatch_records.to_dict(orient="records")
            if dispatch_records:
                refresh_dispatch_history_incremental(dispatch_records)

            # Final production self-healing guard:
            # reconcile ONLY keys represented by this upload directly from the
            # durable SQL event tables. Duplicate events remain deduplicated, but
            # their Batch Master rows are still recalculated.
            reconcile_result = reconcile_affected_batch_master_event_totals(
                receipt_records,
                dispatch_records,
            )
            logger.info(
                "Historical append affected-key reconciliation completed. affected_keys=%s reconciled_rows=%s",
                int(reconcile_result.get("affected_batch_keys", 0)),
                int(reconcile_result.get("batch_master_rows_reconciled", 0)),
            )

            mark_stage("incremental_historical_refresh")

            master = get_batch_master_df()
            supplier_history = get_supplier_history_df()
            customer_history = get_customer_history_df()
            sto_incoming_history = get_sto_incoming_history_df()
            sto_return_history = get_sto_return_history_df()
        else:
            # Explicit rebuild remains the maintenance/recovery path. Keep each
            # expensive unit visible so CurrentStage identifies the real bottleneck.
            update_historical_build_job(
                job_id,
                progress=55,
                current_stage="Loading receipt and dispatch summaries",
            )
            receipt_summary, dispatch_summary = get_event_summaries()
            mark_stage("load_event_summaries")
            ensure_active()

            update_historical_build_job(
                job_id,
                progress=64,
                current_stage="Matching WMS batches with SFDA",
            )
            master = engine.build_master_from_summaries(
                receipt_summary, dispatch_summary, prepared["sfda_summary"],
            )
            logger.info(
                "HISTORICAL_PERF job_id=%s matching_result receipt_groups=%s dispatch_groups=%s master_rows=%s",
                job_id,
                len(receipt_summary),
                len(dispatch_summary),
                len(master),
            )
            mark_stage("match_wms_sfda_build_master")
            ensure_active()

            update_historical_build_job(
                job_id,
                progress=74,
                current_stage="Saving Batch Master to database",
            )
            replace_batch_master(master)
            mark_stage("save_batch_master")
            ensure_active()

            update_historical_build_job(
                job_id,
                progress=80,
                current_stage="Building Supplier and Customer History",
            )
            supplier_summary, customer_summary = get_history_summaries()
            mark_stage("load_history_summaries")
            ensure_active()

            supplier_history = engine.build_supplier_history(supplier_summary, master)
            customer_history = engine.build_customer_history(customer_summary, master)
            mark_stage("build_histories_in_memory")
            ensure_active()

            replace_supplier_history(supplier_history)
            replace_customer_history(customer_history)
            sto_incoming_history = get_sto_incoming_history_df()
            sto_return_history = get_sto_return_history_df()
            mark_stage("save_histories_and_load_sto")
            ensure_active()

        ensure_active()
        update_historical_build_job(
            job_id,
            progress=90,
            current_stage="Generating downloadable audit files",
        )

        # Historical Build now produces one consolidated workbook instead of
        # five separate files. This is an export/presentation change only;
        # all historical tables remain persisted separately in SQL.
        update_historical_build_job(
            job_id,
            progress=94,
            current_stage="Generating Historical Database workbook",
        )
        export_started = perf_counter()
        exported = Exporter.build_historical_database_workbook(
            batch_master=master,
            supplier_history=supplier_history,
            sto_incoming_history=sto_incoming_history,
            customer_history=customer_history,
            sto_return_history=sto_return_history,
            file_name="Historical_Database.xlsx",
        )
        file_name, file_bytes, mime_type = _decode_exported_file(exported)
        update_historical_build_job(
            job_id,
            progress=97,
            current_stage="Uploading Historical Database workbook",
        )
        saved = storage.upload_job_output(
            job_id,
            file_name,
            file_bytes,
            mime_type,
        )
        output_files: List[Dict[str, Any]] = [
            {
                "file_name": file_name,
                "content_type": mime_type,
                "size_bytes": saved.get("size_bytes", len(file_bytes)),
                "blob_name": saved.get("blob_name", ""),
                "download_url": (
                    f"/api/history/{job_id}/download"
                    f"?category=output&file_name={file_name}"
                ),
            }
        ]
        logger.info(
            "HISTORICAL_PERF job_id=%s export=%s sheets=7 batch_rows=%s "
            "supplier_rows=%s sto_in_rows=%s customer_rows=%s sto_return_rows=%s seconds=%.3f",
            job_id,
            file_name,
            len(master),
            len(supplier_history),
            len(sto_incoming_history),
            len(customer_history),
            len(sto_return_history),
            perf_counter() - export_started,
        )
        del exported, file_bytes

        mark_stage("generate_and_upload_audit_files")

        summary = {
            "asn_files": len(input_manifest.get("asn_files", [])),
            "dispatch_files": len(input_manifest.get("dispatch_files", [])),
            "prepared_receipt_events": len(prepared["receipt_events"]),
            "prepared_dispatch_events": len(prepared["dispatch_events"]),
            "inserted_receipt_events": inserted.get("receipt_events", 0),
            "inserted_dispatch_events": inserted.get("dispatch_events", 0),
            "batch_master_rows": len(master),
            "supplier_history_rows": len(supplier_history),
            "customer_history_rows": len(customer_history),
            "sto_incoming_rows": len(sto_incoming_history),
            "sto_return_rows": len(sto_return_history),
            "stage_timings_seconds": timings,
            "total_seconds": round(perf_counter() - job_started_at, 3),
        }

        ensure_active()
        update_historical_build_job(
            job_id,
            status="Completed",
            progress=100,
            current_stage="Historical data completed successfully",
            output_manifest={"files": output_files},
            summary=summary,
            error_message="",
            mark_completed=True,
        )

    except HistoricalBuildCancelled as exc:
        logger.warning(
            "Historical background build stopped cooperatively. job_id=%s reason=%s",
            job_id,
            exc,
        )
        # Do not overwrite the externally supplied Cancelled/Failed status.
        return
    except Exception as exc:
        logger.exception(
            "Historical background build failed. job_id=%s",
            job_id,
        )
        # Only mark Failed if the job is still active. If an admin/user already
        # cancelled it, preserve that terminal state.
        if historical_build_job_is_active(job_id, warehouse_id=warehouse_id):
            update_historical_build_job(
                job_id,
                status="Failed",
                progress=100,
                current_stage="Historical data build failed",
                error_message=str(exc),
                mark_completed=True,
            )
        raise
    finally:
        heartbeat_stop.set()
        if heartbeat_thread is not None and heartbeat_thread.is_alive():
            heartbeat_thread.join(timeout=2.0)
