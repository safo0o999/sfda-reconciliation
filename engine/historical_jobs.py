from __future__ import annotations

import base64
import io
import logging
import mimetypes
from time import perf_counter
from typing import Any, Dict, List

import pandas as pd

from engine.blob_storage import BlobStorage, INPUTS_CONTAINER
from engine.database import (
    append_events,
    get_batch_master_df,
    get_customer_history_df,
    get_event_summaries,
    get_history_summaries,
    get_supplier_history_df,
    get_sto_incoming_history_df,
    get_sto_return_history_df,
    replace_batch_master,
    replace_customer_history,
    replace_supplier_history,
    replace_latest_sfda_snapshot,
    refresh_accept_history_incremental,
    refresh_dispatch_history_incremental,
    reset_history,
    update_historical_build_job,
)
from engine.exporter import Exporter
from engine.full_reconciliation import FullReconciliationEngine


logger = logging.getLogger("SFDA-Reconciliation.HistoricalJobs")


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
) -> None:
    """Execute a historical build outside the initiating HTTP request."""

    storage = BlobStorage()
    storage.initialize_containers()

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

    try:
        update_historical_build_job(
            job_id,
            status="Running",
            progress=5,
            current_stage="Reading uploaded historical files",
            mark_started=True,
            error_message="",
        )

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

        if operation == "rebuild":
            update_historical_build_job(
                job_id,
                progress=32,
                current_stage="Resetting previous historical data",
            )
            reset_history()
            mark_stage("reset_previous_history")

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

            mark_stage("incremental_historical_refresh")

            master = get_batch_master_df()
            supplier_history = get_supplier_history_df()
            customer_history = get_customer_history_df()
            sto_incoming_history = get_sto_incoming_history_df()
            sto_return_history = get_sto_return_history_df()
        else:
            # Explicit rebuild remains the maintenance/recovery path.
            update_historical_build_job(
                job_id, progress=68, current_stage="Building Batch Master",
            )
            receipt_summary, dispatch_summary = get_event_summaries()
            master = engine.build_master_from_summaries(
                receipt_summary, dispatch_summary, prepared["sfda_summary"],
            )
            replace_batch_master(master)
            mark_stage("build_and_save_batch_master")

            update_historical_build_job(
                job_id, progress=80, current_stage="Building Supplier and Customer History",
            )
            supplier_summary, customer_summary = get_history_summaries()
            supplier_history = engine.build_supplier_history(supplier_summary, master)
            customer_history = engine.build_customer_history(customer_summary, master)
            replace_supplier_history(supplier_history)
            replace_customer_history(customer_history)
            sto_incoming_history = get_sto_incoming_history_df()
            sto_return_history = get_sto_return_history_df()
            mark_stage("build_and_save_histories")

        update_historical_build_job(
            job_id,
            progress=90,
            current_stage="Generating downloadable audit files",
        )

        # Generate and upload one workbook at a time.  The old code built
        # all five base64-encoded workbooks in memory before uploading any of
        # them.  Customer History can be very large, so that caused avoidable
        # memory pressure and garbage-collection pauses.
        export_specs = [
            (
                master,
                "Batch_Master.xlsx",
                "Batch Master",
                "SFDA Historical Batch Master",
                ["Generic Item Number", "BN", "Expiry Date"],
            ),
            (
                supplier_history,
                "Supplier_History.xlsx",
                "Supplier History",
                "Historical Supplier Receipt History",
                ["Supplier Name", "Generic Item Number", "BN", "Expiry Date"],
            ),
            (
                customer_history,
                "Customer_History.xlsx",
                "Customer History",
                "Historical Customer Dispatch History",
                ["To Address", "Generic Item Number", "BN", "Expiry Date"],
            ),
            (
                sto_incoming_history,
                "STO_Incoming_History.xlsx",
                "STO Incoming",
                "Historical STO Incoming Receipt History",
                ["Source Warehouse", "Generic Item Number", "BN", "Expiry Date"],
            ),
            (
                sto_return_history,
                "STO_Return_Cancel_Dispatch.xlsx",
                "STO Return",
                "STO Returns - Cancel Previous RSD Dispatch",
                ["Source Warehouse", "Generic Item Number", "BN", "Expiry Date"],
            ),
        ]

        output_files: List[Dict[str, Any]] = []
        for export_index, (
            export_df,
            export_file_name,
            export_sheet_name,
            export_title,
            export_sort_columns,
        ) in enumerate(export_specs, start=1):
            update_historical_build_job(
                job_id,
                progress=min(98, 90 + export_index),
                current_stage=(
                    f"Generating audit file {export_index}/{len(export_specs)}: "
                    f"{export_file_name}"
                ),
            )
            export_started = perf_counter()
            exported = Exporter.build_formatted_excel_file(
                df=export_df,
                file_name=export_file_name,
                sheet_name=export_sheet_name,
                title=export_title,
                sort_columns=export_sort_columns,
            )
            file_name, file_bytes, mime_type = _decode_exported_file(exported)
            saved = storage.upload_job_output(
                job_id,
                file_name,
                file_bytes,
                mime_type,
            )
            output_files.append(
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
            )
            logger.info(
                "HISTORICAL_PERF job_id=%s export=%s rows=%s seconds=%.3f",
                job_id,
                file_name,
                len(export_df),
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

    except Exception as exc:
        logger.exception(
            "Historical background build failed. job_id=%s",
            job_id,
        )
        update_historical_build_job(
            job_id,
            status="Failed",
            progress=100,
            current_stage="Historical data build failed",
            error_message=str(exc),
            mark_completed=True,
        )
        raise
