from __future__ import annotations

import base64
import io
import inspect
import json
import logging
import mimetypes
import os
import threading
from time import perf_counter
from typing import Any, Dict, List

from azure.core.exceptions import ResourceExistsError
from azure.storage.queue import QueueClient, TextBase64EncodePolicy

import pandas as pd

from engine.blob_storage import BlobStorage, INPUTS_CONTAINER
from engine.database import (
    append_events,
    activate_historical_rebuild,
    remove_excluded_historical_keys,
    get_batch_master_df,
    get_customer_history_df,
    get_event_summaries,
    get_history_summaries,
    heartbeat_historical_build_job,
    historical_build_job_is_active,
    get_supplier_history_df,
    get_sto_incoming_history_df,
    get_returns_history_df,
    replace_batch_master,
    replace_customer_history,
    replace_supplier_history,
    replace_latest_sfda_snapshot,
    refresh_accept_history_incremental,
    refresh_dispatch_history_incremental,
    reconcile_affected_batch_master_event_totals,
    refresh_historical_append_incremental,
    update_historical_build_job,
)
from engine.exporter import Exporter
from engine.full_reconciliation import (
    HISTORICAL_RECEIPT_EVENT_KEY_VERSION,
    FullReconciliationEngine,
)
from engine.normalizer import HISTORICAL_MATCH_LOGIC_VERSION
from engine.warehouse_context import historical_build_scope, warehouse_scope


logger = logging.getLogger("SFDA-Reconciliation.HistoricalJobs")

HISTORICAL_JOB_WORKER_VERSION = "HISTORICAL_WORKER_V7_LPN_COLLISION_SAFE_20260902"


def _supports_keyword_argument(func: Any, keyword: str) -> bool:
    """Return True when the imported callable accepts the requested keyword.

    Azure can briefly execute mixed worker code during a deployment/recycle.
    Historical Append must stay compatible with both the pre-optimization
    database helper signature and the optimized signature that accepts
    ``include_counts``.
    """
    try:
        parameters = inspect.signature(func).parameters.values()
        return any(
            p.name == keyword or p.kind is inspect.Parameter.VAR_KEYWORD
            for p in parameters
        )
    except (TypeError, ValueError):
        return False


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



def _nonblank_max(series: pd.Series) -> str:
    values = [str(v).strip() for v in series.tolist() if pd.notna(v) and str(v).strip()]
    return max(values) if values else ""


def _build_rebuild_summaries_from_prepared(
    prepared: Dict[str, Any],
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """Build the four SQL-style cumulative summaries from the NEW upload only.

    This is the key two-phase rebuild boundary: no live historical table is read
    or deleted while the new Batch Master / histories are being prepared.
    """
    receipt = prepared.get("receipt_events")
    dispatch = prepared.get("dispatch_events")
    receipt = receipt.copy() if isinstance(receipt, pd.DataFrame) else pd.DataFrame(receipt or [])
    dispatch = dispatch.copy() if isinstance(dispatch, pd.DataFrame) else pd.DataFrame(dispatch or [])

    keys = ["BN", "Expiry Month Key", "Generic Item Number"]

    if receipt.empty:
        receipt_summary = pd.DataFrame()
        supplier_summary = pd.DataFrame()
    else:
        receipt["Received Quantity"] = pd.to_numeric(receipt["Received Quantity"], errors="coerce").fillna(0)
        receipt["Received Date"] = pd.to_datetime(receipt["Received Date"], errors="coerce")
        receipt["Expiry Date"] = pd.to_datetime(receipt["Expiry Date"], errors="coerce")
        shipment = receipt["Inbound Shipment"].fillna("").astype(str).str.strip().str.upper()
        eligible = receipt.loc[
            shipment.str.startswith(("TRK5060", "TRK800", "TRK49"))
        ].copy()
        eligible["_priority"] = 2
        eship = eligible["Inbound Shipment"].fillna("").astype(str).str.strip().str.upper()
        eligible.loc[eship.str.startswith("TRK5060"), "_priority"] = 0
        eligible.loc[eship.str.startswith("TRK800"), "_priority"] = 1
        eligible = eligible.sort_values(keys + ["_priority", "Received Date", "Event Key"], kind="stable")
        preferred = eligible.drop_duplicates(subset=keys, keep="first").copy()
        agg = eligible.groupby(keys, dropna=False).agg(
            **{
                "Receipt Expiry Date": ("Expiry Date", "max"),
                "Receive Runs": ("Event Key", "size"),
                "Total Receive Qty": ("Received Quantity", "sum"),
                "First Received Date": ("Received Date", "min"),
                "Last Received Date": ("Received Date", "max"),
            }
        ).reset_index()
        preferred = preferred.rename(columns={"Trade Item": "Trade Item Number"})
        keep = keys + ["Trade Item Number", "Trade Name", "Description", "Supplier Name", "Supplier Code", "Item Family Group"]
        receipt_summary = preferred[keep].merge(agg, on=keys, how="inner", validate="one_to_one")

        supplier_source = receipt.loc[shipment.str.startswith("TRK5060")].copy()
        if supplier_source.empty:
            supplier_summary = pd.DataFrame()
        else:
            skeys = ["Supplier Name", "Supplier Code", *keys]
            supplier_summary = supplier_source.groupby(skeys, dropna=False).agg(
                **{
                    "Expiry Date": ("Expiry Date", "max"),
                    "Trade Item Number": ("Trade Item", _nonblank_max),
                    "Trade Name": ("Trade Name", _nonblank_max),
                    "Description": ("Description", _nonblank_max),
                    "Item Family Group": ("Item Family Group", _nonblank_max),
                    "Received Quantity Each": ("Received Quantity", "sum"),
                    "First Received Date": ("Received Date", "min"),
                    "Last Received Date": ("Received Date", "max"),
                }
            ).reset_index()

    if dispatch.empty:
        dispatch_summary = pd.DataFrame()
        customer_summary = pd.DataFrame()
    else:
        dispatch["Dispatched Quantity"] = pd.to_numeric(dispatch["Dispatched Quantity"], errors="coerce").fillna(0)
        dispatch["Dispatch Date"] = pd.to_datetime(dispatch["Dispatch Date"], errors="coerce")
        dispatch["Expiry Date"] = pd.to_datetime(dispatch["Expiry Date"], errors="coerce")
        dispatch_summary = dispatch.groupby(keys, dropna=False).agg(
            **{
                "Dispatch Expiry Date": ("Expiry Date", "max"),
                "Trade Item Number": ("Trade Item Number", _nonblank_max),
                "Trade Name": ("Trade Name", _nonblank_max),
                "Custody": ("Custody", _nonblank_max),
                "Dispatch Runs": ("Event Key", "size"),
                "Total Dispatched Qty": ("Dispatched Quantity", "sum"),
                "First Dispatch Date": ("Dispatch Date", "min"),
                "Last Dispatch Date": ("Dispatch Date", "max"),
            }
        ).reset_index()

        ckeys = ["To Address", *keys]
        customer_summary = dispatch.groupby(ckeys, dropna=False).agg(
            **{
                "Expiry Date": ("Expiry Date", "max"),
                "Trade Item Number": ("Trade Item Number", _nonblank_max),
                "Trade Name": ("Trade Name", _nonblank_max),
                "Custody": ("Custody", _nonblank_max),
                "Dispatch Quantity Each": ("Dispatched Quantity", "sum"),
                "First Dispatch Date": ("Dispatch Date", "min"),
                "Last Dispatch Date": ("Dispatch Date", "max"),
            }
        ).reset_index()

    return receipt_summary, dispatch_summary, supplier_summary, customer_summary


def _build_sto_history_from_prepared(
    prepared: Dict[str, Any],
    master: pd.DataFrame,
    prefix: str,
    *,
    required_action: str = "",
) -> pd.DataFrame:
    receipt = prepared.get("receipt_events")
    receipt = receipt.copy() if isinstance(receipt, pd.DataFrame) else pd.DataFrame(receipt or [])
    if receipt.empty:
        return pd.DataFrame()

    shipment = receipt["Inbound Shipment"].fillna("").astype(str).str.strip().str.upper()
    source = receipt.loc[shipment.str.startswith(prefix.upper())].copy()
    if source.empty:
        return pd.DataFrame()

    source["Received Quantity"] = pd.to_numeric(source["Received Quantity"], errors="coerce").fillna(0)
    source["Received Date"] = pd.to_datetime(source["Received Date"], errors="coerce")
    source["Expiry Date"] = pd.to_datetime(source["Expiry Date"], errors="coerce")
    keys = ["Inbound Shipment", "Supplier Name", "Supplier Code", "BN", "Expiry Month Key", "Generic Item Number"]
    agg = source.groupby(keys, dropna=False).agg(
        **{
            "Receipt Expiry Date": ("Expiry Date", "max"),
            "Trade Item Number": ("Trade Item", _nonblank_max),
            "Receipt Trade Name": ("Trade Name", _nonblank_max),
            "Description": ("Description", _nonblank_max),
            "Item Family Group": ("Item Family Group", _nonblank_max),
            "Received Quantity Each": ("Received Quantity", "sum"),
            "First Received Date": ("Received Date", "min"),
            "Last Received Date": ("Received Date", "max"),
        }
    ).reset_index()

    m = master.copy() if master is not None else pd.DataFrame()
    if m.empty:
        return pd.DataFrame()
    m["Generic Item Number"] = m["Generic Item Number"].fillna("").astype(str).str.strip()
    exact_map = {}
    generic_map = {}
    for row in m.to_dict(orient="records"):
        exact_map.setdefault((str(row.get("BN", "")).strip(), str(row.get("Expiry Month Key", "")).strip()), row)
        generic = str(row.get("Generic Item Number", "")).strip()
        if generic:
            generic_map.setdefault(generic, row)

    rows = []
    for r in agg.to_dict(orient="records"):
        exact_key = (str(r.get("BN", "")).strip(), str(r.get("Expiry Month Key", "")).strip())
        generic = str(r.get("Generic Item Number", "")).strip()
        ref = exact_map.get(exact_key) or generic_map.get(generic)
        if not ref:
            continue
        package_size = pd.to_numeric(pd.Series([ref.get("PackageSize", 0)]), errors="coerce").fillna(0).iloc[0]
        qty_each = float(r.get("Received Quantity Each", 0) or 0)
        row = {
            "Inbound Shipment": r.get("Inbound Shipment", ""),
            "Source Warehouse": r.get("Supplier Name", ""),
            "Source Warehouse Code": r.get("Supplier Code", ""),
            "BN": r.get("BN", ""),
            "Expiry Month Key": r.get("Expiry Month Key", ""),
            "Expiry Date": ref.get("Expiry Date") if pd.notna(ref.get("Expiry Date")) else r.get("Receipt Expiry Date"),
            "Generic Item Number": generic,
            "GTIN": ref.get("GTIN", ""),
            "Drug Name": ref.get("Drug Name", ""),
            "Trade Description": ref.get("Trade Description", "") or r.get("Receipt Trade Name", ""),
            "Description": r.get("Description", ""),
            "Item Family Group": r.get("Item Family Group", ""),
            "PackageSize": float(package_size or 0),
            "Received Quantity Each": qty_each,
            "Received Quantity Pack": (qty_each / float(package_size)) if float(package_size or 0) > 0 else 0.0,
            "First Received Date": r.get("First Received Date"),
            "Last Received Date": r.get("Last Received Date"),
            "SFDA Match Status": (
                "Exact Batch in SFDA-Relevant Master" if exact_key in exact_map
                else "Generic Exists - STO Batch Missing from SFDA"
            ),
        }
        if required_action:
            row["Required Action"] = required_action
        rows.append(row)
    return pd.DataFrame(rows)

def _enqueue_historical_cleanup(job_id: str, warehouse_id: int, warehouse_name: str) -> None:
    """Queue old-generation cleanup after the new build is already active/completed."""
    connection_string = os.getenv("AzureWebJobsStorage", "").strip()
    if not connection_string:
        raise RuntimeError("AzureWebJobsStorage is missing.")
    queue = QueueClient.from_connection_string(
        connection_string,
        "reconciliation-jobs",
        message_encode_policy=TextBase64EncodePolicy(),
    )
    try:
        queue.create_queue()
    except ResourceExistsError:
        pass
    queue.send_message(
        json.dumps(
            {
                "job_id": f"{job_id}-cleanup",
                "job_type": "historical_cleanup",
                "source_job_id": job_id,
                "warehouse_id": int(warehouse_id),
                "warehouse_name": str(warehouse_name or f"Warehouse {warehouse_id}"),
            },
            ensure_ascii=False,
        )
    )


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
            "HISTORICAL_PERF job_id=%s warehouse_id=%s stage=%s seconds=%.3f total_seconds=%.3f",
            job_id,
            warehouse_id,
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
        logger.warning(
            "HISTORICAL_MATCH_LOGIC_VERSION job_id=%s warehouse_id=%s operation=%s version=%s worker=%s",
            job_id, warehouse_id, operation, HISTORICAL_MATCH_LOGIC_VERSION,
            HISTORICAL_JOB_WORKER_VERSION,
        )
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
        # Rebuild is two-phase: do not mutate live historical state before the
        # new dataset and workbook are fully prepared. Append keeps the existing
        # immediate snapshot behavior.
        if operation == "append":
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
        logger.warning(
            "Historical matcher loaded. job_id=%s version=%s",
            job_id, HISTORICAL_MATCH_LOGIC_VERSION,
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

        inserted = {"receipt_events": 0, "dispatch_events": 0}
        if operation == "append":
            update_historical_build_job(
                job_id,
                progress=40,
                current_stage="Saving receipt and dispatch events",
            )
            inserted = append_events(
                prepared["receipt_records"],
                prepared["dispatch_records"],
                assume_empty=False,
            )
            mark_stage("save_receipt_dispatch_events")
            ensure_active()

        has_new_events = (
            inserted.get("receipt_events", 0) > 0
            or inserted.get("dispatch_events", 0) > 0
        )

        # Surface the event-save sub-phases in SummaryJson. These timings remain
        # numeric so existing reporting on stage_timings_seconds stays compatible.
        for phase_name, seconds in (inserted.get("timings_seconds") or {}).items():
            try:
                timings[f"save_events_{phase_name}"] = round(float(seconds or 0), 3)
            except (TypeError, ValueError):
                pass

        if operation == "append":
            # Historical Append is still self-healing, but it no longer performs
            # Accept + Dispatch + final reconciliation as three independent SQL
            # scans. The unified refresh derives its scope from PREPARED INPUT KEYS
            # (not inserted counts), materializes durable events once, and repairs
            # BatchMaster plus Supplier/Customer history in one transaction.
            update_historical_build_job(
                job_id,
                progress=68,
                current_stage=(
                    "Refreshing affected historical batches"
                    if has_new_events
                    else "Rechecking existing historical batches"
                ),
            )

            receipt_records = prepared.get("receipt_records") or []
            if hasattr(receipt_records, "to_dict"):
                receipt_records = receipt_records.to_dict(orient="records")

            dispatch_records = prepared.get("dispatch_records") or []
            if hasattr(dispatch_records, "to_dict"):
                dispatch_records = dispatch_records.to_dict(orient="records")

            refresh_started = perf_counter()
            unified_refresh = refresh_historical_append_incremental(
                receipt_records,
                dispatch_records,
                sfda_df,
            )
            unified_seconds = round(perf_counter() - refresh_started, 3)

            timings["append_unified_refresh"] = unified_seconds
            timings["append_accept_scope_prepare"] = round(
                float(unified_refresh.get("scope_prepare_seconds", 0) or 0), 3
            )
            timings["append_accept_sql_refresh"] = round(
                float(unified_refresh.get("sql_refresh_seconds", 0) or 0), 3
            )
            # Legacy timing fields are retained for dashboards/queries that already
            # select them. The old third pass is intentionally gone.
            timings["append_accept_refresh"] = unified_seconds
            timings["append_dispatch_refresh"] = 0.0
            timings["append_affected_reconcile"] = 0.0

            for phase_name, seconds in (unified_refresh.get("timings_seconds") or {}).items():
                try:
                    timings[f"append_unified_{phase_name}"] = round(float(seconds or 0), 3)
                except (TypeError, ValueError):
                    pass

            logger.info(
                "Historical append unified refresh completed. movement_keys=%s "
                "accept_keys=%s dispatch_keys=%s batch_updated=%s batch_inserted=%s "
                "supplier_rows=%s customer_rows=%s",
                int(unified_refresh.get("affected_batch_keys", 0)),
                int(unified_refresh.get("accept_affected_batch_keys", 0)),
                int(unified_refresh.get("dispatch_affected_batch_keys", 0)),
                int(unified_refresh.get("batch_master_rows_updated", 0)),
                int(unified_refresh.get("batch_master_rows_inserted", 0)),
                int(unified_refresh.get("supplier_history_rows_rebuilt", 0)),
                int(unified_refresh.get("customer_history_rows_rebuilt", 0)),
            )

            mark_stage("incremental_historical_refresh")

            master = get_batch_master_df()
            supplier_history = get_supplier_history_df()
            customer_history = get_customer_history_df()
            sto_incoming_history = get_sto_incoming_history_df()
            returns_history = get_returns_history_df()
            mark_stage("load_historical_export_data")
        else:
            # SAFE REBUILD: build the complete replacement dataset from the new
            # uploads in memory. The currently active historical SQL data remains
            # available and untouched throughout preparation and export.
            update_historical_build_job(
                job_id,
                progress=35,
                current_stage="Building new historical summaries",
            )
            receipt_summary, dispatch_summary, supplier_summary, customer_summary = (
                _build_rebuild_summaries_from_prepared(prepared)
            )
            mark_stage("build_new_summaries_in_memory")
            ensure_active()

            update_historical_build_job(
                job_id,
                progress=55,
                current_stage="Matching new WMS history with SFDA",
            )
            master = engine.build_master_from_summaries(
                receipt_summary, dispatch_summary, prepared["sfda_summary"],
            )
            logger.info(
                "HISTORICAL_PERF job_id=%s safe_rebuild receipt_groups=%s dispatch_groups=%s master_rows=%s",
                job_id, len(receipt_summary), len(dispatch_summary), len(master),
            )
            mark_stage("build_new_batch_master_in_memory")
            ensure_active()

            update_historical_build_job(
                job_id,
                progress=70,
                current_stage="Building new Supplier, Customer and STO History",
            )
            supplier_history = engine.build_supplier_history(supplier_summary, master)
            customer_history = engine.build_customer_history(customer_summary, master)
            sto_incoming_history = _build_sto_history_from_prepared(prepared, master, "TRK800")
            return_parts = []
            for prefix, return_type in (
                ("TRK49", "STO Return"),
                ("TRK30", "Customer Return"),
            ):
                return_frame = _build_sto_history_from_prepared(
                    prepared,
                    master,
                    prefix,
                    required_action="Cancel Previous RSD Dispatch",
                )
                if return_frame.empty:
                    continue
                return_frame = return_frame.copy()
                return_frame["Return Type"] = return_type
                return_frame["Return From"] = return_frame.get("Source Warehouse", "")
                return_frame["Return From Code"] = return_frame.get(
                    "Source Warehouse Code", ""
                )
                return_parts.append(return_frame)
            returns_history = (
                pd.concat(return_parts, ignore_index=True, sort=False)
                if return_parts
                else pd.DataFrame()
            )
            mark_stage("build_new_histories_in_memory")
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
            returns_history=returns_history,
            file_name="Historical_Database.xlsx",
        )
        file_name, file_bytes, mime_type = _decode_exported_file(exported)
        mark_stage("generate_historical_database_workbook")
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
        mark_stage("upload_historical_database_workbook")
        logger.info(
            "HISTORICAL_PERF job_id=%s export=%s sheets=7 batch_rows=%s "
            "supplier_rows=%s sto_in_rows=%s customer_rows=%s return_rows=%s seconds=%.3f",
            job_id,
            file_name,
            len(master),
            len(supplier_history),
            len(sto_incoming_history),
            len(customer_history),
            len(returns_history),
            perf_counter() - export_started,
        )
        del exported, file_bytes

        activation_result: Dict[str, Any] = {}
        if operation == "rebuild":
            ensure_active()
            update_historical_build_job(
                job_id,
                progress=98,
                current_stage="Saving and activating new historical build",
            )

            def _activation_progress(_progress: int, stage: str, _extra: Dict[str, Any]) -> None:
                update_historical_build_job(
                    job_id, progress=98, current_stage=stage
                )

            activation_result = activate_historical_rebuild(
                prepared["receipt_records"],
                prepared["dispatch_records"],
                master,
                supplier_history,
                customer_history,
                build_id=job_id,
                source_job_id=job_id,
                progress_callback=_activation_progress,
            )
            # Activate the SFDA snapshot only after the historical replacement
            # transaction succeeds. A failed rebuild therefore leaves both the
            # previous history and previous snapshot intact.
            replace_latest_sfda_snapshot(sfda_df, sfda_source_name)
            inserted = {
                "receipt_events": int(activation_result.get("inserted_receipt_events", 0)),
                "dispatch_events": int(activation_result.get("inserted_dispatch_events", 0)),
            }
            mark_stage("activate_new_historical_database")

        if operation == "rebuild":
            match_diagnostics = engine.historical_match_diagnostics()
        else:
            match_diagnostics = dict(
                (unified_refresh or {}).get("match_diagnostics") or {}
            )
        match_diagnostics.setdefault("logic_version", HISTORICAL_MATCH_LOGIC_VERSION)

        summary = {
            "historical_job_worker_version": HISTORICAL_JOB_WORKER_VERSION,
            "historical_match_logic_version": HISTORICAL_MATCH_LOGIC_VERSION,
            "historical_receipt_event_key_version": HISTORICAL_RECEIPT_EVENT_KEY_VERSION,
            "receipt_event_key_diagnostics": dict(
                prepared.get("receipt_event_key_diagnostics") or {}
            ),
            "historical_match_diagnostics": match_diagnostics,
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
            # Legacy key is retained for dashboard/API compatibility; it now
            # represents the unified STO + customer return sheet.
            "sto_return_rows": len(returns_history),
            "return_history_rows": len(returns_history),
            "stage_timings_seconds": timings,
            "total_seconds": round(perf_counter() - job_started_at, 3),
            "activation": activation_result,
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

        # User-visible work is complete as soon as the new BuildID is active.
        # Cleanup runs later on the same durable background queue and cannot
        # delay the downloadable workbook or Append readiness.
        if operation == "rebuild":
            try:
                _enqueue_historical_cleanup(job_id, warehouse_id, warehouse_name)
                logger.info(
                    "Historical background cleanup queued. source_job_id=%s warehouse_id=%s",
                    job_id, warehouse_id,
                )
            except Exception:
                # Old inactive generations are harmless; a cleanup enqueue
                # failure must never turn a successful rebuild into Failed.
                logger.exception(
                    "Historical cleanup enqueue failed after successful activation. job_id=%s",
                    job_id,
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
