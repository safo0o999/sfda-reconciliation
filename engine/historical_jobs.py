from __future__ import annotations

import base64
import io
import logging
import mimetypes
from typing import Any, Dict, List

import pandas as pd

from engine.blob_storage import BlobStorage, INPUTS_CONTAINER
from engine.database import (
    append_events,
    get_event_summaries,
    get_history_summaries,
    replace_batch_master,
    replace_customer_history,
    replace_supplier_history,
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

        if operation == "rebuild":
            update_historical_build_job(
                job_id,
                progress=32,
                current_stage="Resetting previous historical data",
            )
            reset_history()

        update_historical_build_job(
            job_id,
            progress=40,
            current_stage="Saving receipt and dispatch events",
        )
        inserted = append_events(
            prepared["receipt_records"],
            prepared["dispatch_records"],
        )

        update_historical_build_job(
            job_id,
            progress=68,
            current_stage="Building Batch Master",
        )
        receipt_summary, dispatch_summary = get_event_summaries()
        master = engine.build_master_from_summaries(
            receipt_summary,
            dispatch_summary,
            prepared["sfda_summary"],
        )
        replace_batch_master(master)

        update_historical_build_job(
            job_id,
            progress=80,
            current_stage="Building Supplier and Customer History",
        )
        supplier_summary, customer_summary = get_history_summaries()
        supplier_history = engine.build_supplier_history(
            supplier_summary,
            master,
        )
        customer_history = engine.build_customer_history(
            customer_summary,
            master,
        )
        replace_supplier_history(supplier_history)
        replace_customer_history(customer_history)

        update_historical_build_job(
            job_id,
            progress=90,
            current_stage="Generating downloadable audit files",
        )

        generated = [
            Exporter.build_formatted_excel_file(
                df=master,
                file_name="Batch_Master.xlsx",
                sheet_name="Batch Master",
                title="SFDA Historical Batch Master",
                sort_columns=[
                    "Generic Item Number",
                    "BN",
                    "Expiry Date",
                ],
            ),
            Exporter.build_formatted_excel_file(
                df=supplier_history,
                file_name="Supplier_History.xlsx",
                sheet_name="Supplier History",
                title="Historical Supplier Receipt History",
                sort_columns=[
                    "Supplier Name",
                    "Generic Item Number",
                    "BN",
                    "Expiry Date",
                ],
            ),
            Exporter.build_formatted_excel_file(
                df=customer_history,
                file_name="Customer_History.xlsx",
                sheet_name="Customer History",
                title="Historical Customer Dispatch History",
                sort_columns=[
                    "To Address",
                    "Generic Item Number",
                    "BN",
                    "Expiry Date",
                ],
            ),
        ]

        output_files: List[Dict[str, Any]] = []
        for exported in generated:
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
