import base64
import io
import json
import logging
import mimetypes
import uuid
import os
from datetime import datetime, timezone
from urllib.parse import quote
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple

import azure.functions as func
import pandas as pd
from azure.core.exceptions import ResourceExistsError
from azure.storage.queue import QueueClient


logger = logging.getLogger("SFDA-Reconciliation")
APPLICATION_NAME = "SFDA Reconciliation"
APPLICATION_VERSION = "5.0.0"
app = func.FunctionApp(http_auth_level=func.AuthLevel.ANONYMOUS)


def sanitize_json(obj):
    if isinstance(obj, dict):
        return {k: sanitize_json(v) for k, v in obj.items()}

    if isinstance(obj, list):
        return [sanitize_json(v) for v in obj]

    if isinstance(obj, tuple):
        return [sanitize_json(v) for v in obj]

    if pd.isna(obj):
        return None

    return obj


def json_response(data: Dict[str, Any], status_code: int = 200) -> func.HttpResponse:
    clean_data = sanitize_json(data)

    return func.HttpResponse(
        body=json.dumps(
            clean_data,
            ensure_ascii=False,
            default=str,
            allow_nan=False,
        ),
        status_code=status_code,
        mimetype="application/json",
        charset="utf-8",
    )


def error_response(message: str, status_code: int = 400, details: str = "") -> func.HttpResponse:
    return json_response(
        {
            "status": "Failed",
            "application": APPLICATION_NAME,
            "version": APPLICATION_VERSION,
            "error": message,
            "details": details,
        },
        status_code,
    )


def read_excel_upload(uploaded_file) -> pd.DataFrame:
    file_name = getattr(uploaded_file, "filename", None) or "uploaded.xlsx"
    file_bytes = uploaded_file.read()
    if not file_bytes:
        raise ValueError(f"The uploaded file '{file_name}' is empty.")
    engine = "xlrd" if file_name.lower().endswith(".xls") else "openpyxl"
    return pd.read_excel(io.BytesIO(file_bytes), engine=engine, dtype=object)


def read_excel_files(files: Iterable[Any]) -> pd.DataFrame:
    frames: List[pd.DataFrame] = []
    for uploaded_file in files or []:
        frame = read_excel_upload(uploaded_file)
        frame["_Source File"] = getattr(uploaded_file, "filename", "uploaded.xlsx")
        frames.append(frame)
    return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()


def count_positive_rows(frame: pd.DataFrame, column: str) -> int:
    if frame is None or frame.empty or column not in frame.columns:
        return 0
    return int((pd.to_numeric(frame[column], errors="coerce").fillna(0) > 0).sum())


def optional_batch_master() -> pd.DataFrame:
    """Return Batch Master when available; never block daily processing."""
    try:
        from engine.database import get_batch_master_df

        return get_batch_master_df()
    except Exception as exc:
        logger.warning("Batch Master optional read skipped: %s", exc)
        return pd.DataFrame()


def build_excel(df: pd.DataFrame, file_name: str, sheet_name: str, title: str):
    from engine.exporter import Exporter

    return Exporter.build_formatted_excel_file(
        df=df,
        file_name=file_name,
        sheet_name=sheet_name,
        title=title,
        sort_columns=["Generic Item Number", "BN", "Expiry Date"],
    )



def read_uploaded_bytes(uploaded_file) -> Tuple[str, bytes, str]:
    file_name = getattr(uploaded_file, "filename", None) or "uploaded.xlsx"
    file_bytes = uploaded_file.read()
    if not file_bytes:
        raise ValueError(f"The uploaded file '{file_name}' is empty.")
    content_type = (
        getattr(uploaded_file, "content_type", None)
        or mimetypes.guess_type(file_name)[0]
        or "application/octet-stream"
    )
    return file_name, file_bytes, content_type


def read_excel_bytes(file_name: str, file_bytes: bytes) -> pd.DataFrame:
    engine = "xlrd" if file_name.lower().endswith(".xls") else "openpyxl"
    return pd.read_excel(io.BytesIO(file_bytes), engine=engine, dtype=object)


def build_run_number(mode: str) -> str:
    stamp = datetime.now(timezone.utc).strftime("%Y%m%d-%H%M%S")
    suffix = uuid.uuid4().hex[:6].upper()
    return f"{mode.upper()}-{stamp}-{suffix}"


def get_submitted_by(req: func.HttpRequest) -> str:
    return (
        req.headers.get("X-MS-CLIENT-PRINCIPAL-NAME")
        or req.headers.get("X-User-Name")
        or req.form.get("submitted_by")
        or "Web User"
    )


def decode_generated_file(file_name: str, value: Any) -> Tuple[bytes, str, str]:
    guessed_type = mimetypes.guess_type(file_name)[0] or "application/octet-stream"

    if isinstance(value, str):
        return value.encode("utf-8-sig"), guessed_type, "text"

    if isinstance(value, dict):
        content = value.get("content", value.get("data", ""))
        encoding = str(value.get("encoding", "base64")).lower()
        content_type = (
            value.get("mime_type")
            or value.get("mime")
            or guessed_type
        )

        if encoding == "base64":
            cleaned = str(content or "")
            if "," in cleaned and cleaned.lower().startswith("data:"):
                cleaned = cleaned.split(",", 1)[1]
            return base64.b64decode(cleaned), content_type, "base64"

        return str(content or "").encode("utf-8-sig"), content_type, encoding

    return b"", guessed_type, "binary"


def iter_generated_files(outputs: Dict[str, Any]):
    for group_name, group in (outputs or {}).items():
        if not isinstance(group, dict):
            continue
        for file_name, value in group.items():
            yield group_name, str(file_name), value


def file_type_from_name(file_name: str) -> str:
    suffix = Path(file_name).suffix.lower().lstrip(".")
    return suffix.upper() if suffix else "FILE"


def save_run_file_record(
    run_number: str,
    category: str,
    file_name: str,
    file_type: str,
    uploaded: Dict[str, Any],
) -> None:
    from engine.database import save_reconciliation_run_file

    save_reconciliation_run_file(
        run_number=run_number,
        file_category=category,
        file_name=file_name,
        file_type=file_type,
        container_name=uploaded["container"],
        blob_name=uploaded["blob_name"],
        content_type=uploaded["content_type"],
        size_bytes=uploaded["size_bytes"],
        etag=uploaded.get("etag", ""),
    )


def save_run_archive(
    run_number: str,
    mode: str,
    input_files: List[Dict[str, Any]],
    outputs: Dict[str, Any],
    summary: Dict[str, Any],
    submitted_by: str,
) -> int:
    from engine.blob_storage import BlobStorage

    storage = BlobStorage()
    storage.initialize_containers()
    stored_files = 0

    for item in input_files:
        uploaded = storage.upload_input(
            run_number,
            item["file_name"],
            item["data"],
            item["content_type"],
        )
        save_run_file_record(
            run_number,
            "input",
            item["file_name"],
            file_type_from_name(item["file_name"]),
            uploaded,
        )
        stored_files += 1

    for _, file_name, value in iter_generated_files(outputs):
        file_bytes, content_type, _ = decode_generated_file(file_name, value)
        uploaded = storage.upload_output(
            run_number,
            file_name,
            file_bytes,
            content_type,
        )
        save_run_file_record(
            run_number,
            "output",
            file_name,
            file_type_from_name(file_name),
            uploaded,
        )
        stored_files += 1

    metadata = {
        "run_number": run_number,
        "process_type": mode,
        "status": "Completed",
        "submitted_by": submitted_by,
        "application": APPLICATION_NAME,
        "application_version": APPLICATION_VERSION,
        "created_at_utc": datetime.now(timezone.utc).isoformat(),
        "summary": summary,
        "input_files": [
            {
                "file_name": item["file_name"],
                "content_type": item["content_type"],
                "size_bytes": len(item["data"]),
            }
            for item in input_files
        ],
        "output_files": [
            file_name
            for _, file_name, _ in iter_generated_files(outputs)
        ],
    }
    metadata_bytes = json.dumps(
        metadata,
        ensure_ascii=False,
        default=str,
        indent=2,
    ).encode("utf-8")
    uploaded = storage.upload_metadata(run_number, metadata_bytes)
    save_run_file_record(
        run_number,
        "metadata",
        "run.json",
        "JSON",
        uploaded,
    )
    return stored_files + 1


def build_download_url(run_number: str, category: str, file_name: str) -> str:
    return (
        f"/api/history/{quote(run_number, safe='')}/download"
        f"?category={quote(category, safe='')}"
        f"&file_name={quote(file_name, safe='')}"
    )


@app.route(route="health", methods=["GET"])
def health(req: func.HttpRequest) -> func.HttpResponse:
    database_status = "Unavailable"
    database_details = ""
    try:
        from engine.database import test_database_connection

        test_database_connection()
        database_status = "Healthy"
    except Exception as exc:
        database_details = str(exc)

    return json_response(
        {
            "status": "Healthy",
            "application": APPLICATION_NAME,
            "version": APPLICATION_VERSION,
            "timestamp": pd.Timestamp.now().isoformat(),
            "components": {
                "azure_function": "Healthy",
                "database": database_status,
            },
            "database_details": database_details,
        }
    )


@app.route(route="version", methods=["GET"])
def version(req: func.HttpRequest) -> func.HttpResponse:
    return json_response(
        {
            "status": "Success",
            "application": APPLICATION_NAME,
            "version": APPLICATION_VERSION,
        }
    )



def load_blob_run_metadata(run_number: str) -> Optional[Dict[str, Any]]:
    try:
        from engine.blob_storage import BlobStorage, METADATA_CONTAINER

        storage = BlobStorage()
        downloaded = storage.download_blob(
            METADATA_CONTAINER,
            f"{run_number}/run.json",
        )
        return json.loads(downloaded["data"].decode("utf-8-sig"))
    except Exception:
        return None


def blob_run_to_history_row(
    run_number: str,
    metadata: Optional[Dict[str, Any]],
    files: List[Dict[str, Any]],
) -> Dict[str, Any]:
    data = metadata or {}
    summary = data.get("summary") or {}
    created = (
        data.get("created_at_utc")
        or data.get("started_at")
        or data.get("timestamp")
    )
    categories = [str(item.get("category", "")) for item in files]
    input_names = [
        str(item.get("file_name", "")).lower()
        for item in files
        if item.get("category") == "input"
    ]
    process_type = str(
        data.get("process_type")
        or data.get("step")
        or ("DISPATCH" if run_number.upper().startswith("DISPATCH") else "ACCEPT")
    ).upper()

    return {
        "RunID": None,
        "RunNumber": run_number,
        "ProcessType": process_type,
        "Status": data.get("status", "Completed"),
        "StartedAt": created,
        "CompletedAt": created,
        "SubmittedBy": data.get("submitted_by", "Web User"),
        "ASNFiles": sum("asn" in name or "asdt" in name for name in input_names),
        "InventoryFiles": sum("inventory" in name for name in input_names),
        "DispatchFiles": sum("dispatch" in name for name in input_names),
        "SFDAFiles": sum("sfda" in name or "drug count" in name for name in input_names),
        "TotalInputRows": summary.get(
            "total_input_rows",
            summary.get("reconciliation_rows", 0),
        ),
        "MasterRecords": summary.get("batch_master_rows", 0),
        "AcceptRecords": summary.get("accept_rows", 0),
        "DispatchRecords": summary.get("dispatch_rows", 0),
        "ExceptionRecords": summary.get("variance_rows", 0),
        "GeneratedFiles": sum(category in {"output", "metadata"} for category in categories),
        "ApplicationVersion": data.get(
            "application_version",
            data.get("version", APPLICATION_VERSION),
        ),
        "ErrorMessage": data.get("error_message", ""),
    }


def blob_files_for_ui(run_number: str) -> List[Dict[str, Any]]:
    from engine.blob_storage import BlobStorage

    rows: List[Dict[str, Any]] = []
    for item in BlobStorage().list_all_run_files(run_number):
        category = str(item.get("category", ""))
        file_name = str(item.get("file_name", ""))
        rows.append(
            {
                "RunFileID": None,
                "RunNumber": run_number,
                "FileCategory": category,
                "FileName": file_name,
                "FileType": file_type_from_name(file_name),
                "ContainerName": item.get("container"),
                "BlobName": item.get("blob_name"),
                "ContentType": item.get("content_type"),
                "SizeBytes": item.get("size_bytes", 0),
                "ETag": "",
                "CreatedAt": item.get("last_modified"),
                "download_url": build_download_url(
                    run_number,
                    category,
                    file_name,
                ),
            }
        )
    return rows


@app.route(route="history", methods=["GET"])
def history(req: func.HttpRequest) -> func.HttpResponse:
    try:
        from engine.blob_storage import BlobStorage
        from engine.database import list_reconciliation_runs

        limit = int(req.params.get("limit", "500") or 500)
        rows = list_reconciliation_runs(limit)
        known = {str(row.get("RunNumber", "")) for row in rows}

        storage = BlobStorage()
        storage.initialize_containers()
        for run_number in storage.list_run_numbers(limit):
            if run_number in known:
                continue
            files = storage.list_all_run_files(run_number)
            rows.append(
                blob_run_to_history_row(
                    run_number,
                    load_blob_run_metadata(run_number),
                    files,
                )
            )

        rows.sort(
            key=lambda row: str(
                row.get("StartedAt")
                or row.get("CompletedAt")
                or row.get("RunNumber")
                or ""
            ),
            reverse=True,
        )
        rows = rows[:limit]
        return json_response(
            {
                "status": "Success",
                "application": APPLICATION_NAME,
                "version": APPLICATION_VERSION,
                "count": len(rows),
                "history": rows,
            }
        )
    except Exception as exc:
        logger.exception("History read failed")
        return error_response("Failed to load reconciliation history.", 500, str(exc))


@app.route(route="history/{run_number}", methods=["GET"])
def history_run(req: func.HttpRequest) -> func.HttpResponse:
    run_number = str(req.route_params.get("run_number", "")).strip()
    if not run_number:
        return error_response("Run number is required.", 400)

    try:
        from engine.database import (
            get_reconciliation_run,
            list_reconciliation_run_files,
        )

        run = get_reconciliation_run(run_number)
        files = list_reconciliation_run_files(run_number)

        if not files:
            files = blob_files_for_ui(run_number)
        else:
            for file in files:
                file["download_url"] = build_download_url(
                    run_number,
                    str(file.get("FileCategory", "")),
                    str(file.get("FileName", "")),
                )

        if not run:
            metadata = load_blob_run_metadata(run_number)
            if not files and not metadata:
                return error_response("Reconciliation run was not found.", 404)
            run = blob_run_to_history_row(
                run_number,
                metadata,
                [
                    {
                        "category": file.get("FileCategory"),
                        "file_name": file.get("FileName"),
                    }
                    for file in files
                ],
            )

        return json_response(
            {
                "status": "Success",
                "application": APPLICATION_NAME,
                "version": APPLICATION_VERSION,
                "run": run,
                "files": files,
            }
        )
    except Exception as exc:
        logger.exception("Run details read failed")
        return error_response("Failed to load run details.", 500, str(exc))


@app.route(route="history/{run_number}/download", methods=["GET"])
def history_download(req: func.HttpRequest) -> func.HttpResponse:
    run_number = str(req.route_params.get("run_number", "")).strip()
    category = str(req.params.get("category", "")).strip().lower()
    file_name = str(req.params.get("file_name", "")).strip()

    if category not in {"input", "output", "metadata"}:
        return error_response("category must be input, output, or metadata.", 400)
    if not run_number or not file_name:
        return error_response("Run number and file name are required.", 400)

    try:
        from engine.blob_storage import (
            BlobStorage,
            INPUTS_CONTAINER,
            METADATA_CONTAINER,
            OUTPUTS_CONTAINER,
        )

        container_name = {
            "input": INPUTS_CONTAINER,
            "output": OUTPUTS_CONTAINER,
            "metadata": METADATA_CONTAINER,
        }[category]
        safe_name = BlobStorage.sanitize_file_name(file_name)
        blob_name = f"{run_number}/{safe_name}"
        downloaded = BlobStorage().download_blob(container_name, blob_name)

        return func.HttpResponse(
            body=downloaded["data"],
            status_code=200,
            mimetype=downloaded["content_type"],
            headers={
                "Content-Disposition": (
                    f'attachment; filename="{safe_name.replace(chr(34), "")}"'
                ),
                "Cache-Control": "no-store",
            },
        )
    except FileNotFoundError as exc:
        return error_response("Stored file was not found.", 404, str(exc))
    except Exception as exc:
        logger.exception("History file download failed")
        return error_response("Failed to download stored file.", 500, str(exc))


@app.route(route="historical/status", methods=["GET"])
def historical_status(req: func.HttpRequest) -> func.HttpResponse:
    try:
        from engine.database import get_historical_status

        status = get_historical_status()
        return json_response(
            {
                "status": "Ready",
                "application": APPLICATION_NAME,
                "version": APPLICATION_VERSION,
                **status,
            }
        )
    except Exception as exc:
        logger.exception("Historical status check failed")
        return error_response(
            "Failed to read historical data status.",
            500,
            str(exc),
        )


@app.route(route="batch-master/build", methods=["GET", "POST"])
def batch_master_build(req: func.HttpRequest) -> func.HttpResponse:
    """Queue a long-running historical build and return immediately."""

    if req.method == "GET":
        return json_response(
            {
                "status": "Ready",
                "application": APPLICATION_NAME,
                "version": APPLICATION_VERSION,
                "message": (
                    "Upload historical ASN and/or Full Dispatch files plus "
                    "the latest SFDA report."
                ),
            }
        )

    try:
        asn_files = req.files.getlist("asn_files")
        dispatch_files = req.files.getlist("dispatch_files")
        sfda_file = req.files.get("sfda")
        operation = str(
            req.form.get("operation", "append")
        ).strip().lower()

        if not asn_files and not dispatch_files:
            return error_response(
                "At least one ASN or Full Dispatch file is required."
            )
        if sfda_file is None:
            return error_response("SFDA file is required.")
        if operation not in {"append", "rebuild"}:
            return error_response(
                "operation must be append or rebuild."
            )

        from engine.blob_storage import BlobStorage
        from engine.database import create_historical_build_job

        job_id = build_run_number("historical")
        storage = BlobStorage()
        storage.initialize_containers()

        manifest: Dict[str, Any] = {
            "asn_files": [],
            "dispatch_files": [],
            "sfda_files": [],
        }

        for category, uploaded_files in (
            ("asn", asn_files),
            ("dispatch", dispatch_files),
            ("sfda", [sfda_file]),
        ):
            target_key = f"{category}_files"

            for uploaded_file in uploaded_files:
                file_name, file_bytes, content_type = read_uploaded_bytes(
                    uploaded_file
                )
                saved = storage.upload_job_input(
                    job_id,
                    category,
                    file_name,
                    file_bytes,
                    content_type,
                )
                manifest[target_key].append(
                    {
                        "file_name": saved["file_name"],
                        "blob_name": saved["blob_name"],
                        "content_type": saved["content_type"],
                        "size_bytes": saved["size_bytes"],
                    }
                )

        create_historical_build_job(
            job_id,
            operation,
            manifest,
        )

        # The producer must use the exact same storage account as the
        # Queue Trigger binding below. Using a different optional storage
        # connection can leave jobs permanently in Queued status.
        connection_string = os.getenv("AzureWebJobsStorage")
        if not connection_string:
            raise RuntimeError(
                "AzureWebJobsStorage is missing."
            )

        queue = QueueClient.from_connection_string(
            connection_string,
            "historical-build-jobs",
        )

        try:
            queue.create_queue()
        except ResourceExistsError:
            logger.info(
                "Historical build queue already exists; continuing."
            )

        queue.send_message(
            json.dumps(
                {
                    "job_id": job_id,
                    "operation": operation,
                    "input_manifest": manifest,
                },
                ensure_ascii=False,
            )
        )

        return json_response(
            {
                "status": "Accepted",
                "application": APPLICATION_NAME,
                "version": APPLICATION_VERSION,
                "job_id": job_id,
                "operation": operation,
                "status_url": (
                    f"/api/historical/build-status/{job_id}"
                ),
                "message": (
                    "Historical build was queued and will continue "
                    "in the background."
                ),
            },
            202,
        )

    except ValueError as exc:
        logger.exception("Historical build submission validation failed")
        return error_response(
            "Historical build submission failed.",
            400,
            str(exc),
        )
    except Exception as exc:
        logger.exception("Historical build submission failed")
        return error_response(
            "Failed to queue historical build.",
            500,
            str(exc),
        )


@app.route(
    route="historical/build-status/{job_id}",
    methods=["GET"],
)
def historical_build_status(req: func.HttpRequest) -> func.HttpResponse:
    job_id = str(
        req.route_params.get("job_id", "")
    ).strip()

    if not job_id:
        return error_response("job_id is required.", 400)

    try:
        from engine.database import get_historical_build_job

        job = get_historical_build_job(job_id)
        return json_response(
            {
                "status": "Success",
                "application": APPLICATION_NAME,
                "version": APPLICATION_VERSION,
                "job": job,
            }
        )
    except KeyError as exc:
        return error_response(
            "Historical build job was not found.",
            404,
            str(exc),
        )
    except Exception as exc:
        logger.exception("Historical build status read failed")
        return error_response(
            "Failed to read historical build status.",
            500,
            str(exc),
        )


@app.queue_trigger(
    arg_name="message",
    queue_name="historical-build-jobs",
    connection="AzureWebJobsStorage",
)
def historical_build_worker(
    message: func.QueueMessage,
) -> None:
    logger.info(
        "Historical build queue message received. message_id=%s",
        getattr(message, "id", ""),
    )

    payload = json.loads(
        message.get_body().decode("utf-8")
    )

    job_id = str(payload.get("job_id", "")).strip()
    operation = str(
        payload.get("operation", "append")
    ).strip().lower()
    input_manifest = payload.get("input_manifest") or {}

    if not job_id:
        raise ValueError(
            "Historical build queue message is missing job_id."
        )

    from engine.historical_jobs import process_historical_build_job

    process_historical_build_job(
        job_id,
        input_manifest,
        operation,
    )



@app.route(
    route="full-reconciliation/run",
    methods=["GET", "POST"],
)
def full_reconciliation_run(req: func.HttpRequest) -> func.HttpResponse:
    """Run the one-time Inventory/SFDA alignment against persisted history."""

    if req.method == "GET":
        return json_response(
            {
                "status": "Ready",
                "application": APPLICATION_NAME,
                "version": APPLICATION_VERSION,
                "required_files": ["inventory", "sfda"],
            }
        )

    try:
        inventory_file = req.files.get("inventory")
        sfda_file = req.files.get("sfda")

        if inventory_file is None:
            return error_response(
                "Current Inventory file is required for Full Reconciliation."
            )
        if sfda_file is None:
            return error_response(
                "Latest SFDA file is required for Full Reconciliation."
            )

        inventory_name, inventory_bytes, inventory_content_type = (
            read_uploaded_bytes(inventory_file)
        )
        sfda_name, sfda_bytes, sfda_content_type = read_uploaded_bytes(
            sfda_file
        )
        inventory_df = read_excel_bytes(inventory_name, inventory_bytes)
        sfda_df = read_excel_bytes(sfda_name, sfda_bytes)

        from engine.database import (
            get_batch_master_df,
            get_customer_history_df,
            get_supplier_history_df,
        )
        from engine.exporter import Exporter
        from engine.full_reconciliation import FullReconciliationEngine

        batch_master = get_batch_master_df()
        supplier_history = get_supplier_history_df()
        customer_history = get_customer_history_df()

        if batch_master.empty:
            return error_response(
                "Historical Batch Master is empty. Complete Step 1 first.",
                400,
            )

        engine = FullReconciliationEngine(
            pd.DataFrame(),
            pd.DataFrame(),
            sfda_df,
        )
        result = engine.run_full_reconciliation(
            inventory_df=inventory_df,
            sfda_df=sfda_df,
            batch_master_df=batch_master,
            supplier_history_df=supplier_history,
            customer_history_df=customer_history,
        )

        accept_details = result["accept_details"]
        supplier_variance = result["supplier_variance"]
        dispatch_details = result["dispatch_details"]
        summary_df = result["summary"]

        accept_upload = result["accept_upload"]
        dispatch_upload = result["dispatch_upload"]

        outputs: Dict[str, Any] = {
            "accept_details": Exporter.build_formatted_excel_file(
                df=accept_details,
                file_name="Full_Accept_Reconciliation.xlsx",
                sheet_name="Full Accept",
                title="One-Time Full Reconciliation - Accept",
                columns=list(accept_details.columns),
                sort_columns=["Generic Item Number", "BN", "Expiry Date"],
            ),
            "supplier_variance": Exporter.build_formatted_excel_file(
                df=supplier_variance,
                file_name="Supplier_Variance.xlsx",
                sheet_name="Supplier Variance",
                title="Supplier Quantity Variance",
                columns=list(supplier_variance.columns),
                sort_columns=["Supplier Name", "Generic Item Number", "BN"],
            ),
            "dispatch_details": Exporter.build_formatted_excel_file(
                df=dispatch_details,
                file_name="Full_Dispatch_Reconciliation.xlsx",
                sheet_name="Full Dispatch",
                title="One-Time Full Reconciliation - Dispatch",
                columns=list(dispatch_details.columns),
                sort_columns=["To Address", "Generic Item Number", "BN"],
            ),
            "summary": Exporter.build_formatted_excel_file(
                df=summary_df,
                file_name="Full_Reconciliation_Summary.xlsx",
                sheet_name="Summary",
                title="Full Reconciliation Summary",
                columns=list(summary_df.columns),
            ),
            "accept_files": Exporter.build_sfda_upload_files(
                accept_upload,
                "To Be Accept",
                "SFDA_Full_Accept",
            ),
            "dispatch_files": Exporter.build_dispatch_files_by_customer(
                dispatch_upload
            ),
        }

        accept_rows = int(len(accept_upload))
        dispatch_rows = int(len(dispatch_upload))
        variance_rows = int(
            pd.to_numeric(
                supplier_variance.get("Supplier Variance", 0),
                errors="coerce",
            ).fillna(0).ne(0).sum()
        )
        generated_files = sum(
            len(group) if isinstance(group, dict) else 0
            for group in outputs.values()
        )

        summary = {
            "inventory_rows": int(len(inventory_df)),
            "sfda_rows": int(len(sfda_df)),
            "batch_master_rows": int(len(batch_master)),
            "supplier_history_rows": int(len(supplier_history)),
            "customer_history_rows": int(len(customer_history)),
            "reconciliation_rows": int(
                len(accept_details) + len(dispatch_details)
            ),
            "accept_rows": accept_rows,
            "supplier_variance_rows": variance_rows,
            "dispatch_allocation_rows": dispatch_rows,
            "accept_files": len(outputs["accept_files"]),
            "dispatch_files": len(outputs["dispatch_files"]),
            "generated_files": generated_files,
        }

        return json_response(
            {
                "status": "Completed",
                "application": APPLICATION_NAME,
                "version": APPLICATION_VERSION,
                "step": "full-reconciliation",
                "summary": summary,
                "outputs": outputs,
            }
        )

    except ValueError as exc:
        logger.exception("Full reconciliation validation failed")
        return error_response(
            "Full Reconciliation validation failed.",
            400,
            str(exc),
        )
    except Exception as exc:
        logger.exception("Full reconciliation failed")
        return error_response(
            "Full Reconciliation failed.",
            500,
            str(exc),
        )


def run_daily(req: func.HttpRequest, mode: str) -> func.HttpResponse:
    run_number = build_run_number(mode)
    submitted_by = get_submitted_by(req)
    run_created = False

    try:
        sfda_file = req.files.get("sfda")
        if sfda_file is None:
            return error_response("SFDA file is required.")

        input_files: List[Dict[str, Any]] = []

        sfda_name, sfda_bytes, sfda_content_type = read_uploaded_bytes(sfda_file)
        sfda_df = read_excel_bytes(sfda_name, sfda_bytes)
        input_files.append(
            {
                "file_name": sfda_name,
                "data": sfda_bytes,
                "content_type": sfda_content_type,
            }
        )

        asn_df = pd.DataFrame()
        dispatch_df = pd.DataFrame()
        inventory_df = pd.DataFrame()

        asn_file_count = 0
        dispatch_file_count = 0

        if mode == "accept":
            asn_file = req.files.get("asn")
            if asn_file is None:
                return error_response("ASN/ASDT file is required for Accept.")
            asn_name, asn_bytes, asn_content_type = read_uploaded_bytes(asn_file)
            asn_df = read_excel_bytes(asn_name, asn_bytes)
            input_files.append(
                {
                    "file_name": asn_name,
                    "data": asn_bytes,
                    "content_type": asn_content_type,
                }
            )
            asn_file_count = 1
        else:
            dispatch_file = req.files.get("dispatch")
            if dispatch_file is None:
                return error_response("Full Dispatch file is required for Dispatch.")
            dispatch_name, dispatch_bytes, dispatch_content_type = read_uploaded_bytes(
                dispatch_file
            )
            dispatch_df = read_excel_bytes(dispatch_name, dispatch_bytes)
            input_files.append(
                {
                    "file_name": dispatch_name,
                    "data": dispatch_bytes,
                    "content_type": dispatch_content_type,
                }
            )
            dispatch_file_count = 1

        from engine.database import create_reconciliation_run

        create_reconciliation_run(
            run_number=run_number,
            process_type=mode,
            submitted_by=submitted_by,
            application_version=APPLICATION_VERSION,
            asn_files=asn_file_count,
            inventory_files=0,
            dispatch_files=dispatch_file_count,
            sfda_files=1,
        )
        run_created = True

        from engine.exporter import Exporter
        from engine.reconciliation import ReconciliationEngine

        batch_master = optional_batch_master()

        processed_transactions = pd.DataFrame()
        try:
            from engine.database import get_daily_processed_transactions

            processed_transactions = get_daily_processed_transactions(mode.upper())
        except Exception as exc:
            logger.warning("Daily processed transaction read skipped: %s", exc)

        result = ReconciliationEngine(
            mode=mode,
            sfda_df=sfda_df,
            asn_df=asn_df,
            dispatch_df=dispatch_df,
            inventory_df=inventory_df,
            batch_master_df=batch_master,
            processed_transactions_df=processed_transactions,
        ).run()

        report = result["report"]
        accept = result["accept"]
        dispatch = result["dispatch"]

        outputs: Dict[str, Any] = {}
        if mode == "accept":
            outputs["accept_details"] = build_excel(
                report,
                "Accept_Details.xlsx",
                "Accept Details",
                "SFDA Accept Details",
            )
            outputs["accept_files"] = Exporter.build_sfda_upload_files(
                accept,
                "To Be Accept",
                "SFDA_Accept",
            )
            outputs["dispatch_files"] = {}
        else:
            outputs["dispatch_details"] = build_excel(
                report,
                "Dispatch_Details.xlsx",
                "Dispatch Details",
                "SFDA Dispatch Details",
            )
            outputs["dispatch_files"] = Exporter.build_dispatch_files_by_customer(
                dispatch
            )
            outputs["accept_files"] = {}

        processed_rows = result.get("processed_transactions", pd.DataFrame())
        saved_transactions = 0
        if processed_rows is not None and not processed_rows.empty:
            from engine.database import save_daily_processed_transactions

            saved_transactions = save_daily_processed_transactions(
                mode.upper(),
                processed_rows.to_dict(orient="records"),
            )

        accept_rows = count_positive_rows(accept, "To Be Accept")
        dispatch_rows = count_positive_rows(
            dispatch,
            "Allocated To Be Dispatch",
        )
        generated_output_files = sum(
            len(group) if isinstance(group, dict) else 0
            for group in outputs.values()
        )
        total_input_rows = int(len(sfda_df) + len(asn_df) + len(dispatch_df))

        summary = {
            "batch_master_available": not batch_master.empty,
            "batch_master_rows": len(batch_master),
            "reconciliation_rows": len(report),
            "total_input_rows": total_input_rows,
            "accept_rows": accept_rows,
            "dispatch_rows": dispatch_rows,
            "accept_files": len(outputs.get("accept_files", {})),
            "dispatch_files": len(outputs.get("dispatch_files", {})),
            "generated_files": generated_output_files,
            "processed_transactions_saved": saved_transactions,
        }

        archived_files = save_run_archive(
            run_number=run_number,
            mode=mode,
            input_files=input_files,
            outputs=outputs,
            summary=summary,
            submitted_by=submitted_by,
        )

        from engine.database import complete_reconciliation_run

        complete_reconciliation_run(
            run_number=run_number,
            status="Completed",
            total_input_rows=total_input_rows,
            master_records=len(batch_master),
            accept_records=accept_rows,
            dispatch_records=dispatch_rows,
            exception_records=0,
            generated_files=archived_files,
        )

        summary["archived_files"] = archived_files

        return json_response(
            {
                "status": "Completed",
                "application": APPLICATION_NAME,
                "version": APPLICATION_VERSION,
                "run_number": run_number,
                "step": mode,
                "summary": summary,
                "outputs": outputs,
            }
        )
    except ValueError as exc:
        logger.exception("Daily reconciliation validation failed")
        if run_created:
            try:
                from engine.database import complete_reconciliation_run

                complete_reconciliation_run(
                    run_number=run_number,
                    status="Failed",
                    error_message=str(exc),
                )
            except Exception:
                logger.exception("Failed to update failed run history")
        return error_response(f"{mode.title()} validation failed.", 400, str(exc))
    except Exception as exc:
        logger.exception("Daily reconciliation failed")
        if run_created:
            try:
                from engine.database import complete_reconciliation_run

                complete_reconciliation_run(
                    run_number=run_number,
                    status="Failed",
                    error_message=str(exc),
                )
            except Exception:
                logger.exception("Failed to update failed run history")
        return error_response(f"{mode.title()} reconciliation failed.", 500, str(exc))


@app.route(route="process-accept", methods=["GET", "POST"])
def process_accept(req: func.HttpRequest) -> func.HttpResponse:
    if req.method == "GET":
        return json_response({"status": "Ready", "step": "accept", "version": APPLICATION_VERSION})
    return run_daily(req, "accept")


@app.route(route="process-dispatch", methods=["GET", "POST"])
def process_dispatch(req: func.HttpRequest) -> func.HttpResponse:
    if req.method == "GET":
        return json_response({"status": "Ready", "step": "dispatch", "version": APPLICATION_VERSION})
    return run_daily(req, "dispatch")


@app.route(route="reconcile", methods=["GET", "POST"])
def reconcile(req: func.HttpRequest) -> func.HttpResponse:
    """Backward-compatible route used by older UI versions."""
    if req.method == "GET":
        return json_response(
            {
                "status": "Ready",
                "message": "Use process_type=accept or process_type=dispatch.",
                "version": APPLICATION_VERSION,
            }
        )
    mode = str(req.form.get("process_type", req.params.get("process_type", ""))).lower()
    if not mode:
        mode = "accept" if req.files.get("asn") is not None else "dispatch"
    if mode not in {"accept", "dispatch"}:
        return error_response("process_type must be accept or dispatch.")
    return run_daily(req, mode)


@app.route(route="ui", methods=["GET"])
def serve_ui(req: func.HttpRequest) -> func.HttpResponse:
    index_path = Path(__file__).resolve().parent / "web" / "index.html"
    if not index_path.exists():
        return error_response("UI file was not found.", 404)
    return func.HttpResponse(
        index_path.read_text(encoding="utf-8"),
        status_code=200,
        mimetype="text/html",
        charset="utf-8",
    )


@app.route(route="", methods=["GET"])
def root(req: func.HttpRequest) -> func.HttpResponse:
    return serve_ui(req)
