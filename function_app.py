import base64
import io
import json
import logging
import mimetypes
import uuid
import os
import time
import threading
from datetime import datetime, timezone, timedelta
from zoneinfo import ZoneInfo
from urllib.parse import quote
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple

import azure.functions as func
import pandas as pd
from azure.core.exceptions import ResourceExistsError
from azure.storage.queue import QueueClient, TextBase64EncodePolicy


logger = logging.getLogger("SFDA-Reconciliation")
APPLICATION_NAME = "SFDA Reconciliation"
APPLICATION_VERSION = "6.0.0"
app = func.FunctionApp(http_auth_level=func.AuthLevel.ANONYMOUS)


_VARIANCE_CACHE: Dict[int, Dict[str, Any]] = {}
_VARIANCE_CACHE_LOCK = threading.Lock()
_VARIANCE_CACHE_TTL_SECONDS = int(os.getenv("VARIANCE_CACHE_TTL_SECONDS", "300") or 300)


def _refresh_dashboard_summary_safe() -> None:
    """Refresh cached Home metrics without failing the business transaction."""
    try:
        from engine.database import refresh_dashboard_summary_cache
        refresh_dashboard_summary_cache()
    except Exception:
        logger.exception("Dashboard summary cache refresh failed")


def _cookie_value(req: func.HttpRequest, name: str) -> str:
    raw = str(req.headers.get("Cookie", "") or "")
    for part in raw.split(";"):
        key, sep, value = part.strip().partition("=")
        if sep and key == name:
            return value
    return ""


def _auth_required() -> bool:
    return os.getenv("AUTH_REQUIRED", "true").strip().lower() not in {"0", "false", "no", "off"}


def _current_user(req: func.HttpRequest) -> Optional[Dict[str, Any]]:
    if not _auth_required():
        return {
            "UserID": 0,
            "Email": req.headers.get("X-MS-CLIENT-PRINCIPAL-NAME") or "Development User",
            "Role": "Admin",
            "Status": "Active",
            "WarehouseID": 1,
            "WarehouseName": "Madinah Warehouse",
            "WarehouseCode": "MADINAH",
        }
    try:
        from engine.auth import session_user
        user = session_user(_cookie_value(req, "sfda_session"))
        if user:
            from engine.warehouse_context import set_current_warehouse
            set_current_warehouse(
                int(user.get("WarehouseID") or 1),
                str(user.get("WarehouseName") or "Madinah Warehouse"),
            )
        return user
    except Exception:
        logger.exception("Authentication lookup failed")
        return None


def _auth_guard(req: func.HttpRequest, admin: bool = False) -> Optional[func.HttpResponse]:
    user = _current_user(req)
    if not user:
        return error_response("Authentication required.", 401)
    if admin and str(user.get("Role", "")).lower() != "admin":
        return error_response("Administrator access is required.", 403)
    from engine.warehouse_context import set_current_warehouse
    set_current_warehouse(
        int(user.get("WarehouseID") or 1),
        str(user.get("WarehouseName") or "Madinah Warehouse"),
    )
    return None


def _request_base_url(req: func.HttpRequest) -> str:
    forwarded_proto = str(req.headers.get("X-Forwarded-Proto", "") or "").split(",")[0].strip()
    proto = forwarded_proto or "https"
    host = str(req.headers.get("Host", "") or "").strip()
    if host:
        return f"{proto}://{host}"
    url = str(req.url or "")
    marker = "/api/"
    return url.split(marker, 1)[0] if marker in url else url.rsplit("/", 1)[0]


def _json_response_with_cookie(
    data: Dict[str, Any],
    token: str,
    max_age: int,
    status_code: int = 200,
) -> func.HttpResponse:
    clean_data = sanitize_json(data)
    cookie = (
        f"sfda_session={token}; Path=/; Max-Age={max_age}; "
        "HttpOnly; Secure; SameSite=Lax"
    )
    return func.HttpResponse(
        body=json.dumps(clean_data, ensure_ascii=False, default=str, allow_nan=False),
        status_code=status_code,
        mimetype="application/json",
        charset="utf-8",
        headers={"Set-Cookie": cookie, "Cache-Control": "no-store"},
    )



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
    user = _current_user(req)
    if user and user.get("Email"):
        return str(user.get("Email"))
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


@app.timer_trigger(
    schedule="0 15 */6 * * *",
    arg_name="timer",
    run_on_startup=False,
    use_monitor=True,
)
def input_retention_cleanup(timer: func.TimerRequest) -> None:
    """Keep uploaded input files for 24h; preserve outputs and metadata."""
    try:
        from engine.blob_storage import BlobStorage
        retention_hours = int(os.getenv("INPUT_RETENTION_HOURS", "24") or 24)
        storage = BlobStorage()
        storage.initialize_containers()
        result = storage.cleanup_expired_inputs(retention_hours=retention_hours)
        logger.info("Input retention cleanup completed: %s", result)
    except Exception:
        logger.exception("Input retention cleanup failed")


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
        or (
            "HISTORICAL_BUILD"
            if run_number.upper().startswith("HISTORICAL")
            else (
                "FULL_DISPATCH"
                if run_number.upper().startswith("FULL-DISPATCH")
                else (
                    "FULL_ACCEPT"
                    if run_number.upper().startswith("FULL-ACCEPT")
                    else (
                        "DISPATCH"
                        if run_number.upper().startswith("DISPATCH")
                        else "ACCEPT"
                    )
                )
            )
        )
    ).upper()

    return {
        "RunID": None,
        "RunNumber": run_number,
        "ProcessType": process_type,
        "Status": data.get("status", "Completed"),
        "StartedAt": created,
        "CompletedAt": created,
        "SubmittedBy": str(
            data.get("submitted_by")
            or data.get("submittedBy")
            or data.get("user")
            or "Web User"
        ),
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




def historical_job_to_history_row(job: Dict[str, Any]) -> Dict[str, Any]:
    """Convert one HistoricalBuildJobs row to the unified History schema."""

    manifest = job.get("input_manifest") or {}
    summary = job.get("summary") or {}
    output_manifest = job.get("output_manifest") or {}
    output_files = output_manifest.get("files") or []

    return {
        "RunID": None,
        "RunNumber": str(job.get("job_id", "")),
        "ProcessType": "HISTORICAL_BUILD",
        "Status": job.get("status", ""),
        "StartedAt": job.get("started_at") or job.get("created_at"),
        "CompletedAt": job.get("completed_at"),
        "SubmittedBy": str(
            job.get("submitted_by")
            or manifest.get("submitted_by")
            or manifest.get("submittedBy")
            or "Web User"
        ),
        "ASNFiles": len(manifest.get("asn_files") or []),
        "InventoryFiles": 0,
        "DispatchFiles": len(manifest.get("dispatch_files") or []),
        "SFDAFiles": len(manifest.get("sfda_files") or []),
        "TotalInputRows": int(
            summary.get("prepared_receipt_events", 0)
            + summary.get("prepared_dispatch_events", 0)
        ),
        "MasterRecords": int(summary.get("batch_master_rows", 0)),
        "AcceptRecords": 0,
        "DispatchRecords": 0,
        "ExceptionRecords": 0,
        "GeneratedFiles": len(output_files),
        "ApplicationVersion": APPLICATION_VERSION,
        "ErrorMessage": job.get("error", ""),
    }


def _history_datetime_fields(
    row: Dict[str, Any],
    files: List[Dict[str, Any]] | None = None,
) -> Dict[str, Any]:
    """Add explicit UTC and Asia/Riyadh timestamps for History API responses.

    For older Full Reconciliation runs where StartedAt / CompletedAt were not
    persisted, recover the timestamp from the archived Blob files. This uses
    actual stored file timestamps and does not invent a run date.
    """
    result = dict(row)
    riyadh = ZoneInfo("Asia/Riyadh")

    if not result.get("StartedAt") and files:
        parsed_dates = []
        for file in files:
            candidate = (
                file.get("CreatedAt")
                or file.get("last_modified")
                or file.get("LastModified")
            )
            if not candidate:
                continue
            try:
                ts = pd.to_datetime(candidate, utc=True, errors="coerce")
                if not pd.isna(ts):
                    parsed_dates.append(ts.to_pydatetime())
            except Exception:
                continue

        if parsed_dates:
            result["StartedAt"] = min(parsed_dates)
            if not result.get("CompletedAt"):
                result["CompletedAt"] = max(parsed_dates)

    for field in ("StartedAt", "CompletedAt", "CreatedAt"):
        value = result.get(field)
        if value is None:
            continue

        if isinstance(value, pd.Timestamp):
            value = value.to_pydatetime()

        if not isinstance(value, datetime):
            try:
                parsed = pd.to_datetime(value, utc=True, errors="coerce")
                if pd.isna(parsed):
                    continue
                value = parsed.to_pydatetime()
            except Exception:
                continue

        utc_value = (
            value.replace(tzinfo=timezone.utc)
            if value.tzinfo is None
            else value.astimezone(timezone.utc)
        )
        result[f"{field}Utc"] = utc_value.isoformat().replace("+00:00", "Z")
        result[f"{field}Saudi"] = utc_value.astimezone(riyadh).isoformat()

    return result


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



@app.route(route="auth/status", methods=["GET"])
def auth_status(req: func.HttpRequest) -> func.HttpResponse:
    try:
        from engine.auth import auth_settings
        settings = auth_settings()
        user = _current_user(req)
        if user:
            from engine.warehouse_context import set_current_warehouse
            set_current_warehouse(int(user.get("WarehouseID") or 1), str(user.get("WarehouseName") or "Madinah Warehouse"))
        return json_response({
            "status": "Success",
            "authenticated": bool(user),
            "user": user,
            "settings": settings,
        })
    except Exception as exc:
        return error_response("Unable to read authentication status.", 500, str(exc))


@app.route(route="auth/warehouses", methods=["GET"])
def auth_warehouses(req: func.HttpRequest) -> func.HttpResponse:
    try:
        from engine.auth import registration_warehouses
        return json_response({
            "status": "Success",
            "warehouses": registration_warehouses(),
        })
    except Exception as exc:
        logger.exception("Unable to load registration warehouses")
        return error_response("Unable to load warehouse list.", 500, str(exc))


@app.route(route="auth/register", methods=["POST"])
def auth_register(req: func.HttpRequest) -> func.HttpResponse:
    try:
        from engine.auth import register_user
        payload = req.get_json() or {}
        result = register_user(
            str(payload.get("email") or payload.get("username") or ""),
            str(payload.get("password") or ""),
            int(payload.get("warehouse_id") or 0),
            _request_base_url(req),
        )
        first_admin = bool(result.get("first_admin"))
        return json_response({
            "status": "Active" if first_admin else "Pending Approval",
            "message": (
                "Administrator account created for Madinah Warehouse. You can sign in now."
                if first_admin
                else (
                    "Your access request was created. An approval email has been sent to the administrator."
                    if result.get("approval_email_sent")
                    else "Your access request was created and is waiting for administrator approval."
                )
            ),
            "first_admin": first_admin,
            "approval_email_sent": bool(result.get("approval_email_sent")),
            "approval_email_error": result.get("approval_email_error") or "",
        }, 201)
    except ValueError as exc:
        return error_response("Unable to create user.", 400, str(exc))
    except Exception as exc:
        logger.exception("User registration failed")
        return error_response("Unable to create user.", 500, str(exc))


@app.route(route="auth/approve", methods=["GET"])
def auth_approve(req: func.HttpRequest) -> func.HttpResponse:
    token = str(req.params.get("token", "") or "").strip()
    try:
        from engine.auth import approve_registration
        user = approve_registration(token)
        html = f"""<!doctype html><html><head><meta charset="utf-8"><title>Access Approved</title>
        <style>body{{font-family:Arial;background:#f3f7fb;color:#0b2c4a;display:grid;place-items:center;height:100vh;margin:0}}
        .card{{background:white;padding:32px;border-radius:16px;box-shadow:0 10px 30px #0b2c4a18;max-width:560px;text-align:center}}
        h1{{margin-top:0}}a{{color:#0f6cbd}}</style></head><body><div class="card">
        <h1>Access Approved</h1><p>{str(user.get("Email") or "")} can now sign in to SFDA Reconciliation.</p>
        <p><a href="/api/ui">Open the application</a></p></div></body></html>"""
        return func.HttpResponse(html, status_code=200, mimetype="text/html", charset="utf-8")
    except Exception as exc:
        html = f"""<!doctype html><html><body style="font-family:Arial;padding:40px">
        <h2>Approval failed</h2><p>{str(exc)}</p></body></html>"""
        return func.HttpResponse(html, status_code=400, mimetype="text/html", charset="utf-8")


@app.route(route="auth/login", methods=["POST"])
def auth_login(req: func.HttpRequest) -> func.HttpResponse:
    try:
        from engine.auth import login_user
        payload = req.get_json() or {}
        user, token, expires_at = login_user(
            str(payload.get("email") or payload.get("username") or ""),
            str(payload.get("password") or ""),
        )
        from engine.warehouse_context import set_current_warehouse
        set_current_warehouse(int(user.get("WarehouseID") or 1), str(user.get("WarehouseName") or "Madinah Warehouse"))
        max_age = max(60, int((expires_at - datetime.now(timezone.utc)).total_seconds()))
        return _json_response_with_cookie({
            "status": "Completed",
            "message": "Signed in successfully.",
            "user": user,
            "expires_at": expires_at,
        }, token, max_age)
    except ValueError as exc:
        return error_response("Sign in failed.", 401, str(exc))
    except Exception as exc:
        logger.exception("Login failed")
        return error_response("Sign in failed.", 500, str(exc))


@app.route(route="auth/logout", methods=["POST"])
def auth_logout(req: func.HttpRequest) -> func.HttpResponse:
    try:
        from engine.auth import logout_token
        logout_token(_cookie_value(req, "sfda_session"))
    except Exception:
        logger.exception("Logout cleanup failed")
    return _json_response_with_cookie(
        {"status": "Completed", "message": "Signed out."},
        "",
        0,
    )


@app.route(route="auth/me", methods=["GET"])
def auth_me(req: func.HttpRequest) -> func.HttpResponse:
    user = _current_user(req)
    if not user:
        return error_response("Authentication required.", 401)
    from engine.warehouse_context import set_current_warehouse
    set_current_warehouse(int(user.get("WarehouseID") or 1), str(user.get("WarehouseName") or "Madinah Warehouse"))
    return json_response({"status": "Success", "user": user})


@app.route(route="user-management/users", methods=["GET"])
def admin_users_route(req: func.HttpRequest) -> func.HttpResponse:
    denied = _auth_guard(req, admin=True)
    if denied:
        return denied
    try:
        from engine.auth import admin_users, admin_warehouses, auth_settings
        return json_response({
            "status": "Success",
            "users": admin_users(),
            "warehouses": admin_warehouses(),
            "settings": auth_settings(),
        })
    except Exception as exc:
        return error_response("Unable to load users.", 500, str(exc))


@app.route(route="user-management/users/{user_id}/status", methods=["POST"])
def admin_user_status_route(req: func.HttpRequest) -> func.HttpResponse:
    denied = _auth_guard(req, admin=True)
    if denied:
        return denied
    try:
        from engine.auth import admin_set_user_status
        payload = req.get_json() or {}
        result = admin_set_user_status(
            int(req.route_params.get("user_id")),
            str(payload.get("status") or ""),
        )
        return json_response({"status": "Completed", "user": result})
    except ValueError as exc:
        return error_response("Unable to update user.", 400, str(exc))
    except Exception as exc:
        return error_response("Unable to update user.", 500, str(exc))


@app.route(route="warehouse-gln/status", methods=["GET"])
def warehouse_gln_status_route(req: func.HttpRequest) -> func.HttpResponse:
    denied = _auth_guard(req)
    if denied:
        return denied

    try:
        from engine.reference_data import get_current_warehouse_gln_status

        return json_response({
            "status": "Success",
            "application": APPLICATION_NAME,
            "version": APPLICATION_VERSION,
            "gln": get_current_warehouse_gln_status(),
        })
    except Exception as exc:
        logger.exception("Warehouse GLN status read failed")
        return error_response(
            "Unable to load warehouse GLN mapping status.",
            500,
            str(exc),
        )


@app.route(route="warehouse-gln/upload", methods=["POST"])
def warehouse_gln_upload_route(req: func.HttpRequest) -> func.HttpResponse:
    denied = _auth_guard(req)
    if denied:
        return denied

    try:
        uploaded_file = req.files.get("gln_file")
        if uploaded_file is None:
            return error_response(
                "GLN file is required.",
                400,
                "Upload an Excel file containing To Address and GLN columns.",
            )

        file_name = str(uploaded_file.filename or "warehouse_gln.xlsx").strip()
        if not file_name.lower().endswith((".xlsx", ".xls")):
            return error_response(
                "Unsupported GLN file type.",
                400,
                "Use .xlsx or .xls.",
            )

        frame = read_excel_upload(uploaded_file)
        user = _current_user(req) or {}

        from engine.reference_data import replace_current_warehouse_gln

        result = replace_current_warehouse_gln(
            frame,
            source_file_name=file_name,
            updated_by=str(user.get("Email") or ""),
        )
        _refresh_dashboard_summary_safe()

        return json_response({
            "status": "Completed",
            "application": APPLICATION_NAME,
            "version": APPLICATION_VERSION,
            "gln": result,
        })
    except (ValueError, KeyError) as exc:
        return error_response(
            "Unable to update warehouse GLN mapping.",
            400,
            str(exc),
        )
    except Exception as exc:
        logger.exception("Warehouse GLN upload failed")
        return error_response(
            "Unable to update warehouse GLN mapping.",
            500,
            str(exc),
        )


@app.route(route="dashboard/summary", methods=["GET"])
def dashboard_summary(req: func.HttpRequest) -> func.HttpResponse:
    """Fast Home payload; never builds Variance or Product Intelligence."""
    denied = _auth_guard(req)
    if denied:
        return denied
    try:
        from engine.database import get_cached_dashboard_summary
        cached = get_cached_dashboard_summary()
        return json_response({
            "status": "Success",
            "historical": cached.get("historical") or {},
            "customer": cached.get("customer") or {},
            "updated_at": cached.get("updated_at"),
        })
    except Exception as exc:
        logger.exception("Dashboard summary failed")
        return error_response("Dashboard summary failed.", 500, str(exc))


@app.route(route="warehouse-data/reset", methods=["POST"])
def warehouse_data_reset_route(req: func.HttpRequest) -> func.HttpResponse:
    """Reset only uploaded/operational data for the signed-in warehouse."""
    denied = _auth_guard(req)
    if denied:
        return denied

    try:
        payload = req.get_json() or {}
        if str(payload.get("confirm") or "").strip().upper() != "RESET":
            return error_response(
                "Reset confirmation is required.",
                400,
                "Send {confirm: 'RESET'} after the user confirms the destructive action.",
            )

        user = _current_user(req) or {}
        warehouse_id = int(user.get("WarehouseID") or 0)
        warehouse_name = str(user.get("WarehouseName") or f"Warehouse {warehouse_id}")
        if warehouse_id < 1:
            return error_response("A valid warehouse is required.", 400)

        from engine.database import reset_current_warehouse_data
        from engine.blob_storage import BlobStorage

        database_result = reset_current_warehouse_data()
        storage = BlobStorage()
        storage.initialize_containers()
        blob_result = storage.delete_current_warehouse_data()

        # Variance Management keeps a short-lived per-warehouse server cache.
        # Reset must invalidate it immediately so the page cannot show stale
        # discrepancies after the warehouse data has been deleted.
        with _VARIANCE_CACHE_LOCK:
            _VARIANCE_CACHE.pop(warehouse_id, None)

        return json_response({
            "status": "Completed",
            "message": "Warehouse operational data was reset successfully.",
            "warehouse_id": warehouse_id,
            "warehouse_name": warehouse_name,
            "database": database_result,
            "blob": blob_result,
            "preserved": [
                "Warehouse account and users",
                "Warehouse GLN mapping",
                "Pack Size reference",
                "Business rules and application logic",
            ],
        })
    except Exception as exc:
        logger.exception("Warehouse data reset failed")
        return error_response(
            "Unable to reset warehouse operational data.",
            500,
            str(exc),
        )


@app.route(route="history", methods=["GET"])
def history(req: func.HttpRequest) -> func.HttpResponse:
    denied = _auth_guard(req)
    if denied:
        return denied
    try:
        from engine.blob_storage import BlobStorage
        from engine.database import (
            list_historical_build_jobs,
            list_reconciliation_runs,
        )

        limit = int(req.params.get("limit", "500") or 500)
        rows = list_reconciliation_runs(limit)

        # Historical Data Builder jobs live in HistoricalBuildJobs, while Daily
        # and Full Accept/Dispatch runs live in ReconciliationRuns. History must
        # merge both sources instead of silently losing Full Reconciliation
        # activity.
        for job in list_historical_build_jobs(limit):
            rows.append(historical_job_to_history_row(job))

        # SQL is now the authoritative normal History source. Blob recovery is
        # deliberately opt-in because scanning three containers on every refresh
        # becomes progressively slower as the platform grows.
        recover_blob = str(req.params.get("recover_blob", "")).lower() in {"1", "true", "yes"}
        blob_files_cache: Dict[str, List[Dict[str, Any]]] = {}
        storage = None

        if recover_blob:
            known = {
                str(row.get("RunNumber", "")).strip()
                for row in rows
                if str(row.get("RunNumber", "")).strip()
            }
            storage = BlobStorage()
            storage.initialize_containers()
            for run_number in storage.list_run_numbers(
                limit, include_fallback_containers=True
            ):
                if run_number in known:
                    continue
                files = storage.list_all_run_files(run_number)
                blob_files_cache[run_number] = files
                rows.append(
                    blob_run_to_history_row(
                        run_number,
                        load_blob_run_metadata(run_number),
                        files,
                    )
                )

        enriched_rows = []
        for row in rows:
            run_number = str(row.get("RunNumber", "")).strip()
            files_for_date: List[Dict[str, Any]] = []
            if recover_blob and run_number and not row.get("StartedAt"):
                if storage is None:
                    storage = BlobStorage()
                files_for_date = (
                    blob_files_cache.get(run_number)
                    or storage.list_all_run_files(run_number)
                )
                blob_files_cache[run_number] = files_for_date
            enriched_rows.append(_history_datetime_fields(row, files_for_date))
        rows = enriched_rows

        rows.sort(
            key=lambda row: str(
                row.get("StartedAtUtc")
                or row.get("StartedAt")
                or row.get("CompletedAtUtc")
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
    denied = _auth_guard(req)
    if denied:
        return denied
    run_number = str(req.route_params.get("run_number", "")).strip()
    if not run_number:
        return error_response("Run number is required.", 400)

    try:
        from engine.database import (
            get_reconciliation_run,
            list_reconciliation_run_files,
        )

        run = get_reconciliation_run(run_number)

        # ReconciliationRunFiles can be incomplete for older / transitional
        # Daily runs. In particular, some runs have SQL records for generated
        # output files while the original ASN / Full Dispatch / SFDA inputs are
        # present only in Blob Storage. The previous implementation used Blob
        # Storage only when SQL returned zero files, so those input files were
        # hidden as soon as even one output record existed in SQL.
        #
        # History is an audit page, therefore build the file list from BOTH
        # sources and de-duplicate by category + file name.
        sql_files = list_reconciliation_run_files(run_number)
        blob_files = blob_files_for_ui(run_number)

        merged_files: Dict[tuple, Dict[str, Any]] = {}

        for file in blob_files:
            key = (
                str(file.get("FileCategory", "")).strip().lower(),
                str(file.get("FileName", "")).strip().lower(),
            )
            merged_files[key] = file

        input_retention_hours = max(1, int(os.getenv("INPUT_RETENTION_HOURS", "24") or 24))
        input_cutoff = datetime.now(timezone.utc) - timedelta(hours=input_retention_hours)

        for file in sql_files:
            category = str(file.get("FileCategory", "")).strip().lower()
            created_at = file.get("CreatedAt")
            expired = False
            if category == "input" and created_at:
                try:
                    parsed = pd.to_datetime(created_at, utc=True, errors="coerce")
                    expired = bool(not pd.isna(parsed) and parsed.to_pydatetime() < input_cutoff)
                except Exception:
                    expired = False
            file["RetentionExpired"] = expired
            file["download_url"] = "" if expired else build_download_url(
                run_number,
                category,
                str(file.get("FileName", "")),
            )
            key = (
                category,
                str(file.get("FileName", "")).strip().lower(),
            )
            # Prefer the SQL row when both sources describe the same file.
            merged_files[key] = file

        files = sorted(
            merged_files.values(),
            key=lambda file: (
                str(file.get("FileCategory", "")),
                str(file.get("FileName", "")),
            ),
        )

        if not run:
            try:
                from engine.database import get_historical_build_job

                historical_job = get_historical_build_job(run_number)
                run = historical_job_to_history_row(historical_job)

                # Historical build inputs are recorded in the SQL job manifest.
                # Keep those file names visible after the 24h Blob retention
                # window expires; only the original bytes/download disappear.
                manifest = historical_job.get("input_manifest") or {}
                job_created = historical_job.get("created_at") or historical_job.get("started_at")
                manifest_expired = False
                if job_created:
                    try:
                        parsed = pd.to_datetime(job_created, utc=True, errors="coerce")
                        manifest_expired = bool(not pd.isna(parsed) and parsed.to_pydatetime() < input_cutoff)
                    except Exception:
                        manifest_expired = False

                for category_key in ("asn_files", "dispatch_files", "sfda_files"):
                    for item in manifest.get(category_key) or []:
                        file_name = str(item.get("file_name") or "").strip()
                        if not file_name:
                            continue
                        key = ("input", file_name.lower())
                        if key in merged_files:
                            continue
                        merged_files[key] = {
                            "RunFileID": None,
                            "RunNumber": run_number,
                            "FileCategory": "input",
                            "FileName": file_name,
                            "FileType": file_type_from_name(file_name),
                            "ContainerName": "runs-inputs",
                            "BlobName": item.get("blob_name") or "",
                            "ContentType": item.get("content_type") or "application/octet-stream",
                            "SizeBytes": int(item.get("size_bytes") or 0),
                            "ETag": "",
                            "CreatedAt": job_created,
                            "RetentionExpired": manifest_expired,
                            "download_url": "" if manifest_expired else build_download_url(
                                run_number, "input", file_name
                            ),
                        }

                files = sorted(
                    merged_files.values(),
                    key=lambda file: (
                        str(file.get("FileCategory", "")),
                        str(file.get("FileName", "")),
                    ),
                )
            except KeyError:
                run = None

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

        run = _history_datetime_fields(run, files)

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
    denied = _auth_guard(req)
    if denied:
        return denied
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
        storage = BlobStorage()
        safe_name = BlobStorage.sanitize_file_name(
            str(file_name).split("/")[-1]
        )

        # Historical Data Builder inputs are stored under
        # <run>/asn/<file>, <run>/dispatch/<file>, etc. Resolve the real BlobName
        # from storage instead of assuming every file is directly under <run>/.
        actual_blob_name = ""
        for item in storage.list_all_run_files(run_number):
            if (
                str(item.get("category", "")).lower() == category.lower()
                and str(item.get("file_name", "")) == file_name
            ):
                actual_blob_name = str(item.get("blob_name", ""))
                break

        blob_name = actual_blob_name or f"{run_number}/{BlobStorage.sanitize_file_name(file_name)}"
        downloaded = storage.download_blob(container_name, blob_name)

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
    denied = _auth_guard(req)
    if denied:
        return denied
    try:
        # Page load must be a single cached-row read. Heavy historical
        # aggregation is refreshed only after data-changing jobs complete.
        from engine.database import get_cached_dashboard_summary

        cached = get_cached_dashboard_summary()
        status = cached.get("historical") or {}
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


@app.route(route="historical/export-current", methods=["GET"])
def historical_export_current(req: func.HttpRequest) -> func.HttpResponse:
    denied = _auth_guard(req)
    if denied:
        return denied
    """Export the current persisted historical tables on demand."""
    try:
        from engine.database import (
            get_batch_master_df,
            get_customer_history_df,
            get_supplier_history_df,
            get_sto_incoming_history_df,
            get_sto_return_history_df,
        )
        from engine.exporter import Exporter

        batch_master = get_batch_master_df()
        supplier_history = get_supplier_history_df()
        customer_history = get_customer_history_df()
        sto_incoming_history = get_sto_incoming_history_df()
        sto_return_history = get_sto_return_history_df()
        if batch_master.empty:
            return error_response(
                "Historical Batch Master is empty. Complete Step 1 first.", 400
            )

        outputs = {}
        outputs.update(Exporter.build_formatted_excel_file(
            df=batch_master, file_name="Batch_Master.xlsx",
            sheet_name="Batch Master", title="SFDA Historical Batch Master",
            sort_columns=["Generic Item Number", "BN", "Expiry Date"],
        ))
        outputs.update(Exporter.build_formatted_excel_file(
            df=supplier_history, file_name="Supplier_History.xlsx",
            sheet_name="Supplier History", title="Historical Supplier Receipt History",
            sort_columns=["Supplier Name", "Generic Item Number", "BN", "Expiry Date"],
        ))
        outputs.update(Exporter.build_formatted_excel_file(
            df=customer_history, file_name="Customer_History.xlsx",
            sheet_name="Customer History", title="Historical Customer Dispatch History",
            sort_columns=["To Address", "Generic Item Number", "BN", "Expiry Date"],
        ))
        outputs.update(Exporter.build_formatted_excel_file(
            df=sto_incoming_history, file_name="STO_Incoming_History.xlsx",
            sheet_name="STO Incoming", title="Historical STO Incoming Receipt History",
            sort_columns=["Source Warehouse", "Generic Item Number", "BN", "Expiry Date"],
        ))
        outputs.update(Exporter.build_formatted_excel_file(
            df=sto_return_history, file_name="STO_Return_Cancel_Dispatch.xlsx",
            sheet_name="STO Return", title="STO Returns - Cancel Previous RSD Dispatch",
            sort_columns=["Source Warehouse", "Generic Item Number", "BN", "Expiry Date"],
        ))
        return json_response({"status": "Completed", "outputs": outputs})
    except Exception as exc:
        logger.exception("Current historical export failed")
        return error_response("Failed to export current historical files.", 500, str(exc))


@app.route(route="batch-master/build", methods=["GET", "POST"])
def batch_master_build(req: func.HttpRequest) -> func.HttpResponse:
    denied = _auth_guard(req)
    if denied:
        return denied
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
        submitted_by = get_submitted_by(req)
        storage = BlobStorage()
        storage.initialize_containers()

        manifest: Dict[str, Any] = {
            "asn_files": [],
            "dispatch_files": [],
            "sfda_files": [],
            "submitted_by": submitted_by,
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
            message_encode_policy=TextBase64EncodePolicy(),
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
                    "warehouse_id": int(
                        __import__(
                            "engine.warehouse_context",
                            fromlist=["current_warehouse_id"],
                        ).current_warehouse_id()
                    ),
                    "warehouse_name": str(
                        __import__(
                            "engine.warehouse_context",
                            fromlist=["current_warehouse_name"],
                        ).current_warehouse_name()
                    ),
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
    denied = _auth_guard(req)
    if denied:
        return denied
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
    warehouse_id_raw = payload.get("warehouse_id")
    warehouse_name = str(
        payload.get("warehouse_name", "")
    ).strip()

    if not job_id:
        raise ValueError(
            "Historical build queue message is missing job_id."
        )

    if warehouse_id_raw in (None, ""):
        raise ValueError(
            "Historical build queue message is missing warehouse_id. "
            "The job was rejected to prevent cross-warehouse processing."
        )

    try:
        warehouse_id = int(warehouse_id_raw)
    except (TypeError, ValueError) as exc:
        raise ValueError(
            "Historical build queue message has an invalid warehouse_id."
        ) from exc

    if warehouse_id < 1:
        raise ValueError(
            "Historical build queue message has an invalid warehouse_id."
        )

    from engine.historical_jobs import process_historical_build_job
    from engine.warehouse_context import warehouse_scope

    with warehouse_scope(
        warehouse_id,
        warehouse_name or f"Warehouse {warehouse_id}",
    ):
        logger.info(
            "Historical build worker scoped to WarehouseID=%s (%s), job_id=%s",
            warehouse_id,
            warehouse_name or f"Warehouse {warehouse_id}",
            job_id,
        )
        process_historical_build_job(
            job_id,
            input_manifest,
            operation,
        )
        _refresh_dashboard_summary_safe()




def build_dashboard_preview(
    report: pd.DataFrame,
    mode: str,
    limit: int = 100,
) -> List[Dict[str, Any]]:
    """Return only currently actionable rows for the Home overview.

    The browser previously kept an old localStorage preview forever, so a later
    successful Dispatch could still show obsolete pending rows. Every completed
    run now replaces that preview with rows from the current result only.
    """

    if report is None or report.empty:
        return []

    frame = report.copy()
    if str(mode).lower() in {"dispatch", "full_dispatch"}:
        action_column = (
            "Allocated To Be Dispatch"
            if "Allocated To Be Dispatch" in frame.columns
            else "To Be Dispatch"
        )
    else:
        action_column = "To Be Accept"

    if action_column in frame.columns:
        action = pd.to_numeric(
            frame[action_column],
            errors="coerce",
        ).fillna(0)
        frame = frame.loc[action.gt(0)].copy()

    if frame.empty:
        return []

    rows: List[Dict[str, Any]] = []
    for record in frame.head(max(1, int(limit))).to_dict(orient="records"):
        to_be_dispatch = record.get(
            "Allocated To Be Dispatch",
            record.get("To Be Dispatch", 0),
        )
        status = (
            "Dispatch Pending"
            if pd.to_numeric(
                pd.Series([to_be_dispatch]),
                errors="coerce",
            ).fillna(0).iloc[0] > 0
            else (
                "Accept Pending"
                if pd.to_numeric(
                    pd.Series([record.get("To Be Accept", 0)]),
                    errors="coerce",
                ).fillna(0).iloc[0] > 0
                else "OK"
            )
        )
        rows.append(
            {
                "BN": record.get("BN"),
                "Expiry Date": record.get("Expiry Date"),
                "GTIN": record.get("GTIN"),
                "Drug Name": record.get("Drug Name"),
                "Active": record.get(
                    "Active",
                    record.get("SFDA Active"),
                ),
                "Quantity Sent Pending": record.get(
                    "Quantity sent pending",
                    record.get("Quantity Sent Pending"),
                ),
                "Quantity Receive Pending": record.get(
                    "Quantity Receive Pending"
                ),
                "Receiving": record.get(
                    "Received Quantity Pack",
                    record.get("Historical Received Quantity Pack"),
                ),
                "To Be Accept": record.get("To Be Accept", 0),
                "Inventory": record.get(
                    "Current Inventory Quantity Pack",
                    record.get("Inventory"),
                ),
                "Variance": record.get(
                    "Quantity Difference",
                    record.get("Supplier Variance"),
                ),
                "To Be Dispatch": to_be_dispatch,
                "Status": status,
            }
        )
    return rows


def _run_full_reconciliation_accept(req: func.HttpRequest) -> func.HttpResponse:
    run_number = build_run_number("FULL-ACCEPT")
    submitted_by = get_submitted_by(req)
    run_created = False
    try:
        sfda_file = req.files.get("sfda")
        if sfda_file is None:
            return error_response("Latest SFDA file is required for Full Accept Reconciliation.")

        sfda_name, sfda_bytes, sfda_content_type = read_uploaded_bytes(sfda_file)
        sfda_df = read_excel_bytes(sfda_name, sfda_bytes)
        from engine.database import (
            complete_reconciliation_run,
            create_reconciliation_run,
            get_batch_master_df,
            get_supplier_history_df,
            get_sto_incoming_history_df,
            get_sto_return_history_df,
            replace_latest_sfda_snapshot,
            sync_batch_master_sfda_snapshot,
        )
        from engine.exporter import Exporter
        from engine.full_reconciliation import FullReconciliationEngine

        create_reconciliation_run(
            run_number=run_number,
            process_type="FULL_ACCEPT",
            submitted_by=submitted_by,
            application_version=APPLICATION_VERSION,
            asn_files=0,
            inventory_files=0,
            dispatch_files=0,
            sfda_files=1,
        )
        run_created = True

        batch_master = get_batch_master_df()
        supplier_history = get_supplier_history_df()
        sto_incoming_history = get_sto_incoming_history_df()
        sto_return_history = get_sto_return_history_df()
        replace_latest_sfda_snapshot(sfda_df, sfda_name)
        if batch_master.empty:
            raise ValueError("Historical Batch Master is empty. Complete Step 1 first.")

        # Keep persisted Batch Master aligned with the same latest SFDA snapshot
        # used by Product Intelligence and Full Accept.
        batch_master_sync = sync_batch_master_sfda_snapshot(sfda_df)
        batch_master = get_batch_master_df()

        result = FullReconciliationEngine(
            pd.DataFrame(), pd.DataFrame(), sfda_df
        ).run_accept_reconciliation(
            sfda_df,
            batch_master,
            supplier_history,
            sto_incoming_history,
            sto_return_history,
        )
        accept_details = result["accept_details"]
        supplier_variance = result["supplier_variance"]
        sto_incoming = result["sto_incoming"]
        sto_return_cancel = result["sto_return_cancel_dispatch"]
        accept_upload = result["accept_upload"]
        outputs = {
            "batch_master": Exporter.build_formatted_excel_file(
                df=batch_master,
                file_name="Batch_Master.xlsx",
                sheet_name="Batch Master",
                title="SFDA Historical Batch Master",
                columns=[
                    column
                    for column in Exporter.BATCH_MASTER_COLUMNS
                    if column in batch_master.columns
                ],
                sort_columns=["Generic Item Number", "BN", "Expiry Date"],
            ),
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
                title="Supplier Quantity Variance - TRK5060 Only",
                columns=list(supplier_variance.columns),
                sort_columns=["Supplier Name", "Generic Item Number", "BN"],
            ),
            "sto_incoming": Exporter.build_formatted_excel_file(
                df=sto_incoming,
                file_name="STO_Incoming_RSD_Follow_Up.xlsx",
                sheet_name="STO Incoming",
                title="STO Incoming - RSD Receive Pending Follow-up",
                columns=list(sto_incoming.columns),
                sort_columns=["Source Warehouse", "Generic Item Number", "BN", "Expiry Date"],
            ),
            "sto_return_cancel_dispatch": Exporter.build_formatted_excel_file(
                df=sto_return_cancel,
                file_name="STO_Return_Cancel_Dispatch.xlsx",
                sheet_name="STO Return",
                title="STO Return - Cancel Previous RSD Dispatch",
                columns=list(sto_return_cancel.columns),
                sort_columns=["Source Warehouse", "Generic Item Number", "BN", "Expiry Date"],
            ),
            "accept_files": Exporter.build_sfda_upload_files(
                accept_upload, "To Be Accept", "SFDA_Full_Accept"
            ),
        }
        summary = {
            "sfda_rows": int(len(sfda_df)),
            "batch_master_rows": int(len(batch_master)),
            "batch_master_sfda_rows_updated": int(
                batch_master_sync.get("updated_rows", 0)
            ),
            "accept_rows": int(len(accept_upload)),
            "supplier_variance_rows": int(len(supplier_variance)),
            "sto_incoming_rows": int(len(sto_incoming)),
            "sto_incoming_followup_rows": int(
                pd.to_numeric(sto_incoming.get("STO Pending RSD Qty", 0), errors="coerce")
                .fillna(0).gt(0).sum()
            ) if not sto_incoming.empty else 0,
            "sto_return_rows": int(len(sto_return_cancel)),
            "accept_files": len(outputs["accept_files"]),
        }

        archived_files = save_run_archive(
            run_number=run_number,
            mode="FULL_ACCEPT",
            input_files=[
                {
                    "file_name": sfda_name,
                    "data": sfda_bytes,
                    "content_type": sfda_content_type,
                }
            ],
            outputs=outputs,
            summary=summary,
            submitted_by=submitted_by,
        )

        complete_reconciliation_run(
            run_number=run_number,
            status="Completed",
            total_input_rows=int(len(sfda_df)),
            master_records=int(len(batch_master)),
            accept_records=int(len(accept_upload)),
            dispatch_records=0,
            exception_records=int(len(supplier_variance)),
            generated_files=archived_files,
        )
        summary["archived_files"] = archived_files

        return json_response({
            "status": "Completed",
            "step": "full-accept",
            "run_number": run_number,
            "outputs": outputs,
            "summary": summary,
            "dashboard_preview": build_dashboard_preview(
                accept_details,
                "accept",
            ),
        })
    except ValueError as exc:
        logger.exception("Full Accept validation failed")
        if run_created:
            try:
                from engine.database import complete_reconciliation_run
                complete_reconciliation_run(
                    run_number=run_number,
                    status="Failed",
                    error_message=str(exc),
                )
            except Exception:
                logger.exception("Unable to mark Full Accept run as failed")
        return error_response("Full Accept Reconciliation validation failed.", 400, str(exc))
    except Exception as exc:
        logger.exception("Full Accept reconciliation failed")
        if run_created:
            try:
                from engine.database import complete_reconciliation_run
                complete_reconciliation_run(
                    run_number=run_number,
                    status="Failed",
                    error_message=str(exc),
                )
            except Exception:
                logger.exception("Unable to mark Full Accept run as failed")
        return error_response("Full Accept Reconciliation failed.", 500, str(exc))



@app.route(route="full-reconciliation/accept", methods=["GET", "POST"])
def full_reconciliation_accept(req: func.HttpRequest) -> func.HttpResponse:
    denied = _auth_guard(req)
    if denied:
        return denied
    if req.method == "GET":
        return json_response({"status": "Ready", "required_files": ["sfda"]})
    return _safe_queue_reconciliation_job(req, "full_accept", ["sfda"])



def _bootstrap_full_dispatch_ledger_from_history(limit: int = 250) -> Dict[str, int]:
    """Reserve allocations from successful legacy Full Dispatch runs once.

    Version 27 introduces a dedicated Full Dispatch consumption ledger. Older
    successful Full Dispatch runs were archived before that ledger existed.
    On the first run after upgrade, recover those allocations from their
    archived Full_Dispatch_Reconciliation.xlsx files and reserve them so the
    same historical WMS dispatch evidence is not proposed again.

    Repeated legacy runs for the same customer/batch do not multiply the
    reservation because the ledger MERGE uses one deterministic transaction key
    and preserves the maximum submitted amount.
    """
    from engine.blob_storage import BlobStorage
    from engine.database import (
        get_full_dispatch_transaction_count,
        list_reconciliation_runs,
        save_full_dispatch_pending_transactions,
    )

    existing = get_full_dispatch_transaction_count()
    if existing > 0:
        return {
            "bootstrapped_runs": 0,
            "bootstrapped_transactions": 0,
        }

    storage = BlobStorage()
    storage.initialize_containers()

    bootstrapped_runs = 0
    bootstrapped_transactions = 0

    for run in list_reconciliation_runs(limit):
        process_type = str(run.get("ProcessType") or "").strip().upper()
        status = str(run.get("Status") or "").strip().upper()
        run_number = str(run.get("RunNumber") or "").strip()

        if process_type != "FULL_DISPATCH" or status != "COMPLETED" or not run_number:
            continue

        files = storage.list_all_run_files(run_number)
        candidate = next(
            (
                file
                for file in files
                if str(file.get("category") or "").lower() == "output"
                and str(file.get("file_name") or "").lower()
                == "full_dispatch_reconciliation.xlsx"
            ),
            None,
        )
        if candidate is None:
            continue

        downloaded = storage.download_blob(
            str(candidate.get("container") or ""),
            str(candidate.get("blob_name") or ""),
        )

        try:
            detail_frame = pd.read_excel(
                io.BytesIO(downloaded["data"]),
                engine="openpyxl",
                header=1,
                dtype=object,
            )
        except Exception:
            logger.exception(
                "Unable to recover legacy Full Dispatch detail file for %s",
                run_number,
            )
            continue

        if detail_frame.empty or "To Be Dispatch" not in detail_frame.columns:
            continue

        saved = save_full_dispatch_pending_transactions(
            detail_frame.to_dict(orient="records"),
            run_number,
        )
        bootstrapped_runs += 1
        bootstrapped_transactions += int(saved)

    return {
        "bootstrapped_runs": bootstrapped_runs,
        "bootstrapped_transactions": bootstrapped_transactions,
    }


def _run_full_reconciliation_dispatch(req: func.HttpRequest) -> func.HttpResponse:
    run_number = build_run_number("FULL-DISPATCH")
    submitted_by = get_submitted_by(req)
    run_created = False
    try:
        inventory_file = req.files.get("inventory")
        sfda_file = req.files.get("sfda")
        if inventory_file is None:
            return error_response("Current Inventory file is required for Full Dispatch Reconciliation.")
        if sfda_file is None:
            return error_response("Updated SFDA file is required for Full Dispatch Reconciliation.")

        inventory_name, inventory_bytes, inventory_content_type = read_uploaded_bytes(inventory_file)
        sfda_name, sfda_bytes, sfda_content_type = read_uploaded_bytes(sfda_file)
        inventory_df = read_excel_bytes(inventory_name, inventory_bytes)
        sfda_df = read_excel_bytes(sfda_name, sfda_bytes)
        from engine.database import (
            complete_reconciliation_run,
            confirm_full_dispatch_transactions_from_sfda,
            create_reconciliation_run,
            get_batch_master_df,
            get_customer_history_df,
            get_full_dispatch_confirmed_allocations,
            replace_full_dispatch_sfda_baseline,
            replace_latest_inventory_snapshot,
            replace_latest_sfda_snapshot,
            save_full_dispatch_pending_transactions,
            sync_batch_master_sfda_snapshot,
        )
        from engine.exporter import Exporter
        from engine.full_reconciliation import FullReconciliationEngine

        create_reconciliation_run(
            run_number=run_number,
            process_type="FULL_DISPATCH",
            submitted_by=submitted_by,
            application_version=APPLICATION_VERSION,
            asn_files=0,
            inventory_files=1,
            dispatch_files=0,
            sfda_files=1,
        )
        run_created = True

        customer_history = get_customer_history_df()
        if customer_history.empty:
            raise ValueError("Customer History is empty. Complete Step 1 first.")

        # First upgrade run only: reserve allocations from successful legacy
        # Full Dispatch runs that pre-date the dedicated consumption ledger.
        ledger_bootstrap = _bootstrap_full_dispatch_ledger_from_history()

        # The latest SFDA report confirms prior Full Dispatch reservations only
        # when it proves the movement. Confirmation is independent from WMS
        # DispatchEvents; Full Reconciliation must never duplicate historical
        # dispatch totals already present in Batch Master.
        full_dispatch_confirmation = (
            confirm_full_dispatch_transactions_from_sfda(
                sfda_df,
                sfda_name,
            )
        )
        reserved_full_dispatch = get_full_dispatch_confirmed_allocations()

        replace_latest_inventory_snapshot(inventory_df, inventory_name)
        replace_latest_sfda_snapshot(sfda_df, sfda_name)
        batch_master_sync = sync_batch_master_sfda_snapshot(sfda_df)
        batch_master = get_batch_master_df()

        result = FullReconciliationEngine(
            pd.DataFrame(), pd.DataFrame(), sfda_df
        ).run_dispatch_reconciliation(
            inventory_df,
            sfda_df,
            customer_history,
            reserved_full_dispatch,
        )
        dispatch_details = result["dispatch_details"]
        summary_df = result["summary"]
        dispatch_upload = result["dispatch_upload"]

        full_dispatch_pending_saved = 0
        if not dispatch_upload.empty:
            full_dispatch_pending_saved = save_full_dispatch_pending_transactions(
                dispatch_upload.to_dict(orient="records"),
                run_number,
            )

        # Current SFDA state becomes the proof baseline for the next Full
        # Dispatch run. It does not change historical WMS dispatch totals.
        replace_full_dispatch_sfda_baseline(sfda_df, sfda_name)

        outputs = {
            "batch_master": Exporter.build_formatted_excel_file(
                df=batch_master,
                file_name="Batch_Master.xlsx",
                sheet_name="Batch Master",
                title="SFDA Historical Batch Master",
                columns=[
                    column
                    for column in Exporter.BATCH_MASTER_COLUMNS
                    if column in batch_master.columns
                ],
                sort_columns=["Generic Item Number", "BN", "Expiry Date"],
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
            "dispatch_files": Exporter.build_dispatch_files_by_customer(dispatch_upload),
        }
        summary = {
            "inventory_rows": int(len(inventory_df)),
            "sfda_rows": int(len(sfda_df)),
            "batch_master_rows": int(len(batch_master)),
            "batch_master_sfda_rows_updated": int(
                batch_master_sync.get("updated_rows", 0)
            ),
            "dispatch_allocation_rows": int(len(dispatch_upload)),
            "dispatch_files": len(outputs["dispatch_files"]),
            "full_dispatch_pending_saved": int(full_dispatch_pending_saved),
            "full_dispatch_confirmation": full_dispatch_confirmation,
            "full_dispatch_ledger_bootstrap": ledger_bootstrap,
        }

        archived_files = save_run_archive(
            run_number=run_number,
            mode="FULL_DISPATCH",
            input_files=[
                {
                    "file_name": inventory_name,
                    "data": inventory_bytes,
                    "content_type": inventory_content_type,
                },
                {
                    "file_name": sfda_name,
                    "data": sfda_bytes,
                    "content_type": sfda_content_type,
                },
            ],
            outputs=outputs,
            summary=summary,
            submitted_by=submitted_by,
        )

        complete_reconciliation_run(
            run_number=run_number,
            status="Completed",
            total_input_rows=int(len(inventory_df) + len(sfda_df)),
            master_records=int(len(batch_master)),
            accept_records=0,
            dispatch_records=int(len(dispatch_upload)),
            exception_records=0,
            generated_files=archived_files,
        )
        summary["archived_files"] = archived_files

        return json_response({
            "status": "Completed",
            "step": "full-dispatch",
            "run_number": run_number,
            "outputs": outputs,
            "summary": summary,
            "dashboard_preview": build_dashboard_preview(
                dispatch_details,
                "dispatch",
            ),
        })
    except ValueError as exc:
        logger.exception("Full Dispatch validation failed")
        if run_created:
            try:
                from engine.database import complete_reconciliation_run
                complete_reconciliation_run(
                    run_number=run_number,
                    status="Failed",
                    error_message=str(exc),
                )
            except Exception:
                logger.exception("Unable to mark Full Dispatch run as failed")
        return error_response("Full Dispatch Reconciliation validation failed.", 400, str(exc))
    except Exception as exc:
        logger.exception("Full Dispatch reconciliation failed")
        if run_created:
            try:
                from engine.database import complete_reconciliation_run
                complete_reconciliation_run(
                    run_number=run_number,
                    status="Failed",
                    error_message=str(exc),
                )
            except Exception:
                logger.exception("Unable to mark Full Dispatch run as failed")
        return error_response("Full Dispatch Reconciliation failed.", 500, str(exc))


@app.route(route="full-reconciliation/dispatch", methods=["GET", "POST"])
def full_reconciliation_dispatch(req: func.HttpRequest) -> func.HttpResponse:
    denied = _auth_guard(req)
    if denied:
        return denied
    if req.method == "GET":
        return json_response({"status": "Ready", "required_files": ["inventory", "sfda"]})
    return _safe_queue_reconciliation_job(req, "full_dispatch", ["inventory", "sfda"])


def _variance_management_result(force_refresh: bool = False) -> Dict[str, Any]:
    """Build and cache the full variance dataset.

    The page endpoint returns only one server-side page at a time, while report
    generation and email reuse the cached full dataset by Variance ID.
    """
    from engine.warehouse_context import current_warehouse_id
    warehouse_id = int(current_warehouse_id())
    now = time.time()
    warehouse_cache = _VARIANCE_CACHE.get(warehouse_id, {})
    cached = warehouse_cache.get("result")
    loaded_at = float(warehouse_cache.get("loaded_at") or 0.0)

    if (
        not force_refresh
        and cached is not None
        and (now - loaded_at) < _VARIANCE_CACHE_TTL_SECONDS
    ):
        return cached

    with _VARIANCE_CACHE_LOCK:
        now = time.time()
        warehouse_cache = _VARIANCE_CACHE.get(warehouse_id, {})
        cached = warehouse_cache.get("result")
        loaded_at = float(warehouse_cache.get("loaded_at") or 0.0)
        if (
            not force_refresh
            and cached is not None
            and (now - loaded_at) < _VARIANCE_CACHE_TTL_SECONDS
        ):
            return cached

        from engine.database import (
            get_customer_history_df,
            get_latest_inventory_snapshot_df,
            get_latest_sfda_snapshot_df,
            get_supplier_history_df,
        )
        from engine.variance_management import VarianceManagementEngine

        result = VarianceManagementEngine().build(
            supplier_history=get_supplier_history_df(),
            customer_history=get_customer_history_df(),
            sfda_snapshot=get_latest_sfda_snapshot_df(),
            inventory_snapshot=get_latest_inventory_snapshot_df(),
        )
        _VARIANCE_CACHE[warehouse_id] = {"result": result, "loaded_at": now}
        return result


def _variance_page(
    result: Dict[str, Any],
    *,
    page: int,
    page_size: int,
    search: str = "",
    variance_type: str = "",
    severity: str = "",
) -> Dict[str, Any]:
    items = list(result.get("items") or [])
    search = str(search or "").strip().lower()
    variance_type = str(variance_type or "").strip()
    severity = str(severity or "").strip()

    def matches(item: Dict[str, Any]) -> bool:
        if variance_type and str(item.get("Variance Type") or "") != variance_type:
            return False
        if severity and str(item.get("Severity") or "") != severity:
            return False
        if search:
            haystack = " ".join(
                str(item.get(key) or "")
                for key in (
                    "BN", "GTIN", "Supplier Name", "Supplier Code",
                    "Customer", "GLN", "Description", "Drug Name",
                    "Generic Item Number",
                )
            ).lower()
            if search not in haystack:
                return False
        return True

    filtered = [item for item in items if matches(item)]
    total = len(filtered)
    page_size = max(25, min(int(page_size or 100), 200))
    total_pages = max(1, (total + page_size - 1) // page_size)
    page = max(1, min(int(page or 1), total_pages))
    start = (page - 1) * page_size
    page_items = filtered[start:start + page_size]

    return {
        "items": page_items,
        "pagination": {
            "page": page,
            "page_size": page_size,
            "total_items": total,
            "total_pages": total_pages,
            "start_item": (start + 1) if total else 0,
            "end_item": min(start + page_size, total),
        },
    }


def _build_variance_report(selected_ids: Optional[List[str]] = None) -> Dict[str, Any]:
    from engine.exporter import Exporter
    from engine.variance_management import VarianceManagementEngine

    result = _variance_management_result()
    report = VarianceManagementEngine().report_frame(result, selected_ids)
    if report.empty:
        raise ValueError("No SFDA-reportable variance items were selected.")

    saudi_now = datetime.now(timezone.utc).astimezone(
        timezone(timedelta(hours=3))
    )
    file_name = f"SFDA_Discrepancy_Report_{saudi_now.strftime('%Y%m%d_%H%M')}.xlsx"
    return Exporter.build_formatted_excel_file(
        df=report,
        file_name=file_name,
        sheet_name="SFDA Discrepancies",
        title="SFDA Receiving Discrepancy Report",
        columns=list(report.columns),
        sort_columns=["Severity", "Supplier Name", "BN", "Expiry Date"],
    )


@app.route(route="variance-management", methods=["GET"])
def variance_management(req: func.HttpRequest) -> func.HttpResponse:
    denied = _auth_guard(req)
    if denied:
        return denied
    try:
        force_refresh = str(req.params.get("refresh", "")).lower() in {"1", "true", "yes"}
        result = _variance_management_result(force_refresh=force_refresh)
        page_result = _variance_page(
            result,
            page=int(req.params.get("page", "1") or 1),
            page_size=int(req.params.get("page_size", "100") or 100),
            search=str(req.params.get("search", "") or ""),
            variance_type=str(req.params.get("type", "") or ""),
            severity=str(req.params.get("severity", "") or ""),
        )
        from engine.outlook_graph import graph_settings
        graph = graph_settings()
        response = {
            "status": "Completed",
            "summary": result.get("summary") or {},
            "metadata": {
                **(result.get("metadata") or {}),
                "cache_age_seconds": max(0, int(time.time() - float(_VARIANCE_CACHE.get(int(__import__("engine.warehouse_context", fromlist=["current_warehouse_id"]).current_warehouse_id()), {}).get("loaded_at") or time.time()))),
                "cache_ttl_seconds": _VARIANCE_CACHE_TTL_SECONDS,
            },
            **page_result,
            "email": {
                "configured": bool(graph.get("configured")),
                "mode": "outlook_draft",
                "default_recipient": os.getenv("SFDA_VARIANCE_EMAIL", "").strip(),
            },
        }
        return json_response(response)
    except Exception as exc:
        logger.exception("Variance Management failed")
        return error_response("Variance Management failed.", 500, str(exc))


@app.route(route="variance-management/report", methods=["POST"])
def variance_management_report(req: func.HttpRequest) -> func.HttpResponse:
    denied = _auth_guard(req)
    if denied:
        return denied
    try:
        payload = req.get_json() or {}
        selected_ids = payload.get("selected_ids") or []
        output = _build_variance_report(selected_ids)
        return json_response({
            "status": "Completed",
            "application": APPLICATION_NAME,
            "version": APPLICATION_VERSION,
            "output": output,
        })
    except ValueError as exc:
        return error_response("Unable to generate SFDA discrepancy report.", 400, str(exc))
    except Exception as exc:
        logger.exception("Variance report generation failed")
        return error_response("Unable to generate SFDA discrepancy report.", 500, str(exc))


@app.route(route="variance-management/outlook/start", methods=["POST"])
def variance_management_outlook_start(req: func.HttpRequest) -> func.HttpResponse:
    denied = _auth_guard(req)
    if denied:
        return denied
    try:
        from engine.database import create_outlook_draft_request
        from engine.email_service import validate_email
        from engine.outlook_graph import (
            authorization_url,
            graph_settings,
            new_oauth_state,
            state_hash,
        )

        if not graph_settings().get("configured"):
            raise RuntimeError(
                "Microsoft Graph is not configured in Azure Function App."
            )

        payload = req.get_json() or {}
        selected_ids = [
            str(value).strip()
            for value in (payload.get("selected_ids") or [])
            if str(value).strip()
        ]
        if not selected_ids:
            raise ValueError("Select at least one variance item.")

        recipient = validate_email(
            str(
                payload.get("recipient")
                or os.getenv("SFDA_VARIANCE_EMAIL", "")
            ).strip()
        )
        subject = str(
            payload.get("subject")
            or "SFDA Receiving Discrepancy Report"
        ).strip()
        body = str(
            payload.get("message")
            or (
                "Dear SFDA Team,\\n\\n"
                "Please find attached the latest receiving discrepancy report "
                "showing missing batch registrations and quantity differences "
                "between WMS receipts and the latest SFDA report.\\n\\n"
                "Regards,\\nDrug Traceability Reconciliation Platform"
            )
        )

        user = _current_user(req) or {}
        requested_by = str(user.get("Email") or "").strip().lower()
        if not requested_by:
            raise RuntimeError("Unable to identify the signed-in application user.")

        state = new_oauth_state()
        request_id = str(uuid.uuid4())
        expires_at = datetime.now(timezone.utc) + timedelta(minutes=15)

        create_outlook_draft_request(
            request_id=request_id,
            state_hash=state_hash(state),
            requested_by_email=requested_by,
            selected_ids_json=json.dumps(selected_ids, ensure_ascii=False),
            recipient_email=recipient,
            subject=subject,
            message_body=body,
            expires_at=expires_at,
        )

        return json_response({
            "status": "Ready",
            "mode": "outlook_draft",
            "authorization_url": authorization_url(state),
            "expires_at": expires_at,
        })
    except ValueError as exc:
        return error_response("Unable to prepare Outlook draft.", 400, str(exc))
    except Exception as exc:
        logger.exception("Outlook draft authorization start failed")
        return error_response("Unable to prepare Outlook draft.", 500, str(exc))


def _outlook_callback_error(message: str, status_code: int = 400) -> func.HttpResponse:
    safe = (
        str(message or "Unable to create Outlook draft.")
        .replace("&", "&amp;")
        .replace("<", "&lt;")
        .replace(">", "&gt;")
        .replace('"', "&quot;")
    )
    return func.HttpResponse(
        f"""<!doctype html>
<html>
<head><meta charset="utf-8"><title>Outlook Draft Error</title></head>
<body style="font-family:Segoe UI,Arial,sans-serif;padding:32px;background:#f5f7fb">
  <div style="max-width:720px;margin:auto;background:white;border:1px solid #dde4ee;border-radius:14px;padding:24px">
    <h2 style="margin-top:0;color:#b42318">Unable to prepare Outlook draft</h2>
    <p>{safe}</p>
    <p>You can close this tab and return to SFDA Reconciliation.</p>
  </div>
</body>
</html>""",
        status_code=status_code,
        mimetype="text/html",
    )


@app.route(route="outlook/callback", methods=["GET"])
def outlook_callback(req: func.HttpRequest) -> func.HttpResponse:
    request_record = None
    try:
        from engine.database import (
            complete_outlook_draft_request,
            fail_outlook_draft_request,
            get_outlook_draft_request_by_state_hash,
        )
        from engine.outlook_graph import (
            create_draft_with_attachment,
            exchange_authorization_code,
            graph_me,
            state_hash,
        )

        microsoft_error = str(req.params.get("error", "") or "").strip()
        microsoft_error_description = str(
            req.params.get("error_description", "") or ""
        ).strip()
        if microsoft_error:
            raise ValueError(
                microsoft_error_description
                or f"Microsoft sign-in failed: {microsoft_error}"
            )

        code = str(req.params.get("code", "") or "").strip()
        state = str(req.params.get("state", "") or "").strip()
        if not code or not state:
            raise ValueError(
                "Microsoft authorization response is missing code or state."
            )

        # Resolve the signed-in application user first. _current_user() also
        # establishes the warehouse context used by SQL Row-Level Security, so
        # the OAuth state can only be read from that user's warehouse.
        app_user = _current_user(req)
        if not app_user:
            raise ValueError(
                "Your SFDA Reconciliation session has expired. "
                "Sign in again and retry."
            )

        request_record = get_outlook_draft_request_by_state_hash(
            state_hash(state)
        )
        if not request_record:
            raise ValueError(
                "The Outlook draft request is invalid or has expired. "
                "Return to Variance Management and try again."
            )

        requested_by = str(
            request_record.get("RequestedByEmail") or ""
        ).strip().lower()
        current_app_email = str(
            app_user.get("Email") or ""
        ).strip().lower()
        if requested_by != current_app_email:
            raise ValueError(
                "This Outlook draft request belongs to a different "
                "SFDA Reconciliation user."
            )

        token = exchange_authorization_code(code)
        access_token = str(token.get("access_token") or "")
        microsoft_user = graph_me(access_token)
        microsoft_email = str(
            microsoft_user.get("mail")
            or microsoft_user.get("userPrincipalName")
            or ""
        ).strip().lower()

        if microsoft_email != requested_by:
            raise ValueError(
                "The Microsoft account used for Outlook does not match "
                f"the signed-in SFDA user ({requested_by}). "
                "Sign in to Microsoft with the same company account."
            )

        selected_ids = json.loads(
            str(request_record.get("SelectedIDsJson") or "[]")
        )
        output = _build_variance_report(selected_ids)
        file_name, file_value = next(iter(output.items()))
        file_bytes, content_type, _ = decode_generated_file(
            file_name,
            file_value,
        )

        draft = create_draft_with_attachment(
            access_token,
            recipient=str(request_record.get("RecipientEmail") or ""),
            subject=str(request_record.get("Subject") or ""),
            body=str(request_record.get("MessageBody") or ""),
            attachment_bytes=file_bytes,
            attachment_name=file_name,
            attachment_content_type=content_type,
        )

        complete_outlook_draft_request(
            str(request_record["RequestID"]),
            graph_message_id=str(draft["message_id"]),
            graph_web_link=str(draft["web_link"]),
        )

        return func.HttpResponse(
            "",
            status_code=302,
            headers={
                "Location": str(draft["web_link"]),
                "Cache-Control": "no-store",
            },
        )
    except ValueError as exc:
        if request_record:
            try:
                from engine.database import fail_outlook_draft_request
                fail_outlook_draft_request(
                    str(request_record["RequestID"]),
                    str(exc),
                )
            except Exception:
                pass
        return _outlook_callback_error(str(exc), 400)
    except Exception as exc:
        logger.exception("Outlook draft callback failed")
        if request_record:
            try:
                from engine.database import fail_outlook_draft_request
                fail_outlook_draft_request(
                    str(request_record["RequestID"]),
                    str(exc),
                )
            except Exception:
                pass
        return _outlook_callback_error(str(exc), 500)


@app.route(route="variance-management/email", methods=["POST"])
def variance_management_email_deprecated(
    req: func.HttpRequest,
) -> func.HttpResponse:
    return error_response(
        "Direct email sending is disabled.",
        410,
        "Use Prepare Outlook Draft from Variance Management instead.",
    )


@app.route(route="product-intelligence", methods=["GET"])
def product_intelligence(req: func.HttpRequest) -> func.HttpResponse:
    denied = _auth_guard(req)
    if denied:
        return denied
    """Return the consolidated Product Intelligence knowledge base."""
    try:
        from engine.database import get_product_intelligence_sources
        from engine.product_intelligence import ProductIntelligenceEngine

        sources = get_product_intelligence_sources()
        result = ProductIntelligenceEngine().build(**sources)
        return json_response(result)
    except Exception as exc:
        logger.exception("Product Intelligence failed")
        return error_response("Product Intelligence failed.", 500, str(exc))


@app.route(
    route="full-reconciliation/run",
    methods=["GET", "POST"],
)
def full_reconciliation_run(req: func.HttpRequest) -> func.HttpResponse:
    denied = _auth_guard(req)
    if denied:
        return denied
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



def refresh_historical_data_after_daily_run(
    mode: str,
    asn_df: pd.DataFrame,
    dispatch_df: pd.DataFrame,
    sfda_df: pd.DataFrame,
    asn_file_name: str = "",
    dispatch_file_name: str = "",
    sfda_file_name: str = "",
) -> Dict[str, Any]:
    """Append successful daily movements and rebuild cumulative history.

    ReceiptEvents and DispatchEvents remain the permanent source of truth.
    EventKey de-duplication prevents the same daily file from increasing the
    historical quantities twice. Batch Master and both history tables are
    rebuilt from the cumulative events after each successful daily run.
    """
    from engine.database import (
        append_events,
        get_event_summaries,
        get_history_summaries,
        replace_batch_master,
        replace_customer_history,
        replace_latest_sfda_snapshot,
        replace_supplier_history,
    )
    from engine.full_reconciliation import FullReconciliationEngine

    history_engine = FullReconciliationEngine(
        asn_df if mode == "accept" else pd.DataFrame(),
        dispatch_df if mode == "dispatch" else pd.DataFrame(),
        sfda_df,
    )
    prepared = history_engine.prepare_incremental()
    inserted = append_events(
        prepared["receipt_records"],
        prepared["dispatch_records"],
    )

    receipt_summary, dispatch_summary = get_event_summaries()
    master = history_engine.build_master_from_summaries(
        receipt_summary,
        dispatch_summary,
        prepared["sfda_summary"],
    )
    replace_batch_master(master)

    supplier_summary, customer_summary = get_history_summaries()
    supplier_history = history_engine.build_supplier_history(
        supplier_summary,
        master,
    )
    customer_history = history_engine.build_customer_history(
        customer_summary,
        master,
    )
    replace_supplier_history(supplier_history)
    replace_customer_history(customer_history)

    sfda_snapshot_rows = replace_latest_sfda_snapshot(
        sfda_df,
        sfda_file_name,
    )

    return {
        "receipt_events_added": int(inserted.get("receipt_events", 0)),
        "dispatch_events_added": int(inserted.get("dispatch_events", 0)),
        "batch_master_rows": int(len(master)),
        "supplier_history_rows": int(len(supplier_history)),
        "customer_history_rows": int(len(customer_history)),
        "sfda_snapshot_rows": int(sfda_snapshot_rows),
        "source_file": asn_file_name if mode == "accept" else dispatch_file_name,
    }


class _BackgroundUploadedFile:
    def __init__(self, file_name: str, data: bytes, content_type: str) -> None:
        self.filename = str(file_name or "uploaded.xlsx")
        self.content_type = str(content_type or "application/octet-stream")
        self._data = data or b""

    def read(self) -> bytes:
        return self._data


class _BackgroundFiles:
    def __init__(self, values: Dict[str, Any]) -> None:
        self._values = values

    def get(self, key: str, default: Any = None) -> Any:
        return self._values.get(key, default)

    def getlist(self, key: str) -> List[Any]:
        value = self._values.get(key)
        if value is None:
            return []
        return value if isinstance(value, list) else [value]


class _BackgroundRequest:
    def __init__(self, files: Dict[str, Any], submitted_by: str = "Background Worker") -> None:
        self.files = _BackgroundFiles(files)
        self.form: Dict[str, Any] = {}
        self.params: Dict[str, Any] = {}
        self.route_params: Dict[str, Any] = {}
        self.headers: Dict[str, Any] = {"X-User-Name": submitted_by}
        self.method = "POST"
        self.url = "background://reconciliation"


def _http_response_payload(response: func.HttpResponse) -> Dict[str, Any]:
    body = response.get_body().decode("utf-8") if response.get_body() else ""
    if not body:
        return {
            "status": "Failed",
            "error": f"Background processing returned HTTP {response.status_code} without JSON.",
        }
    try:
        parsed = json.loads(body)
    except json.JSONDecodeError as exc:
        raise ValueError("Background processing returned invalid JSON.") from exc
    if not isinstance(parsed, dict):
        raise ValueError("Background processing returned an invalid payload.")
    return parsed


def _queue_reconciliation_job(
    req: func.HttpRequest,
    job_type: str,
    required_fields: List[str],
) -> func.HttpResponse:
    """Upload request inputs, queue processing, and return 202 immediately."""
    from engine.blob_storage import BlobStorage
    from engine.warehouse_context import current_warehouse_id, current_warehouse_name

    submitted_by = get_submitted_by(req)
    job_id = build_run_number("JOB")
    storage = BlobStorage()
    storage.initialize_containers()

    manifest: Dict[str, Any] = {}
    for field in required_fields:
        uploaded_file = req.files.get(field)
        if uploaded_file is None:
            label = {
                "sfda": "SFDA file",
                "asn": "ASN/ASDT file",
                "dispatch": "Full Dispatch file",
                "inventory": "Current Inventory file",
            }.get(field, field)
            return error_response(f"{label} is required.")

        file_name, file_bytes, content_type = read_uploaded_bytes(uploaded_file)
        saved = storage.upload_job_input(
            job_id,
            field,
            file_name,
            file_bytes,
            content_type,
        )
        manifest[field] = {
            "file_name": saved["file_name"],
            "blob_name": saved["blob_name"],
            "content_type": saved["content_type"],
            "size_bytes": saved["size_bytes"],
        }

    warehouse_id = int(current_warehouse_id())
    warehouse_name = str(current_warehouse_name())
    storage.write_background_job_status(
        job_id,
        {
            "job_id": job_id,
            "job_type": job_type,
            "status": "Queued",
            "progress": 5,
            "current_stage": "Files uploaded; waiting for background worker",
            "submitted_by": submitted_by,
            "warehouse_id": warehouse_id,
            "warehouse_name": warehouse_name,
            "created_at": datetime.now(timezone.utc).isoformat(),
            "result": {},
            "error": "",
        },
    )

    connection_string = os.getenv("AzureWebJobsStorage")
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
                "job_id": job_id,
                "job_type": job_type,
                "input_manifest": manifest,
                "submitted_by": submitted_by,
                "warehouse_id": warehouse_id,
                "warehouse_name": warehouse_name,
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
            "job_type": job_type,
            "status_url": f"/api/reconciliation-job/{job_id}",
            "message": "Reconciliation was queued and will continue in the background.",
        },
        202,
    )


def _safe_queue_reconciliation_job(
    req: func.HttpRequest,
    job_type: str,
    required_fields: List[str],
) -> func.HttpResponse:
    try:
        return _queue_reconciliation_job(req, job_type, required_fields)
    except ValueError as exc:
        logger.exception("Background reconciliation submission validation failed")
        return error_response("Reconciliation submission failed.", 400, str(exc))
    except Exception as exc:
        logger.exception("Background reconciliation submission failed")
        return error_response("Failed to queue reconciliation.", 500, str(exc))


@app.route(route="reconciliation-job/{job_id}", methods=["GET"])
def reconciliation_job_status(req: func.HttpRequest) -> func.HttpResponse:
    denied = _auth_guard(req)
    if denied:
        return denied
    job_id = str(req.route_params.get("job_id", "")).strip()
    if not job_id:
        return error_response("job_id is required.", 400)
    try:
        from engine.blob_storage import BlobStorage
        job = BlobStorage().read_background_job_status(job_id)
        return json_response({"status": "Success", "job": job})
    except FileNotFoundError:
        return error_response("Background reconciliation job was not found.", 404)
    except Exception as exc:
        logger.exception("Background reconciliation status read failed")
        return error_response("Failed to read reconciliation job status.", 500, str(exc))


@app.queue_trigger(
    arg_name="message",
    queue_name="reconciliation-jobs",
    connection="AzureWebJobsStorage",
)
def reconciliation_background_worker(message: func.QueueMessage) -> None:
    payload = json.loads(message.get_body().decode("utf-8"))
    job_id = str(payload.get("job_id", "")).strip()
    job_type = str(payload.get("job_type", "")).strip().lower()
    input_manifest = payload.get("input_manifest") or {}
    submitted_by = str(payload.get("submitted_by") or "Background Worker")
    warehouse_id_raw = payload.get("warehouse_id")
    warehouse_name = str(payload.get("warehouse_name") or "").strip()

    if not job_id or not job_type:
        raise ValueError("Background reconciliation message is incomplete.")
    if warehouse_id_raw in (None, ""):
        raise ValueError("Background reconciliation message is missing warehouse_id.")
    warehouse_id = int(warehouse_id_raw)
    if warehouse_id < 1:
        raise ValueError("Background reconciliation message has an invalid warehouse_id.")

    from engine.blob_storage import BlobStorage, INPUTS_CONTAINER
    from engine.warehouse_context import warehouse_scope

    with warehouse_scope(warehouse_id, warehouse_name or f"Warehouse {warehouse_id}"):
        storage = BlobStorage()
        storage.initialize_containers()
        base_status = {
            "job_id": job_id,
            "job_type": job_type,
            "submitted_by": submitted_by,
            "warehouse_id": warehouse_id,
            "warehouse_name": warehouse_name or f"Warehouse {warehouse_id}",
        }
        storage.write_background_job_status(
            job_id,
            {
                **base_status,
                "status": "Running",
                "progress": 15,
                "current_stage": "Reading uploaded files",
                "started_at": datetime.now(timezone.utc).isoformat(),
                "result": {},
                "error": "",
            },
        )
        try:
            files: Dict[str, Any] = {}
            for field, item in input_manifest.items():
                downloaded = storage.download_blob(INPUTS_CONTAINER, str(item["blob_name"]))
                files[field] = _BackgroundUploadedFile(
                    str(item.get("file_name") or field),
                    downloaded["data"],
                    str(item.get("content_type") or downloaded["content_type"]),
                )

            storage.write_background_job_status(
                job_id,
                {
                    **base_status,
                    "status": "Running",
                    "progress": 35,
                    "current_stage": "Running reconciliation in background",
                    "started_at": datetime.now(timezone.utc).isoformat(),
                    "result": {},
                    "error": "",
                },
            )
            background_req = _BackgroundRequest(files, submitted_by)
            if job_type == "daily_accept":
                response = run_daily(background_req, "accept")
            elif job_type == "daily_dispatch":
                response = run_daily(background_req, "dispatch")
            elif job_type == "full_accept":
                response = _run_full_reconciliation_accept(background_req)
            elif job_type == "full_dispatch":
                response = _run_full_reconciliation_dispatch(background_req)
            else:
                raise ValueError(f"Unsupported background job type: {job_type}")

            result = _http_response_payload(response)
            failed = response.status_code >= 400 or str(result.get("status", "")).lower() == "failed"
            storage.write_background_job_status(
                job_id,
                {
                    **base_status,
                    "status": "Failed" if failed else "Completed",
                    "progress": 100,
                    "current_stage": "Failed" if failed else "Completed",
                    "completed_at": datetime.now(timezone.utc).isoformat(),
                    "result": result if not failed else {},
                    "error": (
                        str(result.get("details") or result.get("error") or "Background reconciliation failed.")
                        if failed else ""
                    ),
                },
            )
            if not failed:
                _refresh_dashboard_summary_safe()
        except Exception as exc:
            logger.exception("Background reconciliation job failed. job_id=%s", job_id)
            storage.write_background_job_status(
                job_id,
                {
                    **base_status,
                    "status": "Failed",
                    "progress": 100,
                    "current_stage": "Failed",
                    "completed_at": datetime.now(timezone.utc).isoformat(),
                    "result": {},
                    "error": str(exc),
                },
            )
            raise


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
        asn_name = ""
        dispatch_name = ""

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
        accept_confirmation = {
            "baseline_available": False,
            "confirmed_packs": 0.0,
            "confirmed_each": 0.0,
            "confirmed_transactions": 0,
            "confirmed_batches": 0,
        }
        dispatch_confirmation = {
            "baseline_available": False,
            "confirmed_packs": 0.0,
            "confirmed_each": 0.0,
            "confirmed_transactions": 0,
            "confirmed_batches": 0,
        }
        try:
            if mode == "accept":
                from engine.database import (
                    confirm_accept_transactions_from_sfda,
                    get_accept_confirmed_transactions,
                )

                # A generated Accept file is NOT proof of processing.
                # Only the newly uploaded SFDA report may confirm prior pending
                # quantities by showing a matching pending decrease and Active
                # increase. Legacy DailyProcessedTransactions rows are
                # intentionally ignored for Accept because older failed runs
                # could have written them before the workflow completed.
                accept_confirmation = confirm_accept_transactions_from_sfda(
                    sfda_df,
                    sfda_name,
                )
                processed_transactions = get_accept_confirmed_transactions()
            else:
                from engine.database import (
                    confirm_dispatch_transactions_from_sfda,
                    get_dispatch_confirmed_transactions,
                )

                # Dispatch is considered processed only when the newly uploaded
                # SFDA report proves the prior movement.  Legacy
                # DailyProcessedTransactions rows are intentionally ignored.
                dispatch_confirmation = confirm_dispatch_transactions_from_sfda(
                    sfda_df,
                    sfda_name,
                )
                processed_transactions = get_dispatch_confirmed_transactions()
        except Exception as exc:
            logger.warning("Daily processed transaction read skipped: %s", exc)
            if mode == "accept":
                raise

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
        sto_incoming_followup = result.get("sto_incoming_followup", pd.DataFrame())
        sto_return_cancel = result.get("sto_return_cancel_dispatch", pd.DataFrame())

        outputs: Dict[str, Any] = {}
        if mode == "accept":
            outputs["accept_details"] = build_excel(
                report,
                "Accept_Details.xlsx",
                "Accept Details",
                "SFDA Accept Details - Classified by TRK",
            )
            outputs["sto_incoming_followup"] = build_excel(
                sto_incoming_followup,
                "STO_Incoming_RSD_Follow_Up.xlsx",
                "STO Incoming",
                "STO Incoming - RSD Follow-up",
            )
            outputs["sto_return_cancel_dispatch"] = build_excel(
                sto_return_cancel,
                "STO_Return_Cancel_Dispatch.xlsx",
                "STO Return",
                "STO Return - Cancel Previous RSD Dispatch",
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

        # Persistent history and confirmation state are deliberately deferred
        # until after the files have been generated and archived successfully.
        historical_update: Dict[str, Any] = {}
        saved_transactions = 0
        pending_confirmation_saved = 0

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
            "batch_master_rows": int(len(batch_master)),
            "reconciliation_rows": len(report),
            "total_input_rows": total_input_rows,
            "accept_rows": accept_rows,
            "dispatch_rows": dispatch_rows,
            "accept_files": len(outputs.get("accept_files", {})),
            "dispatch_files": len(outputs.get("dispatch_files", {})),
            "sto_incoming_followup_rows": int(len(sto_incoming_followup)),
            "sto_return_rows": int(len(sto_return_cancel)),
            "generated_files": generated_output_files,
            "processed_transactions_saved": 0,
            "accept_confirmation": accept_confirmation if mode == "accept" else {},
            "dispatch_confirmation": dispatch_confirmation if mode == "dispatch" else {},
        }

        archived_files = save_run_archive(
            run_number=run_number,
            mode=mode,
            input_files=input_files,
            outputs=outputs,
            summary=summary,
            submitted_by=submitted_by,
        )

        # Accept keeps the physical receipt history behaviour already approved.
        # Dispatch is different: the generated CSV must NOT update Batch Master
        # or Customer History.  Only SFDA-confirmed dispatch quantities are
        # synchronized to cumulative history.
        if mode == "accept":
            historical_update = refresh_historical_data_after_daily_run(
                mode=mode,
                asn_df=asn_df,
                dispatch_df=dispatch_df,
                sfda_df=sfda_df,
                asn_file_name=asn_name,
                dispatch_file_name=dispatch_name,
                sfda_file_name=sfda_name,
            )
            from engine.database import (
                replace_accept_sfda_baseline,
                save_accept_pending_transactions,
            )

            pending_rows = result.get(
                "pending_confirmation_transactions",
                pd.DataFrame(),
            )
            if pending_rows is not None and not pending_rows.empty:
                pending_confirmation_saved = save_accept_pending_transactions(
                    pending_rows.to_dict(orient="records"),
                    run_number,
                )

            # On the first run there is no proof baseline yet. Store the current
            # SFDA report only after this Accept run has successfully reached
            # the persistence stage. On later runs this is idempotent because
            # confirmation already advanced the baseline before reconciliation.
            replace_accept_sfda_baseline(sfda_df, sfda_name)
        else:
            from engine.database import (
                append_events,
                get_dispatch_confirmed_history_records,
                get_event_summaries,
                get_history_summaries,
                replace_batch_master,
                replace_customer_history,
                replace_dispatch_sfda_baseline,
                replace_latest_sfda_snapshot,
                replace_supplier_history,
                save_dispatch_pending_transactions,
            )
            from engine.full_reconciliation import FullReconciliationEngine

            # Synchronize only confirmations already proven by the SFDA delta.
            confirmed_history_rows = get_dispatch_confirmed_history_records()
            inserted = append_events([], confirmed_history_rows)
            history_engine = FullReconciliationEngine(
                pd.DataFrame(), pd.DataFrame(), sfda_df
            )
            prepared_sfda = history_engine.prepare_incremental()["sfda_summary"]
            receipt_summary, dispatch_summary = get_event_summaries()
            master = history_engine.build_master_from_summaries(
                receipt_summary, dispatch_summary, prepared_sfda
            )
            replace_batch_master(master)
            supplier_summary, customer_summary = get_history_summaries()
            supplier_history = history_engine.build_supplier_history(
                supplier_summary, master
            )
            customer_history = history_engine.build_customer_history(
                customer_summary, master
            )
            replace_supplier_history(supplier_history)
            replace_customer_history(customer_history)
            sfda_snapshot_rows = replace_latest_sfda_snapshot(sfda_df, sfda_name)

            historical_update = {
                "receipt_events_added": 0,
                "dispatch_events_added": int(inserted.get("dispatch_events", 0)),
                "batch_master_rows": int(len(master)),
                "supplier_history_rows": int(len(supplier_history)),
                "customer_history_rows": int(len(customer_history)),
                "sfda_snapshot_rows": int(sfda_snapshot_rows),
                "source_file": sfda_name,
                "confirmation_only": True,
            }

            pending_rows = result.get(
                "pending_confirmation_transactions",
                pd.DataFrame(),
            )
            if pending_rows is not None and not pending_rows.empty:
                pending_confirmation_saved = save_dispatch_pending_transactions(
                    pending_rows.to_dict(orient="records"),
                    run_number,
                )

            # Store the current SFDA state as the proof baseline for the next
            # Dispatch run.  This does not alter Batch Master quantities.
            replace_dispatch_sfda_baseline(sfda_df, sfda_name)

        summary.update(
            {
                "batch_master_available": historical_update["batch_master_rows"] > 0,
                "batch_master_rows": historical_update["batch_master_rows"],
                "historical_update": historical_update,
                "processed_transactions_saved": saved_transactions,
                "pending_confirmation_transactions_saved": pending_confirmation_saved,
            }
        )

        from engine.database import complete_reconciliation_run

        complete_reconciliation_run(
            run_number=run_number,
            status="Completed",
            total_input_rows=total_input_rows,
            master_records=historical_update["batch_master_rows"],
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
                "dashboard_preview": build_dashboard_preview(
                    report,
                    mode,
                ),
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
    denied = _auth_guard(req)
    if denied:
        return denied
    if req.method == "GET":
        return json_response({"status": "Ready", "step": "accept", "version": APPLICATION_VERSION})
    return _safe_queue_reconciliation_job(req, "daily_accept", ["sfda", "asn"])


@app.route(route="process-dispatch", methods=["GET", "POST"])
def process_dispatch(req: func.HttpRequest) -> func.HttpResponse:
    denied = _auth_guard(req)
    if denied:
        return denied
    if req.method == "GET":
        return json_response({"status": "Ready", "step": "dispatch", "version": APPLICATION_VERSION})
    return _safe_queue_reconciliation_job(req, "daily_dispatch", ["sfda", "dispatch"])


@app.route(route="reconcile", methods=["GET", "POST"])
def reconcile(req: func.HttpRequest) -> func.HttpResponse:
    denied = _auth_guard(req)
    if denied:
        return denied
    """Backward-compatible route used by older UI versions."""
    if req.method == "GET":
        return json_response(
            {
                "status": "Ready",
                "message": "Use process_type=accept or process_type=dispatch.",
                "version": APPLICATION_VERSION,
            }
        )
    mode = str(req.params.get("process_type", "")).lower().strip()
    if not mode:
        mode = "accept" if req.files.get("asn") is not None else "dispatch"
    if mode not in {"accept", "dispatch"}:
        return error_response("process_type must be accept or dispatch.")
    if mode == "accept":
        return _safe_queue_reconciliation_job(req, "daily_accept", ["sfda", "asn"])
    return _safe_queue_reconciliation_job(req, "daily_dispatch", ["sfda", "dispatch"])


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
