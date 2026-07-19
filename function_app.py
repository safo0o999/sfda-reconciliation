import io
import json
import logging
from pathlib import Path
from typing import Any, Dict, Iterable, List

import azure.functions as func
import pandas as pd


logger = logging.getLogger("SFDA-Reconciliation")
APPLICATION_NAME = "SFDA Reconciliation"
APPLICATION_VERSION = "5.0.0"
app = func.FunctionApp(http_auth_level=func.AuthLevel.ANONYMOUS)


def json_response(data: Dict[str, Any], status_code: int = 200) -> func.HttpResponse:
    return func.HttpResponse(
        body=json.dumps(data, ensure_ascii=False, default=str),
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


@app.route(route="history", methods=["GET"])
def history(req: func.HttpRequest) -> func.HttpResponse:
    try:
        from engine import database

        reader = getattr(database, "get_reconciliation_history", None)
        rows = reader(limit=100) if callable(reader) else []
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
        return error_response("Unable to load reconciliation history.", 500, str(exc))


@app.route(route="batch-master/build", methods=["GET", "POST"])
def batch_master_build(req: func.HttpRequest) -> func.HttpResponse:
    if req.method == "GET":
        return json_response(
            {
                "status": "Ready",
                "application": APPLICATION_NAME,
                "version": APPLICATION_VERSION,
                "message": "Upload historical ASN and/or Full Dispatch files plus the latest SFDA report.",
            }
        )

    try:
        asn_files = req.files.getlist("asn_files")
        dispatch_files = req.files.getlist("dispatch_files")
        sfda_file = req.files.get("sfda")
        operation = str(req.form.get("operation", "append")).strip().lower()

        if not asn_files and not dispatch_files:
            return error_response("At least one ASN or Full Dispatch file is required.")
        if sfda_file is None:
            return error_response("SFDA file is required.")
        if operation not in {"append", "rebuild"}:
            return error_response("operation must be append or rebuild.")

        asn_df = read_excel_files(asn_files)
        dispatch_df = read_excel_files(dispatch_files)
        sfda_df = read_excel_upload(sfda_file)

        from engine.database import (
            append_events,
            get_event_summaries,
            replace_batch_master,
            reset_history,
        )
        from engine.exporter import Exporter
        from engine.full_reconciliation import FullReconciliationEngine

        engine = FullReconciliationEngine(asn_df, dispatch_df, sfda_df)
        prepared = engine.prepare_incremental()

        if operation == "rebuild":
            reset_history()

        inserted = append_events(
            prepared["receipt_records"],
            prepared["dispatch_records"],
        )
        receipt_summary, dispatch_summary = get_event_summaries()
        master = engine.build_master_from_summaries(
            receipt_summary,
            dispatch_summary,
            prepared["sfda_summary"],
        )
        replace_batch_master(master)

        master_file = Exporter.build_formatted_excel_file(
            df=master,
            file_name="Batch_Master.xlsx",
            sheet_name="Batch Master",
            title="SFDA Historical Batch Master",
            sort_columns=["Generic Item Number", "BN", "Expiry Date"],
        )

        return json_response(
            {
                "status": "Completed",
                "application": APPLICATION_NAME,
                "version": APPLICATION_VERSION,
                "operation": operation,
                "summary": {
                    "asn_files": len(asn_files),
                    "dispatch_files": len(dispatch_files),
                    "prepared_receipt_events": len(prepared["receipt_events"]),
                    "prepared_dispatch_events": len(prepared["dispatch_events"]),
                    "inserted_receipt_events": inserted.get("receipt_events", 0),
                    "inserted_dispatch_events": inserted.get("dispatch_events", 0),
                    "batch_master_rows": len(master),
                },
                "outputs": {"batch_master": master_file},
            }
        )
    except ValueError as exc:
        logger.exception("Batch Master validation failed")
        return error_response("Batch Master input validation failed.", 400, str(exc))
    except Exception as exc:
        logger.exception("Batch Master build failed")
        return error_response("Failed to build Batch Master.", 500, str(exc))


def run_daily(req: func.HttpRequest, mode: str) -> func.HttpResponse:
    try:
        sfda_file = req.files.get("sfda")
        if sfda_file is None:
            return error_response("SFDA file is required.")

        sfda_df = read_excel_upload(sfda_file)
        asn_df = pd.DataFrame()
        dispatch_df = pd.DataFrame()
        inventory_df = pd.DataFrame()

        if mode == "accept":
            asn_file = req.files.get("asn")
            if asn_file is None:
                return error_response("ASN/ASDT file is required for Accept.")
            asn_df = read_excel_upload(asn_file)
        else:
            dispatch_file = req.files.get("dispatch")
            if dispatch_file is None:
                return error_response("Full Dispatch file is required for Dispatch.")
            dispatch_df = read_excel_upload(dispatch_file)

        from engine.exporter import Exporter
        from engine.reconciliation import ReconciliationEngine

        batch_master = optional_batch_master()
        result = ReconciliationEngine(
            mode=mode,
            sfda_df=sfda_df,
            asn_df=asn_df,
            dispatch_df=dispatch_df,
            inventory_df=inventory_df,
            batch_master_df=batch_master,
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

        return json_response(
            {
                "status": "Completed",
                "application": APPLICATION_NAME,
                "version": APPLICATION_VERSION,
                "step": mode,
                "summary": {
                    "batch_master_available": not batch_master.empty,
                    "batch_master_rows": len(batch_master),
                    "reconciliation_rows": len(report),
                    "accept_rows": count_positive_rows(accept, "To Be Accept"),
                    "dispatch_rows": count_positive_rows(
                        dispatch,
                        "Allocated To Be Dispatch",
                    ),
                    "accept_files": len(outputs.get("accept_files", {})),
                    "dispatch_files": len(outputs.get("dispatch_files", {})),
                    "generated_files": sum(
                        len(group) if isinstance(group, dict) else 0
                        for group in outputs.values()
                    ),
                },
                "outputs": outputs,
            }
        )
    except ValueError as exc:
        logger.exception("Daily reconciliation validation failed")
        return error_response(f"{mode.title()} validation failed.", 400, str(exc))
    except Exception as exc:
        logger.exception("Daily reconciliation failed")
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
