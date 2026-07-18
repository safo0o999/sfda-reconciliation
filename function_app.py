import io
import json
import logging
from pathlib import Path
from typing import Iterable, List

import azure.functions as func
import pandas as pd

from engine.database import (
    append_events,
    get_batch_master_df,
    get_dispatch_events_df,
    get_event_summaries,
    initialize_database,
    replace_batch_master,
    reset_history,
    test_database_connection,
)
from engine.exporter import Exporter
from engine.full_reconciliation import FullReconciliationEngine
from engine.reconciliation import ReconciliationEngine


app = func.FunctionApp(
    http_auth_level=func.AuthLevel.ANONYMOUS
)

APPLICATION_NAME = "SFDA Reconciliation"
APPLICATION_VERSION = "5.0.0"


# -----------------------------------------------------------------------------
# Response helpers
# -----------------------------------------------------------------------------


def json_response(data, status_code=200):
    return func.HttpResponse(
        json.dumps(
            data,
            ensure_ascii=False,
            default=str,
        ),
        status_code=status_code,
        mimetype="application/json",
        charset="utf-8",
    )


def error_response(message, status_code=500, error_type=None):
    payload = {
        "status": "Failed",
        "version": APPLICATION_VERSION,
        "error": str(message),
    }

    if error_type:
        payload["error_type"] = error_type

    return json_response(
        payload,
        status_code=status_code,
    )


# -----------------------------------------------------------------------------
# Upload and Excel helpers
# -----------------------------------------------------------------------------


def _uploaded_file_name(uploaded):
    return (
        getattr(uploaded, "filename", None)
        or "uploaded.xlsx"
    )


def _read_uploaded_bytes(uploaded):
    file_name = _uploaded_file_name(uploaded)

    raw = uploaded.read()

    if not raw:
        raise ValueError(
            f"Uploaded file is empty: {file_name}"
        )

    return file_name, raw


def read_excel(uploaded):
    file_name, raw = _read_uploaded_bytes(uploaded)

    lower_name = file_name.lower()

    if lower_name.endswith(".xls"):
        engine = "xlrd"
    elif lower_name.endswith(".xlsx"):
        engine = "openpyxl"
    else:
        raise ValueError(
            "Unsupported file type. Only .xls and .xlsx files "
            f"are accepted: {file_name}"
        )

    try:
        return pd.read_excel(
            io.BytesIO(raw),
            engine=engine,
            dtype=object,
        )
    except Exception as ex:
        raise ValueError(
            f"Unable to read Excel file '{file_name}': {ex}"
        ) from ex


def read_many(files: Iterable):
    frames: List[pd.DataFrame] = []

    for uploaded in files:
        file_name = _uploaded_file_name(uploaded)
        frame = read_excel(uploaded)
        frame["_Source File"] = file_name
        frames.append(frame)

    if not frames:
        return pd.DataFrame()

    return pd.concat(
        frames,
        ignore_index=True,
        sort=False,
    )


def excel_payload(df, file_name, title):
    return Exporter.build_formatted_excel_file(
        df=df,
        file_name=file_name,
        sheet_name=title[:31],
        title=title,
    )


def safe_preview(df, rows=100):
    preview = df.head(rows).copy()
    preview = preview.astype(object).where(
        pd.notna(preview),
        None,
    )
    return preview.to_dict(orient="records")


def _get_files(req, plural_name, singular_name):
    files = list(req.files.getlist(plural_name))

    if not files:
        files = list(req.files.getlist(singular_name))

    return [
        uploaded
        for uploaded in files
        if uploaded is not None
    ]


# -----------------------------------------------------------------------------
# UI and service status
# -----------------------------------------------------------------------------


@app.route(route="", methods=["GET"])
def home(req: func.HttpRequest) -> func.HttpResponse:
    return func.HttpResponse(
        status_code=302,
        headers={
            "Location": "/api/ui"
        },
    )


@app.route(route="ui", methods=["GET"])
def ui(req: func.HttpRequest) -> func.HttpResponse:
    path = (
        Path(__file__).resolve().parent
        / "web"
        / "index.html"
    )

    if not path.exists():
        return error_response(
            "web/index.html was not found.",
            status_code=404,
            error_type="FileNotFoundError",
        )

    return func.HttpResponse(
        path.read_text(encoding="utf-8"),
        mimetype="text/html",
        charset="utf-8",
    )


@app.route(route="health", methods=["GET"])
def health(req: func.HttpRequest) -> func.HttpResponse:
    try:
        database_status = test_database_connection()

        return json_response({
            "status": "Healthy",
            "application": APPLICATION_NAME,
            "version": APPLICATION_VERSION,
            "database": database_status,
        })

    except Exception as ex:
        logging.exception("Health check failed")

        return error_response(
            ex,
            status_code=500,
            error_type=type(ex).__name__,
        )


# -----------------------------------------------------------------------------
# Step 1 - Build or update cumulative Batch Master
# -----------------------------------------------------------------------------


@app.route(
    route="batch-master/build",
    methods=["GET", "POST"],
)
def build_batch_master(
    req: func.HttpRequest,
) -> func.HttpResponse:

    if req.method == "GET":
        return json_response({
            "status": "Ready",
            "application": APPLICATION_NAME,
            "version": APPLICATION_VERSION,
            "required_files": [
                "asn_files",
                "dispatch_files",
                "sfda",
            ],
            "operations": [
                "append",
                "rebuild",
            ],
        })

    try:
        asn_files = _get_files(
            req,
            "asn_files",
            "asn",
        )

        dispatch_files = _get_files(
            req,
            "dispatch_files",
            "dispatch",
        )

        sfda_file = req.files.get("sfda")

        if not asn_files:
            raise ValueError(
                "At least one ASN Receipt file is required."
            )

        if not dispatch_files:
            raise ValueError(
                "At least one Full Dispatch file is required."
            )

        if sfda_file is None:
            raise ValueError(
                "Latest SFDA Drug report is required."
            )

        operation = str(
            req.form.get("operation") or "append"
        ).strip().lower()

        if operation not in {
            "append",
            "rebuild",
        }:
            raise ValueError(
                "operation must be append or rebuild."
            )

        asn_dataframe = read_many(asn_files)
        dispatch_dataframe = read_many(dispatch_files)
        sfda_dataframe = read_excel(sfda_file)

        engine = FullReconciliationEngine(
            asn_df=asn_dataframe,
            dispatch_df=dispatch_dataframe,
            sfda_df=sfda_dataframe,
        )

        prepared = engine.prepare_incremental()

        initialize_database()

        if operation == "rebuild":
            reset_history()

        saved = append_events(
            prepared["receipt_records"],
            prepared["dispatch_records"],
        )

        receipt_summary, dispatch_summary = (
            get_event_summaries()
        )

        master = engine.build_master_from_summaries(
            receipt_summary=receipt_summary,
            dispatch_summary=dispatch_summary,
            sfda_summary=prepared["sfda_summary"],
        )

        replace_batch_master(master)

        outputs = excel_payload(
            master,
            "Batch_Master.xlsx",
            "Batch Master",
        )

        return json_response({
            "status": "Completed",
            "application": APPLICATION_NAME,
            "version": APPLICATION_VERSION,
            "operation": operation,
            "summary": {
                "uploaded_asn_files": len(asn_files),
                "uploaded_dispatch_files": len(
                    dispatch_files
                ),
                "prepared_receipt_events": len(
                    prepared["receipt_records"]
                ),
                "prepared_dispatch_events": len(
                    prepared["dispatch_records"]
                ),
                "new_receipt_events": saved[
                    "receipt_events"
                ],
                "new_dispatch_events": saved[
                    "dispatch_events"
                ],
                "batch_master_rows": len(master),
            },
            "outputs": outputs,
            "preview": safe_preview(master),
        })

    except ValueError as ex:
        logging.exception("Batch Master input failed")

        return error_response(
            ex,
            status_code=400,
            error_type=type(ex).__name__,
        )

    except Exception as ex:
        logging.exception("Batch Master build failed")

        return error_response(
            ex,
            status_code=500,
            error_type=type(ex).__name__,
        )


# -----------------------------------------------------------------------------
# Step 2 and Step 3 - Reconcile and generate SFDA files
# -----------------------------------------------------------------------------


@app.route(
    route="reconcile",
    methods=["GET", "POST"],
)
def reconcile(
    req: func.HttpRequest,
) -> func.HttpResponse:

    if req.method == "GET":
        return json_response({
            "status": "Ready",
            "application": APPLICATION_NAME,
            "version": APPLICATION_VERSION,
            "required_files": [
                "inventory",
                "sfda",
            ],
        })

    try:
        inventory_file = req.files.get("inventory")
        sfda_file = req.files.get("sfda")

        if inventory_file is None:
            raise ValueError(
                "Latest Inventory report is required."
            )

        if sfda_file is None:
            raise ValueError(
                "Latest SFDA Drug report is required."
            )

        batch_master = get_batch_master_df()

        if batch_master.empty:
            raise ValueError(
                "Batch Master is empty. Complete Step 1 first."
            )

        inventory_dataframe = read_excel(
            inventory_file
        )

        sfda_dataframe = read_excel(
            sfda_file
        )

        dispatch_events = get_dispatch_events_df()

        engine = ReconciliationEngine(
            batch_master_df=batch_master,
            inventory_df=inventory_dataframe,
            sfda_df=sfda_dataframe,
            dispatch_events_df=dispatch_events,
        )

        result = engine.run()

        accept_files = (
            Exporter.build_sfda_upload_files(
                df=result["accept"],
                quantity_column="To Be Accept",
                file_prefix="Accept",
            )
        )

        dispatch_files = (
            Exporter.build_dispatch_files_by_customer(
                result["dispatch"]
            )
        )

        reconciliation_excel = excel_payload(
            result["report"],
            "Reconciliation_Report.xlsx",
            "Reconciliation Report",
        )

        outputs = {
            "reconciliation_report": (
                reconciliation_excel
            ),
            "accept_files": accept_files,
            "dispatch_files": dispatch_files,
        }

        return json_response({
            "status": "Completed",
            "application": APPLICATION_NAME,
            "version": APPLICATION_VERSION,
            "summary": {
                "batch_master_rows": len(batch_master),
                "dispatch_history_rows": len(
                    dispatch_events
                ),
                "reconciliation_rows": len(
                    result["report"]
                ),
                "accept_rows": len(result["accept"]),
                "dispatch_allocation_rows": len(
                    result["dispatch"]
                ),
                "accept_files": len(accept_files),
                "dispatch_files": len(dispatch_files),
            },
            "outputs": outputs,
            "preview": safe_preview(
                result["report"]
            ),
        })

    except ValueError as ex:
        logging.exception("Reconciliation input failed")

        return error_response(
            ex,
            status_code=400,
            error_type=type(ex).__name__,
        )

    except Exception as ex:
        logging.exception("Reconciliation failed")

        return error_response(
            ex,
            status_code=500,
            error_type=type(ex).__name__,
        )


# -----------------------------------------------------------------------------
# Database inspection endpoints
# -----------------------------------------------------------------------------


@app.route(
    route="batch-master",
    methods=["GET"],
)
def batch_master(
    req: func.HttpRequest,
) -> func.HttpResponse:
    try:
        frame = get_batch_master_df()

        return json_response({
            "status": "Completed",
            "application": APPLICATION_NAME,
            "version": APPLICATION_VERSION,
            "count": len(frame),
            "rows": safe_preview(
                frame,
                rows=500,
            ),
        })

    except Exception as ex:
        logging.exception("Batch Master read failed")

        return error_response(
            ex,
            status_code=500,
            error_type=type(ex).__name__,
        )


@app.route(
    route="database/status",
    methods=["GET"],
)
def database_status(
    req: func.HttpRequest,
) -> func.HttpResponse:
    try:
        return json_response({
            "status": "Completed",
            "application": APPLICATION_NAME,
            "version": APPLICATION_VERSION,
            "database": test_database_connection(),
        })

    except Exception as ex:
        logging.exception("Database status failed")

        return error_response(
            ex,
            status_code=500,
            error_type=type(ex).__name__,
        )
