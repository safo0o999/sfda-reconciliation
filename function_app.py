import io
import json
import logging
from pathlib import Path
from typing import Any, Dict

import azure.functions as func
import pandas as pd


logger = logging.getLogger("SFDA-Reconciliation")

APPLICATION_NAME = "SFDA Reconciliation"
APPLICATION_VERSION = "5.0.0"

app = func.FunctionApp(
    http_auth_level=func.AuthLevel.ANONYMOUS
)


def json_response(
    data: Dict[str, Any],
    status_code: int = 200,
) -> func.HttpResponse:
    """Return a UTF-8 JSON response."""
    return func.HttpResponse(
        body=json.dumps(
            data,
            ensure_ascii=False,
            default=str,
        ),
        status_code=status_code,
        mimetype="application/json",
        charset="utf-8",
    )


def error_response(
    message: str,
    status_code: int = 400,
    details: str = "",
) -> func.HttpResponse:
    """Return a consistent JSON error response."""
    return json_response(
        {
            "status": "Failed",
            "application": APPLICATION_NAME,
            "version": APPLICATION_VERSION,
            "error": message,
            "details": details,
        },
        status_code=status_code,
    )


def read_excel_upload(uploaded_file) -> pd.DataFrame:
    """Read an uploaded XLS/XLSX file while preserving identifiers."""
    file_name = (
        getattr(uploaded_file, "filename", None)
        or "uploaded.xlsx"
    )
    file_bytes = uploaded_file.read()

    if not file_bytes:
        raise ValueError(
            f"The uploaded file '{file_name}' is empty."
        )

    engine = (
        "xlrd"
        if file_name.lower().endswith(".xls")
        else "openpyxl"
    )

    return pd.read_excel(
        io.BytesIO(file_bytes),
        engine=engine,
        dtype=object,
    )


def count_positive_rows(
    dataframe: pd.DataFrame,
    quantity_column: str,
) -> int:
    """Count rows with a positive numeric quantity."""
    if (
        dataframe is None
        or dataframe.empty
        or quantity_column not in dataframe.columns
    ):
        return 0

    quantities = pd.to_numeric(
        dataframe[quantity_column],
        errors="coerce",
    ).fillna(0)

    return int((quantities > 0).sum())


@app.route(
    route="health",
    methods=["GET"],
)
def health(
    req: func.HttpRequest,
) -> func.HttpResponse:
    """Lightweight application health endpoint."""
    return json_response(
        {
            "status": "Healthy",
            "application": APPLICATION_NAME,
            "azure_function": "Working",
            "version": APPLICATION_VERSION,
            "timestamp": pd.Timestamp.now().isoformat(),
        }
    )


@app.route(
    route="version",
    methods=["GET"],
)
def version(
    req: func.HttpRequest,
) -> func.HttpResponse:
    """Return the deployed application version."""
    return json_response(
        {
            "status": "Success",
            "application": APPLICATION_NAME,
            "version": APPLICATION_VERSION,
        }
    )


@app.route(
    route="history",
    methods=["GET"],
)
def history(
    req: func.HttpRequest,
) -> func.HttpResponse:
    """
    Return reconciliation history when the database module supports it.

    A valid empty response is returned when history persistence has not yet
    been added to the active database module. This prevents the UI from
    receiving a 404 while keeping the limitation explicit.
    """
    raw_limit = req.params.get("limit", "100")

    try:
        limit = max(
            1,
            min(int(raw_limit), 1000),
        )
    except (TypeError, ValueError):
        return error_response(
            "The limit query parameter must be an integer.",
            400,
        )

    try:
        from engine import database

        history_reader = getattr(
            database,
            "get_reconciliation_history",
            None,
        )

        if not callable(history_reader):
            return json_response(
                {
                    "status": "Success",
                    "application": APPLICATION_NAME,
                    "version": APPLICATION_VERSION,
                    "count": 0,
                    "history": [],
                    "message": (
                        "Reconciliation history persistence is not "
                        "available in the active database module."
                    ),
                }
            )

        history_rows = history_reader(limit=limit)

        return json_response(
            {
                "status": "Success",
                "application": APPLICATION_NAME,
                "version": APPLICATION_VERSION,
                "count": len(history_rows),
                "history": history_rows,
            }
        )

    except Exception as exc:
        logger.exception(
            "Error while loading reconciliation history."
        )
        return error_response(
            "Unable to load reconciliation history.",
            500,
            str(exc),
        )


@app.route(
    route="batch-master/build",
    methods=["GET", "POST"],
)
def batch_master_build(
    req: func.HttpRequest,
) -> func.HttpResponse:
    """Build or rebuild Batch Master from historical source files."""

    if req.method == "GET":
        return json_response(
            {
                "status": "Ready",
                "application": APPLICATION_NAME,
                "version": APPLICATION_VERSION,
                "message": (
                    "POST historical ASN files, historical Dispatch "
                    "files, and the latest SFDA report."
                ),
            }
        )

    try:
        logger.info("Starting Batch Master build.")
        run_started_at = pd.Timestamp.utcnow().tz_localize(None)

        asn_files = req.files.getlist("asn_files")
        dispatch_files = req.files.getlist(
            "dispatch_files"
        )
        sfda_file = req.files.get("sfda")

        if not asn_files:
            return error_response(
                "ASN files are required.",
                400,
            )

        if not dispatch_files:
            return error_response(
                "Dispatch files are required.",
                400,
            )

        if sfda_file is None:
            return error_response(
                "SFDA file is required.",
                400,
            )

        try:
            asn_frames = [
                read_excel_upload(file)
                for file in asn_files
            ]
            dispatch_frames = [
                read_excel_upload(file)
                for file in dispatch_files
            ]
            sfda_df = read_excel_upload(sfda_file)

        except Exception as exc:
            logger.exception(
                "Failed while reading Batch Master source files."
            )
            return error_response(
                "Failed to read the uploaded Excel files.",
                400,
                str(exc),
            )

        asn_combined = pd.concat(
            asn_frames,
            ignore_index=True,
        )
        dispatch_combined = pd.concat(
            dispatch_frames,
            ignore_index=True,
        )

        try:
            from engine.database import (
                append_events,
                get_event_summaries,
                record_run_history,
                replace_batch_master,
            )
            from engine.exporter import Exporter
            from engine.full_reconciliation import (
                FullReconciliationEngine,
            )

        except ImportError as exc:
            logger.exception(
                "Unable to import Batch Master components."
            )
            return error_response(
                "Batch Master engine import failed.",
                500,
                str(exc),
            )

        try:
            engine = FullReconciliationEngine(
                asn_df=asn_combined,
                dispatch_df=dispatch_combined,
                sfda_df=sfda_df,
            )
            prepared = engine.prepare_incremental()

        except Exception as exc:
            logger.exception(
                "Batch Master preparation failed."
            )
            return error_response(
                "Failed to prepare Batch Master events.",
                500,
                str(exc),
            )

        try:
            event_result = append_events(
                prepared["receipt_records"],
                prepared["dispatch_records"],
            )
            receipt_summary, dispatch_summary = (
                get_event_summaries()
            )
            batch_master = engine.build_master_from_summaries(
                receipt_summary=receipt_summary,
                dispatch_summary=dispatch_summary,
                sfda_summary=prepared["sfda_summary"],
            )
            database_result = replace_batch_master(
                batch_master
            )

            if database_result.get("status") != "Completed":
                return error_response(
                    "Failed to save Batch Master.",
                    500,
                    database_result.get(
                        "message",
                        "Unknown database error.",
                    ),
                )

            run_summary = {
                "batch_master_rows": len(batch_master),
                "asn_files": len(asn_files),
                "dispatch_files": len(dispatch_files),
                "prepared_receipt_events": len(
                    prepared["receipt_events"]
                ),
                "prepared_dispatch_events": len(
                    prepared["dispatch_events"]
                ),
                "inserted_receipt_events": event_result[
                    "receipt_events"
                ],
                "inserted_dispatch_events": event_result[
                    "dispatch_events"
                ],
            }

        except Exception as exc:
            logger.exception(
                "Batch Master database save failed."
            )
            return error_response(
                "Database operation failed.",
                500,
                str(exc),
            )

        try:
            batch_master_file = (
                Exporter.build_formatted_excel_file(
                    df=batch_master,
                    file_name="Batch_Master.xlsx",
                    sheet_name="Batch Master",
                    title="SFDA Batch Master",
                    sort_columns=[
                        "Generic Item Number",
                        "BN",
                        "Expiry Date",
                    ],
                )
            )
        except Exception as exc:
            logger.exception(
                "Batch Master Excel export failed."
            )
            return error_response(
                "Batch Master was saved, but its Excel "
                "output could not be generated.",
                500,
                str(exc),
            )

        try:
            record_run_history(
                run_type="Batch Master Build",
                status="Completed",
                started_at=run_started_at,
                completed_at=(
                    pd.Timestamp.utcnow().tz_localize(None)
                ),
                summary=run_summary,
            )
        except Exception as exc:
            logger.exception(
                "Batch Master history recording failed."
            )
            return error_response(
                "Batch Master completed, but Run History "
                "could not be recorded.",
                500,
                str(exc),
            )

        return json_response(
            {
                "status": "Completed",
                "application": APPLICATION_NAME,
                "version": APPLICATION_VERSION,
                "summary": {
                    **run_summary,
                    "rows_inserted": database_result.get(
                        "rows_inserted",
                        len(batch_master),
                    ),
                },
                "outputs": {
                    "batch_master": batch_master_file,
                },
            }
        )

    except Exception as exc:
        logger.exception(
            "Unexpected Batch Master endpoint error."
        )
        return error_response(
            "Unexpected Batch Master error.",
            500,
            str(exc),
        )


@app.route(
    route="reconcile",
    methods=["GET", "POST"],
)
def reconcile(
    req: func.HttpRequest,
) -> func.HttpResponse:
    """Run Version 5 reconciliation and return downloadable outputs."""

    if req.method == "GET":
        return json_response(
            {
                "status": "Ready",
                "application": APPLICATION_NAME,
                "version": APPLICATION_VERSION,
                "message": (
                    "POST the latest Inventory and SFDA reports "
                    "to run reconciliation."
                ),
            }
        )

    try:
        logger.info("Starting reconciliation.")

        inventory_file = req.files.get("inventory")
        sfda_file = req.files.get("sfda")

        if inventory_file is None:
            return error_response(
                "Inventory file is required.",
                400,
            )

        if sfda_file is None:
            return error_response(
                "SFDA file is required.",
                400,
            )

        try:
            inventory_df = read_excel_upload(
                inventory_file
            )
            sfda_df = read_excel_upload(sfda_file)

        except Exception as exc:
            logger.exception(
                "Failed while reading reconciliation files."
            )
            return error_response(
                "Failed to read the uploaded Excel files.",
                400,
                str(exc),
            )

        try:
            from engine.database import (
                get_batch_master_df,
                get_dispatch_events_df,
            )
            from engine.exporter import Exporter
            from engine.reconciliation import (
                ReconciliationEngine,
            )

        except ImportError as exc:
            logger.exception(
                "Unable to import reconciliation components."
            )
            return error_response(
                "Reconciliation engine import failed.",
                500,
                str(exc),
            )

        try:
            batch_master = get_batch_master_df()

            if batch_master.empty:
                return error_response(
                    "Batch Master is empty.",
                    400,
                    (
                        "Complete Step 1 — Build Batch Master "
                        "before running reconciliation."
                    ),
                )

            dispatch_events = get_dispatch_events_df()

        except Exception as exc:
            logger.exception(
                "Failed to load Batch Master history."
            )
            return error_response(
                "Failed to read reconciliation history "
                "from the database.",
                500,
                str(exc),
            )

        try:
            engine = ReconciliationEngine(
                batch_master_df=batch_master,
                inventory_df=inventory_df,
                sfda_df=sfda_df,
                dispatch_events_df=dispatch_events,
            )
            result = engine.run()

            report_df = result.get(
                "report",
                pd.DataFrame(),
            )
            accept_df = result.get(
                "accept",
                pd.DataFrame(),
            )
            dispatch_df = result.get(
                "dispatch",
                pd.DataFrame(),
            )

        except Exception as exc:
            logger.exception(
                "Reconciliation calculation failed."
            )
            return error_response(
                "Reconciliation failed.",
                500,
                str(exc),
            )

        try:
            reconciliation_report = (
                Exporter.build_formatted_excel_file(
                    df=report_df,
                    file_name=(
                        "SFDA_Reconciliation_Report.xlsx"
                    ),
                    sheet_name="Reconciliation",
                    title=(
                        "SFDA Reconciliation Report"
                    ),
                    sort_columns=[
                        "Generic No",
                        "Batch No",
                        "Expiry Date",
                    ],
                )
            )

            accept_files = (
                Exporter.build_sfda_upload_files(
                    df=accept_df,
                    quantity_column="To Be Accept",
                    file_prefix="SFDA_Accept",
                )
            )

            dispatch_files = (
                Exporter
                .build_dispatch_files_by_customer(
                    dispatch_df
                )
            )

        except Exception as exc:
            logger.exception(
                "Reconciliation output generation failed."
            )
            return error_response(
                "Reconciliation calculations completed, "
                "but output generation failed.",
                500,
                str(exc),
            )

        accept_rows = count_positive_rows(
            accept_df,
            "To Be Accept",
        )
        dispatch_rows = count_positive_rows(
            dispatch_df,
            "Allocated To Be Dispatch",
        )

        generated_files = (
            len(reconciliation_report)
            + len(accept_files)
            + len(dispatch_files)
        )

        return json_response(
            {
                "status": "Completed",
                "application": APPLICATION_NAME,
                "version": APPLICATION_VERSION,
                "summary": {
                    "files_count": 2,
                    "total_input_rows": (
                        len(inventory_df)
                        + len(sfda_df)
                    ),
                    "batch_master_rows": len(
                        batch_master
                    ),
                    "master_rows": len(
                        batch_master
                    ),
                    "reconciliation_rows": len(
                        report_df
                    ),
                    "accept_rows": accept_rows,
                    "dispatch_rows": dispatch_rows,
                    "dispatch_allocation_rows": (
                        dispatch_rows
                    ),
                    "variance_rows": 0,
                    "accept_files": len(
                        accept_files
                    ),
                    "dispatch_files": len(
                        dispatch_files
                    ),
                    "generated_files": generated_files,
                },
                "outputs": {
                    "reconciliation_report": (
                        reconciliation_report
                    ),
                    "accept_files": accept_files,
                    "dispatch_files": dispatch_files,
                },
            }
        )

    except Exception as exc:
        logger.exception(
            "Unexpected reconciliation endpoint error."
        )
        return error_response(
            "Unexpected reconciliation error.",
            500,
            str(exc),
        )


@app.route(
    route="ui",
    methods=["GET"],
)
def serve_ui(
    req: func.HttpRequest,
) -> func.HttpResponse:
    """Serve web/index.html."""
    try:
        ui_path = (
            Path(__file__).parent
            / "web"
            / "index.html"
        )

        if not ui_path.exists():
            return error_response(
                "UI file was not found.",
                404,
                str(ui_path),
            )

        return func.HttpResponse(
            body=ui_path.read_text(
                encoding="utf-8"
            ),
            status_code=200,
            mimetype="text/html",
            charset="utf-8",
        )

    except Exception as exc:
        logger.exception(
            "Unable to serve the web interface."
        )
        return error_response(
            "Failed to serve the UI.",
            500,
            str(exc),
        )


@app.route(
    route="",
    methods=["GET"],
)
def root(
    req: func.HttpRequest,
) -> func.HttpResponse:
    """Redirect the root endpoint to the web interface."""
    return func.HttpResponse(
        body=(
            '<meta http-equiv="refresh" '
            'content="0; url=/api/ui" />'
        ),
        status_code=200,
        mimetype="text/html",
        charset="utf-8",
    )
