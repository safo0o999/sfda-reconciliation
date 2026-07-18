import base64
import io
import json
import logging
from pathlib import Path

import azure.functions as func
import pandas as pd

from engine.database import (
    append_events,
    get_batch_master_df,
    get_dispatch_events_df,
    get_event_summaries,
    initialize_database,
    replace_batch_master,
    test_database_connection,
)
from engine.exporter import Exporter
from engine.full_reconciliation import FullReconciliationEngine
from engine.reconciliation import ReconciliationEngine


app = func.FunctionApp(http_auth_level=func.AuthLevel.ANONYMOUS)
APPLICATION_NAME = "SFDA Reconciliation"
APPLICATION_VERSION = "5.0.0"


def json_response(data, status_code=200):
    return func.HttpResponse(
        json.dumps(data, ensure_ascii=False, default=str),
        status_code=status_code,
        mimetype="application/json",
        charset="utf-8",
    )


def read_excel(uploaded):
    name = uploaded.filename or "uploaded.xlsx"
    raw = uploaded.read()
    if not raw:
        raise ValueError(f"Uploaded file is empty: {name}")
    engine = "xlrd" if name.lower().endswith(".xls") else "openpyxl"
    return pd.read_excel(io.BytesIO(raw), engine=engine, dtype=object)


def read_many(files):
    frames = []
    for uploaded in files:
        frame = read_excel(uploaded)
        frame["_Source File"] = uploaded.filename or "uploaded.xlsx"
        frames.append(frame)
    return pd.concat(frames, ignore_index=True, sort=False) if frames else pd.DataFrame()


def excel_payload(df, file_name, title):
    return Exporter.build_formatted_excel_file(
        df=df,
        file_name=file_name,
        sheet_name=title[:31],
        title=title,
    )


@app.route(route="", methods=["GET"])
def home(req):
    return func.HttpResponse(status_code=302, headers={"Location": "/api/ui"})


@app.route(route="ui", methods=["GET"])
def ui(req):
    path = Path(__file__).resolve().parent / "web" / "index.html"
    return func.HttpResponse(path.read_text(encoding="utf-8"), mimetype="text/html", charset="utf-8")


@app.route(route="health", methods=["GET"])
def health(req):
    try:
        return json_response({
            "status": "Healthy",
            "application": APPLICATION_NAME,
            "version": APPLICATION_VERSION,
            "database": test_database_connection(),
        })
    except Exception as ex:
        return json_response({"status":"Failed", "version":APPLICATION_VERSION, "error":str(ex)}, 500)


@app.route(route="batch-master/build", methods=["GET", "POST"])
def build_batch_master(req):
    if req.method == "GET":
        return json_response({
            "status":"Ready", "version":APPLICATION_VERSION,
            "required_files":["asn_files", "dispatch_files", "sfda"],
            "operations":["append", "rebuild"],
        })
    try:
        asn_files = list(req.files.getlist("asn_files")) or list(req.files.getlist("asn"))
        dispatch_files = list(req.files.getlist("dispatch_files")) or list(req.files.getlist("dispatch"))
        sfda_file = req.files.get("sfda")
        if not asn_files or not dispatch_files or sfda_file is None:
            raise ValueError("ASN Receipt, Full Dispatch, and latest SFDA Drug report are required.")

        operation = str(req.form.get("operation") or "append").lower().strip()
        engine = FullReconciliationEngine(read_many(asn_files), read_many(dispatch_files), read_excel(sfda_file))
        prepared = engine.prepare_incremental()

        initialize_database()
        if operation == "rebuild":
            from engine.database import Database
            with Database().connect() as connection:
                connection.cursor().execute("DELETE FROM dbo.ReceiptEvents; DELETE FROM dbo.DispatchEvents; DELETE FROM dbo.BatchMaster;")
                connection.commit()
        elif operation != "append":
            raise ValueError("operation must be append or rebuild.")

        saved = append_events(prepared["receipt_records"], prepared["dispatch_records"])
        receipt_summary, dispatch_summary = get_event_summaries()
        master = engine.build_master_from_summaries(receipt_summary, dispatch_summary, prepared["sfda_summary"])
        replace_batch_master(master)

        return json_response({
            "status":"Completed", "version":APPLICATION_VERSION, "operation":operation,
            "summary":{
                "uploaded_asn_files":len(asn_files), "uploaded_dispatch_files":len(dispatch_files),
                "new_receipt_events":saved["receipt_events"], "new_dispatch_events":saved["dispatch_events"],
                "batch_master_rows":len(master),
            },
            "outputs":excel_payload(master, "Batch_Master.xlsx", "Batch Master"),
            "preview":master.head(100).where(pd.notna(master.head(100)), None).to_dict(orient="records"),
        })
    except Exception as ex:
        logging.exception("Batch Master build failed")
        return json_response({"status":"Failed", "error":str(ex), "error_type":type(ex).__name__}, 500)


@app.route(route="reconcile", methods=["GET", "POST"])
def reconcile(req):
    if req.method == "GET":
        return json_response({"status":"Ready", "version":APPLICATION_VERSION, "required_files":["inventory", "sfda"]})
    try:
        inventory = req.files.get("inventory")
        sfda = req.files.get("sfda")
        if inventory is None or sfda is None:
            raise ValueError("Latest Inventory and latest SFDA Drug report are required.")

        master = get_batch_master_df()
        if master.empty:
            raise ValueError("Batch Master is empty. Complete Step 1 first.")
        engine = ReconciliationEngine(master, read_excel(inventory), read_excel(sfda), get_dispatch_events_df())
        result = engine.run()

        accept_files = Exporter.build_sfda_upload_files(result["accept"], "To Be Accept", "Accept")
        dispatch_files = Exporter.build_dispatch_files_by_customer(result["dispatch"])
        return json_response({
            "status":"Completed", "version":APPLICATION_VERSION,
            "summary":{
                "reconciliation_rows":len(result["report"]),
                "accept_rows":len(result["accept"]),
                "dispatch_rows":len(result["dispatch"]),
                "accept_files":len(accept_files), "dispatch_files":len(dispatch_files),
            },
            "outputs":{
                "reconciliation_report":excel_payload(result["report"], "Reconciliation_Report.xlsx", "Reconciliation Report"),
                "accept_files":accept_files,
                "dispatch_files":dispatch_files,
            },
            "preview":result["report"].head(100).where(pd.notna(result["report"].head(100)), None).to_dict(orient="records"),
        })
    except Exception as ex:
        logging.exception("Reconciliation failed")
        return json_response({"status":"Failed", "error":str(ex), "error_type":type(ex).__name__}, 500)


@app.route(route="batch-master", methods=["GET"])
def batch_master(req):
    try:
        frame = get_batch_master_df()
        return json_response({"status":"Completed", "count":len(frame), "rows":frame.where(pd.notna(frame), None).to_dict(orient="records")})
    except Exception as ex:
        return json_response({"status":"Failed", "error":str(ex)}, 500)
