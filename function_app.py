import io
import json
import logging
from pathlib import Path
from typing import Iterable, List
 
import azure.functions as func
import pandas as pd
 
from engine.alert_engine import AlertEngine
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
 
 
def read_excel(file_obj) -> pd.DataFrame:
    """Read Excel file into DataFrame."""
    try:
        if hasattr(file_obj, 'stream'):
            return pd.read_excel(file_obj.stream)
        elif hasattr(file_obj, 'read'):
            return pd.read_excel(io.BytesIO(file_obj.read()))
        else:
            return pd.read_excel(file_obj)
    except Exception as e:
        raise ValueError(f"Error reading Excel file: {str(e)}")
 
 
def json_response(data, status_code=200):
    """Return JSON response."""
    return func.HttpResponse(
        json.dumps(data),
        status_code=status_code,
        mimetype="application/json"
    )
 
 
def error_response(message, status_code=400):
    """Return error response as JSON."""
    return json_response({
        "status": "error",
        "error": message,
        "version": APPLICATION_VERSION
    }, status_code=status_code)
 
 
@app.function_name("health")
@app.route(route="health", methods=["GET"])
def health(req: func.HttpRequest) -> func.HttpResponse:
    """Health check endpoint."""
    try:
        db_status = test_database_connection()
        return json_response({
            "status": "healthy",
            "application": APPLICATION_NAME,
            "version": APPLICATION_VERSION,
            "database": db_status
        })
    except Exception as e:
        return error_response(str(e), 500)
 
 
@app.function_name("batch_master_build")
@app.route(route="batch-master/build", methods=["GET", "POST"])
def batch_master_build(req: func.HttpRequest) -> func.HttpResponse:
    """Build or rebuild the Batch Master from historical files."""
    
    if req.method == "GET":
        return json_response({
            "status": "Ready",
            "application": APPLICATION_NAME,
            "version": APPLICATION_VERSION,
            "required_files": ["asn_files", "dispatch_files", "sfda"],
            "optional_files": ["operation"]
        })
 
    try:
        # Get files
        asn_files = req.files.getlist("asn_files")
        dispatch_files = req.files.getlist("dispatch_files")
        sfda_file = req.files.get("sfda")
        operation = req.form.get("operation", "append").strip().lower()
 
        # Validate
        if not asn_files:
            return error_response("ASN files are required")
        if not dispatch_files:
            return error_response("Dispatch files are required")
        if not sfda_file:
            return error_response("SFDA file is required")
 
        # Read files
        asn_dfs = [read_excel(f) for f in asn_files]
        dispatch_dfs = [read_excel(f) for f in dispatch_files]
        sfda_df = read_excel(sfda_file)
 
        # Combine multiple files
        asn_combined = pd.concat(asn_dfs, ignore_index=True) if asn_dfs else pd.DataFrame()
        dispatch_combined = pd.concat(dispatch_dfs, ignore_index=True) if dispatch_dfs else pd.DataFrame()
 
        # Reset if rebuild
        if operation == "rebuild":
            reset_history()
 
        # Build Batch Master
        engine = FullReconciliationEngine(
            asn_df=asn_combined,
            dispatch_df=dispatch_combined,
            sfda_df=sfda_df
        )
        
        batch_master = engine.run()
 
        # Save to database
        result = replace_batch_master(batch_master)
        
        if result.get("status") != "success":
            return error_response(result.get("message", "Failed to save Batch Master"), 500)
 
        return json_response({
            "status": "Completed",
            "application": APPLICATION_NAME,
            "version": APPLICATION_VERSION,
            "operation": operation,
            "summary": {
                "batch_master_rows": len(batch_master),
                "asn_files": len(asn_files),
                "dispatch_files": len(dispatch_files),
                "rows_inserted": result.get("rows_inserted", 0)
            }
        })
 
    except Exception as e:
        logging.error(f"Error in batch_master_build: {str(e)}", exc_info=True)
        return error_response(f"Build failed: {str(e)}", 500)
 
 
@app.function_name("reconcile")
@app.route(route="reconcile", methods=["GET", "POST"])
def reconcile(req: func.HttpRequest) -> func.HttpResponse:
    """Run daily reconciliation."""
 
    if req.method == "GET":
        return json_response({
            "status": "Ready",
            "application": APPLICATION_NAME,
            "version": APPLICATION_VERSION,
            "required_files": ["inventory", "sfda"],
            "optional_files": ["asn", "dispatch"]
        })
 
    try:
        inventory_file = req.files.get("inventory")
        sfda_file = req.files.get("sfda")
        asn_file = req.files.get("asn")
        dispatch_file = req.files.get("dispatch")
 
        # Validate required files
        if not inventory_file:
            return error_response("Inventory file is required")
        if not sfda_file:
            return error_response("SFDA file is required")
 
        # Read Batch Master from database
        batch_master = get_batch_master_df()
        
        if batch_master.empty:
            return error_response("Batch Master is empty. Run Step 1 first.", 400)
 
        # Read input files
        inventory_df = read_excel(inventory_file)
        sfda_df = read_excel(sfda_file)
        dispatch_events_df = get_dispatch_events_df()
 
        # Run reconciliation
        engine = ReconciliationEngine(
            batch_master_df=batch_master,
            inventory_df=inventory_df,
            sfda_df=sfda_df,
            dispatch_events_df=dispatch_events_df,
        )
 
        result = engine.run()
 
        # Generate alerts if files provided
        alerts = None
        step_type = None
 
        if asn_file:
            step_type = "accept"
            try:
                asn_df = read_excel(asn_file)
                alert_engine = AlertEngine(
                    batch_master_df=batch_master,
                    sfda_df=sfda_df,
                    inventory_df=inventory_df,
                )
                alerts = alert_engine.generate_alerts_for_accept(asn_df)
            except Exception as e:
                logging.warning(f"Alert generation failed: {str(e)}")
 
        elif dispatch_file:
            step_type = "dispatch"
            try:
                alert_engine = AlertEngine(
                    batch_master_df=batch_master,
                    sfda_df=sfda_df,
                    inventory_df=inventory_df,
                )
                alerts = alert_engine.generate_alerts_for_dispatch(sfda_df)
            except Exception as e:
                logging.warning(f"Alert generation failed: {str(e)}")
 
        # Prepare response
        response_data = {
            "status": "Completed",
            "application": APPLICATION_NAME,
            "version": APPLICATION_VERSION,
            "summary": {
                "batch_master_rows": len(batch_master),
                "reconciliation_rows": len(result.get("report", [])),
                "accept_rows": len(result.get("accept", [])),
                "dispatch_rows": len(result.get("dispatch", [])),
            }
        }
 
        if step_type:
            response_data["step"] = step_type
 
        if alerts:
            response_data["alerts"] = alerts
 
        return json_response(response_data)
 
    except Exception as e:
        logging.error(f"Error in reconcile: {str(e)}", exc_info=True)
        return error_response(f"Reconciliation failed: {str(e)}", 500)
 
 
@app.function_name("ui")
@app.route(route="ui", methods=["GET"])
def serve_ui(req: func.HttpRequest) -> func.HttpResponse:
    """Serve the UI."""
    try:
        ui_path = Path(__file__).parent / "web" / "index.html"
        if ui_path.exists():
            with open(ui_path, "r", encoding="utf-8") as f:
                return func.HttpResponse(f.read(), mimetype="text/html")
        return error_response("UI not found", 404)
    except Exception as e:
        return error_response(str(e), 500)
 
 
@app.route(route="", methods=["GET"])
def root(req: func.HttpRequest) -> func.HttpResponse:
    """Redirect to UI."""
    return func.HttpResponse(
        '<meta http-equiv="refresh" content="0; url=/api/ui" />',
        mimetype="text/html"
    )
