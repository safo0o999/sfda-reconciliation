import io
import json
import logging
from pathlib import Path
from typing import List
 
import azure.functions as func
import pandas as pd
 
# Initialize logger
logger = logging.getLogger("SFDA-Reconciliation")
 
APPLICATION_NAME = "SFDA Reconciliation"
APPLICATION_VERSION = "5.0.0"
 
 
def json_response(data, status_code=200):
    """Return JSON response."""
    return func.HttpResponse(
        json.dumps(data),
        status_code=status_code,
        mimetype="application/json"
    )
 
 
def error_response(message, status_code=400, details=""):
    """Return error response as JSON."""
    return json_response({
        "status": "error",
        "error": message,
        "details": details,
        "version": APPLICATION_VERSION
    }, status_code=status_code)
 
 
app = func.FunctionApp(http_auth_level=func.AuthLevel.ANONYMOUS)
 
 
@app.route(route="health", methods=["GET"])
def health(req: func.HttpRequest) -> func.HttpResponse:
    """Health check endpoint."""
    try:
        return json_response({
            "status": "healthy",
            "application": APPLICATION_NAME,
            "version": APPLICATION_VERSION,
            "timestamp": pd.Timestamp.now().isoformat()
        })
    except Exception as e:
        logger.error(f"Health check error: {str(e)}")
        return error_response("Health check failed", 500, str(e))
 
 
@app.route(route="batch-master/build", methods=["GET", "POST"])
def batch_master_build(req: func.HttpRequest) -> func.HttpResponse:
    """Build or rebuild the Batch Master from historical files."""
    
    if req.method == "GET":
        return json_response({
            "status": "Ready",
            "application": APPLICATION_NAME,
            "version": APPLICATION_VERSION,
            "message": "POST files to build Batch Master"
        })
 
    try:
        logger.info("Starting batch master build...")
        
        # Get files
        asn_files = req.files.getlist("asn_files")
        dispatch_files = req.files.getlist("dispatch_files")
        sfda_file = req.files.get("sfda")
 
        # Validate files exist
        if not asn_files:
            return error_response("ASN files are required", 400)
        if not dispatch_files:
            return error_response("Dispatch files are required", 400)
        if not sfda_file:
            return error_response("SFDA file is required", 400)
 
        logger.info(f"Files received - ASN: {len(asn_files)}, Dispatch: {len(dispatch_files)}, SFDA: 1")
 
        # Try to read files
        try:
            asn_dfs = []
            for f in asn_files:
                df = pd.read_excel(io.BytesIO(f.read()))
                asn_dfs.append(df)
                logger.info(f"Read ASN file with {len(df)} rows")
            
            dispatch_dfs = []
            for f in dispatch_files:
                df = pd.read_excel(io.BytesIO(f.read()))
                dispatch_dfs.append(df)
                logger.info(f"Read Dispatch file with {len(df)} rows")
            
            sfda_df = pd.read_excel(io.BytesIO(sfda_file.read()))
            logger.info(f"Read SFDA file with {len(sfda_df)} rows")
 
        except Exception as e:
            logger.error(f"File reading error: {str(e)}")
            return error_response("Failed to read Excel files", 400, str(e))
 
        # Combine files
        asn_combined = pd.concat(asn_dfs, ignore_index=True) if asn_dfs else pd.DataFrame()
        dispatch_combined = pd.concat(dispatch_dfs, ignore_index=True) if dispatch_dfs else pd.DataFrame()
 
        logger.info(f"Combined ASN: {len(asn_combined)} rows, Dispatch: {len(dispatch_combined)} rows")
 
        # Import engine (delayed to avoid import errors at startup)
        try:
            from engine.full_reconciliation import FullReconciliationEngine
            from engine.database import replace_batch_master
        except ImportError as e:
            logger.error(f"Import error: {str(e)}")
            return error_response("Engine import failed", 500, str(e))
 
        # Build Batch Master
        try:
            engine = FullReconciliationEngine(
                asn_df=asn_combined,
                dispatch_df=dispatch_combined,
                sfda_df=sfda_df
            )
            batch_master = engine.run()
            logger.info(f"Batch Master built with {len(batch_master)} rows")
        except Exception as e:
            logger.error(f"Build error: {str(e)}")
            return error_response("Failed to build Batch Master", 500, str(e))
 
        # Save to database
        try:
            result = replace_batch_master(batch_master)
            logger.info(f"Database save result: {result}")
            
            if result.get("status") != "success":
                return error_response(
                    "Failed to save Batch Master",
                    500,
                    result.get("message", "Unknown error")
                )
        except Exception as e:
            logger.error(f"Database save error: {str(e)}")
            return error_response("Database operation failed", 500, str(e))
 
        # Success
        return json_response({
            "status": "Completed",
            "application": APPLICATION_NAME,
            "version": APPLICATION_VERSION,
            "summary": {
                "batch_master_rows": len(batch_master),
                "asn_files": len(asn_files),
                "dispatch_files": len(dispatch_files),
                "rows_inserted": result.get("rows_inserted", 0)
            }
        })
 
    except Exception as e:
        logger.error(f"Unexpected error in batch_master_build: {str(e)}", exc_info=True)
        return error_response("Unexpected error", 500, str(e))
 
 
@app.route(route="reconcile", methods=["GET", "POST"])
def reconcile(req: func.HttpRequest) -> func.HttpResponse:
    """Run daily reconciliation."""
 
    if req.method == "GET":
        return json_response({
            "status": "Ready",
            "application": APPLICATION_NAME,
            "version": APPLICATION_VERSION,
            "message": "POST files to run reconciliation"
        })
 
    try:
        logger.info("Starting reconciliation...")
        
        inventory_file = req.files.get("inventory")
        sfda_file = req.files.get("sfda")
 
        # Validate
        if not inventory_file:
            return error_response("Inventory file is required", 400)
        if not sfda_file:
            return error_response("SFDA file is required", 400)
 
        # Read files
        try:
            inventory_df = pd.read_excel(io.BytesIO(inventory_file.read()))
            sfda_df = pd.read_excel(io.BytesIO(sfda_file.read()))
            logger.info(f"Files read - Inventory: {len(inventory_df)}, SFDA: {len(sfda_df)}")
        except Exception as e:
            logger.error(f"File reading error: {str(e)}")
            return error_response("Failed to read Excel files", 400, str(e))
 
        # Import engine
        try:
            from engine.database import get_batch_master_df, get_dispatch_events_df
            from engine.reconciliation import ReconciliationEngine
        except ImportError as e:
            logger.error(f"Import error: {str(e)}")
            return error_response("Engine import failed", 500, str(e))
 
        # Get Batch Master from database
        try:
            batch_master = get_batch_master_df()
            if batch_master.empty:
                return error_response(
                    "Batch Master is empty",
                    400,
                    "Run Step 1 (Build Batch Master) first"
                )
            
            dispatch_events = get_dispatch_events_df()
            logger.info(f"Batch Master: {len(batch_master)}, Dispatch Events: {len(dispatch_events)}")
        except Exception as e:
            logger.error(f"Database read error: {str(e)}")
            return error_response("Failed to read from database", 500, str(e))
 
        # Run reconciliation
        try:
            engine = ReconciliationEngine(
                batch_master_df=batch_master,
                inventory_df=inventory_df,
                sfda_df=sfda_df,
                dispatch_events_df=dispatch_events,
            )
            result = engine.run()
            logger.info(f"Reconciliation completed")
        except Exception as e:
            logger.error(f"Reconciliation error: {str(e)}")
            return error_response("Reconciliation failed", 500, str(e))
 
        # Return success
        return json_response({
            "status": "Completed",
            "application": APPLICATION_NAME,
            "version": APPLICATION_VERSION,
            "summary": {
                "batch_master_rows": len(batch_master),
                "reconciliation_rows": len(result.get("report", [])),
                "accept_rows": len(result.get("accept", [])),
                "dispatch_rows": len(result.get("dispatch", [])),
            }
        })
 
    except Exception as e:
        logger.error(f"Unexpected error in reconcile: {str(e)}", exc_info=True)
        return error_response("Unexpected error", 500, str(e))
 
 
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
        logger.error(f"UI serving error: {str(e)}")
        return error_response("Failed to serve UI", 500, str(e))
 
 
@app.route(route="", methods=["GET"])
def root(req: func.HttpRequest) -> func.HttpResponse:
    """Redirect to UI."""
    return func.HttpResponse(
        '<meta http-equiv="refresh" content="0; url=/api/ui" />',
        mimetype="text/html"
    )
