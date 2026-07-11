import io
import json

import azure.functions as func
import pandas as pd

app = func.FunctionApp(http_auth_level=func.AuthLevel.ANONYMOUS)


def read_excel_file(uploaded_file):
    file_name = uploaded_file.filename or "uploaded.xlsx"
    file_bytes = uploaded_file.stream.read()

    if file_name.lower().endswith(".xls"):
        engine = "xlrd"
    else:
        engine = "openpyxl"

    return pd.read_excel(
        io.BytesIO(file_bytes),
        engine=engine,
        dtype=object
    )


@app.route(route="health", methods=["GET"])
def health(req: func.HttpRequest) -> func.HttpResponse:
    return func.HttpResponse(
        "SFDA Reconciliation API is Running",
        status_code=200
    )


@app.route(route="version", methods=["GET"])
def version(req: func.HttpRequest) -> func.HttpResponse:
    return func.HttpResponse(
        "Version 1.0.0",
        status_code=200
    )


@app.route(route="process", methods=["POST"])
def process(req: func.HttpRequest) -> func.HttpResponse:
    try:
        asn_file = req.files["asn"]
        inventory_file = req.files["inventory"]
        dispatch_file = req.files["dispatch"]
        sfda_file = req.files["sfda"]
        packsize_file = req.files["packsize"]

        asn_df = read_excel_file(asn_file)
        inventory_df = read_excel_file(inventory_file)
        dispatch_df = read_excel_file(dispatch_file)
        sfda_df = read_excel_file(sfda_file)
        packsize_df = read_excel_file(packsize_file)

        result = {
            "status": "Files Read Successfully",
            "files": {
                "asn": {
                    "name": asn_file.filename,
                    "rows": int(len(asn_df)),
                    "columns": int(len(asn_df.columns))
                },
                "inventory": {
                    "name": inventory_file.filename,
                    "rows": int(len(inventory_df)),
                    "columns": int(len(inventory_df.columns))
                },
                "dispatch": {
                    "name": dispatch_file.filename,
                    "rows": int(len(dispatch_df)),
                    "columns": int(len(dispatch_df.columns))
                },
                "sfda": {
                    "name": sfda_file.filename,
                    "rows": int(len(sfda_df)),
                    "columns": int(len(sfda_df.columns))
                },
                "packsize": {
                    "name": packsize_file.filename,
                    "rows": int(len(packsize_df)),
                    "columns": int(len(packsize_df.columns))
                }
            }
        }

        return func.HttpResponse(
            json.dumps(result),
            status_code=200,
            mimetype="application/json"
        )

    except Exception as ex:
        return func.HttpResponse(
            json.dumps({
                "status": "Failed",
                "error": str(ex)
            }),
            status_code=500,
            mimetype="application/json"
        )
