import io
import json
import logging
import traceback

import azure.functions as func


app = func.FunctionApp(
    http_auth_level=func.AuthLevel.ANONYMOUS
)


REQUIRED_FILES = [
    "asn",
    "inventory",
    "dispatch",
    "sfda",
    "packsize",
]


def json_response(
    data: dict,
    status_code: int = 200
) -> func.HttpResponse:

    return func.HttpResponse(
        body=json.dumps(
            data,
            ensure_ascii=False,
            default=str
        ),
        status_code=status_code,
        mimetype="application/json"
    )


def read_excel_file(uploaded_file):

    # Import داخل الدالة حتى تعمل health حتى لو حصلت
    # مشكلة مؤقتة في تحميل pandas.
    import pandas as pd

    file_name = uploaded_file.filename or "uploaded.xlsx"
    extension = file_name.lower().rsplit(".", 1)[-1]

    if extension not in ["xls", "xlsx"]:
        raise ValueError(
            f"Unsupported file type: {file_name}"
        )

    file_bytes = uploaded_file.read()

    if not file_bytes:
        raise ValueError(
            f"Uploaded file is empty: {file_name}"
        )

    engine = "xlrd" if extension == "xls" else "openpyxl"

    dataframe = pd.read_excel(
        io.BytesIO(file_bytes),
        engine=engine,
        dtype=object
    )

    return dataframe


@app.route(
    route="health",
    methods=["GET"]
)
def health(
    req: func.HttpRequest
) -> func.HttpResponse:

    return json_response({
        "status": "Healthy",
        "application": "SFDA Reconciliation",
        "azure_function": "Working",
        "version": "1.1.0"
    })


@app.route(
    route="version",
    methods=["GET"]
)
def version(
    req: func.HttpRequest
) -> func.HttpResponse:

    return json_response({
        "application": "SFDA Reconciliation",
        "version": "1.1.0"
    })


@app.route(
    route="process",
    methods=["GET", "POST"]
)
def process(
    req: func.HttpRequest
) -> func.HttpResponse:

    if req.method == "GET":
        return json_response({
            "status": "Ready",
            "message": "Use POST with the five required files.",
            "required_files": REQUIRED_FILES
        })

    try:
        missing_files = [
            file_key
            for file_key in REQUIRED_FILES
            if file_key not in req.files
        ]

        if missing_files:
            return json_response(
                {
                    "status": "Failed",
                    "message": "Required files are missing.",
                    "missing_files": missing_files
                },
                status_code=400
            )

        uploaded_files = {
            file_key: req.files[file_key]
            for file_key in REQUIRED_FILES
        }

        dataframes = {
            file_key: read_excel_file(uploaded_file)
            for file_key, uploaded_file in uploaded_files.items()
        }

        files_summary = {}

        for file_key in REQUIRED_FILES:

            dataframe = dataframes[file_key]
            uploaded_file = uploaded_files[file_key]

            files_summary[file_key] = {
                "name": uploaded_file.filename,
                "rows": int(len(dataframe)),
                "columns": int(len(dataframe.columns)),
                "headers": [
                    str(column)
                    for column in dataframe.columns
                ]
            }

        total_rows = sum(
            file_info["rows"]
            for file_info in files_summary.values()
        )

        return json_response({
            "status": "Files Read Successfully",
            "application": "SFDA Reconciliation",
            "version": "1.1.0",
            "summary": {
                "files_count": len(files_summary),
                "total_rows": total_rows
            },
            "files": files_summary
        })

    except Exception as ex:

        logging.exception(
            "Error while processing uploaded files."
        )

        return json_response(
            {
                "status": "Failed",
                "error": str(ex),
                "error_type": type(ex).__name__,
                "trace": traceback.format_exc()
            },
            status_code=500
        )
