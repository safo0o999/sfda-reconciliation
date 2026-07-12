import io
import json
import logging
import traceback
from pathlib import Path

import azure.functions as func

from engine.exporter import Exporter
from engine.reconciliation import ReconciliationEngine


app = func.FunctionApp(
    http_auth_level=func.AuthLevel.ANONYMOUS
)


APPLICATION_NAME = "SFDA Reconciliation"
APPLICATION_VERSION = "1.7.0"

REQUIRED_FILES = [
    "asn",
    "inventory",
    "dispatch",
    "sfda",
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
        mimetype="application/json",
        charset="utf-8"
    )


def read_excel_file(uploaded_file):

    import pandas as pd

    file_name = (
        uploaded_file.filename
        or "uploaded.xlsx"
    )

    if "." not in file_name:
        raise ValueError(
            f"File extension is missing: {file_name}"
        )

    extension = (
        file_name
        .lower()
        .rsplit(".", 1)[-1]
    )

    if extension not in [
        "xls",
        "xlsx"
    ]:
        raise ValueError(
            f"Unsupported file type: {file_name}"
        )

    file_bytes = uploaded_file.read()

    if not file_bytes:
        raise ValueError(
            f"Uploaded file is empty: {file_name}"
        )

    excel_engine = (
        "xlrd"
        if extension == "xls"
        else "openpyxl"
    )

    return pd.read_excel(
        io.BytesIO(file_bytes),
        engine=excel_engine,
        dtype=object
    )


def build_accept_details(
    reconciliation_engine,
    master
):

    keys = [
        "BN",
        "Expiry Date"
    ]

    accept_lookup = (
        master[
            master["To Be Accept"] > 0
        ][
            [
                "GTIN",
                "Drug Name",
                "BN",
                "Expiry Date",
                "PackageSize",
                "To Be Accept"
            ]
        ]
        .drop_duplicates(
            subset=keys,
            keep="first"
        )
        .copy()
    )

    asn_summary = (
        reconciliation_engine.asn[
            reconciliation_engine.asn[
                "Received Quantity"
            ] > 0
        ]
        .groupby(
            keys,
            as_index=False,
            dropna=False
        )
        .agg(
            {
                "Trade Name": "first",
                "Received Quantity": "sum"
            }
        )
    )

    accept_details = (
        accept_lookup
        .merge(
            asn_summary,
            on=keys,
            how="left"
        )
    )

    accept_details[
        "Received Quantity"
    ] = (
        accept_details[
            "Received Quantity"
        ]
        .fillna(0)
    )

    accept_details[
        "PackageSize"
    ] = (
        accept_details[
            "PackageSize"
        ]
        .fillna(0)
    )

    accept_details[
        "To Be Accept"
    ] = (
        accept_details[
            "To Be Accept"
        ]
        .fillna(0)
        .astype(int)
    )

    return Exporter.build_formatted_excel_file(
        df=accept_details,
        file_name="Accept_Details.xlsx",
        sheet_name="Accept Details",
        title="SFDA Accept Details",
        columns=[
            "GTIN",
            "Drug Name",
            "BN",
            "Expiry Date",
            "Trade Name",
            "Received Quantity",
            "PackageSize",
            "To Be Accept"
        ],
        sort_columns=[
            "GTIN",
            "BN",
            "Expiry Date"
        ]
    )


def build_dispatch_details(
    reconciliation_engine,
    master
):

    keys = [
        "BN",
        "Expiry Date"
    ]

    dispatch_lookup = (
        master[
            master["To Be Dispatch"] > 0
        ][
            [
                "BN",
                "Expiry Date",
                "GTIN",
                "Drug Name",
                "PackageSize",
                "To Be Dispatch"
            ]
        ]
        .drop_duplicates(
            subset=keys,
            keep="first"
        )
        .copy()
    )

    dispatch_details = (
        reconciliation_engine.dispatch
        .merge(
            dispatch_lookup,
            on=keys,
            how="inner"
        )
    )

    dispatch_details = (
        dispatch_details[
            dispatch_details[
                "Dispatched Quantity"
            ] > 0
        ]
        .copy()
    )

    dispatch_details[
        "Dispatched Quantity"
    ] = (
        dispatch_details[
            "Dispatched Quantity"
        ]
        .fillna(0)
    )

    dispatch_details[
        "PackageSize"
    ] = (
        dispatch_details[
            "PackageSize"
        ]
        .fillna(0)
    )

    dispatch_details[
        "To Be Dispatch"
    ] = (
        dispatch_details[
            "To Be Dispatch"
        ]
        .fillna(0)
        .astype(int)
    )

    return Exporter.build_formatted_excel_file(
        df=dispatch_details,
        file_name="Dispatch_Details.xlsx",
        sheet_name="Dispatch Details",
        title="SFDA Dispatch Details",
        columns=[
            "To Address",
            "Sales Order Number",
            "Order Line",
            "Trade Item Number",
            "GTIN",
            "Drug Name",
            "BN",
            "Expiry Date",
            "Trade Name",
            "Dispatched Quantity",
            "PackageSize",
            "To Be Dispatch"
        ],
        sort_columns=[
            "To Address",
            "Sales Order Number",
            "GTIN",
            "BN",
            "Expiry Date"
        ]
    )


def build_variance_report(
    variance
):

    return Exporter.build_formatted_excel_file(
        df=variance,
        file_name="Variance_Report.xlsx",
        sheet_name="Variance Report",
        title="SFDA Variance Report",
        sort_columns=[
            "GTIN",
            "BN",
            "Expiry Date"
        ]
    )


@app.route(
    route="",
    methods=["GET"]
)
def home(
    req: func.HttpRequest
) -> func.HttpResponse:

    return func.HttpResponse(
        status_code=302,
        headers={
            "Location": "/api/ui"
        }
    )


@app.route(
    route="ui",
    methods=["GET"]
)
def ui(
    req: func.HttpRequest
) -> func.HttpResponse:

    try:
        html_path = (
            Path(__file__)
            .resolve()
            .parent
            / "web"
            / "index.html"
        )

        if not html_path.exists():
            return func.HttpResponse(
                body="UI file was not found.",
                status_code=404,
                mimetype="text/plain",
                charset="utf-8"
            )

        html_content = (
            html_path
            .read_text(
                encoding="utf-8"
            )
        )

        return func.HttpResponse(
            body=html_content,
            status_code=200,
            mimetype="text/html",
            charset="utf-8"
        )

    except Exception as ex:

        logging.exception(
            "Error while loading the user interface."
        )

        return func.HttpResponse(
            body=(
                "Unable to load UI: "
                f"{str(ex)}"
            ),
            status_code=500,
            mimetype="text/plain",
            charset="utf-8"
        )


@app.route(
    route="health",
    methods=["GET"]
)
def health(
    req: func.HttpRequest
) -> func.HttpResponse:

    return json_response({
        "status": "Healthy",
        "application": APPLICATION_NAME,
        "azure_function": "Working",
        "version": APPLICATION_VERSION
    })


@app.route(
    route="version",
    methods=["GET"]
)
def version(
    req: func.HttpRequest
) -> func.HttpResponse:

    return json_response({
        "application": APPLICATION_NAME,
        "version": APPLICATION_VERSION
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
            "message": (
                "Use POST with the five "
                "required files."
            ),
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
                    "message": (
                        "Required files are missing."
                    ),
                    "missing_files": missing_files
                },
                status_code=400
            )

        uploaded_files = {
            file_key: req.files[file_key]
            for file_key in REQUIRED_FILES
        }

        dataframes = {
            file_key: read_excel_file(
                uploaded_file
            )
            for file_key, uploaded_file
            in uploaded_files.items()
        }

        reconciliation_engine = ReconciliationEngine(
    asn_df=dataframes["asn"],
    inventory_df=dataframes["inventory"],
    dispatch_df=dataframes["dispatch"],
    sfda_df=dataframes["sfda"]
)

        result = (
            reconciliation_engine.run()
        )

        master = result["master"]
        accept = result["accept"]
        dispatch_output = result["dispatch"]
        variance = result["variance"]

        accept_files = (
            Exporter.build_sfda_upload_files(
                df=accept,
                quantity_column="To Be Accept",
                file_prefix="Accept"
            )
        )

        dispatch_files = (
    Exporter.build_dispatch_files_by_customer(
        dispatch_df=dispatch_output
    )
)

        accept_details = (
            build_accept_details(
                reconciliation_engine=(
                    reconciliation_engine
                ),
                master=master
            )
        )

        dispatch_details = (
            build_dispatch_details(
                reconciliation_engine=(
                    reconciliation_engine
                ),
                master=master
            )
        )

        variance_report = (
            build_variance_report(
                variance=variance
            )
        )

        files_summary = {}

        for file_key in REQUIRED_FILES:

            dataframe = dataframes[
                file_key
            ]

            uploaded_file = (
                uploaded_files[
                    file_key
                ]
            )

            files_summary[
                file_key
            ] = {
                "name": (
                    uploaded_file.filename
                ),
                "rows": int(
                    len(dataframe)
                ),
                "columns": int(
                    len(
                        dataframe.columns
                    )
                )
            }

        total_rows = sum(
            file_info["rows"]
            for file_info
            in files_summary.values()
        )

        return json_response({
            "status": (
                "Reconciliation Engine Completed"
            ),
            "application": APPLICATION_NAME,
            "version": APPLICATION_VERSION,
            "summary": {
                "files_count": len(
                    files_summary
                ),
                "total_input_rows": (
                    total_rows
                ),
                "master_rows": int(
                    len(master)
                ),
                "accept_rows": int(
                    len(accept)
                ),
                "dispatch_rows": int(
                    len(dispatch_output)
                ),
                "variance_rows": int(
                    len(variance)
                ),
                "accept_files_count": len(
                    accept_files
                ),
                "dispatch_files_count": len(
                    dispatch_files
                )
            },
            "files": files_summary,
            "outputs": {
                "accept_files": (
                    accept_files
                ),
                "dispatch_files": (
                    dispatch_files
                ),
                "accept_details": (
                    accept_details
                ),
                "dispatch_details": (
                    dispatch_details
                ),
                "variance": (
                    variance_report
                )
            }
        })

    except Exception as ex:

        logging.exception(
            "Error while processing uploaded files."
        )

        return json_response(
            {
                "status": "Failed",
                "error": str(ex),
                "error_type": (
                    type(ex).__name__
                ),
                "trace": (
                    traceback.format_exc()
                )
            },
            status_code=500
        )
