import io
import json
import logging
import traceback
from pathlib import Path

import azure.functions as func
import pandas as pd

from engine.exporter import Exporter
from engine.reconciliation import ReconciliationEngine


app = func.FunctionApp(
    http_auth_level=func.AuthLevel.ANONYMOUS
)


APPLICATION_NAME = "SFDA Reconciliation"
APPLICATION_VERSION = "3.1.0"

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


def _find_column(
    dataframe,
    candidates
):

    normalized = {
        str(column).strip().lower(): column
        for column in dataframe.columns
    }

    for candidate in candidates:
        match = normalized.get(
            str(candidate).strip().lower()
        )
        if match is not None:
            return match

    return None


def _series_or_blank(
    dataframe,
    candidates
):

    column = _find_column(
        dataframe,
        candidates
    )

    if column is None:
        return pd.Series(
            [""] * len(dataframe),
            index=dataframe.index,
            dtype=object
        )

    return dataframe[column]


def _normalize_text(value):

    if pd.isna(value):
        return ""

    text = str(value).strip()

    if text.endswith(".0"):
        text = text[:-2]

    return text


def _join_unique(values):

    unique_values = []

    for value in values:
        text = _normalize_text(value)

        if (
            text
            and text not in unique_values
        ):
            unique_values.append(text)

    return " | ".join(unique_values)


def _join_trk_first(values):

    unique_values = []

    for value in values:
        text = _normalize_text(value)

        if (
            text
            and text not in unique_values
        ):
            unique_values.append(text)

    trk_values = [
        value
        for value in unique_values
        if value.upper().startswith("TRK")
    ]

    other_values = [
        value
        for value in unique_values
        if not value.upper().startswith("TRK")
    ]

    return " | ".join(
        trk_values + other_values
    )



def _column_or_blank(dataframe, candidates, target_name):
    result = dataframe.copy()
    source = _find_column(result, candidates)
    if source is None:
        result[target_name] = ""
    else:
        result[target_name] = result[source]
    return result


def _normalize_key_text(value):
    return _normalize_text(value).upper()


def _normalize_date_key(value):
    if pd.isna(value) or str(value).strip() == "":
        return ""
    parsed = pd.to_datetime(value, errors="coerce", dayfirst=True)
    if pd.isna(parsed):
        return _normalize_text(value)
    return parsed.strftime("%Y-%m-%d")


def _prepare_asn_transactions(asn_df):
    asn = asn_df.copy()
    mappings = {
        "BN": ["BN", "Batch Number", "Batch/Lot", "Lot No/Batch"],
        "Expiry Date": ["Expiry Date", "Expiration Date", "Best Before Date"],
        "Generic Item Number": ["Generic Item Number", "Item Number", "Generic Number"],
        "Trade Item Number": ["Trade Item Number", "Trade Item", "Trade Number"],
        "Trade Name": ["Trade Name", "Trade Description", "Description"],
        "Supplier Name": ["Supplier Name", "Supplier", "Vendor Name"],
        "Supplier Code": ["Supplier Code", "Vendor Code"],
        "Inbound Shipment Number": ["Inbound Shipment", "Inbound Shipment Number", "TRK", "TRK Number", "Inbound Number"],
        "ASN Line": ["ASN Line", "ASN Line Number"],
        "Received Date": ["Received Date", "Receipt Date"],
        "PO Number": ["PO Number", "Purchase Order Number"],
        "Invoice Number": ["Invoice Number", "Invoice"],
        "Received Quantity": ["Received Quantity", "Received Qty", "Receipt Quantity"]
    }
    for target, candidates in mappings.items():
        asn = _column_or_blank(asn, candidates, target)
    asn["Received Quantity"] = pd.to_numeric(asn["Received Quantity"], errors="coerce").fillna(0)
    asn["_BN_KEY"] = asn["BN"].map(_normalize_key_text)
    asn["_EXPIRY_KEY"] = asn["Expiry Date"].map(_normalize_date_key)
    asn = asn[asn["Received Quantity"] > 0].copy()
    return asn


def _prepare_inventory_lookup(inventory_df):
    inventory = inventory_df.copy()
    mappings = {
        "BN": ["BN", "Lot No/Batch", "Batch Number", "Batch/Lot"],
        "Expiry Date": ["Expiry Date", "Expiration Date", "Best Before Date"],
        "Generic Item Number": ["Generic Item Number", "Item Number"],
        "Trade Item Number": ["Trade Item Number", "Trade Item"],
        "Trade Name": ["Trade Item Description", "Trade Name", "Trade Description"],
        "Supplier Name": ["Supplier Name", "Supplier"],
        "Inbound Shipment Number": ["Inbound Shipment", "Inbound Shipment Number"],
        "Received Date": ["Receipt Date", "Received Date"]
    }
    for target, candidates in mappings.items():
        inventory = _column_or_blank(inventory, candidates, target)
    inventory["_BN_KEY"] = inventory["BN"].map(_normalize_key_text)
    inventory["_EXPIRY_KEY"] = inventory["Expiry Date"].map(_normalize_date_key)
    return inventory



def _first_existing_column(dataframe, candidates):
    for column in candidates:
        if column in dataframe.columns:
            return column
    return None


def _json_safe_value(value):
    if value is None:
        return None
    try:
        if pd.isna(value):
            return None
    except Exception:
        pass
    if isinstance(value, pd.Timestamp):
        return value.strftime("%d-%m-%Y")
    if hasattr(value, "isoformat") and not isinstance(value, str):
        try:
            return value.isoformat()
        except Exception:
            pass
    if hasattr(value, "item"):
        try:
            return value.item()
        except Exception:
            pass
    return value


def build_dashboard_preview(master, limit=100):
    if master is None or master.empty:
        return []
    aliases = {
        "BN": ["BN", "Batch Number", "Batch/Lot"],
        "Expiry Date": ["Expiry Date", "Expiration Date", "Best Before Date"],
        "GTIN": ["GTIN"],
        "Drug Name": ["Drug Name", "Trade Name"],
        "PackageSize": ["PackageSize", "Package Size", "PZ"],
        "Active": ["Active"],
        "Quantity Sent Pending": ["Quantity Sent Pending", "Quantity sent pending", "Qty Sent Pending"],
        "Quantity Receive Pending": ["Quantity Receive Pending", "Quantity Receive Pending ", "Qty Receive Pending"],
        "Receiving": ["Receiving", "Receiving Packages", "Received Packages", "Received"],
        "To Be Accept": ["To Be Accept"],
        "Inventory": ["Inventory", "Inventory Packages", "Inventory Packs"],
        "Variance": ["Variance", "Inventory Variance", "Variance (Inv - Active)"],
        "To Be Dispatch": ["To Be Dispatch", "Calculated To Be Dispatch", "Remaining To Be Dispatch"]
    }
    selected = {canonical: _first_existing_column(master, candidates) for canonical, candidates in aliases.items()}
    rows = []
    for _, source_row in master.head(limit).iterrows():
        record = {}
        for canonical, source_column in selected.items():
            record[canonical] = _json_safe_value(source_row[source_column]) if source_column else None
        to_accept = pd.to_numeric(pd.Series([record.get("To Be Accept")]), errors="coerce").fillna(0).iloc[0]
        to_dispatch = pd.to_numeric(pd.Series([record.get("To Be Dispatch")]), errors="coerce").fillna(0).iloc[0]
        variance = pd.to_numeric(pd.Series([record.get("Variance")]), errors="coerce").fillna(0).iloc[0]
        if variance != 0:
            status = "DIFF"
        elif to_accept > 0:
            status = "ACCEPT PENDING"
        elif to_dispatch > 0:
            status = "DISPATCH PENDING"
        else:
            status = "OK"
        record["Status"] = status
        rows.append(record)
    return rows


def build_accept_details(asn_df, inventory_df, master):
    required = master.copy()
    if "To Be Accept" not in required.columns:
        required["To Be Accept"] = 0
    required["To Be Accept"] = pd.to_numeric(required["To Be Accept"], errors="coerce").fillna(0)
    required = required[required["To Be Accept"] > 0].copy()
    required["_BN_KEY"] = required["BN"].map(_normalize_key_text)
    required["_EXPIRY_KEY"] = required["Expiry Date"].map(_normalize_date_key)

    asn = _prepare_asn_transactions(asn_df)
    inventory = _prepare_inventory_lookup(inventory_df)
    output_rows = []

    for _, need in required.iterrows():
        quantity_left = int(round(float(need["To Be Accept"])))
        matches = asn[(asn["_BN_KEY"] == need["_BN_KEY"]) & (asn["_EXPIRY_KEY"] == need["_EXPIRY_KEY"])].copy()
        matches = matches.sort_values(["Received Date", "Inbound Shipment Number", "ASN Line"], na_position="last")

        if matches.empty:
            inv = inventory[(inventory["_BN_KEY"] == need["_BN_KEY"]) & (inventory["_EXPIRY_KEY"] == need["_EXPIRY_KEY"])].head(1)
            fallback = inv.iloc[0] if not inv.empty else None
            output_rows.append({
                "GTIN": need.get("GTIN", ""), "Drug Name": need.get("Drug Name", ""),
                "Generic Item Number": "" if fallback is None else fallback.get("Generic Item Number", ""),
                "Trade Item Number": "" if fallback is None else fallback.get("Trade Item Number", ""),
                "Trade Name": "" if fallback is None else fallback.get("Trade Name", ""),
                "BN": need.get("BN", ""), "Expiry Date": need.get("Expiry Date", ""),
                "Supplier Name": "" if fallback is None else fallback.get("Supplier Name", ""),
                "Supplier Code": "", "Inbound Shipment Number": "" if fallback is None else fallback.get("Inbound Shipment Number", ""),
                "ASN Line": "", "Received Date": "" if fallback is None else fallback.get("Received Date", ""),
                "PO Number": "", "Invoice Number": "", "Received Quantity": 0,
                "PackageSize": need.get("PackageSize", 0), "To Be Accept": quantity_left,
                "Detail Status": "No matching ASN transaction; inventory fallback used" if fallback is not None else "No matching ASN or inventory transaction"
            })
            continue

        for _, txn in matches.iterrows():
            if quantity_left <= 0:
                break
            received_qty = max(0, int(round(float(txn["Received Quantity"]))))
            allocated = min(quantity_left, received_qty)
            if allocated <= 0:
                continue
            output_rows.append({
                "GTIN": need.get("GTIN", ""), "Drug Name": need.get("Drug Name", ""),
                "Generic Item Number": txn.get("Generic Item Number", ""),
                "Trade Item Number": txn.get("Trade Item Number", ""), "Trade Name": txn.get("Trade Name", ""),
                "BN": need.get("BN", ""), "Expiry Date": need.get("Expiry Date", ""),
                "Supplier Name": txn.get("Supplier Name", ""), "Supplier Code": txn.get("Supplier Code", ""),
                "Inbound Shipment Number": txn.get("Inbound Shipment Number", ""), "ASN Line": txn.get("ASN Line", ""),
                "Received Date": txn.get("Received Date", ""), "PO Number": txn.get("PO Number", ""),
                "Invoice Number": txn.get("Invoice Number", ""), "Received Quantity": received_qty,
                "PackageSize": need.get("PackageSize", 0), "To Be Accept": allocated,
                "Detail Status": "Matched to ASN transaction"
            })
            quantity_left -= allocated

        if quantity_left > 0:
            output_rows.append({
                "GTIN": need.get("GTIN", ""), "Drug Name": need.get("Drug Name", ""),
                "Generic Item Number": "", "Trade Item Number": "", "Trade Name": "",
                "BN": need.get("BN", ""), "Expiry Date": need.get("Expiry Date", ""),
                "Supplier Name": "", "Supplier Code": "", "Inbound Shipment Number": "", "ASN Line": "",
                "Received Date": "", "PO Number": "", "Invoice Number": "", "Received Quantity": 0,
                "PackageSize": need.get("PackageSize", 0), "To Be Accept": quantity_left,
                "Detail Status": "Accept quantity exceeds matched ASN received quantity"
            })

    details = pd.DataFrame(output_rows)
    return Exporter.build_formatted_excel_file(
        df=details, file_name="Accept_Details.xlsx", sheet_name="Accept Details", title="SFDA Accept Details",
        columns=["GTIN", "Drug Name", "Generic Item Number", "Trade Item Number", "Trade Name", "BN", "Expiry Date",
                 "Supplier Name", "Supplier Code", "Inbound Shipment Number", "ASN Line", "Received Date", "PO Number",
                 "Invoice Number", "Received Quantity", "PackageSize", "To Be Accept", "Detail Status"],
        sort_columns=["Generic Item Number", "Trade Item Number", "BN", "Expiry Date", "Inbound Shipment Number", "ASN Line"]
    )


def _prepare_dispatch_transactions(dispatch_df):
    dispatch = dispatch_df.copy()
    mappings = {
        "BN": ["BN", "Batch/Lot", "Batch Number", "Lot No/Batch"],
        "Expiry Date": ["Expiry Date", "Best Before Date", "Expiration Date"],
        "Order Number": ["Order Number", "Sales Order Number", "Sales Order", "SO Number"],
        "Order Line": ["Order Line", "order line", "Sales Order Line"],
        "Reference Order Number": ["Reference order #", "Reference Order Number"],
        "Generic Item Number": ["Generic Item Number", "Item Number"],
        "Trade Item Number": ["Trade Item Number", "Trade Item"],
        "Trade Name": ["Trade Description", "Trade Name"],
        "To Address": ["To Address", "Customer Name"],
        "Ship To Customer": ["Ship To Customer", "Customer Code"],
        "Dispatched Quantity": ["Dispatched Quantity", "Pick Qty", "Picked Quantity"]
    }
    for target, candidates in mappings.items():
        dispatch = _column_or_blank(dispatch, candidates, target)
    dispatch["Dispatched Quantity"] = pd.to_numeric(dispatch["Dispatched Quantity"], errors="coerce").fillna(0)
    dispatch["_BN_KEY"] = dispatch["BN"].map(_normalize_key_text)
    dispatch["_EXPIRY_KEY"] = dispatch["Expiry Date"].map(_normalize_date_key)
    dispatch["_CUSTOMER_KEY"] = dispatch["To Address"].map(_normalize_key_text)
    dispatch = dispatch[dispatch["Dispatched Quantity"] > 0].copy()
    return dispatch


def build_dispatch_details(dispatch_df, dispatch_output):
    source = _prepare_dispatch_transactions(dispatch_df)
    allocated = dispatch_output.copy()
    quantity_candidates = ["Allocated To Be Dispatch", "Calculated To Be Dispatch", "To Be Dispatch"]
    quantity_column = next((c for c in quantity_candidates if c in allocated.columns), None)
    if quantity_column is None:
        allocated["_ALLOC_QTY"] = 0
    else:
        allocated["_ALLOC_QTY"] = pd.to_numeric(allocated[quantity_column], errors="coerce").fillna(0)
    allocated = allocated[allocated["_ALLOC_QTY"] > 0].copy()
    allocated["_BN_KEY"] = allocated["BN"].map(_normalize_key_text)
    allocated["_EXPIRY_KEY"] = allocated["Expiry Date"].map(_normalize_date_key)
    if "To Address" not in allocated.columns:
        allocated["To Address"] = ""
    allocated["_CUSTOMER_KEY"] = allocated["To Address"].map(_normalize_key_text)

    rows = []
    for _, item in allocated.iterrows():
        qty_left = int(round(float(item["_ALLOC_QTY"])))
        exact = source[(source["_BN_KEY"] == item["_BN_KEY"]) & (source["_EXPIRY_KEY"] == item["_EXPIRY_KEY"]) & (source["_CUSTOMER_KEY"] == item["_CUSTOMER_KEY"])].copy()
        status = "Matched by customer + batch + expiry"
        matches = exact
        if matches.empty:
            matches = source[(source["_BN_KEY"] == item["_BN_KEY"]) & (source["_EXPIRY_KEY"] == item["_EXPIRY_KEY"])].copy()
            status = "Fallback: matched by batch + expiry only"
        matches = matches.sort_values(["Order Number", "Order Line"], na_position="last")

        if matches.empty:
            rows.append({
                "To Address": item.get("To Address", ""), "GLN": item.get("GLN", ""), "Customer Status": item.get("Customer Status", ""),
                "Order Number": "", "Order Line": "", "Reference Order Number": "", "Ship To Customer": "",
                "Generic Item Number": "", "Trade Item Number": "", "Trade Name": "", "GTIN": item.get("GTIN", ""),
                "Drug Name": item.get("Drug Name", ""), "BN": item.get("BN", ""), "Expiry Date": item.get("Expiry Date", ""),
                "Dispatched Quantity": 0, "PackageSize": item.get("PackageSize", 0), "To Be Dispatch": qty_left,
                "Detail Status": "No matching dispatch transaction"
            })
            continue

        for _, txn in matches.iterrows():
            if qty_left <= 0:
                break
            actual_qty = max(0, int(round(float(txn["Dispatched Quantity"]))))
            allocated_qty = min(qty_left, actual_qty)
            if allocated_qty <= 0:
                continue
            rows.append({
                "To Address": txn.get("To Address", item.get("To Address", "")), "GLN": item.get("GLN", ""),
                "Customer Status": item.get("Customer Status", ""), "Order Number": txn.get("Order Number", ""),
                "Order Line": txn.get("Order Line", ""), "Reference Order Number": txn.get("Reference Order Number", ""),
                "Ship To Customer": txn.get("Ship To Customer", ""), "Generic Item Number": txn.get("Generic Item Number", ""),
                "Trade Item Number": txn.get("Trade Item Number", ""), "Trade Name": txn.get("Trade Name", ""),
                "GTIN": item.get("GTIN", ""), "Drug Name": item.get("Drug Name", ""), "BN": item.get("BN", ""),
                "Expiry Date": item.get("Expiry Date", ""), "Dispatched Quantity": actual_qty,
                "PackageSize": item.get("PackageSize", 0), "To Be Dispatch": allocated_qty, "Detail Status": status
            })
            qty_left -= allocated_qty

        if qty_left > 0:
            rows.append({
                "To Address": item.get("To Address", ""), "GLN": item.get("GLN", ""), "Customer Status": item.get("Customer Status", ""),
                "Order Number": "", "Order Line": "", "Reference Order Number": "", "Ship To Customer": "",
                "Generic Item Number": "", "Trade Item Number": "", "Trade Name": "", "GTIN": item.get("GTIN", ""),
                "Drug Name": item.get("Drug Name", ""), "BN": item.get("BN", ""), "Expiry Date": item.get("Expiry Date", ""),
                "Dispatched Quantity": 0, "PackageSize": item.get("PackageSize", 0), "To Be Dispatch": qty_left,
                "Detail Status": "Allocated quantity exceeds matched dispatch quantity"
            })

    details = pd.DataFrame(rows)
    return Exporter.build_formatted_excel_file(
        df=details, file_name="Dispatch_Details.xlsx", sheet_name="Dispatch Details", title="SFDA Dispatch Details",
        columns=["To Address", "GLN", "Customer Status", "Order Number", "Order Line", "Reference Order Number", "Ship To Customer",
                 "Generic Item Number", "Trade Item Number", "Trade Name", "GTIN", "Drug Name", "BN", "Expiry Date",
                 "Dispatched Quantity", "PackageSize", "To Be Dispatch", "Detail Status"],
        sort_columns=["To Address", "Order Number", "Order Line", "Generic Item Number", "Trade Item Number", "BN", "Expiry Date"]
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
                "Use POST with the four "
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

        dashboard_preview = build_dashboard_preview(
            master=master,
            limit=100
        )

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
                asn_df=dataframes["asn"],
                inventory_df=dataframes["inventory"],
                master=master
            )
        )

        dispatch_details = (
            build_dispatch_details(
                dispatch_df=dataframes["dispatch"],
                dispatch_output=dispatch_output
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
            "dashboard_preview": dashboard_preview,
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
