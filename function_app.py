import base64
import io
import json
import mimetypes
import logging
import traceback
import time
from pathlib import Path

import azure.functions as func
import pandas as pd

from engine.blob_storage import BlobStorage
import engine.database as database_module
from engine.database import (
    complete_reconciliation_run,
    create_reconciliation_run,
    fail_reconciliation_run,
    get_reconciliation_history,
    get_reconciliation_run_by_number,
    get_reconciliation_run_files,
    record_reconciliation_file,
    record_batch_events,
    initialize_database,
    test_database_connection,
    create_verification_run,
    complete_verification_run,
    fail_verification_run,
    record_verification_results,
)

from engine.exporter import Exporter
from engine.batch_history import BatchHistoryEngine
from engine.reconciliation import ReconciliationEngine
from engine.verification import VerificationEngine


app = func.FunctionApp(
    http_auth_level=func.AuthLevel.ANONYMOUS
)


APPLICATION_NAME = "SFDA Reconciliation"
APPLICATION_VERSION = "3.8.1"

REQUIRED_FILES = [
    "asn",
    "inventory",
    "dispatch",
    "sfda",
]

RUN_STATUS_PENDING_UPLOAD = "Pending Upload"
RUN_STATUS_PENDING_VERIFICATION = "Pending Verification"
RUN_STATUS_VERIFIED = "Verified"
RUN_STATUS_INVESTIGATION = "Investigation Required"
RUN_LIFECYCLE_STATUSES = [
    RUN_STATUS_PENDING_UPLOAD,
    RUN_STATUS_PENDING_VERIFICATION,
    RUN_STATUS_VERIFIED,
    RUN_STATUS_INVESTIGATION,
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


def get_blob_storage() -> BlobStorage:
    storage = BlobStorage()
    storage.initialize_containers()
    return storage


def _set_run_lifecycle_status(run_id: int, status: str) -> bool:
    if status not in RUN_LIFECYCLE_STATUSES:
        raise ValueError(f"Unsupported run lifecycle status: {status}")

    candidate_names = (
        "update_reconciliation_run_status",
        "set_reconciliation_run_status",
        "update_run_status",
    )
    for function_name in candidate_names:
        status_function = getattr(database_module, function_name, None)
        if not callable(status_function):
            continue
        try:
            status_function(run_id=run_id, status=status)
        except TypeError:
            status_function(run_id, status)
        return True

    logging.warning(
        "No database lifecycle status updater is available. "
        "Run %s remains stored with the completion status created by the existing database layer.",
        run_id,
    )
    return False


def _verification_readiness(run: dict) -> dict:
    current_status = str(run.get("Status") or run.get("status") or "").strip()
    return {
        "run_number": run.get("RunNumber") or run.get("run_number"),
        "current_status": current_status,
        "allowed_statuses": RUN_LIFECYCLE_STATUSES,
        "can_upload_verification_files": current_status in {
            RUN_STATUS_PENDING_UPLOAD,
            RUN_STATUS_PENDING_VERIFICATION,
        },
        "required_latest_sfda_files": 1,
        "notification_result_files": "multiple",
    }


def read_excel_bytes(file_name: str, file_bytes: bytes):
    class UploadedBytes:
        filename = file_name

        def read(self):
            return file_bytes

    return read_excel_file(UploadedBytes())


def _safe_file_name(value: str, fallback: str) -> str:
    cleaned = Path(str(value or fallback)).name.strip()
    return cleaned or fallback


def _content_type_for(file_name: str, fallback: str = "application/octet-stream") -> str:
    guessed, _ = mimetypes.guess_type(file_name)
    return guessed or fallback


def _payload_to_bytes(value):
    if isinstance(value, bytes):
        return value, "application/octet-stream"

    if isinstance(value, str):
        return value.encode("utf-8-sig"), "text/csv; charset=utf-8"

    if isinstance(value, dict):
        content = value.get("content", value.get("data", ""))
        encoding = str(value.get("encoding", "base64")).lower()
        content_type = (
            value.get("mime_type")
            or value.get("mime")
            or "application/octet-stream"
        )
        if isinstance(content, bytes):
            return content, content_type
        if encoding == "base64":
            cleaned = str(content or "")
            if ";base64," in cleaned:
                cleaned = cleaned.split(";base64,", 1)[1]
            return base64.b64decode(cleaned), content_type
        return str(content or "").encode("utf-8"), content_type

    return str(value or "").encode("utf-8"), "application/octet-stream"


def _iter_output_files(group, fallback_name: str, file_type: str):
    if group is None:
        return []

    if isinstance(group, dict) and any(
        key in group for key in ("content", "data", "encoding", "mime_type", "mime")
    ):
        name = _safe_file_name(
            group.get("file_name") or group.get("name"),
            fallback_name,
        )
        data, content_type = _payload_to_bytes(group)
        return [(name, data, content_type, file_type)]

    if isinstance(group, dict):
        rows = []
        for name, value in group.items():
            safe_name = _safe_file_name(name, fallback_name)
            data, content_type = _payload_to_bytes(value)
            rows.append((safe_name, data, content_type, file_type))
        return rows

    data, content_type = _payload_to_bytes(group)
    return [(fallback_name, data, content_type, file_type)]


def _record_blob_file(run_id: int, category: str, file_type: str, file_name: str, blob_info: dict) -> None:
    record_reconciliation_file(
        run_id=run_id,
        file_category=category,
        file_type=file_type,
        file_name=file_name,
        container_name=blob_info["container"],
        blob_name=blob_info["blob_name"],
        content_type=blob_info.get("content_type") or "application/octet-stream",
        size_bytes=int(blob_info.get("size_bytes") or 0),
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
    for _, source_row in master.iterrows():
        record = {}
        for canonical, source_column in selected.items():
            record[canonical] = (
                _json_safe_value(source_row[source_column])
                if source_column
                else None
            )

        to_accept = pd.to_numeric(
            pd.Series([record.get("To Be Accept")]),
            errors="coerce"
        ).fillna(0).iloc[0]

        to_dispatch = pd.to_numeric(
            pd.Series([record.get("To Be Dispatch")]),
            errors="coerce"
        ).fillna(0).iloc[0]

        # Dashboard is an action table: hide batches with no reconciliation action.
        if to_accept <= 0 and to_dispatch <= 0:
            continue

        variance = pd.to_numeric(
            pd.Series([record.get("Variance")]),
            errors="coerce"
        ).fillna(0).iloc[0]

        if variance != 0:
            status = "DIFF"
        elif to_accept > 0 and to_dispatch > 0:
            status = "ACCEPT & DISPATCH"
        elif to_accept > 0:
            status = "ACCEPT PENDING"
        else:
            status = "DISPATCH PENDING"

        record["Status"] = status
        rows.append(record)

        if len(rows) >= limit:
            break

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
        "BN": [
            "BN",
            "Batch/Lot",
            "Batch Number",
            "Lot No/Batch",
        ],
        "Expiry Date": [
            "Expiry Date",
            "Best Before Date",
            "Expiration Date",
        ],
        "Order Number": [
            "Order Number",
            "Sales Order Number",
            "Sales Order",
            "SO Number",
        ],
        "Order Line": [
            "Order Line",
            "order line",
            "Sales Order Line",
        ],
        "Reference Order Number": [
            "Reference order #",
            "Reference Order Number",
        ],
        "Generic Item Number": [
            "Generic Item Number",
            "Item Number",
        ],
        "Trade Item Number": [
            "Trade Item Number",
            "Trade Item",
        ],
        "Trade Name": [
            "Trade Description",
            "Trade Name",
        ],
        "To Address": [
            "To Address",
            "Customer Name",
        ],
        "Ship To Customer": [
            "Ship To Customer",
            "Customer Code",
        ],
        "Dispatched Quantity": [
            "Dispatched Quantity",
            "Pick Qty",
            "Picked Quantity",
        ],
    }

    for target, candidates in mappings.items():
        dispatch = _column_or_blank(
            dispatch,
            candidates,
            target,
        )

    dispatch["Dispatched Quantity"] = pd.to_numeric(
        dispatch["Dispatched Quantity"],
        errors="coerce",
    ).fillna(0)

    dispatch["_BN_KEY"] = dispatch["BN"].map(
        _normalize_key_text
    )
    dispatch["_EXPIRY_KEY"] = dispatch[
        "Expiry Date"
    ].map(
        _normalize_date_key
    )
    dispatch["_CUSTOMER_KEY"] = dispatch[
        "To Address"
    ].map(
        _normalize_key_text
    )

    dispatch = dispatch[
        dispatch["Dispatched Quantity"] > 0
    ].copy()

    dispatch["_SOURCE_ROW_ID"] = range(
        1,
        len(dispatch) + 1,
    )

    return dispatch.reset_index(
        drop=True
    )


def build_dispatch_details(
    dispatch_df,
    dispatch_output,
):
    """
    Build Dispatch_Details.xlsx as an operational view of Full Dispatch.

    Full Dispatch remains the transaction source of truth.
    This function does not rebuild transactions, perform fallback matching,
    redistribute quantities, or recalculate the dispatch allocation.
    """

    output_columns = [
        "Run Date",
        "GTIN",
        "Generic Item Number",
        "Trade Item Number",
        "Trade Description",
        "Sales Order Number",
        "Pick Qty",
        "Qty / Pack",
        "QU",
        "Batch/Lot",
        "Confirm Date",
        "To Address",
        "GLN",
        "Custody",
        "Order Line Status",
        "To Be Dispatch",
    ]

    def build_empty_output():
        return Exporter.build_formatted_excel_file(
            df=pd.DataFrame(columns=output_columns),
            file_name="Dispatch_Details.xlsx",
            sheet_name="Dispatch Details",
            title="SFDA Dispatch Details",
            columns=output_columns,
        )

    if dispatch_df is None or dispatch_df.empty:
        return build_empty_output()

    if dispatch_output is None or dispatch_output.empty:
        return build_empty_output()

    source = dispatch_df.copy()
    allocated = dispatch_output.copy()

    source["_SOURCE_ROW_ORDER"] = range(
        len(source)
    )

    def source_column(
        candidates,
        default="",
    ):
        column = _find_column(
            source,
            candidates,
        )

        if column is None:
            return pd.Series(
                [default] * len(source),
                index=source.index,
                dtype=object,
            )

        return source[column].copy()

    def allocated_column(
        candidates,
        default="",
    ):
        column = _find_column(
            allocated,
            candidates,
        )

        if column is None:
            return pd.Series(
                [default] * len(allocated),
                index=allocated.index,
                dtype=object,
            )

        return allocated[column].copy()

    # ---------------------------------------------------------
    # Full Dispatch transaction fields
    # ---------------------------------------------------------
    source["_BN_VALUE"] = source_column([
        "BN",
        "Batch/Lot",
        "Batch Number",
        "Lot No/Batch",
    ])

    source["_EXPIRY_VALUE"] = source_column([
        "Expiry Date",
        "Best Before Date",
        "Expiration Date",
    ])

    source["_GENERIC_ITEM_VALUE"] = source_column([
        "Generic Item Number",
        "Item Number",
        "Generic Number",
    ])

    source["_TRADE_ITEM_VALUE"] = source_column([
        "Trade Item Number",
        "Trade Item",
        "Trade Number",
    ])

    source["_TRADE_DESCRIPTION_VALUE"] = source_column([
        "Trade Description",
        "Trade Name",
        "Trade Item Description",
        "Description",
    ])

    source["_SALES_ORDER_VALUE"] = source_column([
        "Sales Order Number",
        "Order Number",
        "Sales Order",
        "SO Number",
    ])

    source["_PICK_QTY_VALUE"] = source_column(
        [
            "Pick Qty",
            "Dispatched Quantity",
            "Picked Quantity",
        ],
        default=0,
    )

    source["_QU_VALUE"] = source_column([
        "QU",
        "Quantity Unit",
        "Unit",
        "UOM",
    ])

    source["_CONFIRM_DATE_VALUE"] = source_column([
        "Confirm Date",
        "Confirmation Date",
        "Dispatch Confirm Date",
        "Dispatched Date",
    ])

    source["_TO_ADDRESS_VALUE"] = source_column([
        "To Address",
        "Customer Name",
    ])

    source["_ORDER_LINE_STATUS_VALUE"] = source_column([
        "Order Line Status",
        "Line Status",
        "Status",
    ])

    source["_BN_KEY"] = source[
        "_BN_VALUE"
    ].map(
        _normalize_key_text
    )

    source["_EXPIRY_KEY"] = source[
        "_EXPIRY_VALUE"
    ].map(
        _normalize_date_key
    )

    source["_CUSTOMER_KEY"] = source[
        "_TO_ADDRESS_VALUE"
    ].map(
        _normalize_key_text
    )

    # ---------------------------------------------------------
    # Existing dispatch_output fields
    # ---------------------------------------------------------
    allocated["_BN_VALUE"] = allocated_column([
        "BN",
        "Batch/Lot",
        "Batch Number",
        "Lot No/Batch",
    ])

    allocated["_EXPIRY_VALUE"] = allocated_column([
        "Expiry Date",
        "Best Before Date",
        "Expiration Date",
    ])

    allocated["_TO_ADDRESS_VALUE"] = allocated_column([
        "To Address",
        "Customer Name",
    ])

    allocated["_GTIN_VALUE"] = allocated_column([
        "GTIN",
    ])

    allocated["_GLN_VALUE"] = allocated_column([
        "GLN",
    ])

    allocated["_CUSTOMER_STATUS_VALUE"] = allocated_column([
        "Customer Status",
        "Custody",
    ])

    allocated["_DRUG_NAME_VALUE"] = allocated_column([
        "Drug Name",
        "Trade Name",
    ])

    allocated["_PACKAGE_SIZE_VALUE"] = allocated_column(
        [
            "PackageSize",
            "Package Size",
            "Pack Size",
        ],
        default=0,
    )

    quantity_column = _find_column(
        allocated,
        [
            "Allocated To Be Dispatch",
            "Calculated To Be Dispatch",
            "To Be Dispatch",
        ],
    )

    if quantity_column is None:
        raise ValueError(
            "dispatch_output does not contain the existing "
            "dispatch allocation quantity column."
        )

    allocated["_ALLOC_QTY"] = pd.to_numeric(
        allocated[quantity_column],
        errors="coerce",
    ).fillna(0)

    allocated = allocated[
        allocated["_ALLOC_QTY"] > 0
    ].copy()

    if allocated.empty:
        return build_empty_output()

    allocated["_BN_KEY"] = allocated[
        "_BN_VALUE"
    ].map(
        _normalize_key_text
    )

    allocated["_EXPIRY_KEY"] = allocated[
        "_EXPIRY_VALUE"
    ].map(
        _normalize_date_key
    )

    allocated["_CUSTOMER_KEY"] = allocated[
        "_TO_ADDRESS_VALUE"
    ].map(
        _normalize_key_text
    )

    # ---------------------------------------------------------
    # Keep Full Dispatch rows only when BN + Expiry exist in
    # dispatch_output. No transaction reconstruction is performed.
    # ---------------------------------------------------------
    dispatch_batch_keys = (
        allocated[
            [
                "_BN_KEY",
                "_EXPIRY_KEY",
            ]
        ]
        .drop_duplicates()
        .copy()
    )

    source = source.merge(
        dispatch_batch_keys.assign(
            _IN_DISPATCH_OUTPUT=True
        ),
        on=[
            "_BN_KEY",
            "_EXPIRY_KEY",
        ],
        how="left",
        sort=False,
        validate="many_to_one",
    )

    source = source[
        source["_IN_DISPATCH_OUTPUT"].fillna(False)
    ].copy()

    if source.empty:
        return build_empty_output()

    # ---------------------------------------------------------
    # Build a direct enrichment lookup from the already calculated
    # dispatch allocation.
    #
    # This is a direct BN + Expiry + To Address join only.
    # There is no fallback customer matching and no redistribution.
    # ---------------------------------------------------------
    allocation_lookup = (
        allocated.groupby(
            [
                "_BN_KEY",
                "_EXPIRY_KEY",
                "_CUSTOMER_KEY",
            ],
            as_index=False,
            sort=False,
            dropna=False,
        )
        .agg({
            "_GTIN_VALUE": _join_unique,
            "_GLN_VALUE": _join_unique,
            "_CUSTOMER_STATUS_VALUE": _join_unique,
            "_DRUG_NAME_VALUE": _join_unique,
            "_PACKAGE_SIZE_VALUE": "first",
            "_ALLOC_QTY": "sum",
        })
    )

    source = source.merge(
        allocation_lookup,
        on=[
            "_BN_KEY",
            "_EXPIRY_KEY",
            "_CUSTOMER_KEY",
        ],
        how="left",
        sort=False,
        validate="many_to_one",
    )

    source = source.sort_values(
        "_SOURCE_ROW_ORDER",
        kind="stable",
    ).reset_index(drop=True)

    # ---------------------------------------------------------
    # Qty / Pack
    # ---------------------------------------------------------
    numeric_pick_qty = pd.to_numeric(
        source["_PICK_QTY_VALUE"],
        errors="coerce",
    ).fillna(0)

    numeric_package_size = pd.to_numeric(
        source["_PACKAGE_SIZE_VALUE"],
        errors="coerce",
    ).fillna(0)

    source["_QTY_PER_PACK"] = 0.0

    valid_package_size = (
        numeric_package_size > 0
    )

    source.loc[
        valid_package_size,
        "_QTY_PER_PACK",
    ] = (
        numeric_pick_qty.loc[
            valid_package_size
        ]
        / numeric_package_size.loc[
            valid_package_size
        ]
    )

    # ---------------------------------------------------------
    # Preserve the locked allocation total.
    #
    # If Full Dispatch contains multiple rows for the same
    # BN + Expiry + To Address, the existing allocation is shown
    # once on the first original row so it is not duplicated.
    # No allocation quantity is recalculated or redistributed.
    # ---------------------------------------------------------
    source["_ALLOCATION_ROW_NUMBER"] = (
        source.groupby(
            [
                "_BN_KEY",
                "_EXPIRY_KEY",
                "_CUSTOMER_KEY",
            ],
            sort=False,
            dropna=False,
        )
        .cumcount()
    )

    source["_TO_BE_DISPATCH_VALUE"] = 0.0

    allocation_first_row = (
        source["_ALLOCATION_ROW_NUMBER"].eq(0)
        & source["_ALLOC_QTY"].notna()
    )

    source.loc[
        allocation_first_row,
        "_TO_BE_DISPATCH_VALUE",
    ] = source.loc[
        allocation_first_row,
        "_ALLOC_QTY",
    ]

    # ---------------------------------------------------------
    # Final operational output
    # ---------------------------------------------------------
    run_date = pd.Timestamp.now().normalize()

    details = pd.DataFrame({
        "Run Date": pd.Series(
            [run_date] * len(source),
            index=source.index,
        ),
        "GTIN": source[
            "_GTIN_VALUE"
        ].fillna(""),
        "Generic Item Number": source[
            "_GENERIC_ITEM_VALUE"
        ],
        "Trade Item Number": source[
            "_TRADE_ITEM_VALUE"
        ],
        "Trade Description": source[
            "_TRADE_DESCRIPTION_VALUE"
        ],
        "Sales Order Number": source[
            "_SALES_ORDER_VALUE"
        ],
        "Pick Qty": source[
            "_PICK_QTY_VALUE"
        ],
        "Qty / Pack": source[
            "_QTY_PER_PACK"
        ],
        "QU": source[
            "_QU_VALUE"
        ],
        "Batch/Lot": source[
            "_BN_VALUE"
        ],
        "Confirm Date": source[
            "_CONFIRM_DATE_VALUE"
        ],
        "To Address": source[
            "_TO_ADDRESS_VALUE"
        ],
        "GLN": source[
            "_GLN_VALUE"
        ].fillna(""),
        "Custody": source[
            "_CUSTOMER_STATUS_VALUE"
        ].fillna(""),
        "Order Line Status": source[
            "_ORDER_LINE_STATUS_VALUE"
        ],
        "To Be Dispatch": source[
            "_TO_BE_DISPATCH_VALUE"
        ],
    })

    return Exporter.build_formatted_excel_file(
        df=details,
        file_name="Dispatch_Details.xlsx",
        sheet_name="Dispatch Details",
        title="SFDA Dispatch Details",
        columns=output_columns,
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

    try:
        initialize_database()

        database_result = test_database_connection()
        storage_result = get_blob_storage().health()

        return json_response({
            "status": "Healthy",
            "application": APPLICATION_NAME,
            "azure_function": "Working",
            "version": APPLICATION_VERSION,
            "database": database_result,
            "storage": storage_result,
        })

    except Exception as ex:
        logging.exception(
            "Database health check failed."
        )

        return json_response(
            {
                "status": "Failed",
                "application": APPLICATION_NAME,
                "version": APPLICATION_VERSION,
                "error": str(ex),
                "error_type": type(ex).__name__,
            },
            status_code=500,
        )

@app.route(
    route="history",
    methods=["GET"]
)
def history(
    req: func.HttpRequest
) -> func.HttpResponse:

    try:
        raw_limit = req.params.get("limit", "100")

        try:
            limit = int(raw_limit)
        except (TypeError, ValueError):
            return json_response(
                {
                    "status": "Failed",
                    "message": "The limit query parameter must be an integer.",
                },
                status_code=400,
            )

        history_rows = get_reconciliation_history(limit=limit)

        return json_response({
            "status": "Success",
            "application": APPLICATION_NAME,
            "version": APPLICATION_VERSION,
            "count": len(history_rows),
            "history": history_rows,
        })

    except Exception as ex:
        logging.exception(
            "Error while loading reconciliation history."
        )

        return json_response(
            {
                "status": "Failed",
                "application": APPLICATION_NAME,
                "version": APPLICATION_VERSION,
                "error": str(ex),
                "error_type": type(ex).__name__,
            },
            status_code=500,
        )


@app.route(
    route="history/{run_number}",
    methods=["GET"]
)
def history_details(
    req: func.HttpRequest
) -> func.HttpResponse:
    try:
        run_number = str(req.route_params.get("run_number") or "").strip()
        if not run_number:
            return json_response({"status": "Failed", "message": "Run number is required."}, 400)

        run = get_reconciliation_run_by_number(run_number)
        if run is None:
            return json_response({"status": "Failed", "message": "Run was not found."}, 404)

        files = get_reconciliation_run_files(int(run["RunID"]))
        for row in files:
            row["download_url"] = (
                "/api/history-file?run_number="
                + run_number
                + "&file_id="
                + str(row["RunFileID"])
            )

        return json_response({
            "status": "Success",
            "application": APPLICATION_NAME,
            "version": APPLICATION_VERSION,
            "run": run,
            "files": files,
        })
    except Exception as ex:
        logging.exception("Error while loading run details.")
        return json_response({
            "status": "Failed",
            "failed_step": current_step,
            "total_seconds": round(
                total_elapsed,
                3,
            ),
            "error": str(ex),
            "error_type": type(ex).__name__,
        }, 500)


@app.route(
    route="history-file",
    methods=["GET"]
)
def history_file(
    req: func.HttpRequest
) -> func.HttpResponse:
    try:
        run_number = str(req.params.get("run_number") or "").strip()
        raw_file_id = req.params.get("file_id")
        if not run_number or not raw_file_id:
            return json_response({
                "status": "Failed",
                "message": "run_number and file_id are required.",
            }, 400)

        run = get_reconciliation_run_by_number(run_number)
        if run is None:
            return json_response({"status": "Failed", "message": "Run was not found."}, 404)

        files = get_reconciliation_run_files(int(run["RunID"]))
        target = next(
            (row for row in files if int(row["RunFileID"]) == int(raw_file_id)),
            None,
        )
        if target is None:
            return json_response({"status": "Failed", "message": "File was not found."}, 404)

        blob = get_blob_storage().download_blob(
            target["ContainerName"],
            target["BlobName"],
        )
        safe_name = _safe_file_name(target["FileName"], "download.bin")
        return func.HttpResponse(
            body=blob["data"],
            status_code=200,
            mimetype=target.get("ContentType") or blob["content_type"],
            headers={
                "Content-Disposition": f'attachment; filename="{safe_name}"',
                "Cache-Control": "no-store",
            },
        )
    except FileNotFoundError as ex:
        return json_response({"status": "Failed", "message": str(ex)}, 404)
    except Exception as ex:
        logging.exception("Error while downloading run file.")
        return json_response({
            "status": "Failed",
            "error": str(ex),
            "error_type": type(ex).__name__,
        }, 500)


@app.route(
    route="verification-readiness/{run_number}",
    methods=["GET"]
)
def verification_readiness(
    req: func.HttpRequest
) -> func.HttpResponse:
    try:
        run_number = str(req.route_params.get("run_number") or "").strip()
        if not run_number:
            return json_response({
                "status": "Failed",
                "message": "Run number is required.",
            }, 400)

        run = get_reconciliation_run_by_number(run_number)
        if not run:
            return json_response({
                "status": "Failed",
                "message": "Run was not found.",
            }, 404)

        return json_response({
            "status": "Success",
            "verification": _verification_readiness(run),
        })
    except Exception as ex:
        logging.exception("Error while loading verification readiness.")
        return json_response({
            "status": "Failed",
            "error": str(ex),
            "error_type": type(ex).__name__,
        }, 500)


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
            "message": "Use POST with the four required files.",
            "required_files": REQUIRED_FILES,
            "run_lifecycle": RUN_LIFECYCLE_STATUSES,
        })

    process_started_at = time.perf_counter()
    current_step = "STEP 01 - Process request received"

    run_record = None
    storage = None
    stored_files = []

    def log_process_step(step_name: str, **details) -> None:
        nonlocal current_step
        current_step = step_name

        detail_text = " ".join(
            f"{key}={value}"
            for key, value in details.items()
        )

        logging.info(
            "[SFDA PROCESS] %s | total_seconds=%.3f%s",
            step_name,
            time.perf_counter() - process_started_at,
            f" | {detail_text}" if detail_text else "",
        )

    logging.info(
        "[SFDA PROCESS] STEP 01 - Process request received "
        "| method=%s | content_length=%s",
        req.method,
        req.headers.get("content-length"),
    )

    try:
        log_process_step(
            "STEP 02 - Validating required files"
        )
        missing_files = [
            file_key
            for file_key in REQUIRED_FILES
            if file_key not in req.files
        ]
        if missing_files:
            return json_response({
                "status": "Failed",
                "message": "Required files are missing.",
                "missing_files": missing_files,
            }, 400)

        submitted_by = (
            req.headers.get("x-ms-client-principal-name")
            or req.headers.get("x-submitted-by")
            or "Web User"
        )

        log_process_step(
            "STEP 03 - Creating reconciliation run",
            submitted_by=submitted_by,
        )

        run_record = create_reconciliation_run(
            submitted_by=submitted_by,
            application_version=APPLICATION_VERSION,
            asn_files=1,
            inventory_files=1,
            dispatch_files=1,
            sfda_files=1,
        )

        log_process_step(
            "STEP 04 - Reconciliation run created",
            run_id=run_record["run_id"],
            run_number=run_record["run_number"],
        )

        storage = get_blob_storage()

        log_process_step(
            "STEP 05 - Blob Storage initialized"
        )

        uploaded_files = {
            key: req.files[key]
            for key in REQUIRED_FILES
        }
        input_payloads = {}
        dataframes = {}
        files_summary = {}

        log_process_step(
            "STEP 06 - Reading input files",
            file_count=len(uploaded_files),
        )

        for file_key, uploaded_file in uploaded_files.items():
            file_started_at = time.perf_counter()
            file_name = _safe_file_name(
                uploaded_file.filename,
                f"{file_key}.xlsx",
            )
            file_bytes = uploaded_file.read()
            if not file_bytes:
                raise ValueError(f"Uploaded file is empty: {file_name}")

            content_type = _content_type_for(file_name)
            blob_info = storage.upload_input(
                run_number=run_record["run_number"],
                file_name=file_name,
                file_bytes=file_bytes,
                content_type=content_type,
            )
            _record_blob_file(
                run_record["run_id"],
                "input",
                file_key,
                file_name,
                blob_info,
            )
            stored_files.append(blob_info)
            input_payloads[file_key] = {
                "name": file_name,
                "bytes": file_bytes,
            }
            dataframe = read_excel_bytes(file_name, file_bytes)
            dataframes[file_key] = dataframe
            files_summary[file_key] = {
                "name": file_name,
                "rows": int(len(dataframe)),
                "columns": int(len(dataframe.columns)),
                "size_bytes": len(file_bytes),
            }

            logging.info(
                "[SFDA PROCESS] Input file completed "
                "| type=%s | name=%s | rows=%s "
                "| size_bytes=%s | seconds=%.3f",
                file_key,
                file_name,
                len(dataframe),
                len(file_bytes),
                time.perf_counter() - file_started_at,
            )

        log_process_step(
            "STEP 07 - All input files loaded",
            total_input_rows=sum(
                len(frame)
                for frame in dataframes.values()
            ),
        )

        reconciliation_engine = ReconciliationEngine(
            asn_df=dataframes["asn"],
            inventory_df=dataframes["inventory"],
            dispatch_df=dataframes["dispatch"],
            sfda_df=dataframes["sfda"],
        )
        log_process_step(
            "STEP 08 - Starting reconciliation engine"
        )
        reconciliation_started_at = time.perf_counter()

        result = reconciliation_engine.run()

        logging.info(
            "[SFDA PROCESS] Reconciliation engine completed "
            "| seconds=%.3f",
            time.perf_counter() - reconciliation_started_at,
        )

        log_process_step(
            "STEP 09 - Building batch history events"
        )
        batch_build_started_at = time.perf_counter()

        batch_events = BatchHistoryEngine.build(
            asn_df=reconciliation_engine.asn,
            inventory_df=reconciliation_engine.inventory,
            dispatch_df=reconciliation_engine.dispatch,
            sfda_df=reconciliation_engine.sfda,
            source_files={
                file_key: payload["name"]
                for file_key, payload
                in input_payloads.items()
            },
        )
        logging.info(
            "[SFDA PROCESS] Batch history events built "
            "| event_count=%s | seconds=%.3f",
            len(batch_events),
            time.perf_counter() - batch_build_started_at,
        )

        log_process_step(
            "STEP 10 - Saving batch history events to SQL",
            event_count=len(batch_events),
        )
        batch_save_started_at = time.perf_counter()

        batch_events_saved = record_batch_events(
            run_id=run_record["run_id"],
            events=batch_events,
        )

        logging.info(
            "[SFDA PROCESS] Batch history events saved "
            "| saved_count=%s | seconds=%.3f",
            batch_events_saved,
            time.perf_counter() - batch_save_started_at,
        )

        master = result["master"]
        accept = result["accept"]
        dispatch_output = result["dispatch"]
        variance = result["variance"]
        log_process_step(
            "STEP 11 - Building dashboard preview"
        )
        dashboard_preview = build_dashboard_preview(
            master=master,
            limit=100,
        )

        log_process_step(
            "STEP 12 - Generating output files",
            master_rows=len(master),
            accept_rows=len(accept),
            dispatch_rows=len(dispatch_output),
            variance_rows=len(variance),
        )
        output_generation_started_at = time.perf_counter()

        accept_files = Exporter.build_sfda_upload_files(
            df=accept,
            quantity_column="To Be Accept",
            file_prefix="Accept",
        )
        dispatch_files = Exporter.build_dispatch_files_by_customer(
            dispatch_df=dispatch_output
        )
        accept_details = build_accept_details(
            asn_df=dataframes["asn"],
            inventory_df=dataframes["inventory"],
            master=master,
        )
        dispatch_details = build_dispatch_details(
            dispatch_df=dataframes["dispatch"],
            dispatch_output=dispatch_output,
        )
        variance_report = build_variance_report(
            variance=variance
        )

        logging.info(
            "[SFDA PROCESS] Output files generated "
            "| accept_files=%s | dispatch_files=%s "
            "| seconds=%.3f",
            len(accept_files),
            len(dispatch_files),
            time.perf_counter() - output_generation_started_at,
        )

        output_groups = [
            (accept_files, "Accept.csv", "accept"),
            (dispatch_files, "Dispatch.csv", "dispatch"),
            (accept_details, "Accept_Details.xlsx", "accept_details"),
            (dispatch_details, "Dispatch_Details.xlsx", "dispatch_details"),
            (variance_report, "Variance_Report.xlsx", "variance"),
        ]
        generated_files_count = 0
        output_manifest = []

        log_process_step(
            "STEP 13 - Uploading output files to Blob Storage"
        )
        output_upload_started_at = time.perf_counter()

        for group, fallback_name, file_type in output_groups:
            for file_name, file_bytes, content_type, normalized_type in _iter_output_files(
                group,
                fallback_name,
                file_type,
            ):
                blob_info = storage.upload_output(
                    run_number=run_record["run_number"],
                    file_name=file_name,
                    file_bytes=file_bytes,
                    content_type=content_type or _content_type_for(file_name),
                )
                _record_blob_file(
                    run_record["run_id"],
                    "output",
                    normalized_type,
                    file_name,
                    blob_info,
                )
                stored_files.append(blob_info)
                output_manifest.append({
                    "file_name": file_name,
                    "file_type": normalized_type,
                    "container": blob_info["container"],
                    "blob_name": blob_info["blob_name"],
                    "size_bytes": blob_info["size_bytes"],
                    "content_type": blob_info["content_type"],
                })
                generated_files_count += 1

                logging.info(
                    "[SFDA PROCESS] Output file saved "
                    "| number=%s | type=%s | name=%s",
                    generated_files_count,
                    normalized_type,
                    file_name,
                )

        logging.info(
            "[SFDA PROCESS] Output upload completed "
            "| generated_files=%s | seconds=%.3f",
            generated_files_count,
            time.perf_counter() - output_upload_started_at,
        )

        total_rows = sum(
            file_info["rows"]
            for file_info in files_summary.values()
        )
        run_metadata = {
            "application": APPLICATION_NAME,
            "version": APPLICATION_VERSION,
            "run_id": run_record["run_id"],
            "run_number": run_record["run_number"],
            "status": RUN_STATUS_PENDING_UPLOAD,
            "lifecycle": {
                "current_status": RUN_STATUS_PENDING_UPLOAD,
                "allowed_statuses": RUN_LIFECYCLE_STATUSES,
                "next_status": RUN_STATUS_PENDING_VERIFICATION,
            },
            "submitted_by": submitted_by,
            "summary": {
                "files_count": len(files_summary),
                "total_input_rows": total_rows,
                "master_rows": int(len(master)),
                "accept_rows": int(len(accept)),
                "dispatch_rows": int(len(dispatch_output)),
                "variance_rows": int(len(variance)),
                "generated_files": generated_files_count,
                "batch_events_saved": batch_events_saved,
            },
            "inputs": files_summary,
            "outputs": output_manifest,
        }
        log_process_step(
            "STEP 14 - Creating run metadata"
        )

        metadata_bytes = json.dumps(
            run_metadata,
            ensure_ascii=False,
            indent=2,
            default=str,
        ).encode("utf-8")
        metadata_upload_started_at = time.perf_counter()

        metadata_blob = storage.upload_metadata(
            run_record["run_number"],
            metadata_bytes,
        )
        _record_blob_file(
            run_record["run_id"],
            "metadata",
            "run_json",
            "run.json",
            metadata_blob,
        )
        stored_files.append(metadata_blob)

        logging.info(
            "[SFDA PROCESS] Run metadata saved "
            "| seconds=%.3f",
            time.perf_counter() - metadata_upload_started_at,
        )

        log_process_step(
            "STEP 15 - Completing reconciliation run"
        )
        completion_started_at = time.perf_counter()

        complete_reconciliation_run(
            run_id=run_record["run_id"],
            total_input_rows=total_rows,
            master_records=int(len(master)),
            accept_records=int(len(accept)),
            dispatch_records=int(len(dispatch_output)),
            exception_records=int(len(variance)),
            generated_files=generated_files_count,
        )
        logging.info(
            "[SFDA PROCESS] Reconciliation run completed "
            "| seconds=%.3f",
            time.perf_counter() - completion_started_at,
        )

        log_process_step(
            "STEP 16 - Setting Pending Upload status"
        )

        lifecycle_status_persisted = _set_run_lifecycle_status(
            run_id=run_record["run_id"],
            status=RUN_STATUS_PENDING_UPLOAD,
        )

        log_process_step(
            "STEP 17 - Returning successful response",
            total_seconds=round(
                time.perf_counter() - process_started_at,
                3,
            ),
        )

        return json_response({
            "status": RUN_STATUS_PENDING_UPLOAD,
            "message": "Upload files generated. Upload them to the SFDA Portal, then return for verification.",
            "run_id": run_record["run_id"],
            "run_number": run_record["run_number"],
            "application": APPLICATION_NAME,
            "version": APPLICATION_VERSION,
            "timing": {
                "total_process_seconds": round(
                    time.perf_counter()
                    - process_started_at,
                    3,
                ),
            },
            "lifecycle": {
                "current_status": RUN_STATUS_PENDING_UPLOAD,
                "next_status": RUN_STATUS_PENDING_VERIFICATION,
                "allowed_statuses": RUN_LIFECYCLE_STATUSES,
                "status_persisted": lifecycle_status_persisted,
                "verification_readiness_url": f"/api/verification-readiness/{run_record['run_number']}",
            },
            "summary": {
                "files_count": len(files_summary),
                "total_input_rows": total_rows,
                "master_rows": int(len(master)),
                "accept_rows": int(len(accept)),
                "dispatch_rows": int(len(dispatch_output)),
                "variance_rows": int(len(variance)),
                "accept_files_count": len(accept_files),
                "dispatch_files_count": len(dispatch_files),
                "generated_files_count": generated_files_count,
                "batch_events_saved": batch_events_saved,
            },
            "dashboard_preview": dashboard_preview,
            "files": files_summary,
            "storage": {
                "inputs_saved": len(files_summary),
                "outputs_saved": generated_files_count,
                "metadata_saved": True,
                "batch_history_saved": batch_events_saved,
            },
            "outputs": {
                "accept_files": accept_files,
                "dispatch_files": dispatch_files,
                "accept_details": accept_details,
                "dispatch_details": dispatch_details,
                "variance": variance_report,
            },
        })

    except Exception as ex:
        total_elapsed = (
            time.perf_counter()
            - process_started_at
        )

        logging.exception(
            "[SFDA PROCESS] FAILED "
            "| current_step=%s | total_seconds=%.3f "
            "| error_type=%s | error=%s",
            current_step,
            total_elapsed,
            type(ex).__name__,
            str(ex),
        )
        if run_record is not None:
            try:
                fail_reconciliation_run(
                    run_id=run_record["run_id"],
                    error_message=(
                        f"{current_step}: {str(ex)}"
                    ),
                )
            except Exception:
                logging.exception("Unable to mark reconciliation run as Failed.")

            if storage is not None:
                try:
                    failure_metadata = {
                        "application": APPLICATION_NAME,
                        "version": APPLICATION_VERSION,
                        "run_id": run_record["run_id"],
                        "run_number": run_record["run_number"],
                        "status": "Failed",
                        "error": str(ex),
                        "error_type": type(ex).__name__,
                    }
                    metadata_blob = storage.upload_metadata(
                        run_record["run_number"],
                        json.dumps(
                            failure_metadata,
                            ensure_ascii=False,
                            indent=2,
                            default=str,
                        ).encode("utf-8"),
                    )
                    _record_blob_file(
                        run_record["run_id"],
                        "metadata",
                        "run_json",
                        "run.json",
                        metadata_blob,
                    )
                except Exception:
                    logging.exception("Unable to save failed run metadata.")

        return json_response({
            "status": "Failed",
            "run_id": run_record.get("run_id") if run_record else None,
            "run_number": run_record.get("run_number") if run_record else None,
            "error": str(ex),
            "error_type": type(ex).__name__,
            "trace": traceback.format_exc(),
        }, 500)

@app.route(
    route="verify",
    methods=["POST"],
)
def verify_upload(
    req: func.HttpRequest,
) -> func.HttpResponse:
    verification_id = None

    try:
        run_number = str(
            req.form.get("run_number")
            or req.form.get("run")
            or ""
        ).strip()

        if not run_number:
            return json_response({
                "status": "Failed",
                "message": "run_number is required.",
            }, 400)

        run = get_reconciliation_run_by_number(
            run_number
        )

        if run is None:
            return json_response({
                "status": "Failed",
                "message": "Run was not found.",
            }, 404)

        latest_sfda_file = (
            req.files.get("latest_sfda")
            or req.files.get(
                "verification_sfda"
            )
        )

        if latest_sfda_file is None:
            return json_response({
                "status": "Failed",
                "message": (
                    "Latest SFDA Drug Count "
                    "report is required."
                ),
            }, 400)

        notification_files = list(
            req.files.getlist(
                "notification_files"
            )
        )

        if not notification_files:
            notification_files = list(
                req.files.getlist(
                    "notifications"
                )
            )

        if not notification_files:
            return json_response({
                "status": "Failed",
                "message": (
                    "At least one Notification "
                    "Result file is required."
                ),
            }, 400)

        verification_id = (
            create_verification_run(
                run_id=int(run["RunID"]),
                latest_sfda_file_name=(
                    latest_sfda_file.filename
                    or "latest_sfda.xlsx"
                ),
                notification_files=len(
                    notification_files
                ),
            )
        )

        storage = get_blob_storage()
        run_files = get_reconciliation_run_files(
            int(run["RunID"])
        )

        expected_rows = []
        original_sfda_dataframe = None

        for file_record in run_files:
            category = str(
                file_record.get(
                    "FileCategory"
                )
                or ""
            ).lower()
            file_type = str(
                file_record.get("FileType")
                or ""
            ).lower()

            if (
                category == "output"
                and file_type
                in {"accept", "dispatch"}
            ):
                blob = storage.download_blob(
                    file_record[
                        "ContainerName"
                    ],
                    file_record["BlobName"],
                )
                expected_rows.extend(
                    VerificationEngine
                    .parse_generated_upload_file(
                        file_record["FileName"],
                        blob["data"],
                        file_type.upper(),
                    )
                )

            elif (
                category == "input"
                and file_type == "sfda"
            ):
                blob = storage.download_blob(
                    file_record[
                        "ContainerName"
                    ],
                    file_record["BlobName"],
                )
                original_sfda_dataframe = (
                    VerificationEngine
                    .read_tabular_bytes(
                        file_record["FileName"],
                        blob["data"],
                    )
                )

        if not expected_rows:
            raise ValueError(
                "No generated Accept or Dispatch "
                "upload files were found for this run."
            )

        if original_sfda_dataframe is None:
            raise ValueError(
                "The original SFDA input report "
                "was not found for this run."
            )

        latest_sfda_name = _safe_file_name(
            latest_sfda_file.filename,
            "latest_sfda.xlsx",
        )
        latest_sfda_bytes = (
            latest_sfda_file.read()
        )
        latest_sfda_dataframe = (
            VerificationEngine
            .read_tabular_bytes(
                latest_sfda_name,
                latest_sfda_bytes,
            )
        )

        latest_blob = storage.upload_input(
            run_number=run_number,
            file_name=(
                "verification/"
                + latest_sfda_name
            ),
            file_bytes=latest_sfda_bytes,
            content_type=(
                _content_type_for(
                    latest_sfda_name
                )
            ),
        )
        _record_blob_file(
            int(run["RunID"]),
            "verification",
            "latest_sfda",
            latest_sfda_name,
            latest_blob,
        )

        notification_rows = []

        for notification_file in (
            notification_files
        ):
            notification_name = (
                _safe_file_name(
                    notification_file.filename,
                    "notification.xlsx",
                )
            )
            notification_bytes = (
                notification_file.read()
            )

            notification_rows.extend(
                VerificationEngine
                .parse_notification_file(
                    notification_name,
                    notification_bytes,
                )
            )

            notification_blob = (
                storage.upload_input(
                    run_number=run_number,
                    file_name=(
                        "verification/"
                        + notification_name
                    ),
                    file_bytes=(
                        notification_bytes
                    ),
                    content_type=(
                        _content_type_for(
                            notification_name
                        )
                    ),
                )
            )
            _record_blob_file(
                int(run["RunID"]),
                "verification",
                "notification_result",
                notification_name,
                notification_blob,
            )

        verification_result = (
            VerificationEngine.verify(
                expected_rows=expected_rows,
                notification_rows=(
                    notification_rows
                ),
                original_sfda=(
                    original_sfda_dataframe
                ),
                latest_sfda=(
                    latest_sfda_dataframe
                ),
            )
        )

        saved_rows = (
            record_verification_results(
                verification_id=(
                    verification_id
                ),
                rows=verification_result[
                    "rows"
                ],
            )
        )

        complete_verification_run(
            verification_id=verification_id,
            status=verification_result[
                "status"
            ],
            summary=verification_result[
                "summary"
            ],
        )

        _set_run_lifecycle_status(
            run_id=int(run["RunID"]),
            status=verification_result[
                "status"
            ],
        )

        response_rows = []

        for row in verification_result[
            "rows"
        ][:500]:
            response_rows.append({
                key: value
                for key, value
                in row.items()
                if key not in {
                    "key",
                    "identity_key",
                }
            })

        return json_response({
            "status": verification_result[
                "status"
            ],
            "verification_id":
                verification_id,
            "run_number": run_number,
            "summary":
                verification_result[
                    "summary"
                ],
            "saved_result_rows":
                saved_rows,
            "results": response_rows,
            "results_truncated": (
                len(
                    verification_result[
                        "rows"
                    ]
                ) > 500
            ),
        })

    except Exception as ex:
        logging.exception(
            "Verification upload failed."
        )

        if verification_id is not None:
            try:
                fail_verification_run(
                    verification_id,
                    str(ex),
                )
            except Exception:
                logging.exception(
                    "Unable to mark verification "
                    "run as failed."
                )

        return json_response({
            "status": "Failed",
            "verification_id":
                verification_id,
            "error": str(ex),
            "error_type":
                type(ex).__name__,
            "trace":
                traceback.format_exc(),
        }, 500)
