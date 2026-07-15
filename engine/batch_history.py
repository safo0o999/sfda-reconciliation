import hashlib
import re
from pathlib import Path
from typing import Dict, List, Optional

import pandas as pd


class BatchHistoryEngine:

    EVENT_RECEIVING = "RECEIVING"
    EVENT_DISPATCH = "DISPATCH"
    EVENT_INVENTORY_SNAPSHOT = "INVENTORY_SNAPSHOT"
    EVENT_SFDA_SNAPSHOT = "SFDA_SNAPSHOT"

    @staticmethod
    def _clean(value) -> str:
        if value is None:
            return ""

        try:
            if pd.isna(value):
                return ""
        except Exception:
            pass

        text = str(value).strip()

        if text.endswith(".0"):
            text = text[:-2]

        return text

    @staticmethod
    def _date_value(value):
        parsed = pd.to_datetime(
            value,
            errors="coerce",
            dayfirst=True
        )

        if pd.isna(parsed):
            return None

        return parsed.to_pydatetime()

    @staticmethod
    def _date_key(value) -> str:
        parsed = BatchHistoryEngine._date_value(value)

        if parsed is None:
            return ""

        return parsed.strftime("%Y-%m-%dT%H:%M:%S")

    @staticmethod
    def _expiry_value(value):
        parsed = pd.to_datetime(
            value,
            errors="coerce",
            dayfirst=True
        )

        if pd.isna(parsed):
            return None

        return parsed.date()

    @staticmethod
    def _quantity(value) -> float:
        parsed = pd.to_numeric(
            pd.Series([value]),
            errors="coerce"
        ).fillna(0).iloc[0]

        return float(parsed)

    @staticmethod
    def _file_date(file_name: Optional[str]):
        name = Path(
            str(file_name or "")
        ).name

        patterns = [
            r"(?P<y>20\d{2})[-_](?P<m>\d{2})[-_](?P<d>\d{2})[T _-]?(?P<h>\d{2})?(?P<mi>\d{2})?(?P<s>\d{2})?",
            r"(?P<d>\d{2})[-_](?P<m>\d{2})[-_](?P<y>20\d{2})"
        ]

        for pattern in patterns:
            match = re.search(
                pattern,
                name
            )

            if not match:
                continue

            values = match.groupdict()

            try:
                return pd.Timestamp(
                    year=int(values["y"]),
                    month=int(values["m"]),
                    day=int(values["d"]),
                    hour=int(values.get("h") or 0),
                    minute=int(values.get("mi") or 0),
                    second=int(values.get("s") or 0)
                ).to_pydatetime()
            except (TypeError, ValueError):
                continue

        return None

    @staticmethod
    def _first_non_blank(row, columns):
        for column in columns:
            if column not in row.index:
                continue

            value = BatchHistoryEngine._clean(
                row.get(column)
            )

            if value:
                return value

        return ""

    @staticmethod
    def _first_date(row, columns, fallback=None):
        for column in columns:
            if column not in row.index:
                continue

            value = BatchHistoryEngine._date_value(
                row.get(column)
            )

            if value is not None:
                return value

        return fallback

    @staticmethod
    def _event_key(parts) -> str:
        canonical = "|".join(
            BatchHistoryEngine._clean(part).upper()
            for part in parts
        )

        return hashlib.sha256(
            canonical.encode("utf-8")
        ).hexdigest()

    @staticmethod
    def _valid_batch_row(row):
        bn = BatchHistoryEngine._clean(
            row.get("BN")
        )
        expiry = BatchHistoryEngine._expiry_value(
            row.get("Expiry Date")
        )

        return bn, expiry

    @staticmethod
    def _aggregate_events(events: List[Dict]) -> List[Dict]:
        if not events:
            return []

        grouped = {}

        for event in events:
            key = event["event_key"]

            if key not in grouped:
                grouped[key] = event.copy()
                continue

            grouped[key]["quantity"] = (
                float(grouped[key].get("quantity") or 0)
                + float(event.get("quantity") or 0)
            )

        return list(grouped.values())

    @staticmethod
    def _receiving_events(
        asn_df,
        source_file_name
    ):
        fallback_date = BatchHistoryEngine._file_date(
            source_file_name
        )
        events = []

        for _, row in asn_df.iterrows():
            bn, expiry = BatchHistoryEngine._valid_batch_row(
                row
            )
            quantity = BatchHistoryEngine._quantity(
                row.get("Received Quantity")
            )

            if not bn or expiry is None or quantity <= 0:
                continue

            event_date = BatchHistoryEngine._first_date(
                row,
                [
                    "Received Date",
                    "Receipt Date",
                    "Actual Receipt Date",
                    "Date Received",
                    "ASN Closed Date",
                    "Closed Date"
                ],
                fallback=fallback_date
            )
            inbound_shipment = BatchHistoryEngine._first_non_blank(
                row,
                [
                    "Inbound Shipment",
                    "Inbound Shipment Number",
                    "TRK",
                    "TRK Number"
                ]
            )
            asn_line = BatchHistoryEngine._first_non_blank(
                row,
                [
                    "ASN Line",
                    "ASN Line Number"
                ]
            )
            source_reference = " | ".join(
                value
                for value in [
                    inbound_shipment,
                    asn_line
                ]
                if value
            )

            event_key = BatchHistoryEngine._event_key([
                BatchHistoryEngine.EVENT_RECEIVING,
                inbound_shipment,
                asn_line,
                bn,
                expiry.isoformat(),
                BatchHistoryEngine._date_key(event_date),
                BatchHistoryEngine._first_non_blank(
                    row,
                    [
                        "Trade Item",
                        "Trade Item Number"
                    ]
                )
            ])

            events.append({
                "event_key": event_key,
                "bn": bn,
                "expiry_date": expiry,
                "event_type": BatchHistoryEngine.EVENT_RECEIVING,
                "event_date": event_date,
                "quantity": quantity,
                "source_system": "WMS_ASN",
                "source_reference": source_reference or source_file_name,
                "generic_item_number": BatchHistoryEngine._first_non_blank(
                    row,
                    ["Generic Item Number"]
                ),
                "trade_item_number": BatchHistoryEngine._first_non_blank(
                    row,
                    [
                        "Trade Item",
                        "Trade Item Number"
                    ]
                ),
                "trade_name": BatchHistoryEngine._first_non_blank(
                    row,
                    [
                        "Trade Name",
                        "Trade Description"
                    ]
                ),
                "gtin": None,
                "supplier_name": BatchHistoryEngine._first_non_blank(
                    row,
                    ["Supplier Name"]
                ),
                "customer_name": None
            })

        return BatchHistoryEngine._aggregate_events(
            events
        )

    @staticmethod
    def _dispatch_events(
        dispatch_df,
        source_file_name
    ):
        fallback_date = BatchHistoryEngine._file_date(
            source_file_name
        )
        events = []

        for _, row in dispatch_df.iterrows():
            bn, expiry = BatchHistoryEngine._valid_batch_row(
                row
            )
            quantity = BatchHistoryEngine._quantity(
                row.get("Dispatched Quantity")
            )

            if not bn or expiry is None or quantity <= 0:
                continue

            event_date = BatchHistoryEngine._first_date(
                row,
                [
                    "Dispatch Date",
                    "Dispatched Date",
                    "Actual Dispatch Date",
                    "Ship Date",
                    "Shipment Date",
                    "Date Dispatched"
                ],
                fallback=fallback_date
            )
            sales_order = BatchHistoryEngine._first_non_blank(
                row,
                [
                    "Sales Order Number",
                    "Order Number"
                ]
            )
            order_line = BatchHistoryEngine._first_non_blank(
                row,
                [
                    "Order Line",
                    "order line"
                ]
            )
            customer = BatchHistoryEngine._first_non_blank(
                row,
                [
                    "To Address",
                    "Customer Name"
                ]
            )
            source_reference = " | ".join(
                value
                for value in [
                    sales_order,
                    order_line,
                    customer
                ]
                if value
            )

            event_key = BatchHistoryEngine._event_key([
                BatchHistoryEngine.EVENT_DISPATCH,
                sales_order,
                order_line,
                customer,
                bn,
                expiry.isoformat(),
                BatchHistoryEngine._date_key(event_date)
            ])

            events.append({
                "event_key": event_key,
                "bn": bn,
                "expiry_date": expiry,
                "event_type": BatchHistoryEngine.EVENT_DISPATCH,
                "event_date": event_date,
                "quantity": quantity,
                "source_system": "WMS_DISPATCH",
                "source_reference": source_reference or source_file_name,
                "generic_item_number": BatchHistoryEngine._first_non_blank(
                    row,
                    ["Generic Item Number"]
                ),
                "trade_item_number": BatchHistoryEngine._first_non_blank(
                    row,
                    ["Trade Item Number"]
                ),
                "trade_name": BatchHistoryEngine._first_non_blank(
                    row,
                    [
                        "Trade Name",
                        "Trade Description"
                    ]
                ),
                "gtin": None,
                "supplier_name": None,
                "customer_name": customer
            })

        return BatchHistoryEngine._aggregate_events(
            events
        )

    @staticmethod
    def _snapshot_events(
        dataframe,
        source_file_name,
        event_type,
        date_columns,
        quantity_column,
        source_system,
        gtin_column=None,
        trade_name_columns=None
    ):
        fallback_date = BatchHistoryEngine._file_date(
            source_file_name
        )
        events = []
        trade_name_columns = trade_name_columns or [
            "Trade Name"
        ]

        for _, row in dataframe.iterrows():
            bn, expiry = BatchHistoryEngine._valid_batch_row(
                row
            )
            quantity = BatchHistoryEngine._quantity(
                row.get(quantity_column)
            )

            if not bn or expiry is None:
                continue

            event_date = BatchHistoryEngine._first_date(
                row,
                date_columns,
                fallback=fallback_date
            )

            event_key = BatchHistoryEngine._event_key([
                event_type,
                source_file_name,
                bn,
                expiry.isoformat(),
                BatchHistoryEngine._date_key(event_date)
            ])

            events.append({
                "event_key": event_key,
                "bn": bn,
                "expiry_date": expiry,
                "event_type": event_type,
                "event_date": event_date,
                "quantity": quantity,
                "source_system": source_system,
                "source_reference": source_file_name,
                "generic_item_number": BatchHistoryEngine._first_non_blank(
                    row,
                    ["Generic Item Number"]
                ),
                "trade_item_number": BatchHistoryEngine._first_non_blank(
                    row,
                    [
                        "Trade Item Number",
                        "Trade Item"
                    ]
                ),
                "trade_name": BatchHistoryEngine._first_non_blank(
                    row,
                    trade_name_columns
                ),
                "gtin": (
                    BatchHistoryEngine._clean(
                        row.get(gtin_column)
                    )
                    if gtin_column
                    else None
                ),
                "supplier_name": None,
                "customer_name": None
            })

        return BatchHistoryEngine._aggregate_events(
            events
        )

    @staticmethod
    def build(
        asn_df,
        inventory_df,
        dispatch_df,
        sfda_df,
        source_files: Optional[Dict[str, str]] = None
    ) -> List[Dict]:

        source_files = source_files or {}

        receiving_events = (
            BatchHistoryEngine._receiving_events(
                asn_df,
                source_files.get("asn")
            )
        )

        dispatch_events = (
            BatchHistoryEngine._dispatch_events(
                dispatch_df,
                source_files.get("dispatch")
            )
        )

        inventory_events = (
            BatchHistoryEngine._snapshot_events(
                dataframe=inventory_df,
                source_file_name=source_files.get(
                    "inventory"
                ),
                event_type=(
                    BatchHistoryEngine
                    .EVENT_INVENTORY_SNAPSHOT
                ),
                date_columns=[
                    "Inventory Snapshot Date",
                    "Snapshot Date",
                    "Report Date",
                    "Inventory Date",
                    "Date Created",
                    "As Of Date"
                ],
                quantity_column="Available Quantity",
                source_system="WMS_INVENTORY",
                trade_name_columns=[
                    "Trade Name",
                    "Trade Item Description"
                ]
            )
        )

        sfda_events = (
            BatchHistoryEngine._snapshot_events(
                dataframe=sfda_df,
                source_file_name=source_files.get(
                    "sfda"
                ),
                event_type=(
                    BatchHistoryEngine
                    .EVENT_SFDA_SNAPSHOT
                ),
                date_columns=[
                    "SFDA Snapshot Date",
                    "Snapshot Date",
                    "Report Date",
                    "Drug Count Date",
                    "Date Created",
                    "As Of Date"
                ],
                quantity_column="Active",
                source_system="SFDA",
                gtin_column="GTIN",
                trade_name_columns=[
                    "Drug Name",
                    "Trade Name"
                ]
            )
        )

        return (
            receiving_events
            + dispatch_events
            + inventory_events
            + sfda_events
        )
