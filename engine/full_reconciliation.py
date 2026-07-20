from __future__ import annotations

import hashlib
from pathlib import Path
from typing import Any, Dict, List

import pandas as pd

from engine.normalizer import Normalizer
from engine.validator import Validator


class FullReconciliationEngine:
    """Build and update the cumulative historical Batch Master.

    WMS history grain:
        BN + Expiry Month + Generic Item Number

    Exact SFDA batch matching:
        BN + Expiry Month

    A WMS batch is retained when either:
        1. the exact BN + Expiry Month exists in SFDA; or
        2. another batch for the same WMS Generic Item Number is proven in SFDA.

    PackageSize is mapped only through:
        SFDA Drug Name -> config/pack_size.xlsx Trade Name
    """

    KEYS = [
        "BN",
        "Expiry Month Key",
        "Generic Item Number",
    ]

    SFDA_KEYS = [
        "BN",
        "Expiry Month Key",
    ]

    RECEIPT_EVENT_COLUMNS = [
        "Event Key",
        "BN",
        "Expiry Month Key",
        "Expiry Date",
        "Generic Item Number",
        "Trade Item",
        "Trade Name",
        "Description",
        "Item Family Group",
        "Received Quantity",
        "Inbound Shipment",
        "ASN Line",
        "Supplier Name",
        "Received Date",
    ]

    DISPATCH_EVENT_COLUMNS = [
        "Event Key",
        "BN",
        "Expiry Month Key",
        "Expiry Date",
        "Generic Item Number",
        "Trade Item Number",
        "Trade Name",
        "Dispatched Quantity",
        "To Address",
        "Sales Order Number",
        "Order Line",
        "Dispatch Date",
    ]

    MASTER_COLUMNS = [
        "GTIN",
        "Drug Name",
        "BN",
        "Expiry Date",
        "PackageSize",
        "Quantity",
        "Active",
        "Quantity sent pending",
        "Quantity Receive Pending",
        "Generic Item Number",
        "Description",
        "Trade Name",
        "Received Quantity Each",
        "Received Quantity Pack",
        "First Received Date",
        "Last Received Date",
        "Receive Runs",
        "Total Dispatched Qty",
        "First Dispatch Date",
        "Last Dispatch Date",
        "Dispatch Runs",
        "Generic Exists in SFDA",
        "Last Updated",
        "Item Family Group",
        # Internal key retained for SQL and matching, but excluded by Batch Master exporter.
        "Expiry Month Key",
        "Trade Item Number",
    ]

    def __init__(
        self,
        asn_df: pd.DataFrame,
        dispatch_df: pd.DataFrame,
        sfda_df: pd.DataFrame,
    ):
        self.asn = asn_df.copy() if asn_df is not None else pd.DataFrame()
        self.dispatch = dispatch_df.copy() if dispatch_df is not None else pd.DataFrame()
        self.sfda = sfda_df.copy() if sfda_df is not None else pd.DataFrame()

        config_path = Path(__file__).resolve().parent.parent / "config" / "pack_size.xlsx"
        self.packsize = pd.read_excel(
            config_path,
            engine="openpyxl",
            dtype=object,
        )

    @staticmethod
    def _month_key(series: pd.Series) -> pd.Series:
        return Normalizer.date(series).dt.strftime("%Y-%m").fillna("")

    @staticmethod
    def _clean_key_part(value: Any) -> str:
        try:
            if pd.isna(value):
                return ""
        except (TypeError, ValueError):
            pass

        if isinstance(value, pd.Timestamp):
            if pd.isna(value):
                return ""
            return value.isoformat()

        text = str(value).strip()
        if text.endswith(".0"):
            text = text[:-2]
        return text.upper()

    @classmethod
    def _event_key(cls, parts: List[Any]) -> str:
        raw_key = "|".join(cls._clean_key_part(part) for part in parts)
        return hashlib.sha256(raw_key.encode("utf-8")).hexdigest()

    @staticmethod
    def _ensure_columns(dataframe: pd.DataFrame, columns: List[str]) -> pd.DataFrame:
        frame = dataframe.copy()
        for column in columns:
            if column not in frame.columns:
                frame[column] = None
        return frame

    @staticmethod
    def _normalize_quantity(series: pd.Series) -> pd.Series:
        return pd.to_numeric(series, errors="coerce").fillna(0)

    @staticmethod
    def _first_non_blank(values: pd.Series) -> str:
        for value in values:
            if pd.isna(value):
                continue
            text = str(value).strip()
            if text and text.lower() != "nan":
                return text
        return ""

    def normalize(self) -> None:
        if not self.asn.empty:
            self.asn = Normalizer.normalize_asn(self.asn)
            self.asn["Expiry Month Key"] = self._month_key(self.asn["Expiry Date"])
            self.asn["Received Quantity"] = self._normalize_quantity(
                self.asn["Received Quantity"]
            )
        else:
            self.asn = pd.DataFrame(columns=self.RECEIPT_EVENT_COLUMNS)

        if not self.dispatch.empty:
            self.dispatch = Normalizer.normalize_dispatch(self.dispatch)
            self.dispatch["Expiry Month Key"] = self._month_key(
                self.dispatch["Expiry Date"]
            )
            self.dispatch["Dispatched Quantity"] = self._normalize_quantity(
                self.dispatch["Dispatched Quantity"]
            )
        else:
            self.dispatch = pd.DataFrame(columns=self.DISPATCH_EVENT_COLUMNS)

        self.sfda = Normalizer.normalize_sfda(self.sfda)
        self.sfda["Expiry Month Key"] = self._month_key(self.sfda["Expiry Date"])
        self.packsize = Normalizer.normalize_packsize(self.packsize)

    def validate(self) -> None:
        if not self.asn.empty:
            Validator.validate(self.asn, "ASN")
        if not self.dispatch.empty:
            Validator.validate(self.dispatch, "DISPATCH")
        Validator.validate(self.sfda, "SFDA")
        Validator.validate(self.packsize, "PACKSIZE")

    def _pack_lookup(self) -> pd.DataFrame:
        lookup = self.packsize[["Trade Name", "PackageSize"]].copy()
        lookup["Drug Name"] = Normalizer.text(lookup["Trade Name"])
        lookup["PackageSize"] = pd.to_numeric(lookup["PackageSize"], errors="coerce")
        lookup = lookup[
            lookup["Drug Name"].ne("")
            & lookup["PackageSize"].notna()
            & lookup["PackageSize"].gt(0)
        ].copy()
        return (
            lookup[["Drug Name", "PackageSize"]]
            .drop_duplicates(subset=["Drug Name"], keep="first")
            .reset_index(drop=True)
        )

    def _sfda_keys(self) -> pd.DataFrame:
        required = [
            "BN",
            "Expiry Month Key",
            "Expiry Date",
            "GTIN",
            "Drug Name",
            "Quantity",
            "Active",
            "Quantity sent pending",
            "Quantity Receive Pending",
        ]

        sfda = self._ensure_columns(self.sfda, required)[required].copy()
        sfda = sfda[
            sfda["BN"].astype(str).str.strip().ne("")
            & sfda["Expiry Month Key"].astype(str).str.strip().ne("")
        ].copy()

        sfda["Expiry Date"] = Normalizer.date(sfda["Expiry Date"])
        for column in [
            "Quantity",
            "Active",
            "Quantity sent pending",
            "Quantity Receive Pending",
        ]:
            sfda[column] = pd.to_numeric(sfda[column], errors="coerce").fillna(0)

        sfda = (
            sfda.groupby(self.SFDA_KEYS, dropna=False)
            .agg(
                **{
                    "Expiry Date": ("Expiry Date", "first"),
                    "GTIN": ("GTIN", "first"),
                    "Drug Name": ("Drug Name", "first"),
                    "Quantity": ("Quantity", "sum"),
                    "Active": ("Active", "sum"),
                    "Quantity sent pending": ("Quantity sent pending", "sum"),
                    "Quantity Receive Pending": ("Quantity Receive Pending", "sum"),
                }
            )
            .reset_index()
        )

        sfda = sfda.merge(
            self._pack_lookup(),
            on="Drug Name",
            how="left",
            validate="many_to_one",
        )
        return sfda

    def _receipt_events(self) -> pd.DataFrame:
        frame = self._ensure_columns(self.asn, self.RECEIPT_EVENT_COLUMNS)
        frame = frame[
            frame["BN"].astype(str).str.strip().ne("")
            & frame["Expiry Month Key"].astype(str).str.strip().ne("")
            & frame["Generic Item Number"].astype(str).str.strip().ne("")
            & pd.to_numeric(frame["Received Quantity"], errors="coerce").fillna(0).ne(0)
        ].copy()

        # Do not inner-join to SFDA here. Missing WMS batches must reach the
        # final master so they can be classified as Missing Batch in SFDA.
        frame["Event Key"] = frame.apply(
            lambda row: self._event_key(
                [
                    "RECEIPT",
                    row.get("Inbound Shipment"),
                    row.get("ASN Line"),
                    row.get("BN"),
                    row.get("Expiry Month Key"),
                    row.get("Generic Item Number"),
                    row.get("Trade Item"),
                    row.get("Received Date"),
                    row.get("Received Quantity"),
                    row.get("_Source File"),
                ]
            ),
            axis=1,
        )
        frame = self._ensure_columns(frame, self.RECEIPT_EVENT_COLUMNS)
        return (
            frame[self.RECEIPT_EVENT_COLUMNS]
            .drop_duplicates(subset=["Event Key"], keep="first")
            .reset_index(drop=True)
        )

    def _dispatch_events(self) -> pd.DataFrame:
        frame = self._ensure_columns(self.dispatch, self.DISPATCH_EVENT_COLUMNS)
        frame = frame[
            frame["BN"].astype(str).str.strip().ne("")
            & frame["Expiry Month Key"].astype(str).str.strip().ne("")
            & frame["Generic Item Number"].astype(str).str.strip().ne("")
            & pd.to_numeric(frame["Dispatched Quantity"], errors="coerce").fillna(0).ne(0)
        ].copy()

        frame["Event Key"] = frame.apply(
            lambda row: self._event_key(
                [
                    "DISPATCH",
                    row.get("Sales Order Number"),
                    row.get("Order Line"),
                    row.get("To Address"),
                    row.get("BN"),
                    row.get("Expiry Month Key"),
                    row.get("Generic Item Number"),
                    row.get("Trade Item Number"),
                    row.get("Dispatch Date"),
                    row.get("Dispatched Quantity"),
                    row.get("_Source File"),
                ]
            ),
            axis=1,
        )
        frame = self._ensure_columns(frame, self.DISPATCH_EVENT_COLUMNS)
        return (
            frame[self.DISPATCH_EVENT_COLUMNS]
            .drop_duplicates(subset=["Event Key"], keep="first")
            .reset_index(drop=True)
        )

    def build_master_from_summaries(
        self,
        receipt_summary: pd.DataFrame,
        dispatch_summary: pd.DataFrame,
        sfda_summary: pd.DataFrame | None = None,
    ) -> pd.DataFrame:
        receipt = receipt_summary.copy() if receipt_summary is not None else pd.DataFrame()
        dispatch = dispatch_summary.copy() if dispatch_summary is not None else pd.DataFrame()

        receipt = self._ensure_columns(
            receipt,
            self.KEYS
            + [
                "Trade Item Number",
                "Trade Name",
                "Description",
                "Item Family Group",
                "Receive Runs",
                "Total Receive Qty",
                "First Received Date",
                "Last Received Date",
            ],
        )
        dispatch = self._ensure_columns(
            dispatch,
            self.KEYS
            + [
                "Trade Item Number",
                "Trade Name",
                "Dispatch Runs",
                "Total Dispatched Qty",
                "First Dispatch Date",
                "Last Dispatch Date",
            ],
        )

        if receipt.empty and dispatch.empty:
            return pd.DataFrame(columns=self.MASTER_COLUMNS)

        receipt = receipt.rename(
            columns={
                "Trade Item Number": "Receipt Trade Item Number",
                "Trade Name": "Receipt Trade Name",
            }
        )
        dispatch = dispatch.rename(
            columns={
                "Trade Item Number": "Dispatch Trade Item Number",
                "Trade Name": "Dispatch Trade Name",
            }
        )

        master = receipt.merge(dispatch, on=self.KEYS, how="outer", validate="one_to_one")

        sfda = self._sfda_keys() if sfda_summary is None else sfda_summary.copy()
        sfda_columns = self.SFDA_KEYS + [
            "Expiry Date",
            "GTIN",
            "Drug Name",
            "PackageSize",
            "Quantity",
            "Active",
            "Quantity sent pending",
            "Quantity Receive Pending",
        ]
        sfda = self._ensure_columns(sfda, sfda_columns)
        sfda = sfda.drop_duplicates(subset=self.SFDA_KEYS, keep="first")
        sfda_match = sfda[sfda_columns].copy()
        sfda_match["_Batch Exists in SFDA"] = True

        master = master.merge(
            sfda_match,
            on=self.SFDA_KEYS,
            how="left",
            validate="many_to_one",
        )

        matched_generics = set(
            master.loc[
                master["_Batch Exists in SFDA"].eq(True),
                "Generic Item Number",
            ]
            .fillna("")
            .astype(str)
            .str.strip()
        )
        matched_generics.discard("")

        master["Generic Exists in SFDA"] = "Generic Not in SFDA"
        master.loc[
            master["Generic Item Number"].isin(matched_generics),
            "Generic Exists in SFDA",
        ] = "Missing Batch in SFDA"
        master.loc[
            master["_Batch Exists in SFDA"].eq(True),
            "Generic Exists in SFDA",
        ] = "Yes"

        master = master[
            master["Generic Exists in SFDA"].ne("Generic Not in SFDA")
        ].copy()

        # Build a Generic-to-SFDA reference from exact matches. This allows a
        # missing batch to show the correct drug and package size while its
        # batch-level SFDA quantities remain zero.
        generic_reference = (
            master.loc[
                master["_Batch Exists in SFDA"].eq(True),
                ["Generic Item Number", "GTIN", "Drug Name", "PackageSize"],
            ]
            .drop_duplicates(subset=["Generic Item Number"], keep="first")
            .rename(
                columns={
                    "GTIN": "_Generic GTIN",
                    "Drug Name": "_Generic Drug Name",
                    "PackageSize": "_Generic PackageSize",
                }
            )
        )
        master = master.merge(
            generic_reference,
            on="Generic Item Number",
            how="left",
            validate="many_to_one",
        )

        batch_exists_mask = master["_Batch Exists in SFDA"].eq(True)
        missing_batch_mask = batch_exists_mask.eq(False)
        master.loc[missing_batch_mask, "GTIN"] = master.loc[
            missing_batch_mask, "_Generic GTIN"
        ]
        master.loc[missing_batch_mask, "Drug Name"] = master.loc[
            missing_batch_mask, "_Generic Drug Name"
        ]
        master.loc[missing_batch_mask, "PackageSize"] = master.loc[
            missing_batch_mask, "_Generic PackageSize"
        ]

        for column in [
            "Quantity",
            "Active",
            "Quantity sent pending",
            "Quantity Receive Pending",
        ]:
            master[column] = pd.to_numeric(master[column], errors="coerce").fillna(0)
            master.loc[missing_batch_mask, column] = 0

        master["Trade Item Number"] = (
            master["Receipt Trade Item Number"].fillna("").astype(str).str.strip()
        )
        missing_trade_item = master["Trade Item Number"].eq("")
        master.loc[missing_trade_item, "Trade Item Number"] = (
            master.loc[missing_trade_item, "Dispatch Trade Item Number"]
            .fillna("")
            .astype(str)
            .str.strip()
        )

        master["Trade Name"] = (
            master["Receipt Trade Name"].fillna("").astype(str).str.strip()
        )
        missing_trade_name = master["Trade Name"].eq("")
        master.loc[missing_trade_name, "Trade Name"] = (
            master.loc[missing_trade_name, "Dispatch Trade Name"]
            .fillna("")
            .astype(str)
            .str.strip()
        )

        for column in [
            "Total Receive Qty",
            "Total Dispatched Qty",
            "Receive Runs",
            "Dispatch Runs",
            "PackageSize",
        ]:
            master[column] = pd.to_numeric(master[column], errors="coerce").fillna(0)

        master["Received Quantity Each"] = master["Total Receive Qty"]
        valid_package = master["PackageSize"].gt(0)
        master["Received Quantity Pack"] = 0.0
        master.loc[valid_package, "Received Quantity Pack"] = (
            master.loc[valid_package, "Received Quantity Each"]
            / master.loc[valid_package, "PackageSize"]
        )

        master["Receive Runs"] = master["Receive Runs"].round(0).astype(int)
        master["Dispatch Runs"] = master["Dispatch Runs"].round(0).astype(int)

        for column in [
            "Expiry Date",
            "First Received Date",
            "Last Received Date",
            "First Dispatch Date",
            "Last Dispatch Date",
        ]:
            master[column] = pd.to_datetime(master[column], errors="coerce")

        master["Last Updated"] = pd.Timestamp.utcnow().tz_localize(None)
        master = master.drop(
            columns=[
                "_Batch Exists in SFDA",
                "_Generic GTIN",
                "_Generic Drug Name",
                "_Generic PackageSize",
            ],
            errors="ignore",
        )
        master = self._ensure_columns(master, self.MASTER_COLUMNS)
        return (
            master[self.MASTER_COLUMNS]
            .sort_values(by=self.KEYS, kind="stable")
            .reset_index(drop=True)
        )

    @staticmethod
    def _records(dataframe: pd.DataFrame) -> List[Dict[str, Any]]:
        records: List[Dict[str, Any]] = []
        for row in dataframe.to_dict(orient="records"):
            clean: Dict[str, Any] = {}
            for key, value in row.items():
                try:
                    if pd.isna(value):
                        clean[key] = None
                        continue
                except (TypeError, ValueError):
                    pass

                if isinstance(value, pd.Timestamp):
                    clean[key] = value.to_pydatetime()
                elif hasattr(value, "item"):
                    try:
                        clean[key] = value.item()
                    except Exception:
                        clean[key] = value
                else:
                    clean[key] = value
            records.append(clean)
        return records

    def prepare_incremental(self) -> Dict[str, Any]:
        self.normalize()
        self.validate()
        receipt_events = self._receipt_events()
        dispatch_events = self._dispatch_events()
        return {
            "receipt_events": receipt_events,
            "dispatch_events": dispatch_events,
            "receipt_records": self._records(receipt_events),
            "dispatch_records": self._records(dispatch_events),
            "sfda_summary": self._sfda_keys(),
        }

    def run(
        self,
        receipt_summary: pd.DataFrame | None = None,
        dispatch_summary: pd.DataFrame | None = None,
    ) -> Dict[str, Any]:
        prepared = self.prepare_incremental()
        if receipt_summary is not None and dispatch_summary is not None:
            master = self.build_master_from_summaries(
                receipt_summary,
                dispatch_summary,
                prepared["sfda_summary"],
            )
            prepared["master"] = master
            prepared["master_records"] = self._records(master)
        return prepared
