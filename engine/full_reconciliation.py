from __future__ import annotations

import hashlib
from typing import Any, Dict, List

import pandas as pd

from engine.normalizer import Normalizer
from engine.validator import Validator


class FullReconciliationEngine:
    """Step 1: normalize ASN/Dispatch files and prepare incremental SQL events.

    Business grain:
        BN + Expiry Month + Generic Item Number

    SQL tables:
        ReceiptEvents
        DispatchEvents
        BatchMaster

    SFDA matching is performed using:
        BN + Expiry Month

    because the SFDA Drug Count report does not reliably contain the WMS
    Generic Item Number.
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
        "BN",
        "Expiry Month Key",
        "Expiry Date",
        "Generic Item Number",
        "Trade Item Number",
        "Trade Name",
        "GTIN",
        "Drug Name",
        "Total Receive Qty",
        "Total Dispatched Qty",
        "Receive Runs",
        "Dispatch Runs",
        "First Received Date",
        "Last Received Date",
        "First Dispatch Date",
        "Last Dispatch Date",
        "Generic Exists in SFDA",
        "Last Updated",
    ]

    def __init__(
        self,
        asn_df: pd.DataFrame,
        dispatch_df: pd.DataFrame,
        sfda_df: pd.DataFrame,
    ):
        self.asn = (
            asn_df.copy()
            if asn_df is not None
            else pd.DataFrame()
        )

        self.dispatch = (
            dispatch_df.copy()
            if dispatch_df is not None
            else pd.DataFrame()
        )

        self.sfda = (
            sfda_df.copy()
            if sfda_df is not None
            else pd.DataFrame()
        )

    @staticmethod
    def _month_key(series: pd.Series) -> pd.Series:
        return (
            Normalizer.date(series)
            .dt.strftime("%Y-%m")
            .fillna("")
        )

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
        raw_key = "|".join(
            cls._clean_key_part(part)
            for part in parts
        )

        return hashlib.sha256(
            raw_key.encode("utf-8")
        ).hexdigest()

    @staticmethod
    def _ensure_columns(
        dataframe: pd.DataFrame,
        columns: List[str],
    ) -> pd.DataFrame:
        frame = dataframe.copy()

        for column in columns:
            if column not in frame.columns:
                frame[column] = None

        return frame

    @staticmethod
    def _normalize_quantity(
        series: pd.Series,
    ) -> pd.Series:
        return (
            pd.to_numeric(
                series,
                errors="coerce",
            )
            .fillna(0)
        )

    def normalize(self) -> None:
        self.asn = Normalizer.normalize_asn(
            self.asn
        )

        self.dispatch = Normalizer.normalize_dispatch(
            self.dispatch
        )

        self.sfda = Normalizer.normalize_sfda(
            self.sfda
        )

        self.asn["Expiry Month Key"] = self._month_key(
            self.asn["Expiry Date"]
        )

        self.dispatch["Expiry Month Key"] = self._month_key(
            self.dispatch["Expiry Date"]
        )

        self.sfda["Expiry Month Key"] = self._month_key(
            self.sfda["Expiry Date"]
        )

        self.asn["Received Quantity"] = self._normalize_quantity(
            self.asn["Received Quantity"]
        )

        self.dispatch["Dispatched Quantity"] = self._normalize_quantity(
            self.dispatch["Dispatched Quantity"]
        )

    def validate(self) -> None:
        Validator.validate(
            self.asn,
            "ASN",
        )

        Validator.validate(
            self.dispatch,
            "DISPATCH",
        )

        Validator.validate(
            self.sfda,
            "SFDA",
        )

    def _sfda_keys(self) -> pd.DataFrame:
        required = [
            "BN",
            "Expiry Month Key",
            "Expiry Date",
            "GTIN",
            "Drug Name",
        ]

        sfda = self._ensure_columns(
            self.sfda,
            required,
        )[required].copy()

        sfda = sfda[
            (sfda["BN"].astype(str).str.strip() != "")
            & (
                sfda["Expiry Month Key"]
                .astype(str)
                .str.strip()
                != ""
            )
        ].copy()

        sfda["Expiry Date"] = Normalizer.date(
            sfda["Expiry Date"]
        )

        sfda = (
            sfda.sort_values(
                by=[
                    "BN",
                    "Expiry Month Key",
                    "Expiry Date",
                ],
                kind="stable",
            )
            .drop_duplicates(
                subset=self.SFDA_KEYS,
                keep="first",
            )
            .reset_index(drop=True)
        )

        return sfda

    def _receipt_events(self) -> pd.DataFrame:
        frame = self._ensure_columns(
            self.asn,
            self.RECEIPT_EVENT_COLUMNS,
        )

        frame = frame[
            (frame["BN"].astype(str).str.strip() != "")
            & (
                frame["Expiry Month Key"]
                .astype(str)
                .str.strip()
                != ""
            )
            & (
                frame["Generic Item Number"]
                .astype(str)
                .str.strip()
                != ""
            )
            & (
                pd.to_numeric(
                    frame["Received Quantity"],
                    errors="coerce",
                ).fillna(0)
                != 0
            )
        ].copy()

        sfda_keys = self._sfda_keys()[
            self.SFDA_KEYS
        ].copy()

        frame = frame.merge(
            sfda_keys,
            on=self.SFDA_KEYS,
            how="inner",
            validate="many_to_one",
        )

        frame["Event Key"] = frame.apply(
            lambda row: self._event_key([
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
            ]),
            axis=1,
        )

        frame = self._ensure_columns(
            frame,
            self.RECEIPT_EVENT_COLUMNS,
        )

        return (
            frame[self.RECEIPT_EVENT_COLUMNS]
            .drop_duplicates(
                subset=["Event Key"],
                keep="first",
            )
            .reset_index(drop=True)
        )

    def _dispatch_events(self) -> pd.DataFrame:
        frame = self._ensure_columns(
            self.dispatch,
            self.DISPATCH_EVENT_COLUMNS,
        )

        frame = frame[
            (frame["BN"].astype(str).str.strip() != "")
            & (
                frame["Expiry Month Key"]
                .astype(str)
                .str.strip()
                != ""
            )
            & (
                frame["Generic Item Number"]
                .astype(str)
                .str.strip()
                != ""
            )
            & (
                pd.to_numeric(
                    frame["Dispatched Quantity"],
                    errors="coerce",
                ).fillna(0)
                != 0
            )
        ].copy()

        sfda_keys = self._sfda_keys()[
            self.SFDA_KEYS
        ].copy()

        frame = frame.merge(
            sfda_keys,
            on=self.SFDA_KEYS,
            how="inner",
            validate="many_to_one",
        )

        frame["Event Key"] = frame.apply(
            lambda row: self._event_key([
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
            ]),
            axis=1,
        )

        frame = self._ensure_columns(
            frame,
            self.DISPATCH_EVENT_COLUMNS,
        )

        return (
            frame[self.DISPATCH_EVENT_COLUMNS]
            .drop_duplicates(
                subset=["Event Key"],
                keep="first",
            )
            .reset_index(drop=True)
        )

    def build_master_from_summaries(
        self,
        receipt_summary: pd.DataFrame,
        dispatch_summary: pd.DataFrame,
        sfda_summary: pd.DataFrame | None = None,
    ) -> pd.DataFrame:
        receipt = (
            receipt_summary.copy()
            if receipt_summary is not None
            else pd.DataFrame()
        )

        dispatch = (
            dispatch_summary.copy()
            if dispatch_summary is not None
            else pd.DataFrame()
        )

        receipt = self._ensure_columns(
            receipt,
            self.KEYS + [
                "Trade Item Number",
                "Trade Name",
                "Receive Runs",
                "Total Receive Qty",
                "First Received Date",
                "Last Received Date",
            ],
        )

        dispatch = self._ensure_columns(
            dispatch,
            self.KEYS + [
                "Trade Item Number",
                "Trade Name",
                "Dispatch Runs",
                "Total Dispatched Qty",
                "First Dispatch Date",
                "Last Dispatch Date",
            ],
        )

        if receipt.empty and dispatch.empty:
            return pd.DataFrame(
                columns=self.MASTER_COLUMNS
            )

        receipt = receipt.rename(
            columns={
                "Trade Item Number": (
                    "Receipt Trade Item Number"
                ),
                "Trade Name": "Receipt Trade Name",
            }
        )

        dispatch = dispatch.rename(
            columns={
                "Trade Item Number": (
                    "Dispatch Trade Item Number"
                ),
                "Trade Name": "Dispatch Trade Name",
            }
        )

        master = receipt.merge(
            dispatch,
            on=self.KEYS,
            how="outer",
            validate="one_to_one",
        )

        sfda = (
            self._sfda_keys()
            if sfda_summary is None
            else sfda_summary.copy()
        )

        sfda = self._ensure_columns(
            sfda,
            self.SFDA_KEYS + [
                "Expiry Date",
                "GTIN",
                "Drug Name",
            ],
        )

        sfda = sfda.drop_duplicates(
            subset=self.SFDA_KEYS,
            keep="first",
        )

        sfda_match = sfda[
            self.SFDA_KEYS
            + [
                "Expiry Date",
                "GTIN",
                "Drug Name",
            ]
        ].copy()
        sfda_match["_Batch Exists in SFDA"] = True

        master = master.merge(
            sfda_match,
            on=self.SFDA_KEYS,
            how="left",
            validate="many_to_one",
        )

        # A Generic is considered an SFDA Generic when at least one of its
        # batches exists in the latest SFDA report. Other batches for the same
        # Generic remain in the Batch Master as Missing Batch alerts.
        matched_generics = set(
            master.loc[
                master["_Batch Exists in SFDA"].fillna(False),
                "Generic Item Number",
            ]
            .fillna("")
            .astype(str)
            .str.strip()
        )

        master["Generic Exists in SFDA"] = "Generic Not in SFDA"
        master.loc[
            master["Generic Item Number"].isin(matched_generics),
            "Generic Exists in SFDA",
        ] = "Missing Batch"
        master.loc[
            master["_Batch Exists in SFDA"].fillna(False),
            "Generic Exists in SFDA",
        ] = "Yes"

        # The final Batch Master contains only Generics proven to be in SFDA.
        # Missing batches are retained for alerting and follow-up.
        master = master[
            master["Generic Exists in SFDA"] != "Generic Not in SFDA"
        ].copy()
        master = master.drop(columns=["_Batch Exists in SFDA"])

        master["Trade Item Number"] = (
            master["Receipt Trade Item Number"]
            .fillna("")
            .astype(str)
            .str.strip()
        )

        missing_trade_item = (
            master["Trade Item Number"] == ""
        )

        master.loc[
            missing_trade_item,
            "Trade Item Number",
        ] = (
            master.loc[
                missing_trade_item,
                "Dispatch Trade Item Number",
            ]
            .fillna("")
            .astype(str)
            .str.strip()
        )

        master["Trade Name"] = (
            master["Receipt Trade Name"]
            .fillna("")
            .astype(str)
            .str.strip()
        )

        missing_trade_name = (
            master["Trade Name"] == ""
        )

        master.loc[
            missing_trade_name,
            "Trade Name",
        ] = (
            master.loc[
                missing_trade_name,
                "Dispatch Trade Name",
            ]
            .fillna("")
            .astype(str)
            .str.strip()
        )

        numeric_defaults = {
            "Total Receive Qty": 0,
            "Total Dispatched Qty": 0,
            "Receive Runs": 0,
            "Dispatch Runs": 0,
        }

        for column, default in numeric_defaults.items():
            master[column] = (
                pd.to_numeric(
                    master[column],
                    errors="coerce",
                )
                .fillna(default)
            )

        master["Receive Runs"] = (
            master["Receive Runs"]
            .round(0)
            .astype(int)
        )

        master["Dispatch Runs"] = (
            master["Dispatch Runs"]
            .round(0)
            .astype(int)
        )

        for column in [
            "Expiry Date",
            "First Received Date",
            "Last Received Date",
            "First Dispatch Date",
            "Last Dispatch Date",
        ]:
            master[column] = pd.to_datetime(
                master[column],
                errors="coerce",
            )

        master["Last Updated"] = (
            pd.Timestamp.utcnow()
            .tz_localize(None)
        )

        master = self._ensure_columns(
            master,
            self.MASTER_COLUMNS,
        )

        return (
            master[self.MASTER_COLUMNS]
            .sort_values(
                by=self.KEYS,
                kind="stable",
            )
            .reset_index(drop=True)
        )

    @staticmethod
    def _records(
        dataframe: pd.DataFrame,
    ) -> List[Dict[str, Any]]:
        records: List[Dict[str, Any]] = []

        for row in dataframe.to_dict(
            orient="records"
        ):
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

    def prepare_incremental(
        self,
    ) -> Dict[str, Any]:
        self.normalize()
        self.validate()

        receipt_events = self._receipt_events()
        dispatch_events = self._dispatch_events()

        return {
            "receipt_events": receipt_events,
            "dispatch_events": dispatch_events,
            "receipt_records": self._records(
                receipt_events
            ),
            "dispatch_records": self._records(
                dispatch_events
            ),
            "sfda_summary": self._sfda_keys(),
        }

    def run(
        self,
        receipt_summary: pd.DataFrame | None = None,
        dispatch_summary: pd.DataFrame | None = None,
    ) -> Dict[str, Any]:
        prepared = self.prepare_incremental()

        if (
            receipt_summary is not None
            and dispatch_summary is not None
        ):
            master = self.build_master_from_summaries(
                receipt_summary,
                dispatch_summary,
                prepared["sfda_summary"],
            )

            prepared["master"] = master
            prepared["master_records"] = self._records(
                master
            )

        return prepared
