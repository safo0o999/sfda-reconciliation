from __future__ import annotations

import hashlib
import logging
import time
from pathlib import Path
from typing import Any, Dict, Iterable, List, Sequence

import pandas as pd

from engine.normalizer import Normalizer
from engine.validator import Validator


logger = logging.getLogger("SFDA-Reconciliation.FullReconciliation")


class FullReconciliationEngine:
    """Historical-data and full-reconciliation engine.

    Stage 1 — Historical Data Builder
        Builds and updates:
            - Batch Master
            - Supplier History
            - Customer History
            - Receipt Events
            - Dispatch Events

    Stage 2 — Full Reconciliation
        Uses the persisted historical tables with:
            - Current Inventory
            - Latest SFDA Drug Count

    Historical WMS grain:
        BN + Expiry Month + Generic Item Number

    Exact SFDA batch matching:
        BN + Expiry Month

    A WMS batch is retained when either:
        1. the exact BN + Expiry Month exists in SFDA; or
        2. another batch for the same WMS Generic Item Number is proven in SFDA.

    PackageSize mapping:
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
        "Supplier Code",
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
        "Trade Description",
        "Supplier Name",
        "Supplier Code",
        "Received Quantity Each",
        "Received Quantity Pack",
        "First Received Date",
        "Last Received Date",
        "Total Dispatched Qty",
        "Total Dispatched Qty Pack",
        "First Dispatch Date",
        "Last Dispatch Date",
        "Generic Exists in SFDA",
        "Last Updated",
        "Item Family Group",
        # Internal key retained for SQL and matching, but excluded by Batch Master exporter.
        "Expiry Month Key",
        "Trade Item Number",
    ]

    SUPPLIER_HISTORY_COLUMNS = [
        "Supplier Name",
        "Supplier Code",
        "GTIN",
        "Drug Name",
        "Generic Item Number",
        "Description",
        "Trade Description",
        "BN",
        "Expiry Date",
        "PackageSize",
        "Received Quantity Each",
        "Received Quantity Pack",
        "First Received Date",
        "Last Received Date",
        "Item Family Group",
        "Expiry Month Key",
        "Trade Item Number",
    ]

    CUSTOMER_HISTORY_COLUMNS = [
        "To Address",
        "GLN",
        "GTIN",
        "Drug Name",
        "Generic Item Number",
        "Trade Description",
        "BN",
        "Expiry Date",
        "PackageSize",
        "Dispatch Quantity Each",
        "Dispatch Quantity Pack",
        "First Dispatch Date",
        "Last Dispatch Date",
        "Expiry Month Key",
        "Trade Item Number",
    ]

    ACCEPT_RECONCILIATION_COLUMNS = [
        "GTIN",
        "Drug Name",
        "BN",
        "Expiry Date",
        "Expiry Month Key",
        "Generic Item Number",
        "PackageSize",
        "Historical Received Quantity Each",
        "Historical Received Quantity Pack",
        "SFDA Quantity",
        "SFDA Active",
        "Quantity Sent Pending",
        "Quantity Receive Pending",
        "To Be Accept",
        "Reconciliation Status",
    ]

    SUPPLIER_VARIANCE_COLUMNS = [
        "Supplier Name",
        "Supplier Code",
        "GTIN",
        "Drug Name",
        "BN",
        "Expiry Date",
        "Expiry Month Key",
        "Generic Item Number",
        "Historical Received Quantity Each",
        "Historical Received Quantity Pack",
        "SFDA Supplier Quantity",
        "Supplier Variance",
        "Variance Status",
        "Required Action",
    ]

    DISPATCH_RECONCILIATION_COLUMNS = [
        "To Address",
        "GLN",
        "GTIN",
        "Drug Name",
        "BN",
        "Expiry Date",
        "Expiry Month Key",
        "Generic Item Number",
        "PackageSize",
        "Historical Dispatch Quantity Each",
        "Historical Dispatch Quantity Pack",
        "Current Inventory Quantity Each",
        "Current Inventory Quantity Pack",
        "SFDA Quantity",
        "SFDA Active",
        "Quantity Sent Pending",
        "Quantity Receive Pending",
        "To Be Dispatch",
        "Reconciliation Status",
    ]

    RECONCILIATION_SUMMARY_COLUMNS = [
        "Metric",
        "Value",
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
        gln_path = Path(__file__).resolve().parent.parent / "config" / "gln.xlsx"
        self.gln = pd.read_excel(
            gln_path,
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
    def _event_key(cls, parts: Sequence[Any]) -> str:
        raw_key = "|".join(cls._clean_key_part(part) for part in parts)
        return hashlib.sha256(raw_key.encode("utf-8")).hexdigest()

    @classmethod
    def _build_event_keys(
        cls,
        event_type: str,
        columns: Iterable[pd.Series],
    ) -> List[str]:
        """Build deterministic hashes without the high overhead of DataFrame.apply."""

        clean = cls._clean_key_part
        prefix = clean(event_type)
        keys: List[str] = []

        for values in zip(*columns):
            raw_key = "|".join([prefix, *(clean(value) for value in values)])
            keys.append(hashlib.sha256(raw_key.encode("utf-8")).hexdigest())

        return keys

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
        started_at = time.perf_counter()
        logger.info(
            "Normalization started. asn_rows=%s dispatch_rows=%s sfda_rows=%s",
            len(self.asn),
            len(self.dispatch),
            len(self.sfda),
        )

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
        logger.info(
            "Normalization completed in %.2f seconds.",
            time.perf_counter() - started_at,
        )

    def validate(self) -> None:
        started_at = time.perf_counter()
        if not self.asn.empty:
            Validator.validate(self.asn, "ASN")
        if not self.dispatch.empty:
            Validator.validate(self.dispatch, "DISPATCH")
        Validator.validate(self.sfda, "SFDA")
        Validator.validate(self.packsize, "PACKSIZE")
        logger.info(
            "Validation completed in %.2f seconds.",
            time.perf_counter() - started_at,
        )

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
        started_at = time.perf_counter()
        source_columns = self.RECEIPT_EVENT_COLUMNS + ["_Source File"]
        frame = self._ensure_columns(self.asn, source_columns)

        received_quantity = pd.to_numeric(
            frame["Received Quantity"],
            errors="coerce",
        ).fillna(0)

        valid_mask = (
            frame["BN"].astype(str).str.strip().ne("")
            & frame["Expiry Month Key"].astype(str).str.strip().ne("")
            & frame["Generic Item Number"].astype(str).str.strip().ne("")
            & received_quantity.ne(0)
        )
        frame = frame.loc[valid_mask].copy()
        frame["Received Quantity"] = received_quantity.loc[valid_mask].to_numpy()

        # Missing WMS batches must remain available for final SFDA classification.
        frame["Event Key"] = self._build_event_keys(
            "RECEIPT",
            [
                frame["Inbound Shipment"],
                frame["ASN Line"],
                frame["BN"],
                frame["Expiry Month Key"],
                frame["Generic Item Number"],
                frame["Trade Item"],
                frame["Received Date"],
                frame["Received Quantity"],
                frame["_Source File"],
            ],
        )

        result = (
            frame[self.RECEIPT_EVENT_COLUMNS]
            .drop_duplicates(subset=["Event Key"], keep="first")
            .reset_index(drop=True)
        )
        logger.info(
            "Receipt events prepared in %.2f seconds. input_rows=%s valid_rows=%s unique_events=%s",
            time.perf_counter() - started_at,
            len(self.asn),
            len(frame),
            len(result),
        )
        return result

    def _dispatch_events(self) -> pd.DataFrame:
        started_at = time.perf_counter()
        source_columns = self.DISPATCH_EVENT_COLUMNS + ["_Source File"]
        frame = self._ensure_columns(self.dispatch, source_columns)

        dispatched_quantity = pd.to_numeric(
            frame["Dispatched Quantity"],
            errors="coerce",
        ).fillna(0)

        valid_mask = (
            frame["BN"].astype(str).str.strip().ne("")
            & frame["Expiry Month Key"].astype(str).str.strip().ne("")
            & frame["Generic Item Number"].astype(str).str.strip().ne("")
            & dispatched_quantity.ne(0)
        )
        frame = frame.loc[valid_mask].copy()
        frame["Dispatched Quantity"] = dispatched_quantity.loc[valid_mask].to_numpy()

        frame["Event Key"] = self._build_event_keys(
            "DISPATCH",
            [
                frame["Sales Order Number"],
                frame["Order Line"],
                frame["To Address"],
                frame["BN"],
                frame["Expiry Month Key"],
                frame["Generic Item Number"],
                frame["Trade Item Number"],
                frame["Dispatch Date"],
                frame["Dispatched Quantity"],
                frame["_Source File"],
            ],
        )

        result = (
            frame[self.DISPATCH_EVENT_COLUMNS]
            .drop_duplicates(subset=["Event Key"], keep="first")
            .reset_index(drop=True)
        )
        logger.info(
            "Dispatch events prepared in %.2f seconds. input_rows=%s valid_rows=%s unique_events=%s",
            time.perf_counter() - started_at,
            len(self.dispatch),
            len(frame),
            len(result),
        )
        return result

    def build_master_from_summaries(
        self,
        receipt_summary: pd.DataFrame,
        dispatch_summary: pd.DataFrame,
        sfda_summary: pd.DataFrame | None = None,
    ) -> pd.DataFrame:
        started_at = time.perf_counter()
        receipt = receipt_summary.copy() if receipt_summary is not None else pd.DataFrame()
        dispatch = dispatch_summary.copy() if dispatch_summary is not None else pd.DataFrame()

        receipt = self._ensure_columns(
            receipt,
            self.KEYS
            + [
                "Trade Item Number",
                "Trade Name",
                "Description",
                "Supplier Name",
                "Supplier Code",
                "Item Family Group",
                "Receipt Expiry Date",
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
                "Dispatch Expiry Date",
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

        # Expiry priority for Batch Master:
        # 1. Exact SFDA expiry
        # 2. ASN Receipt Expiration Date
        # 3. Full Dispatch Best Before Date
        sfda_expiry = pd.to_datetime(
            master["Expiry Date"],
            errors="coerce",
        )
        receipt_expiry = pd.to_datetime(
            master["Receipt Expiry Date"],
            errors="coerce",
        )
        dispatch_expiry = pd.to_datetime(
            master["Dispatch Expiry Date"],
            errors="coerce",
        )
        master["Expiry Date"] = (
            sfda_expiry
            .combine_first(receipt_expiry)
            .combine_first(dispatch_expiry)
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

        master["Trade Description"] = (
            master["Receipt Trade Name"].fillna("").astype(str).str.strip()
        )
        missing_trade_description = master["Trade Description"].eq("")
        master.loc[missing_trade_description, "Trade Description"] = (
            master.loc[missing_trade_description, "Dispatch Trade Name"]
            .fillna("")
            .astype(str)
            .str.strip()
        )

        for column in [
            "Total Receive Qty",
            "Total Dispatched Qty",
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

        master["Total Dispatched Qty Pack"] = 0.0
        master.loc[valid_package, "Total Dispatched Qty Pack"] = (
            master.loc[valid_package, "Total Dispatched Qty"]
            / master.loc[valid_package, "PackageSize"]
        )

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
        result = (
            master[self.MASTER_COLUMNS]
            .sort_values(by=self.KEYS, kind="stable")
            .reset_index(drop=True)
        )
        logger.info(
            "Batch Master built in %.2f seconds. receipt_groups=%s dispatch_groups=%s master_rows=%s",
            time.perf_counter() - started_at,
            len(receipt),
            len(dispatch),
            len(result),
        )
        return result

    def build_supplier_history(
        self,
        supplier_summary: pd.DataFrame,
        master: pd.DataFrame,
    ) -> pd.DataFrame:
        """Build one historical row per supplier and WMS batch."""

        source = (
            supplier_summary.copy()
            if supplier_summary is not None
            else pd.DataFrame()
        )
        required = [
            "Supplier Name",
            "Supplier Code",
            *self.KEYS,
            "Expiry Date",
            "Trade Item Number",
            "Trade Name",
            "Description",
            "Item Family Group",
            "Received Quantity Each",
            "First Received Date",
            "Last Received Date",
        ]
        source = self._ensure_columns(source, required)

        if source.empty:
            return pd.DataFrame(columns=self.SUPPLIER_HISTORY_COLUMNS)

        source = source.rename(
            columns={
                "Expiry Date": "Receipt Expiry Date",
            }
        )

        reference_columns = self.KEYS + [
            "GTIN",
            "Drug Name",
            "Expiry Date",
            "PackageSize",
            "Trade Description",
        ]
        reference = self._ensure_columns(
            master,
            reference_columns,
        )[reference_columns].copy()
        reference = reference.rename(
            columns={
                "Expiry Date": "Master Expiry Date",
            }
        )
        reference = reference.drop_duplicates(
            subset=self.KEYS,
            keep="first",
        )

        result = source.merge(
            reference,
            on=self.KEYS,
            how="inner",
            validate="many_to_one",
        )

        receipt_expiry = pd.to_datetime(
            result["Receipt Expiry Date"],
            errors="coerce",
        )
        master_expiry = pd.to_datetime(
            result["Master Expiry Date"],
            errors="coerce",
        )
        result["Expiry Date"] = receipt_expiry.combine_first(
            master_expiry
        )
        result = result.drop(
            columns=[
                "Receipt Expiry Date",
                "Master Expiry Date",
            ],
            errors="ignore",
        )

        result["Trade Description"] = (
            result["Trade Description"].fillna("")
        )
        missing_trade = (
            result["Trade Description"]
            .astype(str)
            .str.strip()
            .eq("")
        )
        result.loc[
            missing_trade,
            "Trade Description",
        ] = result.loc[
            missing_trade,
            "Trade Name",
        ]

        result["Received Quantity Each"] = pd.to_numeric(
            result["Received Quantity Each"],
            errors="coerce",
        ).fillna(0)
        result["PackageSize"] = pd.to_numeric(
            result["PackageSize"],
            errors="coerce",
        ).fillna(0)

        result["Received Quantity Pack"] = 0.0
        valid_pack = result["PackageSize"].gt(0)
        result.loc[
            valid_pack,
            "Received Quantity Pack",
        ] = (
            result.loc[
                valid_pack,
                "Received Quantity Each",
            ]
            / result.loc[
                valid_pack,
                "PackageSize",
            ]
        )

        result = self._ensure_columns(
            result,
            self.SUPPLIER_HISTORY_COLUMNS,
        )

        return (
            result[self.SUPPLIER_HISTORY_COLUMNS]
            .sort_values(
                [
                    "Supplier Name",
                    "Generic Item Number",
                    "BN",
                    "Expiry Date",
                ],
                kind="stable",
            )
            .reset_index(drop=True)
        )

    def build_customer_history(
        self,
        customer_summary: pd.DataFrame,
        master: pd.DataFrame,
    ) -> pd.DataFrame:
        """Build one historical row per customer and WMS batch, enriched with GLN."""

        source = (
            customer_summary.copy()
            if customer_summary is not None
            else pd.DataFrame()
        )
        required = [
            "To Address",
            *self.KEYS,
            "Expiry Date",
            "Trade Item Number",
            "Trade Name",
            "Dispatch Quantity Each",
            "First Dispatch Date",
            "Last Dispatch Date",
        ]
        source = self._ensure_columns(source, required)

        if source.empty:
            return pd.DataFrame(columns=self.CUSTOMER_HISTORY_COLUMNS)

        # Best Before Date from Full Dispatch is normalized internally as
        # Expiry Date. Keep it separate during Batch Master enrichment.
        source = source.rename(
            columns={
                "Expiry Date": "Dispatch Expiry Date",
            }
        )

        reference_columns = self.KEYS + [
            "GTIN",
            "Drug Name",
            "Expiry Date",
            "PackageSize",
            "Trade Description",
        ]
        reference = self._ensure_columns(
            master,
            reference_columns,
        )[reference_columns].copy()
        reference = reference.rename(
            columns={
                "Expiry Date": "Master Expiry Date",
            }
        )
        reference = reference.drop_duplicates(
            subset=self.KEYS,
            keep="first",
        )

        result = source.merge(
            reference,
            on=self.KEYS,
            how="inner",
            validate="many_to_one",
        )

        # Full Dispatch Best Before Date is primary. Batch Master expiry is
        # only the fallback for older records with a missing ExpiryDate.
        dispatch_expiry = pd.to_datetime(
            result["Dispatch Expiry Date"],
            errors="coerce",
        )
        master_expiry = pd.to_datetime(
            result["Master Expiry Date"],
            errors="coerce",
        )
        result["Expiry Date"] = dispatch_expiry.combine_first(
            master_expiry
        )
        result = result.drop(
            columns=[
                "Dispatch Expiry Date",
                "Master Expiry Date",
            ],
            errors="ignore",
        )

        gln = self._ensure_columns(
            self.gln,
            ["To Address", "GLN"],
        )[["To Address", "GLN"]].copy()
        gln["_Address Key"] = Normalizer.text(
            gln["To Address"]
        )
        gln["GLN"] = gln["GLN"].map(
            self._clean_key_part
        )
        gln = (
            gln[gln["_Address Key"].ne("")]
            .drop_duplicates(
                "_Address Key",
                keep="first",
            )
        )

        result["_Address Key"] = Normalizer.text(
            result["To Address"]
        )
        result = result.merge(
            gln[["_Address Key", "GLN"]],
            on="_Address Key",
            how="left",
            validate="many_to_one",
        )
        result["GLN"] = (
            result["GLN"]
            .fillna("")
            .astype(str)
            .str.strip()
        )

        missing_gln = result["GLN"].str.lower().isin(
            ["", "nan", "none"]
        )
        result.loc[
            missing_gln,
            "GLN",
        ] = "99999999999999"

        result["Trade Description"] = (
            result["Trade Description"].fillna("")
        )
        missing_trade = (
            result["Trade Description"]
            .astype(str)
            .str.strip()
            .eq("")
        )
        result.loc[
            missing_trade,
            "Trade Description",
        ] = result.loc[
            missing_trade,
            "Trade Name",
        ]

        result["Dispatch Quantity Each"] = pd.to_numeric(
            result["Dispatch Quantity Each"],
            errors="coerce",
        ).fillna(0)
        result["PackageSize"] = pd.to_numeric(
            result["PackageSize"],
            errors="coerce",
        ).fillna(0)

        result["Dispatch Quantity Pack"] = 0.0
        valid_pack = result["PackageSize"].gt(0)
        result.loc[
            valid_pack,
            "Dispatch Quantity Pack",
        ] = (
            result.loc[
                valid_pack,
                "Dispatch Quantity Each",
            ]
            / result.loc[
                valid_pack,
                "PackageSize",
            ]
        )

        result = self._ensure_columns(
            result,
            self.CUSTOMER_HISTORY_COLUMNS,
        )

        return (
            result[self.CUSTOMER_HISTORY_COLUMNS]
            .sort_values(
                [
                    "To Address",
                    "Generic Item Number",
                    "BN",
                    "Expiry Date",
                ],
                kind="stable",
            )
            .reset_index(drop=True)
        )

    @staticmethod
    def _normalize_reconciliation_frame(
        dataframe: pd.DataFrame | None,
    ) -> pd.DataFrame:
        """Return a safe copy for Stage 2 processing."""

        if dataframe is None:
            return pd.DataFrame()

        return dataframe.copy()

    def build_historical_data(
        self,
        receipt_summary: pd.DataFrame,
        dispatch_summary: pd.DataFrame,
        supplier_summary: pd.DataFrame | None = None,
        customer_summary: pd.DataFrame | None = None,
    ) -> Dict[str, Any]:
        """Run Stage 1 and return all historical datasets."""

        prepared = self.prepare_incremental()

        master = self.build_master_from_summaries(
            receipt_summary,
            dispatch_summary,
            prepared["sfda_summary"],
        )

        supplier_history = (
            self.build_supplier_history(
                supplier_summary,
                master,
            )
            if supplier_summary is not None
            else pd.DataFrame(
                columns=self.SUPPLIER_HISTORY_COLUMNS
            )
        )

        customer_history = (
            self.build_customer_history(
                customer_summary,
                master,
            )
            if customer_summary is not None
            else pd.DataFrame(
                columns=self.CUSTOMER_HISTORY_COLUMNS
            )
        )

        prepared.update(
            {
                "master": master,
                "master_records": self._records(master),
                "supplier_history": supplier_history,
                "supplier_history_records": self._records(
                    supplier_history
                ),
                "customer_history": customer_history,
                "customer_history_records": self._records(
                    customer_history
                ),
            }
        )

        return prepared

    @staticmethod
    def _rename_history_columns(frame: pd.DataFrame) -> pd.DataFrame:
        """Convert persisted SQL column names to the public report schema."""

        result = frame.copy() if frame is not None else pd.DataFrame()
        rename_map = {
            "SupplierName": "Supplier Name",
            "SupplierCode": "Supplier Code",
            "GenericItemNumber": "Generic Item Number",
            "TradeItemNumber": "Trade Item Number",
            "TradeDescription": "Trade Description",
            "DrugName": "Drug Name",
            "ExpiryMonthKey": "Expiry Month Key",
            "ExpiryDate": "Expiry Date",
            "PackageSize": "PackageSize",
            "ReceivedQuantityEach": "Received Quantity Each",
            "ReceivedQuantityPack": "Received Quantity Pack",
            "FirstReceivedDate": "First Received Date",
            "LastReceivedDate": "Last Received Date",
            "ItemFamilyGroup": "Item Family Group",
            "ToAddress": "To Address",
            "DispatchQuantityEach": "Dispatch Quantity Each",
            "DispatchQuantityPack": "Dispatch Quantity Pack",
            "FirstDispatchDate": "First Dispatch Date",
            "LastDispatchDate": "Last Dispatch Date",
        }
        return result.rename(columns=rename_map)

    @staticmethod
    def _prepare_stage2_sfda(sfda_df: pd.DataFrame) -> pd.DataFrame:
        sfda = Normalizer.normalize_sfda(sfda_df.copy())
        sfda["Expiry Month Key"] = FullReconciliationEngine._month_key(
            sfda["Expiry Date"]
        )
        for column in [
            "Quantity",
            "Active",
            "Quantity sent pending",
            "Quantity Receive Pending",
        ]:
            sfda[column] = pd.to_numeric(sfda[column], errors="coerce").fillna(0)

        return (
            sfda.groupby(["BN", "Expiry Month Key"], dropna=False)
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

    @staticmethod
    def _prepare_stage2_inventory(inventory_df: pd.DataFrame) -> pd.DataFrame:
        inventory = Normalizer.normalize_inventory(inventory_df.copy())
        Validator.validate(inventory, "INVENTORY")
        inventory["Expiry Month Key"] = FullReconciliationEngine._month_key(
            inventory["Expiry Date"]
        )
        inventory["Available Quantity"] = pd.to_numeric(
            inventory["Available Quantity"], errors="coerce"
        ).fillna(0)

        return (
            inventory.groupby(
                ["BN", "Expiry Month Key", "Generic Item Number"],
                dropna=False,
            )
            .agg(
                **{
                    "Inventory Expiry Date": ("Expiry Date", "first"),
                    "Current Inventory Quantity Each": (
                        "Available Quantity",
                        "sum",
                    ),
                    "Inventory Trade Name": ("Trade Name", "first"),
                }
            )
            .reset_index()
        )

    def build_accept_reconciliation(
        self,
        sfda_df: pd.DataFrame,
        batch_master_df: pd.DataFrame,
    ) -> pd.DataFrame:
        """Build the one-time Accept alignment by historical batch."""

        sfda = self._prepare_stage2_sfda(sfda_df)
        master = batch_master_df.copy()
        if master.empty:
            return pd.DataFrame(columns=self.ACCEPT_RECONCILIATION_COLUMNS)

        for column in ["BN", "Generic Item Number", "Expiry Month Key"]:
            master[column] = Normalizer.text(master[column])
        master["Expiry Date"] = Normalizer.date(master["Expiry Date"])
        master["PackageSize"] = pd.to_numeric(
            master.get("PackageSize", 0), errors="coerce"
        ).fillna(0)
        master["Historical Received Quantity Each"] = pd.to_numeric(
            master.get("Received Quantity Each", 0), errors="coerce"
        ).fillna(0)
        master["Historical Received Quantity Pack"] = 0.0
        valid_pack = master["PackageSize"].gt(0)
        master.loc[valid_pack, "Historical Received Quantity Pack"] = (
            master.loc[valid_pack, "Historical Received Quantity Each"]
            / master.loc[valid_pack, "PackageSize"]
        )

        report = master.merge(
            sfda,
            on=["BN", "Expiry Month Key"],
            how="left",
            suffixes=("", " SFDA"),
            validate="many_to_one",
        )

        # Batch Master stores the SFDA values that existed when history was
        # built. After the merge, those older values keep their original names,
        # while the values from the newly uploaded SFDA report receive the
        # `` SFDA`` suffix. Stage 2 must always use the newly uploaded report.
        latest_sfda_columns = [
            "Quantity",
            "Active",
            "Quantity sent pending",
            "Quantity Receive Pending",
        ]
        for column in latest_sfda_columns:
            uploaded_column = f"{column} SFDA"
            source = (
                report[uploaded_column]
                if uploaded_column in report.columns
                else report.get(column, 0)
            )
            report[column] = pd.to_numeric(
                source,
                errors="coerce",
            ).fillna(0)

        # Use the latest SFDA identifiers when an exact batch exists, while
        # retaining Batch Master identifiers as a fallback for missing batches.
        for column in ["GTIN", "Drug Name", "Expiry Date"]:
            uploaded_column = f"{column} SFDA"
            if uploaded_column not in report.columns:
                continue

            uploaded_values = report[uploaded_column]
            if column == "Expiry Date":
                uploaded_values = Normalizer.date(uploaded_values)
                existing_values = Normalizer.date(report[column])
                report[column] = uploaded_values.combine_first(existing_values)
            else:
                uploaded_text = uploaded_values.fillna("").astype(str).str.strip()
                existing_text = report[column].fillna("").astype(str).str.strip()
                valid_uploaded = ~uploaded_text.str.lower().isin(["", "nan", "none"])
                report[column] = existing_text
                report.loc[valid_uploaded, column] = uploaded_text.loc[valid_uploaded]

        report["SFDA Quantity"] = report["Quantity"]
        report["SFDA Active"] = report["Active"]
        report["Quantity Sent Pending"] = report["Quantity sent pending"]
        report["To Be Accept"] = 0
        eligible = report["PackageSize"].gt(0)

        # Full Accept is a historical alignment. Quantities already reflected in
        # SFDA must be deducted from the historical received quantity so that a
        # newly downloaded SFDA report does not generate the same Accept files
        # again after a successful upload.
        historical_received_pack = pd.to_numeric(
            report["Historical Received Quantity Pack"],
            errors="coerce",
        ).fillna(0)
        already_accepted_in_sfda = (
            pd.to_numeric(report["SFDA Active"], errors="coerce").fillna(0)
            + pd.to_numeric(
                report["Quantity Sent Pending"],
                errors="coerce",
            ).fillna(0)
        )
        remaining_accept = (
            historical_received_pack - already_accepted_in_sfda
        ).clip(lower=0)

        quantity_receive_pending = pd.to_numeric(
            report["Quantity Receive Pending"],
            errors="coerce",
        ).fillna(0).clip(lower=0)

        # The requested Accept quantity must never exceed the quantity that SFDA
        # currently allows in Quantity Receive Pending. Apply both limits:
        #   1. Remaining historical quantity not yet reflected in SFDA.
        #   2. Current SFDA Quantity Receive Pending.
        # Apply the cap directly and explicitly. This guarantees that a zero
        # Quantity Receive Pending always produces a zero To Be Accept, even if
        # the historical balance is still positive.
        capped_accept = remaining_accept.clip(
            upper=quantity_receive_pending,
        ).clip(lower=0)
        capped_accept = capped_accept.where(
            quantity_receive_pending.gt(0),
            0,
        )

        report.loc[eligible, "To Be Accept"] = (
            capped_accept.loc[eligible]
            .fillna(0)
            .astype(int)
        )
        report.loc[
            quantity_receive_pending.le(0),
            "To Be Accept",
        ] = 0

        logger.info(
            "Full Accept logic v2026.08.06.3 applied. rows=%s positive_accept_rows=%s",
            len(report),
            int(pd.to_numeric(report["To Be Accept"], errors="coerce").fillna(0).gt(0).sum()),
        )
        report["Reconciliation Status"] = "No Accept Required"
        report.loc[report["To Be Accept"].gt(0), "Reconciliation Status"] = (
            "Accept Required"
        )
        report.loc[~eligible, "Reconciliation Status"] = "Package Size Missing"

        report = self._ensure_columns(
            report,
            self.ACCEPT_RECONCILIATION_COLUMNS,
        )[self.ACCEPT_RECONCILIATION_COLUMNS]
        return report.loc[report["To Be Accept"].gt(0)].reset_index(drop=True)

    def build_supplier_variance(
        self,
        sfda_df: pd.DataFrame,
        supplier_history_df: pd.DataFrame,
    ) -> pd.DataFrame:
        """Compare supplier-reported SFDA quantity with physical receipts."""

        sfda = self._prepare_stage2_sfda(sfda_df)
        supplier = self._rename_history_columns(supplier_history_df)
        if supplier.empty:
            return pd.DataFrame(columns=self.SUPPLIER_VARIANCE_COLUMNS)

        for column in ["BN", "Generic Item Number", "Expiry Month Key"]:
            supplier[column] = Normalizer.text(supplier[column])
        supplier["Expiry Date"] = Normalizer.date(supplier["Expiry Date"])
        supplier["Historical Received Quantity Each"] = pd.to_numeric(
            supplier.get("Received Quantity Each", 0), errors="coerce"
        ).fillna(0)
        supplier["Historical Received Quantity Pack"] = pd.to_numeric(
            supplier.get("Received Quantity Pack", 0), errors="coerce"
        ).fillna(0)

        report = supplier.merge(
            sfda,
            on=["BN", "Expiry Month Key"],
            how="left",
            suffixes=("", " SFDA"),
            validate="many_to_one",
        )
        report["SFDA Supplier Quantity"] = pd.to_numeric(
            report.get("Quantity", 0), errors="coerce"
        ).fillna(0)
        report["Supplier Variance"] = (
            report["Historical Received Quantity Pack"]
            - report["SFDA Supplier Quantity"]
        )
        report["Variance Status"] = "Matched"
        report.loc[report["Supplier Variance"].gt(0), "Variance Status"] = (
            "Supplier Reported Less"
        )
        report.loc[report["Supplier Variance"].lt(0), "Variance Status"] = (
            "Supplier Reported More"
        )
        report["Required Action"] = "No Action"
        report.loc[report["Supplier Variance"].gt(0), "Required Action"] = (
            "Notify supplier to add missing quantity"
        )
        report.loc[report["Supplier Variance"].lt(0), "Required Action"] = (
            "Investigate excess supplier quantity"
        )
        report["GTIN"] = report.get("GTIN", "")
        report["Drug Name"] = report.get("Drug Name", "")

        report = self._ensure_columns(
            report,
            self.SUPPLIER_VARIANCE_COLUMNS,
        )[self.SUPPLIER_VARIANCE_COLUMNS]
        return report.loc[report["Supplier Variance"].ne(0)].reset_index(drop=True)

    def build_dispatch_reconciliation(
        self,
        inventory_df: pd.DataFrame,
        sfda_df: pd.DataFrame,
        customer_history_df: pd.DataFrame,
    ) -> pd.DataFrame:
        """Allocate the SFDA Active minus current inventory variance by GLN."""

        sfda = self._prepare_stage2_sfda(sfda_df)
        inventory = self._prepare_stage2_inventory(inventory_df)
        customer = self._rename_history_columns(customer_history_df)
        if customer.empty:
            return pd.DataFrame(columns=self.DISPATCH_RECONCILIATION_COLUMNS)

        for column in ["BN", "Generic Item Number", "Expiry Month Key"]:
            customer[column] = Normalizer.text(customer[column])
        customer["Expiry Date"] = Normalizer.date(customer["Expiry Date"])
        customer["Historical Dispatch Quantity Each"] = pd.to_numeric(
            customer.get("Dispatch Quantity Each", 0), errors="coerce"
        ).fillna(0)
        customer["Historical Dispatch Quantity Pack"] = pd.to_numeric(
            customer.get("Dispatch Quantity Pack", 0), errors="coerce"
        ).fillna(0)
        customer["GLN"] = customer.get("GLN", "").fillna("").astype(str).str.strip()
        missing_gln = customer["GLN"].eq("") | customer["GLN"].str.upper().eq("DUMMY")
        customer.loc[missing_gln, "GLN"] = "99999999999999"
        customer["Customer Status"] = "REGISTERED"
        customer.loc[missing_gln, "Customer Status"] = "DUMMY"

        batch_current = inventory.groupby(
            ["BN", "Expiry Month Key"], dropna=False
        )["Current Inventory Quantity Each"].sum().reset_index()
        target = sfda.merge(
            batch_current,
            on=["BN", "Expiry Month Key"],
            how="left",
            validate="one_to_one",
        )
        target["Current Inventory Quantity Each"] = pd.to_numeric(
            target["Current Inventory Quantity Each"], errors="coerce"
        ).fillna(0)

        # Use PackageSize stored in Customer History to convert WMS inventory
        # to packs. The first valid package size per batch is sufficient because
        # SFDA matching is at BN + expiry month grain.
        pack_lookup = (
            customer.loc[pd.to_numeric(customer.get("PackageSize", 0), errors="coerce").gt(0)]
            .groupby(["BN", "Expiry Month Key"], dropna=False)["PackageSize"]
            .first()
            .reset_index()
        )
        target = target.merge(
            pack_lookup,
            on=["BN", "Expiry Month Key"],
            how="left",
            validate="one_to_one",
        )
        target["PackageSize"] = pd.to_numeric(
            target.get("PackageSize", 0), errors="coerce"
        ).fillna(0)
        target["Current Inventory Quantity Pack"] = 0.0
        valid_pack = target["PackageSize"].gt(0)
        target.loc[valid_pack, "Current Inventory Quantity Pack"] = (
            target.loc[valid_pack, "Current Inventory Quantity Each"]
            / target.loc[valid_pack, "PackageSize"]
        )
        target["Required Dispatch Pack"] = (
            pd.to_numeric(target["Active"], errors="coerce").fillna(0)
            - target["Current Inventory Quantity Pack"]
        ).clip(lower=0).astype(int)

        details = customer.merge(
            target[
                [
                    "BN",
                    "Expiry Month Key",
                    "GTIN",
                    "Drug Name",
                    "Quantity",
                    "Active",
                    "Quantity sent pending",
                    "Quantity Receive Pending",
                    "PackageSize",
                    "Current Inventory Quantity Each",
                    "Current Inventory Quantity Pack",
                    "Required Dispatch Pack",
                ]
            ],
            on=["BN", "Expiry Month Key"],
            how="inner",
            validate="many_to_one",
        )
        if "GTIN_y" in details.columns:
            details["GTIN"] = details["GTIN_y"].fillna(
                details.get("GTIN_x", "")
            )
        elif "GTIN_x" in details.columns:
            details["GTIN"] = details["GTIN_x"]

        if "Drug Name_y" in details.columns:
            details["Drug Name"] = details["Drug Name_y"].fillna(
                details.get("Drug Name_x", "")
            )
        elif "Drug Name_x" in details.columns:
            details["Drug Name"] = details["Drug Name_x"]

        if "PackageSize_y" in details.columns:
            details["PackageSize"] = pd.to_numeric(
                details["PackageSize_y"],
                errors="coerce",
            ).fillna(
                pd.to_numeric(
                    details.get("PackageSize_x", 0),
                    errors="coerce",
                )
            )
        elif "PackageSize_x" in details.columns:
            details["PackageSize"] = pd.to_numeric(
                details["PackageSize_x"],
                errors="coerce",
            ).fillna(0)

        details = details.sort_values(
            ["BN", "Expiry Month Key", "First Dispatch Date", "To Address"],
            kind="stable",
        ).reset_index(drop=True)
        details["To Be Dispatch"] = 0

        for _, indexes in details.groupby(
            ["BN", "Expiry Month Key"], sort=False, dropna=False
        ).groups.items():
            index_list = list(indexes)
            remaining = int(details.loc[index_list[0], "Required Dispatch Pack"])
            for row_index in index_list:
                if remaining <= 0:
                    break
                available = int(max(details.loc[row_index, "Historical Dispatch Quantity Pack"], 0))
                allocated = min(available, remaining)
                details.loc[row_index, "To Be Dispatch"] = allocated
                remaining -= allocated

        details["SFDA Quantity"] = pd.to_numeric(
            details.get("Quantity", 0),
            errors="coerce",
        ).fillna(0)
        details["SFDA Active"] = pd.to_numeric(
            details.get("Active", 0),
            errors="coerce",
        ).fillna(0)
        details["Quantity Sent Pending"] = pd.to_numeric(
            details.get("Quantity sent pending", 0),
            errors="coerce",
        ).fillna(0)
        details["Quantity Receive Pending"] = pd.to_numeric(
            details.get("Quantity Receive Pending", 0),
            errors="coerce",
        ).fillna(0)
        details["Current Inventory Quantity Pack"] = pd.to_numeric(
            details.get("Current Inventory Quantity Pack", 0),
            errors="coerce",
        ).fillna(0)

        details["Reconciliation Status"] = "No Dispatch Required"
        details.loc[details["To Be Dispatch"].gt(0), "Reconciliation Status"] = (
            "Dispatch Required"
        )

        details = self._ensure_columns(
            details,
            self.DISPATCH_RECONCILIATION_COLUMNS,
        )[self.DISPATCH_RECONCILIATION_COLUMNS]
        return details.loc[details["To Be Dispatch"].gt(0)].reset_index(drop=True)

    def build_reconciliation_summary(
        self,
        accept_details: pd.DataFrame,
        supplier_variance: pd.DataFrame,
        dispatch_details: pd.DataFrame,
    ) -> pd.DataFrame:
        """Build one concise summary for the one-time alignment."""

        metrics = [
            ("Accept detail rows", len(accept_details)),
            ("Accept required rows", int((pd.to_numeric(accept_details.get("To Be Accept", 0), errors="coerce").fillna(0) > 0).sum())),
            ("Accept quantity packs", float(pd.to_numeric(accept_details.get("To Be Accept", 0), errors="coerce").fillna(0).sum())),
            ("Supplier variance rows", int((pd.to_numeric(supplier_variance.get("Supplier Variance", 0), errors="coerce").fillna(0) != 0).sum())),
            ("Dispatch detail rows", len(dispatch_details)),
            ("Dispatch allocation rows", int((pd.to_numeric(dispatch_details.get("To Be Dispatch", 0), errors="coerce").fillna(0) > 0).sum())),
            ("Dispatch quantity packs", float(pd.to_numeric(dispatch_details.get("To Be Dispatch", 0), errors="coerce").fillna(0).sum())),
        ]
        return pd.DataFrame(metrics, columns=self.RECONCILIATION_SUMMARY_COLUMNS)

    def run_accept_reconciliation(
        self,
        sfda_df: pd.DataFrame,
        batch_master_df: pd.DataFrame,
        supplier_history_df: pd.DataFrame,
    ) -> Dict[str, pd.DataFrame]:
        """Run the Full Accept stage using SFDA and persisted history only."""

        Validator.validate(Normalizer.normalize_sfda(sfda_df.copy()), "SFDA")
        accept_details = self.build_accept_reconciliation(
            sfda_df, batch_master_df
        )
        supplier_variance = self.build_supplier_variance(
            sfda_df, supplier_history_df
        )
        accept_upload = accept_details.loc[
            pd.to_numeric(
                accept_details.get("To Be Accept", 0),
                errors="coerce",
            ).fillna(0).gt(0)
        ].copy()

        return {
            "accept_details": accept_details,
            "supplier_variance": supplier_variance,
            "accept_upload": accept_upload,
        }

    def run_dispatch_reconciliation(
        self,
        inventory_df: pd.DataFrame,
        sfda_df: pd.DataFrame,
        customer_history_df: pd.DataFrame,
    ) -> Dict[str, pd.DataFrame]:
        """Run the Full Dispatch stage after Accept is completed in SFDA."""

        Validator.validate(Normalizer.normalize_sfda(sfda_df.copy()), "SFDA")
        dispatch_details = self.build_dispatch_reconciliation(
            inventory_df, sfda_df, customer_history_df
        )
        empty_accept = pd.DataFrame(columns=self.ACCEPT_RECONCILIATION_COLUMNS)
        empty_variance = pd.DataFrame(columns=self.SUPPLIER_VARIANCE_COLUMNS)
        summary = self.build_reconciliation_summary(
            empty_accept, empty_variance, dispatch_details
        )
        dispatch_upload = dispatch_details.loc[
            pd.to_numeric(
                dispatch_details.get("To Be Dispatch", 0),
                errors="coerce",
            ).fillna(0).gt(0)
        ].copy()
        dispatch_upload["Allocated To Be Dispatch"] = dispatch_upload[
            "To Be Dispatch"
        ]

        return {
            "dispatch_details": dispatch_details,
            "summary": summary,
            "dispatch_upload": dispatch_upload,
        }

    def run_full_reconciliation(
        self,
        inventory_df: pd.DataFrame,
        sfda_df: pd.DataFrame,
        batch_master_df: pd.DataFrame,
        supplier_history_df: pd.DataFrame,
        customer_history_df: pd.DataFrame,
    ) -> Dict[str, pd.DataFrame]:
        """Backward-compatible combined Stage 2 entry point."""

        accept_result = self.run_accept_reconciliation(
            sfda_df, batch_master_df, supplier_history_df
        )
        dispatch_result = self.run_dispatch_reconciliation(
            inventory_df, sfda_df, customer_history_df
        )
        summary = self.build_reconciliation_summary(
            accept_result["accept_details"],
            accept_result["supplier_variance"],
            dispatch_result["dispatch_details"],
        )
        return {
            **accept_result,
            **dispatch_result,
            "summary": summary,
        }

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
        started_at = time.perf_counter()
        self.normalize()
        self.validate()

        receipt_events = self._receipt_events()
        dispatch_events = self._dispatch_events()
        sfda_summary = self._sfda_keys()

        receipt_records = self._records(receipt_events)
        dispatch_records = self._records(dispatch_events)

        logger.info(
            "Incremental preparation completed in %.2f seconds. receipt_events=%s dispatch_events=%s sfda_batches=%s",
            time.perf_counter() - started_at,
            len(receipt_events),
            len(dispatch_events),
            len(sfda_summary),
        )

        return {
            "receipt_events": receipt_events,
            "dispatch_events": dispatch_events,
            "receipt_records": receipt_records,
            "dispatch_records": dispatch_records,
            "sfda_summary": sfda_summary,
        }

    def run(
        self,
        receipt_summary: pd.DataFrame | None = None,
        dispatch_summary: pd.DataFrame | None = None,
    ) -> Dict[str, Any]:
        """Backward-compatible Stage 1 entry point."""

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
