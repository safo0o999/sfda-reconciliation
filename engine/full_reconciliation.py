from __future__ import annotations

import hashlib
import logging
import time
from pathlib import Path
from typing import Any, Dict, Iterable, List, Sequence

import pandas as pd

from engine.normalizer import Normalizer
from engine.validator import Validator
from engine.reference_data import load_current_warehouse_gln


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
        "Previously Confirmed Full Dispatch Each",
        "Previously Confirmed Full Dispatch Pack",
        "Reserved Full Dispatch Quantity Each",
        "Reserved Full Dispatch Quantity Pack",
        "Available Historical Dispatch Quantity Each",
        "Available Historical Dispatch Quantity Pack",
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
        # GLN is warehouse-scoped without changing any historical batch logic.
        # Madinah (WarehouseID=1) continues to use config/gln.xlsx exactly as
        # before. Other warehouses use only their own stored mapping.
        self.gln = load_current_warehouse_gln()

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

    @staticmethod
    def _excluded_item_family_mask(series: pd.Series) -> pd.Series:
        """Rows that must never establish SFDA/RSD product identity.

        Laboratory supplies are operational WMS stock but are outside the drug
        reconciliation universe.  They must not be allowed to prove a Generic
        Item Number merely because their BN + expiry month collides with a drug
        batch in SFDA.
        """
        normalized = (
            series.fillna("")
            .astype(str)
            .str.upper()
            .str.replace(r"[^A-Z0-9]+", "", regex=True)
        )
        return normalized.eq("LABORATORYSUPPLIES")

    @staticmethod
    def _identity_text(value: Any) -> str:
        """Stable text key used only to compare already-proven SFDA identities."""
        if value is None:
            return ""
        try:
            if pd.isna(value):
                return ""
        except (TypeError, ValueError):
            pass
        return "".join(ch for ch in str(value).upper().strip() if ch.isalnum())

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

        # ------------------------------------------------------------------
        # Resolve SFDA product identity before a BN + expiry-month match is
        # allowed to prove a WMS Generic.
        #
        # BN + Expiry Month remains the operational candidate key.  We do NOT
        # require the exact expiry day because SFDA and WMS can legitimately
        # carry different days in the same month.
        #
        # The important protection is product identity:
        #   * Build every observed SFDA-GTIN <-> WMS-Generic candidate edge.
        #   * Repeated independent batch/month evidence is used to prove a
        #     stable product mapping.
        #   * A single batch/month collision is never allowed to overwrite an
        #     already-proven mapping.
        #   * For a product with only one historical batch, accept it only when
        #     that BN/month has exactly one WMS Generic AND the row is not an
        #     excluded non-drug family.  This preserves first-time products but
        #     blocks known LABORATORYSUPPLIES collisions from proving a drug.
        #   * Once Generic <-> GTIN is proven, only that Generic may consume
        #     SFDA rows for that GTIN. Other WMS rows sharing BN/month remain
        #     unrelated and cannot inherit GTIN/Drug/PackageSize.
        # ------------------------------------------------------------------
        candidate_columns = self.SFDA_KEYS + [
            "Generic Item Number",
            "Item Family Group",
        ]
        candidates = self._ensure_columns(master, candidate_columns)[candidate_columns].copy()
        candidates["Generic Item Number"] = (
            candidates["Generic Item Number"].fillna("").astype(str).str.strip()
        )
        candidates = candidates[candidates["Generic Item Number"].ne("")].copy()
        candidates = candidates.drop_duplicates(
            subset=self.SFDA_KEYS + ["Generic Item Number"],
            keep="first",
        )
        candidates["_Excluded Family"] = self._excluded_item_family_mask(
            candidates["Item Family Group"]
        )

        sfda_identity = sfda[sfda_columns].copy()
        sfda_identity["_GTIN Identity"] = (
            sfda_identity["GTIN"].fillna("").astype(str).str.strip()
        )
        sfda_identity["_Drug Identity"] = sfda_identity["Drug Name"].map(
            self._identity_text
        )

        # All candidate edges created only from the operational BN+month key.
        edges = candidates.merge(
            sfda_identity,
            on=self.SFDA_KEYS,
            how="inner",
            validate="many_to_one",
        )
        # Keep excluded families in ReceiptEvents for warehouse audit/history,
        # but never let them establish SFDA drug identity.
        identity_edges = edges[~edges["_Excluded Family"]].copy()

        resolved = pd.DataFrame(columns=(
            self.SFDA_KEYS + ["Generic Item Number"] +
            [column for column in sfda_columns if column not in self.SFDA_KEYS]
        ))

        if not identity_edges.empty:
            # Count independent BN/month evidence for every GTIN <-> Generic.
            edge_counts = (
                identity_edges.groupby(
                    ["_GTIN Identity", "_Drug Identity", "Generic Item Number"],
                    dropna=False,
                )
                .size()
                .rename("_Evidence Count")
                .reset_index()
            )

            # Strong historical mapping: the pair occurs on at least two
            # independent SFDA batch/month keys.  Require mutual uniqueness of
            # the best score so one Generic cannot be proven for two drugs and
            # one drug cannot be proven for two Generics.
            gtin_best = edge_counts.groupby(
                ["_GTIN Identity", "_Drug Identity"], dropna=False
            )["_Evidence Count"].transform("max")
            generic_best = edge_counts.groupby(
                ["Generic Item Number"], dropna=False
            )["_Evidence Count"].transform("max")
            strong = edge_counts[
                edge_counts["_Evidence Count"].ge(2)
                & edge_counts["_Evidence Count"].eq(gtin_best)
                & edge_counts["_Evidence Count"].eq(generic_best)
            ].copy()

            # Remove ties at either side.
            if not strong.empty:
                gtin_winners = (
                    strong.groupby(["_GTIN Identity", "_Drug Identity"], dropna=False)[
                        "Generic Item Number"
                    ].nunique()
                )
                generic_winners = (
                    strong.groupby("Generic Item Number", dropna=False)["_GTIN Identity"].nunique()
                )
                strong["_GTIN Winner Count"] = strong.set_index(
                    ["_GTIN Identity", "_Drug Identity"]
                ).index.map(gtin_winners)
                strong["_Generic Winner Count"] = strong["Generic Item Number"].map(
                    generic_winners
                )
                strong = strong[
                    strong["_GTIN Winner Count"].eq(1)
                    & strong["_Generic Winner Count"].eq(1)
                ].copy()

            proven_pairs = strong[[
                "_GTIN Identity", "_Drug Identity", "Generic Item Number"
            ]].drop_duplicates()

            # First-time/single-batch fallback.  It is deliberately conservative:
            # one WMS Generic for the BN/month, one SFDA identity for the key,
            # and the WMS row must not be a known excluded non-drug family.
            candidate_counts = (
                candidates.groupby(self.SFDA_KEYS, dropna=False)["Generic Item Number"]
                .nunique()
                .rename("_Candidate Generic Count")
                .reset_index()
            )
            single = identity_edges.merge(
                candidate_counts,
                on=self.SFDA_KEYS,
                how="left",
                validate="many_to_one",
            )
            single = single[
                single["_Candidate Generic Count"].eq(1)
                & ~single["_Excluded Family"]
            ].copy()

            # Do not create a fallback mapping if either side already has a
            # different strong historical identity.
            if not proven_pairs.empty and not single.empty:
                proven_gtin_to_generic = dict(zip(
                    proven_pairs["_GTIN Identity"],
                    proven_pairs["Generic Item Number"],
                ))
                proven_generic_to_gtin = dict(zip(
                    proven_pairs["Generic Item Number"],
                    proven_pairs["_GTIN Identity"],
                ))
                single = single[
                    single.apply(
                        lambda row: (
                            proven_gtin_to_generic.get(row["_GTIN Identity"], row["Generic Item Number"])
                            == row["Generic Item Number"]
                            and proven_generic_to_gtin.get(row["Generic Item Number"], row["_GTIN Identity"])
                            == row["_GTIN Identity"]
                        ),
                        axis=1,
                    )
                ].copy()

            fallback_pairs = single[[
                "_GTIN Identity", "_Drug Identity", "Generic Item Number"
            ]].drop_duplicates()

            all_pairs = pd.concat(
                [proven_pairs, fallback_pairs],
                ignore_index=True,
            ).drop_duplicates()

            # Final mutual uniqueness guard.  Any identity that still points to
            # multiple products is left unresolved rather than guessed.
            if not all_pairs.empty:
                gtin_pair_count = all_pairs.groupby(
                    ["_GTIN Identity", "_Drug Identity"], dropna=False
                )["Generic Item Number"].transform("nunique")
                generic_pair_count = all_pairs.groupby(
                    "Generic Item Number", dropna=False
                )["_GTIN Identity"].transform("nunique")
                all_pairs = all_pairs[
                    gtin_pair_count.eq(1) & generic_pair_count.eq(1)
                ].copy()

                resolved = identity_edges.merge(
                    all_pairs,
                    on=["_GTIN Identity", "_Drug Identity", "Generic Item Number"],
                    how="inner",
                    validate="many_to_one",
                )
                resolved = resolved.drop_duplicates(
                    subset=self.SFDA_KEYS + ["Generic Item Number"],
                    keep="first",
                )

        resolved_columns = self.SFDA_KEYS + ["Generic Item Number"] + [
            column for column in sfda_columns if column not in self.SFDA_KEYS
        ]
        resolved = self._ensure_columns(resolved, resolved_columns)[resolved_columns]
        resolved["_Batch Exists in SFDA"] = True

        master = master.merge(
            resolved,
            on=self.KEYS,
            how="left",
            validate="one_to_one",
        )
        master["_Batch Exists in SFDA"] = master["_Batch Exists in SFDA"].fillna(False)

        unresolved_sfda = len(sfda) - len(resolved[self.SFDA_KEYS].drop_duplicates())
        if unresolved_sfda > 0:
            logger.warning(
                "SFDA product identity left unresolved safely for %s BN+expiry-month key(s).",
                unresolved_sfda,
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

    def _supplier_only_master_for_accept(
        self,
        batch_master_df: pd.DataFrame,
        supplier_history_df: pd.DataFrame,
    ) -> pd.DataFrame:
        """Return Batch Master rows with receipt quantity replaced by TRK5060 only.

        Batch Master remains a physical historical view and can include STO
        receipt/return movements. Full Accept must not use that combined receipt
        quantity. Supplier Accept therefore receives an isolated supplier-only
        quantity from SupplierHistory (which is TRK5060-only at SQL level).
        """

        master = batch_master_df.copy() if batch_master_df is not None else pd.DataFrame()
        supplier = self._rename_history_columns(supplier_history_df)
        if master.empty or supplier.empty:
            return pd.DataFrame(columns=master.columns if not master.empty else self.MASTER_COLUMNS)

        for column in self.KEYS:
            master[column] = Normalizer.text(master[column])
            supplier[column] = Normalizer.text(supplier[column])

        supplier["Received Quantity Each"] = pd.to_numeric(
            supplier.get("Received Quantity Each", 0), errors="coerce"
        ).fillna(0)

        supplier_qty = (
            supplier.groupby(self.KEYS, dropna=False)["Received Quantity Each"]
            .sum()
            .reset_index()
            .rename(columns={"Received Quantity Each": "_Supplier Received Each"})
        )

        result = master.merge(
            supplier_qty,
            on=self.KEYS,
            how="inner",
            validate="one_to_one",
        )
        result["Received Quantity Each"] = pd.to_numeric(
            result["_Supplier Received Each"], errors="coerce"
        ).fillna(0)
        result = result.drop(columns=["_Supplier Received Each"], errors="ignore")
        return result

    def build_sto_incoming_reconciliation(
        self,
        sfda_df: pd.DataFrame,
        batch_master_df: pd.DataFrame,
        sto_incoming_df: pd.DataFrame,
        supplier_accept_details: pd.DataFrame | None = None,
    ) -> pd.DataFrame:
        """Reconcile TRK800 STO receipts against current RSD Receive Pending.

        STO receipt quantities NEVER enter Supplier Variance.  RSD pending that
        remains after supplier Accept allocation is made available to STO. Any
        unrepresented STO quantity is surfaced for follow-up with the sending
        NUPCO warehouse instead of being reported as a supplier variance.
        """

        sto = sto_incoming_df.copy() if sto_incoming_df is not None else pd.DataFrame()
        if sto.empty:
            return pd.DataFrame(columns=[
                "Source Warehouse", "Source Warehouse Code", "Inbound Shipment",
                "GTIN", "Drug Name", "BN", "Expiry Date", "Expiry Month Key",
                "Generic Item Number", "PackageSize", "STO Received Quantity Each",
                "STO Received Quantity Pack", "Quantity Receive Pending",
                "Available RSD Receive Pending", "To Be Accept", "STO Pending RSD Qty",
                "RSD Status", "Required Action",
            ])

        sfda = self._prepare_stage2_sfda(sfda_df)
        master = batch_master_df.copy() if batch_master_df is not None else pd.DataFrame()

        for column in ["BN", "Expiry Month Key", "Generic Item Number"]:
            if column in sto.columns:
                sto[column] = Normalizer.text(sto[column])
            if not master.empty and column in master.columns:
                master[column] = Normalizer.text(master[column])

        # Enrich missing reference data from Batch Master without changing STO grain.
        reference_columns = self.KEYS + [
            "GTIN", "Drug Name", "Expiry Date", "PackageSize", "Trade Description"
        ]
        if not master.empty:
            reference = self._ensure_columns(master, reference_columns)[reference_columns]
            reference = reference.drop_duplicates(subset=self.KEYS, keep="first")
            sto = sto.merge(
                reference,
                on=self.KEYS,
                how="left",
                suffixes=("", " Master"),
                validate="many_to_one",
            )
            for column in ["GTIN", "Drug Name", "Expiry Date", "PackageSize", "Trade Description"]:
                master_col = f"{column} Master"
                if master_col not in sto.columns:
                    continue
                if column == "Expiry Date":
                    sto[column] = Normalizer.date(sto.get(column, pd.NaT)).combine_first(
                        Normalizer.date(sto[master_col])
                    )
                elif column == "PackageSize":
                    current = pd.to_numeric(sto.get(column, 0), errors="coerce").fillna(0)
                    fallback = pd.to_numeric(sto[master_col], errors="coerce").fillna(0)
                    sto[column] = current.where(current.gt(0), fallback)
                else:
                    current = sto.get(column, "").fillna("").astype(str).str.strip()
                    fallback = sto[master_col].fillna("").astype(str).str.strip()
                    sto[column] = current.where(current.ne(""), fallback)
            sto = sto.drop(columns=[c for c in sto.columns if c.endswith(" Master")], errors="ignore")

        sto = sto.merge(
            sfda,
            on=["BN", "Expiry Month Key"],
            how="left",
            suffixes=("", " SFDA"),
            validate="many_to_one",
        )

        # Prefer exact identifiers/current expiry from the uploaded SFDA report.
        for column in ["GTIN", "Drug Name"]:
            sfda_col = f"{column} SFDA"
            if sfda_col in sto.columns:
                sfda_text = sto[sfda_col].fillna("").astype(str).str.strip()
                current = sto.get(column, "").fillna("").astype(str).str.strip()
                sto[column] = sfda_text.where(sfda_text.ne(""), current)
        if "Expiry Date SFDA" in sto.columns:
            sto["Expiry Date"] = Normalizer.date(sto["Expiry Date SFDA"]).combine_first(
                Normalizer.date(sto.get("Expiry Date", pd.NaT))
            )

        sto["PackageSize"] = pd.to_numeric(sto.get("PackageSize", 0), errors="coerce").fillna(0)
        sto["STO Received Quantity Each"] = pd.to_numeric(
            sto.get("Received Quantity Each", 0), errors="coerce"
        ).fillna(0)
        sto["STO Received Quantity Pack"] = 0.0
        valid_pack = sto["PackageSize"].gt(0)
        sto.loc[valid_pack, "STO Received Quantity Pack"] = (
            sto.loc[valid_pack, "STO Received Quantity Each"]
            / sto.loc[valid_pack, "PackageSize"]
        )

        sto["Quantity Receive Pending"] = pd.to_numeric(
            sto.get("Quantity Receive Pending", 0), errors="coerce"
        ).fillna(0).clip(lower=0)

        supplier_allocated = pd.DataFrame(columns=["BN", "Expiry Month Key", "_Supplier Accept"])
        if supplier_accept_details is not None and not supplier_accept_details.empty:
            supplier_tmp = supplier_accept_details.copy()
            supplier_tmp["To Be Accept"] = pd.to_numeric(
                supplier_tmp.get("To Be Accept", 0), errors="coerce"
            ).fillna(0)
            supplier_allocated = (
                supplier_tmp.groupby(["BN", "Expiry Month Key"], dropna=False)["To Be Accept"]
                .sum().reset_index().rename(columns={"To Be Accept": "_Supplier Accept"})
            )
        sto = sto.merge(
            supplier_allocated,
            on=["BN", "Expiry Month Key"],
            how="left",
            validate="many_to_one",
        )
        sto["_Supplier Accept"] = pd.to_numeric(sto.get("_Supplier Accept", 0), errors="coerce").fillna(0)
        sto["Available RSD Receive Pending"] = (
            sto["Quantity Receive Pending"] - sto["_Supplier Accept"]
        ).clip(lower=0)

        # Allocate remaining pending in stable order when several sending
        # warehouses/shipments share the same batch.
        sto["To Be Accept"] = 0
        sto = sto.reset_index(drop=True)
        for _, group in sto.groupby(["BN", "Expiry Month Key"], dropna=False, sort=False):
            remaining = float(group["Available RSD Receive Pending"].iloc[0] if len(group) else 0)
            for index in group.index:
                requested = float(max(0, sto.at[index, "STO Received Quantity Pack"]))
                allocated = min(remaining, requested)
                sto.at[index, "To Be Accept"] = int(max(0, allocated))
                remaining -= allocated
                if remaining < 0:
                    remaining = 0

        sto["STO Pending RSD Qty"] = (
            pd.to_numeric(sto["STO Received Quantity Pack"], errors="coerce").fillna(0)
            - pd.to_numeric(sto["To Be Accept"], errors="coerce").fillna(0)
        ).clip(lower=0)
        sto["RSD Status"] = "RSD Transfer Pending - Follow Up"
        ready = sto["To Be Accept"].gt(0) & sto["STO Pending RSD Qty"].le(0)
        partial = sto["To Be Accept"].gt(0) & sto["STO Pending RSD Qty"].gt(0)
        sto.loc[ready, "RSD Status"] = "RSD Transfer Available - Accept"
        sto.loc[partial, "RSD Status"] = "Partial RSD Transfer Available"
        sto["Required Action"] = "Ask sending warehouse to dispatch the missing quantity through RSD"
        sto.loc[ready, "Required Action"] = "Accept available RSD transfer"
        sto.loc[partial, "Required Action"] = "Accept available quantity and follow up remaining quantity"
        sto.loc[~valid_pack, "RSD Status"] = "Package Size Missing"
        sto.loc[~valid_pack, "Required Action"] = "Complete Package Size mapping"

        columns = [
            "Source Warehouse", "Source Warehouse Code", "Inbound Shipment",
            "GTIN", "Drug Name", "BN", "Expiry Date", "Expiry Month Key",
            "Generic Item Number", "PackageSize", "STO Received Quantity Each",
            "STO Received Quantity Pack", "Quantity Receive Pending",
            "Available RSD Receive Pending", "To Be Accept", "STO Pending RSD Qty",
            "RSD Status", "Required Action",
        ]
        return self._ensure_columns(sto, columns)[columns].reset_index(drop=True)

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
        confirmed_full_dispatch_df: pd.DataFrame | None = None,
    ) -> pd.DataFrame:
        """Allocate only unconsumed historical Dispatch evidence by GLN.

        Full Reconciliation must not reuse the same historical WMS dispatch
        quantity after SFDA has already confirmed that quantity. The confirmed
        ledger is therefore subtracted customer-by-customer before allocation.
        """

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

        # The SFDA Drug Count is the regulatory source of truth for the expiry
        # date written to Dispatch CSV files. Customer History can contain an
        # older WMS expiry day for the same BN/month. Keep the existing
        # BN + Expiry Month matching for allocation, but carry the exact SFDA
        # date into the result and overwrite the WMS history date below.
        target["SFDA Expiry Date"] = Normalizer.date(target["Expiry Date"])

        details = customer.merge(
            target[
                [
                    "BN",
                    "Expiry Month Key",
                    "SFDA Expiry Date",
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

        if "SFDA Expiry Date" in details.columns:
            sfda_expiry = Normalizer.date(details["SFDA Expiry Date"])
            history_expiry = Normalizer.date(details["Expiry Date"])
            details["Expiry Date"] = sfda_expiry.combine_first(history_expiry)
            details = details.drop(columns=["SFDA Expiry Date"])

        confirmed = (
            confirmed_full_dispatch_df.copy()
            if confirmed_full_dispatch_df is not None
            else pd.DataFrame()
        )
        confirmed_each_column = "Previously Confirmed Full Dispatch Each"
        confirmed_pack_column = "Previously Confirmed Full Dispatch Pack"
        reserved_each_column = "Reserved Full Dispatch Quantity Each"
        reserved_pack_column = "Reserved Full Dispatch Quantity Pack"

        if confirmed.empty:
            details[confirmed_each_column] = 0.0
            details[confirmed_pack_column] = 0.0
            details[reserved_each_column] = 0.0
            details[reserved_pack_column] = 0.0
        else:
            rename_confirmed = {
                "Confirmed Full Dispatch Quantity Each": confirmed_each_column,
                "Confirmed Full Dispatch Quantity Pack": confirmed_pack_column,
                "Reserved Full Dispatch Quantity Each": reserved_each_column,
                "Reserved Full Dispatch Quantity Pack": reserved_pack_column,
            }
            confirmed = confirmed.rename(columns=rename_confirmed)

            for column in [
                "BN",
                "Generic Item Number",
                "To Address",
                "GLN",
            ]:
                if column not in confirmed.columns:
                    confirmed[column] = ""
                confirmed[column] = confirmed[column].fillna("").astype(str).str.strip()

            confirmed["Expiry Date"] = Normalizer.date(
                confirmed.get("Expiry Date", pd.Series(dtype=object))
            )
            if "Expiry Month Key" not in confirmed.columns:
                confirmed["Expiry Month Key"] = self._month_key(
                    confirmed["Expiry Date"]
                )
            confirmed["Expiry Month Key"] = (
                confirmed["Expiry Month Key"]
                .fillna("")
                .astype(str)
                .str.strip()
            )
            confirmed[confirmed_each_column] = pd.to_numeric(
                confirmed.get(confirmed_each_column, 0),
                errors="coerce",
            ).fillna(0)
            confirmed[confirmed_pack_column] = pd.to_numeric(
                confirmed.get(confirmed_pack_column, 0),
                errors="coerce",
            ).fillna(0)
            confirmed[reserved_each_column] = pd.to_numeric(
                confirmed.get(reserved_each_column, 0),
                errors="coerce",
            ).fillna(0)
            confirmed[reserved_pack_column] = pd.to_numeric(
                confirmed.get(reserved_pack_column, 0),
                errors="coerce",
            ).fillna(0)

            confirmed = (
                confirmed.groupby(
                    [
                        "BN",
                        "Expiry Month Key",
                        "Generic Item Number",
                        "To Address",
                        "GLN",
                    ],
                    dropna=False,
                )
                .agg(
                    **{
                        confirmed_each_column: (
                            confirmed_each_column,
                            "sum",
                        ),
                        confirmed_pack_column: (
                            confirmed_pack_column,
                            "sum",
                        ),
                        reserved_each_column: (
                            reserved_each_column,
                            "sum",
                        ),
                        reserved_pack_column: (
                            reserved_pack_column,
                            "sum",
                        ),
                    }
                )
                .reset_index()
            )

            details = details.merge(
                confirmed,
                on=[
                    "BN",
                    "Expiry Month Key",
                    "Generic Item Number",
                    "To Address",
                    "GLN",
                ],
                how="left",
                validate="many_to_one",
            )
            details[confirmed_each_column] = pd.to_numeric(
                details[confirmed_each_column],
                errors="coerce",
            ).fillna(0)
            details[confirmed_pack_column] = pd.to_numeric(
                details[confirmed_pack_column],
                errors="coerce",
            ).fillna(0)
            details[reserved_each_column] = pd.to_numeric(
                details[reserved_each_column],
                errors="coerce",
            ).fillna(0)
            details[reserved_pack_column] = pd.to_numeric(
                details[reserved_pack_column],
                errors="coerce",
            ).fillna(0)

        details["Available Historical Dispatch Quantity Each"] = (
            pd.to_numeric(
                details["Historical Dispatch Quantity Each"],
                errors="coerce",
            ).fillna(0)
            - pd.to_numeric(
                details[reserved_each_column],
                errors="coerce",
            ).fillna(0)
        ).clip(lower=0)

        details["Available Historical Dispatch Quantity Pack"] = (
            pd.to_numeric(
                details["Historical Dispatch Quantity Pack"],
                errors="coerce",
            ).fillna(0)
            - pd.to_numeric(
                details[reserved_pack_column],
                errors="coerce",
            ).fillna(0)
        ).clip(lower=0)

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
                available = int(
                    max(
                        details.loc[
                            row_index,
                            "Available Historical Dispatch Quantity Pack",
                        ],
                        0,
                    )
                )
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
        sto_incoming_df: pd.DataFrame | None = None,
        sto_return_df: pd.DataFrame | None = None,
    ) -> Dict[str, pd.DataFrame]:
        """Run Full Accept with strict receipt-source separation.

        Supplier Accept/Variance uses TRK5060 only. TRK800 is reconciled in a
        dedicated STO table and can produce Accept only when current RSD
        Quantity Receive Pending is available. TRK49 is returned separately as
        a cancel-dispatch action list and never contributes to Accept/Variance.
        """

        Validator.validate(Normalizer.normalize_sfda(sfda_df.copy()), "SFDA")

        supplier_master = self._supplier_only_master_for_accept(
            batch_master_df,
            supplier_history_df,
        )
        supplier_accept_details = self.build_accept_reconciliation(
            sfda_df,
            supplier_master,
        )
        supplier_variance = self.build_supplier_variance(
            sfda_df,
            supplier_history_df,
        )

        sto_incoming = self.build_sto_incoming_reconciliation(
            sfda_df,
            batch_master_df,
            sto_incoming_df if sto_incoming_df is not None else pd.DataFrame(),
            supplier_accept_details,
        )
        sto_return_cancel = (
            sto_return_df.copy()
            if sto_return_df is not None
            else pd.DataFrame()
        )
        if not sto_return_cancel.empty:
            sto_return_cancel["Required Action"] = "Cancel Previous RSD Dispatch"

        supplier_upload = supplier_accept_details.loc[
            pd.to_numeric(
                supplier_accept_details.get("To Be Accept", 0), errors="coerce"
            ).fillna(0).gt(0)
        ].copy()

        sto_upload = sto_incoming.loc[
            pd.to_numeric(sto_incoming.get("To Be Accept", 0), errors="coerce")
            .fillna(0).gt(0)
        ].copy()

        upload_parts = []
        for frame in (supplier_upload, sto_upload):
            if frame is None or frame.empty:
                continue
            upload_parts.append(
                self._ensure_columns(
                    frame,
                    ["GTIN", "BN", "Expiry Date", "To Be Accept"],
                )[["GTIN", "BN", "Expiry Date", "To Be Accept"]]
            )
        if upload_parts:
            accept_upload = pd.concat(upload_parts, ignore_index=True)
            accept_upload["To Be Accept"] = pd.to_numeric(
                accept_upload["To Be Accept"], errors="coerce"
            ).fillna(0)
            accept_upload = (
                accept_upload.groupby(["GTIN", "BN", "Expiry Date"], dropna=False)["To Be Accept"]
                .sum().reset_index()
            )
        else:
            accept_upload = pd.DataFrame(columns=["GTIN", "BN", "Expiry Date", "To Be Accept"])

        return {
            "accept_details": supplier_accept_details,
            "supplier_variance": supplier_variance,
            "sto_incoming": sto_incoming,
            "sto_return_cancel_dispatch": sto_return_cancel,
            "accept_upload": accept_upload,
        }

    def run_dispatch_reconciliation(
        self,
        inventory_df: pd.DataFrame,
        sfda_df: pd.DataFrame,
        customer_history_df: pd.DataFrame,
        confirmed_full_dispatch_df: pd.DataFrame | None = None,
    ) -> Dict[str, pd.DataFrame]:
        """Run the Full Dispatch stage after Accept is completed in SFDA."""

        Validator.validate(Normalizer.normalize_sfda(sfda_df.copy()), "SFDA")
        dispatch_details = self.build_dispatch_reconciliation(
            inventory_df,
            sfda_df,
            customer_history_df,
            confirmed_full_dispatch_df,
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
