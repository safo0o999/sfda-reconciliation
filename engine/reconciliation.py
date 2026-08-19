from __future__ import annotations

import hashlib
from pathlib import Path
from typing import Any, Dict

import pandas as pd

from engine.normalizer import Normalizer
from engine.validator import Validator
from engine.reference_data import DUMMY_GLN as WAREHOUSE_DUMMY_GLN, load_current_warehouse_gln
from engine.warehouse_context import current_warehouse_id
from engine.inbound_classification import (
    ACCEPT_TYPES,
    SUPPLIER,
    STO_INCOMING,
    STO_RETURN,
    classify_inbound_shipment,
    classification_status,
)


class ReconciliationEngine:
    """Daily SFDA reconciliation engine.

    Accept:
        ASN/ASDT + SFDA

    Dispatch:
        Full Dispatch + SFDA

    Package size is always mapped in this order:
        SFDA[Drug Name] -> Pack Size[Trade Name] -> PackageSize

    After that, SFDA is matched to ASN / Full Dispatch by:
        BN + Expiry Month Key

    BN + month only discovers a candidate batch.  Product identity is then
    verified against the proven Generic Item Number -> SFDA GTIN relationship
    stored in Batch Master before any SFDA quantity is used.

    Batch Master defines the SFDA-relevant universe for Daily Accept. If it is
    unavailable, Daily Accept falls back to exact batches in the current SFDA file.
    """

    MATCH_KEYS = ["BN", "Expiry Month Key"]
    # Preserve Madinah legacy fallback exactly as before. Non-Madinah
    # warehouses use WAREHOUSE_DUMMY_GLN (14 nines) below.
    DUMMY_GLN = "9999999999999"

    def __init__(
        self,
        mode: str,
        sfda_df: pd.DataFrame,
        asn_df: pd.DataFrame | None = None,
        dispatch_df: pd.DataFrame | None = None,
        inventory_df: pd.DataFrame | None = None,
        batch_master_df: pd.DataFrame | None = None,
        processed_transactions_df: pd.DataFrame | None = None,
    ) -> None:
        normalized_mode = str(mode or "").strip().lower()
        if normalized_mode not in {"accept", "dispatch"}:
            raise ValueError("mode must be either 'accept' or 'dispatch'.")

        self.mode = normalized_mode
        self.sfda = sfda_df.copy() if sfda_df is not None else pd.DataFrame()
        self.asn = asn_df.copy() if asn_df is not None else pd.DataFrame()
        self.dispatch = dispatch_df.copy() if dispatch_df is not None else pd.DataFrame()

        # Kept only for backward compatibility with older function_app.py calls.
        # Inventory is intentionally not used by the current Dispatch logic.
        self.inventory = inventory_df.copy() if inventory_df is not None else pd.DataFrame()

        self.processed_transactions = (
            processed_transactions_df.copy()
            if processed_transactions_df is not None
            else pd.DataFrame()
        )

        self.batch_master = (
            batch_master_df.copy()
            if batch_master_df is not None
            else pd.DataFrame()
        )

        config_dir = Path(__file__).resolve().parent.parent / "config"

        self.packsize = pd.read_excel(
            config_dir / "pack_size.xlsx",
            engine="openpyxl",
            dtype=object,
        )
        # Warehouse 1 / Madinah intentionally keeps the existing legacy
        # config/gln.xlsx behavior. Every other warehouse receives only its
        # own SQL mapping; an empty mapping intentionally falls back to the
        # controlled dummy GLN during Dispatch.
        self.gln = load_current_warehouse_gln()

    @staticmethod
    def _expiry_month_key(values: pd.Series) -> pd.Series:
        parsed = pd.to_datetime(values, errors="coerce")
        return parsed.dt.strftime("%Y-%m").fillna("")

    @classmethod
    def _ensure_expiry_month_key(cls, frame: pd.DataFrame) -> pd.DataFrame:
        result = frame.copy()
        if "Expiry Date" not in result.columns:
            result["Expiry Date"] = pd.NaT
        result["Expiry Date"] = Normalizer.date(result["Expiry Date"])
        result["Expiry Month Key"] = cls._expiry_month_key(result["Expiry Date"])
        return result

    def _generic_identity_reference(self) -> pd.DataFrame:
        """Return only one-to-one Generic Item Number -> SFDA identity mappings.

        Batch Master is the daily engine's identity authority.  A mapping is
        trusted only when the Generic resolves to one GTIN and that GTIN resolves
        back to one Generic.  Ambiguous historical relationships are deliberately
        excluded rather than guessed.
        """
        if self.batch_master.empty or "Generic Item Number" not in self.batch_master.columns:
            return pd.DataFrame(columns=["Generic Item Number", "_Proven GTIN", "_Proven Drug Name"])

        master = self.batch_master.copy()
        for column in ["Generic Item Number", "GTIN", "Drug Name"]:
            if column not in master.columns:
                master[column] = ""
            master[column] = Normalizer.text(master[column])

        master = master.loc[
            master["Generic Item Number"].ne("") & master["GTIN"].ne("")
        ].copy()

        # Only exact Batch Master rows that were proven by BN + Expiry Month
        # against SFDA may establish the daily Generic <-> GTIN authority.
        # Rows marked "Missing Batch in SFDA" inherit identity for reporting,
        # but inherited identity must never become new proof on a later run.
        if "Generic Exists in SFDA" in master.columns:
            exact_status = Normalizer.text(master["Generic Exists in SFDA"])
            master = master.loc[exact_status.eq("YES")].copy()
        if master.empty:
            return pd.DataFrame(columns=["Generic Item Number", "_Proven GTIN", "_Proven Drug Name"])

        generic_counts = master.groupby("Generic Item Number")["GTIN"].nunique()
        gtin_counts = master.groupby("GTIN")["Generic Item Number"].nunique()
        good_generics = set(generic_counts.loc[generic_counts.eq(1)].index.astype(str))
        good_gtins = set(gtin_counts.loc[gtin_counts.eq(1)].index.astype(str))
        master = master.loc[
            master["Generic Item Number"].isin(good_generics)
            & master["GTIN"].isin(good_gtins)
        ].copy()
        if master.empty:
            return pd.DataFrame(columns=["Generic Item Number", "_Proven GTIN", "_Proven Drug Name"])

        return (
            master[["Generic Item Number", "GTIN", "Drug Name"]]
            .drop_duplicates(subset=["Generic Item Number"], keep="first")
            .rename(columns={"GTIN": "_Proven GTIN", "Drug Name": "_Proven Drug Name"})
            .reset_index(drop=True)
        )

    def _verify_current_sfda_identity(self, report: pd.DataFrame) -> pd.DataFrame:
        """Reject false BN/month collisions before SFDA quantities are consumed."""
        result = report.copy()
        if result.empty:
            return result

        if "Generic Item Number" not in result.columns:
            result["Generic Item Number"] = ""
        if "GTIN" not in result.columns:
            result["GTIN"] = ""
        result["Generic Item Number"] = Normalizer.text(result["Generic Item Number"])
        result["GTIN"] = Normalizer.text(result["GTIN"])

        reference = self._generic_identity_reference()

        # Upload & Run must NOT depend on Historical Build / Batch Master.
        # The current WMS row can establish identity when it has an exact
        # BN + Expiry Month candidate in the current SFDA file.
        # Historical identity, when available, is only an additional cross-check.
        if reference.empty:
            result["_Proven GTIN"] = ""
            result["_Proven Drug Name"] = ""
        else:
            result = result.merge(
                reference,
                on="Generic Item Number",
                how="left",
                validate="many_to_one",
            )

        current_gtin = Normalizer.text(result["GTIN"])
        proven_gtin = Normalizer.text(result["_Proven GTIN"])
        has_candidate = current_gtin.ne("")
        has_proven = proven_gtin.ne("")

        # BN + Expiry Month is the primary proof for the current daily candidate.
        # If Batch Master already has trusted identity, the GTIN must also agree.
        verified = has_candidate & (~has_proven | current_gtin.eq(proven_gtin))
        collision = has_candidate & has_proven & current_gtin.ne(proven_gtin)

        result["SFDA Identity Status"] = "No Current SFDA Batch"
        result.loc[verified, "SFDA Identity Status"] = "Verified Generic-GTIN"
        result.loc[collision, "SFDA Identity Status"] = "Rejected BN/Expiry Collision"

        for column in [
            "Quantity", "Active", "Quantity sent pending",
            "Quantity Receive Pending", "PackageSize"
        ]:
            if column not in result.columns:
                result[column] = 0
            result.loc[collision, column] = 0
        for column in ["GTIN", "Drug Name"]:
            if column not in result.columns:
                result[column] = ""
            result.loc[collision, column] = ""
        if "Package Size Status" in result.columns:
            result.loc[collision, "Package Size Status"] = "Identity Rejected"

        return result.drop(columns=["_Proven GTIN", "_Proven Drug Name"], errors="ignore")

    @staticmethod
    def _safe_int(value: Any) -> int:
        number = pd.to_numeric(
            pd.Series([value]),
            errors="coerce",
        ).fillna(0).iloc[0]

        return max(0, int(number))

    @staticmethod
    def _join_unique(values: pd.Series) -> str:
        unique_values = []
        seen = set()

        for value in values:
            if pd.isna(value):
                continue

            text = str(value).strip()

            if (
                not text
                or text.lower() == "nan"
                or text in seen
            ):
                continue

            seen.add(text)
            unique_values.append(text)

        return " | ".join(unique_values)

    @staticmethod
    def _copy_first_available_column(
        frame: pd.DataFrame,
        target: str,
        candidates: list[str],
    ) -> None:
        for candidate in candidates:
            if candidate in frame.columns:
                frame[target] = Normalizer.text(
                    frame[candidate]
                )
                return

        frame[target] = ""

    @staticmethod
    def _key_value(value: Any) -> str:
        if pd.isna(value):
            return ""
        if isinstance(value, pd.Timestamp):
            return value.strftime("%Y-%m-%d %H:%M:%S")
        return str(value).strip()

    @classmethod
    def _transaction_key(cls, transaction_type: str, row: pd.Series) -> str:
        if transaction_type == "ACCEPT":
            fields = [
                row.get("Inbound Shipment", ""), row.get("ASN Line", ""),
                row.get("BN", ""), row.get("Expiry Date", ""),
                row.get("Received Date", ""), row.get("Generic Item Number", ""),
            ]
        else:
            fields = [
                row.get("Sales Order Number", ""), row.get("Order Line", ""),
                row.get("BN", ""), row.get("Expiry Date", ""),
                row.get("To Address", ""), row.get("Dispatch Date", ""),
                row.get("Generic Item Number", ""),
            ]
        payload = "|".join(cls._key_value(value) for value in fields)
        return hashlib.sha256(f"{transaction_type}|{payload}".encode("utf-8")).hexdigest()

    def _apply_processing_status(
        self,
        frame: pd.DataFrame,
        transaction_type: str,
        quantity_column: str,
    ) -> pd.DataFrame:
        result = frame.copy()

        # Build transaction identities column-wise instead of DataFrame.apply(axis=1).
        # Daily ASN files can contain tens/hundreds of thousands of rows; the old
        # row-wise pandas apply was one of the main Accept CPU bottlenecks.
        if transaction_type == "ACCEPT":
            field_names = [
                "Inbound Shipment", "ASN Line", "BN", "Expiry Date",
                "Received Date", "Generic Item Number",
            ]
        else:
            field_names = [
                "Sales Order Number", "Order Line", "BN", "Expiry Date",
                "To Address", "Dispatch Date", "Generic Item Number",
            ]

        pieces = []
        for field_name in field_names:
            series = result.get(
                field_name, pd.Series("", index=result.index, dtype=object)
            )
            if pd.api.types.is_datetime64_any_dtype(series):
                text = pd.to_datetime(series, errors="coerce").dt.strftime(
                    "%Y-%m-%d %H:%M:%S"
                ).fillna("")
            else:
                text = series.map(self._key_value)
            pieces.append(text.astype(str))

        payload = pieces[0]
        for piece in pieces[1:]:
            payload = payload.str.cat(piece, sep="|")
        prefixed = transaction_type + "|" + payload
        result["Transaction Key"] = prefixed.map(
            lambda value: hashlib.sha256(value.encode("utf-8")).hexdigest()
        )
        previous_map = {}
        previous_date_map = {}
        if not self.processed_transactions.empty and "Transaction Key" in self.processed_transactions.columns:
            previous = self.processed_transactions.copy()
            previous_qty = pd.to_numeric(
                previous.get("Processed Quantity Each", 0), errors="coerce"
            ).fillna(0)
            previous_map = dict(zip(previous["Transaction Key"].astype(str), previous_qty))
            if "Last Processed At" in previous.columns:
                previous_date_map = dict(zip(
                    previous["Transaction Key"].astype(str), previous["Last Processed At"]
                ))
        result["Previous Quantity Each"] = result["Transaction Key"].map(previous_map).fillna(0)
        result["Current Quantity Each"] = pd.to_numeric(
            result[quantity_column], errors="coerce"
        ).fillna(0)
        result["Quantity Difference"] = (
            result["Current Quantity Each"] - result["Previous Quantity Each"]
        )
        result["Previous Run Date"] = result["Transaction Key"].map(previous_date_map)
        result["Processing Status"] = "New"
        existed = result["Transaction Key"].isin(previous_map)
        same = existed & result["Quantity Difference"].abs().lt(0.000001)
        changed = existed & ~same
        result.loc[same, "Processing Status"] = "Previously Processed"
        result.loc[changed, "Processing Status"] = "Changed Since Last Run"
        result["Effective Quantity Each"] = result["Quantity Difference"].clip(lower=0)
        return result


    @staticmethod
    def _ensure_output_columns(
        frame: pd.DataFrame,
        columns: list[str],
    ) -> pd.DataFrame:
        """Return output with every requested column present and in exact order."""
        result = frame.copy()

        defaults = {
            "Processing Status": "New",
            "Previous Run Date": pd.NaT,
            "Previous Quantity Each": 0.0,
            "Current Quantity Each": 0.0,
            "Quantity Difference": 0.0,
            "Package Size Status": "",
            "Batch Master Status": "",
        }

        for column in columns:
            if column not in result.columns:
                result[column] = defaults.get(column, "")

        return result.reindex(columns=columns)

    @staticmethod
    def _transaction_rows(
        frame: pd.DataFrame,
        transaction_type: str,
    ) -> pd.DataFrame:
        if frame.empty:
            return pd.DataFrame()
        if transaction_type == "ACCEPT":
            reference_number = frame.get("Inbound Shipment", "")
            reference_line = frame.get("ASN Line", "")
            transaction_date = frame.get("Received Date", pd.NaT)
            pack_column = "Current Quantity Pack"
        else:
            reference_number = frame.get("Sales Order Number", "")
            reference_line = frame.get("Order Line", "")
            transaction_date = frame.get("Dispatch Date", pd.NaT)
            pack_column = "Current Quantity Pack"
        rows = pd.DataFrame({
            "Transaction Key": frame["Transaction Key"],
            "BN": frame.get("BN", ""),
            "Expiry Date": frame.get("Expiry Date", pd.NaT),
            "Generic Item Number": frame.get("Generic Item Number", ""),
            "Reference Number": reference_number,
            "Reference Line": reference_line,
            "To Address": frame.get("To Address", ""),
            "Transaction Date": transaction_date,
            "Processed Quantity Each": frame["Current Quantity Each"],
            "Processed Quantity Pack": frame.get(pack_column, 0),
        })
        return rows

    def _normalize_common(self) -> None:
        self.sfda = Normalizer.normalize_sfda(
            self.sfda
        )
        self.sfda = self._ensure_expiry_month_key(self.sfda)
        self.packsize = Normalizer.normalize_packsize(
            self.packsize
        )
        self.gln = Normalizer.normalize_gln(
            self.gln
        )

        if not self.batch_master.empty:
            if "BN" in self.batch_master.columns:
                self.batch_master["BN"] = Normalizer.text(
                    self.batch_master["BN"]
                )

            if "Expiry Date" in self.batch_master.columns:
                self.batch_master["Expiry Date"] = Normalizer.date(
                    self.batch_master["Expiry Date"]
                )
                self.batch_master["Expiry Month Key"] = self._expiry_month_key(
                    self.batch_master["Expiry Date"]
                )

            if "Generic Item Number" in self.batch_master.columns:
                self.batch_master["Generic Item Number"] = Normalizer.text(
                    self.batch_master["Generic Item Number"]
                )

    def _validate_common(self) -> None:
        Validator.validate(
            self.sfda,
            "SFDA",
        )
        Validator.validate(
            self.packsize,
            "PACKSIZE",
        )

    def _pack_lookup(self) -> pd.DataFrame:
        """Prepare the single approved Drug Name -> PackageSize mapping."""
        lookup = self.packsize[
            ["Trade Name", "PackageSize"]
        ].copy()

        lookup["Drug Name"] = Normalizer.text(
            lookup["Trade Name"]
        )
        lookup["PackageSize"] = pd.to_numeric(
            lookup["PackageSize"],
            errors="coerce",
        )

        lookup = lookup[
            lookup["Drug Name"].ne("")
            & lookup["PackageSize"].notna()
            & lookup["PackageSize"].gt(0)
        ].copy()

        return (
            lookup[
                ["Drug Name", "PackageSize"]
            ]
            .drop_duplicates(
                subset=["Drug Name"],
                keep="first",
            )
            .reset_index(drop=True)
        )

    def _sfda_summary(self) -> pd.DataFrame:
        """Aggregate SFDA by BN + expiry month, then attach PackageSize.

        A BN/month that points to more than one GTIN is ambiguous and is
        removed from automatic daily reconciliation rather than guessed.
        PackageSize remains mapped from SFDA Drug Name only.
        """
        source = self.sfda.copy()
        source["GTIN"] = Normalizer.text(source["GTIN"])
        source["Drug Name"] = Normalizer.text(source["Drug Name"])

        identity_counts = (
            source.loc[source["GTIN"].ne("")]
            .groupby(self.MATCH_KEYS, dropna=False)["GTIN"]
            .nunique()
            .rename("_GTIN Count")
            .reset_index()
        )

        sfda_summary = (
            source.groupby(self.MATCH_KEYS, dropna=False)
            .agg(
                **{"SFDA Expiry Date": ("Expiry Date", "first")},
                GTIN=("GTIN", "first"),
                **{
                    "Drug Name": ("Drug Name", "first"),
                    "Quantity": ("Quantity", "sum"),
                    "Active": ("Active", "sum"),
                    "Quantity sent pending": ("Quantity sent pending", "sum"),
                    "Quantity Receive Pending": ("Quantity Receive Pending", "sum"),
                },
            )
            .reset_index()
        )

        if not identity_counts.empty:
            sfda_summary = sfda_summary.merge(
                identity_counts, on=self.MATCH_KEYS, how="left", validate="one_to_one"
            )
            sfda_summary = sfda_summary.loc[
                pd.to_numeric(sfda_summary["_GTIN Count"], errors="coerce").fillna(0).eq(1)
            ].drop(columns=["_GTIN Count"], errors="ignore")

        sfda_summary = sfda_summary.merge(
            self._pack_lookup(),
            on="Drug Name",
            how="left",
            validate="many_to_one",
        )

        sfda_summary["PackageSize"] = pd.to_numeric(
            sfda_summary["PackageSize"], errors="coerce"
        )
        valid_package = sfda_summary["PackageSize"].notna() & sfda_summary["PackageSize"].gt(0)
        sfda_summary["Package Size Status"] = valid_package.map(
            {True: "Mapped", False: "Missing"}
        )
        return sfda_summary

    def _enrich_with_master(
        self,
        report: pd.DataFrame,
    ) -> pd.DataFrame:
        report = report.copy()
        report["Batch Master Status"] = "Not Available"

        if self.batch_master.empty:
            return report

        master = self.batch_master.copy()

        keep = [
            column
            for column in [
                "BN",
                "Expiry Date",
                "Expiry Month Key",
                "Generic Item Number",
                "Total Received Qty",
                "Total Receive Qty",
                "Total Dispatched Qty",
                "Generic Exists in SFDA",
            ]
            if column in master.columns
        ]

        identity_keys = self.MATCH_KEYS + ["Generic Item Number"]
        if not set(identity_keys).issubset(keep) or "Generic Item Number" not in report.columns:
            return report

        master = (
            master[keep]
            .drop_duplicates(
                subset=identity_keys,
                keep="first",
            )
        )

        report = report.merge(
            master,
            on=identity_keys,
            how="left",
            suffixes=("", " Master"),
        )

        candidate_columns = [
            "Generic Exists in SFDA",
            "Total Received Qty",
            "Total Receive Qty",
            "Total Dispatched Qty",
        ]

        matched = pd.Series(
            False,
            index=report.index,
        )

        for column in candidate_columns:
            if column in report.columns:
                matched = matched | report[column].notna()

        report["Batch Master Status"] = matched.map(
            {
                True: "Matched",
                False: "Not Found",
            }
        )

        return report

    def _filter_accept_sfda_relevant(self, frame: pd.DataFrame) -> pd.DataFrame:
        """Keep ASN rows whose BN + Expiry Month exists in CURRENT SFDA.

        Upload & Run is intentionally independent from Historical Build /
        Batch Master.  The current SFDA file defines the daily batch universe.
        Product identity is then taken from the matching WMS row and optionally
        cross-checked against trusted Batch Master identity when available.
        """
        if frame.empty:
            return frame.copy()

        result = self._ensure_expiry_month_key(frame)

        # _sfda_summary() already excludes ambiguous SFDA BN/month keys that
        # point to more than one GTIN, so only safe current-SFDA keys survive.
        sfda_keys = self._sfda_summary()[self.MATCH_KEYS].drop_duplicates()
        if sfda_keys.empty:
            result["SFDA Relevance Status"] = "No Current SFDA Batch"
            return result.iloc[0:0].copy()

        result = result.merge(
            sfda_keys.assign(_CurrentSFDAKey=True),
            on=self.MATCH_KEYS,
            how="left",
            validate="many_to_one",
        )
        keep = result["_CurrentSFDAKey"].fillna(False).astype(bool)
        result["SFDA Relevance Status"] = "No Current SFDA Batch"
        result.loc[keep, "SFDA Relevance Status"] = "BN + Expiry Month Found in Current SFDA"
        return result.loc[keep].drop(columns=["_CurrentSFDAKey"], errors="ignore").copy()

    def _apply_daily_generic_reference(self, report: pd.DataFrame) -> pd.DataFrame:
        """Fill SFDA identity for a relevant missing batch using its Generic.

        Exact SFDA batch data always wins.  When the daily ASN batch is missing
        from the current SFDA report but its Generic Item Number belongs to the
        SFDA-relevant Batch Master, use another exact SFDA-relevant master row
        for that Generic to recover GTIN / Drug Name.  PackageSize is then
        mapped again from the approved Pack Size master by Drug Name, so the
        Pack Size master remains the only PackageSize authority.

        Batch-level SFDA quantities are intentionally NOT copied from another
        batch; Quantity / Active / pending values stay zero for a missing batch.
        """
        result = report.copy()
        if result.empty:
            return result

        # Normalize columns that may have come from a left merge with current SFDA.
        for column in ["GTIN", "Drug Name"]:
            if column not in result.columns:
                result[column] = ""
            result[column] = Normalizer.text(result[column])

        if "PackageSize" not in result.columns:
            result["PackageSize"] = 0.0
        result["PackageSize"] = pd.to_numeric(
            result["PackageSize"], errors="coerce"
        ).fillna(0)

        reference = self._generic_identity_reference()
        if not reference.empty:
            reference = reference.rename(
                columns={"_Proven GTIN": "_Generic GTIN", "_Proven Drug Name": "_Generic Drug Name"}
            )
            result["Generic Item Number"] = Normalizer.text(
                result.get("Generic Item Number", pd.Series("", index=result.index, dtype=object))
            )
            result = result.merge(
                reference, on="Generic Item Number", how="left", validate="many_to_one"
            )
            missing_gtin = result["GTIN"].eq("")
            missing_drug = result["Drug Name"].eq("")
            result.loc[missing_gtin, "GTIN"] = Normalizer.text(
                result.loc[missing_gtin, "_Generic GTIN"]
            )
            result.loc[missing_drug, "Drug Name"] = Normalizer.text(
                result.loc[missing_drug, "_Generic Drug Name"]
            )
            result = result.drop(
                columns=["_Generic GTIN", "_Generic Drug Name"], errors="ignore"
            )

        # PackageSize is ALWAYS resolved from the approved Pack Size master.
        # Exact current-SFDA mappings already have PackageSize; fill only missing
        # values after Generic-level Drug Name recovery.
        pack_lookup = self._pack_lookup().rename(
            columns={"PackageSize": "_Mapped PackageSize"}
        )
        result = result.merge(
            pack_lookup,
            on="Drug Name",
            how="left",
            validate="many_to_one",
        )
        mapped_pack = pd.to_numeric(
            result.get("_Mapped PackageSize", 0), errors="coerce"
        ).fillna(0)
        current_pack = pd.to_numeric(
            result.get("PackageSize", 0), errors="coerce"
        ).fillna(0)
        result["PackageSize"] = current_pack.where(current_pack.gt(0), mapped_pack)
        result["Package Size Status"] = result["PackageSize"].gt(0).map(
            {True: "Mapped", False: "Missing from Pack Size Master"}
        )
        return result.drop(columns=["_Mapped PackageSize"], errors="ignore")

    def _run_accept(self) -> Dict[str, pd.DataFrame]:
        if self.asn.empty:
            raise ValueError(
                "ASN/ASDT file is required for Accept reconciliation."
            )

        self.asn = Normalizer.normalize_asn(self.asn)
        self.asn = self._ensure_expiry_month_key(self.asn)
        Validator.validate(self.asn, "ASN")
        self.asn = self._apply_processing_status(
            self.asn, "ACCEPT", "Received Quantity"
        )

        self._copy_first_available_column(
            self.asn,
            "Description",
            ["Description", "Item Description", "Generic Item Description"],
        )
        self._copy_first_available_column(
            self.asn,
            "Supplier Code",
            ["Supplier Code", "Vendor Code", "Supplier Number"],
        )
        self._copy_first_available_column(
            self.asn,
            "Item Family Group",
            ["Item Family Group", "Item Family", "Family Group"],
        )

        # Daily Accept scope rule:
        # Laboratory Supplies are excluded BEFORE matching, grouping, Batch Master
        # lookup, history persistence, or any Daily classification/output.
        item_family = Normalizer.text(
            self.asn.get(
                "Item Family Group",
                pd.Series("", index=self.asn.index, dtype=object),
            )
        ).str.upper()
        self.asn = self.asn.loc[item_family.ne("LABORATORY SUPPLIES")].copy()

        self.asn["Receipt Type"] = self.asn.get(
            "Inbound Shipment",
            pd.Series("", index=self.asn.index, dtype=object),
        ).map(classify_inbound_shipment)

        # Preserve the non-LAB daily receipt source for:
        #   1) Daily Missing From SFDA detection, and
        #   2) incremental receipt history persistence.
        daily_receipt_source = self.asn.copy()

        # Normal Accept remains driven by CURRENT SFDA BN + Expiry Month.
        self.asn = self._filter_accept_sfda_relevant(self.asn)

        # Regulatory routing:
        #   Supplier   -> main Accept Details and first priority against RSD pending.
        #   STO In     -> separate STO file, uses only pending remaining after Supplier.
        #   STO Return -> separate cancellation file; never produces Accept.
        #   TRK30/43/74/Unclassified -> completely invisible to Daily reports/calculation.
        supplier_asn = self.asn.loc[
            self.asn["Receipt Type"].eq(SUPPLIER)
        ].copy()
        sto_incoming_asn = self.asn.loc[
            self.asn["Receipt Type"].eq(STO_INCOMING)
        ].copy()
        sto_return_asn = self.asn.loc[
            self.asn["Receipt Type"].eq(STO_RETURN)
        ].copy()
        eligible_asn = pd.concat(
            [supplier_asn, sto_incoming_asn],
            ignore_index=True,
        )

        receiving_columns = self.MATCH_KEYS + [
            "Generic Item Number",
            "Receipt Type",
            "Expiry Date",
            "Trade Name",
            "Received Quantity Each",
            "Description",
            "Inbound Shipment",
            "Supplier Name",
            "Supplier Code",
            "Item Family Group",
            "Processing Status",
            "Previous Run Date",
            "Previous Quantity Each",
            "Current Quantity Each",
            "Quantity Difference",
        ]
        receiving = (
            eligible_asn.groupby(
                self.MATCH_KEYS + ["Generic Item Number", "Receipt Type"],
                dropna=False,
            )
            .agg(
                **{
                    "Expiry Date": ("Expiry Date", "first"),
                    "Trade Name": ("Trade Name", "first"),
                    "Received Quantity Each": ("Effective Quantity Each", "sum"),
                    "Description": ("Description", self._join_unique),
                    "Inbound Shipment": ("Inbound Shipment", self._join_unique),
                    "Supplier Name": ("Supplier Name", self._join_unique),
                    "Supplier Code": ("Supplier Code", self._join_unique),
                    "Item Family Group": ("Item Family Group", self._join_unique),
                    "Processing Status": ("Processing Status", self._join_unique),
                    "Previous Run Date": ("Previous Run Date", "max"),
                    "Previous Quantity Each": ("Previous Quantity Each", "sum"),
                    "Current Quantity Each": ("Current Quantity Each", "sum"),
                    "Quantity Difference": ("Quantity Difference", "sum"),
                }
            )
            .reset_index()
            if not eligible_asn.empty
            else pd.DataFrame(columns=receiving_columns)
        )

        sfda_summary = self._sfda_summary()
        if receiving.empty:
            report = pd.DataFrame()
        else:
            report = receiving.merge(
                sfda_summary,
                on=self.MATCH_KEYS,
                how="left",
                validate="many_to_one",
            )
            report = self._verify_current_sfda_identity(report)
            report = self._apply_daily_generic_reference(report)
            if "SFDA Expiry Date" in report.columns:
                verified = report.get("SFDA Identity Status", "").eq("Verified Generic-GTIN")
                report.loc[verified, "Expiry Date"] = report.loc[verified, "SFDA Expiry Date"]

        # A strict identity filter can legitimately leave zero Supplier/STO-In rows.
        # Keep the empty report schema-complete so a no-action run returns valid
        # empty outputs instead of failing on downstream column access.
        empty_safe_defaults = {
            "BN": "",
            "Expiry Month Key": "",
            "Generic Item Number": "",
            "Receipt Type": "",
            "Expiry Date": pd.NaT,
            "Trade Name": "",
            "Received Quantity Each": 0.0,
            "Description": "",
            "Inbound Shipment": "",
            "Supplier Name": "",
            "Supplier Code": "",
            "Item Family Group": "",
            "Processing Status": "New",
            "Previous Run Date": pd.NaT,
            "Previous Quantity Each": 0.0,
            "Current Quantity Each": 0.0,
            "Quantity Difference": 0.0,
        }
        for column, default in empty_safe_defaults.items():
            if column not in report.columns:
                report[column] = default

        for column in [
            "Quantity", "Active", "Quantity sent pending", "Quantity Receive Pending",
            "PackageSize",
        ]:
            if column not in report.columns:
                report[column] = 0
            report[column] = pd.to_numeric(
                report[column], errors="coerce"
            ).fillna(0)

        if "Package Size Status" not in report.columns:
            report["Package Size Status"] = report["PackageSize"].gt(0).map(
                {True: "Mapped", False: "Missing from Pack Size Master"}
            )

        report["Received Quantity Pack"] = 0.0
        valid_package = report["PackageSize"].gt(0)
        report.loc[valid_package, "Received Quantity Pack"] = (
            pd.to_numeric(
                report.loc[valid_package, "Received Quantity Each"], errors="coerce"
            ).fillna(0)
            / report.loc[valid_package, "PackageSize"]
        )

        # Supplier ALWAYS receives priority against Quantity Receive Pending.
        # STO Incoming can consume only the quantity remaining after Supplier.
        report["To Be Accept"] = 0
        report["Available RSD Receive Pending"] = 0.0
        report["STO Pending RSD Qty"] = 0.0
        report["Required Action"] = ""
        report["Receipt Type Priority"] = report["Receipt Type"].map(
            {SUPPLIER: 0, STO_INCOMING: 1}
        ).fillna(9)
        report = report.sort_values(
            self.MATCH_KEYS + ["Generic Item Number", "Receipt Type Priority"],
            kind="stable",
        ).reset_index(drop=True)

        valid_package = pd.to_numeric(
            report["PackageSize"], errors="coerce"
        ).fillna(0).gt(0)

        for _, group in report.groupby(
            self.MATCH_KEYS + ["Generic Item Number"], dropna=False, sort=False
        ):
            pending = float(
                pd.to_numeric(
                    pd.Series([group["Quantity Receive Pending"].iloc[0]]),
                    errors="coerce",
                ).fillna(0).iloc[0]
            )
            remaining = max(0.0, pending)
            for index in group.index:
                report.at[index, "Available RSD Receive Pending"] = remaining
                if not bool(valid_package.loc[index]):
                    continue
                requested = float(max(0, report.at[index, "Received Quantity Pack"]))
                allocated = min(remaining, requested)
                report.at[index, "To Be Accept"] = self._safe_int(allocated)
                remaining = max(
                    0.0,
                    remaining - float(report.at[index, "To Be Accept"]),
                )

        sto_mask = report["Receipt Type"].eq(STO_INCOMING)
        report.loc[sto_mask, "STO Pending RSD Qty"] = (
            pd.to_numeric(
                report.loc[sto_mask, "Received Quantity Pack"], errors="coerce"
            ).fillna(0)
            - pd.to_numeric(
                report.loc[sto_mask, "To Be Accept"], errors="coerce"
            ).fillna(0)
        ).clip(lower=0)

        # STO status makes the supplier-first decision visible in its own file.
        sto_has_accept = sto_mask & pd.to_numeric(
            report["To Be Accept"], errors="coerce"
        ).fillna(0).gt(0)
        sto_has_gap = sto_mask & pd.to_numeric(
            report["STO Pending RSD Qty"], errors="coerce"
        ).fillna(0).gt(0)
        sto_ready = sto_has_accept & ~sto_has_gap
        sto_partial = sto_has_accept & sto_has_gap
        sto_missing = sto_mask & ~sto_has_accept & sto_has_gap

        report.loc[sto_mask, "Required Action"] = (
            "Ask sending warehouse to dispatch the missing quantity through RSD"
        )
        report.loc[sto_ready, "Required Action"] = "Accept available RSD transfer"
        report.loc[sto_partial, "Required Action"] = (
            "Accept available quantity and follow up remaining quantity"
        )
        report.loc[
            sto_mask & ~valid_package,
            "Required Action",
        ] = "Complete Package Size mapping"

        report = self._enrich_with_master(report)
        positive_accept = pd.to_numeric(
            report["To Be Accept"], errors="coerce"
        ).fillna(0).gt(0)
        report.loc[positive_accept, "Processing Status"] = "Pending Confirmation"
        report.loc[sto_missing, "Processing Status"] = (
            "STO Incoming - RSD Transfer Not Available"
        )
        report.loc[sto_partial, "Processing Status"] = (
            "STO Incoming - Partial RSD Transfer Available"
        )

        # Accept CSV can contain Supplier + STO, but each batch's RSD pending was
        # allocated only once above, Supplier first then STO.
        accept_source = report.loc[positive_accept].copy()
        if accept_source.empty:
            accept = pd.DataFrame(columns=["GTIN", "To Be Accept", "BN", "Expiry Date"])
        else:
            accept_source = accept_source.loc[
                Normalizer.text(accept_source.get("GTIN", "")).ne("")
            ].copy()
            accept = (
                accept_source.groupby(
                    ["GTIN", "BN", "Expiry Date"], dropna=False
                )["To Be Accept"]
                .sum().reset_index()
                if not accept_source.empty
                else pd.DataFrame(columns=["GTIN", "BN", "Expiry Date", "To Be Accept"])
            )
            accept = accept[["GTIN", "To Be Accept", "BN", "Expiry Date"]]

        details_columns = [
            "Receipt Type",
            "GTIN",
            "Drug Name",
            "BN",
            "Expiry Date",
            "PackageSize",
            "Active",
            "Quantity sent pending",
            "Quantity Receive Pending",
            "Available RSD Receive Pending",
            "Generic Item Number",
            "Received Quantity Each",
            "Received Quantity Pack",
            "Description",
            "Inbound Shipment",
            "Supplier Name",
            "Supplier Code",
            "Item Family Group",
            "To Be Accept",
            "STO Pending RSD Qty",
            "Required Action",
            "Processing Status",
            "Previous Run Date",
            "Previous Quantity Each",
            "Current Quantity Each",
            "Quantity Difference",
            "Package Size Status",
            "Batch Master Status",
        ]

        # Daily Missing From SFDA
        # -----------------------
        # A daily receipt batch is considered "Missing from SFDA" only when:
        #   - its BN + Expiry Month is NOT present in the current SFDA report;
        #   - its Generic Item Number has a trusted identity in Batch Master;
        #   - it is an eligible receipt type (Supplier or STO Incoming);
        #   - Received Quantity Each is greater than zero.
        #
        # Batch Master is therefore required only for this missing-batch
        # classification. It is NOT required for normal Daily Accept matching.
        daily_missing_columns = [
            "Receipt Type",
            "GTIN",
            "Drug Name",
            "BN",
            "Expiry Date",
            "Generic Item Number",
            "Received Quantity Each",
            "Description",
            "Inbound Shipment",
            "Supplier Name",
            "Supplier Code",
            "Item Family Group",
            "Status",
            "Required Action",
        ]
        daily_missing_from_sfda = pd.DataFrame(columns=daily_missing_columns)

        missing_source = daily_receipt_source.loc[
            daily_receipt_source["Receipt Type"].isin([SUPPLIER, STO_INCOMING])
        ].copy()

        if not missing_source.empty:
            current_sfda_keys = self._sfda_summary()[self.MATCH_KEYS].drop_duplicates()
            if not current_sfda_keys.empty:
                missing_source = missing_source.merge(
                    current_sfda_keys.assign(_ExistsInCurrentSFDA=True),
                    on=self.MATCH_KEYS,
                    how="left",
                    validate="many_to_one",
                )
                missing_source = missing_source.loc[
                    ~missing_source["_ExistsInCurrentSFDA"].fillna(False).astype(bool)
                ].drop(columns=["_ExistsInCurrentSFDA"], errors="ignore")
            # If the current SFDA key set is empty, every eligible non-LAB receipt
            # is a candidate, but it still must pass the trusted Generic test below.

            trusted_generic = self._generic_identity_reference()
            if not missing_source.empty and not trusted_generic.empty:
                missing_source["Generic Item Number"] = Normalizer.text(
                    missing_source["Generic Item Number"]
                )
                missing_source = missing_source.merge(
                    trusted_generic,
                    on="Generic Item Number",
                    how="inner",
                    validate="many_to_one",
                )

                missing_source["GTIN"] = Normalizer.text(
                    missing_source["_Proven GTIN"]
                )
                missing_source["Drug Name"] = Normalizer.text(
                    missing_source["_Proven Drug Name"]
                )

                daily_missing_from_sfda = (
                    missing_source.groupby(
                        self.MATCH_KEYS
                        + ["Generic Item Number", "Receipt Type", "GTIN", "Drug Name"],
                        dropna=False,
                    )
                    .agg(
                        **{
                            "Expiry Date": ("Expiry Date", "first"),
                            "Received Quantity Each": ("Effective Quantity Each", "sum"),
                            "Description": ("Description", self._join_unique),
                            "Inbound Shipment": ("Inbound Shipment", self._join_unique),
                            "Supplier Name": ("Supplier Name", self._join_unique),
                            "Supplier Code": ("Supplier Code", self._join_unique),
                            "Item Family Group": ("Item Family Group", self._join_unique),
                        }
                    )
                    .reset_index()
                )
                daily_missing_from_sfda["Received Quantity Each"] = pd.to_numeric(
                    daily_missing_from_sfda["Received Quantity Each"],
                    errors="coerce",
                ).fillna(0)
                daily_missing_from_sfda = daily_missing_from_sfda.loc[
                    daily_missing_from_sfda["Received Quantity Each"].gt(0)
                ].copy()
                daily_missing_from_sfda["Status"] = "Missing Batch in SFDA"
                daily_missing_from_sfda["Required Action"] = "Register Batch in SFDA"
                daily_missing_from_sfda = self._ensure_output_columns(
                    daily_missing_from_sfda,
                    daily_missing_columns,
                )

        # Main Accept Details = Supplier ONLY.
        # Business rule: batches with zero received quantity are not actionable
        # receipts and must not appear in Accept Details.
        supplier_received_qty = pd.to_numeric(
            report["Received Quantity Each"], errors="coerce"
        ).fillna(0)
        supplier_details = self._ensure_output_columns(
            report.loc[
                report["Receipt Type"].eq(SUPPLIER)
                & supplier_received_qty.gt(0)
            ].copy(),
            details_columns,
        )

        # STO Incoming = ALL SFDA-relevant TRK800 rows, not only missing ones.
        # This file shows what can be accepted and what needs sender follow-up.
        sto_incoming_details = self._ensure_output_columns(
            report.loc[report["Receipt Type"].eq(STO_INCOMING)].copy(),
            details_columns,
        )

        # STO Return = separate RSD cancellation action only. Customer Return,
        # Reservation, Principal and Unclassified rows are intentionally omitted.
        sto_return_cancel = pd.DataFrame(columns=details_columns)
        if not sto_return_asn.empty:
            sto_return_cancel = (
                sto_return_asn.groupby(
                    self.MATCH_KEYS + ["Generic Item Number"],
                    dropna=False,
                )
                .agg(
                    **{
                        "Expiry Date": ("Expiry Date", "first"),
                        "Trade Name": ("Trade Name", "first"),
                        "Received Quantity Each": ("Effective Quantity Each", "sum"),
                        "Description": ("Description", self._join_unique),
                        "Inbound Shipment": ("Inbound Shipment", self._join_unique),
                        "Supplier Name": ("Supplier Name", self._join_unique),
                        "Supplier Code": ("Supplier Code", self._join_unique),
                        "Item Family Group": ("Item Family Group", self._join_unique),
                        "Previous Run Date": ("Previous Run Date", "max"),
                        "Previous Quantity Each": ("Previous Quantity Each", "sum"),
                        "Current Quantity Each": ("Current Quantity Each", "sum"),
                        "Quantity Difference": ("Quantity Difference", "sum"),
                    }
                )
                .reset_index()
            )
            sto_return_cancel["Receipt Type"] = STO_RETURN
            sto_return_cancel = sto_return_cancel.merge(
                sfda_summary,
                on=self.MATCH_KEYS,
                how="left",
                validate="many_to_one",
            )
            sto_return_cancel = self._verify_current_sfda_identity(sto_return_cancel)
            sto_return_cancel = self._apply_daily_generic_reference(sto_return_cancel)
            for column in [
                "Quantity", "Active", "Quantity sent pending",
                "Quantity Receive Pending", "PackageSize",
            ]:
                if column not in sto_return_cancel.columns:
                    sto_return_cancel[column] = 0
                sto_return_cancel[column] = pd.to_numeric(
                    sto_return_cancel[column], errors="coerce"
                ).fillna(0)
            sto_return_cancel["Received Quantity Pack"] = 0.0
            valid_return_pack = sto_return_cancel["PackageSize"].gt(0)
            sto_return_cancel.loc[
                valid_return_pack, "Received Quantity Pack"
            ] = (
                pd.to_numeric(
                    sto_return_cancel.loc[
                        valid_return_pack, "Received Quantity Each"
                    ],
                    errors="coerce",
                ).fillna(0)
                / sto_return_cancel.loc[valid_return_pack, "PackageSize"]
            )
            sto_return_cancel["To Be Accept"] = 0
            sto_return_cancel["Available RSD Receive Pending"] = 0
            sto_return_cancel["STO Pending RSD Qty"] = 0
            sto_return_cancel["Required Action"] = "Cancel Previous RSD Dispatch"
            sto_return_cancel["Processing Status"] = (
                "STO Return - Cancel Previous RSD Dispatch"
            )
            sto_return_cancel = self._enrich_with_master(sto_return_cancel)
            sto_return_cancel = self._ensure_output_columns(
                sto_return_cancel,
                details_columns,
            )

        # Pending/processed state includes only Supplier + STO Incoming.  STO
        # Return and excluded TRKs can never be marked as SFDA Accept processed.
        accept_transactions = eligible_asn.copy()
        batch_limits = report[
            self.MATCH_KEYS + ["Generic Item Number", "Receipt Type", "PackageSize", "To Be Accept"]
        ].copy()
        accept_transactions = accept_transactions.merge(
            batch_limits,
            on=self.MATCH_KEYS + ["Generic Item Number", "Receipt Type"],
            how="left",
            validate="many_to_one",
        )
        accept_transactions["Current Quantity Pack"] = 0.0
        accept_transactions["Pending Submit Quantity Each"] = 0.0
        accept_transactions["Pending Submit Quantity Pack"] = 0.0

        package_size = pd.to_numeric(
            accept_transactions.get("PackageSize", 0), errors="coerce"
        ).fillna(0)
        effective_each = pd.to_numeric(
            accept_transactions.get("Effective Quantity Each", 0), errors="coerce"
        ).fillna(0).clip(lower=0)
        current_each = pd.to_numeric(
            accept_transactions.get("Current Quantity Each", 0), errors="coerce"
        ).fillna(0).clip(lower=0)
        valid_transaction_package = package_size.gt(0)
        accept_transactions.loc[
            valid_transaction_package, "Current Quantity Pack"
        ] = (
            current_each.loc[valid_transaction_package]
            / package_size.loc[valid_transaction_package]
        )

        for _, limit_row in batch_limits.iterrows():
            remaining_pack = float(
                pd.to_numeric(
                    pd.Series([limit_row.get("To Be Accept", 0)]),
                    errors="coerce",
                ).fillna(0).iloc[0]
            )
            if remaining_pack <= 0:
                continue

            mask = accept_transactions["Receipt Type"].eq(
                limit_row.get("Receipt Type", "")
            )
            mask = mask & (
                Normalizer.text(accept_transactions["Generic Item Number"])
                == str(limit_row.get("Generic Item Number", "")).strip()
            )
            for key in self.MATCH_KEYS:
                if key == "Expiry Date":
                    mask = mask & (
                        pd.to_datetime(accept_transactions[key], errors="coerce")
                        == pd.to_datetime(limit_row.get(key), errors="coerce")
                    )
                else:
                    mask = mask & (
                        accept_transactions[key].astype(str)
                        == str(limit_row.get(key, ""))
                    )

            for index in accept_transactions.index[mask]:
                psize = package_size.loc[index]
                if pd.isna(psize) or float(psize) <= 0:
                    continue
                available_each = float(effective_each.loc[index])
                if available_each <= 0:
                    continue
                available_pack = available_each / float(psize)
                allocated_pack = min(remaining_pack, available_pack)
                if allocated_pack <= 0:
                    continue
                allocated_each = min(
                    available_each, allocated_pack * float(psize)
                )
                accept_transactions.at[
                    index, "Pending Submit Quantity Pack"
                ] = allocated_pack
                accept_transactions.at[
                    index, "Pending Submit Quantity Each"
                ] = allocated_each
                remaining_pack -= allocated_pack
                if remaining_pack <= 0.0000001:
                    break

        pending_source = accept_transactions.loc[
            pd.to_numeric(
                accept_transactions["Pending Submit Quantity Pack"],
                errors="coerce",
            ).fillna(0).gt(0)
        ].copy()
        if pending_source.empty:
            pending_transactions = pd.DataFrame()
        else:
            pending_source["Current Quantity Each"] = pending_source[
                "Pending Submit Quantity Each"
            ]
            pending_source["Current Quantity Pack"] = pending_source[
                "Pending Submit Quantity Pack"
            ]
            pending_transactions = self._transaction_rows(
                pending_source, "ACCEPT"
            )

        return {
            "report": supplier_details.reset_index(drop=True),
            "accept": accept,
            "dispatch": pd.DataFrame(),
            "processed_transactions": self._transaction_rows(
                accept_transactions, "ACCEPT"
            ),
            "pending_confirmation_transactions": pending_transactions,
            "sto_incoming_followup": sto_incoming_details.reset_index(drop=True),
            "sto_return_cancel_dispatch": sto_return_cancel.reset_index(drop=True),
            "daily_missing_from_sfda": daily_missing_from_sfda.reset_index(drop=True),
            # Internal source used by function_app for incremental history.
            # It is intentionally non-LAB and is not exported directly.
            "history_asn": daily_receipt_source.reset_index(drop=True),
        }

    def _run_dispatch(self) -> Dict[str, pd.DataFrame]:
        if self.dispatch.empty:
            raise ValueError(
                "Full Dispatch file is required for Dispatch reconciliation."
            )

        # Inventory is intentionally not required and not used.
        self.dispatch = Normalizer.normalize_dispatch(
            self.dispatch
        )
        self.dispatch = self._ensure_expiry_month_key(self.dispatch)
        Validator.validate(
            self.dispatch,
            "DISPATCH",
        )
        self.dispatch = self._apply_processing_status(
            self.dispatch, "DISPATCH", "Dispatched Quantity"
        )

        # SFDA receives PackageSize first through:
        # SFDA[Drug Name] -> Pack Size[Trade Name].
        # BN + Expiry Month locates the current SFDA batch.  The matching WMS
        # row supplies Generic identity; Batch Master is only an optional cross-check.
        sfda_batches = self._sfda_summary()

        details = self.dispatch.merge(
            sfda_batches,
            on=self.MATCH_KEYS,
            how="inner",
            validate="many_to_one",
        )
        details = self._verify_current_sfda_identity(details)
        details = details.loc[
            details["SFDA Identity Status"].eq("Verified Generic-GTIN")
        ].copy()
        if "SFDA Expiry Date" in details.columns:
            details["Expiry Date"] = details["SFDA Expiry Date"]

        details = details.reset_index(drop=True)
        details["_Source Order"] = range(len(details))

        valid_package = (
            details["PackageSize"].notna()
            & details["PackageSize"].gt(0)
        )

        # Keep every original Full Dispatch row without aggregation.
        details["Dispatch Quantity Each"] = pd.to_numeric(
            details["Effective Quantity Each"],
            errors="coerce",
        ).fillna(0)

        details["Current Quantity Pack"] = 0.0
        details.loc[valid_package, "Current Quantity Pack"] = (
            details.loc[valid_package, "Current Quantity Each"]
            / details.loc[valid_package, "PackageSize"]
        )

        details["Dispatch Quantity Pack"] = 0.0
        details.loc[
            valid_package,
            "Dispatch Quantity Pack",
        ] = (
            details.loc[
                valid_package,
                "Dispatch Quantity Each",
            ]
            / details.loc[
                valid_package,
                "PackageSize",
            ]
        )

        # CSV quantities must be whole packs. Allocate chronologically per
        # BN + Expiry Date and never exceed SFDA Active for that batch.
        details["Eligible Dispatch Pack"] = (
            pd.to_numeric(
                details["Dispatch Quantity Pack"],
                errors="coerce",
            )
            .fillna(0)
            .clip(lower=0)
            .astype(int)
        )

        details = details.sort_values(
            [
                "BN",
                "Expiry Date",
                "Dispatch Date",
                "_Source Order",
            ],
            kind="stable",
        ).reset_index(drop=True)

        details["Allocated To Be Dispatch"] = 0

        for _, indexes in details.groupby(
            self.MATCH_KEYS + ["Generic Item Number"],
            sort=False,
            dropna=False,
        ).groups.items():
            index_list = list(indexes)
            remaining = self._safe_int(
                details.loc[index_list[0], "Active"]
            )

            for row_index in index_list:
                if remaining <= 0:
                    break

                eligible = self._safe_int(
                    details.loc[
                        row_index,
                        "Eligible Dispatch Pack",
                    ]
                )
                allocated = min(eligible, remaining)
                details.loc[
                    row_index,
                    "Allocated To Be Dispatch",
                ] = allocated
                remaining -= allocated

        # Add GLN at row level so the same detailed rows are used directly
        # to generate customer CSV files.
        gln = (
            self.gln[
                ["To Address", "GLN"]
            ]
            .drop_duplicates(
                subset=["To Address"],
                keep="first",
            )
        )

        details = details.merge(
            gln,
            on="To Address",
            how="left",
        )

        missing_gln = (
            details["GLN"].isna()
            | details["GLN"]
            .astype(str)
            .str.strip()
            .eq("")
        )

        details["Customer Status"] = "REGISTERED"
        details.loc[
            missing_gln,
            "Customer Status",
        ] = "DUMMY"
        details.loc[
            missing_gln,
            "GLN",
        ] = (
            self.DUMMY_GLN
            if int(current_warehouse_id()) == 1
            else WAREHOUSE_DUMMY_GLN
        )

        details = self._enrich_with_master(
            details
        )

        # Dispatch Details is the only dispatch report. Every row represents
        # an original WMS dispatch line; there is no batch summary report.
        details_columns = [
            "GTIN",
            "Drug Name",
            "BN",
            "Expiry Date",
            "Active",
            "Quantity sent pending",
            "Quantity Receive Pending",
            "PackageSize",
            "Generic Item Number",
            "Trade Name",
            "Sales Order Number",
            "Order Line",
            "To Address",
            "Dispatch Date",
            "Dispatch Quantity Each",
            "Dispatch Quantity Pack",
            "Allocated To Be Dispatch",
            "Processing Status",
            "Previous Run Date",
            "Previous Quantity Each",
            "Current Quantity Each",
            "Quantity Difference",
            "GLN",
            "Customer Status",
            "Package Size Status",
            "Batch Master Status",
        ]

        report = self._ensure_output_columns(
            details,
            details_columns,
        )

        dispatch_upload = details.loc[
            details["Allocated To Be Dispatch"] > 0,
            [
                "GTIN",
                "Drug Name",
                "BN",
                "Expiry Date",
                "To Address",
                "GLN",
                "Customer Status",
                "Sales Order Number",
                "Allocated To Be Dispatch",
            ],
        ].copy()

        # A generated Dispatch CSV is only a submission candidate.  It must
        # not be treated as processed until a later SFDA report proves the
        # regulatory movement.  Persist only the quantities that were actually
        # allocated to the generated CSV files as pending confirmation.
        pending_source = details.loc[
            pd.to_numeric(
                details["Allocated To Be Dispatch"],
                errors="coerce",
            ).fillna(0).gt(0)
        ].copy()

        if pending_source.empty:
            pending_transactions = pd.DataFrame()
        else:
            allocated_pack = pd.to_numeric(
                pending_source["Allocated To Be Dispatch"],
                errors="coerce",
            ).fillna(0).clip(lower=0)
            package_size = pd.to_numeric(
                pending_source["PackageSize"],
                errors="coerce",
            ).fillna(0).clip(lower=0)
            pending_source["Current Quantity Pack"] = allocated_pack
            pending_source["Current Quantity Each"] = allocated_pack * package_size
            pending_transactions = self._transaction_rows(
                pending_source,
                "DISPATCH",
            )

        return {
            "report": report,
            "accept": pd.DataFrame(),
            "dispatch": dispatch_upload,
            # Legacy processed rows are intentionally not used for Dispatch
            # de-duplication anymore.  SFDA-confirmed rows are the source of
            # truth, exactly as with Accept.
            "processed_transactions": pd.DataFrame(),
            "pending_confirmation_transactions": pending_transactions,
        }

    def run(self) -> Dict[str, pd.DataFrame]:
        self._normalize_common()
        self._validate_common()

        if self.mode == "accept":
            return self._run_accept()

        return self._run_dispatch()
