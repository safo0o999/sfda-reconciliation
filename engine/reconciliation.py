from __future__ import annotations

from pathlib import Path
from typing import Any, Dict

import pandas as pd

from engine.normalizer import Normalizer
from engine.validator import Validator


class ReconciliationEngine:
    """Daily reconciliation engine.

    Accept mode:
        latest ASN/ASDT + current SFDA

    Dispatch mode:
        latest Full Dispatch + refreshed SFDA

    Batch Master is optional enrichment. Its absence never blocks either mode.
    """

    MATCH_KEYS = ["BN", "Expiry Month Key"]
    DUMMY_GLN = "9999999999999"

    def __init__(
        self,
        mode: str,
        sfda_df: pd.DataFrame,
        asn_df: pd.DataFrame | None = None,
        dispatch_df: pd.DataFrame | None = None,
        inventory_df: pd.DataFrame | None = None,
        batch_master_df: pd.DataFrame | None = None,
    ) -> None:
        normalized_mode = str(mode or "").strip().lower()
        if normalized_mode not in {"accept", "dispatch"}:
            raise ValueError("mode must be either 'accept' or 'dispatch'.")

        self.mode = normalized_mode
        self.sfda = sfda_df.copy() if sfda_df is not None else pd.DataFrame()
        self.asn = asn_df.copy() if asn_df is not None else pd.DataFrame()
        self.dispatch = dispatch_df.copy() if dispatch_df is not None else pd.DataFrame()
        self.inventory = inventory_df.copy() if inventory_df is not None else pd.DataFrame()
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
        self.gln = pd.read_excel(
            config_dir / "gln.xlsx",
            engine="openpyxl",
            dtype=object,
        )

    @staticmethod
    def _month_key(series: pd.Series) -> pd.Series:
        return Normalizer.date(series).dt.strftime("%Y-%m").fillna("")

    @staticmethod
    def _safe_int(value: Any) -> int:
        number = pd.to_numeric(pd.Series([value]), errors="coerce").fillna(0).iloc[0]
        return max(0, int(number))

    @staticmethod
    def _join_unique(values: pd.Series) -> str:
        """Join unique non-empty values without losing multi-shipment details."""
        unique_values = []
        seen = set()

        for value in values:
            if pd.isna(value):
                continue

            text = str(value).strip()
            if not text or text.lower() == "nan" or text in seen:
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
        """Create a normalized target column from the first available ASN alias."""
        for candidate in candidates:
            if candidate in frame.columns:
                frame[target] = Normalizer.text(frame[candidate])
                return

        frame[target] = ""

    def _normalize_common(self) -> None:
        self.sfda = Normalizer.normalize_sfda(self.sfda)
        self.sfda["Expiry Month Key"] = self._month_key(self.sfda["Expiry Date"])
        self.packsize = Normalizer.normalize_packsize(self.packsize)
        self.gln = Normalizer.normalize_gln(self.gln)

        if not self.batch_master.empty:
            if "BN" in self.batch_master.columns:
                self.batch_master["BN"] = Normalizer.text(self.batch_master["BN"])
            if "Expiry Month Key" not in self.batch_master.columns:
                if "Expiry Date" in self.batch_master.columns:
                    self.batch_master["Expiry Month Key"] = self._month_key(
                        self.batch_master["Expiry Date"]
                    )
                else:
                    self.batch_master["Expiry Month Key"] = ""

    def _validate_common(self) -> None:
        Validator.validate(self.sfda, "SFDA")
        Validator.validate(self.packsize, "PACKSIZE")

    def _pack_lookup(self) -> pd.DataFrame:
        lookup = self.packsize[["Trade Name", "PackageSize"]].copy()
        lookup["Trade Name"] = Normalizer.text(lookup["Trade Name"])
        lookup["PackageSize"] = pd.to_numeric(
            lookup["PackageSize"], errors="coerce"
        )
        lookup = lookup[
            lookup["Trade Name"].ne("")
            & lookup["PackageSize"].notna()
            & lookup["PackageSize"].gt(0)
        ]
        return lookup.drop_duplicates("Trade Name", keep="first")

    def _sfda_summary(self) -> pd.DataFrame:
        return (
            self.sfda.groupby(self.MATCH_KEYS, dropna=False)
            .agg(
                GTIN=("GTIN", "first"),
                **{
                    "Drug Name": ("Drug Name", "first"),
                    "Expiry Date": ("Expiry Date", "first"),
                    "Quantity": ("Quantity", "sum"),
                    "Active": ("Active", "sum"),
                    "Quantity sent pending": ("Quantity sent pending", "sum"),
                    "Quantity Receive Pending": (
                        "Quantity Receive Pending",
                        "sum",
                    ),
                },
            )
            .reset_index()
        )

    def _enrich_with_master(self, report: pd.DataFrame) -> pd.DataFrame:
        report = report.copy()
        report["Batch Master Status"] = "Not Available"
        if self.batch_master.empty:
            return report

        master = self.batch_master.copy()
        keep = [
            column
            for column in [
                "BN",
                "Expiry Month Key",
                "Generic Item Number",
                "Total Receive Qty",
                "Total Dispatched Qty",
                "Generic Exists in SFDA",
            ]
            if column in master.columns
        ]
        if not {"BN", "Expiry Month Key"}.issubset(keep):
            return report

        master = master[keep].drop_duplicates(self.MATCH_KEYS, keep="first")
        report = report.merge(
            master,
            on=self.MATCH_KEYS,
            how="left",
            suffixes=("", " Master"),
        )
        matched = report.get("Generic Exists in SFDA").notna() if "Generic Exists in SFDA" in report.columns else report.get("Total Receive Qty").notna()
        report["Batch Master Status"] = matched.map({True: "Matched", False: "Not Found"})
        return report

    def _run_accept(self) -> Dict[str, pd.DataFrame]:
        if self.asn.empty:
            raise ValueError("ASN/ASDT file is required for Accept reconciliation.")

        self.asn = Normalizer.normalize_asn(self.asn)
        Validator.validate(self.asn, "ASN")
        self.asn["Expiry Month Key"] = self._month_key(self.asn["Expiry Date"])

        # Keep the operational receiving fields required in Accept Details.
        # Exact source names are preferred, with common aliases accepted.
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

        receiving = (
            self.asn.groupby(self.MATCH_KEYS, dropna=False)
            .agg(
                **{
                    "Generic Item Number": ("Generic Item Number", "first"),
                    "Trade Name": ("Trade Name", "first"),
                    "Received Quantity Each": ("Received Quantity", "sum"),
                    "ASN Expiry Date": ("Expiry Date", "first"),
                    "Description": ("Description", self._join_unique),
                    "Inbound Shipment": ("Inbound Shipment", self._join_unique),
                    "Supplier Name": ("Supplier Name", self._join_unique),
                    "Supplier Code": ("Supplier Code", self._join_unique),
                    "Item Family Group": ("Item Family Group", self._join_unique),
                }
            )
            .reset_index()
        )

        report = receiving.merge(
            self._sfda_summary(),
            on=self.MATCH_KEYS,
            how="inner",
            validate="one_to_one",
        )
        report = report.merge(
            self._pack_lookup(),
            on="Trade Name",
            how="left",
            validate="many_to_one",
        )
        report["PackageSize"] = pd.to_numeric(report["PackageSize"], errors="coerce")
        report["Package Size Status"] = report["PackageSize"].apply(
            lambda value: "Mapped" if pd.notna(value) and float(value) > 0 else "Missing"
        )
        report["PackageSize"] = report["PackageSize"].fillna(1)
        report.loc[report["PackageSize"] <= 0, "PackageSize"] = 1

        report["Received Quantity Pack"] = (
            pd.to_numeric(report["Received Quantity Each"], errors="coerce").fillna(0)
            / report["PackageSize"]
        )
        report["To Be Accept"] = report.apply(
            lambda row: min(
                self._safe_int(row["Quantity Receive Pending"]),
                self._safe_int(row["Received Quantity Pack"]),
            ),
            axis=1,
        )
        report = self._enrich_with_master(report)

        accept = report.loc[
            report["To Be Accept"] > 0,
            ["GTIN", "To Be Accept", "BN", "Expiry Date"],
        ].copy()

        details_columns = [
            "GTIN",
            "Drug Name",
            "BN",
            "Expiry Date",
            "PackageSize",
            "Active",
            "Quantity sent pending",
            "Quantity Receive Pending",
            "Generic Item Number",
            "Received Quantity Each",
            "Received Quantity Pack",
            "Description",
            "Inbound Shipment",
            "Supplier Name",
            "Supplier Code",
            "Item Family Group",
            "To Be Accept",
            "Package Size Status",
            "Batch Master Status",
        ]
        details = report[[column for column in details_columns if column in report.columns]].copy()
        return {"report": details, "accept": accept, "dispatch": pd.DataFrame()}

    def _run_dispatch(self) -> Dict[str, pd.DataFrame]:
        if self.dispatch.empty:
            raise ValueError("Full Dispatch file is required for Dispatch reconciliation.")

        self.dispatch = Normalizer.normalize_dispatch(self.dispatch)
        Validator.validate(self.dispatch, "DISPATCH")
        self.dispatch["Expiry Month Key"] = self._month_key(
            self.dispatch["Expiry Date"]
        )

        # Aggregate all physical dispatch movements for every SFDA batch.
        # Full Dispatch quantities are stored as Each and converted to Pack
        # after Package Size is mapped from Trade Name.
        dispatch_summary = (
            self.dispatch.groupby(self.MATCH_KEYS, dropna=False)
            .agg(
                **{
                    "Generic Item Number": (
                        "Generic Item Number",
                        "first",
                    ),
                    "Trade Name": ("Trade Name", "first"),
                    "Total Full Dispatch Each": (
                        "Dispatched Quantity",
                        "sum",
                    ),
                }
            )
            .reset_index()
        )

        # Start from SFDA batches and retain only batches supported by an
        # actual movement in the latest Full Dispatch report.
        report = self._sfda_summary().merge(
            dispatch_summary,
            on=self.MATCH_KEYS,
            how="inner",
            validate="one_to_one",
        )
        report = report.merge(
            self._pack_lookup(),
            on="Trade Name",
            how="left",
            validate="many_to_one",
        )

        report["PackageSize"] = pd.to_numeric(
            report["PackageSize"],
            errors="coerce",
        )
        report["Package Size Status"] = report["PackageSize"].apply(
            lambda value: (
                "Mapped"
                if pd.notna(value) and float(value) > 0
                else "Missing"
            )
        )
        report["PackageSize"] = report["PackageSize"].fillna(1)
        report.loc[report["PackageSize"] <= 0, "PackageSize"] = 1

        report["Total Full Dispatch Each"] = pd.to_numeric(
            report["Total Full Dispatch Each"],
            errors="coerce",
        ).fillna(0)
        report["Total Full Dispatch Pack"] = (
            report["Total Full Dispatch Each"]
            / report["PackageSize"]
        )

        # Never send more than the current Active quantity in SFDA.
        # Both values are floored to whole packs before comparison.
        report["To Be Dispatched"] = report.apply(
            lambda row: min(
                self._safe_int(row["Total Full Dispatch Pack"]),
                self._safe_int(row["Active"]),
            ),
            axis=1,
        )
        report["Expected Active After Dispatch"] = report.apply(
            lambda row: max(
                0,
                self._safe_int(row["Active"])
                - self._safe_int(row["To Be Dispatched"]),
            ),
            axis=1,
        )
        report["Full Dispatch Status"] = report.apply(
            lambda row: (
                "No Dispatch Quantity"
                if self._safe_int(row["Total Full Dispatch Pack"]) <= 0
                else (
                    "Limited by SFDA Active"
                    if self._safe_int(row["Total Full Dispatch Pack"])
                    > self._safe_int(row["Active"])
                    else "Full Dispatch Used"
                )
            ),
            axis=1,
        )
        report = self._enrich_with_master(report)

        targets = report.loc[
            report["To Be Dispatched"] > 0,
            self.MATCH_KEYS
            + [
                "GTIN",
                "Drug Name",
                "PackageSize",
                "To Be Dispatched",
            ],
        ].copy()

        # Allocate the approved quantity to the real customers recorded in
        # Full Dispatch, oldest movement first, stopping at To Be Dispatched.
        evidence = self.dispatch.copy().reset_index(drop=True)
        evidence["_Source Order"] = range(len(evidence))
        evidence = evidence.merge(
            targets,
            on=self.MATCH_KEYS,
            how="inner",
            validate="many_to_one",
        )
        evidence["Evidence Packages"] = (
            pd.to_numeric(
                evidence["Dispatched Quantity"],
                errors="coerce",
            ).fillna(0)
            / evidence["PackageSize"]
        ).apply(self._safe_int)
        evidence = evidence.sort_values(
            [
                "BN",
                "Expiry Month Key",
                "Dispatch Date",
                "_Source Order",
            ],
            kind="stable",
        )

        allocated_rows = []
        for _, group in evidence.groupby(
            self.MATCH_KEYS,
            sort=False,
            dropna=False,
        ):
            remaining = self._safe_int(
                group["To Be Dispatched"].iloc[0]
            )

            for _, row in group.iterrows():
                if remaining <= 0:
                    break

                allocated = min(
                    remaining,
                    self._safe_int(row["Evidence Packages"]),
                )
                if allocated <= 0:
                    continue

                item = row.to_dict()
                item["Allocated To Be Dispatch"] = allocated
                allocated_rows.append(item)
                remaining -= allocated

        allocated = pd.DataFrame(allocated_rows)
        if not allocated.empty:
            # Consolidate repeated movements for the same batch/customer into
            # one Upload Dispatch row before GLN assignment.
            group_columns = self.MATCH_KEYS + [
                "GTIN",
                "Drug Name",
                "Expiry Date",
                "To Address",
            ]
            allocated = (
                allocated.groupby(
                    group_columns,
                    dropna=False,
                    as_index=False,
                )
                .agg(
                    **{
                        "Allocated To Be Dispatch": (
                            "Allocated To Be Dispatch",
                            "sum",
                        ),
                        "Dispatch Date": (
                            "Dispatch Date",
                            "min",
                        ),
                    }
                )
            )

            gln = self.gln[["To Address", "GLN"]].drop_duplicates(
                "To Address",
                keep="first",
            )
            allocated = allocated.merge(
                gln,
                on="To Address",
                how="left",
            )
            missing = (
                allocated["GLN"].isna()
                | allocated["GLN"].astype(str).str.strip().eq("")
            )
            allocated["Customer Status"] = "REGISTERED"
            allocated.loc[missing, "Customer Status"] = "DUMMY"
            allocated.loc[missing, "GLN"] = self.DUMMY_GLN
        else:
            allocated = pd.DataFrame(
                columns=[
                    "GTIN",
                    "BN",
                    "Expiry Date",
                    "To Address",
                    "GLN",
                    "Customer Status",
                    "Allocated To Be Dispatch",
                ]
            )

        details_columns = [
            "GTIN",
            "Drug Name",
            "BN",
            "Expiry Date",
            "PackageSize",
            "Active",
            "Quantity sent pending",
            "Quantity Receive Pending",
            "Generic Item Number",
            "Total Full Dispatch Each",
            "Total Full Dispatch Pack",
            "To Be Dispatched",
            "Expected Active After Dispatch",
            "Full Dispatch Status",
            "Package Size Status",
            "Batch Master Status",
        ]
        details = report[
            [
                column
                for column in details_columns
                if column in report.columns
            ]
        ].copy()

        return {
            "report": details,
            "accept": pd.DataFrame(),
            "dispatch": allocated,
        }

    def run(self) -> Dict[str, pd.DataFrame]:
        self._normalize_common()
        self._validate_common()
        if self.mode == "accept":
            return self._run_accept()
        return self._run_dispatch()
