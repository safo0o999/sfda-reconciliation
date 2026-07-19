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
        latest Full Dispatch + latest Inventory + refreshed SFDA

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

        receiving = (
            self.asn.groupby(self.MATCH_KEYS, dropna=False)
            .agg(
                **{
                    "Generic Item Number": ("Generic Item Number", "first"),
                    "Trade Name": ("Trade Name", "first"),
                    "Received Quantity Each": ("Received Quantity", "sum"),
                    "ASN Expiry Date": ("Expiry Date", "first"),
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
            "BN",
            "Expiry Date",
            "GTIN",
            "Drug Name",
            "Generic Item Number",
            "Trade Name",
            "PackageSize",
            "Received Quantity Each",
            "Received Quantity Pack",
            "Quantity Receive Pending",
            "Active",
            "Quantity sent pending",
            "To Be Accept",
            "Package Size Status",
            "Batch Master Status",
        ]
        details = report[[column for column in details_columns if column in report.columns]].copy()
        return {"report": details, "accept": accept, "dispatch": pd.DataFrame()}

    def _run_dispatch(self) -> Dict[str, pd.DataFrame]:
        if self.dispatch.empty:
            raise ValueError("Full Dispatch file is required for Dispatch reconciliation.")
        if self.inventory.empty:
            raise ValueError("Inventory file is required for Dispatch reconciliation.")

        self.dispatch = Normalizer.normalize_dispatch(self.dispatch)
        self.inventory = Normalizer.normalize_inventory(self.inventory)
        Validator.validate(self.dispatch, "DISPATCH")
        Validator.validate(self.inventory, "INVENTORY")
        self.dispatch["Expiry Month Key"] = self._month_key(self.dispatch["Expiry Date"])
        self.inventory["Expiry Month Key"] = self._month_key(self.inventory["Expiry Date"])

        inventory = (
            self.inventory.groupby(self.MATCH_KEYS, dropna=False)
            .agg(
                **{
                    "Generic Item Number": ("Generic Item Number", "first"),
                    "Trade Name": ("Trade Name", "first"),
                    "Inventory Available Each": ("Available Quantity", "sum"),
                }
            )
            .reset_index()
        )
        report = inventory.merge(
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
        report["Inventory Available Pack"] = (
            pd.to_numeric(report["Inventory Available Each"], errors="coerce").fillna(0)
            / report["PackageSize"]
        )
        report["To Be Dispatched"] = report.apply(
            lambda row: max(
                0,
                self._safe_int(row["Active"])
                - self._safe_int(row["Inventory Available Pack"]),
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
                "Expiry Date",
                "PackageSize",
                "To Be Dispatched",
            ],
        ].copy()

        evidence = self.dispatch.copy().reset_index(drop=True)
        evidence["_Source Order"] = range(len(evidence))
        evidence = evidence.merge(
            targets,
            on=self.MATCH_KEYS,
            how="inner",
            validate="many_to_one",
        )
        evidence["Evidence Packages"] = (
            pd.to_numeric(evidence["Dispatched Quantity"], errors="coerce").fillna(0)
            / evidence["PackageSize"]
        ).astype(int)
        evidence = evidence.sort_values(
            ["BN", "Expiry Month Key", "Dispatch Date", "_Source Order"],
            kind="stable",
        )

        allocated_rows = []
        for _, group in evidence.groupby(self.MATCH_KEYS, sort=False, dropna=False):
            remaining = self._safe_int(group["To Be Dispatched"].iloc[0])
            for _, row in group.iterrows():
                if remaining <= 0:
                    break
                allocated = min(remaining, self._safe_int(row["Evidence Packages"]))
                if allocated <= 0:
                    continue
                item = row.to_dict()
                item["Allocated To Be Dispatch"] = allocated
                allocated_rows.append(item)
                remaining -= allocated

        allocated = pd.DataFrame(allocated_rows)
        if not allocated.empty:
            gln = self.gln[["To Address", "GLN"]].drop_duplicates("To Address", keep="first")
            allocated = allocated.merge(gln, on="To Address", how="left")
            missing = allocated["GLN"].isna() | allocated["GLN"].astype(str).str.strip().eq("")
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
            "BN",
            "Expiry Date",
            "GTIN",
            "Drug Name",
            "Generic Item Number",
            "Trade Name",
            "PackageSize",
            "Inventory Available Each",
            "Inventory Available Pack",
            "Active",
            "Quantity sent pending",
            "Quantity Receive Pending",
            "To Be Dispatched",
            "Package Size Status",
            "Batch Master Status",
        ]
        details = report[[column for column in details_columns if column in report.columns]].copy()
        return {"report": details, "accept": pd.DataFrame(), "dispatch": allocated}

    def run(self) -> Dict[str, pd.DataFrame]:
        self._normalize_common()
        self._validate_common()
        if self.mode == "accept":
            return self._run_accept()
        return self._run_dispatch()
