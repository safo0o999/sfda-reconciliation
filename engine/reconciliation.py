from __future__ import annotations

from pathlib import Path
from typing import Any, Dict

import pandas as pd

from engine.normalizer import Normalizer
from engine.validator import Validator


class ReconciliationEngine:
    """Daily SFDA reconciliation engine.

    Accept:
        ASN/ASDT + SFDA

    Dispatch:
        Full Dispatch + SFDA

    Package size is always mapped in this order:
        SFDA[Drug Name] -> Pack Size[Trade Name] -> PackageSize

    After that, SFDA is matched to ASN / Full Dispatch by:
        BN + Expiry Date

    Batch Master is optional enrichment and never blocks daily processing.
    """

    MATCH_KEYS = ["BN", "Expiry Date"]
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

        # Kept only for backward compatibility with older function_app.py calls.
        # Inventory is intentionally not used by the current Dispatch logic.
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

    def _normalize_common(self) -> None:
        self.sfda = Normalizer.normalize_sfda(
            self.sfda
        )
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
        """Aggregate SFDA by exact batch and expiry, then attach PackageSize.

        Important:
        PackageSize is mapped from SFDA Drug Name only.
        It is never mapped from ASN/Dispatch Trade Name.
        """
        sfda_summary = (
            self.sfda.groupby(
                self.MATCH_KEYS,
                dropna=False,
            )
            .agg(
                GTIN=("GTIN", "first"),
                **{
                    "Drug Name": (
                        "Drug Name",
                        "first",
                    ),
                    "Quantity": (
                        "Quantity",
                        "sum",
                    ),
                    "Active": (
                        "Active",
                        "sum",
                    ),
                    "Quantity sent pending": (
                        "Quantity sent pending",
                        "sum",
                    ),
                    "Quantity Receive Pending": (
                        "Quantity Receive Pending",
                        "sum",
                    ),
                },
            )
            .reset_index()
        )

        sfda_summary = sfda_summary.merge(
            self._pack_lookup(),
            on="Drug Name",
            how="left",
            validate="many_to_one",
        )

        sfda_summary["PackageSize"] = pd.to_numeric(
            sfda_summary["PackageSize"],
            errors="coerce",
        )

        valid_package = (
            sfda_summary["PackageSize"].notna()
            & sfda_summary["PackageSize"].gt(0)
        )

        sfda_summary["Package Size Status"] = (
            valid_package.map(
                {
                    True: "Mapped",
                    False: "Missing",
                }
            )
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
                "Generic Item Number",
                "Total Received Qty",
                "Total Receive Qty",
                "Total Dispatched Qty",
                "Generic Exists in SFDA",
            ]
            if column in master.columns
        ]

        if not set(self.MATCH_KEYS).issubset(keep):
            return report

        master = (
            master[keep]
            .drop_duplicates(
                subset=self.MATCH_KEYS,
                keep="first",
            )
        )

        report = report.merge(
            master,
            on=self.MATCH_KEYS,
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

    def _run_accept(self) -> Dict[str, pd.DataFrame]:
        if self.asn.empty:
            raise ValueError(
                "ASN/ASDT file is required for Accept reconciliation."
            )

        self.asn = Normalizer.normalize_asn(
            self.asn
        )
        Validator.validate(
            self.asn,
            "ASN",
        )

        self._copy_first_available_column(
            self.asn,
            "Description",
            [
                "Description",
                "Item Description",
                "Generic Item Description",
            ],
        )
        self._copy_first_available_column(
            self.asn,
            "Supplier Code",
            [
                "Supplier Code",
                "Vendor Code",
                "Supplier Number",
            ],
        )
        self._copy_first_available_column(
            self.asn,
            "Item Family Group",
            [
                "Item Family Group",
                "Item Family",
                "Family Group",
            ],
        )

        receiving = (
            self.asn.groupby(
                self.MATCH_KEYS,
                dropna=False,
            )
            .agg(
                **{
                    "Generic Item Number": (
                        "Generic Item Number",
                        "first",
                    ),
                    "Trade Name": (
                        "Trade Name",
                        "first",
                    ),
                    "Received Quantity Each": (
                        "Received Quantity",
                        "sum",
                    ),
                    "Description": (
                        "Description",
                        self._join_unique,
                    ),
                    "Inbound Shipment": (
                        "Inbound Shipment",
                        self._join_unique,
                    ),
                    "Supplier Name": (
                        "Supplier Name",
                        self._join_unique,
                    ),
                    "Supplier Code": (
                        "Supplier Code",
                        self._join_unique,
                    ),
                    "Item Family Group": (
                        "Item Family Group",
                        self._join_unique,
                    ),
                }
            )
            .reset_index()
        )

        # Match WMS to the already-enriched SFDA table only by BN + Expiry Date.
        report = receiving.merge(
            self._sfda_summary(),
            on=self.MATCH_KEYS,
            how="inner",
            validate="one_to_one",
        )

        valid_package = (
            report["PackageSize"].notna()
            & report["PackageSize"].gt(0)
        )

        report["Received Quantity Pack"] = 0.0
        report.loc[
            valid_package,
            "Received Quantity Pack",
        ] = (
            pd.to_numeric(
                report.loc[
                    valid_package,
                    "Received Quantity Each",
                ],
                errors="coerce",
            ).fillna(0)
            / report.loc[
                valid_package,
                "PackageSize",
            ]
        )

        report["To Be Accept"] = 0

        report.loc[
            valid_package,
            "To Be Accept",
        ] = report.loc[
            valid_package
        ].apply(
            lambda row: min(
                self._safe_int(
                    row["Quantity Receive Pending"]
                ),
                self._safe_int(
                    row["Received Quantity Pack"]
                ),
            ),
            axis=1,
        )

        report = self._enrich_with_master(
            report
        )

        accept = report.loc[
            report["To Be Accept"] > 0,
            [
                "GTIN",
                "To Be Accept",
                "BN",
                "Expiry Date",
            ],
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

        details = report[
            [
                column
                for column in details_columns
                if column in report.columns
            ]
        ].copy()

        return {
            "report": details,
            "accept": accept,
            "dispatch": pd.DataFrame(),
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
        Validator.validate(
            self.dispatch,
            "DISPATCH",
        )

        dispatch_summary = (
            self.dispatch.groupby(
                self.MATCH_KEYS,
                dropna=False,
            )
            .agg(
                **{
                    "Generic Item Number": (
                        "Generic Item Number",
                        "first",
                    ),
                    "Trade Name": (
                        "Trade Name",
                        "first",
                    ),
                    "Total Full Dispatch Each": (
                        "Dispatched Quantity",
                        "sum",
                    ),
                }
            )
            .reset_index()
        )

        # PackageSize already comes from SFDA Drug Name -> Pack Size Trade Name.
        # Full Dispatch is matched only by BN + Expiry Date.
        report = dispatch_summary.merge(
            self._sfda_summary(),
            on=self.MATCH_KEYS,
            how="inner",
            validate="one_to_one",
        )

        valid_package = (
            report["PackageSize"].notna()
            & report["PackageSize"].gt(0)
        )

        report["Total Full Dispatch Pack"] = 0.0
        report.loc[
            valid_package,
            "Total Full Dispatch Pack",
        ] = (
            pd.to_numeric(
                report.loc[
                    valid_package,
                    "Total Full Dispatch Each",
                ],
                errors="coerce",
            ).fillna(0)
            / report.loc[
                valid_package,
                "PackageSize",
            ]
        )

        report["To Be Dispatched"] = 0

        report.loc[
            valid_package,
            "To Be Dispatched",
        ] = report.loc[
            valid_package
        ].apply(
            lambda row: min(
                self._safe_int(
                    row["Total Full Dispatch Pack"]
                ),
                self._safe_int(
                    row["Active"]
                ),
            ),
            axis=1,
        )

        report["Expected Active After Dispatch"] = (
            pd.to_numeric(
                report["Active"],
                errors="coerce",
            ).fillna(0)
            - pd.to_numeric(
                report["To Be Dispatched"],
                errors="coerce",
            ).fillna(0)
        ).clip(lower=0)

        report["Full Dispatch Status"] = (
            pd.to_numeric(
                report["Total Full Dispatch Each"],
                errors="coerce",
            ).fillna(0)
            .gt(0)
            .map(
                {
                    True: "Available",
                    False: "No Dispatch",
                }
            )
        )

        report = self._enrich_with_master(
            report
        )

        # Do not repeat Expiry Date in the merge payload because it is already
        # part of MATCH_KEYS. This prevents Expiry Date_x / Expiry Date_y.
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

        evidence = self.dispatch.copy().reset_index(
            drop=True
        )
        evidence["_Source Order"] = range(
            len(evidence)
        )

        evidence = evidence.merge(
            targets,
            on=self.MATCH_KEYS,
            how="inner",
            validate="many_to_one",
        )

        valid_evidence_package = (
            evidence["PackageSize"].notna()
            & evidence["PackageSize"].gt(0)
        )

        evidence["Evidence Packages"] = 0

        evidence.loc[
            valid_evidence_package,
            "Evidence Packages",
        ] = (
            pd.to_numeric(
                evidence.loc[
                    valid_evidence_package,
                    "Dispatched Quantity",
                ],
                errors="coerce",
            ).fillna(0)
            / evidence.loc[
                valid_evidence_package,
                "PackageSize",
            ]
        ).astype(int)

        evidence = evidence.sort_values(
            [
                "BN",
                "Expiry Date",
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
                    self._safe_int(
                        row["Evidence Packages"]
                    ),
                )

                if allocated <= 0:
                    continue

                item = row.to_dict()
                item[
                    "Allocated To Be Dispatch"
                ] = allocated
                allocated_rows.append(item)
                remaining -= allocated

        allocated = pd.DataFrame(
            allocated_rows
        )

        if not allocated.empty:
            gln = (
                self.gln[
                    ["To Address", "GLN"]
                ]
                .drop_duplicates(
                    subset=["To Address"],
                    keep="first",
                )
            )

            allocated = allocated.merge(
                gln,
                on="To Address",
                how="left",
            )

            missing_gln = (
                allocated["GLN"].isna()
                | allocated["GLN"]
                .astype(str)
                .str.strip()
                .eq("")
            )

            allocated["Customer Status"] = (
                "REGISTERED"
            )
            allocated.loc[
                missing_gln,
                "Customer Status",
            ] = "DUMMY"
            allocated.loc[
                missing_gln,
                "GLN",
            ] = self.DUMMY_GLN
        else:
            allocated = pd.DataFrame(
                columns=[
                    "GTIN",
                    "Drug Name",
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
