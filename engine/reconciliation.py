from __future__ import annotations

import hashlib
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
        result["Transaction Key"] = result.apply(
            lambda row: self._transaction_key(transaction_type, row), axis=1
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

        self.asn = self._apply_processing_status(
            self.asn, "ACCEPT", "Received Quantity"
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
                        "Effective Quantity Each",
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
                    "Processing Status": (
                        "Processing Status",
                        self._join_unique,
                    ),
                    "Previous Run Date": (
                        "Previous Run Date",
                        "max",
                    ),
                    "Previous Quantity Each": (
                        "Previous Quantity Each",
                        "sum",
                    ),
                    "Current Quantity Each": (
                        "Current Quantity Each",
                        "sum",
                    ),
                    "Quantity Difference": (
                        "Quantity Difference",
                        "sum",
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
            "Processing Status",
            "Previous Run Date",
            "Previous Quantity Each",
            "Current Quantity Each",
            "Quantity Difference",
            "Package Size Status",
            "Batch Master Status",
        ]

        details = self._ensure_output_columns(
            report,
            details_columns,
        )

        accept_transactions = self.asn.copy()
        accept_transactions["Current Quantity Pack"] = 0.0

        return {
            "report": details,
            "accept": accept,
            "dispatch": pd.DataFrame(),
            "processed_transactions": self._transaction_rows(accept_transactions, "ACCEPT"),
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
        self.dispatch = self._apply_processing_status(
            self.dispatch, "DISPATCH", "Dispatched Quantity"
        )

        # SFDA receives PackageSize first through:
        # SFDA[Drug Name] -> Pack Size[Trade Name].
        # Full Dispatch is then matched only by BN + Expiry Date.
        sfda_batches = self._sfda_summary()

        details = self.dispatch.merge(
            sfda_batches,
            on=self.MATCH_KEYS,
            how="inner",
            validate="many_to_one",
        )

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
            self.MATCH_KEYS,
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
        ] = self.DUMMY_GLN

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

        return {
            "report": report,
            "accept": pd.DataFrame(),
            "dispatch": dispatch_upload,
            "processed_transactions": self._transaction_rows(details, "DISPATCH"),
        }

    def run(self) -> Dict[str, pd.DataFrame]:
        self._normalize_common()
        self._validate_common()

        if self.mode == "accept":
            return self._run_accept()

        return self._run_dispatch()
