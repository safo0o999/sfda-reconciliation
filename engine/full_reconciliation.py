from __future__ import annotations

import hashlib
from pathlib import Path
from typing import Any, Dict, List

import pandas as pd

from engine.normalizer import Normalizer


class FullReconciliationEngine:
    """
    Build the historical Batch Master without generating SFDA upload CSVs.

    Matching rule:
        BN + Expiry Year/Month

    The exact SFDA and WMS expiry dates remain available for audit.
    """

    KEYS = ["BN", "Expiry Month Key"]

    def __init__(
        self,
        asn_df: pd.DataFrame,
        dispatch_df: pd.DataFrame,
        sfda_df: pd.DataFrame,
    ) -> None:
        self.asn = asn_df.copy()
        self.dispatch = dispatch_df.copy()
        self.sfda = sfda_df.copy()

        config_path = (
            Path(__file__).resolve().parent.parent
            / "config"
        )
        packsize_path = config_path / "pack_size.xlsx"

        if not packsize_path.exists():
            raise FileNotFoundError(
                "config/pack_size.xlsx was not found."
            )

        self.packsize = pd.read_excel(
            packsize_path,
            engine="openpyxl",
            dtype=object,
        )

    @staticmethod
    def _month_key(series: pd.Series) -> pd.Series:
        dates = Normalizer.date(series)
        return dates.dt.strftime("%Y-%m").fillna("")

    @staticmethod
    def _clean_text(series: pd.Series) -> pd.Series:
        return (
            series.fillna("")
            .astype(str)
            .str.strip()
        )

    @staticmethod
    def _first_non_blank(series: pd.Series) -> str:
        for value in series:
            text = str(value or "").strip()
            if text and text.lower() != "nan":
                return text
        return ""

    @staticmethod
    def _first_valid_date(series: pd.Series):
        values = pd.to_datetime(
            series,
            errors="coerce",
        ).dropna()

        if values.empty:
            return pd.NaT

        return values.min()

    @staticmethod
    def _last_valid_date(series: pd.Series):
        values = pd.to_datetime(
            series,
            errors="coerce",
        ).dropna()

        if values.empty:
            return pd.NaT

        return values.max()

    @staticmethod
    def _event_key(values: List[Any]) -> str:
        raw = "|".join(
            "" if value is None else str(value)
            for value in values
        )
        return hashlib.sha256(
            raw.encode("utf-8")
        ).hexdigest()

    def _prepare_packsize(self) -> pd.DataFrame:
        packsize = Normalizer.normalize_packsize(
            self.packsize
        )

        packsize = (
            packsize[
                ["Trade Name", "PackageSize"]
            ]
            .drop_duplicates(
                subset=["Trade Name"],
                keep="first",
            )
            .copy()
        )

        packsize["PackageSize"] = (
            pd.to_numeric(
                packsize["PackageSize"],
                errors="coerce",
            )
            .fillna(1)
        )

        packsize.loc[
            packsize["PackageSize"] <= 0,
            "PackageSize",
        ] = 1

        return packsize

    def _normalize(self) -> None:
        self.asn = Normalizer.normalize_asn(
            self.asn
        )
        self.dispatch = Normalizer.normalize_dispatch(
            self.dispatch
        )
        self.sfda = Normalizer.normalize_sfda(
            self.sfda
        )

        self.asn["Expiry Month Key"] = (
            self._month_key(
                self.asn["Expiry Date"]
            )
        )
        self.dispatch["Expiry Month Key"] = (
            self._month_key(
                self.dispatch["Expiry Date"]
            )
        )
        self.sfda["Expiry Month Key"] = (
            self._month_key(
                self.sfda["Expiry Date"]
            )
        )

        for dataframe, label in [
            (self.asn, "ASN"),
            (self.dispatch, "DISPATCH"),
            (self.sfda, "SFDA"),
        ]:
            invalid = dataframe[
                (dataframe["BN"] == "")
                | (
                    dataframe[
                        "Expiry Month Key"
                    ] == ""
                )
            ]

            if not invalid.empty:
                dataframe.drop(
                    index=invalid.index,
                    inplace=True,
                )

            if dataframe.empty:
                raise ValueError(
                    f"{label} contains no valid "
                    "BN + expiry-month records."
                )

    def _receipt_events(
        self,
        package_lookup: pd.DataFrame,
    ) -> pd.DataFrame:
        receipt = self.asn.copy()

        receipt["Source File"] = (
            self._clean_text(
                receipt.get(
                    "_Source File",
                    pd.Series(
                        [""] * len(receipt),
                        index=receipt.index,
                    ),
                )
            )
        )

        receipt["Event Date"] = pd.to_datetime(
            receipt["Received Date"],
            errors="coerce",
        ).dt.normalize()

        receipt["Received Quantity"] = (
            pd.to_numeric(
                receipt["Received Quantity"],
                errors="coerce",
            )
            .fillna(0)
        )

        receipt = receipt[
            receipt["Received Quantity"] > 0
        ].copy()

        receipt = receipt.merge(
            package_lookup,
            on="Trade Name",
            how="left",
        )

        receipt["PackageSize"] = (
            pd.to_numeric(
                receipt["PackageSize"],
                errors="coerce",
            )
            .fillna(1)
        )
        receipt.loc[
            receipt["PackageSize"] <= 0,
            "PackageSize",
        ] = 1

        group_columns = [
            "BN",
            "Expiry Month Key",
            "Event Date",
            "Source File",
            "Supplier Name",
            "Supplier Code",
            "PO Number",
            "Invoice Number",
            "Inbound Shipment",
            "Trade Name",
            "Generic Item Number",
            "Trade Item",
            "PackageSize",
        ]

        grouped = (
            receipt.groupby(
                group_columns,
                as_index=False,
                dropna=False,
            )
            .agg({
                "Received Quantity": "sum",
                "Expiry Date": "first",
            })
        )

        grouped["Quantity Packages"] = (
            grouped["Received Quantity"]
            / grouped["PackageSize"]
        )

        grouped["Event Type"] = "RECEIPT"

        grouped["Event Key"] = grouped.apply(
            lambda row: self._event_key([
                "RECEIPT",
                row["BN"],
                row["Expiry Month Key"],
                row["Event Date"],
                row["Source File"],
                row["Supplier Name"],
                row["PO Number"],
                row["Invoice Number"],
                row["Inbound Shipment"],
                row["Received Quantity"],
            ]),
            axis=1,
        )

        return grouped

    def _dispatch_events(
        self,
        package_lookup: pd.DataFrame,
    ) -> pd.DataFrame:
        dispatch = self.dispatch.copy()

        dispatch["Source File"] = (
            self._clean_text(
                dispatch.get(
                    "_Source File",
                    pd.Series(
                        [""] * len(dispatch),
                        index=dispatch.index,
                    ),
                )
            )
        )

        dispatch["Event Date"] = pd.to_datetime(
            dispatch["Dispatch Date"],
            errors="coerce",
        ).dt.normalize()

        dispatch["Dispatched Quantity"] = (
            pd.to_numeric(
                dispatch["Dispatched Quantity"],
                errors="coerce",
            )
            .fillna(0)
        )

        dispatch = dispatch[
            dispatch["Dispatched Quantity"] > 0
        ].copy()

        dispatch = dispatch.merge(
            package_lookup,
            on="Trade Name",
            how="left",
        )

        dispatch["PackageSize"] = (
            pd.to_numeric(
                dispatch["PackageSize"],
                errors="coerce",
            )
            .fillna(1)
        )
        dispatch.loc[
            dispatch["PackageSize"] <= 0,
            "PackageSize",
        ] = 1

        group_columns = [
            "BN",
            "Expiry Month Key",
            "Event Date",
            "Source File",
            "To Address",
            "Sales Order Number",
            "Order Line",
            "Trade Name",
            "Generic Item Number",
            "Trade Item Number",
            "PackageSize",
        ]

        grouped = (
            dispatch.groupby(
                group_columns,
                as_index=False,
                dropna=False,
            )
            .agg({
                "Dispatched Quantity": "sum",
                "Expiry Date": "first",
            })
        )

        grouped["Quantity Packages"] = (
            grouped["Dispatched Quantity"]
            / grouped["PackageSize"]
        )

        grouped["Event Type"] = "DISPATCH"

        grouped["Event Key"] = grouped.apply(
            lambda row: self._event_key([
                "DISPATCH",
                row["BN"],
                row["Expiry Month Key"],
                row["Event Date"],
                row["Source File"],
                row["To Address"],
                row["Sales Order Number"],
                row["Order Line"],
                row["Dispatched Quantity"],
            ]),
            axis=1,
        )

        return grouped

    def _sfda_summary(
        self,
        package_lookup: pd.DataFrame,
    ) -> pd.DataFrame:
        sfda = self.sfda.copy()

        sfda = sfda.merge(
            package_lookup,
            left_on="Drug Name",
            right_on="Trade Name",
            how="left",
        )

        sfda["PackageSize"] = (
            pd.to_numeric(
                sfda["PackageSize"],
                errors="coerce",
            )
            .fillna(1)
        )
        sfda.loc[
            sfda["PackageSize"] <= 0,
            "PackageSize",
        ] = 1

        for column in [
            "Quantity",
            "Active",
            "Quantity Receive Pending",
            "Quantity sent pending",
        ]:
            sfda[column] = pd.to_numeric(
                sfda[column],
                errors="coerce",
            ).fillna(0)

        summary = (
            sfda.groupby(
                self.KEYS,
                as_index=False,
                dropna=False,
            )
            .agg({
                "GTIN":
                    self._first_non_blank,
                "Drug Name":
                    self._first_non_blank,
                "Expiry Date":
                    "first",
                "PackageSize":
                    "first",
                "Quantity":
                    "sum",
                "Active":
                    "sum",
                "Quantity Receive Pending":
                    "sum",
                "Quantity sent pending":
                    "sum",
            })
        )

        return summary.rename(
            columns={
                "Expiry Date":
                    "SFDA Expiry Date",
                "Quantity":
                    "SFDA Quantity",
                "Active":
                    "SFDA Active",
                "Quantity Receive Pending":
                    "SFDA Receive Pending",
                "Quantity sent pending":
                    "SFDA Send Pending",
            }
        )

    def prepare_incremental(self) -> Dict[str, Any]:
        """
        Normalize the uploaded files and return only the candidate events
        and the latest SFDA snapshot. Database deduplication happens later.
        """
        self._normalize()
        package_lookup = self._prepare_packsize()

        receipt_events = self._receipt_events(
            package_lookup
        )
        dispatch_events = self._dispatch_events(
            package_lookup
        )
        sfda_summary = self._sfda_summary(
            package_lookup
        )

        return {
            "receipt_events": receipt_events,
            "dispatch_events": dispatch_events,
            "sfda_summary": sfda_summary,
            "receipt_records":
                self._records(receipt_events),
            "dispatch_records":
                self._records(dispatch_events),
        }

    def build_master_from_summaries(
        self,
        receipt_summary: pd.DataFrame,
        dispatch_summary: pd.DataFrame,
        sfda_summary: pd.DataFrame,
    ) -> pd.DataFrame:
        """
        Build the current Batch Master from SQL historical aggregates plus
        the latest SFDA snapshot.
        """
        receipt_summary = receipt_summary.copy()
        dispatch_summary = dispatch_summary.copy()
        sfda_summary = sfda_summary.copy()

        all_keys = pd.concat(
            [
                receipt_summary[self.KEYS]
                if not receipt_summary.empty
                else pd.DataFrame(columns=self.KEYS),
                dispatch_summary[self.KEYS]
                if not dispatch_summary.empty
                else pd.DataFrame(columns=self.KEYS),
                sfda_summary[self.KEYS]
                if not sfda_summary.empty
                else pd.DataFrame(columns=self.KEYS),
            ],
            ignore_index=True,
        ).drop_duplicates()

        master = all_keys.copy()

        if not receipt_summary.empty:
            master = master.merge(
                receipt_summary,
                on=self.KEYS,
                how="left",
            )

        if not dispatch_summary.empty:
            master = master.merge(
                dispatch_summary,
                on=self.KEYS,
                how="left",
            )

        if not sfda_summary.empty:
            master = master.merge(
                sfda_summary,
                on=self.KEYS,
                how="left",
            )

        defaults = {
            "Total Received Units": 0,
            "Total Received Packages": 0,
            "Total Dispatched Units": 0,
            "Total Dispatched Packages": 0,
            "SFDA Quantity": 0,
            "SFDA Active": 0,
            "SFDA Receive Pending": 0,
            "SFDA Send Pending": 0,
            "Receipt PackageSize": 1,
            "Dispatch PackageSize": 1,
            "PackageSize": 1,
            "GTIN": "",
            "Drug Name": "",
            "Receipt Trade Name": "",
            "Dispatch Trade Name": "",
            "Generic Item Number": "",
            "Dispatch Generic Item Number": "",
            "Receipt Trade Item Number": "",
            "Dispatch Trade Item Number": "",
        }

        for column, default in defaults.items():
            if column not in master.columns:
                master[column] = default

        numeric_columns = [
            "Total Received Units",
            "Total Received Packages",
            "Total Dispatched Units",
            "Total Dispatched Packages",
            "SFDA Quantity",
            "SFDA Active",
            "SFDA Receive Pending",
            "SFDA Send Pending",
        ]

        for column in numeric_columns:
            master[column] = pd.to_numeric(
                master[column],
                errors="coerce",
            ).fillna(0)

        master["PackageSize"] = (
            pd.to_numeric(
                master["PackageSize"],
                errors="coerce",
            )
            .fillna(
                pd.to_numeric(
                    master["Receipt PackageSize"],
                    errors="coerce",
                )
            )
            .fillna(
                pd.to_numeric(
                    master["Dispatch PackageSize"],
                    errors="coerce",
                )
            )
            .fillna(1)
        )
        master.loc[
            master["PackageSize"] <= 0,
            "PackageSize",
        ] = 1

        master["GTIN"] = master["GTIN"].fillna("").astype(str)
        master["Drug Name"] = (
            master["Drug Name"]
            .fillna(master["Receipt Trade Name"])
            .fillna(master["Dispatch Trade Name"])
            .fillna("")
        )
        master["Generic Item Number"] = (
            master["Generic Item Number"]
            .fillna(master["Dispatch Generic Item Number"])
            .fillna("")
        )
        master["Trade Item Number"] = (
            master["Receipt Trade Item Number"]
            .fillna(master["Dispatch Trade Item Number"])
            .fillna("")
        )

        master["Net Physical Packages"] = (
            master["Total Received Packages"]
            - master["Total Dispatched Packages"]
        )
        master["Physical vs SFDA Active Variance"] = (
            master["Net Physical Packages"]
            - master["SFDA Active"]
        )
        master["Historical Receipt Uncovered"] = (
            master["SFDA Receive Pending"]
            - master["Total Received Packages"]
        ).clip(lower=0)
        master["Historical Dispatch Uncovered"] = (
            master["SFDA Active"]
            - master["Total Dispatched Packages"]
        ).clip(lower=0)

        master["Master Status"] = "BALANCED"
        master.loc[
            master["GTIN"] == "",
            "Master Status",
        ] = "NOT IN SFDA"
        master.loc[
            (master["GTIN"] != "")
            & (
                master[
                    "Physical vs SFDA Active Variance"
                ].abs() >= 1
            ),
            "Master Status",
        ] = "REVIEW REQUIRED"

        output_columns = [
            "BN",
            "Expiry Month Key",
            "GTIN",
            "Drug Name",
            "Generic Item Number",
            "Trade Item Number",
            "PackageSize",
            "WMS Receipt Expiry Date",
            "WMS Dispatch Expiry Date",
            "SFDA Expiry Date",
            "First Receipt Date",
            "Last Receipt Date",
            "First Dispatch Date",
            "Last Dispatch Date",
            "Total Received Units",
            "Total Received Packages",
            "Total Dispatched Units",
            "Total Dispatched Packages",
            "Net Physical Packages",
            "SFDA Quantity",
            "SFDA Active",
            "SFDA Receive Pending",
            "SFDA Send Pending",
            "Physical vs SFDA Active Variance",
            "Historical Receipt Uncovered",
            "Historical Dispatch Uncovered",
            "Master Status",
        ]

        for column in output_columns:
            if column not in master.columns:
                master[column] = None

        return (
            master[output_columns]
            .sort_values(
                ["Master Status", "BN", "Expiry Month Key"],
                kind="stable",
            )
            .reset_index(drop=True)
        )

    def _build_master(
        self,
        receipt_events: pd.DataFrame,
        dispatch_events: pd.DataFrame,
        sfda_summary: pd.DataFrame,
    ) -> pd.DataFrame:
        receipt_summary = (
            receipt_events.groupby(
                self.KEYS,
                as_index=False,
                dropna=False,
            )
            .agg({
                "Received Quantity": "sum",
                "Quantity Packages": "sum",
                "Event Date": [
                    self._first_valid_date,
                    self._last_valid_date,
                ],
                "Trade Name":
                    self._first_non_blank,
                "Generic Item Number":
                    self._first_non_blank,
                "Trade Item":
                    self._first_non_blank,
                "Expiry Date":
                    "first",
                "PackageSize":
                    "first",
            })
        )

        receipt_summary.columns = [
            "BN",
            "Expiry Month Key",
            "Total Received Units",
            "Total Received Packages",
            "First Receipt Date",
            "Last Receipt Date",
            "Receipt Trade Name",
            "Generic Item Number",
            "Receipt Trade Item Number",
            "WMS Receipt Expiry Date",
            "Receipt PackageSize",
        ]

        dispatch_summary = (
            dispatch_events.groupby(
                self.KEYS,
                as_index=False,
                dropna=False,
            )
            .agg({
                "Dispatched Quantity": "sum",
                "Quantity Packages": "sum",
                "Event Date": [
                    self._first_valid_date,
                    self._last_valid_date,
                ],
                "Trade Name":
                    self._first_non_blank,
                "Generic Item Number":
                    self._first_non_blank,
                "Trade Item Number":
                    self._first_non_blank,
                "Expiry Date":
                    "first",
                "PackageSize":
                    "first",
            })
        )

        dispatch_summary.columns = [
            "BN",
            "Expiry Month Key",
            "Total Dispatched Units",
            "Total Dispatched Packages",
            "First Dispatch Date",
            "Last Dispatch Date",
            "Dispatch Trade Name",
            "Dispatch Generic Item Number",
            "Dispatch Trade Item Number",
            "WMS Dispatch Expiry Date",
            "Dispatch PackageSize",
        ]

        all_keys = pd.concat(
            [
                receipt_summary[self.KEYS],
                dispatch_summary[self.KEYS],
                sfda_summary[self.KEYS],
            ],
            ignore_index=True,
        ).drop_duplicates()

        master = all_keys.merge(
            receipt_summary,
            on=self.KEYS,
            how="left",
        )

        master = master.merge(
            dispatch_summary,
            on=self.KEYS,
            how="left",
        )

        master = master.merge(
            sfda_summary,
            on=self.KEYS,
            how="left",
        )

        numeric_columns = [
            "Total Received Units",
            "Total Received Packages",
            "Total Dispatched Units",
            "Total Dispatched Packages",
            "SFDA Quantity",
            "SFDA Active",
            "SFDA Receive Pending",
            "SFDA Send Pending",
        ]

        for column in numeric_columns:
            master[column] = pd.to_numeric(
                master[column],
                errors="coerce",
            ).fillna(0)

        master["PackageSize"] = (
            pd.to_numeric(
                master["PackageSize"],
                errors="coerce",
            )
            .fillna(
                pd.to_numeric(
                    master["Receipt PackageSize"],
                    errors="coerce",
                )
            )
            .fillna(
                pd.to_numeric(
                    master["Dispatch PackageSize"],
                    errors="coerce",
                )
            )
            .fillna(1)
        )

        master.loc[
            master["PackageSize"] <= 0,
            "PackageSize",
        ] = 1

        master["GTIN"] = (
            master["GTIN"]
            .fillna("")
            .astype(str)
        )

        master["Drug Name"] = (
            master["Drug Name"]
            .fillna(master["Receipt Trade Name"])
            .fillna(master["Dispatch Trade Name"])
            .fillna("")
        )

        master["Generic Item Number"] = (
            master["Generic Item Number"]
            .fillna(
                master[
                    "Dispatch Generic Item Number"
                ]
            )
            .fillna("")
        )

        master["Trade Item Number"] = (
            master[
                "Receipt Trade Item Number"
            ]
            .fillna(
                master[
                    "Dispatch Trade Item Number"
                ]
            )
            .fillna("")
        )

        master["Net Physical Packages"] = (
            master["Total Received Packages"]
            - master[
                "Total Dispatched Packages"
            ]
        )

        master["Physical vs SFDA Active Variance"] = (
            master["Net Physical Packages"]
            - master["SFDA Active"]
        )

        master["Historical Receipt Uncovered"] = (
            master["SFDA Receive Pending"]
            - master["Total Received Packages"]
        ).clip(lower=0)

        master["Historical Dispatch Uncovered"] = (
            master["SFDA Active"]
            - master["Total Dispatched Packages"]
        ).clip(lower=0)

        master["Master Status"] = "BALANCED"

        master.loc[
            master["GTIN"] == "",
            "Master Status",
        ] = "NOT IN SFDA"

        master.loc[
            (
                master["GTIN"] != ""
            )
            & (
                master[
                    "Physical vs SFDA Active Variance"
                ].abs() >= 1
            ),
            "Master Status",
        ] = "REVIEW REQUIRED"

        master = master.sort_values(
            by=[
                "Master Status",
                "BN",
                "Expiry Month Key",
            ],
            kind="stable",
        ).reset_index(drop=True)

        output_columns = [
            "BN",
            "Expiry Month Key",
            "GTIN",
            "Drug Name",
            "Generic Item Number",
            "Trade Item Number",
            "PackageSize",
            "WMS Receipt Expiry Date",
            "WMS Dispatch Expiry Date",
            "SFDA Expiry Date",
            "First Receipt Date",
            "Last Receipt Date",
            "First Dispatch Date",
            "Last Dispatch Date",
            "Total Received Units",
            "Total Received Packages",
            "Total Dispatched Units",
            "Total Dispatched Packages",
            "Net Physical Packages",
            "SFDA Quantity",
            "SFDA Active",
            "SFDA Receive Pending",
            "SFDA Send Pending",
            "Physical vs SFDA Active Variance",
            "Historical Receipt Uncovered",
            "Historical Dispatch Uncovered",
            "Master Status",
        ]

        return master[output_columns].copy()

    @staticmethod
    def _records(
        dataframe: pd.DataFrame,
    ) -> List[Dict[str, Any]]:
        """
        Convert dataframe rows into database-safe Python values.

        Object columns can still contain pandas.NaT, so every individual
        value is checked rather than relying only on the column dtype.
        """
        records: List[Dict[str, Any]] = []

        for source_row in dataframe.to_dict(
            orient="records"
        ):
            clean_row: Dict[str, Any] = {}

            for key, value in source_row.items():
                if value is None:
                    clean_row[key] = None
                    continue

                try:
                    if pd.isna(value):
                        clean_row[key] = None
                        continue
                except (TypeError, ValueError):
                    pass

                if isinstance(value, pd.Timestamp):
                    clean_row[key] = (
                        value.to_pydatetime()
                    )
                else:
                    clean_row[key] = value

            records.append(clean_row)

        return records

    def run(self) -> Dict[str, Any]:
        self._normalize()

        package_lookup = self._prepare_packsize()

        receipt_events = self._receipt_events(
            package_lookup
        )
        dispatch_events = self._dispatch_events(
            package_lookup
        )
        sfda_summary = self._sfda_summary(
            package_lookup
        )

        master = self._build_master(
            receipt_events,
            dispatch_events,
            sfda_summary,
        )

        return {
            "receipt_events":
                receipt_events,
            "dispatch_events":
                dispatch_events,
            "master":
                master,
            "receipt_records":
                self._records(receipt_events),
            "dispatch_records":
                self._records(dispatch_events),
            "master_records":
                self._records(master),
        }
