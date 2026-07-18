from pathlib import Path

import pandas as pd

from config.business_rules import BusinessRules
from engine.normalizer import Normalizer
from engine.validator import Validator


class ReconciliationEngine:
    """Step 2: reconcile Batch Master against latest Inventory and SFDA."""

    KEYS = [
        "BN",
        "Expiry Month Key",
        "Generic Item Number",
    ]
    SFDA_KEYS = [
        "BN",
        "Expiry Month Key",
    ]

    def __init__(
        self,
        batch_master_df,
        inventory_df,
        sfda_df,
        dispatch_events_df=None,
    ):
        self.batch_master = batch_master_df.copy()
        self.inventory = inventory_df.copy()
        self.sfda = sfda_df.copy()
        self.dispatch_events = (
            dispatch_events_df.copy()
            if dispatch_events_df is not None
            else pd.DataFrame()
        )

        config = (
            Path(__file__).resolve().parent.parent
            / "config"
        )
        packsize_path = config / "pack_size.xlsx"
        gln_path = config / "gln.xlsx"

        if not packsize_path.exists():
            raise FileNotFoundError(
                "config/pack_size.xlsx was not found."
            )
        if not gln_path.exists():
            raise FileNotFoundError(
                "config/gln.xlsx was not found."
            )

        self.packsize = pd.read_excel(
            packsize_path,
            engine="openpyxl",
            dtype=object,
        )
        self.gln = pd.read_excel(
            gln_path,
            engine="openpyxl",
            dtype=object,
        )

    @staticmethod
    def _month_key(series):
        return (
            Normalizer.date(series)
            .dt.strftime("%Y-%m")
            .fillna("")
        )

    def normalize(self):
        self.inventory = Normalizer.normalize_inventory(
            self.inventory
        )
        self.sfda = Normalizer.normalize_sfda(
            self.sfda
        )
        self.packsize = Normalizer.normalize_packsize(
            self.packsize
        )
        self.gln = Normalizer.normalize_gln(
            self.gln
        )

        self.inventory["Expiry Month Key"] = (
            self._month_key(
                self.inventory["Expiry Date"]
            )
        )
        self.sfda["Expiry Month Key"] = (
            self._month_key(
                self.sfda["Expiry Date"]
            )
        )

        for column in self.KEYS:
            if column not in self.batch_master.columns:
                self.batch_master[column] = ""

        self.batch_master["BN"] = Normalizer.text(
            self.batch_master["BN"]
        )
        self.batch_master[
            "Generic Item Number"
        ] = Normalizer.identifier(
            self.batch_master[
                "Generic Item Number"
            ]
        )

        if "Expiry Date" in self.batch_master.columns:
            self.batch_master["Expiry Date"] = (
                Normalizer.date(
                    self.batch_master["Expiry Date"]
                )
            )

        if self.dispatch_events is not None:
            for column in self.KEYS:
                if column not in self.dispatch_events.columns:
                    self.dispatch_events[column] = ""

    def validate(self):
        Validator.validate(
            self.inventory,
            "INVENTORY",
        )
        Validator.validate(
            self.sfda,
            "SFDA",
        )
        Validator.validate(
            self.packsize,
            "PACKSIZE",
        )

    def _pack_lookup(self):
        pack = self.packsize[
            ["Trade Name", "PackageSize"]
        ].copy()

        pack["PackageSize"] = (
            pd.to_numeric(
                pack["PackageSize"],
                errors="coerce",
            )
            .fillna(1)
        )

        pack.loc[
            pack["PackageSize"] <= 0,
            "PackageSize",
        ] = 1

        return pack.drop_duplicates(
            "Trade Name",
            keep="first",
        )

    def calculate(self):
        master = self.batch_master.copy()

        inventory = (
            self.inventory.groupby(
                self.KEYS,
                dropna=False,
            )
            .agg(
                **{
                    "Inventory Available Each": (
                        "Available Quantity",
                        "sum",
                    ),
                    "Inventory Trade Name": (
                        "Trade Name",
                        "first",
                    ),
                    "Inventory Expiry Date": (
                        "Expiry Date",
                        "first",
                    ),
                }
            )
            .reset_index()
        )

        sfda = (
            self.sfda.groupby(
                self.SFDA_KEYS,
                dropna=False,
            )
            .agg(
                GTIN=("GTIN", "first"),
                **{
                    "Drug Name": (
                        "Drug Name",
                        "first",
                    ),
                    "SFDA Expiry Date": (
                        "Expiry Date",
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
                    "Send Pending": (
                        "Quantity sent pending",
                        "sum",
                    ),
                    "Receive Pending": (
                        "Quantity Receive Pending",
                        "sum",
                    ),
                }
            )
            .reset_index()
        )

        sfda_match = sfda.copy()
        sfda_match["_Batch Exists in SFDA"] = True

        report = (
            master.merge(
                inventory,
                on=self.KEYS,
                how="left",
            )
            .merge(
                sfda_match,
                on=self.SFDA_KEYS,
                how="left",
                suffixes=("", " SFDA"),
            )
        )

        # Re-evaluate status against the latest SFDA report used for this run.
        # Yes: this exact batch exists in SFDA.
        # Missing Batch: another batch for the same Generic exists in SFDA.
        # Generic Not in SFDA: no batch for this Generic exists in SFDA.
        matched_generics = set(
            report.loc[
                report["_Batch Exists in SFDA"].fillna(False),
                "Generic Item Number",
            ]
            .fillna("")
            .astype(str)
            .str.strip()
        )

        report["Generic Exists in SFDA"] = "Generic Not in SFDA"
        report.loc[
            report["Generic Item Number"].isin(matched_generics),
            "Generic Exists in SFDA",
        ] = "Missing Batch"
        report.loc[
            report["_Batch Exists in SFDA"].fillna(False),
            "Generic Exists in SFDA",
        ] = "Yes"

        report = report[
            report["Generic Exists in SFDA"] != "Generic Not in SFDA"
        ].copy()
        report = report.drop(columns=["_Batch Exists in SFDA"])

        master_trade_name = report.get(
            "Trade Name",
            pd.Series("", index=report.index),
        ).fillna("")

        inventory_trade_name = report.get(
            "Inventory Trade Name",
            pd.Series("", index=report.index),
        ).fillna("")

        report["Trade Name For Pack"] = (
            inventory_trade_name.where(
                inventory_trade_name != "",
                master_trade_name,
            )
        )

        report = report.merge(
            self._pack_lookup(),
            left_on="Trade Name For Pack",
            right_on="Trade Name",
            how="left",
            suffixes=("", " Pack"),
        )

        report["PackageSize"] = (
            pd.to_numeric(
                report["PackageSize"],
                errors="coerce",
            )
            .fillna(1)
        )

        report.loc[
            report["PackageSize"] <= 0,
            "PackageSize",
        ] = 1

        numeric_columns = [
            "Total Receive Qty",
            "Total Dispatched Qty",
            "Inventory Available Each",
            "Quantity",
            "Active",
            "Send Pending",
            "Receive Pending",
        ]

        for column in numeric_columns:
            if column not in report.columns:
                report[column] = 0

            report[column] = (
                pd.to_numeric(
                    report[column],
                    errors="coerce",
                )
                .fillna(0)
            )

        report["Total Receive Pack"] = (
            report["Total Receive Qty"]
            / report["PackageSize"]
        )

        report["Total Dispatched Pack"] = (
            report["Total Dispatched Qty"]
            / report["PackageSize"]
        )

        report["Inventory Available Pack"] = (
            report["Inventory Available Each"]
            / report["PackageSize"]
        )

        report["To Be Accept"] = report.apply(
            lambda row: BusinessRules.to_be_accept(
                row["Total Receive Pack"],
                row["Inventory Available Pack"],
                row["Active"],
                row["Send Pending"],
                row["Receive Pending"],
            ),
            axis=1,
        )

        report["To Be Dispatched"] = report.apply(
            lambda row: BusinessRules.to_be_dispatch(
                row["Active"],
                row["Inventory Available Pack"],
            ),
            axis=1,
        )

        report["Expiry Date"] = report[
            "SFDA Expiry Date"
        ].where(
            report["SFDA Expiry Date"].notna(),
            report.get("Expiry Date"),
        )

        # Build Step 3 datasets before renaming the user-facing report columns.
        eligible_for_csv = report["Generic Exists in SFDA"] == "Yes"

        accept = report[
            eligible_for_csv & (report["To Be Accept"] > 0)
        ][
            [
                "GTIN",
                "To Be Accept",
                "BN",
                "Expiry Date",
            ]
        ].copy()

        dispatch_targets = report[
            eligible_for_csv & (report["To Be Dispatched"] > 0)
        ].copy()

        dispatch = self._allocate_dispatch(
            dispatch_targets
        )

        # Step 2 report: generated fresh on every run and not stored in SQL.
        reconciliation_report = report.rename(
            columns={
                "BN": "Batch No",
                "Generic Item Number": "Generic No",
                "Generic Exists in SFDA": "Exists in SFDA",
                "PackageSize": "Pack Size",
                "Total Receive Qty": "Total Received Each",
                "Total Receive Pack": "Total Received Pack",
                "Total Dispatched Qty": "Total Dispatched Each",
                "Total Dispatched Pack": "Total Dispatched Pack",
                "Inventory Available Each": "Inventory Each",
                "Inventory Available Pack": "Inventory Pack",
                "Quantity": "SFDA Quantity",
                "Active": "SFDA Active",
                "To Be Dispatched": "To Be Dispatch",
            }
        )

        report_columns = [
            "Batch No",
            "Expiry Date",
            "GTIN",
            "Generic No",
            "Exists in SFDA",
            "Pack Size",
            "Total Received Each",
            "Total Received Pack",
            "Total Dispatched Each",
            "Total Dispatched Pack",
            "Inventory Each",
            "Inventory Pack",
            "SFDA Quantity",
            "SFDA Active",
            "Receive Pending",
            "Send Pending",
            "To Be Accept",
            "To Be Dispatch",
        ]

        reconciliation_report = reconciliation_report[
            [
                column
                for column in report_columns
                if column in reconciliation_report.columns
            ]
        ].copy()

        return {
            "report": reconciliation_report,
            "accept": accept,
            "dispatch": dispatch,
        }

    def _allocate_dispatch(self, targets):
        columns = [
            "GTIN",
            "BN",
            "Expiry Date",
            "To Address",
            "GLN",
            "Customer Status",
            "Allocated To Be Dispatch",
        ]

        if (
            targets.empty
            or self.dispatch_events.empty
        ):
            return pd.DataFrame(columns=columns)

        events = self.dispatch_events.copy()

        merged = events.merge(
            targets[
                self.KEYS
                + [
                    "GTIN",
                    "Expiry Date",
                    "PackageSize",
                    "To Be Dispatched",
                ]
            ],
            on=self.KEYS,
            how="inner",
            validate="many_to_one",
        )

        merged["Dispatched Quantity"] = (
            pd.to_numeric(
                merged.get(
                    "Dispatched Quantity",
                    0,
                ),
                errors="coerce",
            )
            .fillna(0)
        )

        merged["PackageSize"] = (
            pd.to_numeric(
                merged["PackageSize"],
                errors="coerce",
            )
            .fillna(1)
        )

        merged.loc[
            merged["PackageSize"] <= 0,
            "PackageSize",
        ] = 1

        merged["Evidence Packages"] = (
            merged["Dispatched Quantity"]
            / merged["PackageSize"]
        ).astype(int)

        sort_columns = [
            column
            for column in [
                "Dispatch Date",
                "Sales Order Number",
                "Order Line",
            ]
            if column in merged.columns
        ]

        if sort_columns:
            merged = merged.sort_values(
                by=sort_columns,
                kind="stable",
            )

        rows = []

        for _, group in merged.groupby(
            self.KEYS,
            sort=False,
            dropna=False,
        ):
            remaining = int(
                group["To Be Dispatched"].iloc[0]
            )

            for _, row in group.iterrows():
                allocated = min(
                    remaining,
                    int(row["Evidence Packages"]),
                )

                if allocated <= 0:
                    continue

                item = row.to_dict()
                item[
                    "Allocated To Be Dispatch"
                ] = allocated
                rows.append(item)

                remaining -= allocated

                if remaining <= 0:
                    break

        output = pd.DataFrame(rows)

        if output.empty:
            return pd.DataFrame(columns=columns)

        output = output.merge(
            self.gln[
                ["To Address", "GLN"]
            ],
            on="To Address",
            how="left",
        )

        output["Customer Status"] = "REGISTERED"

        missing_gln = (
            output["GLN"].isna()
            | (
                output["GLN"]
                .astype(str)
                .str.strip()
                == ""
            )
        )

        output.loc[
            missing_gln,
            "Customer Status",
        ] = "DUMMY"

        output.loc[
            missing_gln,
            "GLN",
        ] = "DUMMY"

        return output

    def run(self):
        self.normalize()
        self.validate()
        return self.calculate()
