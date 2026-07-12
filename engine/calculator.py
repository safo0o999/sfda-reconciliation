import pandas as pd

from engine.grouper import Grouper
from config.business_rules import BusinessRules


class Calculator:

    KEYS = [
        "BN",
        "Expiry Date"
    ]

    @staticmethod
    def _prepare_packsize(packsize_df):

        packsize = (
            packsize_df[
                [
                    "Trade Name",
                    "PackageSize"
                ]
            ]
            .copy()
        )

        packsize = packsize[
            packsize["Trade Name"] != ""
        ]

        packsize = (
            packsize
            .sort_values(
                by="PackageSize",
                ascending=False
            )
            .drop_duplicates(
                subset=["Trade Name"],
                keep="first"
            )
        )

        packsize["PackageSize"] = (
            pd.to_numeric(
                packsize["PackageSize"],
                errors="coerce"
            )
            .fillna(1)
        )

        packsize.loc[
            packsize["PackageSize"] <= 0,
            "PackageSize"
        ] = 1

        return packsize

    @staticmethod
    def _summarize_sources(
        receiving_df,
        inventory_df,
        dispatch_df
    ):

        receiving = Grouper.summarize(
            receiving_df,
            Calculator.KEYS,
            "Received Quantity"
        )

        inventory = Grouper.summarize(
            inventory_df,
            Calculator.KEYS,
            "Available Quantity"
        )

        dispatch = Grouper.summarize(
            dispatch_df,
            Calculator.KEYS,
            "Dispatched Quantity"
        )

        return receiving, inventory, dispatch

    @staticmethod
    def _apply_business_rules(row):

        to_be_accept = BusinessRules.to_be_accept(
            inventory=row["Inventory"],
            receiving=row["Receiving"],
            active=row["Active"],
            qty_sent_pending=row["Quantity sent pending"],
            qty_receive_pending=row[
                "Quantity Receive Pending"
            ]
        )

        to_be_dispatch = BusinessRules.to_be_dispatch(
            inventory=row["Inventory"],
            active=row["Active"]
        )

        return pd.Series(
            {
                "To Be Accept": to_be_accept,
                "To Be Dispatch": to_be_dispatch
            }
        )

    @staticmethod
    def _build_accept(master):

        accept = master[
            master["To Be Accept"] > 0
        ][
            [
                "GTIN",
                "To Be Accept",
                "BN",
                "Expiry Date"
            ]
        ].copy()

        accept["To Be Accept"] = (
            accept["To Be Accept"]
            .fillna(0)
            .astype(int)
        )

        accept["Expiry Date"] = (
            accept["Expiry Date"]
            .dt.strftime("%Y-%m-%d")
        )

        return accept.reset_index(drop=True)

    @staticmethod
    def _build_dispatch(master):

        dispatch = master[
            master["To Be Dispatch"] > 0
        ][
            [
                "GTIN",
                "To Be Dispatch",
                "BN",
                "Expiry Date"
            ]
        ].copy()

        dispatch["To Be Dispatch"] = (
            dispatch["To Be Dispatch"]
            .fillna(0)
            .astype(int)
        )

        dispatch["Expiry Date"] = (
            dispatch["Expiry Date"]
            .dt.strftime("%Y-%m-%d")
        )

        return dispatch.reset_index(drop=True)

    @staticmethod
    def _build_variance(master):

        variance = master.copy()

        variance["New Active"] = (
            variance["Active"]
            + variance["To Be Accept"]
            - variance["To Be Dispatch"]
        )

        variance["Remaining Receive Pending"] = (
            variance["Quantity Receive Pending"]
            - variance["To Be Accept"]
        ).clip(lower=0)

        variance["Remaining Sent Pending"] = (
            variance["Quantity sent pending"]
            - variance["To Be Dispatch"]
        ).clip(lower=0)

        variance["Remaining Receiving"] = (
            variance["Receiving"]
            - variance["To Be Accept"]
        ).clip(lower=0)

        variance["Remaining Dispatch"] = (
            variance["Dispatch"]
            - variance["To Be Dispatch"]
        ).clip(lower=0)

        variance["Active Variance"] = (
            variance["New Active"]
            - variance["Inventory"]
        )

        variance["Receive Variance"] = (
            variance["Remaining Receive Pending"]
            - variance["Remaining Receiving"]
        )

        variance["Dispatch Variance"] = (
            variance["Remaining Sent Pending"]
            - variance["Remaining Dispatch"]
        )

        variance = variance[
            (variance["Active Variance"] != 0)
            | (variance["Receive Variance"] != 0)
            | (variance["Dispatch Variance"] != 0)
        ].copy()

        variance["Expiry Date"] = (
            variance["Expiry Date"]
            .dt.strftime("%Y-%m-%d")
        )

        variance_columns = [
            "GTIN",
            "Drug Name",
            "BN",
            "Expiry Date",
            "PackageSize",
            "Quantity",
            "Active",
            "Quantity Receive Pending",
            "Quantity sent pending",
            "Receiving",
            "Inventory",
            "Dispatch",
            "To Be Accept",
            "To Be Dispatch",
            "New Active",
            "Remaining Receive Pending",
            "Remaining Sent Pending",
            "Active Variance",
            "Receive Variance",
            "Dispatch Variance"
        ]

        return variance[
            variance_columns
        ].reset_index(drop=True)

    @staticmethod
    def calculate(
        sfda_df,
        receiving_df,
        inventory_df,
        dispatch_df,
        packsize_df
    ):

        packsize = Calculator._prepare_packsize(
            packsize_df
        )

        receiving, inventory, dispatch = (
            Calculator._summarize_sources(
                receiving_df,
                inventory_df,
                dispatch_df
            )
        )

        master = sfda_df.copy()

        master = master.merge(
            packsize,
            left_on="Drug Name",
            right_on="Trade Name",
            how="left"
        )

        master["PackageSize"] = (
            pd.to_numeric(
                master["PackageSize"],
                errors="coerce"
            )
            .fillna(1)
        )

        master.loc[
            master["PackageSize"] <= 0,
            "PackageSize"
        ] = 1

        master = master.merge(
            receiving,
            on=Calculator.KEYS,
            how="left"
        )

        master = master.merge(
            inventory,
            on=Calculator.KEYS,
            how="left"
        )

        master = master.merge(
            dispatch,
            on=Calculator.KEYS,
            how="left"
        )

        quantity_columns = [
            "Received Quantity",
            "Available Quantity",
            "Dispatched Quantity"
        ]

        for column in quantity_columns:
            master[column] = (
                pd.to_numeric(
                    master[column],
                    errors="coerce"
                )
                .fillna(0)
            )

        master["Receiving"] = (
            master["Received Quantity"]
            / master["PackageSize"]
        ).fillna(0)

        master["Inventory"] = (
            master["Available Quantity"]
            / master["PackageSize"]
        ).fillna(0)

        master["Dispatch"] = (
            master["Dispatched Quantity"]
            / master["PackageSize"]
        ).fillna(0)

        master["Receiving"] = (
            master["Receiving"]
            .fillna(0)
            .astype(float)
        )

        master["Inventory"] = (
            master["Inventory"]
            .fillna(0)
            .astype(float)
        )

        master["Dispatch"] = (
            master["Dispatch"]
            .fillna(0)
            .astype(float)
        )

        calculated = master.apply(
            Calculator._apply_business_rules,
            axis=1
        )

        master = pd.concat(
            [
                master,
                calculated
            ],
            axis=1
        )

        master["To Be Accept"] = (
            master["To Be Accept"]
            .fillna(0)
            .astype(int)
        )

        master["To Be Dispatch"] = (
            master["To Be Dispatch"]
            .fillna(0)
            .astype(int)
        )

        accept = Calculator._build_accept(master)
        dispatch_output = Calculator._build_dispatch(master)
        variance = Calculator._build_variance(master)

        return {
            "master": master,
            "accept": accept,
            "dispatch": dispatch_output,
            "variance": variance
        }
