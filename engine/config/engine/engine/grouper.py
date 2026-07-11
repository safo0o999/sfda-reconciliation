import pandas as pd


class Grouper:

    @staticmethod
    def group_quantity(
        df,
        trade_item,
        batch,
        expiry,
        quantity
    ):

        grouped = (
            df.groupby(
                [
                    trade_item,
                    batch,
                    expiry
                ],
                as_index=False
            )[quantity]
            .sum()
        )

        return grouped

    @staticmethod
    def group_sfda(df):

        grouped = (
            df.groupby(
                [
                    "GTIN",
                    "Drug Name",
                    "BN",
                    "Expiry Date"
                ],
                as_index=False
            )
            .agg(
                {
                    "Quantity": "sum",
                    "Active": "sum",
                    "Quantity sent pending": "sum",
                    "Quantity Receive Pending": "sum",
                }
            )
        )

        return grouped
