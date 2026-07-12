import pandas as pd

from engine.grouper import Grouper
from config.business_rules import BusinessRules


class Calculator:

    @staticmethod
    def calculate(
        sfda_df,
        receiving_df,
        inventory_df,
        dispatch_df,
        packsize_df
    ):

        receiving = Grouper.summarize(
            receiving_df,
            ["BN", "Expiry Date"],
            "Received Quantity"
        )

        inventory = Grouper.summarize(
            inventory_df,
            ["BN", "Expiry Date"],
            "Available Quantity"
        )

        dispatch = Grouper.summarize(
            dispatch_df,
            ["BN", "Expiry Date"],
            "Dispatched Quantity"
        )

        master = sfda_df.merge(
            receiving,
            on=["BN", "Expiry Date"],
            how="left"
        )

        master = master.merge(
            inventory,
            on=["BN", "Expiry Date"],
            how="left"
        )

        master = master.merge(
            dispatch,
            on=["BN", "Expiry Date"],
            how="left"
        )

        master["Received Quantity"] = (
            master["Received Quantity"]
            .fillna(0)
        )

        master["Available Quantity"] = (
            master["Available Quantity"]
            .fillna(0)
        )

        master["Dispatched Quantity"] = (
            master["Dispatched Quantity"]
            .fillna(0)
        )

        return {
            "master": master
        }
