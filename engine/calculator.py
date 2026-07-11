from engine.grouper import Grouper
from config.business_rules import BusinessRules


class Calculator:

    @staticmethod
    def calculate(
        sfda_df,
        receiving_df,
        inventory_df,
        dispatch_df
    ):

        receiving = Grouper.summarize(
            receiving_df,
            ["GTIN", "BN", "Expiry Date"],
            "Received Quantity"
        )

        inventory = Grouper.summarize(
            inventory_df,
            ["GTIN", "BN", "Expiry Date"],
            "Available Quantity"
        )

        dispatch = Grouper.summarize(
            dispatch_df,
            ["GTIN", "BN", "Expiry Date"],
            "Dispatched Quantity"
        )

        return {
            "receiving": receiving,
            "inventory": inventory,
            "dispatch": dispatch
        }
