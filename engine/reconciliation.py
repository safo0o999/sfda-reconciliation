from pathlib import Path

import pandas as pd

from engine.validator import Validator
from engine.normalizer import Normalizer
from engine.calculator import Calculator


class ReconciliationEngine:

    def __init__(
        self,
        asn_df,
        inventory_df,
        dispatch_df,
        sfda_df
    ):

        self.asn = asn_df.copy()
        self.inventory = inventory_df.copy()
        self.dispatch = dispatch_df.copy()
        self.sfda = sfda_df.copy()

        config_path = (
            Path(__file__).resolve().parent.parent
            / "config"
        )

        packsize_path = (
            config_path
            / "pack_size.xlsx"
        )

        gln_path = (
            config_path
            / "gln.xlsx"
        )

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
            dtype=object
        )

        self.gln = pd.read_excel(
            gln_path,
            engine="openpyxl",
            dtype=object
        )

    def normalize(self):

        self.asn = Normalizer.normalize_asn(
            self.asn
        )

        self.inventory = (
            Normalizer.normalize_inventory(
                self.inventory
            )
        )

        self.dispatch = (
            Normalizer.normalize_dispatch(
                self.dispatch
            )
        )

        self.sfda = Normalizer.normalize_sfda(
            self.sfda
        )

        self.packsize = (
            Normalizer.normalize_packsize(
                self.packsize
            )
        )

        self.gln = Normalizer.normalize_gln(
            self.gln
        )

    def validate(self):

        Validator.validate(
            self.asn,
            "ASN"
        )

        Validator.validate(
            self.inventory,
            "INVENTORY"
        )

        Validator.validate(
            self.dispatch,
            "DISPATCH"
        )

        Validator.validate(
            self.sfda,
            "SFDA"
        )

        Validator.validate(
            self.packsize,
            "PACKSIZE"
        )

        required_gln_columns = [
            "GLN",
            "To Address"
        ]

        missing_gln_columns = [
            column
            for column in required_gln_columns
            if column not in self.gln.columns
        ]

        if missing_gln_columns:
            raise ValueError(
                "GLN missing columns: "
                f"{missing_gln_columns}"
            )

    def calculate(self):

        return Calculator.calculate(
            sfda_df=self.sfda,
            receiving_df=self.asn,
            inventory_df=self.inventory,
            dispatch_df=self.dispatch,
            packsize_df=self.packsize,
            gln_df=self.gln
        )

    def run(self):

        self.normalize()
        self.validate()

        return self.calculate()
