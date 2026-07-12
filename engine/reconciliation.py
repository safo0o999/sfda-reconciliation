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

        self.packsize = pd.read_excel(
            config_path / "pack_size.xlsx",
            dtype=object
        )

        self.gln = pd.read_excel(
            config_path / "gln.xlsx",
            dtype=object
        )

    def normalize(self):

        self.asn = Normalizer.normalize_asn(self.asn)
        self.inventory = Normalizer.normalize_inventory(self.inventory)
        self.dispatch = Normalizer.normalize_dispatch(self.dispatch)
        self.sfda = Normalizer.normalize_sfda(self.sfda)
        self.packsize = Normalizer.normalize_packsize(self.packsize)

    def validate(self):

        Validator.validate(self.asn, "ASN")
        Validator.validate(self.inventory, "INVENTORY")
        Validator.validate(self.dispatch, "DISPATCH")
        Validator.validate(self.sfda, "SFDA")
        Validator.validate(self.packsize, "PACKSIZE")

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
