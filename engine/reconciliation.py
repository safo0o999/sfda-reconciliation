from pathlib import Path

import pandas as pd

from engine.validator import Validator
from engine.normalizer import Normalizer
from engine.calculator import Calculator


class ReconciliationEngine:

    MODE_FULL = "full"
    MODE_ACCEPT = "accept"
    MODE_DISPATCH = "dispatch"

    def __init__(
        self,
        asn_df=None,
        inventory_df=None,
        dispatch_df=None,
        sfda_df=None,
        mode="full"
    ):
        self.mode = str(mode or self.MODE_FULL).strip().lower()
        if self.mode not in {self.MODE_FULL, self.MODE_ACCEPT, self.MODE_DISPATCH}:
            raise ValueError(f"Unsupported reconciliation mode: {self.mode}")

        self.asn = (asn_df if asn_df is not None else pd.DataFrame()).copy()
        self.inventory = (inventory_df if inventory_df is not None else pd.DataFrame()).copy()
        self.dispatch = (dispatch_df if dispatch_df is not None else pd.DataFrame()).copy()
        self.sfda = (sfda_df if sfda_df is not None else pd.DataFrame()).copy()

        config_path = Path(__file__).resolve().parent.parent / "config"
        packsize_path = config_path / "pack_size.xlsx"
        gln_path = config_path / "gln.xlsx"

        if not packsize_path.exists():
            raise FileNotFoundError("config/pack_size.xlsx was not found.")
        if not gln_path.exists():
            raise FileNotFoundError("config/gln.xlsx was not found.")

        self.packsize = pd.read_excel(packsize_path, engine="openpyxl", dtype=object)
        self.gln = pd.read_excel(gln_path, engine="openpyxl", dtype=object)

    @staticmethod
    def _empty_asn():
        return pd.DataFrame(columns=[
            "BN", "Expiry Date", "Trade Name", "Received Quantity",
            "Inbound Shipment", "ASN Line", "Generic Item Number",
            "Trade Item", "Supplier Name", "Received Date"
        ])

    @staticmethod
    def _empty_inventory():
        return pd.DataFrame(columns=[
            "BN", "Expiry Date", "Trade Name", "Available Quantity"
        ])

    @staticmethod
    def _empty_dispatch():
        return pd.DataFrame(columns=[
            "BN", "Expiry Date", "Trade Name", "Dispatched Quantity",
            "To Address", "Sales Order Number", "Order Line",
            "Trade Item Number", "Confirm Date", "Order Line Status"
        ])

    def normalize(self):
        if self.mode in {self.MODE_FULL, self.MODE_ACCEPT}:
            self.asn = Normalizer.normalize_asn(self.asn)
        else:
            self.asn = self._empty_asn()

        if self.mode in {self.MODE_FULL, self.MODE_ACCEPT, self.MODE_DISPATCH}:
            self.inventory = Normalizer.normalize_inventory(self.inventory)

        if self.mode in {self.MODE_FULL, self.MODE_DISPATCH}:
            self.dispatch = Normalizer.normalize_dispatch(self.dispatch)
        else:
            self.dispatch = self._empty_dispatch()

        self.sfda = Normalizer.normalize_sfda(self.sfda)
        self.packsize = Normalizer.normalize_packsize(self.packsize)
        self.gln = Normalizer.normalize_gln(self.gln)

    def validate(self):
        if self.mode in {self.MODE_FULL, self.MODE_ACCEPT}:
            Validator.validate(self.asn, "ASN")
        Validator.validate(self.inventory, "INVENTORY")
        if self.mode in {self.MODE_FULL, self.MODE_DISPATCH}:
            Validator.validate(self.dispatch, "DISPATCH")
        Validator.validate(self.sfda, "SFDA")
        Validator.validate(self.packsize, "PACKSIZE")

        required_gln_columns = ["GLN", "To Address"]
        missing = [c for c in required_gln_columns if c not in self.gln.columns]
        if missing:
            raise ValueError(f"GLN missing columns: {missing}")

    def calculate(self):
        result = Calculator.calculate(
            sfda_df=self.sfda,
            receiving_df=self.asn,
            inventory_df=self.inventory,
            dispatch_df=self.dispatch,
            packsize_df=self.packsize,
            gln_df=self.gln
        )

        if self.mode == self.MODE_ACCEPT:
            result["dispatch"] = result["dispatch"].iloc[0:0].copy()
            # Accept stage variance focuses on receipt pending not covered by uploaded ASN evidence.
            master = result["master"]
            result["variance"] = master[
                (master["Quantity Receive Pending"] - master["To Be Accept"]).clip(lower=0) > 0
            ].copy()
            if not result["variance"].empty:
                result["variance"]["Remaining Receive Pending"] = (
                    result["variance"]["Quantity Receive Pending"]
                    - result["variance"]["To Be Accept"]
                ).clip(lower=0)
                result["variance"]["Variance Type"] = "PENDING RECEIVE WITHOUT ASN EVIDENCE"

        elif self.mode == self.MODE_DISPATCH:
            result["accept"] = result["accept"].iloc[0:0].copy()
            # Dispatch variance is active/inventory gap not supported by Full Dispatch evidence.
            master = result["master"]
            result["variance"] = master[
                master["Dispatch Variance"] > 0
            ].copy() if "Dispatch Variance" in master.columns else result["variance"]
            if result["variance"].empty:
                result["variance"] = master[
                    master["Unexplained Dispatch Variance"] > 0
                ].copy()
            if not result["variance"].empty:
                result["variance"]["Variance Type"] = "MISSING FULL DISPATCH EVIDENCE"

        return result

    def run(self):
        self.normalize()
        self.validate()
        return self.calculate()
