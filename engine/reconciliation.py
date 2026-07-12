from engine.reader import ExcelReader
from engine.validator import Validator
from engine.normalizer import Normalizer
from engine.calculator import Calculator
from engine.exporter import Exporter


class ReconciliationEngine:

    def __init__(
        self,
        asn_file,
        inventory_file,
        dispatch_file,
        sfda_file,
        packsize_file
    ):

        self.asn_file = asn_file
        self.inventory_file = inventory_file
        self.dispatch_file = dispatch_file
        self.sfda_file = sfda_file
        self.packsize_file = packsize_file

    def load(self):

        self.asn = ExcelReader.read(self.asn_file)
        self.inventory = ExcelReader.read(self.inventory_file)
        self.dispatch = ExcelReader.read(self.dispatch_file)
        self.sfda = ExcelReader.read(self.sfda_file)
        self.packsize = ExcelReader.read(self.packsize_file)

    def validate(self):

        Validator.validate(self.asn, "ASN")
        Validator.validate(self.inventory, "INVENTORY")
        Validator.validate(self.dispatch, "DISPATCH")
        Validator.validate(self.sfda, "SFDA")
        Validator.validate(self.packsize, "PACKSIZE")

    def normalize(self):

        self.asn = Normalizer.normalize_asn(self.asn)
        self.inventory = Normalizer.normalize_inventory(self.inventory)
        self.dispatch = Normalizer.normalize_dispatch(self.dispatch)
        self.sfda = Normalizer.normalize_sfda(self.sfda)
        self.packsize = Normalizer.normalize_packsize(self.packsize)

    def calculate(self):

        return Calculator.calculate(
            self.sfda,
            self.asn,
            self.inventory,
            self.dispatch,
            self.packsize
        )

    def export(self):

        result = self.calculate()

        return {
            "master": result["master"],
            "accept": result.get("accept"),
            "dispatch": result.get("dispatch"),
            "variance": result.get("variance")
        }
