import sys
import unittest
from unittest.mock import MagicMock

import pandas as pd

sys.modules.setdefault("pyodbc", MagicMock())

from engine.exporter import Exporter
from engine.full_reconciliation import FullReconciliationEngine


def engine():
    return FullReconciliationEngine(pd.DataFrame(), pd.DataFrame(), pd.DataFrame())


class FullDispatchResidualTests(unittest.TestCase):
    def test_inventory_difference_uses_customer_history_then_dummy_gln(self):
        subject = engine()
        inventory = pd.DataFrame([
            {
                "BN": "B001",
                "Expiry Date": "2027-11-15",
                "Available Quantity": 100,
                "Trade Name": "Drug A",
                "Generic Item Number": "1001",
                "Trade Item Number": "INVENTORY-T1",
            }
        ])
        validated_sfda = pd.DataFrame([
            {
                "GTIN": "06281234567890",
                "Drug Name": "Drug A",
                "BN": "B001",
                "Expiry Date": pd.Timestamp("2027-11-15"),
                "Expiry Month Key": "2027-11",
                "Generic Item Number": "1001",
                "Quantity": 500,
                "Active": 500,
                "Quantity sent pending": 0,
                "Quantity Receive Pending": 0,
            }
        ])
        customer_history = pd.DataFrame([
            {
                "To Address": "Customer A",
                "GLN": "99999999999999",
                "GTIN": "06281234567890",
                "Drug Name": "Drug A",
                "Generic Item Number": "1001",
                "Trade Description": "Drug A",
                "BN": "B001",
                "Expiry Date": pd.Timestamp("2027-11-15"),
                "Expiry Month Key": "2027-11",
                "PackageSize": 1,
                "Dispatch Quantity Each": 50,
                "Dispatch Quantity Pack": 50,
                "First Dispatch Date": pd.Timestamp("2026-06-01"),
                "Last Dispatch Date": pd.Timestamp("2026-06-01"),
                "Custody": "",
                "Trade Item Number": "DISPATCH-T1",
            }
        ])
        batch_master = pd.DataFrame([
            {
                "BN": "B001",
                "Expiry Month Key": "2027-11",
                "Generic Item Number": "1001",
                "Trade Item Number": "MASTER-T1",
            }
        ])

        result = subject.build_dispatch_reconciliation(
            inventory,
            pd.DataFrame(),
            customer_history,
            confirmed_full_dispatch_df=pd.DataFrame(),
            batch_master_df=batch_master,
            validated_sfda_identity_df=validated_sfda,
        )

        self.assertEqual(int(result["To Be Dispatch"].sum()), 400)
        customer_row = result.loc[result["To Address"].eq("Customer A")].iloc[0]
        dummy_row = result.loc[
            result["To Address"].eq("UNMAPPED / DUMMY GLN")
        ].iloc[0]
        self.assertEqual(int(customer_row["To Be Dispatch"]), 50)
        self.assertEqual(customer_row["Trade Item Number"], "DISPATCH-T1")
        self.assertEqual(int(dummy_row["To Be Dispatch"]), 350)
        self.assertEqual(dummy_row["GLN"], "99999999999999")
        self.assertEqual(dummy_row["Trade Item Number"], "MASTER-T1")

        files = Exporter.build_dispatch_files_by_customer(result)
        self.assertEqual(list(files), ["99999999999999_001.csv"])
        self.assertIn(
            "06281234567890;400;B001;15-11-2027",
            next(iter(files.values())),
        )

    def test_full_dispatch_report_schema_includes_trade_item_number(self):
        self.assertIn(
            "Trade Item Number",
            FullReconciliationEngine.DISPATCH_RECONCILIATION_COLUMNS,
        )
        self.assertIn(
            "Trade Item Number",
            Exporter.FULL_DISPATCH_RECONCILIATION_COLUMNS,
        )
        for duplicate_column in [
            "Reserved Full Dispatch Quantity Each",
            "Reserved Full Dispatch Quantity Pack",
        ]:
            self.assertNotIn(
                duplicate_column,
                FullReconciliationEngine.DISPATCH_RECONCILIATION_COLUMNS,
            )
            self.assertNotIn(
                duplicate_column,
                Exporter.FULL_DISPATCH_RECONCILIATION_COLUMNS,
            )

if __name__ == "__main__":
    unittest.main()
