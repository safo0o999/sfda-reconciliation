import sys
import unittest
from unittest.mock import MagicMock

import pandas as pd

sys.modules.setdefault("pyodbc", MagicMock())

from engine.full_reconciliation import FullReconciliationEngine
from engine.inbound_classification import STO_INCOMING, SUPPLIER
from engine.reconciliation import ReconciliationEngine


def _daily_engine(supplier_each=0, sto_each=0):
    engine = object.__new__(ReconciliationEngine)
    engine.supplier_history = pd.DataFrame([
        {
            "BN": "B1",
            "Expiry Date": "2028-08-31",
            "Generic Item Number": "G1",
            "PackageSize": 1,
            "Received Quantity Each": supplier_each,
        }
    ]) if supplier_each else pd.DataFrame()
    engine.sto_incoming_history = pd.DataFrame([
        {
            "BN": "B1",
            "Expiry Date": "2028-08-31",
            "Generic Item Number": "G1",
            "PackageSize": 1,
            "Received Quantity Each": sto_each,
        }
    ]) if sto_each else pd.DataFrame()
    engine.validated_sfda_identity = pd.DataFrame()
    return engine


def _mixed_report(supplier_pack=100, sto_pack=50, quantity=150, pending=50):
    common = {
        "BN": "B1",
        "Expiry Month Key": "2028-08",
        "Generic Item Number": "G1",
        "Quantity": quantity,
        "Quantity Receive Pending": pending,
    }
    return pd.DataFrame([
        {**common, "Receipt Type": SUPPLIER, "Received Quantity Pack": supplier_pack},
        {**common, "Receipt Type": STO_INCOMING, "Received Quantity Pack": sto_pack},
    ])


class AcceptSourceAlignmentTests(unittest.TestCase):
    def test_daily_accept_uses_confirmed_supplier_history_before_sto_pending(self):
        engine = _daily_engine(supplier_each=100, sto_each=50)
        result = engine._apply_full_accept_source_allocation(_mixed_report())

        supplier = result.loc[result["Receipt Type"].eq(SUPPLIER), "To Be Accept"].iloc[0]
        sto = result.loc[result["Receipt Type"].eq(STO_INCOMING), "To Be Accept"].iloc[0]
        self.assertEqual(supplier, 0)
        self.assertEqual(sto, 50)

    def test_daily_accept_new_supplier_increment_after_prior_confirmation(self):
        engine = _daily_engine(supplier_each=150)
        report = _mixed_report(supplier_pack=50, sto_pack=0, quantity=150, pending=50)
        result = engine._apply_full_accept_source_allocation(report)

        supplier = result.loc[result["Receipt Type"].eq(SUPPLIER), "To Be Accept"].iloc[0]
        self.assertEqual(supplier, 50)

    def test_daily_accept_without_history_preserves_supplier_first_fallback(self):
        engine = _daily_engine()
        result = engine._apply_full_accept_source_allocation(_mixed_report())

        supplier = result.loc[result["Receipt Type"].eq(SUPPLIER), "To Be Accept"].iloc[0]
        sto = result.loc[result["Receipt Type"].eq(STO_INCOMING), "To Be Accept"].iloc[0]
        self.assertEqual(supplier, 50)
        self.assertEqual(sto, 0)

    def test_daily_accept_uses_full_accept_identity_scope_when_available(self):
        engine = _daily_engine()
        engine.validated_sfda_identity = pd.DataFrame([
            {
                "BN": "B1", "Expiry Month Key": "2028-08",
                "Generic Item Number": "G1",
            }
        ])
        report = pd.concat([
            _mixed_report(),
            _mixed_report().assign(BN="REJECTED", **{"Generic Item Number": "G2"}),
        ], ignore_index=True)

        result = engine._apply_full_accept_identity_scope(report)

        self.assertEqual(set(result["BN"]), {"B1"})

    @staticmethod
    def _receipt_base():
        return {
        "BN": "B1",
        "Expiry Month Key": "2028-08",
        "Expiry Date": pd.Timestamp("2028-08-31"),
        "Generic Item Number": "G1",
        "Trade Item": "T1",
        "Trade Name": "Drug",
        "Description": "Drug",
        "Item Family Group": "PHARMACEUTICALS",
        "Received Quantity": 100,
        "Inbound Shipment": "TRK5060001",
        "ASN Line": "1",
        "Supplier Name": "Supplier",
        "Supplier Code": "S1",
        "Received Date": pd.Timestamp("2026-08-01"),
        "LPN": "LPN-1",
        }

    def test_rebuild_collapses_same_receipt_across_different_source_files(self):
        engine = object.__new__(FullReconciliationEngine)
        engine._receipt_event_key_diagnostics = {}
        base = self._receipt_base()
        engine.asn = pd.DataFrame([
            {**base, "_Source File": "week-1.xlsx"},
            {**base, "_Source File": "month.xlsx"},
        ])

        events = engine._receipt_events()

        self.assertEqual(len(events), 1)
        self.assertEqual(
            engine.receipt_event_key_diagnostics()[
                "overlapping_source_duplicates_removed"
            ],
            1,
        )

    def test_rebuild_keeps_distinct_lpns_with_otherwise_identical_receipts(self):
        engine = object.__new__(FullReconciliationEngine)
        engine._receipt_event_key_diagnostics = {}
        base = self._receipt_base()
        engine.asn = pd.DataFrame([
            {**base, "LPN": "LPN-1"},
            {**base, "LPN": "LPN-2"},
        ])

        self.assertEqual(len(engine._receipt_events()), 2)

    def test_receipt_event_key_is_stable_across_append_file_names(self):
        keys = []
        for source_file in ("week-1.xlsx", "week-2-overlap.xlsx"):
            engine = object.__new__(FullReconciliationEngine)
            engine._receipt_event_key_diagnostics = {}
            engine.asn = pd.DataFrame([
                {**self._receipt_base(), "_Source File": source_file}
            ])
            keys.append(engine._receipt_events()["Event Key"].iloc[0])

        self.assertEqual(keys[0], keys[1])
