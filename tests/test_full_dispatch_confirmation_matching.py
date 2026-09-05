import sys
import unittest
from unittest.mock import MagicMock


sys.modules.setdefault("pyodbc", MagicMock())

from engine.database import (
    _full_dispatch_transaction_key,
    _prioritize_exact_full_dispatch_confirmation,
)


class FullDispatchConfirmationMatchingTests(unittest.TestCase):
    def test_unique_exact_quantity_is_confirmed_before_fifo_rows(self):
        alhamna = ("ALHAMNA", 25000.0, 0.0, 500.0, 0.0)
        ohod = ("OHOD", 33600.0, 0.0, 672.0, 0.0)
        baha = ("BAHA", 4150.0, 0.0, 83.0, 0.0)

        ordered = _prioritize_exact_full_dispatch_confirmation(
            [alhamna, ohod, baha],
            83.0,
        )

        self.assertEqual(ordered[0][0], "BAHA")
        self.assertEqual([row[0] for row in ordered[1:]], ["ALHAMNA", "OHOD"])

    def test_duplicate_exact_quantities_remain_ambiguous_and_keep_fifo(self):
        first = ("CUSTOMER-A", 4150.0, 0.0, 83.0, 0.0)
        second = ("CUSTOMER-B", 4150.0, 0.0, 83.0, 0.0)
        original = [first, second]

        ordered = _prioritize_exact_full_dispatch_confirmation(original, 83.0)

        self.assertEqual(ordered, original)

    def test_partial_non_exact_evidence_keeps_fifo(self):
        first = ("CUSTOMER-A", 25000.0, 0.0, 500.0, 0.0)
        second = ("CUSTOMER-B", 33600.0, 0.0, 672.0, 0.0)
        original = [first, second]

        ordered = _prioritize_exact_full_dispatch_confirmation(original, 100.0)

        self.assertEqual(ordered, original)

    def test_transaction_key_is_isolated_by_cutover(self):
        common = ("B001", "2028-02-15", "5118151700100", "MOH - BAHA", "6286844000015")

        before = _full_dispatch_transaction_key(*common)
        after = _full_dispatch_transaction_key(*common, "40aa39dd-61e7-4f03-ae47-cc1125ef463a")

        self.assertNotEqual(before, after)


if __name__ == "__main__":
    unittest.main()
