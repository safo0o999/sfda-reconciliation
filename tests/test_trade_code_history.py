import sys
import unittest
from pathlib import Path
from unittest.mock import MagicMock

import pandas as pd

sys.modules.setdefault("pyodbc", MagicMock())

from engine.exporter import Exporter
from engine.trade_code import (
    TRADE_CODE_LOGIC_VERSION,
    aggregate_trade_codes,
    combine_trade_codes,
    split_trade_codes,
    trade_code_count,
    trade_code_status,
)


class TradeCodeHistoryTests(unittest.TestCase):
    def test_receipt_codes_are_first_and_dispatch_adds_only_new_codes(self):
        result = combine_trade_codes(
            "4213220302304 | 4213220302303",
            "4213220302301 | 4213220302304",
        )

        self.assertEqual(
            result,
            "4213220302304 | 4213220302303 | 4213220302301",
        )
        self.assertEqual(split_trade_codes(result), [
            "4213220302304", "4213220302303", "4213220302301"
        ])

    def test_group_aggregation_is_distinct_and_deterministic(self):
        values = pd.Series(["T2", "T1", "T2", "", None])

        self.assertEqual(aggregate_trade_codes(values), "T1 | T2")
        self.assertEqual(trade_code_count("T1 | T2"), 2)
        self.assertEqual(trade_code_status("T1 | T2"), "Multiple Trade Codes")
        self.assertEqual(trade_code_status("T1"), "Unique")
        self.assertEqual(trade_code_status(""), "Missing")

    def test_batch_export_exposes_code_count_and_status_without_mutating_input(self):
        source = pd.DataFrame([
            {"Trade Item Number": "T1 | T2", "BN": "B1"},
            {"Trade Item Number": "T3", "BN": "B2"},
        ])

        report = Exporter._with_trade_code_columns(source, include_summary=True)

        self.assertIn("Trade Item Number", source.columns)
        self.assertNotIn("Trade Item Number", report.columns)
        self.assertEqual(report["Trade Code"].tolist(), ["T1 | T2", "T3"])
        self.assertEqual(report["Trade Code Count"].tolist(), [2, 1])
        self.assertEqual(
            report["Trade Code Status"].tolist(),
            ["Multiple Trade Codes", "Unique"],
        )

    def test_backfill_is_active_build_scoped_and_quantity_guarded(self):
        sql = Path("sql/003_trade_code_history_backfill.sql").read_text(
            encoding="utf-8"
        )

        self.assertIn("IsActive = 1", sql)
        self.assertIn("r.BuildID = @BuildID", sql)
        self.assertIn("d.BuildID = @BuildID", sql)
        self.assertIn("SupplierHistory quantity verification failed", sql)
        self.assertIn("CustomerHistory quantity verification failed", sql)
        self.assertIn("ROLLBACK TRANSACTION", sql)
        self.assertIn("LIKE N'TRK5060%'", sql)

    def test_logic_version_is_explicit(self):
        self.assertEqual(
            TRADE_CODE_LOGIC_VERSION,
            "TRADE_CODE_V1_MULTI_CODE_GRAIN_20260903",
        )


if __name__ == "__main__":
    unittest.main()
