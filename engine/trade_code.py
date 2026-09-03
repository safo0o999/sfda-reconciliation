from __future__ import annotations

from collections.abc import Iterable
from typing import Any

import pandas as pd


TRADE_CODE_DELIMITER = " | "
TRADE_CODE_LOGIC_VERSION = "TRADE_CODE_V1_MULTI_CODE_GRAIN_20260903"


def _iter_values(value: Any) -> Iterable[Any]:
    if value is None:
        return ()
    if isinstance(value, pd.Series):
        return value.tolist()
    if isinstance(value, (list, tuple, set)):
        return value
    return (value,)


def split_trade_codes(value: Any) -> list[str]:
    """Return distinct normalized WMS trade codes in source order."""

    result: list[str] = []
    seen: set[str] = set()
    for item in _iter_values(value):
        if item is None or pd.isna(item):
            continue
        for raw_code in str(item).split("|"):
            code = raw_code.strip()
            if not code or code.lower() in {"nan", "none"} or code in seen:
                continue
            seen.add(code)
            result.append(code)
    return result


def combine_trade_codes(*values: Any) -> str:
    """Combine receipt/dispatch codes without losing valid many-code batches."""

    result: list[str] = []
    seen: set[str] = set()
    for value in values:
        for code in split_trade_codes(value):
            if code in seen:
                continue
            seen.add(code)
            result.append(code)
    return TRADE_CODE_DELIMITER.join(result)


def aggregate_trade_codes(values: Any) -> str:
    """Pandas groupby aggregator for a deterministic distinct code list."""

    return TRADE_CODE_DELIMITER.join(sorted(split_trade_codes(values)))


def trade_code_count(value: Any) -> int:
    return len(split_trade_codes(value))


def trade_code_status(value: Any) -> str:
    count = trade_code_count(value)
    if count == 0:
        return "Missing"
    if count == 1:
        return "Unique"
    return "Multiple Trade Codes"
