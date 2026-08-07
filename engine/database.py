import json
import logging
import os
import time
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Sequence, Set, Tuple

import pandas as pd
import pyodbc


logger = logging.getLogger("SFDA-Reconciliation.Database")

_EVENT_KEY_LOOKUP_BATCH_SIZE = 1000
_BULK_INSERT_BATCH_SIZE = 10000


class Database:
    """Azure SQL connection provider for SFDA Reconciliation v5."""

    def __init__(self):
        connection_string = os.getenv("SQL_CONNECTION_STRING", "").strip()

        if not connection_string:
            raise RuntimeError("SQL_CONNECTION_STRING is missing.")

        self.connection_string = connection_string

    def connect(self):
        return pyodbc.connect(
            self.connection_string,
            autocommit=False,
        )


def _load_schema_sql() -> str:
    """Load the idempotent Version 5 SQL schema from the project SQL folder."""

    schema_path = (
        Path(__file__).resolve().parent.parent
        / "sql"
        / "001_initial_schema.sql"
    )

    if not schema_path.exists():
        raise RuntimeError(
            f"Database schema file is missing: {schema_path}"
        )

    schema_sql = schema_path.read_text(encoding="utf-8").strip()

    if not schema_sql:
        raise RuntimeError(
            f"Database schema file is empty: {schema_path}"
        )

    return schema_sql


def _consume_all_results(cursor: pyodbc.Cursor) -> None:
    """Consume all result sets and row-count messages from a SQL batch."""

    while True:
        if cursor.description is not None:
            cursor.fetchall()

        try:
            has_next = cursor.nextset()
        except pyodbc.ProgrammingError:
            break

        if not has_next:
            break


def initialize_database() -> None:
    """Create or safely upgrade all Version 5 database objects."""

    started_at = time.perf_counter()

    with Database().connect() as connection:
        cursor = connection.cursor()

        try:
            cursor.execute(_load_schema_sql())
            _consume_all_results(cursor)
            connection.commit()

        except Exception:
            connection.rollback()
            raise

    logger.info(
        "Database schema check completed in %.2f seconds.",
        time.perf_counter() - started_at,
    )


def _value(
    row: Dict[str, Any],
    name: str,
    default: Any = None,
) -> Any:
    value = row.get(name, default)

    try:
        if pd.isna(value):
            return default
    except (TypeError, ValueError):
        pass

    if isinstance(value, pd.Timestamp):
        return value.to_pydatetime()

    return value


def _text(
    row: Dict[str, Any],
    name: str,
    default: str = "",
) -> str:
    value = _value(row, name, default)

    if value is None:
        return default

    text = str(value).strip()

    if text.endswith(".0"):
        text = text[:-2]

    return text


def _number(
    row: Dict[str, Any],
    name: str,
    default: float = 0,
) -> float:
    value = pd.to_numeric(
        pd.Series([_value(row, name, default)]),
        errors="coerce",
    ).iloc[0]

    if pd.isna(value):
        return float(default)

    return float(value)


def _number_with_fallback(
    row: Dict[str, Any],
    primary_name: str,
    fallback_name: str,
    default: float = 0,
) -> float:
    primary_value = _value(row, primary_name)

    if primary_value is not None:
        parsed = pd.to_numeric(
            pd.Series([primary_value]),
            errors="coerce",
        ).iloc[0]

        if not pd.isna(parsed):
            return float(parsed)

    return _number(row, fallback_name, default)


def _integer(
    row: Dict[str, Any],
    name: str,
    default: int = 0,
) -> int:
    return int(_number(row, name, default))


def _text_with_fallback(
    row: Dict[str, Any],
    primary_name: str,
    fallback_name: str,
    default: str = "",
) -> str:
    primary = _text(row, primary_name, "")

    if primary:
        return primary

    return _text(row, fallback_name, default)


def _validate_event_identity(
    event_key: str,
    bn: str,
    expiry_month_key: str,
    generic_item_number: str,
) -> None:
    missing = []

    if not event_key:
        missing.append("Event Key")
    if not bn:
        missing.append("BN")
    if not expiry_month_key:
        missing.append("Expiry Month Key")
    if not generic_item_number:
        missing.append("Generic Item Number")

    if missing:
        raise ValueError(
            "Event cannot be saved because required values are missing: "
            + ", ".join(missing)
        )


def _receipt_parameters(row: Dict[str, Any]) -> Tuple[Any, ...]:
    event_key = _text(row, "Event Key")
    bn = _text(row, "BN")
    expiry_month_key = _text(row, "Expiry Month Key")
    generic_item_number = _text(row, "Generic Item Number")

    _validate_event_identity(
        event_key,
        bn,
        expiry_month_key,
        generic_item_number,
    )

    return (
        event_key,
        bn,
        expiry_month_key,
        _value(row, "Expiry Date"),
        generic_item_number,
        _text(row, "Trade Item"),
        _text(row, "Trade Name"),
        _number(row, "Received Quantity"),
        _text(row, "Inbound Shipment"),
        _text(row, "ASN Line"),
        _text(row, "Supplier Name"),
        _text(row, "Supplier Code"),
        _text(row, "Description"),
        _text(row, "Item Family Group"),
        _value(row, "Received Date"),
    )


def _dispatch_parameters(row: Dict[str, Any]) -> Tuple[Any, ...]:
    event_key = _text(row, "Event Key")
    bn = _text(row, "BN")
    expiry_month_key = _text(row, "Expiry Month Key")
    generic_item_number = _text(row, "Generic Item Number")

    _validate_event_identity(
        event_key,
        bn,
        expiry_month_key,
        generic_item_number,
    )

    return (
        event_key,
        bn,
        expiry_month_key,
        _value(row, "Expiry Date"),
        generic_item_number,
        _text(row, "Trade Item Number"),
        _text(row, "Trade Name"),
        _number(row, "Dispatched Quantity"),
        _text(row, "To Address"),
        _text(row, "Sales Order Number"),
        _text(row, "Order Line"),
        _value(row, "Dispatch Date"),
    )


def _deduplicate_parameters(
    rows: Sequence[Dict[str, Any]],
    parameter_builder,
) -> List[Tuple[Any, ...]]:
    """Prepare rows once and de-duplicate them by EventKey in memory."""

    unique: Dict[str, Tuple[Any, ...]] = {}

    for row in rows or []:
        parameters = parameter_builder(row)
        event_key = str(parameters[0])
        unique.setdefault(event_key, parameters)

    return list(unique.values())


def _chunks(
    values: Sequence[Any],
    size: int,
) -> Iterable[Sequence[Any]]:
    for start in range(0, len(values), size):
        yield values[start : start + size]


def _existing_event_keys(
    cursor: pyodbc.Cursor,
    table_name: str,
    event_keys: Sequence[str],
) -> Set[str]:
    """Fetch existing EventKeys using bounded IN queries.

    SQL Server accepts at most 2100 parameters per statement, so keys are
    checked in safe batches instead of one query per event.
    """

    existing: Set[str] = set()

    for key_batch in _chunks(
        list(event_keys),
        _EVENT_KEY_LOOKUP_BATCH_SIZE,
    ):
        placeholders = ",".join("?" for _ in key_batch)
        sql = (
            f"SELECT EventKey FROM dbo.{table_name} "
            f"WHERE EventKey IN ({placeholders});"
        )

        rows = cursor.execute(sql, tuple(key_batch)).fetchall()
        existing.update(str(row[0]) for row in rows)

    return existing


def _bulk_insert_rows(
    cursor: pyodbc.Cursor,
    insert_sql: str,
    rows: Sequence[Tuple[Any, ...]],
) -> int:
    """Insert rows in bounded fast_executemany batches."""

    if not rows:
        return 0

    cursor.fast_executemany = True
    inserted = 0

    for row_batch in _chunks(
        list(rows),
        _BULK_INSERT_BATCH_SIZE,
    ):
        cursor.executemany(insert_sql, row_batch)
        inserted += len(row_batch)

    return inserted


def append_events(
    receipt_rows: List[Dict[str, Any]],
    dispatch_rows: List[Dict[str, Any]],
    *,
    assume_empty: bool = False,
) -> Dict[str, int]:
    """Append new receipt and dispatch events with server-side de-duplication.

    The previous implementation first downloaded every matching EventKey from
    SQL in many IN queries. For large historical files that lookup dominated
    the runtime. This version sends rows in fast_executemany batches and lets
    SQL Server perform the EventKey existence check through the clustered
    primary key.

    ``assume_empty`` is used immediately after ``reset_history()`` during a
    rebuild, allowing direct bulk inserts without unnecessary NOT EXISTS
    checks.
    """

    initialize_database()
    started_at = time.perf_counter()

    direct_receipt_sql = r"""
        INSERT INTO dbo.ReceiptEvents
        (
            EventKey, BN, ExpiryMonthKey, ExpiryDate, GenericItemNumber,
            TradeItemNumber, TradeName, ReceivedQuantity, InboundShipment,
            ASNLine, SupplierName, SupplierCode, Description, ItemFamilyGroup,
            ReceivedDate
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
    """

    direct_dispatch_sql = r"""
        INSERT INTO dbo.DispatchEvents
        (
            EventKey, BN, ExpiryMonthKey, ExpiryDate, GenericItemNumber,
            TradeItemNumber, TradeName, DispatchedQuantity, ToAddress,
            SalesOrderNumber, OrderLine, DispatchDate
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
    """

    missing_receipt_sql = r"""
        INSERT INTO dbo.ReceiptEvents
        (
            EventKey, BN, ExpiryMonthKey, ExpiryDate, GenericItemNumber,
            TradeItemNumber, TradeName, ReceivedQuantity, InboundShipment,
            ASNLine, SupplierName, SupplierCode, Description, ItemFamilyGroup,
            ReceivedDate
        )
        SELECT ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?
        WHERE NOT EXISTS
        (
            SELECT 1
            FROM dbo.ReceiptEvents WITH (UPDLOCK, HOLDLOCK)
            WHERE EventKey = ?
        );
    """

    missing_dispatch_sql = r"""
        INSERT INTO dbo.DispatchEvents
        (
            EventKey, BN, ExpiryMonthKey, ExpiryDate, GenericItemNumber,
            TradeItemNumber, TradeName, DispatchedQuantity, ToAddress,
            SalesOrderNumber, OrderLine, DispatchDate
        )
        SELECT ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?
        WHERE NOT EXISTS
        (
            SELECT 1
            FROM dbo.DispatchEvents WITH (UPDLOCK, HOLDLOCK)
            WHERE EventKey = ?
        );
    """

    prepared_receipts = _deduplicate_parameters(
        receipt_rows or [],
        _receipt_parameters,
    )
    prepared_dispatches = _deduplicate_parameters(
        dispatch_rows or [],
        _dispatch_parameters,
    )

    logger.info(
        "Optimized event save started. prepared_receipts=%s "
        "prepared_dispatches=%s assume_empty=%s",
        len(prepared_receipts),
        len(prepared_dispatches),
        assume_empty,
    )

    with Database().connect() as connection:
        cursor = connection.cursor()

        try:
            before_receipts = int(
                cursor.execute(
                    "SELECT COUNT_BIG(*) FROM dbo.ReceiptEvents;"
                ).fetchone()[0]
            )
            before_dispatches = int(
                cursor.execute(
                    "SELECT COUNT_BIG(*) FROM dbo.DispatchEvents;"
                ).fetchone()[0]
            )

            if assume_empty:
                _bulk_insert_rows(
                    cursor,
                    direct_receipt_sql,
                    prepared_receipts,
                )
                _bulk_insert_rows(
                    cursor,
                    direct_dispatch_sql,
                    prepared_dispatches,
                )
            else:
                receipt_parameters = [
                    tuple(row) + (row[0],)
                    for row in prepared_receipts
                ]
                dispatch_parameters = [
                    tuple(row) + (row[0],)
                    for row in prepared_dispatches
                ]

                _bulk_insert_rows(
                    cursor,
                    missing_receipt_sql,
                    receipt_parameters,
                )
                _bulk_insert_rows(
                    cursor,
                    missing_dispatch_sql,
                    dispatch_parameters,
                )

            after_receipts = int(
                cursor.execute(
                    "SELECT COUNT_BIG(*) FROM dbo.ReceiptEvents;"
                ).fetchone()[0]
            )
            after_dispatches = int(
                cursor.execute(
                    "SELECT COUNT_BIG(*) FROM dbo.DispatchEvents;"
                ).fetchone()[0]
            )

            connection.commit()

        except Exception:
            connection.rollback()
            raise

    inserted_receipts = max(0, after_receipts - before_receipts)
    inserted_dispatches = max(0, after_dispatches - before_dispatches)

    logger.info(
        "Optimized event save completed in %.2f seconds. "
        "new_receipts=%s duplicate_receipts=%s "
        "new_dispatches=%s duplicate_dispatches=%s",
        time.perf_counter() - started_at,
        inserted_receipts,
        max(0, len(prepared_receipts) - inserted_receipts),
        inserted_dispatches,
        max(0, len(prepared_dispatches) - inserted_dispatches),
    )

    return {
        "receipt_events": inserted_receipts,
        "dispatch_events": inserted_dispatches,
    }


def get_event_summaries() -> Tuple[pd.DataFrame, pd.DataFrame]:
    """Return cumulative receipt and dispatch summaries for Batch Master."""

    initialize_database()
    started_at = time.perf_counter()

    receipt_sql = r"""
        SELECT
            BN,
            ExpiryMonthKey AS [Expiry Month Key],
            MAX(ExpiryDate) AS [Receipt Expiry Date],
            GenericItemNumber AS [Generic Item Number],
            MAX(NULLIF(TradeItemNumber, '')) AS [Trade Item Number],
            MAX(NULLIF(TradeName, '')) AS [Trade Name],
            MAX(NULLIF(Description, '')) AS [Description],
            MAX(NULLIF(SupplierName, '')) AS [Supplier Name],
            MAX(NULLIF(SupplierCode, '')) AS [Supplier Code],
            MAX(NULLIF(ItemFamilyGroup, '')) AS [Item Family Group],
            COUNT_BIG(*) AS [Receive Runs],
            SUM(ReceivedQuantity) AS [Total Receive Qty],
            MIN(ReceivedDate) AS [First Received Date],
            MAX(ReceivedDate) AS [Last Received Date]
        FROM dbo.ReceiptEvents
        GROUP BY
            BN,
            ExpiryMonthKey,
            GenericItemNumber;
    """

    dispatch_sql = r"""
        SELECT
            BN,
            ExpiryMonthKey AS [Expiry Month Key],
            MAX(ExpiryDate) AS [Dispatch Expiry Date],
            GenericItemNumber AS [Generic Item Number],
            MAX(NULLIF(TradeItemNumber, '')) AS [Trade Item Number],
            MAX(NULLIF(TradeName, '')) AS [Trade Name],
            COUNT_BIG(*) AS [Dispatch Runs],
            SUM(DispatchedQuantity) AS [Total Dispatched Qty],
            MIN(DispatchDate) AS [First Dispatch Date],
            MAX(DispatchDate) AS [Last Dispatch Date]
        FROM dbo.DispatchEvents
        GROUP BY
            BN,
            ExpiryMonthKey,
            GenericItemNumber;
    """

    with Database().connect() as connection:
        receipt = pd.read_sql(receipt_sql, connection)
        dispatch = pd.read_sql(dispatch_sql, connection)

    logger.info(
        "Event summaries loaded in %.2f seconds. "
        "receipt_groups=%s dispatch_groups=%s",
        time.perf_counter() - started_at,
        len(receipt),
        len(dispatch),
    )

    return receipt, dispatch


def get_history_summaries() -> Tuple[pd.DataFrame, pd.DataFrame]:
    """Return supplier and customer history at their operational grains."""

    initialize_database()
    supplier_sql = r"""
        SELECT
            SupplierName AS [Supplier Name],
            SupplierCode AS [Supplier Code],
            BN,
            ExpiryMonthKey AS [Expiry Month Key],
            MAX(ExpiryDate) AS [Expiry Date],
            GenericItemNumber AS [Generic Item Number],
            MAX(NULLIF(TradeItemNumber, '')) AS [Trade Item Number],
            MAX(NULLIF(TradeName, '')) AS [Trade Name],
            MAX(NULLIF(Description, '')) AS [Description],
            MAX(NULLIF(ItemFamilyGroup, '')) AS [Item Family Group],
            SUM(ReceivedQuantity) AS [Received Quantity Each],
            MIN(ReceivedDate) AS [First Received Date],
            MAX(ReceivedDate) AS [Last Received Date]
        FROM dbo.ReceiptEvents
        GROUP BY
            SupplierName,
            SupplierCode,
            BN,
            ExpiryMonthKey,
            GenericItemNumber;
    """
    customer_sql = r"""
       SELECT
    ToAddress AS [To Address],
    BN,
    ExpiryMonthKey AS [Expiry Month Key],
    MAX(ExpiryDate) AS [Expiry Date],
    GenericItemNumber AS [Generic Item Number],
    MAX(NULLIF(TradeItemNumber, '')) AS [Trade Item Number],
    MAX(NULLIF(TradeName, '')) AS [Trade Name],
    SUM(DispatchedQuantity) AS [Dispatch Quantity Each],
    MIN(DispatchDate) AS [First Dispatch Date],
    MAX(DispatchDate) AS [Last Dispatch Date]
FROM dbo.DispatchEvents
GROUP BY
    ToAddress,
    BN,
    ExpiryMonthKey,
    GenericItemNumber;
    """
    with Database().connect() as connection:
        supplier = pd.read_sql(supplier_sql, connection)
        customer = pd.read_sql(customer_sql, connection)
    return supplier, customer


def _replace_history_table(
    table_name: str,
    insert_sql: str,
    rows: Sequence[Tuple[Any, ...]],
) -> int:
    with Database().connect() as connection:
        cursor = connection.cursor()
        try:
            cursor.execute(f"DELETE FROM dbo.{table_name};")
            inserted = _bulk_insert_rows(cursor, insert_sql, list(rows))
            connection.commit()
            return inserted
        except Exception:
            connection.rollback()
            raise


def replace_supplier_history(history: pd.DataFrame) -> Dict[str, Any]:
    initialize_database()
    insert_sql = r"""
        INSERT INTO dbo.SupplierHistory
        (SupplierName, SupplierCode, GTIN, DrugName, GenericItemNumber, Description,
         TradeDescription, BN, ExpiryMonthKey, ExpiryDate, PackageSize,
         ReceivedQuantityEach, ReceivedQuantityPack, FirstReceivedDate,
         LastReceivedDate, ItemFamilyGroup, TradeItemNumber, LastUpdated)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, SYSUTCDATETIME());
    """
    rows = [(
        _text(r, "Supplier Name"), _text(r, "Supplier Code"), _text(r, "GTIN"),
        _text(r, "Drug Name"), _text(r, "Generic Item Number"), _text(r, "Description"),
        _text(r, "Trade Description"), _text(r, "BN"), _text(r, "Expiry Month Key"),
        _value(r, "Expiry Date"), _number(r, "PackageSize"),
        _number(r, "Received Quantity Each"), _number(r, "Received Quantity Pack"),
        _value(r, "First Received Date"), _value(r, "Last Received Date"),
        _text(r, "Item Family Group"), _text(r, "Trade Item Number")
    ) for r in history.to_dict(orient="records")]
    inserted = _replace_history_table("SupplierHistory", insert_sql, rows)
    return {"status": "Completed", "rows_inserted": inserted}


def replace_customer_history(history: pd.DataFrame) -> Dict[str, Any]:
    initialize_database()
    insert_sql = r"""
        INSERT INTO dbo.CustomerHistory
        (ToAddress, GLN, GTIN, DrugName, GenericItemNumber, TradeDescription,
         BN, ExpiryMonthKey, ExpiryDate, PackageSize, DispatchQuantityEach,
         DispatchQuantityPack, FirstDispatchDate, LastDispatchDate,
         TradeItemNumber, LastUpdated)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, SYSUTCDATETIME());
    """
    rows = [(
        _text(r, "To Address"), _text(r, "GLN"), _text(r, "GTIN"),
        _text(r, "Drug Name"), _text(r, "Generic Item Number"),
        _text(r, "Trade Description"), _text(r, "BN"), _text(r, "Expiry Month Key"),
        _value(r, "Expiry Date"), _number(r, "PackageSize"),
        _number(r, "Dispatch Quantity Each"), _number(r, "Dispatch Quantity Pack"),
        _value(r, "First Dispatch Date"), _value(r, "Last Dispatch Date"),
        _text(r, "Trade Item Number")
    ) for r in history.to_dict(orient="records")]
    inserted = _replace_history_table("CustomerHistory", insert_sql, rows)
    return {"status": "Completed", "rows_inserted": inserted}


def get_supplier_history_df() -> pd.DataFrame:
    initialize_database()
    with Database().connect() as connection:
        return pd.read_sql(r"""
            SELECT
                SupplierName AS [Supplier Name],
                SupplierCode AS [Supplier Code],
                GTIN,
                DrugName AS [Drug Name],
                GenericItemNumber AS [Generic Item Number],
                Description,
                TradeDescription AS [Trade Description],
                BN,
                ExpiryMonthKey AS [Expiry Month Key],
                ExpiryDate AS [Expiry Date],
                PackageSize,
                ReceivedQuantityEach AS [Received Quantity Each],
                ReceivedQuantityPack AS [Received Quantity Pack],
                FirstReceivedDate AS [First Received Date],
                LastReceivedDate AS [Last Received Date],
                ItemFamilyGroup AS [Item Family Group],
                TradeItemNumber AS [Trade Item Number],
                LastUpdated AS [Last Updated]
            FROM dbo.SupplierHistory
            ORDER BY SupplierName, GenericItemNumber, BN, ExpiryDate;
        """, connection)


def get_customer_history_df() -> pd.DataFrame:
    initialize_database()
    with Database().connect() as connection:
        return pd.read_sql(r"""
            SELECT
                ToAddress AS [To Address],
                GLN,
                GTIN,
                DrugName AS [Drug Name],
                GenericItemNumber AS [Generic Item Number],
                TradeDescription AS [Trade Description],
                BN,
                ExpiryMonthKey AS [Expiry Month Key],
                ExpiryDate AS [Expiry Date],
                PackageSize,
                DispatchQuantityEach AS [Dispatch Quantity Each],
                DispatchQuantityPack AS [Dispatch Quantity Pack],
                FirstDispatchDate AS [First Dispatch Date],
                LastDispatchDate AS [Last Dispatch Date],
                TradeItemNumber AS [Trade Item Number],
                LastUpdated AS [Last Updated]
            FROM dbo.CustomerHistory
            ORDER BY ToAddress, GenericItemNumber, BN, ExpiryDate;
        """, connection)


def replace_batch_master(master: pd.DataFrame) -> Dict[str, Any]:
    """Atomically replace Batch Master from cumulative event summaries."""

    initialize_database()
    started_at = time.perf_counter()

    required_columns = [
        "BN",
        "Expiry Month Key",
        "Generic Item Number",
    ]

    missing = [
        column
        for column in required_columns
        if column not in master.columns
    ]

    if missing:
        raise ValueError(
            "Batch Master is missing required columns: "
            + ", ".join(missing)
        )

    insert_sql = r"""
        INSERT INTO dbo.BatchMaster
        (
            BN,
            ExpiryMonthKey,
            ExpiryDate,
            GenericItemNumber,
            TradeItemNumber,
            TradeName,
            GTIN,
            DrugName,
            PackageSize,
            SFDAQuantity,
            Active,
            QuantitySentPending,
            QuantityReceivePending,
            Description,
            ItemFamilyGroup,
            SupplierName,
            SupplierCode,
            TotalReceiveQty,
            TotalDispatchedQty,
            ReceiveRuns,
            DispatchRuns,
            FirstReceivedDate,
            LastReceivedDate,
            FirstDispatchDate,
            LastDispatchDate,
            GenericExistsInSFDA,
            LastUpdated
        )
        VALUES (
            ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
            ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
            ?, ?, ?, ?, ?, ?, ?
        );
    """

    rows: Iterable[Tuple[Any, ...]] = (
        (
            _text(row, "BN"),
            _text(row, "Expiry Month Key"),
            _value(row, "Expiry Date"),
            _text(row, "Generic Item Number"),
            _text_with_fallback(
                row,
                "Trade Item Number",
                "Trade Item",
            ),
            _text_with_fallback(
                row,
                "Trade Description",
                "Trade Name",
            ),
            _text(row, "GTIN"),
            _text(row, "Drug Name"),
            _number(row, "PackageSize"),
            _number_with_fallback(
                row,
                "Quantity",
                "SFDA Quantity",
            ),
            _number(row, "Active"),
            _number(row, "Quantity sent pending"),
            _number(row, "Quantity Receive Pending"),
            _text(row, "Description"),
            _text(row, "Item Family Group"),
            _text(row, "Supplier Name"),
            _text(row, "Supplier Code"),
            _number_with_fallback(
                row,
                "Received Quantity Each",
                "Total Receive Qty",
            ),
            _number(row, "Total Dispatched Qty"),
            _integer(row, "Receive Runs"),
            _integer(row, "Dispatch Runs"),
            _value(row, "First Received Date"),
            _value(row, "Last Received Date"),
            _value(row, "First Dispatch Date"),
            _value(row, "Last Dispatch Date"),
            _text(
                row,
                "Generic Exists in SFDA",
                "Yes",
            ) or "Yes",
            _value(
                row,
                "Last Updated",
                pd.Timestamp.utcnow().tz_localize(None),
            ),
        )
        for row in master.to_dict(orient="records")
    )

    prepared_rows = list(rows)

    with Database().connect() as connection:
        cursor = connection.cursor()

        try:
            cursor.execute("DELETE FROM dbo.BatchMaster;")

            inserted_rows = _bulk_insert_rows(
                cursor,
                insert_sql,
                prepared_rows,
            )

            connection.commit()

        except Exception:
            connection.rollback()
            raise

    logger.info(
        "Batch Master replacement completed in %.2f seconds. rows_inserted=%s",
        time.perf_counter() - started_at,
        inserted_rows,
    )

    return {
        "status": "Completed",
        "rows_inserted": inserted_rows,
    }


def get_batch_master_df() -> pd.DataFrame:
    initialize_database()

    sql = r"""
        SELECT
            GTIN,
            DrugName AS [Drug Name],
            BN,
            ExpiryDate AS [Expiry Date],
            PackageSize,
            SFDAQuantity AS [Quantity],
            Active,
            QuantitySentPending AS [Quantity sent pending],
            QuantityReceivePending AS [Quantity Receive Pending],
            GenericItemNumber AS [Generic Item Number],
            Description,
            TradeName AS [Trade Description],
            SupplierName AS [Supplier Name],
            SupplierCode AS [Supplier Code],
            TotalReceiveQty AS [Received Quantity Each],
            CASE
                WHEN ISNULL(PackageSize, 0) > 0
                    THEN TotalReceiveQty / PackageSize
                ELSE 0
            END AS [Received Quantity Pack],
            FirstReceivedDate AS [First Received Date],
            LastReceivedDate AS [Last Received Date],
            TotalDispatchedQty AS [Total Dispatched Qty],
            CASE
                WHEN ISNULL(PackageSize, 0) > 0
                    THEN TotalDispatchedQty / PackageSize
                ELSE 0
            END AS [Total Dispatched Qty Pack],
            FirstDispatchDate AS [First Dispatch Date],
            LastDispatchDate AS [Last Dispatch Date],
            GenericExistsInSFDA AS [Generic Exists in SFDA],
            LastUpdated AS [Last Updated],
            ItemFamilyGroup AS [Item Family Group],
            ExpiryMonthKey AS [Expiry Month Key],
            TradeItemNumber AS [Trade Item Number]
        FROM dbo.BatchMaster
        ORDER BY
            BN,
            ExpiryMonthKey,
            GenericItemNumber;
    """

    with Database().connect() as connection:
        return pd.read_sql(sql, connection)


def get_dispatch_events_df() -> pd.DataFrame:
    """Return dispatch evidence used for To Address allocation in Step 3."""

    initialize_database()

    sql = r"""
        SELECT
            EventKey AS [Event Key],
            BN,
            ExpiryMonthKey AS [Expiry Month Key],
            ExpiryDate AS [Expiry Date],
            GenericItemNumber AS [Generic Item Number],
            TradeItemNumber AS [Trade Item Number],
            TradeName AS [Trade Name],
            DispatchedQuantity AS [Dispatched Quantity],
            ToAddress AS [To Address],
            SalesOrderNumber AS [Sales Order Number],
            OrderLine AS [Order Line],
            DispatchDate AS [Dispatch Date]
        FROM dbo.DispatchEvents
        ORDER BY
            DispatchDate,
            SalesOrderNumber,
            OrderLine,
            EventKey;
    """

    with Database().connect() as connection:
        return pd.read_sql(sql, connection)


def record_run_history(
    run_type: str,
    status: str,
    started_at: Any,
    completed_at: Any = None,
    summary: Optional[Dict[str, Any]] = None,
    error_message: str = "",
) -> str:
    """Persist one auditable application run and return its RunID."""

    initialize_database()

    sql = r"""
        DECLARE @RunID uniqueidentifier = NEWID();

        INSERT INTO dbo.RunHistory
        (
            RunID,
            RunType,
            Status,
            StartedAt,
            CompletedAt,
            SummaryJson,
            ErrorMessage
        )
        VALUES (@RunID, ?, ?, ?, ?, ?, ?);

        SELECT CONVERT(nvarchar(36), @RunID) AS RunID;
    """

    summary_json = (
        json.dumps(summary or {}, ensure_ascii=False, default=str)
        if summary is not None
        else None
    )

    with Database().connect() as connection:
        cursor = connection.cursor()

        try:
            cursor.execute(
                sql,
                (
                    run_type,
                    status,
                    started_at,
                    completed_at,
                    summary_json,
                    error_message or None,
                ),
            )
            row = cursor.fetchone()
            connection.commit()

        except Exception:
            connection.rollback()
            raise

    return str(row[0]) if row is not None else ""


def get_reconciliation_history(limit: int = 100) -> List[Dict[str, Any]]:
    """Return the latest Batch Master and reconciliation runs."""

    initialize_database()
    safe_limit = max(1, min(int(limit), 1000))

    sql = f"""
        SELECT TOP ({safe_limit})
            CONVERT(nvarchar(36), RunID) AS RunID,
            RunType,
            Status,
            StartedAt,
            CompletedAt,
            SummaryJson,
            ErrorMessage
        FROM dbo.RunHistory
        ORDER BY StartedAt DESC, CreatedAt DESC;
    """

    with Database().connect() as connection:
        rows = connection.cursor().execute(sql).fetchall()

    history: List[Dict[str, Any]] = []

    for row in rows:
        try:
            summary = json.loads(row[5]) if row[5] else {}
        except (TypeError, ValueError, json.JSONDecodeError):
            summary = {}

        history.append(
            {
                "run_id": row[0],
                "run_type": row[1],
                "status": row[2],
                "started_at": row[3],
                "completed_at": row[4],
                "summary": summary,
                "error": row[6] or "",
            }
        )

    return history


def reset_history() -> None:
    """Delete all cumulative history and Batch Master rows for rebuild mode."""

    initialize_database()

    with Database().connect() as connection:
        cursor = connection.cursor()

        try:
            cursor.execute(
                """
                DELETE FROM dbo.CustomerHistory;
                DELETE FROM dbo.SupplierHistory;
                DELETE FROM dbo.BatchMaster;
                DELETE FROM dbo.DispatchEvents;
                DELETE FROM dbo.ReceiptEvents;
                DELETE FROM dbo.RunHistory;
                """
            )
            connection.commit()

        except Exception:
            connection.rollback()
            raise


def get_historical_status() -> Dict[str, Any]:
    """Return the readiness and latest build state of historical data."""

    initialize_database()

    sql = r"""
        SELECT
            (SELECT COUNT_BIG(*) FROM dbo.BatchMaster) AS BatchMasterRows,
            (SELECT COUNT_BIG(*) FROM dbo.SupplierHistory) AS SupplierHistoryRows,
            (SELECT COUNT_BIG(*) FROM dbo.CustomerHistory) AS CustomerHistoryRows,
            (SELECT MAX(LastUpdated) FROM dbo.BatchMaster) AS LastBuildUtc;
    """

    with Database().connect() as connection:
        row = connection.cursor().execute(sql).fetchone()

    batch_rows = int(row[0] or 0)
    supplier_rows = int(row[1] or 0)
    customer_rows = int(row[2] or 0)

    return {
        "exists": batch_rows > 0,
        "batch_master_rows": batch_rows,
        "supplier_history_rows": supplier_rows,
        "customer_history_rows": customer_rows,
        "last_build_utc": row[3],
    }



def create_historical_build_job(
    job_id: str,
    operation: str,
    input_manifest: Dict[str, Any],
) -> Dict[str, Any]:
    """Create one queued historical-build job."""

    initialize_database()

    sql = r"""
        INSERT INTO dbo.HistoricalBuildJobs
        (
            JobID,
            Operation,
            Status,
            Progress,
            CurrentStage,
            InputManifestJson,
            UpdatedAt
        )
        VALUES (?, ?, 'Queued', 0, 'Queued for processing', ?, SYSUTCDATETIME());
    """

    with Database().connect() as connection:
        cursor = connection.cursor()
        try:
            cursor.execute(
                sql,
                (
                    str(job_id),
                    str(operation),
                    json.dumps(
                        input_manifest or {},
                        ensure_ascii=False,
                        default=str,
                    ),
                ),
            )
            connection.commit()
        except Exception:
            connection.rollback()
            raise

    return get_historical_build_job(job_id)


def update_historical_build_job(
    job_id: str,
    *,
    status: Optional[str] = None,
    progress: Optional[int] = None,
    current_stage: Optional[str] = None,
    output_manifest: Optional[Dict[str, Any]] = None,
    summary: Optional[Dict[str, Any]] = None,
    error_message: Optional[str] = None,
    mark_started: bool = False,
    mark_completed: bool = False,
) -> None:
    """Update job status without overwriting fields that were not supplied."""

    initialize_database()

    assignments = ["UpdatedAt = SYSUTCDATETIME()"]
    parameters: List[Any] = []

    if status is not None:
        assignments.append("Status = ?")
        parameters.append(str(status))
    if progress is not None:
        assignments.append("Progress = ?")
        parameters.append(max(0, min(int(progress), 100)))
    if current_stage is not None:
        assignments.append("CurrentStage = ?")
        parameters.append(str(current_stage))
    if output_manifest is not None:
        assignments.append("OutputManifestJson = ?")
        parameters.append(
            json.dumps(output_manifest, ensure_ascii=False, default=str)
        )
    if summary is not None:
        assignments.append("SummaryJson = ?")
        parameters.append(
            json.dumps(summary, ensure_ascii=False, default=str)
        )
    if error_message is not None:
        assignments.append("ErrorMessage = ?")
        parameters.append(str(error_message))
    if mark_started:
        assignments.append("StartedAt = COALESCE(StartedAt, SYSUTCDATETIME())")
    if mark_completed:
        assignments.append("CompletedAt = SYSUTCDATETIME()")

    parameters.append(str(job_id))

    sql = f"""
        UPDATE dbo.HistoricalBuildJobs
        SET {", ".join(assignments)}
        WHERE JobID = ?;
    """

    with Database().connect() as connection:
        cursor = connection.cursor()
        try:
            cursor.execute(sql, tuple(parameters))
            if cursor.rowcount == 0:
                raise KeyError(f"Historical build job was not found: {job_id}")
            connection.commit()
        except Exception:
            connection.rollback()
            raise


def get_historical_build_job(job_id: str) -> Dict[str, Any]:
    """Return one historical-build job as a JSON-safe dictionary."""

    initialize_database()

    sql = r"""
        SELECT
            JobID,
            Operation,
            Status,
            Progress,
            CurrentStage,
            InputManifestJson,
            OutputManifestJson,
            SummaryJson,
            ErrorMessage,
            CreatedAt,
            StartedAt,
            CompletedAt,
            UpdatedAt
        FROM dbo.HistoricalBuildJobs
        WHERE JobID = ?;
    """

    with Database().connect() as connection:
        row = connection.cursor().execute(sql, (str(job_id),)).fetchone()

    if row is None:
        raise KeyError(f"Historical build job was not found: {job_id}")

    def parse_json(value: Any) -> Dict[str, Any]:
        if not value:
            return {}
        try:
            parsed = json.loads(value)
            return parsed if isinstance(parsed, dict) else {}
        except (TypeError, ValueError, json.JSONDecodeError):
            return {}

    return {
        "job_id": row[0],
        "operation": row[1],
        "status": row[2],
        "progress": int(row[3] or 0),
        "current_stage": row[4] or "",
        "input_manifest": parse_json(row[5]),
        "output_manifest": parse_json(row[6]),
        "summary": parse_json(row[7]),
        "error": row[8] or "",
        "created_at": row[9],
        "started_at": row[10],
        "completed_at": row[11],
        "updated_at": row[12],
    }

def test_database_connection() -> Dict[str, Optional[Any]]:
    initialize_database()

    sql = r"""
        SELECT
            DB_NAME() AS DatabaseName,
            @@SERVERNAME AS ServerName,
            SYSUTCDATETIME() AS ServerUtcTime,
            (SELECT COUNT_BIG(*) FROM dbo.ReceiptEvents) AS ReceiptEvents,
            (SELECT COUNT_BIG(*) FROM dbo.DispatchEvents) AS DispatchEvents,
            (SELECT COUNT_BIG(*) FROM dbo.BatchMaster) AS BatchMasterRows,
            (SELECT COUNT_BIG(*) FROM dbo.RunHistory) AS RunHistoryRows;
    """

    with Database().connect() as connection:
        row = connection.cursor().execute(sql).fetchone()

    return {
        "status": "Connected",
        "database": row[0],
        "server": row[1],
        "server_utc_time": row[2],
        "receipt_events": int(row[3]),
        "dispatch_events": int(row[4]),
        "batch_master_rows": int(row[5]),
        "run_history_rows": int(row[6]),
    }


def replace_latest_inventory_snapshot(
    inventory_df: pd.DataFrame,
    source_file_name: str = "",
) -> int:
    """Replace the latest Inventory snapshot used by Product Intelligence."""
    initialize_database()
    frame = inventory_df.copy() if inventory_df is not None else pd.DataFrame()
    if frame.empty:
        return 0

    from engine.full_reconciliation import FullReconciliationEngine
    from engine.normalizer import Normalizer

    frame = Normalizer.normalize_inventory(frame)
    frame["Expiry Month Key"] = FullReconciliationEngine._month_key(frame["Expiry Date"])
    frame["Available Quantity"] = pd.to_numeric(frame["Available Quantity"], errors="coerce").fillna(0)

    rows = [
        (
            _text(row, "BN"),
            _text(row, "Expiry Month Key"),
            _value(row, "Expiry Date"),
            _text(row, "Generic Item Number"),
            _text(row, "Trade Name"),
            _number(row, "Available Quantity"),
            source_file_name,
        )
        for row in frame.to_dict(orient="records")
        if _text(row, "BN") and _text(row, "Expiry Month Key")
    ]

    sql = r"""
        INSERT INTO dbo.LatestInventorySnapshot
        (BN, ExpiryMonthKey, ExpiryDate, GenericItemNumber, TradeName,
         AvailableQuantity, SourceFileName)
        VALUES (?, ?, ?, ?, ?, ?, ?);
    """
    with Database().connect() as connection:
        cursor = connection.cursor()
        try:
            cursor.execute("DELETE FROM dbo.LatestInventorySnapshot;")
            _bulk_insert_rows(cursor, sql, rows)
            connection.commit()
        except Exception:
            connection.rollback()
            raise
    return len(rows)


def replace_latest_sfda_snapshot(
    sfda_df: pd.DataFrame,
    source_file_name: str = "",
) -> int:
    """Replace the latest SFDA snapshot used by Product Intelligence."""
    initialize_database()
    frame = sfda_df.copy() if sfda_df is not None else pd.DataFrame()
    if frame.empty:
        return 0

    from engine.full_reconciliation import FullReconciliationEngine
    from engine.normalizer import Normalizer

    frame = Normalizer.normalize_sfda(frame)
    frame["Expiry Month Key"] = FullReconciliationEngine._month_key(frame["Expiry Date"])
    for column in ["Quantity", "Active", "Quantity sent pending", "Quantity Receive Pending"]:
        frame[column] = pd.to_numeric(frame[column], errors="coerce").fillna(0)

    rows = [
        (
            _text(row, "GTIN"),
            _text(row, "Drug Name"),
            _text(row, "BN"),
            _text(row, "Expiry Month Key"),
            _value(row, "Expiry Date"),
            _number(row, "Quantity"),
            _number(row, "Active"),
            _number(row, "Quantity sent pending"),
            _number(row, "Quantity Receive Pending"),
            source_file_name,
        )
        for row in frame.to_dict(orient="records")
        if _text(row, "BN") and _text(row, "Expiry Month Key")
    ]

    sql = r"""
        INSERT INTO dbo.LatestSFDASnapshot
        (GTIN, DrugName, BN, ExpiryMonthKey, ExpiryDate, Quantity, Active,
         QuantitySentPending, QuantityReceivePending, SourceFileName)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
    """
    with Database().connect() as connection:
        cursor = connection.cursor()
        try:
            cursor.execute("DELETE FROM dbo.LatestSFDASnapshot;")
            _bulk_insert_rows(cursor, sql, rows)
            connection.commit()
        except Exception:
            connection.rollback()
            raise
    return len(rows)


def get_latest_inventory_snapshot_df() -> pd.DataFrame:
    initialize_database()
    sql = r"""
        SELECT BN, ExpiryMonthKey AS [Expiry Month Key], ExpiryDate AS [Expiry Date],
               GenericItemNumber AS [Generic Item Number], TradeName AS [Trade Name],
               AvailableQuantity AS [Available Quantity], SourceFileName AS [Source File],
               SnapshotUtc AS [Snapshot UTC]
        FROM dbo.LatestInventorySnapshot;
    """
    with Database().connect() as connection:
        return pd.read_sql(sql, connection)


def get_latest_sfda_snapshot_df() -> pd.DataFrame:
    initialize_database()
    sql = r"""
        SELECT GTIN, DrugName AS [Drug Name], BN,
               ExpiryMonthKey AS [Expiry Month Key], ExpiryDate AS [Expiry Date],
               Quantity, Active, QuantitySentPending AS [Quantity sent pending],
               QuantityReceivePending AS [Quantity Receive Pending],
               SourceFileName AS [Source File], SnapshotUtc AS [Snapshot UTC]
        FROM dbo.LatestSFDASnapshot;
    """
    with Database().connect() as connection:
        return pd.read_sql(sql, connection)


def get_product_intelligence_sources() -> Dict[str, pd.DataFrame]:
    """Load all persisted datasets required by Product Intelligence."""
    return {
        "batch_master": get_batch_master_df(),
        "supplier_history": get_supplier_history_df(),
        "customer_history": get_customer_history_df(),
        "inventory_snapshot": get_latest_inventory_snapshot_df(),
        "sfda_snapshot": get_latest_sfda_snapshot_df(),
    }

# ================================================================
# Daily Upload & Run audit/history compatibility
# ================================================================

def create_reconciliation_run(
    run_number: str,
    process_type: str,
    submitted_by: str,
    application_version: str,
    asn_files: int = 0,
    inventory_files: int = 0,
    dispatch_files: int = 0,
    sfda_files: int = 0,
) -> None:
    initialize_database()
    sql = r"""
        INSERT INTO dbo.ReconciliationRuns
        (
            RunNumber, ProcessType, Status, StartedAt, SubmittedBy,
            ASNFiles, InventoryFiles, DispatchFiles, SFDAFiles,
            ApplicationVersion
        )
        VALUES (?, ?, 'Running', SYSUTCDATETIME(), ?, ?, ?, ?, ?, ?);
    """
    with Database().connect() as connection:
        cursor = connection.cursor()
        try:
            cursor.execute(
                sql,
                (
                    str(run_number), str(process_type).upper(), str(submitted_by),
                    int(asn_files), int(inventory_files), int(dispatch_files),
                    int(sfda_files), str(application_version),
                ),
            )
            connection.commit()
        except Exception:
            connection.rollback()
            raise


def complete_reconciliation_run(
    run_number: str,
    status: str,
    total_input_rows: int = 0,
    master_records: int = 0,
    accept_records: int = 0,
    dispatch_records: int = 0,
    exception_records: int = 0,
    generated_files: int = 0,
    error_message: str = "",
) -> None:
    initialize_database()
    sql = r"""
        UPDATE dbo.ReconciliationRuns
        SET Status = ?,
            CompletedAt = SYSUTCDATETIME(),
            TotalInputRows = ?,
            MasterRecords = ?,
            AcceptRecords = ?,
            DispatchRecords = ?,
            ExceptionRecords = ?,
            GeneratedFiles = ?,
            ErrorMessage = ?
        WHERE RunNumber = ?;
    """
    with Database().connect() as connection:
        cursor = connection.cursor()
        try:
            cursor.execute(
                sql,
                (
                    str(status), int(total_input_rows or 0), int(master_records or 0),
                    int(accept_records or 0), int(dispatch_records or 0),
                    int(exception_records or 0), int(generated_files or 0),
                    str(error_message or "") or None, str(run_number),
                ),
            )
            connection.commit()
        except Exception:
            connection.rollback()
            raise


def save_reconciliation_run_file(
    run_number: str,
    file_category: str,
    file_name: str,
    file_type: str,
    container_name: str,
    blob_name: str,
    content_type: str,
    size_bytes: int,
    etag: str = "",
) -> None:
    initialize_database()
    sql = r"""
        INSERT INTO dbo.ReconciliationRunFiles
        (
            RunNumber, FileCategory, FileName, FileType, ContainerName,
            BlobName, ContentType, SizeBytes, ETag
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?);
    """
    with Database().connect() as connection:
        cursor = connection.cursor()
        try:
            cursor.execute(
                sql,
                (
                    str(run_number), str(file_category), str(file_name), str(file_type),
                    str(container_name), str(blob_name), str(content_type),
                    int(size_bytes or 0), str(etag or ""),
                ),
            )
            connection.commit()
        except Exception:
            connection.rollback()
            raise


def list_reconciliation_runs(limit: int = 500) -> List[Dict[str, Any]]:
    initialize_database()
    safe_limit = max(1, min(int(limit), 5000))
    sql = f"""
        SELECT TOP ({safe_limit})
            RunID, RunNumber, ProcessType, Status, StartedAt, CompletedAt,
            SubmittedBy, ASNFiles, InventoryFiles, DispatchFiles, SFDAFiles,
            TotalInputRows, MasterRecords, AcceptRecords, DispatchRecords,
            ExceptionRecords, GeneratedFiles, ApplicationVersion, ErrorMessage
        FROM dbo.ReconciliationRuns
        ORDER BY StartedAt DESC, RunID DESC;
    """
    with Database().connect() as connection:
        cursor = connection.cursor()
        rows = cursor.execute(sql).fetchall()
        names = [column[0] for column in cursor.description]
    return [dict(zip(names, row)) for row in rows]


def get_reconciliation_run(run_number: str) -> Optional[Dict[str, Any]]:
    initialize_database()
    sql = r"""
        SELECT TOP (1)
            RunID, RunNumber, ProcessType, Status, StartedAt, CompletedAt,
            SubmittedBy, ASNFiles, InventoryFiles, DispatchFiles, SFDAFiles,
            TotalInputRows, MasterRecords, AcceptRecords, DispatchRecords,
            ExceptionRecords, GeneratedFiles, ApplicationVersion, ErrorMessage
        FROM dbo.ReconciliationRuns
        WHERE RunNumber = ?
        ORDER BY RunID DESC;
    """
    with Database().connect() as connection:
        cursor = connection.cursor()
        row = cursor.execute(sql, (str(run_number),)).fetchone()
        if row is None:
            return None
        names = [column[0] for column in cursor.description]
    return dict(zip(names, row))


def list_reconciliation_run_files(run_number: str) -> List[Dict[str, Any]]:
    initialize_database()
    sql = r"""
        SELECT RunFileID, RunNumber, FileCategory, FileName, FileType,
               ContainerName, BlobName, ContentType, SizeBytes, ETag, CreatedAt
        FROM dbo.ReconciliationRunFiles
        WHERE RunNumber = ?
        ORDER BY CreatedAt, RunFileID;
    """
    with Database().connect() as connection:
        cursor = connection.cursor()
        rows = cursor.execute(sql, (str(run_number),)).fetchall()
        names = [column[0] for column in cursor.description]
    return [dict(zip(names, row)) for row in rows]


def _daily_transaction_key(process_type: str, row: Dict[str, Any]) -> str:
    import hashlib
    preferred = [
        "Transaction Key", "Event Key", "Inbound Shipment", "ASN Line",
        "Sales Order Number", "Order Line", "BN", "Expiry Month Key",
        "Generic Item Number", "Received Date", "Dispatch Date",
        "Received Quantity", "Dispatched Quantity",
    ]
    parts = [str(process_type).upper()]
    for name in preferred:
        value = _value(row, name, "")
        parts.append(str(value or "").strip().upper())
    return hashlib.sha256("|".join(parts).encode("utf-8")).hexdigest()


def save_daily_processed_transactions(
    process_type: str,
    rows: List[Dict[str, Any]],
) -> int:
    initialize_database()
    prepared = []
    seen = set()
    for row in rows or []:
        key = _daily_transaction_key(process_type, row)
        if key in seen:
            continue
        seen.add(key)
        prepared.append((key, str(process_type).upper(), json.dumps(row, ensure_ascii=False, default=str)))

    sql = r"""
        INSERT INTO dbo.DailyProcessedTransactions
            (TransactionKey, ProcessType, PayloadJson)
        SELECT ?, ?, ?
        WHERE NOT EXISTS
        (
            SELECT 1 FROM dbo.DailyProcessedTransactions WITH (UPDLOCK, HOLDLOCK)
            WHERE TransactionKey = ?
        );
    """
    inserted = 0
    with Database().connect() as connection:
        cursor = connection.cursor()
        try:
            for key, ptype, payload in prepared:
                cursor.execute(sql, (key, ptype, payload, key))
                inserted += max(0, int(cursor.rowcount or 0))
            connection.commit()
        except Exception:
            connection.rollback()
            raise
    return inserted


def get_daily_processed_transactions(process_type: str) -> pd.DataFrame:
    initialize_database()
    sql = r"""
        SELECT PayloadJson
        FROM dbo.DailyProcessedTransactions
        WHERE ProcessType = ?
        ORDER BY CreatedAt;
    """
    with Database().connect() as connection:
        rows = connection.cursor().execute(sql, (str(process_type).upper(),)).fetchall()
    records: List[Dict[str, Any]] = []
    for row in rows:
        try:
            value = json.loads(row[0]) if row[0] else {}
            if isinstance(value, dict):
                records.append(value)
        except (TypeError, ValueError, json.JSONDecodeError):
            continue
    return pd.DataFrame(records)
