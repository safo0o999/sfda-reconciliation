import json
import logging
import os
import time
import threading
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Sequence, Set, Tuple

import pandas as pd
import pyodbc


logger = logging.getLogger("SFDA-Reconciliation.Database")

_EVENT_KEY_LOOKUP_BATCH_SIZE = 1000
_BULK_INSERT_BATCH_SIZE = 10000


class Database:
    """Azure SQL connection provider for SFDA Reconciliation Version 6."""

    def __init__(self):
        connection_string = os.getenv("SQL_CONNECTION_STRING", "").strip()

        if not connection_string:
            raise RuntimeError("SQL_CONNECTION_STRING is missing.")

        self.connection_string = connection_string

    def connect(self):
        from engine.warehouse_context import current_warehouse_id

        warehouse_id = int(current_warehouse_id())
        if warehouse_id < 1:
            raise RuntimeError(
                "A valid WarehouseID is required before opening the SQL connection."
            )

        connection = pyodbc.connect(self.connection_string, autocommit=False)
        cursor = connection.cursor()
        cursor.execute(
            "EXEC sys.sp_set_session_context @key=N'WarehouseID', @value=?;",
            warehouse_id,
        )

        applied_warehouse_id = cursor.execute(
            "SELECT TRY_CONVERT(int, SESSION_CONTEXT(N'WarehouseID'));"
        ).fetchone()[0]

        if int(applied_warehouse_id or 0) != warehouse_id:
            connection.close()
            raise RuntimeError(
                "SQL warehouse session context could not be established safely."
            )

        return connection


def _load_schema_sql() -> str:
    """Load the idempotent Version 6 SQL schema from the project SQL folder."""

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


_DATABASE_READY = False
_DATABASE_READY_LOCK = threading.Lock()


def run_database_migrations() -> None:
    """
    Execute the full Version 6 schema migration.

    This is an administration/deployment operation only. Normal API requests
    must never execute the full 001_initial_schema.sql file.
    """
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
        "Database migration completed in %.2f seconds.",
        time.perf_counter() - started_at,
    )


def initialize_database() -> None:
    """
    Lightweight runtime readiness check.

    Existing repository functions still call initialize_database() for
    compatibility, but this function now performs only a one-time metadata
    validation per Function worker. It no longer runs the full schema
    migration during normal reads/writes.
    """
    global _DATABASE_READY

    if _DATABASE_READY:
        return

    with _DATABASE_READY_LOCK:
        if _DATABASE_READY:
            return

        started_at = time.perf_counter()
        required_tables = (
            "BatchMaster",
            "ReconciliationRuns",
            "LatestSFDASnapshot",
            "LatestInventorySnapshot",
            "Warehouses",
            "ApplicationUsers",
            "AuthSessions",
        )

        with Database().connect() as connection:
            cursor = connection.cursor()
            missing = []
            for table_name in required_tables:
                exists = cursor.execute(
                    "SELECT CASE WHEN OBJECT_ID(?, N'U') IS NULL THEN 0 ELSE 1 END;",
                    (f"dbo.{table_name}",),
                ).fetchone()[0]
                if not int(exists or 0):
                    missing.append(table_name)

        if missing:
            raise RuntimeError(
                "Version 6 database migration is incomplete. Missing table(s): "
                + ", ".join(missing)
            )

        _DATABASE_READY = True
        logger.info(
            "Database runtime readiness check completed in %.3f seconds.",
            time.perf_counter() - started_at,
        )


def verify_auth_schema() -> None:
    """
    Verify that the Version 6 authentication / warehouse foundation exists.

    This is intentionally a lightweight metadata check. It MUST NOT execute
    the full 001_initial_schema.sql migration during a normal web request.
    Database migrations are deployment/administration work, not part of
    sign-in or registration.
    """

    required_tables = (
        "Warehouses",
        "ApplicationUsers",
        "AuthSessions",
    )

    required_columns = {
        "Warehouses": ("WarehouseID", "WarehouseName", "Status"),
        "ApplicationUsers": (
            "UserID",
            "Email",
            "PasswordSalt",
            "PasswordHash",
            "Role",
            "Status",
            "WarehouseID",
            "RequestedWarehouseName",
        ),
        "AuthSessions": ("SessionID", "UserID", "TokenHash", "ExpiresAt"),
    }

    with Database().connect() as connection:
        cursor = connection.cursor()

        missing_tables = []
        for table_name in required_tables:
            exists = cursor.execute(
                "SELECT CASE WHEN OBJECT_ID(?, N'U') IS NULL THEN 0 ELSE 1 END;",
                (f"dbo.{table_name}",),
            ).fetchone()[0]
            if not int(exists or 0):
                missing_tables.append(table_name)

        if missing_tables:
            raise RuntimeError(
                "Version 6 authentication database migration is incomplete. "
                "Missing table(s): " + ", ".join(missing_tables)
            )

        missing_columns = []
        for table_name, columns in required_columns.items():
            for column_name in columns:
                exists = cursor.execute(
                    """
                    SELECT COUNT(*)
                    FROM sys.columns
                    WHERE object_id = OBJECT_ID(?)
                      AND name = ?;
                    """,
                    (f"dbo.{table_name}", column_name),
                ).fetchone()[0]
                if not int(exists or 0):
                    missing_columns.append(f"{table_name}.{column_name}")

        if missing_columns:
            raise RuntimeError(
                "Version 6 authentication database migration is incomplete. "
                "Missing column(s): " + ", ".join(missing_columns)
            )

        madinah = cursor.execute(
            """
            SELECT TOP (1) WarehouseID
            FROM dbo.Warehouses
            WHERE WarehouseCode=N'MADINAH'
               OR WarehouseID=1
            ORDER BY CASE WHEN WarehouseCode=N'MADINAH' THEN 0 ELSE 1 END,
                     WarehouseID;
            """
        ).fetchone()

        if not madinah:
            raise RuntimeError(
                "Version 6 authentication database migration is incomplete. "
                "Madinah Warehouse bootstrap record is missing."
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


def _warehouse_scoped_key(value: str) -> str:
    """Namespace deterministic keys without exceeding legacy SQL key widths.

    Warehouse 1 keeps the historical key format unchanged so existing Madinah
    de-duplication remains stable. For other warehouses, the warehouse identity
    and original key are re-hashed into a fixed 64-character SHA-256 hex key.
    This prevents cross-warehouse PK collisions without lengthening varchar(64)
    / nvarchar(64) key columns.
    """
    import hashlib

    from engine.warehouse_context import current_warehouse_id

    key = str(value or "").strip()
    warehouse_id = int(current_warehouse_id())

    if not key or warehouse_id == 1:
        return key

    return hashlib.sha256(
        f"W{warehouse_id}|{key}".encode("utf-8")
    ).hexdigest()


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
    event_key = _warehouse_scoped_key(_text(row, "Event Key"))
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
    event_key = _warehouse_scoped_key(_text(row, "Event Key"))
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

    # Keep the general historical/batch-master bulk path fast.  Large
    # CustomerHistory / BatchMaster replacements can exceed the HTTP timeout
    # when fast_executemany is disabled globally.
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
    """Return cumulative receipt and dispatch summaries for Batch Master.

    Batch Master receipt totals include only the three physical WMS receipt
    classes relevant to warehouse stock history:
      * TRK5060 - Supplier
      * TRK800  - STO incoming
      * TRK49   - STO return

    TRK30 Customer Return, TRK43 Reservation and TRK74 Principal are retained
    in ReceiptEvents for audit only and never enter Batch Master quantities.
    Unknown TRK prefixes are also excluded until classified explicitly.
    """

    initialize_database()
    started_at = time.perf_counter()

    receipt_sql = r"""
        WITH EligibleReceipt AS
        (
            SELECT *
            FROM dbo.ReceiptEvents
            WHERE
                UPPER(LTRIM(RTRIM(ISNULL(InboundShipment, '')))) LIKE 'TRK5060%'
                OR UPPER(LTRIM(RTRIM(ISNULL(InboundShipment, '')))) LIKE 'TRK800%'
                OR UPPER(LTRIM(RTRIM(ISNULL(InboundShipment, '')))) LIKE 'TRK49%'
        ),
        ReceiptAggregate AS
        (
            SELECT
                BN,
                ExpiryMonthKey,
                GenericItemNumber,
                MAX(ExpiryDate) AS ReceiptExpiryDate,
                COUNT_BIG(*) AS ReceiveRuns,
                SUM(ReceivedQuantity) AS TotalReceiveQty,
                MIN(ReceivedDate) AS FirstReceivedDate,
                MAX(ReceivedDate) AS LastReceivedDate
            FROM EligibleReceipt
            GROUP BY BN, ExpiryMonthKey, GenericItemNumber
        )
        SELECT
            a.BN,
            a.ExpiryMonthKey AS [Expiry Month Key],
            a.ReceiptExpiryDate AS [Receipt Expiry Date],
            a.GenericItemNumber AS [Generic Item Number],
            s.TradeItemNumber AS [Trade Item Number],
            s.TradeName AS [Trade Name],
            s.Description AS [Description],
            s.SupplierName AS [Supplier Name],
            s.SupplierCode AS [Supplier Code],
            s.ItemFamilyGroup AS [Item Family Group],
            a.ReceiveRuns AS [Receive Runs],
            a.TotalReceiveQty AS [Total Receive Qty],
            a.FirstReceivedDate AS [First Received Date],
            a.LastReceivedDate AS [Last Received Date]
        FROM ReceiptAggregate a
        OUTER APPLY
        (
            SELECT TOP (1)
                r.TradeItemNumber,
                r.TradeName,
                r.Description,
                r.SupplierName,
                r.SupplierCode,
                r.ItemFamilyGroup
            FROM EligibleReceipt r
            WHERE r.BN = a.BN
              AND r.ExpiryMonthKey = a.ExpiryMonthKey
              AND r.GenericItemNumber = a.GenericItemNumber
            ORDER BY
                CASE
                    WHEN UPPER(LTRIM(RTRIM(ISNULL(r.InboundShipment, '')))) LIKE 'TRK5060%'
                    THEN 0
                    WHEN UPPER(LTRIM(RTRIM(ISNULL(r.InboundShipment, '')))) LIKE 'TRK800%'
                    THEN 1
                    ELSE 2
                END,
                r.ReceivedDate ASC,
                r.EventKey ASC
        ) s;
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
        GROUP BY BN, ExpiryMonthKey, GenericItemNumber;
    """

    with Database().connect() as connection:
        receipt = pd.read_sql(receipt_sql, connection)
        dispatch = pd.read_sql(dispatch_sql, connection)

    logger.info(
        "Historical event summaries loaded in %.2f seconds. receipt_groups=%s dispatch_groups=%s",
        time.perf_counter() - started_at,
        len(receipt),
        len(dispatch),
    )
    return receipt, dispatch


def get_history_summaries() -> Tuple[pd.DataFrame, pd.DataFrame]:
    """Return Supplier History and Customer History at their operational grains.

    Supplier History is intentionally TRK5060-only. This guarantees that
    Supplier Variance can never be inflated by STO, returns, reservations or
    principal movements.
    """

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
        WHERE UPPER(LTRIM(RTRIM(ISNULL(InboundShipment, '')))) LIKE 'TRK5060%'
        GROUP BY SupplierName, SupplierCode, BN, ExpiryMonthKey, GenericItemNumber;
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
        GROUP BY ToAddress, BN, ExpiryMonthKey, GenericItemNumber;
    """

    with Database().connect() as connection:
        supplier = pd.read_sql(supplier_sql, connection)
        customer = pd.read_sql(customer_sql, connection)
    return supplier, customer


def _get_sto_receipt_history(
    prefix: str,
    *,
    sfda_relevant_only: bool = False,
) -> pd.DataFrame:
    """Return aggregated STO receipt evidence enriched from Batch Master.

    STO Incoming (TRK800) must not expose every physical inter-warehouse receipt.
    When ``sfda_relevant_only=True`` a receipt is kept only when either:

      1. its exact BN + Expiry Month exists in Batch Master; OR
      2. its Generic Item Number exists anywhere in Batch Master.

    Batch Master is already the platform's SFDA-relevant historical universe, so
    this makes it the eligibility filter for STO Incoming while preserving the
    actual STO batch number from ReceiptEvents.

    If the exact STO batch is not in Batch Master but the Generic Item Number is,
    the row is retained as a generic-level SFDA-relevant batch. Drug/GTIN/package
    enrichment is taken from the best available Batch Master row for that generic,
    preferring an exact BN + Expiry match when one exists.
    """

    initialize_database()
    safe_prefix = str(prefix or "").strip().upper()
    if safe_prefix not in {"TRK800", "TRK49"}:
        raise ValueError("Unsupported STO receipt prefix.")

    relevance_where = ""
    if sfda_relevant_only:
        relevance_where = r"""
        WHERE EXISTS
        (
            SELECT 1
            FROM dbo.BatchMaster bm_filter
            WHERE
                (
                    bm_filter.BN = r.BN
                    AND bm_filter.ExpiryMonthKey = r.ExpiryMonthKey
                )
                OR
                (
                    NULLIF(LTRIM(RTRIM(r.GenericItemNumber)), '') IS NOT NULL
                    AND bm_filter.GenericItemNumber = r.GenericItemNumber
                )
        )
        """

    sql = rf"""
        WITH Receipt AS
        (
            SELECT
                InboundShipment,
                SupplierName,
                SupplierCode,
                BN,
                ExpiryMonthKey,
                MAX(ExpiryDate) AS ExpiryDate,
                GenericItemNumber,
                MAX(NULLIF(TradeItemNumber, '')) AS TradeItemNumber,
                MAX(NULLIF(TradeName, '')) AS TradeName,
                MAX(NULLIF(Description, '')) AS Description,
                MAX(NULLIF(ItemFamilyGroup, '')) AS ItemFamilyGroup,
                SUM(ReceivedQuantity) AS ReceivedQuantityEach,
                MIN(ReceivedDate) AS FirstReceivedDate,
                MAX(ReceivedDate) AS LastReceivedDate
            FROM dbo.ReceiptEvents
            WHERE UPPER(LTRIM(RTRIM(ISNULL(InboundShipment, '')))) LIKE ?
            GROUP BY
                InboundShipment,
                SupplierName,
                SupplierCode,
                BN,
                ExpiryMonthKey,
                GenericItemNumber
        )
        SELECT
            r.InboundShipment AS [Inbound Shipment],
            r.SupplierName AS [Source Warehouse],
            r.SupplierCode AS [Source Warehouse Code],
            r.BN,
            r.ExpiryMonthKey AS [Expiry Month Key],
            COALESCE(b.ExpiryDate, r.ExpiryDate) AS [Expiry Date],
            r.GenericItemNumber AS [Generic Item Number],
            COALESCE(NULLIF(b.GTIN, ''), '') AS GTIN,
            COALESCE(NULLIF(b.DrugName, ''), '') AS [Drug Name],
            COALESCE(NULLIF(b.TradeName, ''), r.TradeName, '') AS [Trade Description],
            r.Description,
            r.ItemFamilyGroup AS [Item Family Group],
            COALESCE(b.PackageSize, 0) AS PackageSize,
            r.ReceivedQuantityEach AS [Received Quantity Each],
            CASE
                WHEN COALESCE(b.PackageSize, 0) > 0
                THEN r.ReceivedQuantityEach / b.PackageSize
                ELSE 0
            END AS [Received Quantity Pack],
            r.FirstReceivedDate AS [First Received Date],
            r.LastReceivedDate AS [Last Received Date],
            CASE
                WHEN b.BN = r.BN
                 AND b.ExpiryMonthKey = r.ExpiryMonthKey
                    THEN 'Exact Batch in SFDA-Relevant Master'
                WHEN b.GenericItemNumber = r.GenericItemNumber
                    THEN 'Generic Exists - STO Batch Missing from SFDA'
                ELSE ''
            END AS [SFDA Match Status]
        FROM Receipt r
        OUTER APPLY
        (
            SELECT TOP (1)
                bm.BN,
                bm.ExpiryMonthKey,
                bm.ExpiryDate,
                bm.GenericItemNumber,
                bm.GTIN,
                bm.DrugName,
                bm.TradeName,
                bm.PackageSize,
                bm.GenericExistsInSFDA
            FROM dbo.BatchMaster bm
            WHERE
                (
                    bm.BN = r.BN
                    AND bm.ExpiryMonthKey = r.ExpiryMonthKey
                )
                OR
                (
                    NULLIF(LTRIM(RTRIM(r.GenericItemNumber)), '') IS NOT NULL
                    AND bm.GenericItemNumber = r.GenericItemNumber
                )
            ORDER BY
                CASE
                    WHEN bm.BN = r.BN
                     AND bm.ExpiryMonthKey = r.ExpiryMonthKey
                    THEN 0 ELSE 1
                END,
                bm.LastUpdated DESC,
                bm.BN
        ) b
        {relevance_where}
        ORDER BY
            r.LastReceivedDate,
            r.BN,
            r.GenericItemNumber;
    """

    with Database().connect() as connection:
        return pd.read_sql(
            sql,
            connection,
            params=(safe_prefix + "%",),
        )


def get_sto_incoming_history_df() -> pd.DataFrame:
    # STO Incoming is an RSD follow-up list, not a complete physical-transfer
    # archive. Show only SFDA-relevant batches (exact SFDA batch or equivalent
    # Generic Item Number proven in SFDA through Batch Master).
    return _get_sto_receipt_history(
        "TRK800",
        sfda_relevant_only=True,
    )


def get_sto_return_history_df() -> pd.DataFrame:
    """Return only SFDA-relevant STO Return (TRK49) cancellation actions.

    Keep a TRK49 return only when either the exact BN + Expiry Month exists in
    Batch Master, or the Generic Item Number exists anywhere in Batch Master.
    Unrelated physical returns must never appear in the RSD cancel-dispatch list.
    """
    frame = _get_sto_receipt_history(
        "TRK49",
        sfda_relevant_only=True,
    )
    if frame.empty:
        frame["Required Action"] = pd.Series(dtype=object)
        return frame
    frame["Required Action"] = "Cancel Previous RSD Dispatch"
    return frame


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
        frame = pd.read_sql(r"""
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

    # GLN is reference data, not historical movement. Overlay the currently
    # approved warehouse mapping at read time so legacy cross-warehouse GLNs
    # can never leak into Full Dispatch, Product Intelligence or Variance.
    from engine.reference_data import apply_current_warehouse_gln
    return apply_current_warehouse_gln(frame)


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
            ExpiryMonthKey AS [Expiry Month Key],
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
    """Delete cumulative historical tables for the current warehouse only.

    Database connections always carry WarehouseID session context and Version 6
    RLS isolates these tables. This function is used only by Historical Build
    operation=rebuild and does not touch users, warehouse configuration, GLN or
    reference data.
    """

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
    """Return lightweight warehouse-scoped historical readiness/dashboard coverage.

    The previous query relied only on RLS predicates. Explicit WarehouseID filters
    let SQL Server use the Version 6 composite indexes directly and make Home
    refreshes predictable as historical data grows.
    """

    initialize_database()
    from engine.warehouse_context import current_warehouse_id

    warehouse_id = int(current_warehouse_id())
    sql = r"""
        SELECT
            (SELECT COUNT_BIG(*) FROM dbo.BatchMaster WHERE WarehouseID = ?) AS BatchMasterRows,
            (SELECT COUNT_BIG(*) FROM dbo.SupplierHistory WHERE WarehouseID = ?) AS SupplierHistoryRows,
            (SELECT COUNT_BIG(*) FROM dbo.CustomerHistory WHERE WarehouseID = ?) AS CustomerHistoryRows,
            (SELECT MAX(LastUpdated) FROM dbo.BatchMaster WHERE WarehouseID = ?) AS LastBuildUtc,
            (SELECT COUNT_BIG(*) FROM dbo.LatestSFDASnapshot WHERE WarehouseID = ?) AS TotalSFDABatches,
            (
                SELECT COUNT_BIG(*)
                FROM (
                    SELECT s.BN, s.ExpiryMonthKey, s.GenericItemNumber
                    FROM dbo.SupplierHistory s
                    LEFT JOIN dbo.LatestSFDASnapshot f
                      ON f.WarehouseID = s.WarehouseID
                     AND f.BN = s.BN
                     AND f.ExpiryMonthKey = s.ExpiryMonthKey
                    WHERE s.WarehouseID = ? AND f.BN IS NULL
                    GROUP BY s.BN, s.ExpiryMonthKey, s.GenericItemNumber
                ) missing_supplier
            ) AS MissingSupplierBatches,
            (
                SELECT COUNT_BIG(*)
                FROM (
                    SELECT r.BN, r.ExpiryMonthKey, r.GenericItemNumber
                    FROM dbo.ReceiptEvents r
                    LEFT JOIN dbo.LatestSFDASnapshot f
                      ON f.WarehouseID = r.WarehouseID
                     AND f.BN = r.BN
                     AND f.ExpiryMonthKey = r.ExpiryMonthKey
                    WHERE
                        r.WarehouseID = ?
                        AND UPPER(LTRIM(RTRIM(ISNULL(r.InboundShipment, '')))) LIKE 'TRK800%'
                        AND EXISTS
                        (
                            SELECT 1
                            FROM dbo.BatchMaster bm_filter
                            WHERE bm_filter.WarehouseID = r.WarehouseID
                              AND (
                                    (bm_filter.BN = r.BN AND bm_filter.ExpiryMonthKey = r.ExpiryMonthKey)
                                    OR
                                    (
                                        NULLIF(LTRIM(RTRIM(r.GenericItemNumber)), '') IS NOT NULL
                                        AND bm_filter.GenericItemNumber = r.GenericItemNumber
                                    )
                                  )
                        )
                    GROUP BY r.BN, r.ExpiryMonthKey, r.GenericItemNumber
                    HAVING COALESCE(MAX(f.QuantityReceivePending), 0) <= 0
                ) sto_followup
            ) AS STOFollowupBatches,
            (
                SELECT MIN(d.TransactionDate)
                FROM (
                    SELECT ReceivedDate AS TransactionDate FROM dbo.ReceiptEvents WHERE WarehouseID = ?
                    UNION ALL
                    SELECT DispatchDate AS TransactionDate FROM dbo.DispatchEvents WHERE WarehouseID = ?
                ) d
            ) AS HistoricalFrom,
            (
                SELECT MAX(d.TransactionDate)
                FROM (
                    SELECT ReceivedDate AS TransactionDate FROM dbo.ReceiptEvents WHERE WarehouseID = ?
                    UNION ALL
                    SELECT DispatchDate AS TransactionDate FROM dbo.DispatchEvents WHERE WarehouseID = ?
                ) d
            ) AS HistoricalTo;
    """

    params = (warehouse_id,) * 11
    with Database().connect() as connection:
        row = connection.cursor().execute(sql, params).fetchone()

    batch_rows = int(row[0] or 0)
    supplier_rows = int(row[1] or 0)
    customer_rows = int(row[2] or 0)

    return {
        "exists": batch_rows > 0,
        "batch_master_rows": batch_rows,
        "supplier_history_rows": supplier_rows,
        "customer_history_rows": customer_rows,
        "last_build_utc": row[3],
        "total_sfda_batches": int(row[4] or 0),
        "missing_supplier_batches": int(row[5] or 0),
        "sto_followup_batches": int(row[6] or 0),
        "historical_from": row[7],
        "historical_to": row[8],
    }


def get_dashboard_customer_summary(limit: int = 10) -> Dict[str, Any]:
    """Return Home customer/GLN metrics without building Product Intelligence.

    SQL first aggregates CustomerHistory to one row per customer, then the small
    result is overlaid with the current warehouse GLN reference. This preserves
    the exact GLN business rule while avoiding full Batch Master / snapshot reads.
    """

    initialize_database()
    from engine.warehouse_context import current_warehouse_id
    from engine.reference_data import apply_current_warehouse_gln, DUMMY_GLN

    warehouse_id = int(current_warehouse_id())
    sql = r"""
        SELECT
            ToAddress AS [To Address],
            SUM(COALESCE(DispatchQuantityPack, 0)) AS dispatched_pack,
            MAX(LastDispatchDate) AS last_dispatch
        FROM dbo.CustomerHistory
        WHERE WarehouseID = ?
          AND NULLIF(LTRIM(RTRIM(ISNULL(ToAddress, ''))), '') IS NOT NULL
        GROUP BY ToAddress;
    """
    with Database().connect() as connection:
        customers = pd.read_sql(sql, connection, params=[warehouse_id])

    if customers.empty:
        return {
            "summary": {
                "customer_count": 0,
                "customer_with_gln_count": 0,
                "customer_dummy_gln_count": 0,
                "customer_unmapped_count": 0,
            },
            "customers": [],
        }

    customers = apply_current_warehouse_gln(customers)
    gln = customers.get("GLN", pd.Series("", index=customers.index)).fillna("").astype(str).str.strip()
    address = customers["To Address"].fillna("").astype(str).str.strip()
    dummy_mask = gln.str.upper().eq("DUMMY") | gln.eq(DUMMY_GLN)
    mapped_mask = gln.ne("") & ~dummy_mask
    unmapped_mask = gln.eq("")

    customers["dispatched_pack"] = pd.to_numeric(customers["dispatched_pack"], errors="coerce").fillna(0)
    customers = customers.sort_values(["dispatched_pack", "To Address"], ascending=[False, True])

    top_rows = []
    for row in customers.head(max(1, min(int(limit), 50))).to_dict(orient="records"):
        top_rows.append({
            "To Address": row.get("To Address"),
            "GLN": row.get("GLN"),
            "dispatched_pack": row.get("dispatched_pack", 0),
            "last_dispatch": row.get("last_dispatch"),
        })

    return {
        "summary": {
            "customer_count": int(address.replace("", pd.NA).nunique()),
            "customer_with_gln_count": int(address.loc[mapped_mask].replace("", pd.NA).nunique()),
            "customer_dummy_gln_count": int(address.loc[dummy_mask].replace("", pd.NA).nunique()),
            "customer_unmapped_count": int(address.loc[unmapped_mask].replace("", pd.NA).nunique()),
        },
        "customers": top_rows,
    }


def refresh_dashboard_summary_cache() -> Dict[str, Any]:
    """Rebuild the small Home Dashboard cache for the current warehouse.

    This intentionally performs the heavier historical/GLN aggregation only when
    warehouse data changes (Historical/Daily/Full reconciliation or GLN update),
    not every time a user opens or refreshes Home.
    """
    initialize_database()
    from engine.warehouse_context import current_warehouse_id

    warehouse_id = int(current_warehouse_id())
    historical = get_historical_status()
    customer = get_dashboard_customer_summary(limit=10)
    customer_summary = customer.get("summary") or {}
    customers_json = json.dumps(
        customer.get("customers") or [],
        ensure_ascii=False,
        default=str,
    )

    sql = r"""
        MERGE dbo.WarehouseDashboardSummary AS target
        USING (SELECT ? AS WarehouseID) AS source
          ON target.WarehouseID = source.WarehouseID
        WHEN MATCHED THEN
            UPDATE SET
                BatchMasterRows = ?,
                SupplierHistoryRows = ?,
                CustomerHistoryRows = ?,
                LastBuildUtc = ?,
                TotalSFDABatches = ?,
                MissingSupplierBatches = ?,
                STOFollowupBatches = ?,
                HistoricalFrom = ?,
                HistoricalTo = ?,
                CustomerCount = ?,
                CustomersWithGLN = ?,
                DummyGLNCustomers = ?,
                UnmappedGLNCustomers = ?,
                CustomersJson = ?,
                UpdatedAt = SYSUTCDATETIME()
        WHEN NOT MATCHED THEN
            INSERT
            (
                WarehouseID,
                BatchMasterRows,
                SupplierHistoryRows,
                CustomerHistoryRows,
                LastBuildUtc,
                TotalSFDABatches,
                MissingSupplierBatches,
                STOFollowupBatches,
                HistoricalFrom,
                HistoricalTo,
                CustomerCount,
                CustomersWithGLN,
                DummyGLNCustomers,
                UnmappedGLNCustomers,
                CustomersJson,
                UpdatedAt
            )
            VALUES
            (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, SYSUTCDATETIME());
    """

    values = (
        warehouse_id,
        int(historical.get("batch_master_rows") or 0),
        int(historical.get("supplier_history_rows") or 0),
        int(historical.get("customer_history_rows") or 0),
        historical.get("last_build_utc"),
        int(historical.get("total_sfda_batches") or 0),
        int(historical.get("missing_supplier_batches") or 0),
        int(historical.get("sto_followup_batches") or 0),
        historical.get("historical_from"),
        historical.get("historical_to"),
        int(customer_summary.get("customer_count") or 0),
        int(customer_summary.get("customer_with_gln_count") or 0),
        int(customer_summary.get("customer_dummy_gln_count") or 0),
        int(customer_summary.get("customer_unmapped_count") or 0),
        customers_json,
    )
    params = values + values

    with Database().connect() as connection:
        cursor = connection.cursor()
        try:
            cursor.execute(sql, params)
            connection.commit()
        except Exception:
            connection.rollback()
            raise

    return {
        "historical": historical,
        "customer": customer,
    }


def get_cached_dashboard_summary() -> Dict[str, Any]:
    """Return Home Dashboard data from one warehouse-scoped SQL row.

    Existing deployments may have no cached row immediately after the migration;
    in that one-time case the cache is built synchronously. Subsequent Home
    refreshes are a single-row read.
    """
    initialize_database()
    from engine.warehouse_context import current_warehouse_id

    warehouse_id = int(current_warehouse_id())
    sql = r"""
        SELECT
            BatchMasterRows,
            SupplierHistoryRows,
            CustomerHistoryRows,
            LastBuildUtc,
            TotalSFDABatches,
            MissingSupplierBatches,
            STOFollowupBatches,
            HistoricalFrom,
            HistoricalTo,
            CustomerCount,
            CustomersWithGLN,
            DummyGLNCustomers,
            UnmappedGLNCustomers,
            CustomersJson,
            UpdatedAt
        FROM dbo.WarehouseDashboardSummary
        WHERE WarehouseID = ?;
    """

    try:
        with Database().connect() as connection:
            row = connection.cursor().execute(sql, warehouse_id).fetchone()
    except pyodbc.Error as exc:
        # Page-load endpoints must never fall back to historical aggregation.
        # If the cache table is unavailable, return an empty lightweight payload
        # and let a data-changing job rebuild the cache later.
        if "WarehouseDashboardSummary" in str(exc) or "Invalid object name" in str(exc):
            logger.warning("WarehouseDashboardSummary is unavailable during page load.")
            row = None
        else:
            raise

    if row is None:
        return {
            "historical": {
                "exists": False,
                "batch_master_rows": 0,
                "supplier_history_rows": 0,
                "customer_history_rows": 0,
                "last_build_utc": None,
                "total_sfda_batches": 0,
                "missing_supplier_batches": 0,
                "sto_followup_batches": 0,
                "historical_from": None,
                "historical_to": None,
            },
            "customer": {
                "summary": {
                    "customer_count": 0,
                    "customer_with_gln_count": 0,
                    "customer_dummy_gln_count": 0,
                    "customer_unmapped_count": 0,
                },
                "customers": [],
            },
            "updated_at": None,
        }

    try:
        customers = json.loads(row[13]) if row[13] else []
    except (TypeError, ValueError, json.JSONDecodeError):
        customers = []

    batch_rows = int(row[0] or 0)
    return {
        "historical": {
            "exists": batch_rows > 0,
            "batch_master_rows": batch_rows,
            "supplier_history_rows": int(row[1] or 0),
            "customer_history_rows": int(row[2] or 0),
            "last_build_utc": row[3],
            "total_sfda_batches": int(row[4] or 0),
            "missing_supplier_batches": int(row[5] or 0),
            "sto_followup_batches": int(row[6] or 0),
            "historical_from": row[7],
            "historical_to": row[8],
        },
        "customer": {
            "summary": {
                "customer_count": int(row[9] or 0),
                "customer_with_gln_count": int(row[10] or 0),
                "customer_dummy_gln_count": int(row[11] or 0),
                "customer_unmapped_count": int(row[12] or 0),
            },
            "customers": customers if isinstance(customers, list) else [],
        },
        "updated_at": row[14],
    }


def clear_dashboard_summary_cache() -> None:
    """Delete the cached Home row for the current warehouse only."""
    initialize_database()
    from engine.warehouse_context import current_warehouse_id
    warehouse_id = int(current_warehouse_id())

    try:
        with Database().connect() as connection:
            cursor = connection.cursor()
            cursor.execute(
                "DELETE FROM dbo.WarehouseDashboardSummary WHERE WarehouseID = ?;",
                warehouse_id,
            )
            connection.commit()
    except pyodbc.Error as exc:
        if "WarehouseDashboardSummary" in str(exc) or "Invalid object name" in str(exc):
            return
        raise


def reset_current_warehouse_data(warehouse_id: Optional[int] = None) -> Dict[str, Any]:
    """Atomically delete operational data for exactly one warehouse.

    Final reset design:
      * Warehouse/user/reference configuration is preserved.
      * FK relationships are discovered from SQL metadata.
      * Every child row is scoped through its COMPLETE FK path back to a
        WarehouseID-scoped operational root.
      * No alias string replacement is used.
      * Children are deleted before parents.
      * The whole reset is one transaction: any error rolls everything back.
      * Core historical tables are verified empty before COMMIT.
    """
    initialize_database()
    from engine.warehouse_context import current_warehouse_id

    resolved_warehouse_id = int(warehouse_id or current_warehouse_id())
    if resolved_warehouse_id < 1:
        raise RuntimeError("A valid WarehouseID is required for reset.")

    # Business-owned operational roots only.
    # Users, Warehouses, GLN/reference mapping and Pack Size are intentionally absent.
    reset_roots = (
        "DailyDispatchConfirmations",
        "FullDispatchConfirmations",
        "DailyProcessedTransactions",
        "DailyAcceptTransactions",
        "DailyDispatchTransactions",
        "FullDispatchTransactions",
        "DailyAcceptSFDABaseline",
        "DailyDispatchSFDABaseline",
        "FullDispatchSFDABaseline",
        "LatestInventorySnapshot",
        "LatestSFDASnapshot",
        "CustomerHistory",
        "SupplierHistory",
        "BatchMaster",
        "DispatchEvents",
        "ReceiptEvents",
        "HistoricalBuildJobs",
        "OutlookDraftRequests",
        "ReconciliationRuns",
        "RunHistory",
    )

    deleted: Dict[str, int] = {}

    def q(name: str) -> str:
        return "[" + str(name).replace("]", "]]") + "]"

    # Each path edge is:
    # (child_table, parent_table, ((child_col, parent_col), ...))
    PathEdge = Tuple[str, str, Tuple[Tuple[str, str], ...]]

    def compile_scope(path_to_root: Tuple[PathEdge, ...], alias: str = "t") -> str:
        """Compile a warehouse predicate for one table through the full FK path.

        Example:
          VerificationResults -> VerificationRuns -> ReconciliationRuns

        becomes:

          EXISTS (
            SELECT 1 FROM VerificationRuns p0
            WHERE t.VerificationID = p0.VerificationID
              AND EXISTS (
                SELECT 1 FROM ReconciliationRuns p1
                WHERE p0.RunID = p1.RunID
                  AND p1.WarehouseID = ?
              )
          )

        The current table never needs its own WarehouseID column.
        """
        if not path_to_root:
            return f"{alias}.WarehouseID = ?"

        child_table, parent_table, pairs = path_to_root[0]
        parent_alias = f"p{len(path_to_root)}"

        join_sql = " AND ".join(
            f"{alias}.{q(child_col)} = {parent_alias}.{q(parent_col)}"
            for child_col, parent_col in pairs
        )
        if not join_sql:
            raise RuntimeError(
                f"FK path from {child_table} to {parent_table} has no column mapping."
            )

        parent_scope = compile_scope(path_to_root[1:], parent_alias)
        return (
            f"EXISTS (SELECT 1 FROM dbo.{q(parent_table)} {parent_alias} "
            f"WHERE {join_sql} AND {parent_scope})"
        )

    with Database().connect() as connection:
        cursor = connection.cursor()

        try:
            logger.info(
                "WAREHOUSE_RESET_V4 started. WarehouseID=%s",
                resolved_warehouse_id,
            )

            # Build the dbo FK graph:
            # referenced/parent table -> child table -> ordered FK column pairs.
            fk_rows = cursor.execute(
                r"""
                SELECT
                    OBJECT_NAME(fk.referenced_object_id) AS ParentTable,
                    OBJECT_NAME(fk.parent_object_id) AS ChildTable,
                    pc.name AS ChildColumn,
                    rc.name AS ParentColumn,
                    fk.object_id AS ForeignKeyID,
                    fkc.constraint_column_id
                FROM sys.foreign_keys fk
                JOIN sys.foreign_key_columns fkc
                  ON fkc.constraint_object_id = fk.object_id
                JOIN sys.columns pc
                  ON pc.object_id = fk.parent_object_id
                 AND pc.column_id = fkc.parent_column_id
                JOIN sys.columns rc
                  ON rc.object_id = fk.referenced_object_id
                 AND rc.column_id = fkc.referenced_column_id
                WHERE OBJECT_SCHEMA_NAME(fk.parent_object_id) = N'dbo'
                  AND OBJECT_SCHEMA_NAME(fk.referenced_object_id) = N'dbo'
                ORDER BY
                    ParentTable,
                    ChildTable,
                    fk.object_id,
                    fkc.constraint_column_id;
                """
            ).fetchall()

            # Key by (parent, child, FK id) first so multiple independent FKs
            # between the same pair of tables are not accidentally merged.
            raw_edges: Dict[
                Tuple[str, str, int],
                List[Tuple[str, str]],
            ] = {}
            for row in fk_rows:
                parent = str(row[0] or "").strip()
                child = str(row[1] or "").strip()
                fk_id = int(row[4])
                if not parent or not child:
                    continue
                raw_edges.setdefault((parent, child, fk_id), []).append(
                    (str(row[2]), str(row[3]))
                )

            # parent -> list[(child, column_pairs)]
            edges: Dict[str, List[Tuple[str, Tuple[Tuple[str, str], ...]]]] = {}
            for (parent, child, _fk_id), pairs in raw_edges.items():
                edges.setdefault(parent, []).append((child, tuple(pairs)))

            # Keep ordering deterministic for repeatable logs/debugging.
            for parent in list(edges):
                edges[parent] = sorted(edges[parent], key=lambda item: item[0])

            existing_roots: List[str] = []
            for table in reset_roots:
                exists = cursor.execute(
                    "SELECT CASE WHEN OBJECT_ID(?, N'U') IS NULL THEN 0 ELSE 1 END;",
                    (f"dbo.{table}",),
                ).fetchone()[0]
                if int(exists or 0):
                    existing_roots.append(table)

            scoped_roots: List[str] = []
            for table in existing_roots:
                has_wh = cursor.execute(
                    "SELECT CASE WHEN COL_LENGTH(?, N'WarehouseID') IS NULL THEN 0 ELSE 1 END;",
                    (f"dbo.{table}",),
                ).fetchone()[0]
                if int(has_wh or 0):
                    scoped_roots.append(table)
                else:
                    logger.warning(
                        "Warehouse reset root %s has no WarehouseID and will only "
                        "be deleted if reached safely through another scoped root.",
                        table,
                    )

            if not scoped_roots:
                raise RuntimeError(
                    "No WarehouseID-scoped operational root tables were found for reset."
                )

            active_paths: Set[Tuple[str, ...]] = set()

            def delete_tree(
                table: str,
                path_to_root: Tuple[PathEdge, ...],
                ancestry: Tuple[str, ...],
            ) -> None:
                """Delete FK descendants first, always scoped to the same warehouse."""
                if table in ancestry:
                    cycle = " -> ".join(ancestry + (table,))
                    raise RuntimeError(
                        f"Cyclic FK dependency encountered during warehouse reset: {cycle}"
                    )

                current_ancestry = ancestry + (table,)
                path_signature = current_ancestry
                if path_signature in active_paths:
                    return

                active_paths.add(path_signature)
                try:
                    # First delete all children, extending their path back to
                    # the same WarehouseID-scoped root.
                    for child, pairs in edges.get(table, []):
                        child_edge: PathEdge = (child, table, pairs)
                        child_path = (child_edge,) + path_to_root
                        delete_tree(child, child_path, current_ancestry)

                    predicate = compile_scope(path_to_root, "t")
                    sql = f"DELETE t FROM dbo.{q(table)} t WHERE {predicate};"

                    cursor.execute(sql, resolved_warehouse_id)
                    affected = max(0, int(cursor.rowcount or 0))
                    deleted[table] = deleted.get(table, 0) + affected

                    if affected:
                        logger.info(
                            "WAREHOUSE_RESET_V4 deleted %s row(s) from %s.",
                            affected,
                            table,
                        )
                finally:
                    active_paths.discard(path_signature)

            # Process every WarehouseID-scoped root. Descendants may be reached
            # more than once through different roots/FKs; subsequent DELETEs are
            # harmless and preserve correctness.
            for root in scoped_roots:
                delete_tree(root, tuple(), tuple())

            # Keep exactly one zeroed dashboard cache row so Home and Full
            # Reconciliation remain instant after reset.
            cache_exists = cursor.execute(
                "SELECT CASE WHEN OBJECT_ID(N'dbo.WarehouseDashboardSummary', N'U') "
                "IS NULL THEN 0 ELSE 1 END;"
            ).fetchone()[0]

            if int(cache_exists or 0):
                cursor.execute(
                    r"""
                    MERGE dbo.WarehouseDashboardSummary AS target
                    USING (SELECT ? AS WarehouseID) AS source
                      ON target.WarehouseID = source.WarehouseID
                    WHEN MATCHED THEN UPDATE SET
                        BatchMasterRows = 0,
                        SupplierHistoryRows = 0,
                        CustomerHistoryRows = 0,
                        LastBuildUtc = NULL,
                        TotalSFDABatches = 0,
                        MissingSupplierBatches = 0,
                        STOFollowupBatches = 0,
                        HistoricalFrom = NULL,
                        HistoricalTo = NULL,
                        CustomerCount = 0,
                        CustomersWithGLN = 0,
                        DummyGLNCustomers = 0,
                        UnmappedGLNCustomers = 0,
                        CustomersJson = N'[]',
                        UpdatedAt = SYSUTCDATETIME()
                    WHEN NOT MATCHED THEN INSERT
                    (
                        WarehouseID,
                        BatchMasterRows,
                        SupplierHistoryRows,
                        CustomerHistoryRows,
                        LastBuildUtc,
                        TotalSFDABatches,
                        MissingSupplierBatches,
                        STOFollowupBatches,
                        HistoricalFrom,
                        HistoricalTo,
                        CustomerCount,
                        CustomersWithGLN,
                        DummyGLNCustomers,
                        UnmappedGLNCustomers,
                        CustomersJson,
                        UpdatedAt
                    )
                    VALUES
                    (
                        ?, 0, 0, 0, NULL, 0, 0, 0,
                        NULL, NULL, 0, 0, 0, 0, N'[]', SYSUTCDATETIME()
                    );
                    """,
                    resolved_warehouse_id,
                    resolved_warehouse_id,
                )

            # Hard verification before COMMIT.
            core_tables = (
                "BatchMaster",
                "ReceiptEvents",
                "DispatchEvents",
                "SupplierHistory",
                "CustomerHistory",
                "LatestSFDASnapshot",
            )
            remaining: Dict[str, int] = {}

            for table in core_tables:
                exists = cursor.execute(
                    "SELECT CASE WHEN OBJECT_ID(?, N'U') IS NULL THEN 0 ELSE 1 END;",
                    (f"dbo.{table}",),
                ).fetchone()[0]
                if not int(exists or 0):
                    continue

                has_wh = cursor.execute(
                    "SELECT CASE WHEN COL_LENGTH(?, N'WarehouseID') IS NULL THEN 0 ELSE 1 END;",
                    (f"dbo.{table}",),
                ).fetchone()[0]
                if not int(has_wh or 0):
                    raise RuntimeError(
                        f"Reset verification cannot safely scope dbo.{table}; "
                        "WarehouseID column is missing."
                    )

                remaining[table] = int(
                    cursor.execute(
                        f"SELECT COUNT_BIG(*) FROM dbo.{q(table)} WHERE WarehouseID = ?;",
                        resolved_warehouse_id,
                    ).fetchone()[0]
                    or 0
                )

            not_empty = {name: count for name, count in remaining.items() if count}
            if not_empty:
                raise RuntimeError(
                    "Warehouse reset verification failed; rows remain: "
                    + json.dumps(not_empty, ensure_ascii=False)
                )

            connection.commit()

            logger.info(
                "WAREHOUSE_RESET_V4 completed. WarehouseID=%s deleted_rows_total=%s",
                resolved_warehouse_id,
                int(sum(deleted.values())),
            )

        except Exception:
            connection.rollback()
            logger.exception(
                "WAREHOUSE_RESET_V4 failed for WarehouseID=%s; full transaction rolled back.",
                resolved_warehouse_id,
            )
            raise

    return {
        "status": "Completed",
        "warehouse_id": resolved_warehouse_id,
        "deleted_rows": deleted,
        "deleted_rows_total": int(sum(deleted.values())),
        "verification": {
            "core_tables_empty": True,
            "version": "WAREHOUSE_RESET_V4",
        },
    }

def create_historical_build_job(
    job_id: str,
    operation: str,
    input_manifest: Dict[str, Any],
) -> Dict[str, Any]:
    """Create one queued historical-build job."""

    initialize_database()

    from engine.warehouse_context import current_warehouse_id

    warehouse_id = int(current_warehouse_id())

    sql = r"""
        INSERT INTO dbo.HistoricalBuildJobs
        (
            WarehouseID,
            JobID,
            Operation,
            Status,
            Progress,
            CurrentStage,
            InputManifestJson,
            UpdatedAt
        )
        VALUES (?, ?, ?, 'Queued', 0, 'Queued for processing', ?, SYSUTCDATETIME());
    """

    with Database().connect() as connection:
        cursor = connection.cursor()
        try:
            cursor.execute(
                sql,
                (
                    warehouse_id,
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


def list_historical_build_jobs(limit: int = 500) -> List[Dict[str, Any]]:
    """Return historical Full Reconciliation build jobs for the current warehouse."""

    initialize_database()
    from engine.warehouse_context import current_warehouse_id

    warehouse_id = int(current_warehouse_id())
    safe_limit = max(1, min(int(limit), 5000))
    sql = f"""
        SELECT TOP ({safe_limit})
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
        WHERE WarehouseID = ?
        ORDER BY COALESCE(StartedAt, CreatedAt) DESC, CreatedAt DESC;
    """

    with Database().connect() as connection:
        rows = connection.cursor().execute(sql, warehouse_id).fetchall()

    def parse_json(value: Any) -> Dict[str, Any]:
        if not value:
            return {}
        try:
            parsed = json.loads(value)
            return parsed if isinstance(parsed, dict) else {}
        except (TypeError, ValueError, json.JSONDecodeError):
            return {}

    result: List[Dict[str, Any]] = []
    for row in rows:
        result.append(
            {
                "job_id": row[0],
                "operation": row[1] or "",
                "status": row[2] or "",
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
        )
    return result


def sync_batch_master_sfda_snapshot(
    sfda_df: pd.DataFrame,
) -> Dict[str, int]:
    """Synchronize the current SFDA position into persisted Batch Master.

    Full Accept / Full Dispatch use the latest SFDA Drug Count as the regulatory
    source of truth. Product Intelligence already reads the latest SFDA
    snapshot, but Batch Master historically retained the older SFDA values from
    the historical build. This function updates only the SFDA-controlled fields
    while preserving WMS receipt/dispatch history.

    Matching follows the existing Full Reconciliation grain of
    BN + ExpiryMonthKey. The exact ExpiryDate stored in Batch Master is replaced
    with the date from the latest SFDA report so exported Batch_Master.xlsx uses
    the current regulatory expiry date.
    """

    initialize_database()
    if sfda_df is None or sfda_df.empty:
        return {"sfda_rows": 0, "updated_rows": 0}

    from engine.full_reconciliation import FullReconciliationEngine
    from engine.normalizer import Normalizer

    frame = Normalizer.normalize_sfda(sfda_df.copy())
    frame["Expiry Month Key"] = FullReconciliationEngine._month_key(
        frame["Expiry Date"]
    )

    for column in [
        "Quantity",
        "Active",
        "Quantity sent pending",
        "Quantity Receive Pending",
    ]:
        frame[column] = pd.to_numeric(
            frame[column],
            errors="coerce",
        ).fillna(0)

    grouped = (
        frame.groupby(["BN", "Expiry Month Key"], dropna=False)
        .agg(
            **{
                "Expiry Date": ("Expiry Date", "first"),
                "GTIN": ("GTIN", "first"),
                "Drug Name": ("Drug Name", "first"),
                "Quantity": ("Quantity", "sum"),
                "Active": ("Active", "sum"),
                "Quantity sent pending": ("Quantity sent pending", "sum"),
                "Quantity Receive Pending": (
                    "Quantity Receive Pending",
                    "sum",
                ),
            }
        )
        .reset_index()
    )

    rows = [
        (
            _value(row, "Expiry Date"),
            _text(row, "GTIN"),
            _text(row, "Drug Name"),
            _number(row, "Quantity"),
            _number(row, "Active"),
            _number(row, "Quantity sent pending"),
            _number(row, "Quantity Receive Pending"),
            _text(row, "BN"),
            _text(row, "Expiry Month Key"),
        )
        for row in grouped.to_dict(orient="records")
        if _text(row, "BN") and _text(row, "Expiry Month Key")
    ]

    if not rows:
        return {"sfda_rows": int(len(grouped)), "updated_rows": 0}

    sql = r"""
        UPDATE dbo.BatchMaster
        SET
            ExpiryDate = ?,
            GTIN = ?,
            DrugName = ?,
            SFDAQuantity = ?,
            Active = ?,
            QuantitySentPending = ?,
            QuantityReceivePending = ?,
            LastUpdated = SYSUTCDATETIME()
        WHERE BN = ?
          AND ExpiryMonthKey = ?;
    """

    updated = 0
    with Database().connect() as connection:
        cursor = connection.cursor()
        try:
            # Regular executemany avoids ODBC variable-string buffer inference
            # issues previously seen on snapshot persistence.
            cursor.fast_executemany = False
            for row in rows:
                cursor.execute(sql, row)
                updated += max(0, int(cursor.rowcount or 0))
            connection.commit()
        except Exception:
            connection.rollback()
            raise

    return {
        "sfda_rows": int(len(grouped)),
        "updated_rows": int(updated),
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
    """Persist one archived run file while preserving the legacy RunID FK.

    Older Version 5 databases require ReconciliationRunFiles.RunID to be NOT NULL.
    RunNumber remains the public lookup key, so resolve the matching RunID inside the
    same SQL statement before inserting the file row.
    """
    initialize_database()
    sql = r"""
        INSERT INTO dbo.ReconciliationRunFiles
        (
            RunID, RunNumber, FileCategory, FileName, FileType, ContainerName,
            BlobName, ContentType, SizeBytes, ETag
        )
        SELECT
            r.RunID, ?, ?, ?, ?, ?, ?, ?, ?, ?
        FROM dbo.ReconciliationRuns AS r
        WHERE r.RunNumber = ?;

        IF @@ROWCOUNT = 0
        BEGIN
            THROW 50002, 'Reconciliation run was not found for archived file.', 1;
        END;
    """
    with Database().connect() as connection:
        cursor = connection.cursor()
        try:
            cursor.execute(
                sql,
                (
                    str(run_number), str(file_category), str(file_name), str(file_type),
                    str(container_name), str(blob_name), str(content_type),
                    int(size_bytes or 0), str(etag or ""), str(run_number),
                ),
            )
            connection.commit()
        except Exception:
            connection.rollback()
            raise


def list_reconciliation_runs(limit: int = 500) -> List[Dict[str, Any]]:
    initialize_database()
    from engine.warehouse_context import current_warehouse_id

    warehouse_id = int(current_warehouse_id())
    safe_limit = max(1, min(int(limit), 5000))
    sql = f"""
        SELECT TOP ({safe_limit})
            RunID, RunNumber, ProcessType, Status, StartedAt, CompletedAt,
            SubmittedBy, ASNFiles, InventoryFiles, DispatchFiles, SFDAFiles,
            TotalInputRows, MasterRecords, AcceptRecords, DispatchRecords,
            ExceptionRecords, GeneratedFiles, ApplicationVersion, ErrorMessage
        FROM dbo.ReconciliationRuns
        WHERE WarehouseID = ?
        ORDER BY StartedAt DESC, RunID DESC;
    """
    with Database().connect() as connection:
        cursor = connection.cursor()
        rows = cursor.execute(sql, warehouse_id).fetchall()
        names = [column[0] for column in cursor.description]
    return [dict(zip(names, row)) for row in rows]


def get_reconciliation_run(run_number: str) -> Optional[Dict[str, Any]]:
    initialize_database()
    from engine.warehouse_context import current_warehouse_id

    warehouse_id = int(current_warehouse_id())
    sql = r"""
        SELECT TOP (1)
            RunID, RunNumber, ProcessType, Status, StartedAt, CompletedAt,
            SubmittedBy, ASNFiles, InventoryFiles, DispatchFiles, SFDAFiles,
            TotalInputRows, MasterRecords, AcceptRecords, DispatchRecords,
            ExceptionRecords, GeneratedFiles, ApplicationVersion, ErrorMessage
        FROM dbo.ReconciliationRuns
        WHERE WarehouseID = ? AND RunNumber = ?
        ORDER BY RunID DESC;
    """
    with Database().connect() as connection:
        cursor = connection.cursor()
        row = cursor.execute(sql, (warehouse_id, str(run_number))).fetchone()
        if row is None:
            return None
        names = [column[0] for column in cursor.description]
    return dict(zip(names, row))


def list_reconciliation_run_files(run_number: str) -> List[Dict[str, Any]]:
    initialize_database()
    from engine.warehouse_context import current_warehouse_id

    warehouse_id = int(current_warehouse_id())
    sql = r"""
        SELECT f.RunFileID, f.RunID, f.RunNumber, f.FileCategory, f.FileName, f.FileType,
               f.ContainerName, f.BlobName, f.ContentType, f.SizeBytes, f.ETag, f.CreatedAt
        FROM dbo.ReconciliationRunFiles AS f
        INNER JOIN dbo.ReconciliationRuns AS r
            ON r.RunID = f.RunID
        WHERE r.WarehouseID = ? AND f.RunNumber = ?
        ORDER BY f.CreatedAt, f.RunFileID;
    """
    with Database().connect() as connection:
        cursor = connection.cursor()
        rows = cursor.execute(sql, (warehouse_id, str(run_number))).fetchall()
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
    return _warehouse_scoped_key(hashlib.sha256("|".join(parts).encode("utf-8")).hexdigest())


def _daily_processed_transaction_columns(
    connection: pyodbc.Connection,
) -> Set[str]:
    """Return the physical columns available on the daily de-duplication table.

    Older deployments used ``TransactionType`` while Version 5 uses
    ``ProcessType``.  Production databases can therefore contain either name
    or both names after an in-place schema upgrade.
    """
    rows = connection.cursor().execute(
        r"""
        SELECT c.name
        FROM sys.columns AS c
        WHERE c.object_id = OBJECT_ID(N'dbo.DailyProcessedTransactions');
        """
    ).fetchall()
    return {str(row[0]) for row in rows}


def save_daily_processed_transactions(
    process_type: str,
    rows: List[Dict[str, Any]],
) -> int:
    """Persist daily transaction identities without breaking legacy schemas.

    The same process value is written to both ``ProcessType`` and the legacy
    ``TransactionType`` column whenever both exist.  This is required because
    some existing Azure SQL databases still enforce ``TransactionType`` as
    NOT NULL.
    """
    initialize_database()
    prepared = []
    seen = set()
    normalized_process_type = str(process_type).upper()

    for row in rows or []:
        key = _daily_transaction_key(normalized_process_type, row)
        if key in seen:
            continue
        seen.add(key)
        prepared.append(
            (
                key,
                normalized_process_type,
                json.dumps(row, ensure_ascii=False, default=str),
            )
        )

    if not prepared:
        return 0

    inserted = 0
    with Database().connect() as connection:
        cursor = connection.cursor()
        columns = _daily_processed_transaction_columns(connection)

        has_process_type = "ProcessType" in columns
        has_transaction_type = "TransactionType" in columns

        if not has_process_type and not has_transaction_type:
            raise RuntimeError(
                "DailyProcessedTransactions is missing both ProcessType and "
                "TransactionType columns."
            )

        if has_process_type and has_transaction_type:
            sql = r"""
                INSERT INTO dbo.DailyProcessedTransactions
                    (TransactionKey, ProcessType, TransactionType, PayloadJson)
                SELECT ?, ?, ?, ?
                WHERE NOT EXISTS
                (
                    SELECT 1
                    FROM dbo.DailyProcessedTransactions WITH (UPDLOCK, HOLDLOCK)
                    WHERE TransactionKey = ?
                );
            """
        elif has_process_type:
            sql = r"""
                INSERT INTO dbo.DailyProcessedTransactions
                    (TransactionKey, ProcessType, PayloadJson)
                SELECT ?, ?, ?
                WHERE NOT EXISTS
                (
                    SELECT 1
                    FROM dbo.DailyProcessedTransactions WITH (UPDLOCK, HOLDLOCK)
                    WHERE TransactionKey = ?
                );
            """
        else:
            sql = r"""
                INSERT INTO dbo.DailyProcessedTransactions
                    (TransactionKey, TransactionType, PayloadJson)
                SELECT ?, ?, ?
                WHERE NOT EXISTS
                (
                    SELECT 1
                    FROM dbo.DailyProcessedTransactions WITH (UPDLOCK, HOLDLOCK)
                    WHERE TransactionKey = ?
                );
            """

        try:
            for key, ptype, payload in prepared:
                if has_process_type and has_transaction_type:
                    parameters = (key, ptype, ptype, payload, key)
                else:
                    parameters = (key, ptype, payload, key)

                cursor.execute(sql, parameters)
                inserted += max(0, int(cursor.rowcount or 0))

            connection.commit()
        except Exception:
            connection.rollback()
            raise

    return inserted


def get_daily_processed_transactions(process_type: str) -> pd.DataFrame:
    """Read processed daily rows from Version 5 or legacy database schemas."""
    initialize_database()
    normalized_process_type = str(process_type).upper()

    with Database().connect() as connection:
        columns = _daily_processed_transaction_columns(connection)
        has_process_type = "ProcessType" in columns
        has_transaction_type = "TransactionType" in columns

        if has_process_type and has_transaction_type:
            sql = r"""
                SELECT PayloadJson
                FROM dbo.DailyProcessedTransactions
                WHERE UPPER(
                    COALESCE(
                        NULLIF(ProcessType, N''),
                        NULLIF(TransactionType, N'')
                    )
                ) = ?
                ORDER BY CreatedAt;
            """
        elif has_process_type:
            sql = r"""
                SELECT PayloadJson
                FROM dbo.DailyProcessedTransactions
                WHERE UPPER(ProcessType) = ?
                ORDER BY CreatedAt;
            """
        elif has_transaction_type:
            sql = r"""
                SELECT PayloadJson
                FROM dbo.DailyProcessedTransactions
                WHERE UPPER(TransactionType) = ?
                ORDER BY CreatedAt;
            """
        else:
            raise RuntimeError(
                "DailyProcessedTransactions is missing both ProcessType and "
                "TransactionType columns."
            )

        rows = connection.cursor().execute(
            sql,
            (normalized_process_type,),
        ).fetchall()

    records: List[Dict[str, Any]] = []
    for row in rows:
        try:
            value = json.loads(row[0]) if row[0] else {}
            if isinstance(value, dict):
                records.append(value)
        except (TypeError, ValueError, json.JSONDecodeError):
            continue

    return pd.DataFrame(records)

# -----------------------------------------------------------------------------
# SFDA-confirmed daily Accept state
# -----------------------------------------------------------------------------

def _prepare_accept_sfda_state(sfda_df: pd.DataFrame) -> pd.DataFrame:
    """Normalize SFDA Accept proof state to BN + expiry month grain.

    Day differences inside the same expiry month are intentionally ignored.
    Ambiguous BN/month combinations that contain more than one GTIN are excluded
    from automatic confirmation rather than guessed.
    """
    from engine.normalizer import Normalizer

    frame = sfda_df.copy() if sfda_df is not None else pd.DataFrame()
    if frame.empty:
        return pd.DataFrame(columns=[
            "GTIN", "BN", "Expiry Date", "Active", "Quantity Receive Pending"
        ])

    frame = Normalizer.normalize_sfda(frame)
    required = ["GTIN", "BN", "Expiry Date", "Active", "Quantity Receive Pending"]
    missing = [column for column in required if column not in frame.columns]
    if missing:
        raise ValueError(
            "SFDA confirmation state is missing required columns: " + ", ".join(missing)
        )

    frame["BN"] = Normalizer.text(frame["BN"])
    frame["Expiry Date"] = Normalizer.date(frame["Expiry Date"])
    frame["GTIN"] = Normalizer.text(frame["GTIN"])
    frame["Expiry Month Key"] = pd.to_datetime(
        frame["Expiry Date"], errors="coerce"
    ).dt.strftime("%Y-%m").fillna("")
    frame["Active"] = pd.to_numeric(frame["Active"], errors="coerce").fillna(0)
    frame["Quantity Receive Pending"] = pd.to_numeric(
        frame["Quantity Receive Pending"], errors="coerce"
    ).fillna(0)

    gtin_count = (
        frame.loc[frame["GTIN"].ne("")]
        .groupby(["BN", "Expiry Month Key"], dropna=False)["GTIN"]
        .nunique()
        .rename("_GTIN Count")
        .reset_index()
    )
    state = (
        frame.groupby(["BN", "Expiry Month Key"], dropna=False)
        .agg(
            GTIN=("GTIN", "first"),
            Active=("Active", "sum"),
            **{"Quantity Receive Pending": ("Quantity Receive Pending", "sum")},
        )
        .reset_index()
    )
    state = state.merge(
        gtin_count, on=["BN", "Expiry Month Key"], how="left", validate="one_to_one"
    )
    state = state.loc[
        pd.to_numeric(state["_GTIN Count"], errors="coerce").fillna(0).eq(1)
    ].drop(columns=["_GTIN Count"], errors="ignore")
    state["Expiry Date"] = pd.to_datetime(
        state["Expiry Month Key"] + "-01", errors="coerce"
    )
    return state[["GTIN", "BN", "Expiry Date", "Active", "Quantity Receive Pending"]]

def get_accept_confirmed_transactions() -> pd.DataFrame:
    """Return only SFDA-confirmed Accept quantities for daily de-duplication."""
    initialize_database()
    sql = r"""
        SELECT
            TransactionKey AS [Transaction Key],
            ConfirmedQuantityEach AS [Processed Quantity Each],
            LastConfirmedAt AS [Last Processed At]
        FROM dbo.DailyAcceptTransactions
        WHERE ConfirmedQuantityEach > 0;
    """
    with Database().connect() as connection:
        return pd.read_sql(sql, connection)


def save_accept_pending_transactions(
    rows: List[Dict[str, Any]],
    run_number: str,
) -> int:
    """Store quantities represented by a generated Accept file as unconfirmed.

    Re-running the same ASN before SFDA changes does not accumulate duplicate
    pending quantity. Once some quantity is confirmed, a later submission can
    extend the submitted target by only the newly generated amount.
    """
    initialize_database()
    prepared: Dict[str, Dict[str, Any]] = {}
    for row in rows or []:
        key = _text(row, "Transaction Key")
        if not key:
            continue
        each_qty = max(0.0, _number(row, "Processed Quantity Each"))
        pack_qty = max(0.0, _number(row, "Processed Quantity Pack"))
        if each_qty <= 0 and pack_qty <= 0:
            continue
        current = prepared.setdefault(
            key,
            {
                "Transaction Key": key,
                "BN": _text(row, "BN"),
                "Expiry Date": _value(row, "Expiry Date"),
                "ExpiryMonthKey": (
                    ""
                    if pd.isna(pd.to_datetime(_value(row, "Expiry Date"), errors="coerce"))
                    else pd.to_datetime(_value(row, "Expiry Date"), errors="coerce").strftime("%Y-%m")
                ),
                "Generic Item Number": _text(row, "Generic Item Number"),
                "Reference Number": _text(row, "Reference Number"),
                "Reference Line": _text(row, "Reference Line"),
                "Each": 0.0,
                "Pack": 0.0,
            },
        )
        current["Each"] += each_qty
        current["Pack"] += pack_qty

    if not prepared:
        return 0

    sql = r"""
        MERGE dbo.DailyAcceptTransactions WITH (HOLDLOCK) AS target
        USING
        (
            SELECT
                ? AS TransactionKey,
                ? AS BN,
                ? AS ExpiryDate,
                ? AS ExpiryMonthKey,
                ? AS GenericItemNumber,
                ? AS ReferenceNumber,
                ? AS ReferenceLine,
                ? AS NewQuantityEach,
                ? AS NewQuantityPack,
                ? AS RunNumber
        ) AS source
        ON target.TransactionKey = source.TransactionKey
        WHEN MATCHED THEN
            UPDATE SET
                BN = source.BN,
                ExpiryDate = source.ExpiryDate,
                ExpiryMonthKey = source.ExpiryMonthKey,
                GenericItemNumber = source.GenericItemNumber,
                ReferenceNumber = source.ReferenceNumber,
                ReferenceLine = source.ReferenceLine,
                SubmittedQuantityEach = CASE
                    WHEN target.SubmittedQuantityEach >=
                         target.ConfirmedQuantityEach + source.NewQuantityEach
                    THEN target.SubmittedQuantityEach
                    ELSE target.ConfirmedQuantityEach + source.NewQuantityEach
                END,
                SubmittedQuantityPack = CASE
                    WHEN target.SubmittedQuantityPack >=
                         target.ConfirmedQuantityPack + source.NewQuantityPack
                    THEN target.SubmittedQuantityPack
                    ELSE target.ConfirmedQuantityPack + source.NewQuantityPack
                END,
                LastSubmittedRun = source.RunNumber,
                UpdatedAt = SYSUTCDATETIME()
        WHEN NOT MATCHED THEN
            INSERT
            (
                TransactionKey, BN, ExpiryDate, ExpiryMonthKey,
                GenericItemNumber, ReferenceNumber, ReferenceLine,
                SubmittedQuantityEach, SubmittedQuantityPack,
                ConfirmedQuantityEach, ConfirmedQuantityPack,
                FirstSubmittedRun, LastSubmittedRun
            )
            VALUES
            (
                source.TransactionKey, source.BN, source.ExpiryDate,
                source.ExpiryMonthKey, source.GenericItemNumber,
                source.ReferenceNumber,
                source.ReferenceLine, source.NewQuantityEach,
                source.NewQuantityPack, 0, 0, source.RunNumber, source.RunNumber
            );
    """

    saved = 0
    with Database().connect() as connection:
        cursor = connection.cursor()
        try:
            for row in prepared.values():
                cursor.execute(
                    sql,
                    (
                        row["Transaction Key"], row["BN"], row["Expiry Date"],
                        row["ExpiryMonthKey"], row["Generic Item Number"],
                        row["Reference Number"], row["Reference Line"],
                        row["Each"], row["Pack"], str(run_number),
                    ),
                )
                saved += 1
            connection.commit()
        except Exception:
            connection.rollback()
            raise
    return saved


def _replace_accept_sfda_baseline_with_connection(
    connection: pyodbc.Connection,
    state: pd.DataFrame,
    source_file_name: str = "",
) -> int:
    cursor = connection.cursor()
    cursor.execute("DELETE FROM dbo.DailyAcceptSFDABaseline;")
    rows = [
        (
            _text(row, "GTIN"),
            _text(row, "BN"),
            _value(row, "Expiry Date"),
            _number(row, "Active"),
            _number(row, "Quantity Receive Pending"),
            str(source_file_name or ""),
        )
        for row in state.to_dict(orient="records")
        if _text(row, "BN") and _value(row, "Expiry Date") is not None
    ]
    if rows:
        cursor.fast_executemany = True
        cursor.executemany(
            r"""
            INSERT INTO dbo.DailyAcceptSFDABaseline
            (GTIN, BN, ExpiryDate, Active, QuantityReceivePending, SourceFileName)
            VALUES (?, ?, ?, ?, ?, ?);
            """,
            rows,
        )
    return len(rows)


def replace_accept_sfda_baseline(
    sfda_df: pd.DataFrame,
    source_file_name: str = "",
) -> int:
    """Store the current SFDA report as the next Accept confirmation baseline."""
    initialize_database()
    state = _prepare_accept_sfda_state(sfda_df)
    with Database().connect() as connection:
        try:
            count = _replace_accept_sfda_baseline_with_connection(
                connection, state, source_file_name
            )
            connection.commit()
            return count
        except Exception:
            connection.rollback()
            raise


def confirm_accept_transactions_from_sfda(
    sfda_df: pd.DataFrame,
    source_file_name: str = "",
) -> Dict[str, Any]:
    """Confirm prior Accept submissions only when SFDA proves the movement.

    For each BN + Expiry Month, confirmed packs are the conservative minimum of:
      * the decrease in Quantity Receive Pending, and
      * the corresponding increase in Active.

    The evidence is allocated to previously submitted, still-unconfirmed ASN
    transaction identities. The baseline is advanced in the same SQL
    transaction so the same SFDA delta can never be applied twice.
    """
    initialize_database()
    current = _prepare_accept_sfda_state(sfda_df)

    with Database().connect() as connection:
        previous = pd.read_sql(
            r"""
            SELECT
                GTIN, BN, ExpiryDate AS [Expiry Date],
                Active,
                QuantityReceivePending AS [Quantity Receive Pending]
            FROM dbo.DailyAcceptSFDABaseline;
            """,
            connection,
        )

        if previous.empty:
            return {
                "baseline_available": False,
                "confirmed_packs": 0.0,
                "confirmed_each": 0.0,
                "confirmed_transactions": 0,
                "confirmed_batches": 0,
            }

        # SQL DATE values are returned by pyodbc/pandas as Python date/object
        # values, while the freshly normalized SFDA file uses datetime64[ns].
        # Normalize both sides to the exact same merge-key types before
        # comparing the previous SFDA baseline with the newly uploaded report.
        previous["BN"] = previous["BN"].fillna("").astype(str).str.strip()
        current["BN"] = current["BN"].fillna("").astype(str).str.strip()
        previous["Expiry Date"] = pd.to_datetime(
            previous["Expiry Date"],
            errors="coerce",
        ).dt.normalize()
        current["Expiry Date"] = pd.to_datetime(
            current["Expiry Date"],
            errors="coerce",
        ).dt.normalize()

        comparison = previous.merge(
            current,
            on=["BN", "Expiry Date"],
            how="inner",
            suffixes=(" Previous", " Current"),
        )
        if comparison.empty:
            evidence_rows = []
        else:
            comparison["Pending Decrease"] = (
                pd.to_numeric(
                    comparison["Quantity Receive Pending Previous"],
                    errors="coerce",
                ).fillna(0)
                - pd.to_numeric(
                    comparison["Quantity Receive Pending Current"],
                    errors="coerce",
                ).fillna(0)
            ).clip(lower=0)
            comparison["Active Increase"] = (
                pd.to_numeric(
                    comparison["Active Current"], errors="coerce"
                ).fillna(0)
                - pd.to_numeric(
                    comparison["Active Previous"], errors="coerce"
                ).fillna(0)
            ).clip(lower=0)
            comparison["Confirmed Pack Evidence"] = comparison[
                ["Pending Decrease", "Active Increase"]
            ].min(axis=1)
            evidence_rows = comparison.loc[
                comparison["Confirmed Pack Evidence"].gt(0)
            ].to_dict(orient="records")

        cursor = connection.cursor()
        confirmed_pack_total = 0.0
        confirmed_each_total = 0.0
        confirmed_transaction_keys: Set[str] = set()
        confirmed_batches = 0

        try:
            for evidence in evidence_rows:
                bn = _text(evidence, "BN")
                expiry = _value(evidence, "Expiry Date")
                remaining_pack = max(
                    0.0, _number(evidence, "Confirmed Pack Evidence")
                )
                if remaining_pack <= 0:
                    continue

                pending_rows = cursor.execute(
                    r"""
                    SELECT
                        TransactionKey,
                        SubmittedQuantityEach, ConfirmedQuantityEach,
                        SubmittedQuantityPack, ConfirmedQuantityPack
                    FROM dbo.DailyAcceptTransactions WITH (UPDLOCK, HOLDLOCK)
                    WHERE BN = ?
                      AND YEAR(ExpiryDate) = YEAR(?)
                      AND MONTH(ExpiryDate) = MONTH(?)
                      AND SubmittedQuantityPack > ConfirmedQuantityPack
                    ORDER BY CreatedAt, TransactionKey;
                    """,
                    (bn, expiry, expiry),
                ).fetchall()

                batch_confirmed = 0.0
                for pending in pending_rows:
                    if remaining_pack <= 0.0000001:
                        break
                    transaction_key = str(pending[0])
                    submitted_each = float(pending[1] or 0)
                    confirmed_each = float(pending[2] or 0)
                    submitted_pack = float(pending[3] or 0)
                    confirmed_pack = float(pending[4] or 0)
                    open_pack = max(0.0, submitted_pack - confirmed_pack)
                    open_each = max(0.0, submitted_each - confirmed_each)
                    if open_pack <= 0:
                        continue

                    allocate_pack = min(remaining_pack, open_pack)
                    each_per_pack = open_each / open_pack if open_pack > 0 else 0
                    allocate_each = min(open_each, allocate_pack * each_per_pack)

                    cursor.execute(
                        r"""
                        UPDATE dbo.DailyAcceptTransactions
                        SET ConfirmedQuantityPack = ConfirmedQuantityPack + ?,
                            ConfirmedQuantityEach = ConfirmedQuantityEach + ?,
                            LastConfirmedAt = SYSUTCDATETIME(),
                            UpdatedAt = SYSUTCDATETIME()
                        WHERE TransactionKey = ?;
                        """,
                        (allocate_pack, allocate_each, transaction_key),
                    )
                    remaining_pack -= allocate_pack
                    batch_confirmed += allocate_pack
                    confirmed_pack_total += allocate_pack
                    confirmed_each_total += allocate_each
                    confirmed_transaction_keys.add(transaction_key)

                if batch_confirmed > 0:
                    confirmed_batches += 1

            # Advance the proof baseline atomically with the confirmations.
            _replace_accept_sfda_baseline_with_connection(
                connection, current, source_file_name
            )
            connection.commit()
        except Exception:
            connection.rollback()
            raise

    return {
        "baseline_available": True,
        "confirmed_packs": float(confirmed_pack_total),
        "confirmed_each": float(confirmed_each_total),
        "confirmed_transactions": len(confirmed_transaction_keys),
        "confirmed_batches": int(confirmed_batches),
    }

# -----------------------------------------------------------------------------
# SFDA-confirmed daily Dispatch state
# -----------------------------------------------------------------------------

def _prepare_dispatch_sfda_state(sfda_df: pd.DataFrame) -> pd.DataFrame:
    """Normalize SFDA Dispatch proof state to BN + expiry month grain.

    Day differences inside the same expiry month are intentionally ignored.
    Ambiguous BN/month combinations with more than one GTIN are excluded.
    """
    from engine.normalizer import Normalizer

    frame = sfda_df.copy() if sfda_df is not None else pd.DataFrame()
    if frame.empty:
        return pd.DataFrame(columns=[
            "GTIN", "BN", "Expiry Date", "Active", "Quantity sent pending"
        ])

    frame = Normalizer.normalize_sfda(frame)
    required = ["GTIN", "BN", "Expiry Date", "Active", "Quantity sent pending"]
    missing = [column for column in required if column not in frame.columns]
    if missing:
        raise ValueError(
            "SFDA dispatch confirmation state is missing required columns: "
            + ", ".join(missing)
        )

    frame["BN"] = Normalizer.text(frame["BN"])
    frame["Expiry Date"] = Normalizer.date(frame["Expiry Date"])
    frame["GTIN"] = Normalizer.text(frame["GTIN"])
    frame["Expiry Month Key"] = pd.to_datetime(
        frame["Expiry Date"], errors="coerce"
    ).dt.strftime("%Y-%m").fillna("")
    frame["Active"] = pd.to_numeric(frame["Active"], errors="coerce").fillna(0)
    frame["Quantity sent pending"] = pd.to_numeric(
        frame["Quantity sent pending"], errors="coerce"
    ).fillna(0)

    gtin_count = (
        frame.loc[frame["GTIN"].ne("")]
        .groupby(["BN", "Expiry Month Key"], dropna=False)["GTIN"]
        .nunique()
        .rename("_GTIN Count")
        .reset_index()
    )
    state = (
        frame.groupby(["BN", "Expiry Month Key"], dropna=False)
        .agg(
            GTIN=("GTIN", "first"),
            Active=("Active", "sum"),
            **{"Quantity sent pending": ("Quantity sent pending", "sum")},
        )
        .reset_index()
    )
    state = state.merge(
        gtin_count, on=["BN", "Expiry Month Key"], how="left", validate="one_to_one"
    )
    state = state.loc[
        pd.to_numeric(state["_GTIN Count"], errors="coerce").fillna(0).eq(1)
    ].drop(columns=["_GTIN Count"], errors="ignore")
    state["Expiry Date"] = pd.to_datetime(
        state["Expiry Month Key"] + "-01", errors="coerce"
    )
    return state[["GTIN", "BN", "Expiry Date", "Active", "Quantity sent pending"]]

def get_dispatch_confirmed_transactions() -> pd.DataFrame:
    """Return only SFDA-confirmed Dispatch quantities for daily de-duplication."""
    initialize_database()
    sql = r"""
        SELECT
            TransactionKey AS [Transaction Key],
            ConfirmedQuantityEach AS [Processed Quantity Each],
            LastConfirmedAt AS [Last Processed At]
        FROM dbo.DailyDispatchTransactions
        WHERE ConfirmedQuantityEach > 0;
    """
    with Database().connect() as connection:
        return pd.read_sql(sql, connection)


def save_dispatch_pending_transactions(
    rows: List[Dict[str, Any]],
    run_number: str,
) -> int:
    """Store generated Dispatch quantities as pending, not processed."""
    initialize_database()
    prepared: Dict[str, Dict[str, Any]] = {}
    for row in rows or []:
        key = _text(row, "Transaction Key")
        if not key:
            continue
        each_qty = max(0.0, _number(row, "Processed Quantity Each"))
        pack_qty = max(0.0, _number(row, "Processed Quantity Pack"))
        if each_qty <= 0 and pack_qty <= 0:
            continue
        current = prepared.setdefault(
            key,
            {
                "Transaction Key": key,
                "BN": _text(row, "BN"),
                "Expiry Date": _value(row, "Expiry Date"),
                "Generic Item Number": _text(row, "Generic Item Number"),
                "Reference Number": _text(row, "Reference Number"),
                "Reference Line": _text(row, "Reference Line"),
                "To Address": _text(row, "To Address"),
                "Transaction Date": _value(row, "Transaction Date"),
                "Each": 0.0,
                "Pack": 0.0,
            },
        )
        current["Each"] += each_qty
        current["Pack"] += pack_qty

    if not prepared:
        return 0

    sql = r"""
        MERGE dbo.DailyDispatchTransactions WITH (HOLDLOCK) AS target
        USING
        (
            SELECT
                ? AS TransactionKey, ? AS BN, ? AS ExpiryDate,
                ? AS GenericItemNumber, ? AS ReferenceNumber,
                ? AS ReferenceLine, ? AS ToAddress, ? AS TransactionDate,
                ? AS NewQuantityEach, ? AS NewQuantityPack, ? AS RunNumber
        ) AS source
        ON target.TransactionKey = source.TransactionKey
        WHEN MATCHED THEN
            UPDATE SET
                BN = source.BN,
                ExpiryDate = source.ExpiryDate,
                GenericItemNumber = source.GenericItemNumber,
                ReferenceNumber = source.ReferenceNumber,
                ReferenceLine = source.ReferenceLine,
                ToAddress = source.ToAddress,
                TransactionDate = source.TransactionDate,
                SubmittedQuantityEach = CASE
                    WHEN target.SubmittedQuantityEach >=
                         target.ConfirmedQuantityEach + source.NewQuantityEach
                    THEN target.SubmittedQuantityEach
                    ELSE target.ConfirmedQuantityEach + source.NewQuantityEach
                END,
                SubmittedQuantityPack = CASE
                    WHEN target.SubmittedQuantityPack >=
                         target.ConfirmedQuantityPack + source.NewQuantityPack
                    THEN target.SubmittedQuantityPack
                    ELSE target.ConfirmedQuantityPack + source.NewQuantityPack
                END,
                LastSubmittedRun = source.RunNumber,
                UpdatedAt = SYSUTCDATETIME()
        WHEN NOT MATCHED THEN
            INSERT
            (
                TransactionKey, BN, ExpiryDate, GenericItemNumber,
                ReferenceNumber, ReferenceLine, ToAddress, TransactionDate,
                SubmittedQuantityEach, SubmittedQuantityPack,
                ConfirmedQuantityEach, ConfirmedQuantityPack,
                FirstSubmittedRun, LastSubmittedRun
            )
            VALUES
            (
                source.TransactionKey, source.BN, source.ExpiryDate,
                source.GenericItemNumber, source.ReferenceNumber,
                source.ReferenceLine, source.ToAddress, source.TransactionDate,
                source.NewQuantityEach, source.NewQuantityPack,
                0, 0, source.RunNumber, source.RunNumber
            );
    """

    saved = 0
    with Database().connect() as connection:
        cursor = connection.cursor()
        try:
            for row in prepared.values():
                cursor.execute(
                    sql,
                    (
                        row["Transaction Key"], row["BN"], row["Expiry Date"],
                        row["Generic Item Number"], row["Reference Number"],
                        row["Reference Line"], row["To Address"],
                        row["Transaction Date"], row["Each"], row["Pack"],
                        str(run_number),
                    ),
                )
                saved += 1
            connection.commit()
        except Exception:
            connection.rollback()
            raise
    return saved


def _replace_dispatch_sfda_baseline_with_connection(
    connection: pyodbc.Connection,
    state: pd.DataFrame,
    source_file_name: str = "",
) -> int:
    cursor = connection.cursor()
    cursor.execute("DELETE FROM dbo.DailyDispatchSFDABaseline;")
    rows = [
        (
            _text(row, "GTIN"), _text(row, "BN"), _value(row, "Expiry Date"),
            _number(row, "Active"), _number(row, "Quantity sent pending"),
            str(source_file_name or ""),
        )
        for row in state.to_dict(orient="records")
        if _text(row, "BN") and _value(row, "Expiry Date") is not None
    ]
    if rows:
        # Dispatch SFDA baseline is deliberately inserted without
        # fast_executemany. ODBC Driver 18 can infer a string buffer that is
        # smaller than a later value in the batch (for example 128 vs 166
        # characters), causing HY000 "String data, right truncation" even
        # though the SQL columns themselves are large enough.
        #
        # Regular executemany lets the driver bind each value safely and keeps
        # the confirmation + baseline replacement inside the same transaction.
        cursor.fast_executemany = False
        cursor.executemany(
            r"""
            INSERT INTO dbo.DailyDispatchSFDABaseline
            (GTIN, BN, ExpiryDate, Active, QuantitySentPending, SourceFileName)
            VALUES (?, ?, ?, ?, ?, ?);
            """,
            rows,
        )
    return len(rows)


def replace_dispatch_sfda_baseline(
    sfda_df: pd.DataFrame,
    source_file_name: str = "",
) -> int:
    initialize_database()
    state = _prepare_dispatch_sfda_state(sfda_df)
    with Database().connect() as connection:
        try:
            count = _replace_dispatch_sfda_baseline_with_connection(
                connection, state, source_file_name
            )
            connection.commit()
            return count
        except Exception:
            connection.rollback()
            raise


def confirm_dispatch_transactions_from_sfda(
    sfda_df: pd.DataFrame,
    source_file_name: str = "",
) -> Dict[str, Any]:
    """Confirm prior Dispatch submissions only when the next SFDA report proves it.

    Conservative evidence per BN + Expiry Month is the minimum of the decrease
    in Active and the increase in Quantity sent pending.  Evidence is allocated
    only to previously submitted, still-unconfirmed dispatch transactions.
    """
    import hashlib

    initialize_database()
    current = _prepare_dispatch_sfda_state(sfda_df)

    with Database().connect() as connection:
        previous = pd.read_sql(
            r"""
            SELECT GTIN, BN, ExpiryDate AS [Expiry Date], Active,
                   QuantitySentPending AS [Quantity sent pending]
            FROM dbo.DailyDispatchSFDABaseline;
            """,
            connection,
        )

        if previous.empty:
            return {
                "baseline_available": False,
                "confirmed_packs": 0.0,
                "confirmed_each": 0.0,
                "confirmed_transactions": 0,
                "confirmed_batches": 0,
            }

        previous["BN"] = previous["BN"].fillna("").astype(str).str.strip()
        current["BN"] = current["BN"].fillna("").astype(str).str.strip()
        previous["Expiry Date"] = pd.to_datetime(
            previous["Expiry Date"], errors="coerce"
        ).dt.normalize()
        current["Expiry Date"] = pd.to_datetime(
            current["Expiry Date"], errors="coerce"
        ).dt.normalize()

        comparison = previous.merge(
            current,
            on=["BN", "Expiry Date"],
            how="inner",
            suffixes=(" Previous", " Current"),
        )
        if comparison.empty:
            evidence_rows = []
        else:
            comparison["Active Decrease"] = (
                pd.to_numeric(comparison["Active Previous"], errors="coerce").fillna(0)
                - pd.to_numeric(comparison["Active Current"], errors="coerce").fillna(0)
            ).clip(lower=0)
            comparison["Sent Pending Increase"] = (
                pd.to_numeric(
                    comparison["Quantity sent pending Current"], errors="coerce"
                ).fillna(0)
                - pd.to_numeric(
                    comparison["Quantity sent pending Previous"], errors="coerce"
                ).fillna(0)
            ).clip(lower=0)
            comparison["Confirmed Pack Evidence"] = comparison[
                ["Active Decrease", "Sent Pending Increase"]
            ].min(axis=1)
            evidence_rows = comparison.loc[
                comparison["Confirmed Pack Evidence"].gt(0)
            ].to_dict(orient="records")

        cursor = connection.cursor()
        confirmed_pack_total = 0.0
        confirmed_each_total = 0.0
        confirmed_transaction_keys: Set[str] = set()
        confirmed_batches = 0

        try:
            for evidence in evidence_rows:
                bn = _text(evidence, "BN")
                expiry = _value(evidence, "Expiry Date")
                remaining_pack = max(0.0, _number(evidence, "Confirmed Pack Evidence"))
                if remaining_pack <= 0:
                    continue

                pending_rows = cursor.execute(
                    r"""
                    SELECT TransactionKey,
                           SubmittedQuantityEach, ConfirmedQuantityEach,
                           SubmittedQuantityPack, ConfirmedQuantityPack
                    FROM dbo.DailyDispatchTransactions WITH (UPDLOCK, HOLDLOCK)
                    WHERE BN = ?
                      AND YEAR(ExpiryDate) = YEAR(?)
                      AND MONTH(ExpiryDate) = MONTH(?)
                      AND SubmittedQuantityPack > ConfirmedQuantityPack
                    ORDER BY CreatedAt, TransactionKey;
                    """,
                    (bn, expiry, expiry),
                ).fetchall()

                batch_confirmed = 0.0
                for pending in pending_rows:
                    if remaining_pack <= 0.0000001:
                        break
                    transaction_key = str(pending[0])
                    submitted_each = float(pending[1] or 0)
                    confirmed_each = float(pending[2] or 0)
                    submitted_pack = float(pending[3] or 0)
                    confirmed_pack = float(pending[4] or 0)
                    open_pack = max(0.0, submitted_pack - confirmed_pack)
                    open_each = max(0.0, submitted_each - confirmed_each)
                    if open_pack <= 0:
                        continue

                    allocate_pack = min(remaining_pack, open_pack)
                    each_per_pack = open_each / open_pack if open_pack > 0 else 0
                    allocate_each = min(open_each, allocate_pack * each_per_pack)
                    new_cumulative_pack = confirmed_pack + allocate_pack
                    confirmation_key = hashlib.sha256(
                        f"{transaction_key}|{new_cumulative_pack:.6f}".encode("utf-8")
                    ).hexdigest()

                    cursor.execute(
                        r"""
                        UPDATE dbo.DailyDispatchTransactions
                        SET ConfirmedQuantityPack = ConfirmedQuantityPack + ?,
                            ConfirmedQuantityEach = ConfirmedQuantityEach + ?,
                            LastConfirmedAt = SYSUTCDATETIME(),
                            UpdatedAt = SYSUTCDATETIME()
                        WHERE TransactionKey = ?;
                        """,
                        (allocate_pack, allocate_each, transaction_key),
                    )
                    cursor.execute(
                        r"""
                        INSERT INTO dbo.DailyDispatchConfirmations
                            (ConfirmationKey, TransactionKey,
                             ConfirmedQuantityEach, ConfirmedQuantityPack)
                        SELECT ?, ?, ?, ?
                        WHERE NOT EXISTS
                        (
                            SELECT 1 FROM dbo.DailyDispatchConfirmations
                            WHERE ConfirmationKey = ?
                        );
                        """,
                        (
                            confirmation_key, transaction_key,
                            allocate_each, allocate_pack, confirmation_key,
                        ),
                    )

                    remaining_pack -= allocate_pack
                    batch_confirmed += allocate_pack
                    confirmed_pack_total += allocate_pack
                    confirmed_each_total += allocate_each
                    confirmed_transaction_keys.add(transaction_key)

                if batch_confirmed > 0:
                    confirmed_batches += 1

            _replace_dispatch_sfda_baseline_with_connection(
                connection, current, source_file_name
            )
            connection.commit()
        except Exception:
            connection.rollback()
            raise

    return {
        "baseline_available": True,
        "confirmed_packs": float(confirmed_pack_total),
        "confirmed_each": float(confirmed_each_total),
        "confirmed_transactions": len(confirmed_transaction_keys),
        "confirmed_batches": int(confirmed_batches),
    }


def get_dispatch_confirmed_history_records() -> List[Dict[str, Any]]:
    """Return idempotent DispatchEvents created only from SFDA confirmations."""
    import hashlib
    initialize_database()
    sql = r"""
        SELECT
            c.ConfirmationKey,
            t.BN, t.ExpiryDate, t.GenericItemNumber,
            t.ReferenceNumber, t.ReferenceLine, t.ToAddress,
            t.TransactionDate, c.ConfirmedQuantityEach
        FROM dbo.DailyDispatchConfirmations AS c
        INNER JOIN dbo.DailyDispatchTransactions AS t
            ON t.TransactionKey = c.TransactionKey
        WHERE c.ConfirmedQuantityEach > 0
        ORDER BY c.ConfirmedAt, c.ConfirmationKey;
    """
    with Database().connect() as connection:
        rows = connection.cursor().execute(sql).fetchall()

    records: List[Dict[str, Any]] = []
    for row in rows:
        expiry = pd.to_datetime(row[2], errors="coerce")
        expiry_month_key = "" if pd.isna(expiry) else expiry.strftime("%Y-%m")
        records.append(
            {
                # DispatchEvents.EventKey is varchar(64).  ConfirmationKey is
                # already a 64-character SHA-256 hex digest, so prefixing it
                # with "DISPATCH-CONFIRMED-" makes the value longer than the
                # physical SQL column.  Re-hash the namespaced value to retain
                # a deterministic/idempotent 64-character key.
                "Event Key": hashlib.sha256(
                    ("DISPATCH-CONFIRMED|" + str(row[0])).encode("utf-8")
                ).hexdigest(),
                "BN": str(row[1] or "").strip(),
                "Expiry Month Key": expiry_month_key,
                "Expiry Date": None if pd.isna(expiry) else expiry,
                "Generic Item Number": str(row[3] or "").strip(),
                "Trade Item Number": "",
                "Trade Name": "",
                "Dispatched Quantity": float(row[8] or 0),
                "To Address": str(row[6] or "").strip(),
                "Sales Order Number": str(row[4] or "").strip(),
                "Order Line": str(row[5] or "").strip(),
                "Dispatch Date": row[7],
            }
        )
    return records


def refresh_dispatch_history_incremental(
    confirmed_history_rows: List[Dict[str, Any]],
) -> Dict[str, Any]:
    """Refresh only historical rows affected by newly confirmed Daily Dispatch.

    Daily Dispatch must never rebuild the full historical database. The durable
    source of truth remains DispatchEvents, but only BN + expiry-month + Generic
    keys represented by confirmed dispatch evidence are recalculated here.

    SupplierHistory is intentionally untouched because a dispatch confirmation
    cannot change supplier receipt history.
    """
    initialize_database()
    started_at = time.perf_counter()

    affected = sorted({
        (
            str(row.get("BN") or "").strip(),
            str(row.get("Expiry Month Key") or "").strip(),
            str(row.get("Generic Item Number") or "").strip(),
        )
        for row in (confirmed_history_rows or [])
        if str(row.get("BN") or "").strip()
        and str(row.get("Expiry Month Key") or "").strip()
        and str(row.get("Generic Item Number") or "").strip()
    })

    if not affected:
        with Database().connect() as connection:
            counts = connection.cursor().execute(
                r"""
                SELECT
                    (SELECT COUNT_BIG(*) FROM dbo.BatchMaster),
                    (SELECT COUNT_BIG(*) FROM dbo.SupplierHistory),
                    (SELECT COUNT_BIG(*) FROM dbo.CustomerHistory);
                """
            ).fetchone()
        return {
            "affected_batch_keys": 0,
            "batch_master_rows_updated": 0,
            "customer_history_rows_rebuilt": 0,
            "batch_master_rows": int(counts[0] or 0),
            "supplier_history_rows": int(counts[1] or 0),
            "customer_history_rows": int(counts[2] or 0),
        }

    with Database().connect() as connection:
        cursor = connection.cursor()
        try:
            cursor.execute(r"""
                CREATE TABLE #AffectedDispatchKeys
                (
                    BN nvarchar(255) NOT NULL,
                    ExpiryMonthKey nvarchar(20) NOT NULL,
                    GenericItemNumber nvarchar(255) NOT NULL,
                    PRIMARY KEY (BN, ExpiryMonthKey, GenericItemNumber)
                );
            """)
            cursor.fast_executemany = True
            cursor.executemany(
                "INSERT INTO #AffectedDispatchKeys (BN, ExpiryMonthKey, GenericItemNumber) VALUES (?, ?, ?);",
                affected,
            )

            # Recalculate dispatch totals only for affected Batch Master rows.
            cursor.execute(r"""
                ;WITH DispatchAggregate AS
                (
                    SELECT
                        d.BN,
                        d.ExpiryMonthKey,
                        d.GenericItemNumber,
                        COUNT_BIG(*) AS DispatchRuns,
                        SUM(COALESCE(d.DispatchedQuantity, 0)) AS TotalDispatchedQty,
                        MIN(d.DispatchDate) AS FirstDispatchDate,
                        MAX(d.DispatchDate) AS LastDispatchDate
                    FROM dbo.DispatchEvents d
                    INNER JOIN #AffectedDispatchKeys a
                        ON a.BN = d.BN
                       AND a.ExpiryMonthKey = d.ExpiryMonthKey
                       AND a.GenericItemNumber = d.GenericItemNumber
                    GROUP BY d.BN, d.ExpiryMonthKey, d.GenericItemNumber
                )
                UPDATE bm
                SET
                    bm.TotalDispatchedQty = COALESCE(a.TotalDispatchedQty, 0),
                    bm.DispatchRuns = COALESCE(a.DispatchRuns, 0),
                    bm.FirstDispatchDate = a.FirstDispatchDate,
                    bm.LastDispatchDate = a.LastDispatchDate,
                    bm.LastUpdated = SYSUTCDATETIME()
                FROM dbo.BatchMaster bm
                INNER JOIN DispatchAggregate a
                    ON a.BN = bm.BN
                   AND a.ExpiryMonthKey = bm.ExpiryMonthKey
                   AND a.GenericItemNumber = bm.GenericItemNumber;
            """)
            batch_updated = max(0, int(cursor.rowcount or 0))

            # Rebuild CustomerHistory only for customers/batches touched by the
            # affected keys. GLN is reference data and is overlaid at read time.
            cursor.execute(r"""
                DELETE ch
                FROM dbo.CustomerHistory ch
                INNER JOIN #AffectedDispatchKeys a
                    ON a.BN = ch.BN
                   AND a.ExpiryMonthKey = ch.ExpiryMonthKey
                   AND a.GenericItemNumber = ch.GenericItemNumber;
            """)

            cursor.execute(r"""
                INSERT INTO dbo.CustomerHistory
                (
                    ToAddress, GLN, GTIN, DrugName, GenericItemNumber,
                    TradeDescription, BN, ExpiryMonthKey, ExpiryDate,
                    PackageSize, DispatchQuantityEach, DispatchQuantityPack,
                    FirstDispatchDate, LastDispatchDate, TradeItemNumber,
                    LastUpdated
                )
                SELECT
                    d.ToAddress,
                    N'',
                    bm.GTIN,
                    bm.DrugName,
                    d.GenericItemNumber,
                    COALESCE(NULLIF(MAX(d.TradeName), N''), bm.TradeName, N''),
                    d.BN,
                    d.ExpiryMonthKey,
                    MAX(d.ExpiryDate),
                    COALESCE(bm.PackageSize, 0),
                    SUM(COALESCE(d.DispatchedQuantity, 0)),
                    CASE
                        WHEN COALESCE(bm.PackageSize, 0) > 0
                            THEN SUM(COALESCE(d.DispatchedQuantity, 0)) / bm.PackageSize
                        ELSE 0
                    END,
                    MIN(d.DispatchDate),
                    MAX(d.DispatchDate),
                    COALESCE(NULLIF(MAX(d.TradeItemNumber), N''), bm.TradeItemNumber, N''),
                    SYSUTCDATETIME()
                FROM dbo.DispatchEvents d
                INNER JOIN #AffectedDispatchKeys a
                    ON a.BN = d.BN
                   AND a.ExpiryMonthKey = d.ExpiryMonthKey
                   AND a.GenericItemNumber = d.GenericItemNumber
                INNER JOIN dbo.BatchMaster bm
                    ON bm.BN = d.BN
                   AND bm.ExpiryMonthKey = d.ExpiryMonthKey
                   AND bm.GenericItemNumber = d.GenericItemNumber
                GROUP BY
                    d.ToAddress,
                    d.BN,
                    d.ExpiryMonthKey,
                    d.GenericItemNumber,
                    bm.GTIN,
                    bm.DrugName,
                    bm.TradeName,
                    bm.PackageSize,
                    bm.TradeItemNumber;
            """)
            customer_rebuilt = max(0, int(cursor.rowcount or 0))

            counts = cursor.execute(
                r"""
                SELECT
                    (SELECT COUNT_BIG(*) FROM dbo.BatchMaster),
                    (SELECT COUNT_BIG(*) FROM dbo.SupplierHistory),
                    (SELECT COUNT_BIG(*) FROM dbo.CustomerHistory);
                """
            ).fetchone()

            connection.commit()
        except Exception:
            connection.rollback()
            raise

    logger.info(
        "Incremental Daily Dispatch history refresh completed in %.2f seconds. affected_keys=%s batch_updated=%s customer_rows=%s",
        time.perf_counter() - started_at,
        len(affected),
        batch_updated,
        customer_rebuilt,
    )
    return {
        "affected_batch_keys": len(affected),
        "batch_master_rows_updated": batch_updated,
        "customer_history_rows_rebuilt": customer_rebuilt,
        "batch_master_rows": int(counts[0] or 0),
        "supplier_history_rows": int(counts[1] or 0),
        "customer_history_rows": int(counts[2] or 0),
    }


# -----------------------------------------------------------------------------
# SFDA-confirmed Full Dispatch consumption state
# -----------------------------------------------------------------------------

def _full_dispatch_transaction_key(
    bn: str,
    expiry_date: Any,
    generic_item_number: str,
    to_address: str,
    gln: str,
) -> str:
    """Return one stable Full Dispatch customer-history consumption key."""
    import hashlib

    expiry = pd.to_datetime(expiry_date, errors="coerce")
    expiry_text = "" if pd.isna(expiry) else expiry.strftime("%Y-%m")
    raw = "|".join(
        [
            "FULL-DISPATCH",
            str(bn or "").strip().upper(),
            expiry_text,
            str(generic_item_number or "").strip(),
            str(to_address or "").strip().upper(),
            str(gln or "").strip(),
        ]
    )
    return _warehouse_scoped_key(hashlib.sha256(raw.encode("utf-8")).hexdigest())


def get_full_dispatch_confirmed_allocations() -> pd.DataFrame:
    """Return Full Dispatch quantities already reserved from historical evidence.

    Submitted quantities are treated as reserved immediately after a successful
    Full Dispatch run. This prevents regenerating the same historical WMS
    movement on a retry before SFDA confirmation. Confirmed quantities are also
    returned for audit/status purposes.
    """
    initialize_database()
    sql = r"""
        SELECT
            BN,
            ExpiryDate AS [Expiry Date],
            GenericItemNumber AS [Generic Item Number],
            ToAddress AS [To Address],
            GLN,
            SubmittedQuantityEach AS [Reserved Full Dispatch Quantity Each],
            SubmittedQuantityPack AS [Reserved Full Dispatch Quantity Pack],
            ConfirmedQuantityEach AS [Confirmed Full Dispatch Quantity Each],
            ConfirmedQuantityPack AS [Confirmed Full Dispatch Quantity Pack],
            LastConfirmedAt AS [Last Full Dispatch Confirmed At]
        FROM dbo.FullDispatchTransactions
        WHERE SubmittedQuantityPack > 0
           OR SubmittedQuantityEach > 0;
    """
    with Database().connect() as connection:
        return pd.read_sql(sql, connection)


def save_full_dispatch_pending_transactions(
    rows: List[Dict[str, Any]],
    run_number: str,
) -> int:
    """Persist generated Full Dispatch allocations as awaiting SFDA proof.

    Re-running before SFDA confirmation may regenerate the same allocation.
    The MERGE keeps one deterministic customer/batch transaction and never
    double-counts the same submitted quantity.
    """
    initialize_database()
    prepared: Dict[str, Dict[str, Any]] = {}

    for row in rows or []:
        bn = _text(row, "BN").upper()
        expiry = _value(row, "Expiry Date")
        generic = _text(row, "Generic Item Number")
        to_address = _text(row, "To Address")
        gln = _text(row, "GLN")
        pack_qty = max(
            0.0,
            _number_with_fallback(
                row,
                "Allocated To Be Dispatch",
                "To Be Dispatch",
            ),
        )
        package_size = max(0.0, _number(row, "PackageSize"))
        each_qty = pack_qty * package_size

        if not bn or expiry is None or pack_qty <= 0:
            continue

        expiry_timestamp = pd.to_datetime(expiry, errors="coerce")
        expiry_month_key = (
            ""
            if pd.isna(expiry_timestamp)
            else expiry_timestamp.strftime("%Y-%m")
        )
        if not expiry_month_key:
            continue

        transaction_key = _full_dispatch_transaction_key(
            bn,
            expiry,
            generic,
            to_address,
            gln,
        )

        current = prepared.setdefault(
            transaction_key,
            {
                "TransactionKey": transaction_key,
                "BN": bn,
                "ExpiryDate": expiry,
                "ExpiryMonthKey": expiry_month_key,
                "GenericItemNumber": generic,
                "ToAddress": to_address,
                "GLN": gln,
                "Each": 0.0,
                "Pack": 0.0,
            },
        )
        current["Each"] += each_qty
        current["Pack"] += pack_qty

    if not prepared:
        return 0

    sql = r"""
        MERGE dbo.FullDispatchTransactions WITH (HOLDLOCK) AS target
        USING
        (
            SELECT
                ? AS TransactionKey, ? AS BN, ? AS ExpiryDate,
                ? AS ExpiryMonthKey, ? AS GenericItemNumber,
                ? AS ToAddress, ? AS GLN,
                ? AS NewQuantityEach, ? AS NewQuantityPack, ? AS RunNumber
        ) AS source
        ON target.TransactionKey = source.TransactionKey
        WHEN MATCHED THEN
            UPDATE SET
                BN = source.BN,
                ExpiryDate = source.ExpiryDate,
                GenericItemNumber = source.GenericItemNumber,
                ToAddress = source.ToAddress,
                GLN = source.GLN,
                SubmittedQuantityEach = CASE
                    WHEN target.SubmittedQuantityEach >=
                         target.ConfirmedQuantityEach + source.NewQuantityEach
                    THEN target.SubmittedQuantityEach
                    ELSE target.ConfirmedQuantityEach + source.NewQuantityEach
                END,
                SubmittedQuantityPack = CASE
                    WHEN target.SubmittedQuantityPack >=
                         target.ConfirmedQuantityPack + source.NewQuantityPack
                    THEN target.SubmittedQuantityPack
                    ELSE target.ConfirmedQuantityPack + source.NewQuantityPack
                END,
                LastSubmittedRun = source.RunNumber,
                UpdatedAt = SYSUTCDATETIME()
        WHEN NOT MATCHED THEN
            INSERT
            (
                TransactionKey, BN, ExpiryDate, ExpiryMonthKey,
                GenericItemNumber, ToAddress, GLN,
                SubmittedQuantityEach, SubmittedQuantityPack,
                ConfirmedQuantityEach, ConfirmedQuantityPack,
                FirstSubmittedRun, LastSubmittedRun
            )
            VALUES
            (
                source.TransactionKey, source.BN, source.ExpiryDate,
                source.ExpiryMonthKey, source.GenericItemNumber,
                source.ToAddress, source.GLN,
                source.NewQuantityEach, source.NewQuantityPack,
                0, 0, source.RunNumber, source.RunNumber
            );
    """

    saved = 0
    with Database().connect() as connection:
        cursor = connection.cursor()
        try:
            for row in prepared.values():
                cursor.execute(
                    sql,
                    (
                        row["TransactionKey"],
                        row["BN"],
                        row["ExpiryDate"],
                        row["ExpiryMonthKey"],
                        row["GenericItemNumber"],
                        row["ToAddress"],
                        row["GLN"],
                        row["Each"],
                        row["Pack"],
                        str(run_number),
                    ),
                )
                saved += 1
            connection.commit()
        except Exception:
            connection.rollback()
            raise

    return saved


def _replace_full_dispatch_sfda_baseline_with_connection(
    connection: pyodbc.Connection,
    state: pd.DataFrame,
    source_file_name: str = "",
) -> int:
    cursor = connection.cursor()
    cursor.execute("DELETE FROM dbo.FullDispatchSFDABaseline;")

    rows = [
        (
            _text(row, "GTIN"),
            _text(row, "BN"),
            _value(row, "Expiry Date"),
            _number(row, "Active"),
            _number(row, "Quantity sent pending"),
            str(source_file_name or ""),
        )
        for row in state.to_dict(orient="records")
        if _text(row, "BN") and _value(row, "Expiry Date") is not None
    ]

    if rows:
        # Keep this proof snapshot on regular executemany. ODBC Driver 18 may
        # otherwise infer a short string buffer and fail on a later longer BN
        # or source file name.
        cursor.fast_executemany = False
        cursor.executemany(
            r"""
            INSERT INTO dbo.FullDispatchSFDABaseline
            (GTIN, BN, ExpiryDate, Active, QuantitySentPending, SourceFileName)
            VALUES (?, ?, ?, ?, ?, ?);
            """,
            rows,
        )

    return len(rows)


def replace_full_dispatch_sfda_baseline(
    sfda_df: pd.DataFrame,
    source_file_name: str = "",
) -> int:
    initialize_database()
    state = _prepare_dispatch_sfda_state(sfda_df)

    with Database().connect() as connection:
        try:
            count = _replace_full_dispatch_sfda_baseline_with_connection(
                connection,
                state,
                source_file_name,
            )
            connection.commit()
            return count
        except Exception:
            connection.rollback()
            raise


def confirm_full_dispatch_transactions_from_sfda(
    sfda_df: pd.DataFrame,
    source_file_name: str = "",
) -> Dict[str, Any]:
    """Confirm prior Full Dispatch allocations from the next SFDA report.

    The proof is conservative: for each exact SFDA BN + Expiry Date, confirmed
    packs equal the smaller of Active decrease and Quantity Sent Pending
    increase. That evidence is allocated FIFO only to previously generated,
    still-unconfirmed Full Dispatch allocations.

    This ledger does NOT append new WMS DispatchEvents or increase Batch Master
    historical dispatch totals. It only marks historical customer dispatch
    evidence as already consumed by Full Reconciliation so it cannot be
    proposed again.
    """
    import hashlib

    initialize_database()
    current = _prepare_dispatch_sfda_state(sfda_df)

    with Database().connect() as connection:
        previous = pd.read_sql(
            r"""
            SELECT
                GTIN,
                BN,
                ExpiryDate AS [Expiry Date],
                Active,
                QuantitySentPending AS [Quantity sent pending]
            FROM dbo.FullDispatchSFDABaseline;
            """,
            connection,
        )

        if previous.empty:
            return {
                "baseline_available": False,
                "confirmed_packs": 0.0,
                "confirmed_each": 0.0,
                "confirmed_transactions": 0,
                "confirmed_batches": 0,
            }

        previous["BN"] = previous["BN"].fillna("").astype(str).str.strip()
        current["BN"] = current["BN"].fillna("").astype(str).str.strip()
        previous["Expiry Date"] = pd.to_datetime(
            previous["Expiry Date"],
            errors="coerce",
        ).dt.normalize()
        current["Expiry Date"] = pd.to_datetime(
            current["Expiry Date"],
            errors="coerce",
        ).dt.normalize()

        comparison = previous.merge(
            current,
            on=["BN", "Expiry Date"],
            how="inner",
            suffixes=(" Previous", " Current"),
        )

        if comparison.empty:
            evidence_rows: List[Dict[str, Any]] = []
        else:
            comparison["Active Decrease"] = (
                pd.to_numeric(
                    comparison["Active Previous"],
                    errors="coerce",
                ).fillna(0)
                - pd.to_numeric(
                    comparison["Active Current"],
                    errors="coerce",
                ).fillna(0)
            ).clip(lower=0)

            comparison["Sent Pending Increase"] = (
                pd.to_numeric(
                    comparison["Quantity sent pending Current"],
                    errors="coerce",
                ).fillna(0)
                - pd.to_numeric(
                    comparison["Quantity sent pending Previous"],
                    errors="coerce",
                ).fillna(0)
            ).clip(lower=0)

            comparison["Confirmed Pack Evidence"] = comparison[
                ["Active Decrease", "Sent Pending Increase"]
            ].min(axis=1)

            evidence_rows = comparison.loc[
                comparison["Confirmed Pack Evidence"].gt(0)
            ].to_dict(orient="records")

        cursor = connection.cursor()
        confirmed_pack_total = 0.0
        confirmed_each_total = 0.0
        confirmed_transaction_keys: Set[str] = set()
        confirmed_batches = 0

        try:
            for evidence in evidence_rows:
                bn = _text(evidence, "BN")
                expiry = _value(evidence, "Expiry Date")
                remaining_pack = max(
                    0.0,
                    _number(evidence, "Confirmed Pack Evidence"),
                )
                if remaining_pack <= 0:
                    continue

                pending_rows = cursor.execute(
                    r"""
                    SELECT
                        TransactionKey,
                        SubmittedQuantityEach,
                        ConfirmedQuantityEach,
                        SubmittedQuantityPack,
                        ConfirmedQuantityPack
                    FROM dbo.FullDispatchTransactions
                         WITH (UPDLOCK, HOLDLOCK)
                    WHERE BN = ?
                      AND ExpiryMonthKey = ?
                      AND SubmittedQuantityPack > ConfirmedQuantityPack
                    ORDER BY CreatedAt, TransactionKey;
                    """,
                    (
                        bn,
                        pd.to_datetime(expiry, errors="coerce").strftime("%Y-%m"),
                    ),
                ).fetchall()

                batch_confirmed = 0.0

                for pending in pending_rows:
                    if remaining_pack <= 0.0000001:
                        break

                    transaction_key = str(pending[0])
                    submitted_each = float(pending[1] or 0)
                    confirmed_each = float(pending[2] or 0)
                    submitted_pack = float(pending[3] or 0)
                    confirmed_pack = float(pending[4] or 0)

                    open_pack = max(0.0, submitted_pack - confirmed_pack)
                    open_each = max(0.0, submitted_each - confirmed_each)
                    if open_pack <= 0:
                        continue

                    allocate_pack = min(remaining_pack, open_pack)
                    each_per_pack = (
                        open_each / open_pack
                        if open_pack > 0
                        else 0
                    )
                    allocate_each = min(
                        open_each,
                        allocate_pack * each_per_pack,
                    )

                    new_cumulative_pack = confirmed_pack + allocate_pack
                    confirmation_key = hashlib.sha256(
                        (
                            f"FULL-DISPATCH|{transaction_key}|"
                            f"{new_cumulative_pack:.6f}"
                        ).encode("utf-8")
                    ).hexdigest()

                    cursor.execute(
                        r"""
                        UPDATE dbo.FullDispatchTransactions
                        SET
                            ConfirmedQuantityPack =
                                ConfirmedQuantityPack + ?,
                            ConfirmedQuantityEach =
                                ConfirmedQuantityEach + ?,
                            LastConfirmedAt = SYSUTCDATETIME(),
                            UpdatedAt = SYSUTCDATETIME()
                        WHERE TransactionKey = ?;
                        """,
                        (
                            allocate_pack,
                            allocate_each,
                            transaction_key,
                        ),
                    )

                    cursor.execute(
                        r"""
                        INSERT INTO dbo.FullDispatchConfirmations
                        (
                            ConfirmationKey,
                            TransactionKey,
                            ConfirmedQuantityEach,
                            ConfirmedQuantityPack
                        )
                        SELECT ?, ?, ?, ?
                        WHERE NOT EXISTS
                        (
                            SELECT 1
                            FROM dbo.FullDispatchConfirmations
                            WHERE ConfirmationKey = ?
                        );
                        """,
                        (
                            confirmation_key,
                            transaction_key,
                            allocate_each,
                            allocate_pack,
                            confirmation_key,
                        ),
                    )

                    remaining_pack -= allocate_pack
                    batch_confirmed += allocate_pack
                    confirmed_pack_total += allocate_pack
                    confirmed_each_total += allocate_each
                    confirmed_transaction_keys.add(transaction_key)

                if batch_confirmed > 0:
                    confirmed_batches += 1

            _replace_full_dispatch_sfda_baseline_with_connection(
                connection,
                current,
                source_file_name,
            )
            connection.commit()

        except Exception:
            connection.rollback()
            raise

    return {
        "baseline_available": True,
        "confirmed_packs": float(confirmed_pack_total),
        "confirmed_each": float(confirmed_each_total),
        "confirmed_transactions": len(confirmed_transaction_keys),
        "confirmed_batches": int(confirmed_batches),
    }


def get_full_dispatch_transaction_count() -> int:
    """Return the number of persisted Full Dispatch reservation rows."""
    initialize_database()
    with Database().connect() as connection:
        row = connection.cursor().execute(
            "SELECT COUNT_BIG(*) FROM dbo.FullDispatchTransactions;"
        ).fetchone()
    return int(row[0] or 0)


# ================================================================
# Application authentication
# ================================================================

def _auth_row(cursor: pyodbc.Cursor, row: Any) -> Optional[Dict[str, Any]]:
    if row is None:
        return None
    names = [column[0] for column in cursor.description]
    return dict(zip(names, row))


def normalize_warehouse_name(value: str) -> str:
    text = " ".join(str(value or "").strip().split())
    if not text:
        raise ValueError("Warehouse name is required.")
    if len(text) > 150:
        raise ValueError("Warehouse name is too long.")
    return text


def get_or_create_warehouse(name: str) -> Dict[str, Any]:
    warehouse_name = normalize_warehouse_name(name)
    with Database().connect() as connection:
        cursor = connection.cursor()
        row = cursor.execute(
            "SELECT TOP (1) WarehouseID, WarehouseCode, WarehouseName, Status, CreatedAt "
            "FROM dbo.Warehouses WHERE LOWER(WarehouseName)=LOWER(?);",
            warehouse_name,
        ).fetchone()
        if not row:
            cursor.execute(
                "INSERT INTO dbo.Warehouses (WarehouseName, Status) VALUES (?, N'Active');",
                warehouse_name,
            )
            connection.commit()
            row = cursor.execute(
                "SELECT TOP (1) WarehouseID, WarehouseCode, WarehouseName, Status, CreatedAt "
                "FROM dbo.Warehouses WHERE LOWER(WarehouseName)=LOWER(?);",
                warehouse_name,
            ).fetchone()
        names = [column[0] for column in cursor.description]
        return dict(zip(names, row))


def get_madinah_warehouse() -> Dict[str, Any]:
    with Database().connect() as connection:
        cursor = connection.cursor()
        row = cursor.execute(
            "SELECT TOP (1) WarehouseID, WarehouseCode, WarehouseName, Status, CreatedAt "
            "FROM dbo.Warehouses WHERE WarehouseCode=N'MADINAH' ORDER BY WarehouseID;"
        ).fetchone()
        if not row:
            raise RuntimeError("Madinah Warehouse bootstrap record is missing.")
        names = [column[0] for column in cursor.description]
        return dict(zip(names, row))


def list_warehouses() -> List[Dict[str, Any]]:
    with Database().connect() as connection:
        cursor = connection.cursor()
        rows = cursor.execute(
            "SELECT WarehouseID, WarehouseCode, WarehouseName, Status, CreatedAt "
            "FROM dbo.Warehouses ORDER BY WarehouseName;"
        ).fetchall()
        names = [column[0] for column in cursor.description]
        return [dict(zip(names, row)) for row in rows]


# Warehouses approved for self-service registration. Madinah is retained as the
# existing legacy warehouse, identified by WarehouseCode=MADINAH.
_REGISTRATION_WAREHOUSE_NAMES = (
    "Baha LC",
    "Dammam LC",
    "Riyadh SMSA",
    "Riyadh Agility",
    "Qassim LC",
    "Jeddah Maersk",
    "Jeddah Tamer",
    "Asir Naqel",
)


def list_registration_warehouses() -> List[Dict[str, Any]]:
    """Return only warehouses approved for the public registration dropdown.

    Existing ad-hoc/test warehouse rows remain in the database for data integrity,
    but they cannot be selected by a newly registering user.
    """
    placeholders = ",".join("?" for _ in _REGISTRATION_WAREHOUSE_NAMES)
    sql = f"""
        SELECT WarehouseID, WarehouseCode, WarehouseName, Status, CreatedAt
        FROM dbo.Warehouses
        WHERE LOWER(Status)=N'active'
          AND (WarehouseCode=N'MADINAH' OR WarehouseName IN ({placeholders}))
        ORDER BY
            CASE WHEN WarehouseCode=N'MADINAH' THEN 0 ELSE 1 END,
            WarehouseName;
    """
    with Database().connect() as connection:
        cursor = connection.cursor()
        rows = cursor.execute(sql, tuple(_REGISTRATION_WAREHOUSE_NAMES)).fetchall()
        names = [column[0] for column in cursor.description]
        return [dict(zip(names, row)) for row in rows]


def get_registration_warehouse_by_id(warehouse_id: int) -> Dict[str, Any]:
    """Resolve a registration WarehouseID and reject non-approved warehouses."""
    try:
        warehouse_id = int(warehouse_id)
    except Exception as exc:
        raise ValueError("A valid warehouse must be selected.") from exc

    if warehouse_id <= 0:
        raise ValueError("A valid warehouse must be selected.")

    allowed = list_registration_warehouses()
    for warehouse in allowed:
        if int(warehouse.get("WarehouseID") or 0) == warehouse_id:
            return warehouse

    raise ValueError("The selected warehouse is not available for registration.")


def find_user_by_email(email: str) -> Optional[Dict[str, Any]]:
    sql = """
        SELECT TOP (1)
            u.UserID, u.Email, u.PasswordSalt, u.PasswordHash, u.Role, u.Status,
            u.ApprovalTokenHash, u.ApprovalExpiresAt, u.ApprovedAt,
            u.CreatedAt, u.UpdatedAt, u.LastLoginAt,
            u.WarehouseID, w.WarehouseName, w.WarehouseCode,
            u.RequestedWarehouseName
        FROM dbo.ApplicationUsers u
        LEFT JOIN dbo.Warehouses w ON w.WarehouseID=u.WarehouseID
        WHERE LOWER(u.Email) = LOWER(?);
    """
    with Database().connect() as connection:
        cursor = connection.cursor()
        row = cursor.execute(sql, (str(email),)).fetchone()
        return _auth_row(cursor, row)


def count_active_admins() -> int:
    with Database().connect() as connection:
        return int(connection.cursor().execute(
            "SELECT COUNT(*) FROM dbo.ApplicationUsers WHERE Status=N'Active' AND Role=N'Admin';"
        ).fetchone()[0] or 0)


def create_pending_user_record(
    email: str,
    password_salt: str,
    password_hash: str,
    role: str,
    approval_token_hash: str,
    approval_expires_at: Any,
    warehouse_id: int,
    requested_warehouse_name: str,
    status: str = "Pending",
) -> Dict[str, Any]:
    with Database().connect() as connection:
        cursor = connection.cursor()
        existing = cursor.execute(
            "SELECT TOP (1) UserID FROM dbo.ApplicationUsers WHERE LOWER(Email)=LOWER(?);",
            (email,),
        ).fetchone()
        approved_now = status == "Active"
        if existing:
            user_id = int(existing[0])
            cursor.execute(
                """
                UPDATE dbo.ApplicationUsers
                SET PasswordSalt=?, PasswordHash=?, Role=?, Status=?,
                    ApprovalTokenHash=?, ApprovalExpiresAt=?,
                    ApprovedAt=CASE WHEN ?=N'Active' THEN SYSUTCDATETIME() ELSE NULL END,
                    WarehouseID=?, RequestedWarehouseName=?, UpdatedAt=SYSUTCDATETIME()
                WHERE UserID=?;
                """,
                (
                    password_salt, password_hash, role, status,
                    None if approved_now else approval_token_hash,
                    None if approved_now else approval_expires_at,
                    status, int(warehouse_id), requested_warehouse_name, user_id,
                ),
            )
        else:
            cursor.execute(
                """
                INSERT INTO dbo.ApplicationUsers
                (Email, PasswordSalt, PasswordHash, Role, Status,
                 ApprovalTokenHash, ApprovalExpiresAt, ApprovedAt,
                 WarehouseID, RequestedWarehouseName)
                VALUES (?, ?, ?, ?, ?, ?, ?,
                        CASE WHEN ?=N'Active' THEN SYSUTCDATETIME() ELSE NULL END,
                        ?, ?);
                """,
                (
                    email, password_salt, password_hash, role, status,
                    None if approved_now else approval_token_hash,
                    None if approved_now else approval_expires_at,
                    status, int(warehouse_id), requested_warehouse_name,
                ),
            )
        row = cursor.execute(
            """
            SELECT TOP (1) u.UserID, u.Email, u.Role, u.Status, u.ApprovalExpiresAt,
                   u.ApprovedAt, u.CreatedAt, u.UpdatedAt, u.LastLoginAt,
                   u.WarehouseID, w.WarehouseName, w.WarehouseCode,
                   u.RequestedWarehouseName
            FROM dbo.ApplicationUsers u
            LEFT JOIN dbo.Warehouses w ON w.WarehouseID=u.WarehouseID
            WHERE LOWER(u.Email)=LOWER(?);
            """,
            (email,),
        ).fetchone()
        result = _auth_row(cursor, row) or {}
        connection.commit()
        return result


def approve_user_by_token_hash(token_hash: str) -> Optional[Dict[str, Any]]:
    with Database().connect() as connection:
        cursor = connection.cursor()
        row = cursor.execute(
            "SELECT TOP (1) UserID FROM dbo.ApplicationUsers "
            "WHERE ApprovalTokenHash=? AND Status=N'Pending' "
            "AND ApprovalExpiresAt>=SYSUTCDATETIME();",
            (token_hash,),
        ).fetchone()
        if not row:
            return None
        user_id = int(row[0])
        cursor.execute(
            """
            UPDATE dbo.ApplicationUsers
            SET Status=N'Active', ApprovedAt=SYSUTCDATETIME(),
                ApprovalTokenHash=NULL, ApprovalExpiresAt=NULL,
                UpdatedAt=SYSUTCDATETIME()
            WHERE UserID=?;
            """,
            (user_id,),
        )
        result_row = cursor.execute(
            """
            SELECT u.UserID,u.Email,u.Role,u.Status,u.ApprovedAt,u.CreatedAt,u.UpdatedAt,u.LastLoginAt,
                   u.WarehouseID,w.WarehouseName,w.WarehouseCode,u.RequestedWarehouseName
            FROM dbo.ApplicationUsers u
            LEFT JOIN dbo.Warehouses w ON w.WarehouseID=u.WarehouseID
            WHERE u.UserID=?;
            """,
            (user_id,),
        ).fetchone()
        result = _auth_row(cursor, result_row)
        connection.commit()
        return result


def create_auth_session(user_id: int, token_hash: str, expires_at: Any) -> None:
    with Database().connect() as connection:
        cursor = connection.cursor()
        cursor.execute("DELETE FROM dbo.AuthSessions WHERE ExpiresAt<SYSUTCDATETIME();")
        cursor.execute(
            "INSERT INTO dbo.AuthSessions (UserID,TokenHash,ExpiresAt) VALUES (?,?,?);",
            (int(user_id), token_hash, expires_at),
        )
        cursor.execute(
            "UPDATE dbo.ApplicationUsers SET LastLoginAt=SYSUTCDATETIME(),UpdatedAt=SYSUTCDATETIME() WHERE UserID=?;",
            (int(user_id),),
        )
        connection.commit()


def get_auth_session_user(token_hash: str) -> Optional[Dict[str, Any]]:
    sql = """
        SELECT TOP (1) u.UserID,u.Email,u.Role,u.Status,u.ApprovedAt,u.CreatedAt,u.UpdatedAt,u.LastLoginAt,
               u.WarehouseID,w.WarehouseName,w.WarehouseCode,s.ExpiresAt
        FROM dbo.AuthSessions s
        INNER JOIN dbo.ApplicationUsers u ON u.UserID=s.UserID
        LEFT JOIN dbo.Warehouses w ON w.WarehouseID=u.WarehouseID
        WHERE s.TokenHash=? AND s.ExpiresAt>=SYSUTCDATETIME() AND u.Status=N'Active';
    """
    with Database().connect() as connection:
        cursor = connection.cursor()
        row = cursor.execute(sql, (token_hash,)).fetchone()
        return _auth_row(cursor, row)


def delete_auth_session(token_hash: str) -> None:
    with Database().connect() as connection:
        cursor = connection.cursor()
        cursor.execute("DELETE FROM dbo.AuthSessions WHERE TokenHash=?;", (token_hash,))
        connection.commit()


def list_auth_users() -> List[Dict[str, Any]]:
    sql = """
        SELECT u.UserID,u.Email,u.Role,u.Status,u.ApprovedAt,u.CreatedAt,u.UpdatedAt,u.LastLoginAt,
               u.WarehouseID,w.WarehouseName,w.WarehouseCode,u.RequestedWarehouseName
        FROM dbo.ApplicationUsers u
        LEFT JOIN dbo.Warehouses w ON w.WarehouseID=u.WarehouseID
        ORDER BY CASE u.Status WHEN N'Pending' THEN 0 WHEN N'Active' THEN 1 ELSE 2 END,u.CreatedAt DESC;
    """
    with Database().connect() as connection:
        cursor = connection.cursor()
        rows = cursor.execute(sql).fetchall()
        names = [column[0] for column in cursor.description]
        return [dict(zip(names, row)) for row in rows]


def set_user_status(user_id: int, status: str) -> Dict[str, Any]:
    with Database().connect() as connection:
        cursor = connection.cursor()
        cursor.execute(
            """
            UPDATE dbo.ApplicationUsers
            SET Status=?,
                ApprovedAt=CASE WHEN ?=N'Active' AND ApprovedAt IS NULL THEN SYSUTCDATETIME() ELSE ApprovedAt END,
                UpdatedAt=SYSUTCDATETIME()
            WHERE UserID=?;
            """,
            (status, status, int(user_id)),
        )
        if status != "Active":
            cursor.execute("DELETE FROM dbo.AuthSessions WHERE UserID=?;", (int(user_id),))
        row = cursor.execute(
            """
            SELECT u.UserID,u.Email,u.Role,u.Status,u.ApprovedAt,u.CreatedAt,u.UpdatedAt,u.LastLoginAt,
                   u.WarehouseID,w.WarehouseName,w.WarehouseCode,u.RequestedWarehouseName
            FROM dbo.ApplicationUsers u
            LEFT JOIN dbo.Warehouses w ON w.WarehouseID=u.WarehouseID
            WHERE u.UserID=?;
            """,
            (int(user_id),),
        ).fetchone()
        if not row:
            connection.rollback()
            raise ValueError("User was not found.")
        result = _auth_row(cursor, row)
        connection.commit()
        return result


def create_outlook_draft_request(
    *,
    request_id: str,
    state_hash: str,
    requested_by_email: str,
    selected_ids_json: str,
    recipient_email: str,
    subject: str,
    message_body: str,
    expires_at: Any,
) -> None:
    initialize_database()
    with Database().connect() as connection:
        cursor = connection.cursor()
        cursor.execute(
            "DELETE FROM dbo.OutlookDraftRequests "
            "WHERE ExpiresAt < SYSUTCDATETIME() AND Status=N'Pending';"
        )
        cursor.execute(
            """
            INSERT INTO dbo.OutlookDraftRequests
            (
                RequestID, StateHash, RequestedByEmail, SelectedIDsJson,
                RecipientEmail, Subject, MessageBody, Status, ExpiresAt
            )
            VALUES
            (
                CAST(? AS uniqueidentifier), ?, ?, ?, ?, ?, ?,
                N'Pending', ?
            );
            """,
            (
                request_id,
                state_hash,
                requested_by_email,
                selected_ids_json,
                recipient_email,
                subject,
                message_body,
                expires_at,
            ),
        )
        connection.commit()


def get_outlook_draft_request_by_state_hash(
    state_hash: str,
) -> Optional[Dict[str, Any]]:
    initialize_database()
    sql = """
        SELECT TOP (1)
            RequestID, StateHash, RequestedByEmail, SelectedIDsJson,
            RecipientEmail, Subject, MessageBody, Status,
            GraphMessageID, GraphWebLink, ErrorMessage,
            CreatedAt, ExpiresAt, CompletedAt
        FROM dbo.OutlookDraftRequests
        WHERE StateHash=?
          AND Status=N'Pending'
          AND ExpiresAt >= SYSUTCDATETIME();
    """
    with Database().connect() as connection:
        cursor = connection.cursor()
        row = cursor.execute(sql, (state_hash,)).fetchone()
        if not row:
            return None
        names = [column[0] for column in cursor.description]
        return dict(zip(names, row))


def complete_outlook_draft_request(
    request_id: str,
    *,
    graph_message_id: str,
    graph_web_link: str,
) -> None:
    initialize_database()
    with Database().connect() as connection:
        cursor = connection.cursor()
        cursor.execute(
            """
            UPDATE dbo.OutlookDraftRequests
            SET Status=N'Completed',
                GraphMessageID=?,
                GraphWebLink=?,
                ErrorMessage=NULL,
                CompletedAt=SYSUTCDATETIME()
            WHERE RequestID=CAST(? AS uniqueidentifier);
            """,
            (graph_message_id, graph_web_link, request_id),
        )
        connection.commit()


def fail_outlook_draft_request(
    request_id: str,
    error_message: str,
) -> None:
    initialize_database()
    with Database().connect() as connection:
        cursor = connection.cursor()
        cursor.execute(
            """
            UPDATE dbo.OutlookDraftRequests
            SET Status=N'Failed',
                ErrorMessage=?,
                CompletedAt=SYSUTCDATETIME()
            WHERE RequestID=CAST(? AS uniqueidentifier);
            """,
            (str(error_message or "")[:4000], request_id),
        )
        connection.commit()
