import json
import os
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple

import pandas as pd
import pyodbc


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


_RECEIPT_INSERT_SQL = r"""
IF EXISTS
(
    SELECT 1
    FROM dbo.ReceiptEvents
    WHERE EventKey = ?
)
BEGIN
    SELECT CAST(0 AS int) AS Inserted;
END
ELSE
BEGIN
    INSERT INTO dbo.ReceiptEvents
    (
        EventKey,
        BN,
        ExpiryMonthKey,
        ExpiryDate,
        GenericItemNumber,
        TradeItemNumber,
        TradeName,
        ReceivedQuantity,
        InboundShipment,
        ASNLine,
        SupplierName,
        ReceivedDate
    )
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);

    SELECT CAST(1 AS int) AS Inserted;
END;
"""


_DISPATCH_INSERT_SQL = r"""
IF EXISTS
(
    SELECT 1
    FROM dbo.DispatchEvents
    WHERE EventKey = ?
)
BEGIN
    SELECT CAST(0 AS int) AS Inserted;
END
ELSE
BEGIN
    INSERT INTO dbo.DispatchEvents
    (
        EventKey,
        BN,
        ExpiryMonthKey,
        ExpiryDate,
        GenericItemNumber,
        TradeItemNumber,
        TradeName,
        DispatchedQuantity,
        ToAddress,
        SalesOrderNumber,
        OrderLine,
        DispatchDate
    )
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);

    SELECT CAST(1 AS int) AS Inserted;
END;
"""


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


def _fetch_inserted_result(cursor: pyodbc.Cursor) -> int:
    """Read the Inserted value returned by the event insert SQL batch."""

    while True:
        if cursor.description is not None:
            row = cursor.fetchone()

            if row is not None:
                return int(row[0])

        try:
            has_next = cursor.nextset()
        except pyodbc.ProgrammingError:
            return 0

        if not has_next:
            return 0


def initialize_database() -> None:
    """Create or safely upgrade all Version 5 database objects."""

    with Database().connect() as connection:
        cursor = connection.cursor()

        try:
            cursor.execute(_load_schema_sql())
            _consume_all_results(cursor)
            connection.commit()

        except Exception:
            connection.rollback()
            raise


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


def _integer(
    row: Dict[str, Any],
    name: str,
    default: int = 0,
) -> int:
    return int(_number(row, name, default))


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

    values = (
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
        _value(row, "Received Date"),
    )

    return (event_key, *values)


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

    values = (
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

    return (event_key, *values)


def append_events(
    receipt_rows: List[Dict[str, Any]],
    dispatch_rows: List[Dict[str, Any]],
) -> Dict[str, int]:
    """Append only new receipt and dispatch events.

    EventKey is the immutable de-duplication key. Existing rows are ignored.
    """

    initialize_database()

    inserted_receipts = 0
    inserted_dispatches = 0

    with Database().connect() as connection:
        cursor = connection.cursor()

        try:
            for row in receipt_rows or []:
                cursor.execute(
                    _RECEIPT_INSERT_SQL,
                    _receipt_parameters(row),
                )
                inserted_receipts += _fetch_inserted_result(
                    cursor
                )

            for row in dispatch_rows or []:
                cursor.execute(
                    _DISPATCH_INSERT_SQL,
                    _dispatch_parameters(row),
                )
                inserted_dispatches += _fetch_inserted_result(
                    cursor
                )

            connection.commit()

        except Exception:
            connection.rollback()
            raise

    return {
        "receipt_events": inserted_receipts,
        "dispatch_events": inserted_dispatches,
    }


def get_event_summaries() -> Tuple[pd.DataFrame, pd.DataFrame]:
    """Return cumulative receipt and dispatch summaries for Batch Master."""

    initialize_database()

    receipt_sql = r"""
        SELECT
            BN,
            ExpiryMonthKey AS [Expiry Month Key],
            GenericItemNumber AS [Generic Item Number],
            MAX(NULLIF(TradeItemNumber, '')) AS [Trade Item Number],
            MAX(NULLIF(TradeName, '')) AS [Trade Name],
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

    return receipt, dispatch


def replace_batch_master(master: pd.DataFrame) -> Dict[str, Any]:
    """Atomically replace Batch Master from cumulative event summaries."""

    initialize_database()

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
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
    """

    rows: Iterable[Tuple[Any, ...]] = (
        (
            _text(row, "BN"),
            _text(row, "Expiry Month Key"),
            _value(row, "Expiry Date"),
            _text(row, "Generic Item Number"),
            _text(
                row,
                "Trade Item Number",
                _text(row, "Trade Item"),
            ),
            _text(row, "Trade Name"),
            _text(row, "GTIN"),
            _text(row, "Drug Name"),
            _number(row, "Total Receive Qty"),
            _number(row, "Total Dispatched Qty"),
            _integer(row, "Receive Runs"),
            _integer(row, "Dispatch Runs"),
            _value(row, "First Received Date"),
            _value(row, "Last Received Date"),
            _value(row, "First Dispatch Date"),
            _value(row, "Last Dispatch Date"),
            _text(row, "Generic Exists in SFDA", "Yes") or "Yes",
            _value(
                row,
                "Last Updated",
                pd.Timestamp.utcnow().tz_localize(None),
            ),
        )
        for row in master.to_dict(orient="records")
    )

    with Database().connect() as connection:
        cursor = connection.cursor()

        try:
            cursor.execute("DELETE FROM dbo.BatchMaster;")

            prepared_rows = list(rows)

            if prepared_rows:
                cursor.fast_executemany = True
                cursor.executemany(insert_sql, prepared_rows)

            connection.commit()

        except Exception:
            connection.rollback()
            raise

    return {
        "status": "Completed",
        "rows_inserted": len(master),
    }


def get_batch_master_df() -> pd.DataFrame:
    initialize_database()

    sql = r"""
        SELECT
            BN,
            ExpiryMonthKey AS [Expiry Month Key],
            ExpiryDate AS [Expiry Date],
            GenericItemNumber AS [Generic Item Number],
            TradeItemNumber AS [Trade Item Number],
            TradeName AS [Trade Name],
            GTIN,
            DrugName AS [Drug Name],
            TotalReceiveQty AS [Total Receive Qty],
            TotalDispatchedQty AS [Total Dispatched Qty],
            ReceiveRuns AS [Receive Runs],
            DispatchRuns AS [Dispatch Runs],
            FirstReceivedDate AS [First Received Date],
            LastReceivedDate AS [Last Received Date],
            FirstDispatchDate AS [First Dispatch Date],
            LastDispatchDate AS [Last Dispatch Date],
            GenericExistsInSFDA AS [Generic Exists in SFDA],
            LastUpdated AS [Last Updated]
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
