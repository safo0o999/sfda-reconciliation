import os
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


_SCHEMA_SQL = r"""
SET XACT_ABORT ON;

IF OBJECT_ID('dbo.ReceiptEvents', 'U') IS NULL
BEGIN
    CREATE TABLE dbo.ReceiptEvents
    (
        EventKey varchar(64) NOT NULL,
        BN nvarchar(120) NOT NULL,
        ExpiryMonthKey char(7) NOT NULL,
        ExpiryDate date NULL,
        GenericItemNumber nvarchar(120) NOT NULL,
        TradeItemNumber nvarchar(120) NULL,
        TradeName nvarchar(500) NULL,
        ReceivedQuantity decimal(19,4) NOT NULL,
        InboundShipment nvarchar(150) NULL,
        ASNLine nvarchar(100) NULL,
        SupplierName nvarchar(500) NULL,
        ReceivedDate datetime2 NULL,
        CreatedAt datetime2 NOT NULL
            CONSTRAINT DF_ReceiptEvents_CreatedAt DEFAULT SYSUTCDATETIME(),
        CONSTRAINT PK_ReceiptEvents PRIMARY KEY (EventKey)
    );
END;

IF OBJECT_ID('dbo.DispatchEvents', 'U') IS NULL
BEGIN
    CREATE TABLE dbo.DispatchEvents
    (
        EventKey varchar(64) NOT NULL,
        BN nvarchar(120) NOT NULL,
        ExpiryMonthKey char(7) NOT NULL,
        ExpiryDate date NULL,
        GenericItemNumber nvarchar(120) NOT NULL,
        TradeItemNumber nvarchar(120) NULL,
        TradeName nvarchar(500) NULL,
        DispatchedQuantity decimal(19,4) NOT NULL,
        ToAddress nvarchar(500) NULL,
        SalesOrderNumber nvarchar(150) NULL,
        OrderLine nvarchar(100) NULL,
        DispatchDate datetime2 NULL,
        CreatedAt datetime2 NOT NULL
            CONSTRAINT DF_DispatchEvents_CreatedAt DEFAULT SYSUTCDATETIME(),
        CONSTRAINT PK_DispatchEvents PRIMARY KEY (EventKey)
    );
END;

IF OBJECT_ID('dbo.BatchMaster', 'U') IS NULL
BEGIN
    CREATE TABLE dbo.BatchMaster
    (
        BN nvarchar(120) NOT NULL,
        ExpiryMonthKey char(7) NOT NULL,
        ExpiryDate date NULL,
        GenericItemNumber nvarchar(120) NOT NULL,
        TradeItemNumber nvarchar(120) NULL,
        TradeName nvarchar(500) NULL,
        GTIN nvarchar(20) NULL,
        DrugName nvarchar(500) NULL,
        TotalReceiveQty decimal(19,4) NOT NULL
            CONSTRAINT DF_BatchMaster_TotalReceiveQty DEFAULT 0,
        TotalDispatchedQty decimal(19,4) NOT NULL
            CONSTRAINT DF_BatchMaster_TotalDispatchedQty DEFAULT 0,
        ReceiveRuns int NOT NULL
            CONSTRAINT DF_BatchMaster_ReceiveRuns DEFAULT 0,
        DispatchRuns int NOT NULL
            CONSTRAINT DF_BatchMaster_DispatchRuns DEFAULT 0,
        FirstReceivedDate datetime2 NULL,
        LastReceivedDate datetime2 NULL,
        FirstDispatchDate datetime2 NULL,
        LastDispatchDate datetime2 NULL,
        GenericExistsInSFDA nvarchar(30) NOT NULL
            CONSTRAINT DF_BatchMaster_GenericExistsInSFDA DEFAULT 'Yes',
        LastUpdated datetime2 NOT NULL
            CONSTRAINT DF_BatchMaster_LastUpdated DEFAULT SYSUTCDATETIME(),
        CONSTRAINT PK_BatchMaster PRIMARY KEY
        (
            BN,
            ExpiryMonthKey,
            GenericItemNumber
        )
    );
END;

IF COL_LENGTH('dbo.BatchMaster', 'TradeItemNumber') IS NULL
BEGIN
    ALTER TABLE dbo.BatchMaster
    ADD TradeItemNumber nvarchar(120) NULL;
END;

IF COL_LENGTH('dbo.BatchMaster', 'TradeName') IS NULL
BEGIN
    ALTER TABLE dbo.BatchMaster
    ADD TradeName nvarchar(500) NULL;
END;

IF COL_LENGTH('dbo.BatchMaster', 'GenericExistsInSFDA') IS NOT NULL
BEGIN
    ALTER TABLE dbo.BatchMaster
    ALTER COLUMN GenericExistsInSFDA nvarchar(30) NOT NULL;
END;

IF NOT EXISTS
(
    SELECT 1
    FROM sys.indexes
    WHERE name = 'IX_ReceiptEvents_Batch'
      AND object_id = OBJECT_ID('dbo.ReceiptEvents')
)
BEGIN
    CREATE INDEX IX_ReceiptEvents_Batch
        ON dbo.ReceiptEvents
        (
            BN,
            ExpiryMonthKey,
            GenericItemNumber
        )
        INCLUDE
        (
            ReceivedQuantity,
            ReceivedDate,
            TradeItemNumber,
            TradeName
        );
END;

IF NOT EXISTS
(
    SELECT 1
    FROM sys.indexes
    WHERE name = 'IX_DispatchEvents_Batch'
      AND object_id = OBJECT_ID('dbo.DispatchEvents')
)
BEGIN
    CREATE INDEX IX_DispatchEvents_Batch
        ON dbo.DispatchEvents
        (
            BN,
            ExpiryMonthKey,
            GenericItemNumber
        )
        INCLUDE
        (
            DispatchedQuantity,
            DispatchDate,
            ToAddress,
            TradeItemNumber,
            TradeName
        );
END;

IF NOT EXISTS
(
    SELECT 1
    FROM sys.indexes
    WHERE name = 'IX_DispatchEvents_Allocation'
      AND object_id = OBJECT_ID('dbo.DispatchEvents')
)
BEGIN
    CREATE INDEX IX_DispatchEvents_Allocation
        ON dbo.DispatchEvents
        (
            BN,
            ExpiryMonthKey,
            GenericItemNumber,
            DispatchDate
        )
        INCLUDE
        (
            DispatchedQuantity,
            ToAddress,
            SalesOrderNumber,
            OrderLine
        );
END;
"""


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
    """Advance through every result set and row-count message.

    SQL Server may return non-query result messages for multi-statement
    batches. Consuming them prevents later fetch operations from failing with:
    "No results. Previous SQL was not a query."
    """

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
    """Return the Inserted value from a multi-statement SQL batch."""

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
            cursor.execute(_SCHEMA_SQL)
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


def replace_batch_master(master: pd.DataFrame) -> None:
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
            (SELECT COUNT_BIG(*) FROM dbo.BatchMaster) AS BatchMasterRows;
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
    }
