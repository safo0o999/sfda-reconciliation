import json
import logging
import os
import time
import threading
from pathlib import Path
from typing import Any, Callable, Dict, Iterable, List, Optional, Sequence, Set, Tuple

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
        from engine.warehouse_context import (
            current_historical_build_id,
            current_warehouse_id,
            historical_maintenance_enabled,
        )

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

        # Historical tables are versioned independently from warehouse RLS.
        # A rebuild may bind to a not-yet-active BuildID while ordinary requests
        # automatically resolve to the warehouse's currently active build.
        build_id = str(current_historical_build_id() or '').strip()
        maintenance = bool(historical_maintenance_enabled())
        cursor.execute(
            "EXEC sys.sp_set_session_context @key=N'HistoricalBuildMaintenance', @value=?;",
            1 if maintenance else 0,
        )
        if build_id:
            cursor.execute(
                "EXEC sys.sp_set_session_context @key=N'HistoricalBuildID', @value=?;",
                build_id,
            )
        else:
            try:
                row = cursor.execute(
                    """
                    SELECT TOP (1) BuildID
                    FROM dbo.HistoricalBuildVersions
                    WHERE WarehouseID = ? AND IsActive = 1
                    ORDER BY ActivatedAt DESC, CreatedAt DESC;
                    """,
                    warehouse_id,
                ).fetchone()
                active_build_id = str(row[0] or '').strip() if row else ''
                if active_build_id:
                    cursor.execute(
                        "EXEC sys.sp_set_session_context @key=N'HistoricalBuildID', @value=?;",
                        active_build_id,
                    )
            except pyodbc.Error:
                # Backward-compatible before migration 002 is applied.
                pass

        return connection


def _load_schema_sql() -> str:
    """Load all idempotent SQL migrations in filename order."""

    sql_dir = Path(__file__).resolve().parent.parent / "sql"
    paths = sorted(sql_dir.glob("*.sql"))
    if not paths:
        raise RuntimeError(f"Database schema folder contains no SQL migrations: {sql_dir}")

    batches = []
    for schema_path in paths:
        text = schema_path.read_text(encoding="utf-8").strip()
        if text:
            batches.append(f"-- BEGIN {schema_path.name}\n{text}\n-- END {schema_path.name}")

    if not batches:
        raise RuntimeError(f"Database migration files are empty: {sql_dir}")

    return "\n\n".join(batches)


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
            "PasswordResetTokenHash",
            "PasswordResetExpiresAt",
            "PasswordResetStatus",
            "PasswordResetRequestedAt",
            "PasswordResetApprovedAt",
            "PasswordResetApprovedBy",
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


def _package_size(
    row: Dict[str, Any],
    name: str = "PackageSize",
) -> float:
    """Return a strictly positive operational PackageSize.

    Business rule: Pack Size must never be stored as zero. Any missing,
    invalid, null, or non-positive value falls back to 1.
    """
    value = _number(row, name, 1.0)
    return float(value) if float(value) > 0 else 1.0


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
    """Namespace deterministic event keys by warehouse and historical build.

    Legacy BuildIDs preserve the old key format so migration does not invalidate
    existing de-duplication. New rebuild generations hash WarehouseID + BuildID +
    EventKey into the existing 64-character EventKey column, allowing multiple
    historical generations to coexist without PK collisions.
    """
    import hashlib

    from engine.warehouse_context import current_historical_build_id, current_warehouse_id

    key = str(value or "").strip()
    warehouse_id = int(current_warehouse_id())
    build_id = str(current_historical_build_id() or "").strip()

    if not key:
        return key

    is_legacy = (not build_id) or build_id.upper().startswith("LEGACY-")
    if is_legacy:
        if warehouse_id == 1:
            return key
        return hashlib.sha256(f"W{warehouse_id}|{key}".encode("utf-8")).hexdigest()

    return hashlib.sha256(
        f"W{warehouse_id}|B{build_id}|{key}".encode("utf-8")
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
        _text(row, "Custody"),
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



def remove_excluded_historical_keys(
    receipt_keys: List[Dict[str, Any]],
    dispatch_keys: List[Dict[str, Any]],
) -> Dict[str, int]:
    """Remove legacy rows for keys explicitly excluded by the current upload.

    Performance note:
    keys are staged once into temporary tables and all cleanup is executed with
    set-based DELETE statements.  The business rule is unchanged:
      * Receipt exclusions = Laboratory Supplies from ASN.
      * Dispatch exclusions = Custody=Biochemicals from Full Dispatch.
    """
    initialize_database()

    def _clean_keys(items: List[Dict[str, Any]]) -> list[tuple[str, str, str]]:
        return sorted({
            (
                str(item.get("BN") or "").strip(),
                str(item.get("Expiry Month Key") or "").strip(),
                str(item.get("Generic Item Number") or "").strip(),
            )
            for item in (items or [])
            if str(item.get("BN") or "").strip()
            and str(item.get("Expiry Month Key") or "").strip()
            and str(item.get("Generic Item Number") or "").strip()
        })

    receipt = _clean_keys(receipt_keys)
    dispatch = _clean_keys(dispatch_keys)
    deleted = {
        "receipt_events": 0,
        "dispatch_events": 0,
        "supplier_history": 0,
        "customer_history": 0,
        "batch_master": 0,
    }

    if not receipt and not dispatch:
        return deleted

    with Database().connect() as connection:
        cursor = connection.cursor()
        try:
            cursor.execute(r"""
                CREATE TABLE #ExcludedReceiptKeys
                (
                    BN nvarchar(255) NOT NULL,
                    ExpiryMonthKey nvarchar(20) NOT NULL,
                    GenericItemNumber nvarchar(255) NOT NULL,
                    PRIMARY KEY (BN, ExpiryMonthKey, GenericItemNumber)
                );
                CREATE TABLE #ExcludedDispatchKeys
                (
                    BN nvarchar(255) NOT NULL,
                    ExpiryMonthKey nvarchar(20) NOT NULL,
                    GenericItemNumber nvarchar(255) NOT NULL,
                    PRIMARY KEY (BN, ExpiryMonthKey, GenericItemNumber)
                );
            """)
            cursor.fast_executemany = True
            if receipt:
                cursor.executemany(
                    "INSERT INTO #ExcludedReceiptKeys (BN, ExpiryMonthKey, GenericItemNumber) VALUES (?, ?, ?);",
                    receipt,
                )
            if dispatch:
                cursor.executemany(
                    "INSERT INTO #ExcludedDispatchKeys (BN, ExpiryMonthKey, GenericItemNumber) VALUES (?, ?, ?);",
                    dispatch,
                )

            if receipt:
                cursor.execute(r"""
                    DELETE r
                    FROM dbo.ReceiptEvents r
                    INNER JOIN #ExcludedReceiptKeys k
                        ON k.BN = r.BN
                       AND k.ExpiryMonthKey = r.ExpiryMonthKey
                       AND k.GenericItemNumber = r.GenericItemNumber;
                """)
                deleted["receipt_events"] = max(0, int(cursor.rowcount or 0))

                cursor.execute(r"""
                    DELETE sh
                    FROM dbo.SupplierHistory sh
                    INNER JOIN #ExcludedReceiptKeys k
                        ON k.BN = sh.BN
                       AND k.ExpiryMonthKey = sh.ExpiryMonthKey
                       AND k.GenericItemNumber = sh.GenericItemNumber;
                """)
                deleted["supplier_history"] = max(0, int(cursor.rowcount or 0))

            if dispatch:
                cursor.execute(r"""
                    DELETE d
                    FROM dbo.DispatchEvents d
                    INNER JOIN #ExcludedDispatchKeys k
                        ON k.BN = d.BN
                       AND k.ExpiryMonthKey = d.ExpiryMonthKey
                       AND k.GenericItemNumber = d.GenericItemNumber;
                """)
                deleted["dispatch_events"] = max(0, int(cursor.rowcount or 0))

                cursor.execute(r"""
                    DELETE ch
                    FROM dbo.CustomerHistory ch
                    INNER JOIN #ExcludedDispatchKeys k
                        ON k.BN = ch.BN
                       AND k.ExpiryMonthKey = ch.ExpiryMonthKey
                       AND k.GenericItemNumber = ch.GenericItemNumber;
                """)
                deleted["customer_history"] = max(0, int(cursor.rowcount or 0))

            cursor.execute(r"""
                DELETE bm
                FROM dbo.BatchMaster bm
                WHERE EXISTS
                (
                    SELECT 1
                    FROM #ExcludedReceiptKeys r
                    WHERE r.BN = bm.BN
                      AND r.ExpiryMonthKey = bm.ExpiryMonthKey
                      AND r.GenericItemNumber = bm.GenericItemNumber
                )
                OR EXISTS
                (
                    SELECT 1
                    FROM #ExcludedDispatchKeys d
                    WHERE d.BN = bm.BN
                      AND d.ExpiryMonthKey = bm.ExpiryMonthKey
                      AND d.GenericItemNumber = bm.GenericItemNumber
                );
            """)
            deleted["batch_master"] = max(0, int(cursor.rowcount or 0))

            connection.commit()
        except Exception:
            connection.rollback()
            raise

    logger.info(
        "Historical scope cleanup completed. receipt_keys=%s dispatch_keys=%s deleted=%s",
        len(receipt),
        len(dispatch),
        deleted,
    )
    return deleted


def _json_safe_event_value(value: Any) -> Any:
    """Convert prepared SQL parameter values into compact JSON-safe scalars."""
    if value is None:
        return None
    try:
        if pd.isna(value):
            return None
    except Exception:
        pass

    if isinstance(value, pd.Timestamp):
        return value.isoformat()

    # datetime/date and numpy scalar values expose one of these helpers.
    isoformat = getattr(value, "isoformat", None)
    if callable(isoformat) and not isinstance(value, (str, bytes)):
        try:
            return isoformat()
        except Exception:
            pass

    item = getattr(value, "item", None)
    if callable(item):
        try:
            value = item()
        except Exception:
            pass

    if isinstance(value, (str, int, float, bool)):
        return value
    return str(value)


def _event_json_payload(rows: Sequence[Tuple[Any, ...]]) -> str:
    return json.dumps(
        [[_json_safe_event_value(value) for value in row] for row in rows],
        ensure_ascii=False,
        separators=(",", ":"),
    )


def append_events(
    receipt_rows: List[Dict[str, Any]],
    dispatch_rows: List[Dict[str, Any]],
    *,
    assume_empty: bool = False,
) -> Dict[str, Any]:
    """Append receipt/dispatch events with idempotent bulk SQL de-duplication.

    Historical Append now prefers a chunked ``OPENJSON`` bulk path.  One JSON
    payload represents thousands of rows, so the Function worker no longer needs
    to send tens of thousands of ODBC parameter rows through a temporary staging
    table before SQL can perform the de-duplication check.  The target EventKey
    semantics, WarehouseID/BuildID scope, and durable event tables are unchanged.

    If OPENJSON is unavailable or a deployment temporarily runs against an older
    SQL compatibility level, the function automatically rolls back and retries
    with the previous temp-table/fast_executemany path.  Re-running the same file
    remains safe because both paths use the same EventKey anti-join.
    """

    initialize_database()
    from engine.warehouse_context import current_warehouse_id

    warehouse_id = int(current_warehouse_id())
    started_at = time.perf_counter()
    phase_timings: Dict[str, float] = {}

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
            SalesOrderNumber, OrderLine, DispatchDate, Custody
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
    """

    receipt_rows = [
        row
        for row in (receipt_rows or [])
        if (
            "".join(
                ch
                for ch in str(row.get("Item Family Group") or "").upper()
                if ch.isalnum()
            )
            != "LABORATORYSUPPLIES"
        )
    ]
    dispatch_rows = [
        row
        for row in (dispatch_rows or [])
        if (
            "".join(
                ch
                for ch in str(row.get("Custody") or "").upper()
                if ch.isalnum()
            )
            != "BIOCHEMICALS"
        )
    ]

    from engine.warehouse_context import current_historical_build_id, historical_build_scope
    active_build_id = (
        str(current_historical_build_id() or "").strip()
        or get_active_historical_build_id(warehouse_id)
    )

    prep_started = time.perf_counter()
    with historical_build_scope(active_build_id):
        prepared_receipts = _deduplicate_parameters(receipt_rows, _receipt_parameters)
        prepared_dispatches = _deduplicate_parameters(dispatch_rows, _dispatch_parameters)
    phase_timings["prepare_parameters"] = round(time.perf_counter() - prep_started, 3)

    # The duplicate probe may narrow these lists to only missing EventKeys.
    # Durable target INSERTs still keep their anti-join for race-safe idempotency.
    receipts_to_save = prepared_receipts
    dispatches_to_save = prepared_dispatches

    logger.info(
        "Bulk event save started. WarehouseID=%s BuildID=%s prepared_receipts=%s "
        "prepared_dispatches=%s assume_empty=%s",
        warehouse_id,
        active_build_id,
        len(prepared_receipts),
        len(prepared_dispatches),
        assume_empty,
    )

    inserted_receipts = 0
    inserted_dispatches = 0
    save_mode = "direct" if assume_empty else "openjson"

    receipt_json_sql = r"""
        ;WITH Parsed AS
        (
            SELECT
                CONVERT(varchar(64), JSON_VALUE(j.value, '$[0]')) AS EventKey,
                CONVERT(nvarchar(255), JSON_VALUE(j.value, '$[1]')) AS BN,
                CONVERT(char(7), JSON_VALUE(j.value, '$[2]')) AS ExpiryMonthKey,
                TRY_CONVERT(date, JSON_VALUE(j.value, '$[3]')) AS ExpiryDate,
                CONVERT(nvarchar(255), JSON_VALUE(j.value, '$[4]')) AS GenericItemNumber,
                JSON_VALUE(j.value, '$[5]') AS TradeItemNumber,
                JSON_VALUE(j.value, '$[6]') AS TradeName,
                TRY_CONVERT(decimal(38, 6), JSON_VALUE(j.value, '$[7]')) AS ReceivedQuantity,
                JSON_VALUE(j.value, '$[8]') AS InboundShipment,
                JSON_VALUE(j.value, '$[9]') AS ASNLine,
                JSON_VALUE(j.value, '$[10]') AS SupplierName,
                JSON_VALUE(j.value, '$[11]') AS SupplierCode,
                JSON_VALUE(j.value, '$[12]') AS Description,
                JSON_VALUE(j.value, '$[13]') AS ItemFamilyGroup,
                TRY_CONVERT(datetime2(7), JSON_VALUE(j.value, '$[14]')) AS ReceivedDate
            FROM OPENJSON(CAST(? AS nvarchar(max))) AS j
        )
        INSERT INTO dbo.ReceiptEvents
        (
            EventKey, BN, ExpiryMonthKey, ExpiryDate, GenericItemNumber,
            TradeItemNumber, TradeName, ReceivedQuantity, InboundShipment,
            ASNLine, SupplierName, SupplierCode, Description, ItemFamilyGroup,
            ReceivedDate
        )
        SELECT
            s.EventKey, s.BN, s.ExpiryMonthKey, s.ExpiryDate, s.GenericItemNumber,
            s.TradeItemNumber, s.TradeName, s.ReceivedQuantity, s.InboundShipment,
            s.ASNLine, s.SupplierName, s.SupplierCode, s.Description, s.ItemFamilyGroup,
            s.ReceivedDate
        FROM Parsed AS s
        WHERE s.EventKey IS NOT NULL
          AND NOT EXISTS
          (
              SELECT 1
              FROM dbo.ReceiptEvents AS t WITH (UPDLOCK)
              WHERE t.WarehouseID = ?
                AND t.BuildID = ?
                AND t.EventKey = s.EventKey
          );
    """

    dispatch_json_sql = r"""
        ;WITH Parsed AS
        (
            SELECT
                CONVERT(varchar(64), JSON_VALUE(j.value, '$[0]')) AS EventKey,
                CONVERT(nvarchar(255), JSON_VALUE(j.value, '$[1]')) AS BN,
                CONVERT(char(7), JSON_VALUE(j.value, '$[2]')) AS ExpiryMonthKey,
                TRY_CONVERT(date, JSON_VALUE(j.value, '$[3]')) AS ExpiryDate,
                CONVERT(nvarchar(255), JSON_VALUE(j.value, '$[4]')) AS GenericItemNumber,
                JSON_VALUE(j.value, '$[5]') AS TradeItemNumber,
                JSON_VALUE(j.value, '$[6]') AS TradeName,
                TRY_CONVERT(decimal(38, 6), JSON_VALUE(j.value, '$[7]')) AS DispatchedQuantity,
                JSON_VALUE(j.value, '$[8]') AS ToAddress,
                JSON_VALUE(j.value, '$[9]') AS SalesOrderNumber,
                JSON_VALUE(j.value, '$[10]') AS OrderLine,
                TRY_CONVERT(datetime2(7), JSON_VALUE(j.value, '$[11]')) AS DispatchDate,
                JSON_VALUE(j.value, '$[12]') AS Custody
            FROM OPENJSON(CAST(? AS nvarchar(max))) AS j
        )
        INSERT INTO dbo.DispatchEvents
        (
            EventKey, BN, ExpiryMonthKey, ExpiryDate, GenericItemNumber,
            TradeItemNumber, TradeName, DispatchedQuantity, ToAddress,
            SalesOrderNumber, OrderLine, DispatchDate, Custody
        )
        SELECT
            s.EventKey, s.BN, s.ExpiryMonthKey, s.ExpiryDate, s.GenericItemNumber,
            s.TradeItemNumber, s.TradeName, s.DispatchedQuantity, s.ToAddress,
            s.SalesOrderNumber, s.OrderLine, s.DispatchDate, s.Custody
        FROM Parsed AS s
        WHERE s.EventKey IS NOT NULL
          AND NOT EXISTS
          (
              SELECT 1
              FROM dbo.DispatchEvents AS t WITH (UPDLOCK)
              WHERE t.WarehouseID = ?
                AND t.BuildID = ?
                AND t.EventKey = s.EventKey
          );
    """

    def _missing_prepared_event_keys() -> tuple[Set[str], Set[str]]:
        """Return only EventKeys missing from the active durable build.

        This is both the retry fast path and the partial-duplicate optimization.
        Large multi-month uploads often contain a mix of already-durable rows and
        genuinely new rows.  The old boolean probe could only detect the
        all-duplicate case, so mixed uploads still sent every prepared row through
        OPENJSON.  Returning missing keys lets the JSON stage carry only rows that
        can actually be inserted.
        """
        with Database().connect() as connection:
            cursor = connection.cursor()
            try:
                cursor.execute(r"""
                    CREATE TABLE #ReceiptEventKeyProbe
                    (
                        EventKey varchar(64) NOT NULL PRIMARY KEY
                    );

                    CREATE TABLE #DispatchEventKeyProbe
                    (
                        EventKey varchar(64) NOT NULL PRIMARY KEY
                    );
                """)
                cursor.fast_executemany = True
                if prepared_receipts:
                    cursor.executemany(
                        "INSERT INTO #ReceiptEventKeyProbe (EventKey) VALUES (?);",
                        [(str(row[0]),) for row in prepared_receipts],
                    )
                if prepared_dispatches:
                    cursor.executemany(
                        "INSERT INTO #DispatchEventKeyProbe (EventKey) VALUES (?);",
                        [(str(row[0]),) for row in prepared_dispatches],
                    )
                cursor.fast_executemany = False

                missing_receipts: Set[str] = set()
                missing_dispatches: Set[str] = set()

                if prepared_receipts:
                    cursor.execute(
                        r"""
                        SELECT s.EventKey
                        FROM #ReceiptEventKeyProbe AS s
                        WHERE NOT EXISTS
                        (
                            SELECT 1
                            FROM dbo.ReceiptEvents AS t
                            WHERE t.WarehouseID = ?
                              AND t.BuildID = ?
                              AND t.EventKey = s.EventKey
                        );
                        """,
                        warehouse_id,
                        active_build_id,
                    )
                    missing_receipts = {str(row[0]) for row in cursor.fetchall()}

                if prepared_dispatches:
                    cursor.execute(
                        r"""
                        SELECT s.EventKey
                        FROM #DispatchEventKeyProbe AS s
                        WHERE NOT EXISTS
                        (
                            SELECT 1
                            FROM dbo.DispatchEvents AS t
                            WHERE t.WarehouseID = ?
                              AND t.BuildID = ?
                              AND t.EventKey = s.EventKey
                        );
                        """,
                        warehouse_id,
                        active_build_id,
                    )
                    missing_dispatches = {str(row[0]) for row in cursor.fetchall()}

                return missing_receipts, missing_dispatches
            finally:
                cursor.close()

    def _save_openjson() -> tuple[int, int]:
        """Stage JSON payloads first, then touch each target event table once.

        The previous OPENJSON path performed a target-table NOT EXISTS probe for
        every 2,000-row JSON chunk.  On large mixed append files that caused the
        same ReceiptEvents / DispatchEvents indexes to be revisited dozens of
        times.  This path keeps the efficient JSON transport but separates it
        from durable-table de-duplication:

        1. Load all prepared rows into session-local temp staging tables.
        2. Build one EventKey index per stage.
        3. Execute one anti-join INSERT into ReceiptEvents and one into
           DispatchEvents.

        EventKey semantics, WarehouseID/BuildID scoping, and retry safety remain
        unchanged.
        """
        receipt_inserted = 0
        dispatch_inserted = 0
        json_batch_size = max(
            250,
            int(os.getenv("SFDA_APPEND_JSON_BATCH_SIZE", "4000") or 4000),
        )

        receipt_stage_json_sql = r"""
            ;WITH Parsed AS
            (
                SELECT
                    CONVERT(varchar(64), JSON_VALUE(j.value, '$[0]')) AS EventKey,
                    CONVERT(nvarchar(255), JSON_VALUE(j.value, '$[1]')) AS BN,
                    CONVERT(char(7), JSON_VALUE(j.value, '$[2]')) AS ExpiryMonthKey,
                    TRY_CONVERT(date, JSON_VALUE(j.value, '$[3]')) AS ExpiryDate,
                    CONVERT(nvarchar(255), JSON_VALUE(j.value, '$[4]')) AS GenericItemNumber,
                    JSON_VALUE(j.value, '$[5]') AS TradeItemNumber,
                    JSON_VALUE(j.value, '$[6]') AS TradeName,
                    TRY_CONVERT(decimal(38, 6), JSON_VALUE(j.value, '$[7]')) AS ReceivedQuantity,
                    JSON_VALUE(j.value, '$[8]') AS InboundShipment,
                    JSON_VALUE(j.value, '$[9]') AS ASNLine,
                    JSON_VALUE(j.value, '$[10]') AS SupplierName,
                    JSON_VALUE(j.value, '$[11]') AS SupplierCode,
                    JSON_VALUE(j.value, '$[12]') AS Description,
                    JSON_VALUE(j.value, '$[13]') AS ItemFamilyGroup,
                    TRY_CONVERT(datetime2(7), JSON_VALUE(j.value, '$[14]')) AS ReceivedDate
                FROM OPENJSON(CAST(? AS nvarchar(max))) AS j
            )
            INSERT INTO #ReceiptEventJsonStage
            (
                EventKey, BN, ExpiryMonthKey, ExpiryDate, GenericItemNumber,
                TradeItemNumber, TradeName, ReceivedQuantity, InboundShipment,
                ASNLine, SupplierName, SupplierCode, Description, ItemFamilyGroup,
                ReceivedDate
            )
            SELECT
                EventKey, BN, ExpiryMonthKey, ExpiryDate, GenericItemNumber,
                TradeItemNumber, TradeName, ReceivedQuantity, InboundShipment,
                ASNLine, SupplierName, SupplierCode, Description, ItemFamilyGroup,
                ReceivedDate
            FROM Parsed
            WHERE EventKey IS NOT NULL;
        """

        dispatch_stage_json_sql = r"""
            ;WITH Parsed AS
            (
                SELECT
                    CONVERT(varchar(64), JSON_VALUE(j.value, '$[0]')) AS EventKey,
                    CONVERT(nvarchar(255), JSON_VALUE(j.value, '$[1]')) AS BN,
                    CONVERT(char(7), JSON_VALUE(j.value, '$[2]')) AS ExpiryMonthKey,
                    TRY_CONVERT(date, JSON_VALUE(j.value, '$[3]')) AS ExpiryDate,
                    CONVERT(nvarchar(255), JSON_VALUE(j.value, '$[4]')) AS GenericItemNumber,
                    JSON_VALUE(j.value, '$[5]') AS TradeItemNumber,
                    JSON_VALUE(j.value, '$[6]') AS TradeName,
                    TRY_CONVERT(decimal(38, 6), JSON_VALUE(j.value, '$[7]')) AS DispatchedQuantity,
                    JSON_VALUE(j.value, '$[8]') AS ToAddress,
                    JSON_VALUE(j.value, '$[9]') AS SalesOrderNumber,
                    JSON_VALUE(j.value, '$[10]') AS OrderLine,
                    TRY_CONVERT(datetime2(7), JSON_VALUE(j.value, '$[11]')) AS DispatchDate,
                    JSON_VALUE(j.value, '$[12]') AS Custody
                FROM OPENJSON(CAST(? AS nvarchar(max))) AS j
            )
            INSERT INTO #DispatchEventJsonStage
            (
                EventKey, BN, ExpiryMonthKey, ExpiryDate, GenericItemNumber,
                TradeItemNumber, TradeName, DispatchedQuantity, ToAddress,
                SalesOrderNumber, OrderLine, DispatchDate, Custody
            )
            SELECT
                EventKey, BN, ExpiryMonthKey, ExpiryDate, GenericItemNumber,
                TradeItemNumber, TradeName, DispatchedQuantity, ToAddress,
                SalesOrderNumber, OrderLine, DispatchDate, Custody
            FROM Parsed
            WHERE EventKey IS NOT NULL;
        """

        with Database().connect() as connection:
            cursor = connection.cursor()
            try:
                stage_create_started = time.perf_counter()
                cursor.execute(r"""
                    SELECT TOP (0)
                        EventKey, BN, ExpiryMonthKey, ExpiryDate, GenericItemNumber,
                        TradeItemNumber, TradeName, ReceivedQuantity, InboundShipment,
                        ASNLine, SupplierName, SupplierCode, Description, ItemFamilyGroup,
                        ReceivedDate
                    INTO #ReceiptEventJsonStage
                    FROM dbo.ReceiptEvents;

                    SELECT TOP (0)
                        EventKey, BN, ExpiryMonthKey, ExpiryDate, GenericItemNumber,
                        TradeItemNumber, TradeName, DispatchedQuantity, ToAddress,
                        SalesOrderNumber, OrderLine, DispatchDate, Custody
                    INTO #DispatchEventJsonStage
                    FROM dbo.DispatchEvents;
                """)
                phase_timings["openjson_stage_create"] = round(
                    time.perf_counter() - stage_create_started, 3
                )

                receipt_started = time.perf_counter()
                for row_batch in _chunks(receipts_to_save, json_batch_size):
                    cursor.execute(
                        receipt_stage_json_sql,
                        _event_json_payload(row_batch),
                    )
                phase_timings["openjson_stage_receipts"] = round(
                    time.perf_counter() - receipt_started, 3
                )

                dispatch_started = time.perf_counter()
                for row_batch in _chunks(dispatches_to_save, json_batch_size):
                    cursor.execute(
                        dispatch_stage_json_sql,
                        _event_json_payload(row_batch),
                    )
                phase_timings["openjson_stage_dispatches"] = round(
                    time.perf_counter() - dispatch_started, 3
                )

                index_started = time.perf_counter()
                if receipts_to_save:
                    cursor.execute(
                        "CREATE UNIQUE CLUSTERED INDEX IX_ReceiptEventJsonStage_EventKey "
                        "ON #ReceiptEventJsonStage(EventKey);"
                    )
                if dispatches_to_save:
                    cursor.execute(
                        "CREATE UNIQUE CLUSTERED INDEX IX_DispatchEventJsonStage_EventKey "
                        "ON #DispatchEventJsonStage(EventKey);"
                    )
                phase_timings["openjson_stage_indexes"] = round(
                    time.perf_counter() - index_started, 3
                )

                receipt_insert_started = time.perf_counter()
                if receipts_to_save:
                    cursor.execute(
                        r"""
                        INSERT INTO dbo.ReceiptEvents
                        (
                            EventKey, BN, ExpiryMonthKey, ExpiryDate, GenericItemNumber,
                            TradeItemNumber, TradeName, ReceivedQuantity, InboundShipment,
                            ASNLine, SupplierName, SupplierCode, Description, ItemFamilyGroup,
                            ReceivedDate
                        )
                        SELECT
                            s.EventKey, s.BN, s.ExpiryMonthKey, s.ExpiryDate, s.GenericItemNumber,
                            s.TradeItemNumber, s.TradeName, s.ReceivedQuantity, s.InboundShipment,
                            s.ASNLine, s.SupplierName, s.SupplierCode, s.Description, s.ItemFamilyGroup,
                            s.ReceivedDate
                        FROM #ReceiptEventJsonStage AS s
                        WHERE NOT EXISTS
                        (
                            SELECT 1
                            FROM dbo.ReceiptEvents AS t WITH (UPDLOCK, HOLDLOCK)
                            WHERE t.WarehouseID = ?
                              AND t.BuildID = ?
                              AND t.EventKey = s.EventKey
                        );
                        """,
                        warehouse_id,
                        active_build_id,
                    )
                    receipt_inserted = max(0, int(cursor.rowcount or 0))
                phase_timings["openjson_insert_receipts"] = round(
                    time.perf_counter() - receipt_insert_started, 3
                )

                dispatch_insert_started = time.perf_counter()
                if dispatches_to_save:
                    cursor.execute(
                        r"""
                        INSERT INTO dbo.DispatchEvents
                        (
                            EventKey, BN, ExpiryMonthKey, ExpiryDate, GenericItemNumber,
                            TradeItemNumber, TradeName, DispatchedQuantity, ToAddress,
                            SalesOrderNumber, OrderLine, DispatchDate, Custody
                        )
                        SELECT
                            s.EventKey, s.BN, s.ExpiryMonthKey, s.ExpiryDate, s.GenericItemNumber,
                            s.TradeItemNumber, s.TradeName, s.DispatchedQuantity, s.ToAddress,
                            s.SalesOrderNumber, s.OrderLine, s.DispatchDate, s.Custody
                        FROM #DispatchEventJsonStage AS s
                        WHERE NOT EXISTS
                        (
                            SELECT 1
                            FROM dbo.DispatchEvents AS t WITH (UPDLOCK, HOLDLOCK)
                            WHERE t.WarehouseID = ?
                              AND t.BuildID = ?
                              AND t.EventKey = s.EventKey
                        );
                        """,
                        warehouse_id,
                        active_build_id,
                    )
                    dispatch_inserted = max(0, int(cursor.rowcount or 0))
                phase_timings["openjson_insert_dispatches"] = round(
                    time.perf_counter() - dispatch_insert_started, 3
                )

                # Preserve the legacy timing field names used by the dashboard
                # while exposing the new stage/insert split above.
                phase_timings["openjson_receipts"] = round(
                    phase_timings.get("openjson_stage_receipts", 0.0)
                    + phase_timings.get("openjson_insert_receipts", 0.0),
                    3,
                )
                phase_timings["openjson_dispatches"] = round(
                    phase_timings.get("openjson_stage_dispatches", 0.0)
                    + phase_timings.get("openjson_insert_dispatches", 0.0),
                    3,
                )

                commit_started = time.perf_counter()
                connection.commit()
                phase_timings["commit"] = round(
                    time.perf_counter() - commit_started, 3
                )
            except Exception:
                connection.rollback()
                raise
        return receipt_inserted, dispatch_inserted

    def _save_temp_stage() -> tuple[int, int]:
        receipt_inserted = 0
        dispatch_inserted = 0
        with Database().connect() as connection:
            cursor = connection.cursor()
            try:
                stage_create_started = time.perf_counter()
                cursor.execute(r"""
                    SELECT TOP (0)
                        EventKey, BN, ExpiryMonthKey, ExpiryDate, GenericItemNumber,
                        TradeItemNumber, TradeName, ReceivedQuantity, InboundShipment,
                        ASNLine, SupplierName, SupplierCode, Description, ItemFamilyGroup,
                        ReceivedDate
                    INTO #ReceiptEventStage
                    FROM dbo.ReceiptEvents;

                    SELECT TOP (0)
                        EventKey, BN, ExpiryMonthKey, ExpiryDate, GenericItemNumber,
                        TradeItemNumber, TradeName, DispatchedQuantity, ToAddress,
                        SalesOrderNumber, OrderLine, DispatchDate, Custody
                    INTO #DispatchEventStage
                    FROM dbo.DispatchEvents;
                """)
                cursor.execute(
                    "CREATE UNIQUE CLUSTERED INDEX IX_ReceiptEventStage_EventKey ON #ReceiptEventStage(EventKey);"
                )
                cursor.execute(
                    "CREATE UNIQUE CLUSTERED INDEX IX_DispatchEventStage_EventKey ON #DispatchEventStage(EventKey);"
                )
                phase_timings["temp_stage_create"] = round(time.perf_counter() - stage_create_started, 3)

                cursor.fast_executemany = True
                receipt_stage_started = time.perf_counter()
                if prepared_receipts:
                    cursor.executemany(
                        r"""
                        INSERT INTO #ReceiptEventStage
                        (
                            EventKey, BN, ExpiryMonthKey, ExpiryDate, GenericItemNumber,
                            TradeItemNumber, TradeName, ReceivedQuantity, InboundShipment,
                            ASNLine, SupplierName, SupplierCode, Description, ItemFamilyGroup,
                            ReceivedDate
                        )
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
                        """,
                        prepared_receipts,
                    )
                phase_timings["temp_stage_receipts"] = round(time.perf_counter() - receipt_stage_started, 3)

                dispatch_stage_started = time.perf_counter()
                if prepared_dispatches:
                    cursor.executemany(
                        r"""
                        INSERT INTO #DispatchEventStage
                        (
                            EventKey, BN, ExpiryMonthKey, ExpiryDate, GenericItemNumber,
                            TradeItemNumber, TradeName, DispatchedQuantity, ToAddress,
                            SalesOrderNumber, OrderLine, DispatchDate, Custody
                        )
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
                        """,
                        prepared_dispatches,
                    )
                phase_timings["temp_stage_dispatches"] = round(time.perf_counter() - dispatch_stage_started, 3)

                receipt_insert_started = time.perf_counter()
                if prepared_receipts:
                    cursor.execute(
                        r"""
                        INSERT INTO dbo.ReceiptEvents
                        (
                            EventKey, BN, ExpiryMonthKey, ExpiryDate, GenericItemNumber,
                            TradeItemNumber, TradeName, ReceivedQuantity, InboundShipment,
                            ASNLine, SupplierName, SupplierCode, Description, ItemFamilyGroup,
                            ReceivedDate
                        )
                        SELECT
                            s.EventKey, s.BN, s.ExpiryMonthKey, s.ExpiryDate, s.GenericItemNumber,
                            s.TradeItemNumber, s.TradeName, s.ReceivedQuantity, s.InboundShipment,
                            s.ASNLine, s.SupplierName, s.SupplierCode, s.Description, s.ItemFamilyGroup,
                            s.ReceivedDate
                        FROM #ReceiptEventStage s
                        WHERE NOT EXISTS
                        (
                            SELECT 1
                            FROM dbo.ReceiptEvents t WITH (UPDLOCK)
                            WHERE t.WarehouseID = ?
                              AND t.BuildID = ?
                              AND t.EventKey = s.EventKey
                        );
                        """,
                        (warehouse_id, active_build_id),
                    )
                    receipt_inserted = max(0, int(cursor.rowcount or 0))
                phase_timings["temp_insert_receipts"] = round(time.perf_counter() - receipt_insert_started, 3)

                dispatch_insert_started = time.perf_counter()
                if prepared_dispatches:
                    cursor.execute(
                        r"""
                        INSERT INTO dbo.DispatchEvents
                        (
                            EventKey, BN, ExpiryMonthKey, ExpiryDate, GenericItemNumber,
                            TradeItemNumber, TradeName, DispatchedQuantity, ToAddress,
                            SalesOrderNumber, OrderLine, DispatchDate, Custody
                        )
                        SELECT
                            s.EventKey, s.BN, s.ExpiryMonthKey, s.ExpiryDate, s.GenericItemNumber,
                            s.TradeItemNumber, s.TradeName, s.DispatchedQuantity, s.ToAddress,
                            s.SalesOrderNumber, s.OrderLine, s.DispatchDate, s.Custody
                        FROM #DispatchEventStage s
                        WHERE NOT EXISTS
                        (
                            SELECT 1
                            FROM dbo.DispatchEvents t WITH (UPDLOCK)
                            WHERE t.WarehouseID = ?
                              AND t.BuildID = ?
                              AND t.EventKey = s.EventKey
                        );
                        """,
                        (warehouse_id, active_build_id),
                    )
                    dispatch_inserted = max(0, int(cursor.rowcount or 0))
                phase_timings["temp_insert_dispatches"] = round(time.perf_counter() - dispatch_insert_started, 3)

                commit_started = time.perf_counter()
                connection.commit()
                phase_timings["commit"] = round(time.perf_counter() - commit_started, 3)
            except Exception:
                connection.rollback()
                raise
        return receipt_inserted, dispatch_inserted

    if assume_empty:
        with Database().connect() as connection:
            cursor = connection.cursor()
            try:
                direct_started = time.perf_counter()
                inserted_receipts = _bulk_insert_rows(cursor, direct_receipt_sql, prepared_receipts)
                phase_timings["direct_receipts"] = round(time.perf_counter() - direct_started, 3)
                direct_started = time.perf_counter()
                inserted_dispatches = _bulk_insert_rows(cursor, direct_dispatch_sql, prepared_dispatches)
                phase_timings["direct_dispatches"] = round(time.perf_counter() - direct_started, 3)
                commit_started = time.perf_counter()
                connection.commit()
                phase_timings["commit"] = round(time.perf_counter() - commit_started, 3)
            except Exception:
                connection.rollback()
                raise
    else:
        json_enabled = str(os.getenv("SFDA_APPEND_JSON_BULK", "1") or "1").strip().lower() not in {
            "0", "false", "no", "off"
        }
        if json_enabled:
            duplicate_probe_started = time.perf_counter()
            probe_succeeded = False
            missing_receipt_keys: Set[str] = set()
            missing_dispatch_keys: Set[str] = set()
            try:
                missing_receipt_keys, missing_dispatch_keys = _missing_prepared_event_keys()
                probe_succeeded = True
                receipts_to_save = [
                    row for row in prepared_receipts if str(row[0]) in missing_receipt_keys
                ]
                dispatches_to_save = [
                    row for row in prepared_dispatches if str(row[0]) in missing_dispatch_keys
                ]
                phase_timings["duplicate_probe_missing_receipts"] = len(receipts_to_save)
                phase_timings["duplicate_probe_missing_dispatches"] = len(dispatches_to_save)
            except Exception:
                logger.exception(
                    "Historical event duplicate/partial probe failed; continuing with full OPENJSON save. "
                    "WarehouseID=%s BuildID=%s",
                    warehouse_id,
                    active_build_id,
                )
                receipts_to_save = prepared_receipts
                dispatches_to_save = prepared_dispatches
            phase_timings["duplicate_probe"] = round(
                time.perf_counter() - duplicate_probe_started, 3
            )

            if probe_succeeded and not receipts_to_save and not dispatches_to_save:
                save_mode = "duplicate_fast_path"
                inserted_receipts = 0
                inserted_dispatches = 0
            else:
                if probe_succeeded:
                    save_mode = "openjson_partial"
                try:
                    inserted_receipts, inserted_dispatches = _save_openjson()
                except Exception:
                    logger.exception(
                        "OPENJSON historical event bulk save failed; retrying with temp-table fallback. "
                        "WarehouseID=%s BuildID=%s",
                        warehouse_id,
                        active_build_id,
                    )
                    save_mode = "temp_fallback"
                    inserted_receipts, inserted_dispatches = _save_temp_stage()
        else:
            save_mode = "temp"
            inserted_receipts, inserted_dispatches = _save_temp_stage()

    total_seconds = time.perf_counter() - started_at
    phase_timings["total"] = round(total_seconds, 3)
    logger.info(
        "Bulk event save completed in %.2f seconds. mode=%s WarehouseID=%s "
        "new_receipts=%s duplicate_receipts=%s new_dispatches=%s duplicate_dispatches=%s timings=%s",
        total_seconds,
        save_mode,
        warehouse_id,
        inserted_receipts,
        max(0, len(prepared_receipts) - inserted_receipts),
        inserted_dispatches,
        max(0, len(prepared_dispatches) - inserted_dispatches),
        phase_timings,
    )

    return {
        "receipt_events": inserted_receipts,
        "dispatch_events": inserted_dispatches,
        "save_mode": save_mode,
        "timings_seconds": phase_timings,
    }

def get_event_summaries() -> Tuple[pd.DataFrame, pd.DataFrame]:
    """Return warehouse-scoped cumulative summaries for Batch Master.

    This version keeps the same business rules but avoids the expensive
    EligibleReceipt + OUTER APPLY pattern.  One windowed pass calculates both
    receipt totals and the preferred descriptive row for every
    BN + ExpiryMonth + Generic group.
    """

    initialize_database()
    from engine.warehouse_context import current_warehouse_id

    warehouse_id = int(current_warehouse_id())
    started_at = time.perf_counter()

    receipt_sql = r"""
        WITH EligibleReceipt AS
        (
            SELECT *
            FROM dbo.ReceiptEvents
            WHERE WarehouseID = ?
              AND (
                    InboundShipment LIKE 'TRK5060%'
                    OR InboundShipment LIKE 'TRK800%'
                    OR InboundShipment LIKE 'TRK49%'
                  )
              AND REPLACE(REPLACE(REPLACE(
                    UPPER(LTRIM(RTRIM(ISNULL(ItemFamilyGroup, '')))),
                    ' ', ''), '-', ''), '_', '') <> 'LABORATORYSUPPLIES'
        ),
        ReceiptTradeCodes AS
        (
            SELECT
                BN,
                ExpiryMonthKey,
                GenericItemNumber,
                LEFT(
                    STRING_AGG(CONVERT(nvarchar(max), TradeItemNumber), N' | ')
                        WITHIN GROUP (ORDER BY TradeItemNumber),
                    255
                ) AS TradeItemNumber
            FROM
            (
                SELECT DISTINCT
                    BN, ExpiryMonthKey, GenericItemNumber, TradeItemNumber
                FROM EligibleReceipt
                WHERE NULLIF(LTRIM(RTRIM(TradeItemNumber)), '') IS NOT NULL
            ) AS distinct_codes
            GROUP BY BN, ExpiryMonthKey, GenericItemNumber
        ),
        RankedReceipt AS
        (
            SELECT
                r.BN,
                r.ExpiryMonthKey,
                r.ExpiryDate,
                r.GenericItemNumber,
                tc.TradeItemNumber,
                r.TradeName,
                r.Description,
                r.SupplierName,
                r.SupplierCode,
                r.ItemFamilyGroup,
                r.ReceivedQuantity,
                r.ReceivedDate,
                r.EventKey,
                r.InboundShipment,
                ROW_NUMBER() OVER
                (
                    PARTITION BY r.BN, r.ExpiryMonthKey, r.GenericItemNumber
                    ORDER BY
                        CASE
                            WHEN r.InboundShipment LIKE 'TRK5060%' THEN 0
                            WHEN r.InboundShipment LIKE 'TRK800%' THEN 1
                            ELSE 2
                        END,
                        r.ReceivedDate ASC,
                        r.EventKey ASC
                ) AS rn,
                MAX(r.ExpiryDate) OVER
                    (PARTITION BY r.BN, r.ExpiryMonthKey, r.GenericItemNumber) AS ReceiptExpiryDate,
                COUNT_BIG(*) OVER
                    (PARTITION BY r.BN, r.ExpiryMonthKey, r.GenericItemNumber) AS ReceiveRuns,
                SUM(r.ReceivedQuantity) OVER
                    (PARTITION BY r.BN, r.ExpiryMonthKey, r.GenericItemNumber) AS TotalReceiveQty,
                MIN(r.ReceivedDate) OVER
                    (PARTITION BY r.BN, r.ExpiryMonthKey, r.GenericItemNumber) AS FirstReceivedDate,
                MAX(r.ReceivedDate) OVER
                    (PARTITION BY r.BN, r.ExpiryMonthKey, r.GenericItemNumber) AS LastReceivedDate
            FROM EligibleReceipt AS r
            LEFT JOIN ReceiptTradeCodes AS tc
              ON tc.BN = r.BN
             AND tc.ExpiryMonthKey = r.ExpiryMonthKey
             AND tc.GenericItemNumber = r.GenericItemNumber
        )
        SELECT
            BN,
            ExpiryMonthKey AS [Expiry Month Key],
            ReceiptExpiryDate AS [Receipt Expiry Date],
            GenericItemNumber AS [Generic Item Number],
            TradeItemNumber AS [Trade Item Number],
            TradeName AS [Trade Name],
            Description,
            SupplierName AS [Supplier Name],
            SupplierCode AS [Supplier Code],
            ItemFamilyGroup AS [Item Family Group],
            ReceiveRuns AS [Receive Runs],
            TotalReceiveQty AS [Total Receive Qty],
            FirstReceivedDate AS [First Received Date],
            LastReceivedDate AS [Last Received Date]
        FROM RankedReceipt
        WHERE rn = 1;
    """

    dispatch_sql = r"""
        WITH EligibleDispatch AS
        (
            SELECT *
            FROM dbo.DispatchEvents
            WHERE WarehouseID = ?
              AND REPLACE(REPLACE(REPLACE(
                    UPPER(LTRIM(RTRIM(ISNULL(Custody, '')))),
                    ' ', ''), '-', ''), '_', '') <> 'BIOCHEMICALS'
        ),
        DispatchTradeCodes AS
        (
            SELECT
                BN,
                ExpiryMonthKey,
                GenericItemNumber,
                LEFT(
                    STRING_AGG(CONVERT(nvarchar(max), TradeItemNumber), N' | ')
                        WITHIN GROUP (ORDER BY TradeItemNumber),
                    255
                ) AS TradeItemNumber
            FROM
            (
                SELECT DISTINCT
                    BN, ExpiryMonthKey, GenericItemNumber, TradeItemNumber
                FROM EligibleDispatch
                WHERE NULLIF(LTRIM(RTRIM(TradeItemNumber)), '') IS NOT NULL
            ) AS distinct_codes
            GROUP BY BN, ExpiryMonthKey, GenericItemNumber
        )
        SELECT
            d.BN,
            d.ExpiryMonthKey AS [Expiry Month Key],
            MAX(d.ExpiryDate) AS [Dispatch Expiry Date],
            d.GenericItemNumber AS [Generic Item Number],
            tc.TradeItemNumber AS [Trade Item Number],
            MAX(NULLIF(d.TradeName, '')) AS [Trade Name],
            MAX(NULLIF(d.Custody, '')) AS [Custody],
            COUNT_BIG(*) AS [Dispatch Runs],
            SUM(d.DispatchedQuantity) AS [Total Dispatched Qty],
            MIN(d.DispatchDate) AS [First Dispatch Date],
            MAX(d.DispatchDate) AS [Last Dispatch Date]
        FROM EligibleDispatch AS d
        LEFT JOIN DispatchTradeCodes AS tc
          ON tc.BN = d.BN
         AND tc.ExpiryMonthKey = d.ExpiryMonthKey
         AND tc.GenericItemNumber = d.GenericItemNumber
        GROUP BY d.BN, d.ExpiryMonthKey, d.GenericItemNumber, tc.TradeItemNumber;
    """

    with Database().connect() as connection:
        receipt = pd.read_sql(receipt_sql, connection, params=(warehouse_id,))
        dispatch = pd.read_sql(dispatch_sql, connection, params=(warehouse_id,))

    logger.info(
        "Historical event summaries loaded in %.2f seconds. WarehouseID=%s receipt_groups=%s dispatch_groups=%s",
        time.perf_counter() - started_at,
        warehouse_id,
        len(receipt),
        len(dispatch),
    )
    return receipt, dispatch


def get_history_summaries() -> Tuple[pd.DataFrame, pd.DataFrame]:
    """Return warehouse-scoped Supplier and Customer History summaries."""

    initialize_database()
    from engine.warehouse_context import current_warehouse_id

    warehouse_id = int(current_warehouse_id())
    started_at = time.perf_counter()

    supplier_sql = r"""
        SELECT
            SupplierName AS [Supplier Name],
            SupplierCode AS [Supplier Code],
            BN,
            ExpiryMonthKey AS [Expiry Month Key],
            MAX(ExpiryDate) AS [Expiry Date],
            GenericItemNumber AS [Generic Item Number],
            TradeItemNumber AS [Trade Item Number],
            MAX(NULLIF(TradeName, '')) AS [Trade Name],
            MAX(NULLIF(Description, '')) AS [Description],
            MAX(NULLIF(ItemFamilyGroup, '')) AS [Item Family Group],
            SUM(ReceivedQuantity) AS [Received Quantity Each],
            MIN(ReceivedDate) AS [First Received Date],
            MAX(ReceivedDate) AS [Last Received Date]
        FROM dbo.ReceiptEvents
        WHERE WarehouseID = ?
          AND InboundShipment LIKE 'TRK5060%'
          AND REPLACE(REPLACE(REPLACE(
                UPPER(LTRIM(RTRIM(ISNULL(ItemFamilyGroup, '')))),
                ' ', ''), '-', ''), '_', '') <> 'LABORATORYSUPPLIES'
        GROUP BY SupplierName, SupplierCode, BN, ExpiryMonthKey,
                 GenericItemNumber, TradeItemNumber;
    """

    customer_sql = r"""
        SELECT
            ToAddress AS [To Address],
            BN,
            ExpiryMonthKey AS [Expiry Month Key],
            MAX(ExpiryDate) AS [Expiry Date],
            GenericItemNumber AS [Generic Item Number],
            TradeItemNumber AS [Trade Item Number],
            MAX(NULLIF(TradeName, '')) AS [Trade Name],
            MAX(NULLIF(Custody, '')) AS [Custody],
            SUM(DispatchedQuantity) AS [Dispatch Quantity Each],
            MIN(DispatchDate) AS [First Dispatch Date],
            MAX(DispatchDate) AS [Last Dispatch Date]
        FROM dbo.DispatchEvents
        WHERE WarehouseID = ?
          AND REPLACE(REPLACE(REPLACE(
                UPPER(LTRIM(RTRIM(ISNULL(Custody, '')))),
                ' ', ''), '-', ''), '_', '') <> 'BIOCHEMICALS'
        GROUP BY ToAddress, BN, ExpiryMonthKey, GenericItemNumber, TradeItemNumber;
    """

    with Database().connect() as connection:
        supplier = pd.read_sql(supplier_sql, connection, params=(warehouse_id,))
        customer = pd.read_sql(customer_sql, connection, params=(warehouse_id,))

    logger.info(
        "Historical history summaries loaded in %.2f seconds. WarehouseID=%s supplier_rows=%s customer_rows=%s",
        time.perf_counter() - started_at,
        warehouse_id,
        len(supplier),
        len(customer),
    )
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
    if safe_prefix not in {"TRK800", "TRK49", "TRK30"}:
        raise ValueError("Unsupported receipt-history prefix.")

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
                TradeItemNumber,
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
                GenericItemNumber,
                TradeItemNumber
        )
        SELECT
            r.InboundShipment AS [Inbound Shipment],
            r.SupplierName AS [Source Warehouse],
            r.SupplierCode AS [Source Warehouse Code],
            r.BN,
            r.ExpiryMonthKey AS [Expiry Month Key],
            COALESCE(b.ExpiryDate, r.ExpiryDate) AS [Expiry Date],
            r.GenericItemNumber AS [Generic Item Number],
            COALESCE(NULLIF(r.TradeItemNumber, ''), '') AS [Trade Item Number],
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


def get_returns_history_df() -> pd.DataFrame:
    """Return the unified STO + customer return history from durable ASN events.

    The ASN ``Supplier Name`` is the party returning the stock.  Full Dispatch
    later matches it to Customer History ``To Address`` and obtains the GLN from
    the approved warehouse mapping.  This derived view deliberately does not
    mutate ReceiptEvents, DispatchEvents, Batch Master, or Customer History.
    """
    parts: List[pd.DataFrame] = []
    for prefix, return_type in (
        ("TRK49", "STO Return"),
        ("TRK30", "Customer Return"),
    ):
        frame = _get_sto_receipt_history(prefix, sfda_relevant_only=True)
        if frame.empty:
            continue
        frame = frame.copy()
        frame["Return Type"] = return_type
        frame["Return From"] = frame.get("Source Warehouse", "")
        frame["Return From Code"] = frame.get("Source Warehouse Code", "")
        frame["Required Action"] = "Cancel Previous RSD Dispatch"
        parts.append(frame)

    if not parts:
        return pd.DataFrame(columns=[
            "Return Type", "Inbound Shipment", "Return From",
            "Return From Code", "BN", "Expiry Month Key", "Expiry Date",
            "Generic Item Number", "Trade Item Number", "GTIN", "Drug Name", "Trade Description",
            "Description", "Item Family Group", "PackageSize",
            "Received Quantity Each", "Received Quantity Pack",
            "First Received Date", "Last Received Date", "SFDA Match Status",
            "Required Action",
        ])

    result = pd.concat(parts, ignore_index=True, sort=False)
    return result.sort_values(
        ["Last Received Date", "Return Type", "Return From", "BN", "Trade Item Number"],
        kind="stable",
    ).reset_index(drop=True)


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
        _value(r, "Expiry Date"), _package_size(r, "PackageSize"),
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
         Custody, TradeItemNumber, LastUpdated)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, SYSUTCDATETIME());
    """
    rows = [(
        _text(r, "To Address"), _text(r, "GLN"), _text(r, "GTIN"),
        _text(r, "Drug Name"), _text(r, "Generic Item Number"),
        _text(r, "Trade Description"), _text(r, "BN"), _text(r, "Expiry Month Key"),
        _value(r, "Expiry Date"), _package_size(r, "PackageSize"),
        _number(r, "Dispatch Quantity Each"), _number(r, "Dispatch Quantity Pack"),
        _value(r, "First Dispatch Date"), _value(r, "Last Dispatch Date"),
        _text(r, "Custody"), _text(r, "Trade Item Number")
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
                Custody,
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
            Custody,
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
            ?, ?, ?, ?, ?, ?, ?, ?
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
            _package_size(row, "PackageSize"),
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
            _text(row, "Custody"),
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
            Custody,
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
            DispatchDate AS [Dispatch Date],
            Custody
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
    """Deprecated destructive Historical Rebuild reset.

    Historical rebuilds must use the two-phase safe flow in
    ``activate_historical_rebuild``.  This guard intentionally fails closed so
    no legacy or accidentally restored call site can erase live history before
    a replacement dataset has been fully built and validated.
    """
    raise RuntimeError(
        "Legacy reset_history() is disabled. Historical rebuild must use "
        "Build -> Validate -> activate_historical_rebuild()."
    )



def _upsert_historical_build_version(build_id: str, source_job_id: str = "", status: str = "Building") -> None:
    """Create/update one warehouse-scoped historical generation metadata row."""
    initialize_database()
    from engine.warehouse_context import current_warehouse_id, historical_maintenance_scope

    warehouse_id = int(current_warehouse_id())
    build_id = str(build_id or "").strip()
    if not build_id:
        raise ValueError("Historical BuildID is required.")

    with historical_maintenance_scope():
        with Database().connect() as connection:
            cursor = connection.cursor()
            cursor.execute(
                r"""
                IF EXISTS (
                    SELECT 1 FROM dbo.HistoricalBuildVersions
                    WHERE WarehouseID = ? AND BuildID = ?
                )
                BEGIN
                    UPDATE dbo.HistoricalBuildVersions
                    SET Status = ?, SourceJobID = NULLIF(?, N''), UpdatedAt = SYSUTCDATETIME()
                    WHERE WarehouseID = ? AND BuildID = ?;
                END
                ELSE
                BEGIN
                    INSERT INTO dbo.HistoricalBuildVersions
                    (WarehouseID, BuildID, Status, IsActive, SourceJobID, CreatedAt, UpdatedAt)
                    VALUES (?, ?, ?, 0, NULLIF(?, N''), SYSUTCDATETIME(), SYSUTCDATETIME());
                END;
                """,
                warehouse_id, build_id,
                status, source_job_id,
                warehouse_id, build_id,
                warehouse_id, build_id, status, source_job_id,
            )
            connection.commit()


def get_active_historical_build_id(warehouse_id: Optional[int] = None) -> str:
    """Return the active historical generation for one warehouse."""
    initialize_database()
    from engine.warehouse_context import current_warehouse_id, historical_maintenance_scope

    resolved = int(warehouse_id or current_warehouse_id())
    with historical_maintenance_scope():
        with Database().connect() as connection:
            row = connection.cursor().execute(
                r"""
                SELECT TOP (1) BuildID
                FROM dbo.HistoricalBuildVersions
                WHERE WarehouseID = ? AND IsActive = 1
                ORDER BY ActivatedAt DESC, CreatedAt DESC;
                """,
                resolved,
            ).fetchone()
    return str(row[0] or "").strip() if row else ""


def activate_historical_rebuild(
    receipt_rows: List[Dict[str, Any]],
    dispatch_rows: List[Dict[str, Any]],
    master: pd.DataFrame,
    supplier_history: pd.DataFrame,
    customer_history: pd.DataFrame,
    *,
    build_id: str,
    source_job_id: str = "",
    progress_callback: Optional[Callable[[int, str, Dict[str, Any]], None]] = None,
) -> Dict[str, Any]:
    """Persist and activate a new Historical Build without deleting the old one.

    HISTORICAL_VERSIONED_ACTIVATION_V2 writes the complete new generation under
    its BuildID while the current generation remains active. After cardinality
    verification, activation is a tiny metadata switch in HistoricalBuildVersions.
    The user-visible rebuild can therefore finish immediately; old generations
    are removed later by a background cleanup message.
    """
    initialize_database()
    from engine.warehouse_context import (
        current_warehouse_id,
        historical_build_scope,
        historical_maintenance_scope,
    )

    warehouse_id = int(current_warehouse_id())
    build_id = str(build_id or "").strip()
    if not build_id:
        raise ValueError("Historical BuildID is required for versioned activation.")

    started_at = time.perf_counter()
    _upsert_historical_build_version(build_id, source_job_id, "Building")

    def emit(stage: str, extra: Optional[Dict[str, Any]] = None) -> None:
        if progress_callback is None:
            return
        try:
            progress_callback(98, stage, dict(extra or {}))
        except Exception:
            logger.exception("HISTORICAL_VERSIONED_ACTIVATION progress callback failed.")

    try:
        # Bind every SQL connection in this block to the NEW, still-inactive build.
        # Historical Build RLS makes the generation visible to this worker only.
        with historical_build_scope(build_id):
            prepared_receipts = _deduplicate_parameters(receipt_rows or [], _receipt_parameters)
            prepared_dispatches = _deduplicate_parameters(dispatch_rows or [], _dispatch_parameters)

            batch_master_rows = [
                (
                    _text(row, "BN"), _text(row, "Expiry Month Key"), _value(row, "Expiry Date"),
                    _text(row, "Generic Item Number"), _text_with_fallback(row, "Trade Item Number", "Trade Item"),
                    _text_with_fallback(row, "Trade Description", "Trade Name"), _text(row, "GTIN"),
                    _text(row, "Drug Name"), _package_size(row, "PackageSize"),
                    _number_with_fallback(row, "Quantity", "SFDA Quantity"), _number(row, "Active"),
                    _number(row, "Quantity sent pending"), _number(row, "Quantity Receive Pending"),
                    _text(row, "Description"), _text(row, "Item Family Group"), _text(row, "Custody"),
                    _text(row, "Supplier Name"), _text(row, "Supplier Code"),
                    _number_with_fallback(row, "Received Quantity Each", "Total Receive Qty"),
                    _number(row, "Total Dispatched Qty"), _integer(row, "Receive Runs"),
                    _integer(row, "Dispatch Runs"), _value(row, "First Received Date"),
                    _value(row, "Last Received Date"), _value(row, "First Dispatch Date"),
                    _value(row, "Last Dispatch Date"), _text(row, "Generic Exists in SFDA", "Yes") or "Yes",
                    _value(row, "Last Updated", pd.Timestamp.utcnow().tz_localize(None)),
                )
                for row in (master if master is not None else pd.DataFrame()).to_dict(orient="records")
            ]
            supplier_rows = [(
                _text(r, "Supplier Name"), _text(r, "Supplier Code"), _text(r, "GTIN"),
                _text(r, "Drug Name"), _text(r, "Generic Item Number"), _text(r, "Description"),
                _text(r, "Trade Description"), _text(r, "BN"), _text(r, "Expiry Month Key"),
                _value(r, "Expiry Date"), _package_size(r, "PackageSize"), _number(r, "Received Quantity Each"),
                _number(r, "Received Quantity Pack"), _value(r, "First Received Date"),
                _value(r, "Last Received Date"), _text(r, "Item Family Group"), _text(r, "Trade Item Number")
            ) for r in (supplier_history if supplier_history is not None else pd.DataFrame()).to_dict(orient="records")]
            customer_rows = [(
                _text(r, "To Address"), _text(r, "GLN"), _text(r, "GTIN"), _text(r, "Drug Name"),
                _text(r, "Generic Item Number"), _text(r, "Trade Description"), _text(r, "BN"),
                _text(r, "Expiry Month Key"), _value(r, "Expiry Date"), _package_size(r, "PackageSize"),
                _number(r, "Dispatch Quantity Each"), _number(r, "Dispatch Quantity Pack"),
                _value(r, "First Dispatch Date"), _value(r, "Last Dispatch Date"), _text(r, "Custody"),
                _text(r, "Trade Item Number")
            ) for r in (customer_history if customer_history is not None else pd.DataFrame()).to_dict(orient="records")]

            receipt_insert_sql = r"""
                INSERT INTO dbo.ReceiptEvents
                (EventKey, BN, ExpiryMonthKey, ExpiryDate, GenericItemNumber,
                 TradeItemNumber, TradeName, ReceivedQuantity, InboundShipment,
                 ASNLine, SupplierName, SupplierCode, Description, ItemFamilyGroup, ReceivedDate)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
            """
            dispatch_insert_sql = r"""
                INSERT INTO dbo.DispatchEvents
                (EventKey, BN, ExpiryMonthKey, ExpiryDate, GenericItemNumber,
                 TradeItemNumber, TradeName, DispatchedQuantity, ToAddress,
                 SalesOrderNumber, OrderLine, DispatchDate, Custody)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
            """
            master_insert_sql = r"""
                INSERT INTO dbo.BatchMaster
                (BN, ExpiryMonthKey, ExpiryDate, GenericItemNumber, TradeItemNumber,
                 TradeName, GTIN, DrugName, PackageSize, SFDAQuantity, Active,
                 QuantitySentPending, QuantityReceivePending, Description, ItemFamilyGroup,
                 Custody, SupplierName, SupplierCode, TotalReceiveQty, TotalDispatchedQty,
                 ReceiveRuns, DispatchRuns, FirstReceivedDate, LastReceivedDate,
                 FirstDispatchDate, LastDispatchDate, GenericExistsInSFDA, LastUpdated)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
            """
            supplier_insert_sql = r"""
                INSERT INTO dbo.SupplierHistory
                (SupplierName, SupplierCode, GTIN, DrugName, GenericItemNumber, Description,
                 TradeDescription, BN, ExpiryMonthKey, ExpiryDate, PackageSize,
                 ReceivedQuantityEach, ReceivedQuantityPack, FirstReceivedDate,
                 LastReceivedDate, ItemFamilyGroup, TradeItemNumber, LastUpdated)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, SYSUTCDATETIME());
            """
            customer_insert_sql = r"""
                INSERT INTO dbo.CustomerHistory
                (ToAddress, GLN, GTIN, DrugName, GenericItemNumber, TradeDescription,
                 BN, ExpiryMonthKey, ExpiryDate, PackageSize, DispatchQuantityEach,
                 DispatchQuantityPack, FirstDispatchDate, LastDispatchDate,
                 Custody, TradeItemNumber, LastUpdated)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, SYSUTCDATETIME());
            """

            emit("Saving new historical generation", {"build_id": build_id})
            with Database().connect() as connection:
                cursor = connection.cursor()
                try:
                    cursor.timeout = int(os.getenv("HISTORICAL_ACTIVATION_SQL_TIMEOUT_SECONDS", "0") or 0)
                except Exception:
                    pass
                try:
                    inserted_receipts = _bulk_insert_rows(cursor, receipt_insert_sql, prepared_receipts)
                    inserted_dispatches = _bulk_insert_rows(cursor, dispatch_insert_sql, prepared_dispatches)
                    inserted_master = _bulk_insert_rows(cursor, master_insert_sql, batch_master_rows)
                    inserted_supplier = _bulk_insert_rows(cursor, supplier_insert_sql, supplier_rows)
                    inserted_customer = _bulk_insert_rows(cursor, customer_insert_sql, customer_rows)

                    live_receipts = int(cursor.execute("SELECT COUNT_BIG(*) FROM dbo.ReceiptEvents;").fetchone()[0] or 0)
                    live_dispatches = int(cursor.execute("SELECT COUNT_BIG(*) FROM dbo.DispatchEvents;").fetchone()[0] or 0)
                    live_master = int(cursor.execute("SELECT COUNT_BIG(*) FROM dbo.BatchMaster;").fetchone()[0] or 0)
                    if live_receipts != inserted_receipts or live_dispatches != inserted_dispatches or live_master != inserted_master:
                        raise RuntimeError(
                            "Historical generation verification failed before activation: "
                            f"ReceiptEvents expected={inserted_receipts} actual={live_receipts}; "
                            f"DispatchEvents expected={inserted_dispatches} actual={live_dispatches}; "
                            f"BatchMaster expected={inserted_master} actual={live_master}."
                        )
                    connection.commit()
                except Exception:
                    connection.rollback()
                    raise

        # Tiny atomic metadata switch: no old historical rows are deleted here.
        emit("Activating new historical generation", {"build_id": build_id})
        with historical_maintenance_scope():
            with Database().connect() as connection:
                cursor = connection.cursor()
                try:
                    cursor.execute(
                        r"""
                        UPDATE dbo.HistoricalBuildVersions
                        SET IsActive = 0,
                            Status = CASE WHEN Status = N'Active' THEN N'Previous' ELSE Status END,
                            UpdatedAt = SYSUTCDATETIME()
                        WHERE WarehouseID = ? AND IsActive = 1 AND BuildID <> ?;

                        UPDATE dbo.HistoricalBuildVersions
                        SET IsActive = 1,
                            Status = N'Active',
                            ActivatedAt = SYSUTCDATETIME(),
                            CompletedAt = SYSUTCDATETIME(),
                            UpdatedAt = SYSUTCDATETIME(),
                            LastError = NULL
                        WHERE WarehouseID = ? AND BuildID = ?;
                        """,
                        warehouse_id, build_id, warehouse_id, build_id,
                    )
                    connection.commit()
                except Exception:
                    connection.rollback()
                    raise

    except Exception as exc:
        try:
            with historical_maintenance_scope():
                with Database().connect() as connection:
                    connection.cursor().execute(
                        """
                        UPDATE dbo.HistoricalBuildVersions
                        SET Status = N'Failed', LastError = ?, UpdatedAt = SYSUTCDATETIME()
                        WHERE WarehouseID = ? AND BuildID = ? AND IsActive = 0;
                        """,
                        str(exc)[:2000], warehouse_id, build_id,
                    )
                    connection.commit()
        except Exception:
            logger.exception("Failed to record Historical Build generation failure.")
        raise

    result = {
        "status": "Completed",
        "version": "HISTORICAL_VERSIONED_ACTIVATION_V2",
        "warehouse_id": warehouse_id,
        "build_id": build_id,
        "inserted_receipt_events": inserted_receipts,
        "inserted_dispatch_events": inserted_dispatches,
        "inserted_batch_master_rows": inserted_master,
        "inserted_supplier_history_rows": inserted_supplier,
        "inserted_customer_history_rows": inserted_customer,
        "seconds": round(time.perf_counter() - started_at, 3),
        "cleanup_pending": True,
    }
    logger.info("HISTORICAL_VERSIONED_ACTIVATION_V2 completed. %s", result)
    return result


def cleanup_inactive_historical_builds(
    *,
    warehouse_id: Optional[int] = None,
    keep_inactive: int = 0,
    batch_size: Optional[int] = None,
) -> Dict[str, Any]:
    """Delete inactive Historical Build generations in background.

    The active BuildID is never touched. Cleanup is intentionally independent of
    user-visible Historical Build completion and commits each bounded batch.
    """
    initialize_database()
    from engine.warehouse_context import current_warehouse_id, historical_maintenance_scope

    resolved_warehouse_id = int(warehouse_id or current_warehouse_id())
    effective_batch_size = max(1000, min(int(batch_size or os.getenv("HISTORICAL_CLEANUP_BATCH_SIZE", "10000") or 10000), 50000))
    deleted: Dict[str, int] = {}
    removed_builds: List[str] = []

    with historical_maintenance_scope():
        with Database().connect() as connection:
            rows = connection.cursor().execute(
                r"""
                SELECT BuildID
                FROM dbo.HistoricalBuildVersions
                WHERE WarehouseID = ? AND IsActive = 0 AND Status <> N'Building'
                ORDER BY COALESCE(ActivatedAt, CreatedAt) DESC;
                """,
                resolved_warehouse_id,
            ).fetchall()

        candidates = [str(r[0] or '').strip() for r in rows if str(r[0] or '').strip()]
        candidates = candidates[max(0, int(keep_inactive)):]

        for old_build_id in candidates:
            for table in ("CustomerHistory", "SupplierHistory", "BatchMaster", "DispatchEvents", "ReceiptEvents"):
                table_total = 0
                while True:
                    with Database().connect() as connection:
                        cursor = connection.cursor()
                        cursor.execute(
                            f"DELETE TOP ({effective_batch_size}) FROM dbo.[{table}] WHERE WarehouseID = ? AND BuildID = ?;",
                            resolved_warehouse_id, old_build_id,
                        )
                        affected = max(0, int(cursor.rowcount or 0))
                        connection.commit()
                    table_total += affected
                    if affected < effective_batch_size:
                        break
                deleted[f"{old_build_id}:{table}"] = table_total

            with Database().connect() as connection:
                connection.cursor().execute(
                    "DELETE FROM dbo.HistoricalBuildVersions WHERE WarehouseID = ? AND BuildID = ? AND IsActive = 0;",
                    resolved_warehouse_id, old_build_id,
                )
                connection.commit()
            removed_builds.append(old_build_id)

    result = {
        "status": "Completed",
        "version": "HISTORICAL_BACKGROUND_CLEANUP_V1",
        "warehouse_id": resolved_warehouse_id,
        "removed_builds": removed_builds,
        "deleted_rows": deleted,
    }
    logger.info("HISTORICAL_BACKGROUND_CLEANUP_V1 completed. %s", result)
    return result


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
                FROM dbo.BatchMaster AS coverage
                CROSS APPLY
                (
                    VALUES
                        (coverage.FirstReceivedDate),
                        (coverage.FirstDispatchDate)
                ) AS d(TransactionDate)
                WHERE coverage.WarehouseID = ?
            ) AS HistoricalFrom,
            (
                SELECT MAX(d.TransactionDate)
                FROM dbo.BatchMaster AS coverage
                CROSS APPLY
                (
                    VALUES
                        (coverage.LastReceivedDate),
                        (coverage.LastDispatchDate)
                ) AS d(TransactionDate)
                WHERE coverage.WarehouseID = ?
            ) AS HistoricalTo;
    """

    params = (warehouse_id,) * 9
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
            COALESCE(
                (
                    SELECT MIN(d.TransactionDate)
                    FROM dbo.BatchMaster AS coverage
                    CROSS APPLY
                    (
                        VALUES
                            (coverage.FirstReceivedDate),
                            (coverage.FirstDispatchDate)
                    ) AS d(TransactionDate)
                    WHERE coverage.WarehouseID = ?
                ),
                HistoricalFrom
            ) AS HistoricalFrom,
            COALESCE(
                (
                    SELECT MAX(d.TransactionDate)
                    FROM dbo.BatchMaster AS coverage
                    CROSS APPLY
                    (
                        VALUES
                            (coverage.LastReceivedDate),
                            (coverage.LastDispatchDate)
                    ) AS d(TransactionDate)
                    WHERE coverage.WarehouseID = ?
                ),
                HistoricalTo
            ) AS HistoricalTo,
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
            row = connection.cursor().execute(
                sql,
                warehouse_id,
                warehouse_id,
                warehouse_id,
            ).fetchone()
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


def reset_current_warehouse_data(
    warehouse_id: Optional[int] = None,
    progress_callback: Optional[Callable[[int, str, Dict[str, Any]], None]] = None,
    batch_size: Optional[int] = None,
) -> Dict[str, Any]:
    """Verified batched reset of operational data for exactly one warehouse.

    WAREHOUSE_RESET_V9_VERIFIED uses a fixed WarehouseID-scoped delete plan for
    operational tables. Before deleting FullReconciliationRuns it also discovers
    only its direct FK children, because legacy child tables may not carry a
    WarehouseID column and must be scoped safely through the parent FullRunID.

    Large tables are drained with committed DELETE TOP batches. This prevents a
    single 180-second timeout from leaving DispatchEvents/ReceiptEvents behind
    after earlier tables were already committed. The operation remains
    idempotent and performs a final zero-row verification before Success.

    Warehouse configuration and reference data are preserved: Warehouses,
    ApplicationUsers, AuthSessions, GLN mapping, Pack Size and business rules.
    """
    initialize_database()
    from engine.warehouse_context import current_warehouse_id

    resolved_warehouse_id = int(warehouse_id or current_warehouse_id())
    if resolved_warehouse_id < 1:
        raise RuntimeError("A valid WarehouseID is required for reset.")

    # Explicit warehouse-scoped plan. Optional/legacy tables are included when
    # they carry WarehouseID. Direct children of FullReconciliationRuns are
    # handled separately through FullRunID so legacy tables without WarehouseID
    # can still be removed safely for only the selected warehouse.
    reset_tables: Tuple[str, ...] = (
        # Confirmation / pending / daily state first.
        "DailyDispatchConfirmations",
        "FullDispatchConfirmations",
        "FullDispatchCutoverBaseline",
        "FullDispatchCutovers",
        "DailyProcessedTransactions",
        "DailyAcceptTransactions",
        "DailyDispatchTransactions",
        "FullDispatchTransactions",
        "DailyAcceptSFDABaseline",
        "DailyDispatchSFDABaseline",
        "FullDispatchSFDABaseline",
        # Run/file metadata before parent run rows.
        "ReconciliationRunFiles",
        "OutlookDraftRequests",
        "ReconciliationRuns",
        "FullReconciliationRuns",
        "HistoricalBuildJobs",
        "RunHistory",
        # Snapshots / cache.
        "LatestInventorySnapshot",
        "LatestSFDASnapshot",
        "WarehouseDashboardSummary",
        # Historical derived data before raw events.
        "CustomerHistory",
        "SupplierHistory",
        "BatchMaster",
        # Optional legacy event tables.
        "BatchEvents",
        "FullDispatchEvents",
        "FullReceiptEvents",
        # Current event sources last.
        "DispatchEvents",
        "ReceiptEvents",
    )

    deleted: Dict[str, int] = {}
    skipped: List[str] = []

    def q(name: str) -> str:
        return "[" + str(name).replace("]", "]]" ) + "]"

    def emit(progress: int, stage: str, extra: Optional[Dict[str, Any]] = None) -> None:
        if progress_callback is None:
            return
        try:
            progress_callback(int(progress), str(stage), dict(extra or {}))
        except Exception:
            # Progress reporting must never make destructive work fail.
            logger.exception("WAREHOUSE_RESET_V9_VERIFIED progress callback failed.")

    with Database().connect() as connection:
        cursor = connection.cursor()
        # Reset must clear ALL historical generations for this warehouse, not
        # only the currently active BuildID exposed by Historical Build RLS.
        try:
            cursor.execute(
                "EXEC sys.sp_set_session_context @key=N'HistoricalBuildMaintenance', @value=1;"
            )
        except Exception:
            pass
        # Give a single direct DELETE enough room to finish on a large table,
        # while avoiding an unbounded database call.
        try:
            cursor.timeout = int(os.getenv("WAREHOUSE_RESET_SQL_TIMEOUT_SECONDS", "180") or 180)
        except Exception:
            pass

        try:
            logger.info(
                "WAREHOUSE_RESET_V9_VERIFIED started. WarehouseID=%s tables=%s",
                resolved_warehouse_id,
                len(reset_tables),
            )
            emit(32, "Starting fast warehouse database reset", {"deleted_rows_total": 0})

            # Discover direct FK children of FullReconciliationRuns before the
            # normal WarehouseID-scoped delete plan. Some legacy child tables do
            # not have their own WarehouseID column, so they must be scoped via
            # the parent FullReconciliationRuns row instead. This is especially
            # important for older Warehouse 1 / admin data.
            full_run_children = cursor.execute(
                """
                SELECT DISTINCT
                    OBJECT_SCHEMA_NAME(fk.parent_object_id) AS ChildSchema,
                    OBJECT_NAME(fk.parent_object_id) AS ChildTable,
                    pc.name AS ChildColumn,
                    rc.name AS ParentColumn
                FROM sys.foreign_keys AS fk
                INNER JOIN sys.foreign_key_columns AS fkc
                    ON fk.object_id = fkc.constraint_object_id
                INNER JOIN sys.columns AS pc
                    ON pc.object_id = fkc.parent_object_id
                   AND pc.column_id = fkc.parent_column_id
                INNER JOIN sys.columns AS rc
                    ON rc.object_id = fkc.referenced_object_id
                   AND rc.column_id = fkc.referenced_column_id
                WHERE fk.referenced_object_id = OBJECT_ID(N'dbo.FullReconciliationRuns')
                  AND rc.name = N'FullRunID';
                """
            ).fetchall()

            if full_run_children:
                emit(33, "Clearing Full Reconciliation child records", {"deleted_rows_total": 0})

            for child in full_run_children:
                child_schema = str(child[0] or "dbo")
                child_table = str(child[1] or "")
                child_column = str(child[2] or "")
                parent_column = str(child[3] or "")
                if not child_table or not child_column or not parent_column:
                    continue

                cursor.execute(
                    f"""
                    DELETE child
                    FROM {q(child_schema)}.{q(child_table)} AS child
                    INNER JOIN dbo.FullReconciliationRuns AS parent
                        ON child.{q(child_column)} = parent.{q(parent_column)}
                    WHERE parent.WarehouseID = ?;
                    """,
                    resolved_warehouse_id,
                )
                affected = max(0, int(cursor.rowcount or 0))
                connection.commit()
                deleted[child_table] = deleted.get(child_table, 0) + affected
                logger.info(
                    "WAREHOUSE_RESET_V9_VERIFIED cleared %s FK child row(s) from %s.%s for WarehouseID=%s.",
                    affected,
                    child_schema,
                    child_table,
                    resolved_warehouse_id,
                )

            existing_tables: List[str] = []
            legacy_admin_tables: List[str] = []
            for table in reset_tables:
                metadata = cursor.execute(
                    """
                    SELECT
                        CASE WHEN OBJECT_ID(?, N'U') IS NULL THEN 0 ELSE 1 END AS TableExists,
                        CASE WHEN COL_LENGTH(?, N'WarehouseID') IS NULL THEN 0 ELSE 1 END AS HasWarehouseID;
                    """,
                    (f"dbo.{table}", f"dbo.{table}"),
                ).fetchone()
                exists = bool(metadata and int(metadata[0] or 0))
                has_warehouse = bool(metadata and int(metadata[1] or 0))
                if exists and has_warehouse:
                    existing_tables.append(table)
                elif exists and resolved_warehouse_id == 1:
                    # A known operational table without WarehouseID predates
                    # Multi-Warehouse and therefore belongs to Warehouse 1.
                    # Leaving it behind would make an Admin reset incomplete.
                    legacy_admin_tables.append(table)
                elif exists:
                    skipped.append(table)
                    logger.warning(
                        "WAREHOUSE_RESET_V9_VERIFIED skipped %s because it has no WarehouseID column.",
                        table,
                    )

            if not existing_tables and not legacy_admin_tables:
                raise RuntimeError("No operational tables were found for reset.")

            work_items = [(table, True) for table in existing_tables] + [
                (table, False) for table in legacy_admin_tables
            ]
            total = len(work_items)
            for index, (table, is_warehouse_scoped) in enumerate(work_items, start=1):
                progress = 34 + int((index - 1) * 34 / max(1, total))
                emit(
                    progress,
                    f"Clearing {table}",
                    {
                        "current_table": table,
                        "table_index": index,
                        "table_total": total,
                        "deleted_rows_total": int(sum(deleted.values())),
                    },
                )

                # Delete in committed batches.  The previous implementation used
                # one large DELETE with a 180-second statement timeout.  Because
                # earlier tables were committed first, a timeout on DispatchEvents
                # or ReceiptEvents could leave the warehouse only partially reset.
                # Bounded batches make progress durable and observable instead.
                effective_batch_size = int(
                    batch_size
                    or os.getenv(
                        "WAREHOUSE_RESET_BATCH_SIZE",
                        "25000" if resolved_warehouse_id == 1 else "10000",
                    )
                    or (25000 if resolved_warehouse_id == 1 else 10000)
                )
                effective_batch_size = max(1000, min(effective_batch_size, 50000))
                table_deleted = 0
                batch_no = 0
                while True:
                    if is_warehouse_scoped:
                        cursor.execute(
                            f"DELETE TOP ({effective_batch_size}) FROM dbo.{q(table)} WHERE WarehouseID = ?;",
                            resolved_warehouse_id,
                        )
                    else:
                        cursor.execute(
                            f"DELETE TOP ({effective_batch_size}) FROM dbo.{q(table)};"
                        )
                    affected = max(0, int(cursor.rowcount or 0))
                    connection.commit()
                    table_deleted += affected
                    batch_no += 1
                    if affected:
                        emit(
                            progress,
                            f"Clearing {table}",
                            {
                                "current_table": table,
                                "table_index": index,
                                "table_total": total,
                                "batch": batch_no,
                                "batch_deleted": affected,
                                "table_deleted": table_deleted,
                                "deleted_rows_total": int(sum(deleted.values())) + table_deleted,
                            },
                        )
                        logger.info(
                            "WAREHOUSE_RESET_V9_VERIFIED batch. WarehouseID=%s table=%s batch=%s deleted=%s table_deleted=%s.",
                            resolved_warehouse_id, table, batch_no, affected, table_deleted,
                        )
                    if affected < effective_batch_size:
                        break

                deleted[table] = deleted.get(table, 0) + table_deleted
                logger.info(
                    "WAREHOUSE_RESET_V9_VERIFIED table complete. WarehouseID=%s table=%s deleted=%s batches=%s.",
                    resolved_warehouse_id, table, table_deleted, batch_no,
                )

            # Reinitialize Historical Build version metadata after every full
            # account/warehouse reset. This guarantees the next Append always
            # has one valid active generation even when no historical rows exist.
            try:
                reset_build_id = f"RESET-W{resolved_warehouse_id}-{int(time.time())}"
                cursor.execute(
                    "DELETE FROM dbo.HistoricalBuildVersions WHERE WarehouseID = ?;",
                    resolved_warehouse_id,
                )
                deleted_versions = max(0, int(cursor.rowcount or 0))
                cursor.execute(
                    """
                    INSERT INTO dbo.HistoricalBuildVersions
                    (WarehouseID, BuildID, Status, IsActive, SourceJobID, CreatedAt, ActivatedAt, CompletedAt, UpdatedAt)
                    VALUES (?, ?, N'Active', 1, N'Warehouse Reset', SYSUTCDATETIME(), SYSUTCDATETIME(), SYSUTCDATETIME(), SYSUTCDATETIME());
                    """,
                    resolved_warehouse_id, reset_build_id,
                )
                connection.commit()
                deleted["HistoricalBuildVersions"] = (
                    deleted.get("HistoricalBuildVersions", 0) + deleted_versions
                )
                logger.info(
                    "WAREHOUSE_RESET_V9_VERIFIED created fresh active BuildID=%s WarehouseID=%s.",
                    reset_build_id, resolved_warehouse_id,
                )
            except pyodbc.Error as exc:
                if "HistoricalBuildVersions" not in str(exc) and "Invalid object name" not in str(exc):
                    raise

            # Quick verification only across the core operational tables.  No
            # expensive joins/aggregations are needed after a reset.
            emit(70, "Verifying warehouse database is empty", {"deleted_rows_total": int(sum(deleted.values()))})
            # Verify every WarehouseID-scoped table that participated in the
            # reset, not only a small core subset. Success is impossible while
            # any warehouse-owned row remains.
            remaining: Dict[str, int] = {}
            for table, is_warehouse_scoped in work_items:
                sql = (
                    f"SELECT COUNT_BIG(*) FROM dbo.{q(table)} WHERE WarehouseID = ?;"
                    if is_warehouse_scoped
                    else f"SELECT COUNT_BIG(*) FROM dbo.{q(table)};"
                )
                count = int(
                    (
                        cursor.execute(sql, resolved_warehouse_id)
                        if is_warehouse_scoped
                        else cursor.execute(sql)
                    ).fetchone()[0]
                    or 0
                )
                if count:
                    remaining[table] = count

            build_version_row = cursor.execute(
                """
                SELECT
                    COUNT_BIG(*) AS TotalRows,
                    SUM(CASE WHEN IsActive = 1 THEN 1 ELSE 0 END) AS ActiveRows,
                    SUM(CASE WHEN IsActive = 1 AND SourceJobID = N'Warehouse Reset' THEN 1 ELSE 0 END) AS ResetRows
                FROM dbo.HistoricalBuildVersions
                WHERE WarehouseID = ?;
                """,
                resolved_warehouse_id,
            ).fetchone()
            build_version_audit = {
                "total_rows": int((build_version_row[0] if build_version_row else 0) or 0),
                "active_rows": int((build_version_row[1] if build_version_row else 0) or 0),
                "reset_rows": int((build_version_row[2] if build_version_row else 0) or 0),
            }
            if build_version_audit != {
                "total_rows": 1,
                "active_rows": 1,
                "reset_rows": 1,
            }:
                raise RuntimeError(
                    "Warehouse reset verification found invalid HistoricalBuildVersions state: "
                    + json.dumps(build_version_audit, sort_keys=True)
                )

            if remaining:
                raise RuntimeError(
                    "Warehouse reset verification found remaining operational rows: "
                    + ", ".join(f"{name}={count}" for name, count in remaining.items())
                )

            result = {
                "status": "Completed",
                "version": "WAREHOUSE_RESET_V9_VERIFIED",
                "warehouse_id": resolved_warehouse_id,
                "deleted_rows": deleted,
                "deleted_rows_total": int(sum(deleted.values())),
                "tables_cleared": len(work_items),
                "skipped_tables": skipped,
                "legacy_admin_tables_cleared": legacy_admin_tables,
                "remaining_rows": remaining,
                "historical_build_versions": build_version_audit,
                "clean_state_verified": True,
                "preserved": [
                    "Warehouses",
                    "ApplicationUsers",
                    "AuthSessions",
                    "Warehouse GLN mapping",
                    "Pack Size reference",
                    "Business rules and application logic",
                ],
            }
            logger.info(
                "WAREHOUSE_RESET_V9_VERIFIED completed. WarehouseID=%s deleted_rows_total=%s tables=%s",
                resolved_warehouse_id,
                result["deleted_rows_total"],
                result["tables_cleared"],
            )
            emit(72, "Warehouse database reset completed", result)
            return result
        except Exception:
            try:
                connection.rollback()
            except Exception:
                pass
            logger.exception(
                "WAREHOUSE_RESET_V9_VERIFIED failed for WarehouseID=%s. Already committed table clears remain deleted; retry is safe.",
                resolved_warehouse_id,
            )
            raise

def _expire_stale_historical_build_jobs(
    connection,
    warehouse_id: int,
) -> int:
    """Expire abandoned Historical Build locks using separate queue/run timeouts.

    Queued jobs that never receive a worker start are ghost submissions and must not
    block a warehouse for hours. Running jobs are allowed a much longer inactivity
    window because a legitimate historical build can be heavy. Both thresholds are
    configurable through application settings.
    """
    queued_stale_minutes = max(2, int(
        os.getenv("HISTORICAL_QUEUED_STALE_MINUTES", "10") or 10
    ))
    running_stale_minutes = max(5, int(
        os.getenv(
            "HISTORICAL_RUNNING_STALE_MINUTES",
            os.getenv("HISTORICAL_JOB_STALE_MINUTES", "15") or 15,
        ) or 15
    ))

    cursor = connection.cursor()

    # A job that never started and stopped updating is a stale queue submission.
    cursor.execute(
        r"""
        UPDATE dbo.HistoricalBuildJobs
        SET Status = 'Failed',
            CurrentStage = 'Expired stale queued Historical Build',
            ErrorMessage = COALESCE(NULLIF(ErrorMessage, ''),
                'Historical Build stayed Queued without a worker start and was automatically released.'),
            CompletedAt = COALESCE(CompletedAt, SYSUTCDATETIME()),
            UpdatedAt = SYSUTCDATETIME()
        WHERE WarehouseID = ?
          AND Status = 'Queued'
          AND StartedAt IS NULL
          AND COALESCE(UpdatedAt, CreatedAt) < DATEADD(MINUTE, -?, SYSUTCDATETIME());
        """,
        (int(warehouse_id), int(queued_stale_minutes)),
    )
    expired_queued = int(cursor.rowcount or 0)

    # Running jobs get the longer inactivity timeout and are judged by UpdatedAt.
    cursor.execute(
        r"""
        UPDATE dbo.HistoricalBuildJobs
        SET Status = 'Failed',
            CurrentStage = 'Expired stale running Historical Build',
            ErrorMessage = COALESCE(NULLIF(ErrorMessage, ''),
                'Historical Build stopped updating while Running and was automatically released.'),
            CompletedAt = COALESCE(CompletedAt, SYSUTCDATETIME()),
            UpdatedAt = SYSUTCDATETIME()
        WHERE WarehouseID = ?
          AND Status = 'Running'
          AND COALESCE(UpdatedAt, StartedAt, CreatedAt) < DATEADD(MINUTE, -?, SYSUTCDATETIME());
        """,
        (int(warehouse_id), int(running_stale_minutes)),
    )
    expired_running = int(cursor.rowcount or 0)

    expired = expired_queued + expired_running
    if expired:
        connection.commit()
        logger.warning(
            "Expired stale Historical Build lock(s). WarehouseID=%s queued=%s running=%s",
            warehouse_id,
            expired_queued,
            expired_running,
        )
    return expired


def claim_historical_build_job(job_id: str) -> bool:
    """Atomically claim one queued Historical Build for a single worker.

    Queue delivery is at-least-once and a recovered message can coexist with the
    original message after a host recycle. Only the worker that changes the row
    from Queued -> Running is allowed to execute the heavy build.
    """
    initialize_database()
    with Database().connect() as connection:
        cursor = connection.cursor()
        cursor.execute(
            """
            UPDATE dbo.HistoricalBuildJobs
            SET Status = 'Running',
                Progress = CASE WHEN Progress < 1 THEN 1 ELSE Progress END,
                CurrentStage = 'Historical worker claimed job',
                StartedAt = COALESCE(StartedAt, SYSUTCDATETIME()),
                UpdatedAt = SYSUTCDATETIME(),
                ErrorMessage = ''
            OUTPUT INSERTED.JobID
            WHERE JobID = ?
              AND Status = 'Queued';
            """,
            (str(job_id),),
        )
        # pyodbc cursor.rowcount is not reliable for SQL Server DML (it can be -1
        # even when the UPDATE succeeds). OUTPUT gives an authoritative atomic claim.
        claimed = cursor.fetchone() is not None
        connection.commit()
    return claimed


def acquire_historical_requeue_lease(
    job_id: str,
    min_age_seconds: int = 60,
) -> bool:
    """Throttle self-healing re-enqueue attempts for a ghost queued job.

    The status endpoint may be polled by several browser requests. This atomic
    update allows only one request per recovery window to send a replacement
    queue message for the same JobID.
    """
    initialize_database()
    safe_age = max(30, int(min_age_seconds or 60))
    with Database().connect() as connection:
        cursor = connection.cursor()
        cursor.execute(
            """
            UPDATE dbo.HistoricalBuildJobs
            SET CurrentStage = 'Queued for processing - recovery enqueue',
                UpdatedAt = SYSUTCDATETIME()
            OUTPUT INSERTED.JobID
            WHERE JobID = ?
              AND Status = 'Queued'
              AND StartedAt IS NULL
              AND COALESCE(UpdatedAt, CreatedAt) < DATEADD(SECOND, -?, SYSUTCDATETIME());
            """,
            (str(job_id), safe_age),
        )
        acquired = cursor.fetchone() is not None
        connection.commit()
    return acquired


def acquire_historical_running_recovery_lease(
    job_id: str,
    min_age_seconds: int = 300,
) -> bool:
    """Move an abandoned Running Historical Build back to Queued exactly once.

    A healthy worker refreshes UpdatedAt every 30 seconds. If the Azure Functions
    host is recycled, that heartbeat stops while the durable SQL job row remains
    Running. This atomic transition lets the status endpoint re-enqueue the SAME
    JobID after a conservative inactivity window. The build then restarts from
    the beginning: Rebuild safely resets warehouse history again, while Append is
    protected by event-level deduplication.
    """
    initialize_database()
    safe_age = max(180, int(min_age_seconds or 300))
    with Database().connect() as connection:
        cursor = connection.cursor()
        cursor.execute(
            """
            UPDATE dbo.HistoricalBuildJobs
            SET Status = 'Queued',
                CurrentStage = 'Recovering interrupted Historical Build',
                UpdatedAt = SYSUTCDATETIME(),
                ErrorMessage = 'Azure worker interruption detected; same JobID queued for recovery.'
            OUTPUT INSERTED.JobID
            WHERE JobID = ?
              AND Status = 'Running'
              AND COALESCE(UpdatedAt, StartedAt, CreatedAt)
                    < DATEADD(SECOND, -?, SYSUTCDATETIME());
            """,
            (str(job_id), safe_age),
        )
        acquired = cursor.fetchone() is not None
        connection.commit()
    return acquired


def heartbeat_historical_build_job(
    job_id: str,
    warehouse_id: Optional[int] = None,
) -> bool:
    """Refresh UpdatedAt only while the warehouse-scoped job is Running.

    WarehouseID is included explicitly in addition to SQL RLS. This prevents a
    background thread with a missing Python context from touching or misreading
    another warehouse's job.
    """
    initialize_database()
    from engine.warehouse_context import current_warehouse_id

    resolved_warehouse_id = int(warehouse_id or current_warehouse_id())
    with Database().connect() as connection:
        cursor = connection.cursor()
        cursor.execute(
            """
            UPDATE dbo.HistoricalBuildJobs
            SET UpdatedAt = SYSUTCDATETIME()
            OUTPUT INSERTED.JobID
            WHERE WarehouseID = ?
              AND JobID = ?
              AND Status = 'Running';
            """,
            (resolved_warehouse_id, str(job_id)),
        )
        # Never use cursor.rowcount as the cancellation signal here. SQL Server
        # drivers may report -1 for successful DML. OUTPUT proves the row was
        # actually still Running and refreshed by this heartbeat.
        active = cursor.fetchone() is not None
        connection.commit()
    return active


def historical_build_job_is_active(
    job_id: str,
    warehouse_id: Optional[int] = None,
) -> bool:
    """Return True only while the warehouse-scoped Historical Build is active."""
    initialize_database()
    from engine.warehouse_context import current_warehouse_id

    resolved_warehouse_id = int(warehouse_id or current_warehouse_id())
    with Database().connect() as connection:
        cursor = connection.cursor()
        row = cursor.execute(
            """
            SELECT Status
            FROM dbo.HistoricalBuildJobs
            WHERE WarehouseID = ? AND JobID = ?;
            """,
            (resolved_warehouse_id, str(job_id)),
        ).fetchone()
    return bool(row and str(row[0] or '').strip() in {'Queued', 'Running'})


def cancel_historical_build_job(job_id: str, reason: str = 'Cancelled by user.') -> bool:
    """Cooperatively cancel a queued/running Historical Build."""
    initialize_database()
    with Database().connect() as connection:
        cursor = connection.cursor()
        cursor.execute(
            """
            UPDATE dbo.HistoricalBuildJobs
            SET Status = 'Cancelled',
                CurrentStage = 'Historical Build cancelled',
                ErrorMessage = ?,
                CompletedAt = COALESCE(CompletedAt, SYSUTCDATETIME()),
                UpdatedAt = SYSUTCDATETIME()
            WHERE JobID = ? AND Status IN ('Queued', 'Running');
            """,
            (str(reason or 'Cancelled by user.'), str(job_id)),
        )
        changed = int(cursor.rowcount or 0) > 0
        connection.commit()
    return changed


def get_active_historical_build_job(warehouse_id: Optional[int] = None) -> Optional[Dict[str, Any]]:
    """Return the newest genuinely active Historical Build for one warehouse, if any."""
    initialize_database()
    from engine.warehouse_context import current_warehouse_id

    resolved_warehouse_id = int(warehouse_id or current_warehouse_id())
    if resolved_warehouse_id < 1:
        raise RuntimeError("A valid WarehouseID is required.")

    with Database().connect() as connection:
        _expire_stale_historical_build_jobs(connection, resolved_warehouse_id)
        cursor = connection.cursor()
        row = cursor.execute(
            r"""
            SELECT TOP (1) JobID, Operation, Status, Progress, CurrentStage, CreatedAt, UpdatedAt
            FROM dbo.HistoricalBuildJobs
            WHERE WarehouseID = ?
              AND Status IN ('Queued', 'Running')
            ORDER BY CreatedAt DESC;
            """,
            resolved_warehouse_id,
        ).fetchone()

    if not row:
        return None
    return {
        "JobID": str(row[0] or ""),
        "Operation": str(row[1] or ""),
        "Status": str(row[2] or ""),
        "Progress": int(row[3] or 0),
        "CurrentStage": str(row[4] or ""),
        "CreatedAt": row[5],
        "UpdatedAt": row[6],
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
            # Serialize creation per warehouse and reject an equivalent heavy
            # job while another Historical Build is still active. Expire abandoned
            # queue/worker rows first so a dead job cannot lock the warehouse forever.
            cursor.execute("SET TRANSACTION ISOLATION LEVEL SERIALIZABLE;")
            _expire_stale_historical_build_jobs(connection, warehouse_id)
            cursor = connection.cursor()
            active = cursor.execute(
                r"""
                SELECT TOP (1) JobID
                FROM dbo.HistoricalBuildJobs WITH (UPDLOCK, HOLDLOCK)
                WHERE WarehouseID = ?
                  AND Status IN ('Queued', 'Running')
                ORDER BY CreatedAt DESC;
                """,
                (warehouse_id,),
            ).fetchone()
            if active:
                raise ValueError(
                    f"A Historical Build is already active for this warehouse: {active[0]}"
                )
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
            UpdatedAt,
            WarehouseID
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
        "warehouse_id": int(row[13] or 0),
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
    validated_sfda_identity_df: pd.DataFrame | None = None,
) -> Dict[str, int]:
    """Synchronize BatchMaster from the current SFDA snapshot by BN + expiry month.

    The regulatory candidate key is BN + ExpiryMonthKey.  The existing GTIN in
    BatchMaster is never used as a prerequisite for an exact batch match because
    it may have been inherited earlier from another batch of the same Generic.

    Safety rule: Full Accept/Dispatch pass only rows already approved by the V6
    multi-evidence identity gate. Independently, only unambiguous SFDA BN +
    expiry-month keys (exactly one GTIN) are eligible. When a key matches, GTIN,
    Drug Name, exact SFDA expiry date and all SFDA quantities are overwritten
    from that exact SFDA row and the batch is marked GenericExistsInSFDA = Yes.
    """

    initialize_database()
    if sfda_df is None or sfda_df.empty:
        return {"sfda_rows": 0, "unambiguous_sfda_keys": 0, "updated_rows": 0}

    from engine.full_reconciliation import FullReconciliationEngine
    from engine.normalizer import Normalizer

    if validated_sfda_identity_df is not None:
        frame = validated_sfda_identity_df.copy()
        required = [
            "BN",
            "Expiry Month Key",
            "GTIN",
            "Expiry Date",
            "Drug Name",
            "Quantity",
            "Active",
            "Quantity sent pending",
            "Quantity Receive Pending",
        ]
        for column in required:
            if column not in frame.columns:
                frame[column] = None
    else:
        frame = Normalizer.normalize_sfda(sfda_df.copy())
        frame["Expiry Month Key"] = FullReconciliationEngine._month_key(frame["Expiry Date"])
    frame["GTIN"] = Normalizer.text(frame["GTIN"])
    frame["Drug Name"] = Normalizer.text(frame["Drug Name"])
    frame["BN"] = Normalizer.text(frame["BN"])
    if "Generic Item Number" not in frame.columns:
        frame["Generic Item Number"] = ""
    frame["Generic Item Number"] = Normalizer.text(frame["Generic Item Number"])
    frame["Expiry Date"] = Normalizer.date(frame["Expiry Date"])
    for column in ["Quantity", "Active", "Quantity sent pending", "Quantity Receive Pending"]:
        frame[column] = pd.to_numeric(frame[column], errors="coerce").fillna(0)

    frame = frame.loc[
        frame["BN"].ne("")
        & frame["Expiry Month Key"].astype(str).str.strip().ne("")
        & frame["GTIN"].ne("")
    ].copy()

    identity_counts = (
        frame.groupby(["BN", "Expiry Month Key"], dropna=False)["GTIN"]
        .nunique()
        .rename("_GTINCount")
        .reset_index()
    )
    frame = frame.merge(
        identity_counts,
        on=["BN", "Expiry Month Key"],
        how="left",
        validate="many_to_one",
    )
    frame = frame.loc[frame["_GTINCount"].eq(1)].copy()

    validated_generic_scope = validated_sfda_identity_df is not None
    group_keys = ["BN", "Expiry Month Key"]
    if validated_generic_scope:
        frame = frame.loc[frame["Generic Item Number"].ne("")].copy()
        group_keys.append("Generic Item Number")

    grouped = (
        frame.groupby(group_keys, dropna=False)
        .agg(**{
            "GTIN": ("GTIN", "first"),
            "Expiry Date": ("Expiry Date", "first"),
            "Drug Name": ("Drug Name", "first"),
            "Quantity": ("Quantity", "sum"),
            "Active": ("Active", "sum"),
            "Quantity sent pending": ("Quantity sent pending", "sum"),
            "Quantity Receive Pending": ("Quantity Receive Pending", "sum"),
        })
        .reset_index()
    )

    rows = [
        (
            _text(row, "BN"),
            _text(row, "Expiry Month Key"),
            _text(row, "Generic Item Number"),
            _text(row, "GTIN"),
            _value(row, "Expiry Date"),
            _text(row, "Drug Name"),
            _number(row, "Quantity"),
            _number(row, "Active"),
            _number(row, "Quantity sent pending"),
            _number(row, "Quantity Receive Pending"),
        )
        for row in grouped.to_dict(orient="records")
        if _text(row, "BN") and _text(row, "Expiry Month Key") and _text(row, "GTIN")
    ]
    if not rows:
        return {
            "sfda_rows": int(len(sfda_df)),
            "unambiguous_sfda_keys": 0,
            "updated_rows": 0,
        }

    from engine.warehouse_context import current_warehouse_id
    warehouse_id = int(current_warehouse_id())

    with Database().connect() as connection:
        cursor = connection.cursor()
        try:
            cursor.execute(r"""
                CREATE TABLE #SFDABatchState (
                    BN nvarchar(255) NOT NULL,
                    ExpiryMonthKey nvarchar(20) NOT NULL,
                    GenericItemNumber nvarchar(255) NOT NULL,
                    GTIN nvarchar(100) NOT NULL,
                    ExpiryDate date NULL,
                    DrugName nvarchar(1000) NULL,
                    SFDAQuantity decimal(38,6) NOT NULL,
                    Active decimal(38,6) NOT NULL,
                    QuantitySentPending decimal(38,6) NOT NULL,
                    QuantityReceivePending decimal(38,6) NOT NULL,
                    PRIMARY KEY (BN, ExpiryMonthKey, GenericItemNumber)
                );
            """)
            cursor.fast_executemany = True
            cursor.executemany(r"""
                INSERT INTO #SFDABatchState
                (BN, ExpiryMonthKey, GenericItemNumber, GTIN, ExpiryDate, DrugName, SFDAQuantity, Active,
                 QuantitySentPending, QuantityReceivePending)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
            """, rows)
            cursor.fast_executemany = False

            cursor.execute(r"""
                UPDATE bm
                SET bm.GTIN = s.GTIN,
                    bm.ExpiryDate = s.ExpiryDate,
                    bm.DrugName = s.DrugName,
                    bm.SFDAQuantity = s.SFDAQuantity,
                    bm.Active = s.Active,
                    bm.QuantitySentPending = s.QuantitySentPending,
                    bm.QuantityReceivePending = s.QuantityReceivePending,
                    bm.GenericExistsInSFDA = N'Yes',
                    bm.LastUpdated = SYSUTCDATETIME()
                FROM dbo.BatchMaster AS bm
                INNER JOIN #SFDABatchState AS s
                    ON s.BN = bm.BN
                   AND s.ExpiryMonthKey = bm.ExpiryMonthKey
                   AND (
                        s.GenericItemNumber = N''
                        OR s.GenericItemNumber = bm.GenericItemNumber
                   )
                WHERE bm.WarehouseID = ?;
            """, (warehouse_id,))
            updated = max(0, int(cursor.rowcount or 0))
            connection.commit()
        except Exception:
            connection.rollback()
            raise

    return {
        "sfda_rows": int(len(sfda_df)),
        "unambiguous_sfda_keys": int(
            len(grouped[["BN", "Expiry Month Key"]].drop_duplicates())
        ),
        "validated_identity_rows": int(len(frame)),
        "updated_rows": updated,
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
    """Confirm prior Accept submissions from SFDA, optimized for large runs.

    Confirmation grain is BN + expiry month.  All still-open Accept transactions
    are read once, evidence is allocated in memory, and SQL updates are sent as
    one executemany batch.  The previous implementation issued one SELECT and
    many UPDATE round-trips per confirmed batch, which became very slow when a
    new SFDA report confirmed many batches at once.
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

        for frame in (previous, current):
            frame["BN"] = frame["BN"].fillna("").astype(str).str.strip()
            frame["Expiry Month Key"] = pd.to_datetime(
                frame["Expiry Date"], errors="coerce"
            ).dt.strftime("%Y-%m").fillna("")

        previous_month = (
            previous.groupby(["BN", "Expiry Month Key"], dropna=False)
            .agg(
                **{
                    "Active Previous": ("Active", "sum"),
                    "Quantity Receive Pending Previous": (
                        "Quantity Receive Pending", "sum"
                    ),
                }
            )
            .reset_index()
        )
        current_month = (
            current.groupby(["BN", "Expiry Month Key"], dropna=False)
            .agg(
                **{
                    "Active Current": ("Active", "sum"),
                    "Quantity Receive Pending Current": (
                        "Quantity Receive Pending", "sum"
                    ),
                }
            )
            .reset_index()
        )
        comparison = previous_month.merge(
            current_month,
            on=["BN", "Expiry Month Key"],
            how="inner",
            validate="one_to_one",
        )

        if not comparison.empty:
            comparison["Pending Decrease"] = (
                pd.to_numeric(
                    comparison["Quantity Receive Pending Previous"], errors="coerce"
                ).fillna(0)
                - pd.to_numeric(
                    comparison["Quantity Receive Pending Current"], errors="coerce"
                ).fillna(0)
            ).clip(lower=0)
            comparison["Active Increase"] = (
                pd.to_numeric(comparison["Active Current"], errors="coerce").fillna(0)
                - pd.to_numeric(comparison["Active Previous"], errors="coerce").fillna(0)
            ).clip(lower=0)
            comparison["Confirmed Pack Evidence"] = comparison[
                ["Pending Decrease", "Active Increase"]
            ].min(axis=1)
            evidence = comparison.loc[
                comparison["Confirmed Pack Evidence"].gt(0),
                ["BN", "Expiry Month Key", "Confirmed Pack Evidence"],
            ].copy()
        else:
            evidence = pd.DataFrame(
                columns=["BN", "Expiry Month Key", "Confirmed Pack Evidence"]
            )

        cursor = connection.cursor()
        confirmed_pack_total = 0.0
        confirmed_each_total = 0.0
        confirmed_transaction_keys: Set[str] = set()
        confirmed_batches = 0

        try:
            updates: List[Tuple[float, float, str]] = []
            if not evidence.empty:
                open_rows = pd.read_sql(
                    r"""
                    SELECT
                        TransactionKey,
                        BN,
                        ExpiryMonthKey,
                        SubmittedQuantityEach,
                        ConfirmedQuantityEach,
                        SubmittedQuantityPack,
                        ConfirmedQuantityPack,
                        CreatedAt
                    FROM dbo.DailyAcceptTransactions
                    WHERE SubmittedQuantityPack > ConfirmedQuantityPack
                    ORDER BY CreatedAt, TransactionKey;
                    """,
                    connection,
                )

                if not open_rows.empty:
                    open_rows["BN"] = open_rows["BN"].fillna("").astype(str).str.strip()
                    open_rows["ExpiryMonthKey"] = (
                        open_rows["ExpiryMonthKey"].fillna("").astype(str).str.strip()
                    )
                    grouped_open = {
                        (str(bn), str(month)): group
                        for (bn, month), group in open_rows.groupby(
                            ["BN", "ExpiryMonthKey"], sort=False, dropna=False
                        )
                    }

                    for ev in evidence.to_dict(orient="records"):
                        bn = str(ev.get("BN") or "").strip()
                        month = str(ev.get("Expiry Month Key") or "").strip()
                        remaining_pack = max(
                            0.0, float(ev.get("Confirmed Pack Evidence") or 0)
                        )
                        if remaining_pack <= 0:
                            continue
                        pending_group = grouped_open.get((bn, month))
                        if pending_group is None or pending_group.empty:
                            continue

                        batch_confirmed = 0.0
                        for pending in pending_group.itertuples(index=False):
                            if remaining_pack <= 0.0000001:
                                break
                            transaction_key = str(pending.TransactionKey)
                            submitted_each = float(pending.SubmittedQuantityEach or 0)
                            confirmed_each = float(pending.ConfirmedQuantityEach or 0)
                            submitted_pack = float(pending.SubmittedQuantityPack or 0)
                            confirmed_pack = float(pending.ConfirmedQuantityPack or 0)
                            open_pack = max(0.0, submitted_pack - confirmed_pack)
                            open_each = max(0.0, submitted_each - confirmed_each)
                            if open_pack <= 0:
                                continue
                            allocate_pack = min(remaining_pack, open_pack)
                            each_per_pack = open_each / open_pack if open_pack > 0 else 0
                            allocate_each = min(open_each, allocate_pack * each_per_pack)
                            updates.append((allocate_pack, allocate_each, transaction_key))
                            remaining_pack -= allocate_pack
                            batch_confirmed += allocate_pack
                            confirmed_pack_total += allocate_pack
                            confirmed_each_total += allocate_each
                            confirmed_transaction_keys.add(transaction_key)

                        if batch_confirmed > 0:
                            confirmed_batches += 1

            if updates:
                cursor.fast_executemany = True
                cursor.executemany(
                    r"""
                    UPDATE dbo.DailyAcceptTransactions
                    SET ConfirmedQuantityPack = ConfirmedQuantityPack + ?,
                        ConfirmedQuantityEach = ConfirmedQuantityEach + ?,
                        LastConfirmedAt = SYSUTCDATETIME(),
                        UpdatedAt = SYSUTCDATETIME()
                    WHERE TransactionKey = ?;
                    """,
                    updates,
                )

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
                "Custody": _text(row, "Custody"),
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
                ? AS ReferenceLine, ? AS ToAddress, ? AS Custody, ? AS TransactionDate,
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
                Custody = source.Custody,
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
                ReferenceNumber, ReferenceLine, ToAddress, Custody, TransactionDate,
                SubmittedQuantityEach, SubmittedQuantityPack,
                ConfirmedQuantityEach, ConfirmedQuantityPack,
                FirstSubmittedRun, LastSubmittedRun
            )
            VALUES
            (
                source.TransactionKey, source.BN, source.ExpiryDate,
                source.GenericItemNumber, source.ReferenceNumber,
                source.ReferenceLine, source.ToAddress, source.Custody, source.TransactionDate,
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
                        row["Reference Line"], row["To Address"], row["Custody"],
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
        previous["Expiry Month Key"] = previous["Expiry Date"].dt.strftime("%Y-%m")
        current["Expiry Month Key"] = current["Expiry Date"].dt.strftime("%Y-%m")
        previous["GTIN"] = previous["GTIN"].fillna("").astype(str).str.strip()
        current["GTIN"] = current["GTIN"].fillna("").astype(str).str.strip()

        # Confirmation uses verified SFDA identity + BN + expiry month. Exact
        # SFDA expiry day is an output/regulatory value, not a WMS match key.
        comparison = previous.merge(
            current,
            on=["GTIN", "BN", "Expiry Month Key"],
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
                expiry = _value(evidence, "Expiry Date Current")
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
            t.ReferenceNumber, t.ReferenceLine, t.ToAddress, t.Custody,
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
                "Dispatched Quantity": float(row[9] or 0),
                "To Address": str(row[6] or "").strip(),
                "Sales Order Number": str(row[4] or "").strip(),
                "Order Line": str(row[5] or "").strip(),
                "Custody": str(row[7] or "").strip(),
                "Dispatch Date": row[8],
            }
        )
    return records




def reconcile_affected_batch_master_event_totals(
    receipt_history_rows: List[Dict[str, Any]],
    dispatch_history_rows: List[Dict[str, Any]],
) -> Dict[str, int]:
    """Reconcile ONLY Batch Master keys represented by the current uploaded files.

    This is the production self-healing path for Historical Append.

    Event tables remain the durable source of truth and event deduplication remains
    unchanged.  The uploaded file determines the affected
    BN + ExpiryMonthKey + GenericItemNumber keys.  For those keys only, SQL
    recalculates cumulative receipt/dispatch totals and first/last movement dates
    directly from ReceiptEvents / DispatchEvents, then overwrites the derived
    BatchMaster movement fields.

    This repairs cases where a previous job saved an event successfully but failed
    before refreshing BatchMaster, without rebuilding or scanning the full master.
    """
    initialize_database()
    started_at = time.perf_counter()

    def _key(row: Dict[str, Any]) -> tuple[str, str, str]:
        return (
            str(row.get("BN") or "").strip(),
            str(row.get("Expiry Month Key") or "").strip(),
            str(row.get("Generic Item Number") or "").strip(),
        )

    affected = sorted({
        key
        for row in list(receipt_history_rows or []) + list(dispatch_history_rows or [])
        for key in [_key(row)]
        if all(key)
    })

    if not affected:
        return {
            "affected_batch_keys": 0,
            "batch_master_rows_reconciled": 0,
        }

    with Database().connect() as connection:
        cursor = connection.cursor()
        try:
            cursor.execute(r"""
                CREATE TABLE #AffectedMovementKeys
                (
                    BN nvarchar(255) NOT NULL,
                    ExpiryMonthKey nvarchar(20) NOT NULL,
                    GenericItemNumber nvarchar(255) NOT NULL,
                    PRIMARY KEY (BN, ExpiryMonthKey, GenericItemNumber)
                );
            """)
            cursor.fast_executemany = True
            cursor.executemany(
                """
                INSERT INTO #AffectedMovementKeys
                    (BN, ExpiryMonthKey, GenericItemNumber)
                VALUES (?, ?, ?);
                """,
                affected,
            )

            cursor.execute(r"""
                ;WITH ReceiptAggregate AS
                (
                    SELECT
                        r.BN,
                        r.ExpiryMonthKey,
                        r.GenericItemNumber,
                        MAX(NULLIF(r.Description, N'')) AS Description,
                        MAX(NULLIF(r.SupplierName, N'')) AS SupplierName,
                        MAX(NULLIF(r.SupplierCode, N'')) AS SupplierCode,
                        MAX(NULLIF(r.TradeName, N'')) AS TradeName,
                        MAX(NULLIF(r.ItemFamilyGroup, N'')) AS ItemFamilyGroup,
                        SUM(COALESCE(r.ReceivedQuantity, 0)) AS TotalReceiveQty,
                        COUNT_BIG(*) AS ReceiveRuns,
                        MIN(r.ReceivedDate) AS FirstReceivedDate,
                        MAX(r.ReceivedDate) AS LastReceivedDate
                    FROM dbo.ReceiptEvents AS r
                    INNER JOIN #AffectedMovementKeys AS a
                        ON a.BN = r.BN
                       AND a.ExpiryMonthKey = r.ExpiryMonthKey
                       AND a.GenericItemNumber = r.GenericItemNumber
                    WHERE
                        UPPER(LTRIM(RTRIM(ISNULL(r.InboundShipment, N'')))) LIKE N'TRK5060%'
                        OR UPPER(LTRIM(RTRIM(ISNULL(r.InboundShipment, N'')))) LIKE N'TRK800%'
                        OR UPPER(LTRIM(RTRIM(ISNULL(r.InboundShipment, N'')))) LIKE N'TRK49%'
                    GROUP BY
                        r.BN,
                        r.ExpiryMonthKey,
                        r.GenericItemNumber
                ),
                DispatchAggregate AS
                (
                    SELECT
                        d.BN,
                        d.ExpiryMonthKey,
                        d.GenericItemNumber,
                        SUM(COALESCE(d.DispatchedQuantity, 0)) AS TotalDispatchedQty,
                        COUNT_BIG(*) AS DispatchRuns,
                        MIN(d.DispatchDate) AS FirstDispatchDate,
                        MAX(d.DispatchDate) AS LastDispatchDate
                    FROM dbo.DispatchEvents AS d
                    INNER JOIN #AffectedMovementKeys AS a
                        ON a.BN = d.BN
                       AND a.ExpiryMonthKey = d.ExpiryMonthKey
                       AND a.GenericItemNumber = d.GenericItemNumber
                    GROUP BY
                        d.BN,
                        d.ExpiryMonthKey,
                        d.GenericItemNumber
                ),
                TradeCodes AS
                (
                    SELECT
                        BN,
                        ExpiryMonthKey,
                        GenericItemNumber,
                        LEFT(
                            STRING_AGG(CONVERT(nvarchar(max), TradeItemNumber), N' | ')
                                WITHIN GROUP (ORDER BY SourcePriority, TradeItemNumber),
                            255
                        ) AS TradeItemNumber
                    FROM
                    (
                        SELECT
                            BN,
                            ExpiryMonthKey,
                            GenericItemNumber,
                            TradeItemNumber,
                            MIN(SourcePriority) AS SourcePriority
                        FROM
                        (
                            SELECT r.BN, r.ExpiryMonthKey, r.GenericItemNumber,
                                   NULLIF(LTRIM(RTRIM(r.TradeItemNumber)), N'') AS TradeItemNumber,
                                   0 AS SourcePriority
                            FROM dbo.ReceiptEvents r
                            INNER JOIN #AffectedMovementKeys a
                                ON a.BN = r.BN
                               AND a.ExpiryMonthKey = r.ExpiryMonthKey
                               AND a.GenericItemNumber = r.GenericItemNumber
                            UNION ALL
                            SELECT d.BN, d.ExpiryMonthKey, d.GenericItemNumber,
                                   NULLIF(LTRIM(RTRIM(d.TradeItemNumber)), N''),
                                   1
                            FROM dbo.DispatchEvents d
                            INNER JOIN #AffectedMovementKeys a
                                ON a.BN = d.BN
                               AND a.ExpiryMonthKey = d.ExpiryMonthKey
                               AND a.GenericItemNumber = d.GenericItemNumber
                        ) source_codes
                        WHERE TradeItemNumber IS NOT NULL
                        GROUP BY BN, ExpiryMonthKey, GenericItemNumber, TradeItemNumber
                    ) distinct_codes
                    GROUP BY BN, ExpiryMonthKey, GenericItemNumber
                )
                UPDATE bm
                SET
                    bm.Description = COALESCE(NULLIF(ra.Description, N''), bm.Description),
                    bm.SupplierName = COALESCE(NULLIF(ra.SupplierName, N''), bm.SupplierName),
                    bm.SupplierCode = COALESCE(NULLIF(ra.SupplierCode, N''), bm.SupplierCode),
                    bm.TradeName = COALESCE(NULLIF(ra.TradeName, N''), bm.TradeName),
                    bm.ItemFamilyGroup = COALESCE(NULLIF(ra.ItemFamilyGroup, N''), bm.ItemFamilyGroup),
                    bm.TradeItemNumber = COALESCE(NULLIF(tc.TradeItemNumber, N''), bm.TradeItemNumber),
                    bm.TotalReceiveQty = COALESCE(ra.TotalReceiveQty, 0),
                    bm.ReceiveRuns = COALESCE(ra.ReceiveRuns, 0),
                    bm.FirstReceivedDate = ra.FirstReceivedDate,
                    bm.LastReceivedDate = ra.LastReceivedDate,
                    bm.TotalDispatchedQty = COALESCE(da.TotalDispatchedQty, 0),
                    bm.DispatchRuns = COALESCE(da.DispatchRuns, 0),
                    bm.FirstDispatchDate = da.FirstDispatchDate,
                    bm.LastDispatchDate = da.LastDispatchDate,
                    bm.LastUpdated = SYSUTCDATETIME()
                FROM dbo.BatchMaster AS bm
                INNER JOIN #AffectedMovementKeys AS a
                    ON a.BN = bm.BN
                   AND a.ExpiryMonthKey = bm.ExpiryMonthKey
                   AND a.GenericItemNumber = bm.GenericItemNumber
                LEFT JOIN ReceiptAggregate AS ra
                    ON ra.BN = bm.BN
                   AND ra.ExpiryMonthKey = bm.ExpiryMonthKey
                   AND ra.GenericItemNumber = bm.GenericItemNumber
                LEFT JOIN DispatchAggregate AS da
                    ON da.BN = bm.BN
                   AND da.ExpiryMonthKey = bm.ExpiryMonthKey
                   AND da.GenericItemNumber = bm.GenericItemNumber
                LEFT JOIN TradeCodes AS tc
                    ON tc.BN = bm.BN
                   AND tc.ExpiryMonthKey = bm.ExpiryMonthKey
                   AND tc.GenericItemNumber = bm.GenericItemNumber;
            """)
            reconciled = max(0, int(cursor.rowcount or 0))
            connection.commit()
        except Exception:
            connection.rollback()
            raise

    logger.info(
        "Affected Batch Master reconciliation completed in %.2f seconds. affected_keys=%s reconciled_rows=%s",
        time.perf_counter() - started_at,
        len(affected),
        reconciled,
    )
    return {
        "affected_batch_keys": int(len(affected)),
        "batch_master_rows_reconciled": int(reconciled),
    }


def reconcile_batch_master_event_totals() -> Dict[str, int]:
    """Self-heal Batch Master movement totals from durable event tables.

    ReceiptEvents and DispatchEvents are the source of truth. Historical jobs can
    legitimately save events and then fail before refreshing BatchMaster. This
    set-based repair removes that inconsistency by recalculating movement totals,
    run counts, and first/last movement dates directly from SQL events for every
    BatchMaster row visible to the current warehouse RLS context.

    Only physical receipt classes (TRK5060/TRK800/TRK49) contribute to receipt
    totals. DispatchEvents already contain only confirmed Full Dispatch rows.
    """
    initialize_database()
    started_at = time.perf_counter()

    with Database().connect() as connection:
        cursor = connection.cursor()
        try:
            cursor.execute(r"""
                ;WITH ReceiptAggregate AS
                (
                    SELECT
                        r.BN,
                        r.ExpiryMonthKey,
                        r.GenericItemNumber,
                        SUM(COALESCE(r.ReceivedQuantity, 0)) AS TotalReceiveQty,
                        COUNT_BIG(*) AS ReceiveRuns,
                        MIN(r.ReceivedDate) AS FirstReceivedDate,
                        MAX(r.ReceivedDate) AS LastReceivedDate
                    FROM dbo.ReceiptEvents r
                    WHERE
                        UPPER(LTRIM(RTRIM(ISNULL(r.InboundShipment, N'')))) LIKE N'TRK5060%'
                        OR UPPER(LTRIM(RTRIM(ISNULL(r.InboundShipment, N'')))) LIKE N'TRK800%'
                        OR UPPER(LTRIM(RTRIM(ISNULL(r.InboundShipment, N'')))) LIKE N'TRK49%'
                    GROUP BY r.BN, r.ExpiryMonthKey, r.GenericItemNumber
                ),
                DispatchAggregate AS
                (
                    SELECT
                        d.BN,
                        d.ExpiryMonthKey,
                        d.GenericItemNumber,
                        SUM(COALESCE(d.DispatchedQuantity, 0)) AS TotalDispatchedQty,
                        COUNT_BIG(*) AS DispatchRuns,
                        MIN(d.DispatchDate) AS FirstDispatchDate,
                        MAX(d.DispatchDate) AS LastDispatchDate,
                        MAX(NULLIF(d.Custody, N'')) AS Custody
                    FROM dbo.DispatchEvents d
                    GROUP BY d.BN, d.ExpiryMonthKey, d.GenericItemNumber
                ),
                TradeCodes AS
                (
                    SELECT
                        BN,
                        ExpiryMonthKey,
                        GenericItemNumber,
                        LEFT(
                            STRING_AGG(CONVERT(nvarchar(max), TradeItemNumber), N' | ')
                                WITHIN GROUP (ORDER BY SourcePriority, TradeItemNumber),
                            255
                        ) AS TradeItemNumber
                    FROM
                    (
                        SELECT
                            BN, ExpiryMonthKey, GenericItemNumber, TradeItemNumber,
                            MIN(SourcePriority) AS SourcePriority
                        FROM
                        (
                            SELECT r.BN, r.ExpiryMonthKey, r.GenericItemNumber,
                                   NULLIF(LTRIM(RTRIM(r.TradeItemNumber)), N'') AS TradeItemNumber,
                                   0 AS SourcePriority
                            FROM dbo.ReceiptEvents r
                            UNION ALL
                            SELECT d.BN, d.ExpiryMonthKey, d.GenericItemNumber,
                                   NULLIF(LTRIM(RTRIM(d.TradeItemNumber)), N''),
                                   1
                            FROM dbo.DispatchEvents d
                        ) source_codes
                        WHERE TradeItemNumber IS NOT NULL
                        GROUP BY BN, ExpiryMonthKey, GenericItemNumber, TradeItemNumber
                    ) distinct_codes
                    GROUP BY BN, ExpiryMonthKey, GenericItemNumber
                )
                UPDATE bm
                SET
                    bm.TradeItemNumber = COALESCE(NULLIF(tc.TradeItemNumber, N''), bm.TradeItemNumber),
                    bm.TotalReceiveQty = COALESCE(ra.TotalReceiveQty, 0),
                    bm.ReceiveRuns = COALESCE(ra.ReceiveRuns, 0),
                    bm.FirstReceivedDate = ra.FirstReceivedDate,
                    bm.LastReceivedDate = ra.LastReceivedDate,
                    bm.TotalDispatchedQty = COALESCE(da.TotalDispatchedQty, 0),
                    bm.DispatchRuns = COALESCE(da.DispatchRuns, 0),
                    bm.FirstDispatchDate = da.FirstDispatchDate,
                    bm.LastDispatchDate = da.LastDispatchDate,
                    bm.LastUpdated = SYSUTCDATETIME()
                FROM dbo.BatchMaster bm
                LEFT JOIN ReceiptAggregate ra
                    ON ra.BN = bm.BN
                   AND ra.ExpiryMonthKey = bm.ExpiryMonthKey
                   AND ra.GenericItemNumber = bm.GenericItemNumber
                LEFT JOIN DispatchAggregate da
                    ON da.BN = bm.BN
                   AND da.ExpiryMonthKey = bm.ExpiryMonthKey
                   AND da.GenericItemNumber = bm.GenericItemNumber
                LEFT JOIN TradeCodes tc
                    ON tc.BN = bm.BN
                   AND tc.ExpiryMonthKey = bm.ExpiryMonthKey
                   AND tc.GenericItemNumber = bm.GenericItemNumber
                WHERE
                    ISNULL(bm.TradeItemNumber, N'') <> ISNULL(tc.TradeItemNumber, ISNULL(bm.TradeItemNumber, N''))
                    OR COALESCE(bm.TotalReceiveQty, 0) <> COALESCE(ra.TotalReceiveQty, 0)
                    OR COALESCE(bm.ReceiveRuns, 0) <> COALESCE(ra.ReceiveRuns, 0)
                    OR ISNULL(bm.FirstReceivedDate, '19000101') <> ISNULL(ra.FirstReceivedDate, '19000101')
                    OR ISNULL(bm.LastReceivedDate, '19000101') <> ISNULL(ra.LastReceivedDate, '19000101')
                    OR COALESCE(bm.TotalDispatchedQty, 0) <> COALESCE(da.TotalDispatchedQty, 0)
                    OR COALESCE(bm.DispatchRuns, 0) <> COALESCE(da.DispatchRuns, 0)
                    OR ISNULL(bm.FirstDispatchDate, '19000101') <> ISNULL(da.FirstDispatchDate, '19000101')
                    OR ISNULL(bm.LastDispatchDate, '19000101') <> ISNULL(da.LastDispatchDate, '19000101');
            """)
            repaired = max(0, int(cursor.rowcount or 0))
            connection.commit()
        except Exception:
            connection.rollback()
            raise

    logger.info(
        "Batch Master event-total consistency repair completed in %.2f seconds. repaired_rows=%s",
        time.perf_counter() - started_at,
        repaired,
    )
    return {"batch_master_rows_repaired": repaired}



def _prepare_incremental_accept_refresh_scope(
    receipt_history_rows: List[Dict[str, Any]],
    sfda_df: pd.DataFrame,
    dispatch_history_rows: Optional[List[Dict[str, Any]]] = None,
) -> tuple[list[tuple[str, str, str]], list[tuple[Any, ...]], Dict[str, Any]]:
    """Prepare the affected Accept keys and exact SFDA identity rows efficiently.

    Historical Append can contain thousands of WMS rows while the SFDA report can
    contain many thousands of batch rows. Exact SFDA identity must be discoverable
    from either Receipt or Dispatch history, because an SFDA batch may legitimately
    be dispatch-only in the uploaded historical window. Receipt keys remain the
    only source for Stage-2 "Missing Batch in SFDA" discovery.

    The uploaded WMS candidates are indexed once and SFDA work is restricted to
    the exact intersecting BN + ExpiryMonthKey keys. Product-name validation is a
    conflict detector after the exact key match; it no longer rejects known aliases
    merely because their fuzzy score is below 60.
    """
    started_at = time.perf_counter()

    def _eligible_physical_receipt(row: Dict[str, Any]) -> bool:
        shipment = str(row.get("Inbound Shipment") or "").strip().upper()
        return (
            shipment.startswith("TRK5060")
            or shipment.startswith("TRK800")
            or shipment.startswith("TRK49")
        )

    affected = sorted({
        (
            str(row.get("BN") or "").strip(),
            str(row.get("Expiry Month Key") or "").strip(),
            str(row.get("Generic Item Number") or "").strip(),
        )
        for row in (receipt_history_rows or [])
        if _eligible_physical_receipt(row)
        and str(row.get("BN") or "").strip()
        and str(row.get("Expiry Month Key") or "").strip()
        and str(row.get("Generic Item Number") or "").strip()
    })

    from engine.full_reconciliation import FullReconciliationEngine
    from engine.normalizer import (
        HISTORICAL_MATCH_LEGACY_THRESHOLD,
        HISTORICAL_MATCH_LOGIC_VERSION,
        Normalizer,
    )
    from engine.pack_size_resolver import PackSizeResolver

    current_sfda = Normalizer.normalize_sfda(
        sfda_df.copy() if sfda_df is not None else pd.DataFrame()
    )
    if not current_sfda.empty:
        current_sfda["Expiry Month Key"] = FullReconciliationEngine._month_key(
            current_sfda["Expiry Date"]
        )
        for column in [
            "Quantity", "Active", "Quantity sent pending",
            "Quantity Receive Pending",
        ]:
            current_sfda[column] = pd.to_numeric(
                current_sfda[column], errors="coerce"
            ).fillna(0)

    pack_resolver = PackSizeResolver.from_config()

    candidate_frames = []
    receipt_candidates = pd.DataFrame(receipt_history_rows or [])
    if not receipt_candidates.empty:
        receipt_candidates["_Identity Source Priority"] = 0
        candidate_frames.append(receipt_candidates)

    dispatch_candidates = pd.DataFrame(dispatch_history_rows or [])
    if not dispatch_candidates.empty:
        dispatch_candidates["_Identity Source Priority"] = 1
        candidate_frames.append(dispatch_candidates)

    candidate_rows = (
        pd.concat(candidate_frames, ignore_index=True, sort=False)
        if candidate_frames else pd.DataFrame()
    )
    if not candidate_rows.empty:
        for col in ["BN", "Expiry Month Key", "Generic Item Number", "Trade Name", "Description"]:
            if col not in candidate_rows.columns:
                candidate_rows[col] = ""
        candidate_rows["BN"] = Normalizer.text(candidate_rows["BN"])
        candidate_rows["Expiry Month Key"] = (
            candidate_rows["Expiry Month Key"].fillna("").astype(str).str.strip()
        )
        candidate_rows["Generic Item Number"] = (
            candidate_rows["Generic Item Number"].fillna("").astype(str).str.strip()
        )
        candidate_rows["Trade Name"] = (
            candidate_rows["Trade Name"].fillna("").astype(str).str.strip()
        )
        candidate_rows["Description"] = (
            candidate_rows["Description"].fillna("").astype(str).str.strip()
        )
        candidate_rows = candidate_rows.loc[
            candidate_rows["BN"].ne("")
            & candidate_rows["Expiry Month Key"].ne("")
            & candidate_rows["Generic Item Number"].ne("")
        ].sort_values("_Identity Source Priority", kind="stable").copy()

    resolved_generics: Dict[tuple[str, str], list[str]] = {}
    evidence_by_identity: Dict[tuple[str, str, str], tuple[str, str]] = {}
    candidate_keys: Set[tuple[str, str]] = set()
    identity_exact_candidates = 0
    identity_accepted = 0
    identity_rejected = 0
    identity_accepted_below_legacy_threshold = 0

    if not candidate_rows.empty:
        candidate_keys = set(
            zip(
                candidate_rows["BN"].astype(str),
                candidate_rows["Expiry Month Key"].astype(str),
            )
        )

        # Build both WMS identity evidence sources once. Receipt rows may carry
        # a detailed Description with full concentration (e.g. 100MG/10ML),
        # while Trade Name can contain only the total dose (100MG). Dispatch-only
        # candidates naturally keep Description blank.
        for (bn, month, generic), group in candidate_rows.groupby(
            ["BN", "Expiry Month Key", "Generic Item Number"],
            sort=False,
            dropna=False,
        ):
            trade = next(
                (str(v).strip() for v in group["Trade Name"] if str(v).strip()),
                "",
            )
            description = next(
                (str(v).strip() for v in group["Description"] if str(v).strip()),
                "",
            )
            evidence_by_identity[(str(bn), str(month), str(generic))] = (trade, description)

    if not current_sfda.empty and candidate_keys:
        current_sfda["BN"] = Normalizer.text(current_sfda["BN"])
        current_sfda["Drug Name"] = Normalizer.text(current_sfda["Drug Name"])
        current_sfda["GTIN"] = Normalizer.text(current_sfda["GTIN"])

        # Same safety rule as before: one BN+expiry key may resolve only when it
        # points to exactly one non-empty GTIN in the current SFDA report.
        gtin_counts = (
            current_sfda.loc[current_sfda["GTIN"].ne("")]
            .groupby(["BN", "Expiry Month Key"])["GTIN"]
            .nunique()
        )
        safe_keys = set(gtin_counts[gtin_counts.eq(1)].index.tolist())
        relevant_keys = safe_keys.intersection(candidate_keys)

        if relevant_keys:
            sfda_index = pd.MultiIndex.from_frame(
                current_sfda[["BN", "Expiry Month Key"]]
            )
            current_sfda = current_sfda.loc[
                sfda_index.isin(relevant_keys)
            ].copy()
        else:
            current_sfda = current_sfda.iloc[0:0].copy()

        # Candidate Generics are already grouped once by exact batch key, so each
        # SFDA key performs O(1) dictionary lookups instead of rescanning all ASN
        # rows.
        candidates_by_batch: Dict[tuple[str, str], list[tuple[str, str, str]]] = {}
        for (bn, month, generic), (trade, description) in evidence_by_identity.items():
            candidates_by_batch.setdefault((bn, month), []).append((generic, trade, description))

        for (bn, month), sf_group in current_sfda.groupby(
            ["BN", "Expiry Month Key"], sort=False
        ):
            candidates = candidates_by_batch.get((str(bn), str(month)), [])
            if not candidates:
                continue

            sfda_drug = str(sf_group.iloc[0]["Drug Name"])
            accepted: list[str] = []
            for generic, trade, description in candidates:
                trade_score = Normalizer.drug_name_match_score(sfda_drug, trade) if str(trade).strip() else 0.0
                description_score = Normalizer.drug_name_match_score(sfda_drug, description) if str(description).strip() else 0.0
                score = max(float(trade_score), float(description_score))
                reference_match = pack_resolver.same_product_identity(sfda_drug, trade)
                description_reference_match = pack_resolver.same_product_identity(sfda_drug, description)
                identity_exact_candidates += 1
                passed = Normalizer.drug_name_validation_pass(
                    sfda_drug,
                    trade,
                    threshold=HISTORICAL_MATCH_LEGACY_THRESHOLD,
                    reference_match=reference_match,
                    wms_description=description,
                    reference_match_description=description_reference_match,
                )
                if passed:
                    identity_accepted += 1
                    if score < float(HISTORICAL_MATCH_LEGACY_THRESHOLD):
                        identity_accepted_below_legacy_threshold += 1
                    accepted.append(str(generic))
                else:
                    identity_rejected += 1
                    logger.warning(
                        "Incremental exact batch match rejected by product-identity validation. "
                        "BN=%s expiry_month=%s generic=%s SFDA_drug=%s WMS_trade=%s "
                        "WMS_description=%s score=%.2f product_master_trade=%s product_master_description=%s",
                        bn, month, str(generic), sfda_drug, str(trade), str(description), score,
                        str(reference_match), str(description_reference_match),
                    )
            if accepted:
                resolved_generics[(str(bn), str(month))] = accepted

    sfda_rows: list[tuple[Any, ...]] = []
    if not current_sfda.empty and resolved_generics:
        for row in current_sfda.to_dict(orient="records"):
            bn = _text(row, "BN")
            month = _text(row, "Expiry Month Key")
            if not bn or not month:
                continue
            generics = resolved_generics.get((bn, month), [])
            if not generics:
                continue

            drug_name = _text(row, "Drug Name")
            for generic in generics:
                trade_text, description_text = evidence_by_identity.get(
                    (bn, month, str(generic)), ("", "")
                )
                resolved_pack, _pack_status = pack_resolver.resolve(
                    drug_name, trade_text or description_text
                )
                package_size = float(resolved_pack or 1.0)
                if package_size <= 0:
                    package_size = 1.0
                sfda_rows.append((
                    _text(row, "GTIN"), drug_name, bn, month, generic,
                    package_size,
                    _value(row, "Expiry Date"), _number(row, "Quantity"),
                    _number(row, "Active"), _number(row, "Quantity sent pending"),
                    _number(row, "Quantity Receive Pending"),
                ))

    metrics = {
        "logic_version": HISTORICAL_MATCH_LOGIC_VERSION,
        "append_match_pipeline": "V6_PREPARED_TRADE_AND_DESCRIPTION",
        "legacy_threshold": float(HISTORICAL_MATCH_LEGACY_THRESHOLD),
        "identity_exact_candidates": int(identity_exact_candidates),
        "identity_accepted": int(identity_accepted),
        "identity_rejected": int(identity_rejected),
        "identity_accepted_below_legacy_threshold": int(identity_accepted_below_legacy_threshold),
        "candidate_rows": int(len(candidate_rows)),
        "candidate_rows_with_description": int(
            candidate_rows["Description"].astype(str).str.strip().ne("").sum()
        ) if not candidate_rows.empty else 0,
        "identity_receipt_rows": int(len(receipt_history_rows or [])),
        "identity_dispatch_rows": int(len(dispatch_history_rows or [])),
        "affected_receipt_keys": int(len(affected)),
        "candidate_batch_keys": int(len(candidate_keys)),
        "relevant_sfda_batch_keys": int(len(resolved_generics)),
        "prepared_sfda_identity_rows": int(len(sfda_rows)),
        "seconds": round(time.perf_counter() - started_at, 3),
    }
    logger.info("Incremental Accept scope prepared. %s", metrics)
    return affected, sfda_rows, metrics


def refresh_historical_append_incremental(
    receipt_history_rows: List[Dict[str, Any]],
    dispatch_history_rows: List[Dict[str, Any]],
    sfda_df: pd.DataFrame,
) -> Dict[str, Any]:
    """Unified, self-healing Historical Append refresh.

    The previous Historical Append path performed three large SQL passes:
    Accept refresh, Dispatch refresh, then a second durable-event reconciliation.
    Each pass re-read ReceiptEvents/DispatchEvents for almost the same batch keys.

    This function preserves the same business rules and crash-recovery semantics,
    but materializes the affected durable event rows once, aggregates them once,
    updates BatchMaster once, and rebuilds only the touched supplier/customer
    history rows.  The affected scope is derived from the uploaded file rather
    than inserted-event counts, so retrying a file after a post-save failure still
    repairs BatchMaster even when every EventKey is already a duplicate.
    """
    initialize_database()
    started_at = time.perf_counter()

    from engine.warehouse_context import current_historical_build_id, current_warehouse_id

    warehouse_id = int(current_warehouse_id())
    build_id = (
        str(current_historical_build_id() or "").strip()
        or get_active_historical_build_id(warehouse_id)
    )
    if not build_id:
        raise RuntimeError("Historical Append requires an active BuildID.")

    accept_affected, sfda_rows, scope_metrics = _prepare_incremental_accept_refresh_scope(
        receipt_history_rows,
        sfda_df,
        dispatch_history_rows=dispatch_history_rows,
    )

    def _movement_key(row: Dict[str, Any]) -> tuple[str, str, str]:
        return (
            str(row.get("BN") or "").strip(),
            str(row.get("Expiry Month Key") or "").strip(),
            str(row.get("Generic Item Number") or "").strip(),
        )

    receipt_keys = sorted({
        key
        for row in (receipt_history_rows or [])
        for key in [_movement_key(row)]
        if all(key)
    })
    dispatch_keys = sorted({
        key
        for row in (dispatch_history_rows or [])
        for key in [_movement_key(row)]
        if all(key)
    })
    movement_keys = sorted(set(receipt_keys).union(dispatch_keys))

    if not movement_keys:
        return {
            "affected_batch_keys": 0,
            "accept_affected_batch_keys": 0,
            "dispatch_affected_batch_keys": 0,
            "batch_master_rows_updated": 0,
            "batch_master_rows_inserted": 0,
            "supplier_history_rows_rebuilt": 0,
            "customer_history_rows_rebuilt": 0,
            "scope_prepare_seconds": float(scope_metrics.get("seconds", 0) or 0),
            "sql_refresh_seconds": 0.0,
            "match_diagnostics": scope_metrics,
            "timings_seconds": {},
        }

    timings: Dict[str, float] = {}
    sql_started_at = time.perf_counter()

    with Database().connect() as connection:
        cursor = connection.cursor()
        try:
            stage_started = time.perf_counter()
            cursor.execute(r"""
                CREATE TABLE #AffectedAcceptKeys
                (
                    BN nvarchar(255) NOT NULL,
                    ExpiryMonthKey nvarchar(20) NOT NULL,
                    GenericItemNumber nvarchar(255) NOT NULL,
                    PRIMARY KEY (BN, ExpiryMonthKey, GenericItemNumber)
                );

                CREATE TABLE #AffectedDispatchKeys
                (
                    BN nvarchar(255) NOT NULL,
                    ExpiryMonthKey nvarchar(20) NOT NULL,
                    GenericItemNumber nvarchar(255) NOT NULL,
                    PRIMARY KEY (BN, ExpiryMonthKey, GenericItemNumber)
                );

                CREATE TABLE #AffectedMovementKeys
                (
                    BN nvarchar(255) NOT NULL,
                    ExpiryMonthKey nvarchar(20) NOT NULL,
                    GenericItemNumber nvarchar(255) NOT NULL,
                    PRIMARY KEY (BN, ExpiryMonthKey, GenericItemNumber)
                );

                CREATE TABLE #CurrentAcceptSFDA
                (
                    GTIN nvarchar(255) NULL,
                    DrugName nvarchar(500) NULL,
                    BN nvarchar(255) NOT NULL,
                    ExpiryMonthKey nvarchar(20) NOT NULL,
                    ResolvedGenericItemNumber nvarchar(255) NOT NULL,
                    PackageSize decimal(38, 6) NULL,
                    ExpiryDate date NULL,
                    Quantity decimal(38, 6) NULL,
                    Active decimal(38, 6) NULL,
                    QuantitySentPending decimal(38, 6) NULL,
                    QuantityReceivePending decimal(38, 6) NULL
                );

                -- IMPORTANT: create durable-event temp tables in this outer,
                -- non-parameterized batch. With ODBC Driver 18 a temp table
                -- created by SELECT...INTO inside a parameterized execute can
                -- be scoped to the driver's prepared statement and disappear
                -- before the next cursor.execute call. Creating the temp
                -- tables here keeps them alive for the whole SQL session.
                SELECT TOP (0)
                    BN, ExpiryMonthKey, GenericItemNumber, ExpiryDate,
                    TradeItemNumber, TradeName, ReceivedQuantity, InboundShipment,
                    SupplierName, SupplierCode, Description, ItemFamilyGroup, ReceivedDate
                INTO #ReceiptAffectedRows
                FROM dbo.ReceiptEvents;

                SELECT TOP (0)
                    BN, ExpiryMonthKey, GenericItemNumber, ExpiryDate,
                    TradeItemNumber, TradeName, DispatchedQuantity, ToAddress,
                    DispatchDate, Custody
                INTO #DispatchAffectedRows
                FROM dbo.DispatchEvents;
            """)
            cursor.fast_executemany = True
            if accept_affected:
                cursor.executemany(
                    "INSERT INTO #AffectedAcceptKeys (BN, ExpiryMonthKey, GenericItemNumber) VALUES (?, ?, ?);",
                    accept_affected,
                )
            if dispatch_keys:
                cursor.executemany(
                    "INSERT INTO #AffectedDispatchKeys (BN, ExpiryMonthKey, GenericItemNumber) VALUES (?, ?, ?);",
                    dispatch_keys,
                )
            cursor.executemany(
                "INSERT INTO #AffectedMovementKeys (BN, ExpiryMonthKey, GenericItemNumber) VALUES (?, ?, ?);",
                movement_keys,
            )
            if sfda_rows:
                cursor.executemany(
                    r"""
                    INSERT INTO #CurrentAcceptSFDA
                    (GTIN, DrugName, BN, ExpiryMonthKey, ResolvedGenericItemNumber, PackageSize,
                     ExpiryDate, Quantity, Active, QuantitySentPending, QuantityReceivePending)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
                    """,
                    sfda_rows,
                )
                cursor.execute(
                    "CREATE INDEX IX_CurrentAcceptSFDA_Batch ON #CurrentAcceptSFDA "
                    "(BN, ExpiryMonthKey, ResolvedGenericItemNumber, GTIN);"
                )
            timings["stage_affected_keys"] = round(time.perf_counter() - stage_started, 3)

            # Materialize the durable event rows exactly once. Explicit WarehouseID
            # + BuildID filters make the historical covering indexes directly usable
            # instead of relying only on the security predicate to infer scope.
            # fast_executemany is only needed for the small temp-key staging above;
            # turn it off before normal SQL execution so subsequent statements use
            # the regular cursor execution path.
            cursor.fast_executemany = False

            receipt_materialize_started = time.perf_counter()
            cursor.execute(r"""
                INSERT INTO #ReceiptAffectedRows
                (
                    BN, ExpiryMonthKey, GenericItemNumber, ExpiryDate,
                    TradeItemNumber, TradeName, ReceivedQuantity, InboundShipment,
                    SupplierName, SupplierCode, Description, ItemFamilyGroup, ReceivedDate
                )
                SELECT
                    r.BN,
                    r.ExpiryMonthKey,
                    r.GenericItemNumber,
                    r.ExpiryDate,
                    r.TradeItemNumber,
                    r.TradeName,
                    r.ReceivedQuantity,
                    r.InboundShipment,
                    r.SupplierName,
                    r.SupplierCode,
                    r.Description,
                    r.ItemFamilyGroup,
                    r.ReceivedDate
                FROM dbo.ReceiptEvents AS r
                INNER JOIN #AffectedMovementKeys AS a
                    ON a.BN = r.BN
                   AND a.ExpiryMonthKey = r.ExpiryMonthKey
                   AND a.GenericItemNumber = r.GenericItemNumber
                WHERE r.WarehouseID = ?
                  AND r.BuildID = ?
                  AND (
                        UPPER(LTRIM(RTRIM(ISNULL(r.InboundShipment, N'')))) LIKE N'TRK5060%'
                     OR UPPER(LTRIM(RTRIM(ISNULL(r.InboundShipment, N'')))) LIKE N'TRK800%'
                     OR UPPER(LTRIM(RTRIM(ISNULL(r.InboundShipment, N'')))) LIKE N'TRK49%'
                  );

                CREATE CLUSTERED INDEX IX_ReceiptAffectedRows_Batch
                    ON #ReceiptAffectedRows (BN, ExpiryMonthKey, GenericItemNumber);
            """, warehouse_id, build_id)
            timings["materialize_receipt_events"] = round(
                time.perf_counter() - receipt_materialize_started, 3
            )

            dispatch_materialize_started = time.perf_counter()
            cursor.execute(r"""
                INSERT INTO #DispatchAffectedRows
                (
                    BN, ExpiryMonthKey, GenericItemNumber, ExpiryDate,
                    TradeItemNumber, TradeName, DispatchedQuantity, ToAddress,
                    DispatchDate, Custody
                )
                SELECT
                    d.BN,
                    d.ExpiryMonthKey,
                    d.GenericItemNumber,
                    d.ExpiryDate,
                    d.TradeItemNumber,
                    d.TradeName,
                    d.DispatchedQuantity,
                    d.ToAddress,
                    d.DispatchDate,
                    d.Custody
                FROM dbo.DispatchEvents AS d
                INNER JOIN #AffectedMovementKeys AS a
                    ON a.BN = d.BN
                   AND a.ExpiryMonthKey = d.ExpiryMonthKey
                   AND a.GenericItemNumber = d.GenericItemNumber
                WHERE d.WarehouseID = ?
                  AND d.BuildID = ?;

                CREATE CLUSTERED INDEX IX_DispatchAffectedRows_Batch
                    ON #DispatchAffectedRows (BN, ExpiryMonthKey, GenericItemNumber);
            """, warehouse_id, build_id)
            timings["materialize_dispatch_events"] = round(
                time.perf_counter() - dispatch_materialize_started, 3
            )

            aggregate_started = time.perf_counter()
            cursor.execute(r"""
                SELECT
                    r.BN,
                    r.ExpiryMonthKey,
                    r.GenericItemNumber,
                    MAX(r.ExpiryDate) AS ExpiryDate,
                    MAX(NULLIF(r.TradeName, N'')) AS TradeName,
                    MAX(NULLIF(r.Description, N'')) AS Description,
                    MAX(NULLIF(r.ItemFamilyGroup, N'')) AS ItemFamilyGroup,
                    MAX(NULLIF(r.SupplierName, N'')) AS SupplierName,
                    MAX(NULLIF(r.SupplierCode, N'')) AS SupplierCode,
                    SUM(COALESCE(r.ReceivedQuantity, 0)) AS TotalReceiveQty,
                    COUNT_BIG(*) AS ReceiveRuns,
                    MIN(r.ReceivedDate) AS FirstReceivedDate,
                    MAX(r.ReceivedDate) AS LastReceivedDate
                INTO #ReceiptAggregate
                FROM #ReceiptAffectedRows AS r
                GROUP BY r.BN, r.ExpiryMonthKey, r.GenericItemNumber;

                CREATE UNIQUE CLUSTERED INDEX IX_ReceiptAggregate_Batch
                    ON #ReceiptAggregate (BN, ExpiryMonthKey, GenericItemNumber);

                SELECT
                    d.BN,
                    d.ExpiryMonthKey,
                    d.GenericItemNumber,
                    MAX(d.ExpiryDate) AS ExpiryDate,
                    MAX(NULLIF(d.TradeName, N'')) AS TradeName,
                    SUM(COALESCE(d.DispatchedQuantity, 0)) AS TotalDispatchedQty,
                    COUNT_BIG(*) AS DispatchRuns,
                    MIN(d.DispatchDate) AS FirstDispatchDate,
                    MAX(d.DispatchDate) AS LastDispatchDate,
                    MAX(NULLIF(d.Custody, N'')) AS Custody
                INTO #DispatchAggregate
                FROM #DispatchAffectedRows AS d
                GROUP BY d.BN, d.ExpiryMonthKey, d.GenericItemNumber;

                CREATE UNIQUE CLUSTERED INDEX IX_DispatchAggregate_Batch
                    ON #DispatchAggregate (BN, ExpiryMonthKey, GenericItemNumber);

                SELECT
                    BN,
                    ExpiryMonthKey,
                    GenericItemNumber,
                    LEFT(
                        STRING_AGG(CONVERT(nvarchar(max), TradeItemNumber), N' | ')
                            WITHIN GROUP (ORDER BY SourcePriority, TradeItemNumber),
                        255
                    ) AS TradeItemNumber
                INTO #BatchTradeCodes
                FROM
                (
                    SELECT
                        BN,
                        ExpiryMonthKey,
                        GenericItemNumber,
                        TradeItemNumber,
                        MIN(SourcePriority) AS SourcePriority
                    FROM
                    (
                        SELECT
                            BN, ExpiryMonthKey, GenericItemNumber,
                            TradeItemNumber, 0 AS SourcePriority
                        FROM #ReceiptAffectedRows
                        WHERE NULLIF(LTRIM(RTRIM(TradeItemNumber)), N'') IS NOT NULL

                        UNION ALL

                        SELECT
                            BN, ExpiryMonthKey, GenericItemNumber,
                            TradeItemNumber, 1 AS SourcePriority
                        FROM #DispatchAffectedRows
                        WHERE NULLIF(LTRIM(RTRIM(TradeItemNumber)), N'') IS NOT NULL
                    ) AS all_codes
                    GROUP BY BN, ExpiryMonthKey, GenericItemNumber, TradeItemNumber
                ) AS distinct_codes
                GROUP BY BN, ExpiryMonthKey, GenericItemNumber;

                CREATE UNIQUE CLUSTERED INDEX IX_BatchTradeCodes_Batch
                    ON #BatchTradeCodes (BN, ExpiryMonthKey, GenericItemNumber);
            """)
            timings["aggregate_movement_events"] = round(time.perf_counter() - aggregate_started, 3)

            batch_update_started = time.perf_counter()
            cursor.execute(r"""
                UPDATE bm
                SET
                    bm.ExpiryDate = COALESCE(sf.ExpiryDate, ra.ExpiryDate, da.ExpiryDate, bm.ExpiryDate),
                    bm.TradeItemNumber = COALESCE(NULLIF(btc.TradeItemNumber, N''), bm.TradeItemNumber),
                    bm.TradeName = COALESCE(NULLIF(ra.TradeName, N''), NULLIF(da.TradeName, N''), bm.TradeName),
                    bm.Description = COALESCE(NULLIF(ra.Description, N''), bm.Description),
                    bm.ItemFamilyGroup = COALESCE(NULLIF(ra.ItemFamilyGroup, N''), bm.ItemFamilyGroup),
                    bm.SupplierName = COALESCE(NULLIF(ra.SupplierName, N''), bm.SupplierName),
                    bm.SupplierCode = COALESCE(NULLIF(ra.SupplierCode, N''), bm.SupplierCode),
                    bm.TotalReceiveQty = COALESCE(ra.TotalReceiveQty, 0),
                    bm.ReceiveRuns = COALESCE(ra.ReceiveRuns, 0),
                    bm.FirstReceivedDate = ra.FirstReceivedDate,
                    bm.LastReceivedDate = ra.LastReceivedDate,
                    bm.TotalDispatchedQty = COALESCE(da.TotalDispatchedQty, 0),
                    bm.DispatchRuns = COALESCE(da.DispatchRuns, 0),
                    bm.FirstDispatchDate = da.FirstDispatchDate,
                    bm.LastDispatchDate = da.LastDispatchDate,
                    bm.Custody = COALESCE(NULLIF(da.Custody, N''), bm.Custody),
                    bm.GTIN = COALESCE(NULLIF(sf.GTIN, N''), bm.GTIN),
                    bm.DrugName = COALESCE(NULLIF(sf.DrugName, N''), bm.DrugName),
                    bm.PackageSize = CASE
                        WHEN COALESCE(sf.PackageSize, 0) > 0 THEN sf.PackageSize
                        WHEN COALESCE(bm.PackageSize, 0) > 0 THEN bm.PackageSize
                        ELSE 1
                    END,
                    bm.SFDAQuantity = COALESCE(sf.Quantity, bm.SFDAQuantity),
                    bm.Active = COALESCE(sf.Active, bm.Active),
                    bm.QuantitySentPending = COALESCE(sf.QuantitySentPending, bm.QuantitySentPending),
                    bm.QuantityReceivePending = COALESCE(sf.QuantityReceivePending, bm.QuantityReceivePending),
                    bm.GenericExistsInSFDA = CASE WHEN sf.BN IS NOT NULL THEN N'Yes' ELSE bm.GenericExistsInSFDA END,
                    bm.LastUpdated = SYSUTCDATETIME()
                FROM dbo.BatchMaster AS bm
                INNER JOIN #AffectedMovementKeys AS a
                    ON a.BN = bm.BN
                   AND a.ExpiryMonthKey = bm.ExpiryMonthKey
                   AND a.GenericItemNumber = bm.GenericItemNumber
                LEFT JOIN #ReceiptAggregate AS ra
                    ON ra.BN = bm.BN
                   AND ra.ExpiryMonthKey = bm.ExpiryMonthKey
                   AND ra.GenericItemNumber = bm.GenericItemNumber
                LEFT JOIN #DispatchAggregate AS da
                    ON da.BN = bm.BN
                   AND da.ExpiryMonthKey = bm.ExpiryMonthKey
                   AND da.GenericItemNumber = bm.GenericItemNumber
                LEFT JOIN #BatchTradeCodes AS btc
                    ON btc.BN = bm.BN
                   AND btc.ExpiryMonthKey = bm.ExpiryMonthKey
                   AND btc.GenericItemNumber = bm.GenericItemNumber
                OUTER APPLY
                (
                    SELECT TOP (1)
                        s.BN, s.ExpiryDate, s.GTIN, s.DrugName, s.PackageSize,
                        s.Quantity, s.Active, s.QuantitySentPending, s.QuantityReceivePending
                    FROM #CurrentAcceptSFDA AS s
                    WHERE s.BN = bm.BN
                      AND s.ExpiryMonthKey = bm.ExpiryMonthKey
                      AND s.ResolvedGenericItemNumber = bm.GenericItemNumber
                    ORDER BY s.GTIN
                ) AS sf
                WHERE bm.WarehouseID = ?
                  AND bm.BuildID = ?;
            """, warehouse_id, build_id)
            batch_updated = max(0, int(cursor.rowcount or 0))
            timings["batch_master_update"] = round(time.perf_counter() - batch_update_started, 3)

            batch_insert_started = time.perf_counter()
            cursor.execute(r"""
                ;WITH AffectedGenerics AS
                (
                    -- Stage 2 (Missing Batch in SFDA) remains receipt-driven.
                    SELECT DISTINCT GenericItemNumber
                    FROM #AffectedAcceptKeys
                ),
                ExactIdentityRows AS
                (
                    -- Previously proven exact identities already in BatchMaster.
                    SELECT
                        bm.GenericItemNumber,
                        bm.GTIN,
                        bm.DrugName,
                        bm.PackageSize,
                        bm.TradeItemNumber,
                        bm.TradeName,
                        bm.LastUpdated
                    FROM dbo.BatchMaster AS bm
                    INNER JOIN AffectedGenerics AS ag
                        ON ag.GenericItemNumber = bm.GenericItemNumber
                    WHERE bm.WarehouseID = ?
                      AND bm.BuildID = ?
                      AND NULLIF(bm.GTIN, N'') IS NOT NULL
                      AND UPPER(LTRIM(RTRIM(ISNULL(bm.GenericExistsInSFDA, N'')))) = N'YES'

                    UNION ALL

                    -- Exact identities discovered in THIS Append, including a
                    -- dispatch-only exact SFDA batch. This allows that exact row
                    -- to establish the Generic immediately for receipt Stage 2.
                    SELECT
                        s.ResolvedGenericItemNumber,
                        s.GTIN,
                        s.DrugName,
                        s.PackageSize,
                        CAST(NULL AS nvarchar(255)) AS TradeItemNumber,
                        CAST(NULL AS nvarchar(500)) AS TradeName,
                        SYSUTCDATETIME() AS LastUpdated
                    FROM #CurrentAcceptSFDA AS s
                    INNER JOIN AffectedGenerics AS ag
                        ON ag.GenericItemNumber = s.ResolvedGenericItemNumber
                    WHERE NULLIF(s.GTIN, N'') IS NOT NULL
                ),
                GenericReference AS
                (
                    SELECT GenericItemNumber, GTIN, DrugName, PackageSize, TradeItemNumber, TradeName
                    FROM
                    (
                        SELECT
                            e.GenericItemNumber,
                            e.GTIN,
                            e.DrugName,
                            e.PackageSize,
                            e.TradeItemNumber,
                            e.TradeName,
                            ROW_NUMBER() OVER
                            (
                                PARTITION BY e.GenericItemNumber
                                ORDER BY e.LastUpdated DESC, e.GTIN
                            ) AS rn
                        FROM ExactIdentityRows AS e
                    ) AS x
                    WHERE rn = 1
                )
                INSERT INTO dbo.BatchMaster
                (
                    BN, ExpiryMonthKey, ExpiryDate, GenericItemNumber,
                    TradeItemNumber, TradeName, GTIN, DrugName, PackageSize,
                    SFDAQuantity, Active, QuantitySentPending, QuantityReceivePending,
                    Description, ItemFamilyGroup, SupplierName, SupplierCode,
                    TotalReceiveQty, TotalDispatchedQty, ReceiveRuns, DispatchRuns,
                    FirstReceivedDate, LastReceivedDate, FirstDispatchDate, LastDispatchDate,
                    Custody, GenericExistsInSFDA, LastUpdated
                )
                SELECT
                    a.BN,
                    a.ExpiryMonthKey,
                    COALESCE(sf.ExpiryDate, ra.ExpiryDate, da.ExpiryDate),
                    a.GenericItemNumber,
                    COALESCE(NULLIF(btc.TradeItemNumber, N''), gr.TradeItemNumber, N''),
                    COALESCE(NULLIF(ra.TradeName, N''), NULLIF(da.TradeName, N''), gr.TradeName, N''),
                    COALESCE(NULLIF(sf.GTIN, N''), gr.GTIN, N''),
                    COALESCE(NULLIF(sf.DrugName, N''), gr.DrugName, N''),
                    CASE
                        WHEN COALESCE(sf.PackageSize, 0) > 0 THEN sf.PackageSize
                        WHEN COALESCE(gr.PackageSize, 0) > 0 THEN gr.PackageSize
                        ELSE 1
                    END,
                    COALESCE(sf.Quantity, 0),
                    COALESCE(sf.Active, 0),
                    COALESCE(sf.QuantitySentPending, 0),
                    COALESCE(sf.QuantityReceivePending, 0),
                    COALESCE(ra.Description, N''),
                    COALESCE(ra.ItemFamilyGroup, N''),
                    COALESCE(ra.SupplierName, N''),
                    COALESCE(ra.SupplierCode, N''),
                    COALESCE(ra.TotalReceiveQty, 0),
                    COALESCE(da.TotalDispatchedQty, 0),
                    COALESCE(ra.ReceiveRuns, 0),
                    COALESCE(da.DispatchRuns, 0),
                    ra.FirstReceivedDate,
                    ra.LastReceivedDate,
                    da.FirstDispatchDate,
                    da.LastDispatchDate,
                    COALESCE(NULLIF(da.Custody, N''), N''),
                    CASE WHEN sf.BN IS NOT NULL THEN N'Yes' ELSE N'Missing Batch in SFDA' END,
                    SYSUTCDATETIME()
                FROM #AffectedMovementKeys AS a
                LEFT JOIN #ReceiptAggregate AS ra
                    ON ra.BN = a.BN
                   AND ra.ExpiryMonthKey = a.ExpiryMonthKey
                   AND ra.GenericItemNumber = a.GenericItemNumber
                LEFT JOIN #DispatchAggregate AS da
                    ON da.BN = a.BN
                   AND da.ExpiryMonthKey = a.ExpiryMonthKey
                   AND da.GenericItemNumber = a.GenericItemNumber
                LEFT JOIN #BatchTradeCodes AS btc
                    ON btc.BN = a.BN
                   AND btc.ExpiryMonthKey = a.ExpiryMonthKey
                   AND btc.GenericItemNumber = a.GenericItemNumber
                LEFT JOIN GenericReference AS gr
                    ON gr.GenericItemNumber = a.GenericItemNumber
                OUTER APPLY
                (
                    SELECT TOP (1)
                        s.BN, s.ExpiryDate, s.GTIN, s.DrugName, s.PackageSize,
                        s.Quantity, s.Active, s.QuantitySentPending, s.QuantityReceivePending
                    FROM #CurrentAcceptSFDA AS s
                    WHERE s.BN = a.BN
                      AND s.ExpiryMonthKey = a.ExpiryMonthKey
                      AND s.ResolvedGenericItemNumber = a.GenericItemNumber
                    ORDER BY s.GTIN
                ) AS sf
                WHERE
                    (
                        -- Stage 1: exact SFDA batch may be Receipt, Dispatch, or both.
                        sf.BN IS NOT NULL
                        OR
                        -- Stage 2: keep the existing receipt-only discovery rule.
                        (ra.BN IS NOT NULL AND gr.GenericItemNumber IS NOT NULL)
                    )
                  AND NOT EXISTS
                  (
                      SELECT 1
                      FROM dbo.BatchMaster AS existing
                      WHERE existing.WarehouseID = ?
                        AND existing.BuildID = ?
                        AND existing.BN = a.BN
                        AND existing.ExpiryMonthKey = a.ExpiryMonthKey
                        AND existing.GenericItemNumber = a.GenericItemNumber
                  );
            """, warehouse_id, build_id, warehouse_id, build_id)
            batch_inserted = max(0, int(cursor.rowcount or 0))
            timings["batch_master_insert"] = round(time.perf_counter() - batch_insert_started, 3)

            supplier_started = time.perf_counter()
            cursor.execute(r"""
                DELETE sh
                FROM dbo.SupplierHistory AS sh
                INNER JOIN #AffectedAcceptKeys AS a
                    ON a.BN = sh.BN
                   AND a.ExpiryMonthKey = sh.ExpiryMonthKey
                   AND a.GenericItemNumber = sh.GenericItemNumber
                WHERE sh.WarehouseID = ?
                  AND sh.BuildID = ?;
            """, warehouse_id, build_id)

            cursor.execute(r"""
                INSERT INTO dbo.SupplierHistory
                (
                    SupplierName, SupplierCode, GTIN, DrugName, GenericItemNumber,
                    Description, TradeDescription, BN, ExpiryMonthKey, ExpiryDate,
                    PackageSize, ReceivedQuantityEach, ReceivedQuantityPack,
                    FirstReceivedDate, LastReceivedDate, ItemFamilyGroup,
                    TradeItemNumber, LastUpdated
                )
                SELECT
                    r.SupplierName,
                    r.SupplierCode,
                    bm.GTIN,
                    bm.DrugName,
                    r.GenericItemNumber,
                    COALESCE(NULLIF(MAX(r.Description), N''), bm.Description, N''),
                    COALESCE(NULLIF(MAX(r.TradeName), N''), bm.TradeName, N''),
                    r.BN,
                    r.ExpiryMonthKey,
                    COALESCE(bm.ExpiryDate, MAX(r.ExpiryDate)),
                    CASE WHEN COALESCE(bm.PackageSize, 0) > 0 THEN bm.PackageSize ELSE 1 END,
                    SUM(COALESCE(r.ReceivedQuantity, 0)),
                    SUM(COALESCE(r.ReceivedQuantity, 0)) /
                        CASE WHEN COALESCE(bm.PackageSize, 0) > 0 THEN bm.PackageSize ELSE 1 END,
                    MIN(r.ReceivedDate),
                    MAX(r.ReceivedDate),
                    COALESCE(NULLIF(MAX(r.ItemFamilyGroup), N''), bm.ItemFamilyGroup, N''),
                    COALESCE(NULLIF(r.TradeItemNumber, N''), N''),
                    SYSUTCDATETIME()
                FROM #ReceiptAffectedRows AS r
                INNER JOIN #AffectedAcceptKeys AS a
                    ON a.BN = r.BN
                   AND a.ExpiryMonthKey = r.ExpiryMonthKey
                   AND a.GenericItemNumber = r.GenericItemNumber
                INNER JOIN dbo.BatchMaster AS bm
                    ON bm.WarehouseID = ?
                   AND bm.BuildID = ?
                   AND bm.BN = r.BN
                   AND bm.ExpiryMonthKey = r.ExpiryMonthKey
                   AND bm.GenericItemNumber = r.GenericItemNumber
                WHERE UPPER(LTRIM(RTRIM(ISNULL(r.InboundShipment, N'')))) LIKE N'TRK5060%'
                  AND REPLACE(REPLACE(REPLACE(
                        UPPER(LTRIM(RTRIM(ISNULL(r.ItemFamilyGroup, N'')))),
                        N' ', N''), N'-', N''), N'_', N'') <> N'LABORATORYSUPPLIES'
                GROUP BY
                    r.SupplierName, r.SupplierCode, r.BN, r.ExpiryMonthKey,
                    r.GenericItemNumber, bm.GTIN, bm.DrugName, bm.Description,
                    bm.TradeName, bm.ExpiryDate, bm.PackageSize,
                    bm.ItemFamilyGroup, bm.TradeItemNumber, r.TradeItemNumber;
            """, warehouse_id, build_id)
            supplier_rebuilt = max(0, int(cursor.rowcount or 0))
            timings["supplier_history_refresh"] = round(time.perf_counter() - supplier_started, 3)

            customer_started = time.perf_counter()
            cursor.execute(r"""
                DELETE ch
                FROM dbo.CustomerHistory AS ch
                INNER JOIN #AffectedDispatchKeys AS a
                    ON a.BN = ch.BN
                   AND a.ExpiryMonthKey = ch.ExpiryMonthKey
                   AND a.GenericItemNumber = ch.GenericItemNumber
                WHERE ch.WarehouseID = ?
                  AND ch.BuildID = ?;
            """, warehouse_id, build_id)

            cursor.execute(r"""
                INSERT INTO dbo.CustomerHistory
                (
                    ToAddress, GLN, GTIN, DrugName, GenericItemNumber,
                    TradeDescription, BN, ExpiryMonthKey, ExpiryDate,
                    PackageSize, DispatchQuantityEach, DispatchQuantityPack,
                    FirstDispatchDate, LastDispatchDate, Custody, TradeItemNumber,
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
                    COALESCE(bm.ExpiryDate, MAX(d.ExpiryDate)),
                    CASE WHEN COALESCE(bm.PackageSize, 0) > 0 THEN bm.PackageSize ELSE 1 END,
                    SUM(COALESCE(d.DispatchedQuantity, 0)),
                    SUM(COALESCE(d.DispatchedQuantity, 0)) /
                        CASE WHEN COALESCE(bm.PackageSize, 0) > 0 THEN bm.PackageSize ELSE 1 END,
                    MIN(d.DispatchDate),
                    MAX(d.DispatchDate),
                    COALESCE(NULLIF(MAX(d.Custody), N''), NULLIF(MAX(bm.Custody), N''), N''),
                    COALESCE(NULLIF(d.TradeItemNumber, N''), N''),
                    SYSUTCDATETIME()
                FROM #DispatchAffectedRows AS d
                INNER JOIN #AffectedDispatchKeys AS a
                    ON a.BN = d.BN
                   AND a.ExpiryMonthKey = d.ExpiryMonthKey
                   AND a.GenericItemNumber = d.GenericItemNumber
                INNER JOIN dbo.BatchMaster AS bm
                    ON bm.WarehouseID = ?
                   AND bm.BuildID = ?
                   AND bm.BN = d.BN
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
                    bm.ExpiryDate,
                    bm.PackageSize,
                    bm.TradeItemNumber,
                    d.TradeItemNumber;
            """, warehouse_id, build_id)
            customer_rebuilt = max(0, int(cursor.rowcount or 0))
            timings["customer_history_refresh"] = round(time.perf_counter() - customer_started, 3)

            commit_started = time.perf_counter()
            connection.commit()
            timings["commit"] = round(time.perf_counter() - commit_started, 3)
        except Exception:
            connection.rollback()
            raise

    sql_seconds = time.perf_counter() - sql_started_at
    timings["sql_total"] = round(sql_seconds, 3)
    total_seconds = time.perf_counter() - started_at
    timings["total"] = round(total_seconds, 3)

    logger.info(
        "Unified Historical Append refresh completed in %.2f seconds. WarehouseID=%s BuildID=%s "
        "movement_keys=%s accept_keys=%s dispatch_keys=%s batch_updated=%s batch_inserted=%s "
        "supplier_rows=%s customer_rows=%s timings=%s",
        total_seconds,
        warehouse_id,
        build_id,
        len(movement_keys),
        len(accept_affected),
        len(dispatch_keys),
        batch_updated,
        batch_inserted,
        supplier_rebuilt,
        customer_rebuilt,
        timings,
    )

    return {
        "affected_batch_keys": int(len(movement_keys)),
        "accept_affected_batch_keys": int(len(accept_affected)),
        "dispatch_affected_batch_keys": int(len(dispatch_keys)),
        "batch_master_rows_updated": int(batch_updated),
        "batch_master_rows_inserted": int(batch_inserted),
        "supplier_history_rows_rebuilt": int(supplier_rebuilt),
        "customer_history_rows_rebuilt": int(customer_rebuilt),
        "scope_prepare_seconds": float(scope_metrics.get("seconds", 0) or 0),
        "sql_refresh_seconds": float(sql_seconds),
        "match_diagnostics": scope_metrics,
        "timings_seconds": timings,
    }

def refresh_accept_history_incremental(
    receipt_history_rows: List[Dict[str, Any]],
    sfda_df: pd.DataFrame,
    *,
    include_counts: bool = True,
) -> Dict[str, Any]:
    """Refresh only historical rows affected by one successful Daily Accept.

    Daily Accept must not rebuild the complete historical database. ReceiptEvents
    remain the durable source of truth; only BN + expiry-month + Generic keys
    represented by the uploaded ASN are recalculated here.

    Batch Master includes physical receipt classes TRK5060, TRK800 and TRK49.
    SupplierHistory remains TRK5060-only. CustomerHistory is intentionally
    untouched because an Accept run cannot change customer dispatch history.
    """
    initialize_database()
    started_at = time.perf_counter()
    affected, sfda_rows, scope_metrics = _prepare_incremental_accept_refresh_scope(
        receipt_history_rows,
        sfda_df,
    )

    if not affected:
        counts = (0, 0, 0)
        if include_counts:
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
            "batch_master_rows_inserted": 0,
            "supplier_history_rows_rebuilt": 0,
            "batch_master_rows": int(counts[0] or 0),
            "supplier_history_rows": int(counts[1] or 0),
            "customer_history_rows": int(counts[2] or 0),
            "scope_prepare_seconds": float(scope_metrics.get("seconds", 0) or 0),
        }

    sql_started_at = time.perf_counter()
    with Database().connect() as connection:
        cursor = connection.cursor()
        try:
            cursor.execute(r"""
                CREATE TABLE #AffectedAcceptKeys
                (
                    BN nvarchar(255) NOT NULL,
                    ExpiryMonthKey nvarchar(20) NOT NULL,
                    GenericItemNumber nvarchar(255) NOT NULL,
                    PRIMARY KEY (BN, ExpiryMonthKey, GenericItemNumber)
                );
            """)
            cursor.fast_executemany = True
            cursor.executemany(
                "INSERT INTO #AffectedAcceptKeys (BN, ExpiryMonthKey, GenericItemNumber) VALUES (?, ?, ?);",
                affected,
            )

            cursor.execute(r"""
                SELECT
                    BN,
                    ExpiryMonthKey,
                    GenericItemNumber,
                    LEFT(
                        STRING_AGG(CONVERT(nvarchar(max), TradeItemNumber), N' | ')
                            WITHIN GROUP (ORDER BY SourcePriority, TradeItemNumber),
                        255
                    ) AS TradeItemNumber
                INTO #AffectedAcceptTradeCodes
                FROM
                (
                    SELECT
                        BN, ExpiryMonthKey, GenericItemNumber, TradeItemNumber,
                        MIN(SourcePriority) AS SourcePriority
                    FROM
                    (
                        SELECT r.BN, r.ExpiryMonthKey, r.GenericItemNumber,
                               NULLIF(LTRIM(RTRIM(r.TradeItemNumber)), N'') AS TradeItemNumber,
                               0 AS SourcePriority
                        FROM dbo.ReceiptEvents r
                        INNER JOIN #AffectedAcceptKeys a
                            ON a.BN = r.BN
                           AND a.ExpiryMonthKey = r.ExpiryMonthKey
                           AND a.GenericItemNumber = r.GenericItemNumber
                        UNION ALL
                        SELECT d.BN, d.ExpiryMonthKey, d.GenericItemNumber,
                               NULLIF(LTRIM(RTRIM(d.TradeItemNumber)), N''),
                               1
                        FROM dbo.DispatchEvents d
                        INNER JOIN #AffectedAcceptKeys a
                            ON a.BN = d.BN
                           AND a.ExpiryMonthKey = d.ExpiryMonthKey
                           AND a.GenericItemNumber = d.GenericItemNumber
                    ) source_codes
                    WHERE TradeItemNumber IS NOT NULL
                    GROUP BY BN, ExpiryMonthKey, GenericItemNumber, TradeItemNumber
                ) distinct_codes
                GROUP BY BN, ExpiryMonthKey, GenericItemNumber;

                CREATE UNIQUE CLUSTERED INDEX IX_AffectedAcceptTradeCodes_Batch
                    ON #AffectedAcceptTradeCodes (BN, ExpiryMonthKey, GenericItemNumber);
            """)

            cursor.execute(r"""
                CREATE TABLE #CurrentAcceptSFDA
                (
                    GTIN nvarchar(255) NULL,
                    DrugName nvarchar(500) NULL,
                    BN nvarchar(255) NOT NULL,
                    ExpiryMonthKey nvarchar(20) NOT NULL,
                    ResolvedGenericItemNumber nvarchar(255) NOT NULL,
                    PackageSize decimal(38, 6) NULL,
                    ExpiryDate date NULL,
                    Quantity decimal(38, 6) NULL,
                    Active decimal(38, 6) NULL,
                    QuantitySentPending decimal(38, 6) NULL,
                    QuantityReceivePending decimal(38, 6) NULL
                );
            """)
            if sfda_rows:
                cursor.executemany(
                    r"""
                    INSERT INTO #CurrentAcceptSFDA
                    (GTIN, DrugName, BN, ExpiryMonthKey, ResolvedGenericItemNumber, PackageSize, ExpiryDate, Quantity,
                     Active, QuantitySentPending, QuantityReceivePending)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
                    """,
                    sfda_rows,
                )
                cursor.execute(
                    "CREATE INDEX IX_CurrentAcceptSFDA_Batch ON #CurrentAcceptSFDA (BN, ExpiryMonthKey, ResolvedGenericItemNumber, GTIN);"
                )

            # Aggregate only the affected physical receipt keys.
            cursor.execute(r"""
                ;WITH ReceiptAggregate AS
                (
                    SELECT
                        r.BN,
                        r.ExpiryMonthKey,
                        r.GenericItemNumber,
                        MAX(r.ExpiryDate) AS ExpiryDate,
                        MAX(NULLIF(r.TradeName, N'')) AS TradeName,
                        MAX(NULLIF(r.Description, N'')) AS Description,
                        MAX(NULLIF(r.ItemFamilyGroup, N'')) AS ItemFamilyGroup,
                        MAX(NULLIF(r.SupplierName, N'')) AS SupplierName,
                        MAX(NULLIF(r.SupplierCode, N'')) AS SupplierCode,
                        SUM(COALESCE(r.ReceivedQuantity, 0)) AS TotalReceiveQty,
                        COUNT_BIG(*) AS ReceiveRuns,
                        MIN(r.ReceivedDate) AS FirstReceivedDate,
                        MAX(r.ReceivedDate) AS LastReceivedDate
                    FROM dbo.ReceiptEvents r
                    INNER JOIN #AffectedAcceptKeys a
                        ON a.BN = r.BN
                       AND a.ExpiryMonthKey = r.ExpiryMonthKey
                       AND a.GenericItemNumber = r.GenericItemNumber
                    WHERE
                        UPPER(LTRIM(RTRIM(ISNULL(r.InboundShipment, N'')))) LIKE N'TRK5060%'
                        OR UPPER(LTRIM(RTRIM(ISNULL(r.InboundShipment, N'')))) LIKE N'TRK800%'
                        OR UPPER(LTRIM(RTRIM(ISNULL(r.InboundShipment, N'')))) LIKE N'TRK49%'
                    GROUP BY r.BN, r.ExpiryMonthKey, r.GenericItemNumber
                )
                UPDATE bm
                SET
                    bm.ExpiryDate = COALESCE(sf.ExpiryDate, ra.ExpiryDate, bm.ExpiryDate),
                    bm.TradeItemNumber = COALESCE(NULLIF(tc.TradeItemNumber, N''), bm.TradeItemNumber),
                    bm.TradeName = COALESCE(NULLIF(ra.TradeName, N''), bm.TradeName),
                    bm.Description = COALESCE(NULLIF(ra.Description, N''), bm.Description),
                    bm.ItemFamilyGroup = COALESCE(NULLIF(ra.ItemFamilyGroup, N''), bm.ItemFamilyGroup),
                    bm.SupplierName = COALESCE(NULLIF(ra.SupplierName, N''), bm.SupplierName),
                    bm.SupplierCode = COALESCE(NULLIF(ra.SupplierCode, N''), bm.SupplierCode),
                    bm.TotalReceiveQty = COALESCE(ra.TotalReceiveQty, 0),
                    bm.ReceiveRuns = COALESCE(ra.ReceiveRuns, 0),
                    bm.FirstReceivedDate = ra.FirstReceivedDate,
                    bm.LastReceivedDate = ra.LastReceivedDate,
                    bm.GTIN = COALESCE(NULLIF(sf.GTIN, N''), bm.GTIN),
                    bm.DrugName = COALESCE(NULLIF(sf.DrugName, N''), bm.DrugName),
                    bm.PackageSize = CASE WHEN COALESCE(sf.PackageSize, 0) > 0 THEN sf.PackageSize ELSE bm.PackageSize END,
                    bm.SFDAQuantity = COALESCE(sf.Quantity, bm.SFDAQuantity),
                    bm.Active = COALESCE(sf.Active, bm.Active),
                    bm.QuantitySentPending = COALESCE(sf.QuantitySentPending, bm.QuantitySentPending),
                    bm.QuantityReceivePending = COALESCE(sf.QuantityReceivePending, bm.QuantityReceivePending),
                    bm.GenericExistsInSFDA = CASE WHEN sf.BN IS NOT NULL THEN N'Yes' ELSE bm.GenericExistsInSFDA END,
                    bm.LastUpdated = SYSUTCDATETIME()
                FROM dbo.BatchMaster bm
                INNER JOIN ReceiptAggregate ra
                    ON ra.BN = bm.BN
                   AND ra.ExpiryMonthKey = bm.ExpiryMonthKey
                   AND ra.GenericItemNumber = bm.GenericItemNumber
                LEFT JOIN #AffectedAcceptTradeCodes tc
                    ON tc.BN = bm.BN
                   AND tc.ExpiryMonthKey = bm.ExpiryMonthKey
                   AND tc.GenericItemNumber = bm.GenericItemNumber
                OUTER APPLY
                (
                    SELECT TOP (1)
                        s.BN, s.ExpiryDate, s.GTIN, s.DrugName, s.PackageSize, s.Quantity, s.Active,
                        s.QuantitySentPending, s.QuantityReceivePending
                    FROM #CurrentAcceptSFDA s
                    WHERE s.BN = bm.BN
                      AND s.ExpiryMonthKey = bm.ExpiryMonthKey
                      AND s.ResolvedGenericItemNumber = bm.GenericItemNumber
                    ORDER BY s.GTIN
                ) sf;
            """)
            batch_updated = max(0, int(cursor.rowcount or 0))

            # Insert newly seen batches only when their Generic already has a
            # proven identity in Batch Master. This preserves the GenericÃƒÂ¢Ã¢â‚¬ Ã¢â‚¬ÂGTIN
            # authority and prevents BN/expiry collisions from inventing a drug.
            cursor.execute(r"""
                ;WITH ReceiptAggregate AS
                (
                    SELECT
                        r.BN,
                        r.ExpiryMonthKey,
                        r.GenericItemNumber,
                        MAX(r.ExpiryDate) AS ExpiryDate,
                        MAX(NULLIF(r.TradeName, N'')) AS TradeName,
                        MAX(NULLIF(r.Description, N'')) AS Description,
                        MAX(NULLIF(r.ItemFamilyGroup, N'')) AS ItemFamilyGroup,
                        MAX(NULLIF(r.SupplierName, N'')) AS SupplierName,
                        MAX(NULLIF(r.SupplierCode, N'')) AS SupplierCode,
                        SUM(COALESCE(r.ReceivedQuantity, 0)) AS TotalReceiveQty,
                        COUNT_BIG(*) AS ReceiveRuns,
                        MIN(r.ReceivedDate) AS FirstReceivedDate,
                        MAX(r.ReceivedDate) AS LastReceivedDate
                    FROM dbo.ReceiptEvents r
                    INNER JOIN #AffectedAcceptKeys a
                        ON a.BN = r.BN
                       AND a.ExpiryMonthKey = r.ExpiryMonthKey
                       AND a.GenericItemNumber = r.GenericItemNumber
                    WHERE
                        UPPER(LTRIM(RTRIM(ISNULL(r.InboundShipment, N'')))) LIKE N'TRK5060%'
                        OR UPPER(LTRIM(RTRIM(ISNULL(r.InboundShipment, N'')))) LIKE N'TRK800%'
                        OR UPPER(LTRIM(RTRIM(ISNULL(r.InboundShipment, N'')))) LIKE N'TRK49%'
                    GROUP BY r.BN, r.ExpiryMonthKey, r.GenericItemNumber
                ),
                ExactIdentityRows AS
                (
                    SELECT
                        bm.GenericItemNumber,
                        bm.GTIN,
                        bm.DrugName,
                        bm.PackageSize,
                        bm.TradeItemNumber,
                        bm.TradeName,
                        bm.LastUpdated
                    FROM dbo.BatchMaster bm
                    WHERE NULLIF(bm.GTIN, N'') IS NOT NULL
                      AND UPPER(LTRIM(RTRIM(ISNULL(bm.GenericExistsInSFDA, N'')))) = N'YES'
                      AND EXISTS
                    (
                        SELECT 1
                        FROM #AffectedAcceptKeys a
                        WHERE a.GenericItemNumber = bm.GenericItemNumber
                    )
                ),
                GenericReference AS
                (
                    SELECT *
                    FROM
                    (
                        SELECT
                            e.GenericItemNumber,
                            e.GTIN,
                            e.DrugName,
                            e.PackageSize,
                            e.TradeItemNumber,
                            e.TradeName,
                            ROW_NUMBER() OVER
                            (
                                PARTITION BY e.GenericItemNumber
                                ORDER BY e.LastUpdated DESC, e.GTIN
                            ) AS rn
                        FROM ExactIdentityRows e
                    ) x
                    WHERE rn = 1
                ),
                DispatchAggregate AS
                (
                    SELECT
                        d.BN,
                        d.ExpiryMonthKey,
                        d.GenericItemNumber,
                        SUM(COALESCE(d.DispatchedQuantity, 0)) AS TotalDispatchedQty,
                        COUNT_BIG(*) AS DispatchRuns,
                        MIN(d.DispatchDate) AS FirstDispatchDate,
                        MAX(d.DispatchDate) AS LastDispatchDate
                    FROM dbo.DispatchEvents d
                    INNER JOIN #AffectedAcceptKeys a
                        ON a.BN = d.BN
                       AND a.ExpiryMonthKey = d.ExpiryMonthKey
                       AND a.GenericItemNumber = d.GenericItemNumber
                    GROUP BY d.BN, d.ExpiryMonthKey, d.GenericItemNumber
                )
                INSERT INTO dbo.BatchMaster
                (
                    BN, ExpiryMonthKey, ExpiryDate, GenericItemNumber,
                    TradeItemNumber, TradeName, GTIN, DrugName, PackageSize,
                    SFDAQuantity, Active, QuantitySentPending, QuantityReceivePending,
                    Description, ItemFamilyGroup, SupplierName, SupplierCode,
                    TotalReceiveQty, TotalDispatchedQty, ReceiveRuns, DispatchRuns,
                    FirstReceivedDate, LastReceivedDate, FirstDispatchDate, LastDispatchDate,
                    GenericExistsInSFDA, LastUpdated
                )
                SELECT
                    ra.BN,
                    ra.ExpiryMonthKey,
                    COALESCE(sf.ExpiryDate, ra.ExpiryDate),
                    ra.GenericItemNumber,
                    COALESCE(NULLIF(tc.TradeItemNumber, N''), gr.TradeItemNumber, N''),
                    COALESCE(NULLIF(ra.TradeName, N''), gr.TradeName, N''),
                    COALESCE(NULLIF(sf.GTIN, N''), gr.GTIN, N''),
                    COALESCE(NULLIF(sf.DrugName, N''), gr.DrugName, N''),
                    CASE WHEN COALESCE(sf.PackageSize, 0) > 0 THEN sf.PackageSize WHEN COALESCE(gr.PackageSize, 0) > 0 THEN gr.PackageSize ELSE 1 END,
                    COALESCE(sf.Quantity, 0),
                    COALESCE(sf.Active, 0),
                    COALESCE(sf.QuantitySentPending, 0),
                    COALESCE(sf.QuantityReceivePending, 0),
                    COALESCE(ra.Description, N''),
                    COALESCE(ra.ItemFamilyGroup, N''),
                    COALESCE(ra.SupplierName, N''),
                    COALESCE(ra.SupplierCode, N''),
                    COALESCE(ra.TotalReceiveQty, 0),
                    COALESCE(da.TotalDispatchedQty, 0),
                    COALESCE(ra.ReceiveRuns, 0),
                    COALESCE(da.DispatchRuns, 0),
                    ra.FirstReceivedDate,
                    ra.LastReceivedDate,
                    da.FirstDispatchDate,
                    da.LastDispatchDate,
                    CASE WHEN sf.BN IS NOT NULL THEN N'Yes' ELSE N'Missing Batch in SFDA' END,
                    SYSUTCDATETIME()
                FROM ReceiptAggregate ra
                LEFT JOIN #AffectedAcceptTradeCodes tc
                    ON tc.BN = ra.BN
                   AND tc.ExpiryMonthKey = ra.ExpiryMonthKey
                   AND tc.GenericItemNumber = ra.GenericItemNumber
                LEFT JOIN GenericReference gr
                    ON gr.GenericItemNumber = ra.GenericItemNumber
                LEFT JOIN DispatchAggregate da
                    ON da.BN = ra.BN
                   AND da.ExpiryMonthKey = ra.ExpiryMonthKey
                   AND da.GenericItemNumber = ra.GenericItemNumber
                OUTER APPLY
                (
                    SELECT TOP (1)
                        s.BN, s.ExpiryDate, s.GTIN, s.DrugName, s.PackageSize, s.Quantity, s.Active,
                        s.QuantitySentPending, s.QuantityReceivePending
                    FROM #CurrentAcceptSFDA s
                    WHERE s.BN = ra.BN
                      AND s.ExpiryMonthKey = ra.ExpiryMonthKey
                      AND s.ResolvedGenericItemNumber = ra.GenericItemNumber
                    ORDER BY s.GTIN
                ) sf
                WHERE (sf.BN IS NOT NULL OR gr.GenericItemNumber IS NOT NULL)
                  AND NOT EXISTS
                (
                    SELECT 1
                    FROM dbo.BatchMaster bm
                    WHERE bm.BN = ra.BN
                      AND bm.ExpiryMonthKey = ra.ExpiryMonthKey
                      AND bm.GenericItemNumber = ra.GenericItemNumber
                );
            """)
            batch_inserted = max(0, int(cursor.rowcount or 0))

            # Rebuild SupplierHistory only for the affected batch keys. It stays
            # strictly TRK5060-only, exactly matching the historical business rule.
            cursor.execute(r"""
                DELETE sh
                FROM dbo.SupplierHistory sh
                INNER JOIN #AffectedAcceptKeys a
                    ON a.BN = sh.BN
                   AND a.ExpiryMonthKey = sh.ExpiryMonthKey
                   AND a.GenericItemNumber = sh.GenericItemNumber;
            """)

            cursor.execute(r"""
                INSERT INTO dbo.SupplierHistory
                (
                    SupplierName, SupplierCode, GTIN, DrugName, GenericItemNumber,
                    Description, TradeDescription, BN, ExpiryMonthKey, ExpiryDate,
                    PackageSize, ReceivedQuantityEach, ReceivedQuantityPack,
                    FirstReceivedDate, LastReceivedDate, ItemFamilyGroup,
                    TradeItemNumber, LastUpdated
                )
                SELECT
                    r.SupplierName,
                    r.SupplierCode,
                    bm.GTIN,
                    bm.DrugName,
                    r.GenericItemNumber,
                    COALESCE(NULLIF(MAX(r.Description), N''), bm.Description, N''),
                    COALESCE(NULLIF(MAX(r.TradeName), N''), bm.TradeName, N''),
                    r.BN,
                    r.ExpiryMonthKey,
                    COALESCE(bm.ExpiryDate, MAX(r.ExpiryDate)),
                    COALESCE(bm.PackageSize, 0),
                    SUM(COALESCE(r.ReceivedQuantity, 0)),
                    CASE
                        WHEN COALESCE(bm.PackageSize, 0) > 0
                            THEN SUM(COALESCE(r.ReceivedQuantity, 0)) / bm.PackageSize
                        ELSE 0
                    END,
                    MIN(r.ReceivedDate),
                    MAX(r.ReceivedDate),
                    COALESCE(NULLIF(MAX(r.ItemFamilyGroup), N''), bm.ItemFamilyGroup, N''),
                    COALESCE(NULLIF(r.TradeItemNumber, N''), N''),
                    SYSUTCDATETIME()
                FROM dbo.ReceiptEvents r
                INNER JOIN #AffectedAcceptKeys a
                    ON a.BN = r.BN
                   AND a.ExpiryMonthKey = r.ExpiryMonthKey
                   AND a.GenericItemNumber = r.GenericItemNumber
                INNER JOIN dbo.BatchMaster bm
                    ON bm.BN = r.BN
                   AND bm.ExpiryMonthKey = r.ExpiryMonthKey
                   AND bm.GenericItemNumber = r.GenericItemNumber
                WHERE UPPER(LTRIM(RTRIM(ISNULL(r.InboundShipment, N'')))) LIKE N'TRK5060%'
                  AND REPLACE(REPLACE(REPLACE(
                        UPPER(LTRIM(RTRIM(ISNULL(r.ItemFamilyGroup, N'')))),
                        N' ', N''), N'-', N''), N'_', N'') <> N'LABORATORYSUPPLIES'
                GROUP BY
                    r.SupplierName, r.SupplierCode, r.BN, r.ExpiryMonthKey,
                    r.GenericItemNumber, r.TradeItemNumber, bm.GTIN, bm.DrugName, bm.Description,
                    bm.TradeName, bm.ExpiryDate, bm.PackageSize,
                    bm.ItemFamilyGroup, bm.TradeItemNumber;
            """)
            supplier_rebuilt = max(0, int(cursor.rowcount or 0))

            counts = (0, 0, 0)
            if include_counts:
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

    sql_seconds = time.perf_counter() - sql_started_at
    logger.info(
        "Incremental Daily Accept history refresh completed in %.2f seconds. "
        "scope_prepare_seconds=%.3f sql_seconds=%.3f affected_keys=%s "
        "batch_updated=%s batch_inserted=%s supplier_rows=%s",
        time.perf_counter() - started_at,
        float(scope_metrics.get("seconds", 0) or 0),
        sql_seconds,
        len(affected),
        batch_updated,
        batch_inserted,
        supplier_rebuilt,
    )
    return {
        "affected_batch_keys": len(affected),
        "batch_master_rows_updated": batch_updated,
        "batch_master_rows_inserted": batch_inserted,
        "supplier_history_rows_rebuilt": supplier_rebuilt,
        "batch_master_rows": int(counts[0] or 0),
        "supplier_history_rows": int(counts[1] or 0),
        "customer_history_rows": int(counts[2] or 0),
        "scope_prepare_seconds": float(scope_metrics.get("seconds", 0) or 0),
        "sql_refresh_seconds": round(sql_seconds, 3),
    }

def refresh_dispatch_history_incremental(
    confirmed_history_rows: List[Dict[str, Any]],
    *,
    include_counts: bool = True,
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
        counts = (0, 0, 0)
        if include_counts:
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

            cursor.execute(r"""
                SELECT
                    BN,
                    ExpiryMonthKey,
                    GenericItemNumber,
                    LEFT(
                        STRING_AGG(CONVERT(nvarchar(max), TradeItemNumber), N' | ')
                            WITHIN GROUP (ORDER BY SourcePriority, TradeItemNumber),
                        255
                    ) AS TradeItemNumber
                INTO #AffectedDispatchTradeCodes
                FROM
                (
                    SELECT
                        BN, ExpiryMonthKey, GenericItemNumber, TradeItemNumber,
                        MIN(SourcePriority) AS SourcePriority
                    FROM
                    (
                        SELECT r.BN, r.ExpiryMonthKey, r.GenericItemNumber,
                               NULLIF(LTRIM(RTRIM(r.TradeItemNumber)), N'') AS TradeItemNumber,
                               0 AS SourcePriority
                        FROM dbo.ReceiptEvents r
                        INNER JOIN #AffectedDispatchKeys a
                            ON a.BN = r.BN
                           AND a.ExpiryMonthKey = r.ExpiryMonthKey
                           AND a.GenericItemNumber = r.GenericItemNumber
                        UNION ALL
                        SELECT d.BN, d.ExpiryMonthKey, d.GenericItemNumber,
                               NULLIF(LTRIM(RTRIM(d.TradeItemNumber)), N''),
                               1
                        FROM dbo.DispatchEvents d
                        INNER JOIN #AffectedDispatchKeys a
                            ON a.BN = d.BN
                           AND a.ExpiryMonthKey = d.ExpiryMonthKey
                           AND a.GenericItemNumber = d.GenericItemNumber
                    ) source_codes
                    WHERE TradeItemNumber IS NOT NULL
                    GROUP BY BN, ExpiryMonthKey, GenericItemNumber, TradeItemNumber
                ) distinct_codes
                GROUP BY BN, ExpiryMonthKey, GenericItemNumber;

                CREATE UNIQUE CLUSTERED INDEX IX_AffectedDispatchTradeCodes_Batch
                    ON #AffectedDispatchTradeCodes (BN, ExpiryMonthKey, GenericItemNumber);
            """)

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
                        MAX(d.DispatchDate) AS LastDispatchDate,
                        MAX(NULLIF(d.Custody, N'')) AS Custody
                    FROM dbo.DispatchEvents d
                    INNER JOIN #AffectedDispatchKeys a
                        ON a.BN = d.BN
                       AND a.ExpiryMonthKey = d.ExpiryMonthKey
                       AND a.GenericItemNumber = d.GenericItemNumber
                    GROUP BY d.BN, d.ExpiryMonthKey, d.GenericItemNumber
                )
                UPDATE bm
                SET
                    bm.TradeItemNumber = COALESCE(NULLIF(tc.TradeItemNumber, N''), bm.TradeItemNumber),
                    bm.TotalDispatchedQty = COALESCE(a.TotalDispatchedQty, 0),
                    bm.DispatchRuns = COALESCE(a.DispatchRuns, 0),
                    bm.FirstDispatchDate = a.FirstDispatchDate,
                    bm.LastDispatchDate = a.LastDispatchDate,
                    bm.Custody = COALESCE(NULLIF(a.Custody, N''), bm.Custody),
                    bm.LastUpdated = SYSUTCDATETIME()
                FROM dbo.BatchMaster bm
                INNER JOIN DispatchAggregate a
                    ON a.BN = bm.BN
                   AND a.ExpiryMonthKey = bm.ExpiryMonthKey
                   AND a.GenericItemNumber = bm.GenericItemNumber
                LEFT JOIN #AffectedDispatchTradeCodes tc
                    ON tc.BN = bm.BN
                   AND tc.ExpiryMonthKey = bm.ExpiryMonthKey
                   AND tc.GenericItemNumber = bm.GenericItemNumber;
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
                    FirstDispatchDate, LastDispatchDate, Custody, TradeItemNumber,
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
                    COALESCE(NULLIF(MAX(d.Custody), N''), NULLIF(MAX(bm.Custody), N''), N''),
                    COALESCE(NULLIF(d.TradeItemNumber, N''), N''),
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
                    d.TradeItemNumber,
                    bm.GTIN,
                    bm.DrugName,
                    bm.TradeName,
                    bm.PackageSize,
                    bm.TradeItemNumber;
            """)
            customer_rebuilt = max(0, int(cursor.rowcount or 0))

            counts = (0, 0, 0)
            if include_counts:
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

def _full_dispatch_cutover_schema_available(connection: pyodbc.Connection) -> bool:
    """Return whether migration 004 is fully installed."""
    row = connection.cursor().execute(r"""
        SELECT CASE
            WHEN OBJECT_ID(N'dbo.FullDispatchCutovers', N'U') IS NOT NULL
             AND OBJECT_ID(N'dbo.FullDispatchCutoverBaseline', N'U') IS NOT NULL
             AND COL_LENGTH(N'dbo.FullDispatchTransactions', N'CutoverID') IS NOT NULL
            THEN 1 ELSE 0 END;
    """).fetchone()
    return bool(int(row[0] or 0))


def full_dispatch_cutover_schema_available() -> bool:
    """Public deployment guard for the Full Dispatch cutover extension."""
    initialize_database()
    with Database().connect() as connection:
        return _full_dispatch_cutover_schema_available(connection)

def _get_active_full_dispatch_cutover_with_connection(
    connection: pyodbc.Connection,
) -> Optional[Dict[str, Any]]:
    """Return the active cutover visible to the current warehouse session."""
    if not _full_dispatch_cutover_schema_available(connection):
        return None
    row = connection.cursor().execute(r"""
        SELECT TOP (1)
            CutoverID, Status, ActivatedAt, ActivatedBy,
            InventorySourceFile, SFDASourceFile,
            CustomerBaselineRows, CustomerBaselinePack,
            AlignmentRows, MaxDifferencePack
        FROM dbo.FullDispatchCutovers
        WHERE Status = N'Active'
        ORDER BY ActivatedAt DESC;
    """).fetchone()
    if row is None:
        return None
    return {
        "cutover_id": str(row[0]),
        "status": str(row[1] or ""),
        "activated_at": row[2],
        "activated_by": str(row[3] or ""),
        "inventory_source_file": str(row[4] or ""),
        "sfda_source_file": str(row[5] or ""),
        "customer_baseline_rows": int(row[6] or 0),
        "customer_baseline_pack": float(row[7] or 0),
        "alignment_rows": int(row[8] or 0),
        "max_difference_pack": float(row[9] or 0),
    }


def get_active_full_dispatch_cutover() -> Optional[Dict[str, Any]]:
    """Return active Full Dispatch cutover metadata for this warehouse."""
    initialize_database()
    with Database().connect() as connection:
        return _get_active_full_dispatch_cutover_with_connection(connection)


def get_full_dispatch_cutover_alignment() -> pd.DataFrame:
    """Compare latest SFDA Active against physical Inventory in pack units.

    Batch Master supplies the verified SFDA/WMS identity and stable PackageSize.
    The result contains only exact regulatory batches represented in both the
    active historical build and the latest SFDA snapshot.
    """
    initialize_database()
    sql = r"""
        WITH MasterIdentity AS
        (
            SELECT
                BN, ExpiryMonthKey, GenericItemNumber,
                MAX(NULLIF(GTIN, N'')) AS GTIN,
                MAX(CASE WHEN PackageSize > 0 THEN PackageSize ELSE 1 END) AS PackageSize
            FROM dbo.BatchMaster
            WHERE NULLIF(GTIN, N'') IS NOT NULL
            GROUP BY BN, ExpiryMonthKey, GenericItemNumber
        ),
        Inventory AS
        (
            SELECT BN, ExpiryMonthKey, GenericItemNumber,
                   SUM(AvailableQuantity) AS InventoryEach
            FROM dbo.LatestInventorySnapshot
            GROUP BY BN, ExpiryMonthKey, GenericItemNumber
        ),
        MappedInventory AS
        (
            SELECT
                m.GTIN,
                m.BN,
                m.ExpiryMonthKey,
                SUM(COALESCE(i.InventoryEach, 0)) AS InventoryEach,
                SUM(
                    COALESCE(i.InventoryEach, 0)
                    / NULLIF(m.PackageSize, 0)
                ) AS InventoryPack
            FROM MasterIdentity AS m
            LEFT JOIN Inventory AS i
                ON i.BN = m.BN
               AND i.ExpiryMonthKey = m.ExpiryMonthKey
               AND i.GenericItemNumber = m.GenericItemNumber
            GROUP BY m.GTIN, m.BN, m.ExpiryMonthKey
        ),
        SFDA AS
        (
            SELECT GTIN, BN, ExpiryMonthKey,
                   SUM(Active) AS SFDAActive
            FROM dbo.LatestSFDASnapshot
            GROUP BY GTIN, BN, ExpiryMonthKey
        )
        SELECT
            m.BN,
            m.ExpiryMonthKey AS [Expiry Month Key],
            N'' AS [Generic Item Number],
            m.GTIN,
            CAST(1 AS decimal(18, 4)) AS PackageSize,
            m.InventoryEach AS [Current Inventory Quantity Each],
            m.InventoryPack AS [Current Inventory Quantity Pack],
            s.SFDAActive AS [SFDA Active],
            s.SFDAActive - m.InventoryPack AS [Difference Pack]
        FROM MappedInventory AS m
        INNER JOIN SFDA AS s
            ON s.GTIN = m.GTIN
           AND s.BN = m.BN
           AND s.ExpiryMonthKey = m.ExpiryMonthKey
        ;
    """
    with Database().connect() as connection:
        return pd.read_sql(sql, connection)


def get_full_dispatch_cutover_status() -> Dict[str, Any]:
    """Return cutover status plus a lightweight latest-snapshot alignment."""
    initialize_database()
    schema_available = full_dispatch_cutover_schema_available()
    cutover = get_active_full_dispatch_cutover()
    alignment = get_full_dispatch_cutover_alignment()
    difference = pd.to_numeric(
        alignment.get("Difference Pack", pd.Series(dtype=float)),
        errors="coerce",
    ).fillna(0)
    minimum_actionable_pack = max(
        1.0,
        float(os.getenv("FULL_DISPATCH_CUTOVER_MIN_ACTIONABLE_PACK", "1") or 1),
    )
    blocking = difference.ge(minimum_actionable_pack)
    return {
        "schema_available": schema_available,
        "active": cutover is not None,
        "cutover": cutover,
        "alignment_rows": int(len(alignment)),
        "mismatch_rows": int(blocking.sum()),
        "max_difference_pack": max(
            0.0, float(difference.max()) if not difference.empty else 0.0
        ),
        "minimum_actionable_pack": float(minimum_actionable_pack),
        "ready_to_activate": bool(
            schema_available
            and cutover is None
            and not alignment.empty
            and not blocking.any()
        ),
    }


def activate_full_dispatch_cutover(activated_by: str = "") -> Dict[str, Any]:
    """Close current cumulative Customer History as the post-alignment baseline.

    Activation is rejected only when a verified SFDA batch exceeds physical
    Inventory by at least one complete, dispatchable pack.
    Baseline capture is set-based and transactional.
    """
    import uuid

    initialize_database()
    if not full_dispatch_cutover_schema_available():
        raise ValueError(
            "Full Dispatch cutover migration 004 is not installed. "
            "Run the database migration before activation."
        )
    alignment = get_full_dispatch_cutover_alignment()
    if alignment.empty:
        raise ValueError(
            "Cutover cannot be activated because latest Inventory/SFDA alignment is empty. "
            "Complete a Full Dispatch run with the final updated files first."
        )

    minimum_actionable_pack = max(
        1.0,
        float(os.getenv("FULL_DISPATCH_CUTOVER_MIN_ACTIONABLE_PACK", "1") or 1),
    )
    differences = pd.to_numeric(alignment["Difference Pack"], errors="coerce").fillna(0)
    mismatch_rows = int(differences.ge(minimum_actionable_pack).sum())
    max_difference = max(
        0.0, float(differences.max()) if not differences.empty else 0.0
    )
    if mismatch_rows:
        raise ValueError(
            "Cutover cannot be activated: "
            f"{mismatch_rows} batch(es) still have SFDA Active above Inventory "
            f"by at least {minimum_actionable_pack:g} pack. Maximum positive "
            f"difference: {max_difference:g} pack."
        )

    cutover_id = str(uuid.uuid4())
    with Database().connect() as connection:
        cursor = connection.cursor()
        try:
            existing = _get_active_full_dispatch_cutover_with_connection(connection)
            if existing is not None:
                raise ValueError(
                    "Full Dispatch cutover is already active for this warehouse."
                )

            inventory_source_row = cursor.execute(r"""
                SELECT TOP (1) SourceFileName
                FROM dbo.LatestInventorySnapshot
                ORDER BY SnapshotUtc DESC;
            """).fetchone()
            sfda_source_row = cursor.execute(r"""
                SELECT TOP (1) SourceFileName
                FROM dbo.LatestSFDASnapshot
                ORDER BY SnapshotUtc DESC;
            """).fetchone()
            inventory_source = str(inventory_source_row[0] or "") if inventory_source_row else ""
            sfda_source = str(sfda_source_row[0] or "") if sfda_source_row else ""

            cursor.execute(r"""
                INSERT INTO dbo.FullDispatchCutovers
                (
                    CutoverID, Status, ActivatedBy,
                    InventorySourceFile, SFDASourceFile,
                    AlignmentRows, MaxDifferencePack
                )
                VALUES (?, N'Active', ?, ?, ?, ?, ?);
            """, (
                cutover_id,
                str(activated_by or ""),
                inventory_source,
                sfda_source,
                int(len(alignment)),
                max_difference,
            ))

            cursor.execute(r"""
                INSERT INTO dbo.FullDispatchCutoverBaseline
                (
                    CutoverID, BN, ExpiryMonthKey, GenericItemNumber,
                    ToAddress, GLN, PackageSize,
                    ClosedQuantityEach, ClosedQuantityPack
                )
                SELECT
                    ?,
                    BN,
                    ExpiryMonthKey,
                    GenericItemNumber,
                    LTRIM(RTRIM(ISNULL(ToAddress, N''))),
                    CASE
                        WHEN NULLIF(LTRIM(RTRIM(ISNULL(GLN, N''))), N'') IS NULL
                          OR UPPER(LTRIM(RTRIM(ISNULL(GLN, N'')))) = N'DUMMY'
                        THEN N'99999999999999'
                        ELSE LTRIM(RTRIM(GLN))
                    END,
                    MAX(CASE WHEN PackageSize > 0 THEN PackageSize ELSE 1 END),
                    SUM(DispatchQuantityEach),
                    SUM(DispatchQuantityPack)
                FROM dbo.CustomerHistory
                GROUP BY
                    BN, ExpiryMonthKey, GenericItemNumber,
                    LTRIM(RTRIM(ISNULL(ToAddress, N''))),
                    CASE
                        WHEN NULLIF(LTRIM(RTRIM(ISNULL(GLN, N''))), N'') IS NULL
                          OR UPPER(LTRIM(RTRIM(ISNULL(GLN, N'')))) = N'DUMMY'
                        THEN N'99999999999999'
                        ELSE LTRIM(RTRIM(GLN))
                    END;
            """, cutover_id)

            baseline = cursor.execute(r"""
                SELECT COUNT_BIG(*), COALESCE(SUM(ClosedQuantityPack), 0)
                FROM dbo.FullDispatchCutoverBaseline
                WHERE CutoverID = ?;
            """, cutover_id).fetchone()
            baseline_rows = int(baseline[0] or 0)
            baseline_pack = float(baseline[1] or 0)

            cursor.execute(r"""
                UPDATE dbo.FullDispatchCutovers
                SET CustomerBaselineRows = ?,
                    CustomerBaselinePack = ?
                WHERE CutoverID = ?;
            """, baseline_rows, baseline_pack, cutover_id)
            connection.commit()
        except Exception:
            connection.rollback()
            raise

    return {
        "status": "Activated",
        "cutover_id": cutover_id,
        "customer_baseline_rows": baseline_rows,
        "customer_baseline_pack": baseline_pack,
        "alignment_rows": int(len(alignment)),
        "max_difference_pack": max_difference,
        "minimum_actionable_pack": minimum_actionable_pack,
    }


def get_full_dispatch_cutover_baseline() -> pd.DataFrame:
    """Return the active cutover's closed cumulative customer quantities."""
    initialize_database()
    columns = [
        "BN", "Expiry Month Key", "Generic Item Number", "To Address", "GLN",
        "PackageSize", "Cutover Closed Quantity Each",
        "Cutover Closed Quantity Pack",
    ]
    if not full_dispatch_cutover_schema_available():
        return pd.DataFrame(columns=columns)
    sql = r"""
        SELECT
            b.BN,
            b.ExpiryMonthKey AS [Expiry Month Key],
            b.GenericItemNumber AS [Generic Item Number],
            b.ToAddress AS [To Address],
            b.GLN,
            MAX(b.PackageSize) AS PackageSize,
            SUM(b.ClosedQuantityEach) AS [Cutover Closed Quantity Each],
            SUM(b.ClosedQuantityPack) AS [Cutover Closed Quantity Pack]
        FROM dbo.FullDispatchCutoverBaseline AS b
        INNER JOIN dbo.FullDispatchCutovers AS c
            ON c.WarehouseID = b.WarehouseID
           AND c.CutoverID = b.CutoverID
           AND c.Status = N'Active'
        GROUP BY
            b.BN, b.ExpiryMonthKey, b.GenericItemNumber, b.ToAddress, b.GLN;
    """
    with Database().connect() as connection:
        return pd.read_sql(sql, connection)

def _full_dispatch_transaction_key(
    bn: str,
    expiry_date: Any,
    generic_item_number: str,
    to_address: str,
    gln: str,
    cutover_id: str = "",
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
            str(cutover_id or "").strip().upper(),
        ]
    )
    return _warehouse_scoped_key(hashlib.sha256(raw.encode("utf-8")).hexdigest())


def _full_cancel_dispatch_transaction_key(
    bn: str,
    expiry_date: Any,
    generic_item_number: str,
    to_address: str,
    gln: str,
    return_type: str,
) -> str:
    """Return a stable key isolated from ordinary Full Dispatch rows."""
    import hashlib

    expiry = pd.to_datetime(expiry_date, errors="coerce")
    expiry_text = "" if pd.isna(expiry) else expiry.strftime("%Y-%m")
    raw = "|".join([
        "FULL-CANCEL-DISPATCH",
        str(return_type or "").strip().upper(),
        str(bn or "").strip().upper(),
        expiry_text,
        str(generic_item_number or "").strip(),
        str(to_address or "").strip().upper(),
        str(gln or "").strip(),
    ])
    return _warehouse_scoped_key(hashlib.sha256(raw.encode("utf-8")).hexdigest())


def _full_cancel_dispatch_schema_available(connection: pyodbc.Connection) -> bool:
    row = connection.cursor().execute(
        "SELECT CASE WHEN COL_LENGTH(N'dbo.FullDispatchTransactions', "
        "N'TransactionType') IS NULL THEN 0 ELSE 1 END;"
    ).fetchone()
    return bool(int(row[0] or 0))


def full_cancel_dispatch_schema_available() -> bool:
    """Public deployment guard for the optional cancel-dispatch extension."""
    initialize_database()
    with Database().connect() as connection:
        return _full_cancel_dispatch_schema_available(connection)


def get_full_dispatch_confirmed_allocations() -> pd.DataFrame:
    """Return only SFDA-confirmed Full Dispatch historical consumption.

    A generated CSV is not proof that the user uploaded it. Submitted quantities
    remain pending for audit and confirmation, but only quantities proven by a
    later SFDA report are deducted from Customer History. Therefore a retry with
    unchanged SFDA correctly regenerates the same operational file.
    """
    initialize_database()
    pre_cutover_sql = r"""
        SELECT
            BN,
            ExpiryDate AS [Expiry Date],
            GenericItemNumber AS [Generic Item Number],
            ToAddress AS [To Address],
            GLN,
            ConfirmedQuantityEach AS [Reserved Full Dispatch Quantity Each],
            ConfirmedQuantityPack AS [Reserved Full Dispatch Quantity Pack],
            ConfirmedQuantityEach AS [Confirmed Full Dispatch Quantity Each],
            ConfirmedQuantityPack AS [Confirmed Full Dispatch Quantity Pack],
            LastConfirmedAt AS [Last Full Dispatch Confirmed At]
        FROM dbo.FullDispatchTransactions
        WHERE (ConfirmedQuantityPack > 0
           OR ConfirmedQuantityEach > 0)
    """
    with Database().connect() as connection:
        cutover = _get_active_full_dispatch_cutover_with_connection(connection)
        if cutover is not None:
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
                WHERE CutoverID = ?
                  AND (SubmittedQuantityPack > 0 OR SubmittedQuantityEach > 0)
            """
            if _full_cancel_dispatch_schema_available(connection):
                sql += " AND ISNULL(TransactionType, 'DISPATCH') = 'DISPATCH'"
            sql += ";"
            return pd.read_sql(
                sql,
                connection,
                params=[cutover["cutover_id"]],
            )

        if _full_cancel_dispatch_schema_available(connection):
            sql = (
                pre_cutover_sql
                + " AND ISNULL(TransactionType, 'DISPATCH') = 'DISPATCH';"
            )
        else:
            sql = pre_cutover_sql + ";"
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
    with Database().connect() as cutover_connection:
        active_cutover = _get_active_full_dispatch_cutover_with_connection(
            cutover_connection
        )
    cutover_id = (
        str(active_cutover.get("cutover_id") or "")
        if active_cutover is not None
        else ""
    )

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
        package_size = max(0.0, _package_size(row, "PackageSize"))
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
            cutover_id,
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
                "CutoverID": cutover_id or None,
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
                ? AS ToAddress, ? AS GLN, ? AS CutoverID,
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
                CutoverID = source.CutoverID,
                SubmittedQuantityEach = CASE
                    WHEN source.CutoverID IS NOT NULL
                         AND target.LastSubmittedRun = source.RunNumber
                    THEN target.SubmittedQuantityEach
                    WHEN source.CutoverID IS NOT NULL
                    THEN target.SubmittedQuantityEach + source.NewQuantityEach
                    WHEN target.SubmittedQuantityEach >=
                         target.ConfirmedQuantityEach + source.NewQuantityEach
                    THEN target.SubmittedQuantityEach
                    ELSE target.ConfirmedQuantityEach + source.NewQuantityEach
                END,
                SubmittedQuantityPack = CASE
                    WHEN source.CutoverID IS NOT NULL
                         AND target.LastSubmittedRun = source.RunNumber
                    THEN target.SubmittedQuantityPack
                    WHEN source.CutoverID IS NOT NULL
                    THEN target.SubmittedQuantityPack + source.NewQuantityPack
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
                GenericItemNumber, ToAddress, GLN, CutoverID,
                SubmittedQuantityEach, SubmittedQuantityPack,
                ConfirmedQuantityEach, ConfirmedQuantityPack,
                FirstSubmittedRun, LastSubmittedRun
            )
            VALUES
            (
                source.TransactionKey, source.BN, source.ExpiryDate,
                source.ExpiryMonthKey, source.GenericItemNumber,
                source.ToAddress, source.GLN, source.CutoverID,
                source.NewQuantityEach, source.NewQuantityPack,
                0, 0, source.RunNumber, source.RunNumber
            );
    """

    bulk_sql = r"""
        ;WITH source AS
        (
            SELECT
                CONVERT(varchar(64), JSON_VALUE(j.value, '$[0]')) AS TransactionKey,
                CONVERT(nvarchar(255), JSON_VALUE(j.value, '$[1]')) AS BN,
                TRY_CONVERT(date, JSON_VALUE(j.value, '$[2]')) AS ExpiryDate,
                CONVERT(nvarchar(7), JSON_VALUE(j.value, '$[3]')) AS ExpiryMonthKey,
                CONVERT(nvarchar(255), JSON_VALUE(j.value, '$[4]')) AS GenericItemNumber,
                CONVERT(nvarchar(1000), JSON_VALUE(j.value, '$[5]')) AS ToAddress,
                CONVERT(nvarchar(100), JSON_VALUE(j.value, '$[6]')) AS GLN,
                TRY_CONVERT(uniqueidentifier, NULLIF(JSON_VALUE(j.value, '$[7]'), '')) AS CutoverID,
                TRY_CONVERT(decimal(38,6), JSON_VALUE(j.value, '$[8]')) AS NewQuantityEach,
                TRY_CONVERT(decimal(38,6), JSON_VALUE(j.value, '$[9]')) AS NewQuantityPack,
                CONVERT(nvarchar(255), JSON_VALUE(j.value, '$[10]')) AS RunNumber
            FROM OPENJSON(CAST(? AS nvarchar(max))) AS j
        )
        MERGE dbo.FullDispatchTransactions WITH (HOLDLOCK) AS target
        USING source
        ON target.TransactionKey = source.TransactionKey
        WHEN MATCHED THEN
            UPDATE SET
                BN = source.BN,
                ExpiryDate = source.ExpiryDate,
                ExpiryMonthKey = source.ExpiryMonthKey,
                GenericItemNumber = source.GenericItemNumber,
                ToAddress = source.ToAddress,
                GLN = source.GLN,
                CutoverID = source.CutoverID,
                SubmittedQuantityEach = CASE
                    WHEN source.CutoverID IS NOT NULL
                         AND target.LastSubmittedRun = source.RunNumber
                    THEN target.SubmittedQuantityEach
                    WHEN source.CutoverID IS NOT NULL
                    THEN target.SubmittedQuantityEach + source.NewQuantityEach
                    WHEN target.SubmittedQuantityEach >=
                         target.ConfirmedQuantityEach + source.NewQuantityEach
                    THEN target.SubmittedQuantityEach
                    ELSE target.ConfirmedQuantityEach + source.NewQuantityEach
                END,
                SubmittedQuantityPack = CASE
                    WHEN source.CutoverID IS NOT NULL
                         AND target.LastSubmittedRun = source.RunNumber
                    THEN target.SubmittedQuantityPack
                    WHEN source.CutoverID IS NOT NULL
                    THEN target.SubmittedQuantityPack + source.NewQuantityPack
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
                GenericItemNumber, ToAddress, GLN, CutoverID,
                SubmittedQuantityEach, SubmittedQuantityPack,
                ConfirmedQuantityEach, ConfirmedQuantityPack,
                FirstSubmittedRun, LastSubmittedRun
            )
            VALUES
            (
                source.TransactionKey, source.BN, source.ExpiryDate,
                source.ExpiryMonthKey, source.GenericItemNumber,
                source.ToAddress, source.GLN, source.CutoverID,
                source.NewQuantityEach, source.NewQuantityPack,
                0, 0, source.RunNumber, source.RunNumber
            );
    """

    saved = len(prepared)
    with Database().connect() as connection:
        cursor = connection.cursor()
        try:
            json_batch_size = max(
                100,
                int(os.getenv("FULL_DISPATCH_JSON_BATCH_SIZE", "4000") or 4000),
            )
            bulk_rows = [
                [
                    row["TransactionKey"], row["BN"], str(row["ExpiryDate"]),
                    row["ExpiryMonthKey"], row["GenericItemNumber"],
                    row["ToAddress"], row["GLN"], row["CutoverID"],
                    row["Each"], row["Pack"],
                    str(run_number),
                ]
                for row in prepared.values()
            ]
            for row_batch in _chunks(bulk_rows, json_batch_size):
                cursor.execute(
                    bulk_sql,
                    json.dumps(
                        row_batch,
                        ensure_ascii=False,
                        separators=(",", ":"),
                    ),
                )
            connection.commit()
        except Exception as bulk_exc:
            connection.rollback()
            logger.warning(
                "OPENJSON Full Dispatch ledger save failed; using row fallback: %s",
                bulk_exc,
            )
            cursor = connection.cursor()
            try:
                for row in prepared.values():
                    cursor.execute(
                        sql,
                        (
                            row["TransactionKey"], row["BN"], row["ExpiryDate"],
                            row["ExpiryMonthKey"], row["GenericItemNumber"],
                            row["ToAddress"], row["GLN"], row["CutoverID"],
                            row["Each"], row["Pack"],
                            str(run_number),
                        ),
                    )
                connection.commit()
            except Exception:
                connection.rollback()
                raise

    return saved


def get_full_cancel_dispatch_confirmed_allocations() -> pd.DataFrame:
    """Return confirmed cancel quantities, or an empty frame pre-migration."""
    columns = [
        "Return Type", "BN", "Expiry Date", "Expiry Month Key",
        "Generic Item Number", "To Address", "GLN",
        "Previously Confirmed Cancel Dispatch Each",
        "Previously Confirmed Cancel Dispatch Pack",
    ]
    initialize_database()
    with Database().connect() as connection:
        if not _full_cancel_dispatch_schema_available(connection):
            return pd.DataFrame(columns=columns)
        return pd.read_sql(r"""
            SELECT
                ISNULL(SourceType, '') AS [Return Type],
                BN,
                ExpiryDate AS [Expiry Date],
                ExpiryMonthKey AS [Expiry Month Key],
                GenericItemNumber AS [Generic Item Number],
                ToAddress AS [To Address],
                GLN,
                ConfirmedQuantityEach AS [Previously Confirmed Cancel Dispatch Each],
                ConfirmedQuantityPack AS [Previously Confirmed Cancel Dispatch Pack]
            FROM dbo.FullDispatchTransactions
            WHERE TransactionType = 'CANCEL_DISPATCH'
              AND (ConfirmedQuantityPack > 0 OR ConfirmedQuantityEach > 0);
        """, connection)


def save_full_cancel_dispatch_pending_transactions(
    rows: List[Dict[str, Any]],
    run_number: str,
) -> Dict[str, Any]:
    """Persist generated cancellation quantities without touching Dispatch rows."""
    initialize_database()
    prepared: Dict[str, Dict[str, Any]] = {}

    with Database().connect() as connection:
        if not _full_cancel_dispatch_schema_available(connection):
            return {"schema_available": False, "saved": 0}

        for row in rows or []:
            bn = _text(row, "BN").upper()
            expiry = _value(row, "Expiry Date")
            generic = _text(row, "Generic Item Number")
            to_address = _text(row, "To Address")
            gln = _text(row, "GLN")
            return_type = _text(row, "Return Type")
            pack_qty = max(0.0, _number(row, "To Be Cancel Dispatch"))
            package_size = max(0.0, _package_size(row, "PackageSize"))
            each_qty = pack_qty * package_size
            expiry_timestamp = pd.to_datetime(expiry, errors="coerce")
            expiry_month_key = (
                "" if pd.isna(expiry_timestamp)
                else expiry_timestamp.strftime("%Y-%m")
            )
            if not bn or not expiry_month_key or pack_qty <= 0:
                continue

            transaction_key = _full_cancel_dispatch_transaction_key(
                bn, expiry, generic, to_address, gln, return_type
            )
            current = prepared.setdefault(transaction_key, {
                "TransactionKey": transaction_key,
                "BN": bn,
                "ExpiryDate": expiry,
                "ExpiryMonthKey": expiry_month_key,
                "GenericItemNumber": generic,
                "ToAddress": to_address,
                "GLN": gln,
                "ReturnType": return_type,
                "Each": 0.0,
                "Pack": 0.0,
            })
            current["Each"] += each_qty
            current["Pack"] += pack_qty

        if not prepared:
            return {"schema_available": True, "saved": 0}

        sql = r"""
            MERGE dbo.FullDispatchTransactions WITH (HOLDLOCK) AS target
            USING
            (
                SELECT
                    ? AS TransactionKey, ? AS BN, ? AS ExpiryDate,
                    ? AS ExpiryMonthKey, ? AS GenericItemNumber,
                    ? AS ToAddress, ? AS GLN, ? AS SourceType,
                    ? AS NewQuantityEach, ? AS NewQuantityPack, ? AS RunNumber
            ) AS source
            ON target.TransactionKey = source.TransactionKey
            WHEN MATCHED THEN
                UPDATE SET
                    BN = source.BN,
                    ExpiryDate = source.ExpiryDate,
                    ExpiryMonthKey = source.ExpiryMonthKey,
                    GenericItemNumber = source.GenericItemNumber,
                    ToAddress = source.ToAddress,
                    GLN = source.GLN,
                    TransactionType = 'CANCEL_DISPATCH',
                    SourceType = source.SourceType,
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
                    FirstSubmittedRun, LastSubmittedRun,
                    TransactionType, SourceType
                )
                VALUES
                (
                    source.TransactionKey, source.BN, source.ExpiryDate,
                    source.ExpiryMonthKey, source.GenericItemNumber,
                    source.ToAddress, source.GLN,
                    source.NewQuantityEach, source.NewQuantityPack,
                    0, 0, source.RunNumber, source.RunNumber,
                    'CANCEL_DISPATCH', source.SourceType
                );
        """
        bulk_sql = r"""
            ;WITH source AS
            (
                SELECT
                    CONVERT(varchar(64), JSON_VALUE(j.value, '$[0]')) AS TransactionKey,
                    CONVERT(nvarchar(255), JSON_VALUE(j.value, '$[1]')) AS BN,
                    TRY_CONVERT(date, JSON_VALUE(j.value, '$[2]')) AS ExpiryDate,
                    CONVERT(nvarchar(7), JSON_VALUE(j.value, '$[3]')) AS ExpiryMonthKey,
                    CONVERT(nvarchar(255), JSON_VALUE(j.value, '$[4]')) AS GenericItemNumber,
                    CONVERT(nvarchar(1000), JSON_VALUE(j.value, '$[5]')) AS ToAddress,
                    CONVERT(nvarchar(100), JSON_VALUE(j.value, '$[6]')) AS GLN,
                    CONVERT(nvarchar(64), JSON_VALUE(j.value, '$[7]')) AS SourceType,
                    TRY_CONVERT(decimal(38,6), JSON_VALUE(j.value, '$[8]')) AS NewQuantityEach,
                    TRY_CONVERT(decimal(38,6), JSON_VALUE(j.value, '$[9]')) AS NewQuantityPack,
                    CONVERT(nvarchar(255), JSON_VALUE(j.value, '$[10]')) AS RunNumber
                FROM OPENJSON(CAST(? AS nvarchar(max))) AS j
            )
            MERGE dbo.FullDispatchTransactions WITH (HOLDLOCK) AS target
            USING source
            ON target.TransactionKey = source.TransactionKey
            WHEN MATCHED THEN
                UPDATE SET
                    BN = source.BN,
                    ExpiryDate = source.ExpiryDate,
                    ExpiryMonthKey = source.ExpiryMonthKey,
                    GenericItemNumber = source.GenericItemNumber,
                    ToAddress = source.ToAddress,
                    GLN = source.GLN,
                    TransactionType = 'CANCEL_DISPATCH',
                    SourceType = source.SourceType,
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
                    FirstSubmittedRun, LastSubmittedRun,
                    TransactionType, SourceType
                )
                VALUES
                (
                    source.TransactionKey, source.BN, source.ExpiryDate,
                    source.ExpiryMonthKey, source.GenericItemNumber,
                    source.ToAddress, source.GLN,
                    source.NewQuantityEach, source.NewQuantityPack,
                    0, 0, source.RunNumber, source.RunNumber,
                    'CANCEL_DISPATCH', source.SourceType
                );
        """
        cursor = connection.cursor()
        try:
            json_batch_size = max(
                100,
                int(os.getenv("FULL_DISPATCH_JSON_BATCH_SIZE", "4000") or 4000),
            )
            bulk_rows = [
                [
                    row["TransactionKey"], row["BN"], str(row["ExpiryDate"]),
                    row["ExpiryMonthKey"], row["GenericItemNumber"],
                    row["ToAddress"], row["GLN"], row["ReturnType"],
                    row["Each"], row["Pack"], str(run_number),
                ]
                for row in prepared.values()
            ]
            for row_batch in _chunks(bulk_rows, json_batch_size):
                cursor.execute(
                    bulk_sql,
                    json.dumps(
                        row_batch,
                        ensure_ascii=False,
                        separators=(",", ":"),
                    ),
                )
            connection.commit()
        except Exception as bulk_exc:
            connection.rollback()
            logger.warning(
                "OPENJSON Cancel Dispatch ledger save failed; using row fallback: %s",
                bulk_exc,
            )
            cursor = connection.cursor()
            try:
                for row in prepared.values():
                    cursor.execute(sql, (
                        row["TransactionKey"], row["BN"], row["ExpiryDate"],
                        row["ExpiryMonthKey"], row["GenericItemNumber"],
                        row["ToAddress"], row["GLN"], row["ReturnType"],
                        row["Each"], row["Pack"], str(run_number),
                    ))
                connection.commit()
            except Exception:
                connection.rollback()
                raise

    return {"schema_available": True, "saved": len(prepared)}


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


def confirm_full_cancel_dispatch_transactions_from_sfda(
    sfda_df: pd.DataFrame,
    source_file_name: str = "",
) -> Dict[str, Any]:
    """Confirm cancel-dispatch submissions from inverse SFDA movement.

    Evidence is conservative and uses the same Full Dispatch baseline: Active
    must increase while Quantity Sent Pending decreases for the same verified
    GTIN + BN + expiry month.  This function intentionally does not replace the
    baseline; the existing Full Dispatch confirmation does that after both
    movement directions have inspected the same previous snapshot.
    """
    import hashlib

    initialize_database()
    current = _prepare_dispatch_sfda_state(sfda_df)
    with Database().connect() as connection:
        if not _full_cancel_dispatch_schema_available(connection):
            return {
                "schema_available": False,
                "baseline_available": False,
                "confirmed_packs": 0.0,
                "confirmed_each": 0.0,
                "confirmed_transactions": 0,
                "confirmed_batches": 0,
            }

        previous = pd.read_sql(r"""
            SELECT GTIN, BN, ExpiryDate AS [Expiry Date], Active,
                   QuantitySentPending AS [Quantity sent pending]
            FROM dbo.FullDispatchSFDABaseline;
        """, connection)
        if previous.empty:
            return {
                "schema_available": True,
                "baseline_available": False,
                "confirmed_packs": 0.0,
                "confirmed_each": 0.0,
                "confirmed_transactions": 0,
                "confirmed_batches": 0,
            }

        for frame in (previous, current):
            frame["BN"] = frame["BN"].fillna("").astype(str).str.strip()
            frame["GTIN"] = frame["GTIN"].fillna("").astype(str).str.strip()
            frame["Expiry Date"] = pd.to_datetime(
                frame["Expiry Date"], errors="coerce"
            ).dt.normalize()
            frame["Expiry Month Key"] = frame["Expiry Date"].dt.strftime("%Y-%m")

        comparison = previous.merge(
            current,
            on=["GTIN", "BN", "Expiry Month Key"],
            how="inner",
            suffixes=(" Previous", " Current"),
        )
        if comparison.empty:
            evidence_rows: List[Dict[str, Any]] = []
        else:
            comparison["Active Increase"] = (
                pd.to_numeric(comparison["Active Current"], errors="coerce").fillna(0)
                - pd.to_numeric(comparison["Active Previous"], errors="coerce").fillna(0)
            ).clip(lower=0)
            comparison["Sent Pending Decrease"] = (
                pd.to_numeric(
                    comparison["Quantity sent pending Previous"], errors="coerce"
                ).fillna(0)
                - pd.to_numeric(
                    comparison["Quantity sent pending Current"], errors="coerce"
                ).fillna(0)
            ).clip(lower=0)
            comparison["Confirmed Pack Evidence"] = comparison[
                ["Active Increase", "Sent Pending Decrease"]
            ].min(axis=1)
            evidence_rows = comparison.loc[
                comparison["Confirmed Pack Evidence"].gt(0)
            ].to_dict(orient="records")

        cursor = connection.cursor()
        confirmed_pack_total = 0.0
        confirmed_each_total = 0.0
        confirmed_keys: Set[str] = set()
        confirmed_batches = 0
        try:
            for evidence in evidence_rows:
                bn = _text(evidence, "BN")
                expiry = _value(evidence, "Expiry Date Current")
                expiry_month = pd.to_datetime(expiry, errors="coerce").strftime("%Y-%m")
                remaining_pack = max(0.0, _number(evidence, "Confirmed Pack Evidence"))
                if remaining_pack <= 0:
                    continue

                pending_rows = cursor.execute(r"""
                    SELECT TransactionKey,
                           SubmittedQuantityEach, ConfirmedQuantityEach,
                           SubmittedQuantityPack, ConfirmedQuantityPack
                    FROM dbo.FullDispatchTransactions WITH (UPDLOCK, HOLDLOCK)
                    WHERE TransactionType = 'CANCEL_DISPATCH'
                      AND BN = ?
                      AND ExpiryMonthKey = ?
                      AND SubmittedQuantityPack > ConfirmedQuantityPack
                    ORDER BY CreatedAt, TransactionKey;
                """, (bn, expiry_month)).fetchall()

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
                        (
                            f"FULL-CANCEL-DISPATCH|{transaction_key}|"
                            f"{new_cumulative_pack:.6f}"
                        ).encode("utf-8")
                    ).hexdigest()

                    cursor.execute(r"""
                        UPDATE dbo.FullDispatchTransactions
                        SET ConfirmedQuantityPack = ConfirmedQuantityPack + ?,
                            ConfirmedQuantityEach = ConfirmedQuantityEach + ?,
                            LastConfirmedAt = SYSUTCDATETIME(),
                            UpdatedAt = SYSUTCDATETIME()
                        WHERE TransactionKey = ?;
                    """, (allocate_pack, allocate_each, transaction_key))
                    cursor.execute(r"""
                        INSERT INTO dbo.FullDispatchConfirmations
                            (ConfirmationKey, TransactionKey,
                             ConfirmedQuantityEach, ConfirmedQuantityPack)
                        SELECT ?, ?, ?, ?
                        WHERE NOT EXISTS
                        (
                            SELECT 1 FROM dbo.FullDispatchConfirmations
                            WHERE ConfirmationKey = ?
                        );
                    """, (
                        confirmation_key, transaction_key,
                        allocate_each, allocate_pack, confirmation_key,
                    ))
                    remaining_pack -= allocate_pack
                    batch_confirmed += allocate_pack
                    confirmed_pack_total += allocate_pack
                    confirmed_each_total += allocate_each
                    confirmed_keys.add(transaction_key)

                if batch_confirmed > 0:
                    confirmed_batches += 1

            connection.commit()
        except Exception:
            connection.rollback()
            raise

    return {
        "schema_available": True,
        "baseline_available": True,
        "source_file_name": str(source_file_name or ""),
        "confirmed_packs": float(confirmed_pack_total),
        "confirmed_each": float(confirmed_each_total),
        "confirmed_transactions": len(confirmed_keys),
        "confirmed_batches": int(confirmed_batches),
    }


def _prioritize_exact_full_dispatch_confirmation(
    pending_rows: Sequence[Any],
    confirmed_pack_evidence: float,
) -> List[Any]:
    """Prioritize one uniquely exact customer allocation over batch FIFO.

    SFDA confirms Dispatch movement at batch level and does not return the GLN.
    When the proven movement exactly equals the open quantity of one and only
    one previously generated customer allocation, that quantity is strong
    evidence for that specific allocation.  Put that row first so confirmation
    is assigned to the matching customer instead of an unrelated older row.

    Ambiguous or non-exact cases preserve the existing stable FIFO order.  A
    later submission-aware workflow can resolve those cases without guessing.
    """
    ordered = list(pending_rows or [])
    evidence = max(0.0, float(confirmed_pack_evidence or 0))
    if evidence <= 0 or len(ordered) < 2:
        return ordered

    tolerance = 0.000001
    exact_indexes: List[int] = []
    for index, pending in enumerate(ordered):
        submitted_pack = float(pending[3] or 0)
        confirmed_pack = float(pending[4] or 0)
        open_pack = max(0.0, submitted_pack - confirmed_pack)
        if abs(open_pack - evidence) <= tolerance:
            exact_indexes.append(index)

    if len(exact_indexes) != 1:
        return ordered

    exact_index = exact_indexes[0]
    return [ordered[exact_index], *ordered[:exact_index], *ordered[exact_index + 1 :]]


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
        transaction_type_available = _full_cancel_dispatch_schema_available(connection)
        active_cutover = _get_active_full_dispatch_cutover_with_connection(connection)
        cutover_id = (
            str(active_cutover.get("cutover_id") or "")
            if active_cutover is not None
            else ""
        )
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
        previous["Expiry Month Key"] = previous["Expiry Date"].dt.strftime("%Y-%m")
        current["Expiry Month Key"] = current["Expiry Date"].dt.strftime("%Y-%m")
        previous["GTIN"] = previous["GTIN"].fillna("").astype(str).str.strip()
        current["GTIN"] = current["GTIN"].fillna("").astype(str).str.strip()

        comparison = previous.merge(
            current,
            on=["GTIN", "BN", "Expiry Month Key"],
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
                expiry = _value(evidence, "Expiry Date Current")
                remaining_pack = max(
                    0.0,
                    _number(evidence, "Confirmed Pack Evidence"),
                )
                if remaining_pack <= 0:
                    continue

                pending_sql = r"""
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
                """
                if transaction_type_available:
                    pending_sql += " AND TransactionType = 'DISPATCH'"
                pending_params: List[Any] = [
                    bn,
                    pd.to_datetime(expiry, errors="coerce").strftime("%Y-%m"),
                ]
                if cutover_id:
                    pending_sql += " AND CutoverID = ?"
                    pending_params.append(cutover_id)
                pending_sql += " ORDER BY CreatedAt, TransactionKey;"
                pending_rows = cursor.execute(
                    pending_sql,
                    pending_params,
                ).fetchall()
                pending_rows = _prioritize_exact_full_dispatch_confirmation(
                    pending_rows,
                    remaining_pack,
                )

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



def create_password_reset_request(
    user_id: int,
    token_hash: str,
    expires_at: Any,
) -> None:
    """Create or replace an admin-approved password reset request."""
    with Database().connect() as connection:
        cursor = connection.cursor()
        cursor.execute(
            """
            UPDATE dbo.ApplicationUsers
            SET PasswordResetTokenHash=?,
                PasswordResetExpiresAt=?,
                PasswordResetStatus=N'Pending',
                PasswordResetRequestedAt=SYSUTCDATETIME(),
                PasswordResetApprovedAt=NULL,
                PasswordResetApprovedBy=NULL,
                UpdatedAt=SYSUTCDATETIME()
            WHERE UserID=? AND Status=N'Active';
            """,
            (token_hash, expires_at, int(user_id)),
        )
        connection.commit()


def get_password_reset_request_by_token_hash(
    token_hash: str,
) -> Optional[Dict[str, Any]]:
    """Return reset request state without exposing password material."""
    sql = """
        SELECT TOP (1)
            u.UserID,u.Email,u.PasswordResetStatus,u.PasswordResetRequestedAt,
            u.PasswordResetApprovedAt,u.PasswordResetApprovedBy,
            u.PasswordResetExpiresAt,u.Status,
            u.WarehouseID,w.WarehouseName
        FROM dbo.ApplicationUsers u
        LEFT JOIN dbo.Warehouses w ON w.WarehouseID=u.WarehouseID
        WHERE u.PasswordResetTokenHash=?;
    """
    with Database().connect() as connection:
        cursor = connection.cursor()
        row = cursor.execute(sql, (token_hash,)).fetchone()
        if not row:
            return None
        names = [column[0] for column in cursor.description]
        return dict(zip(names, row))


def set_password_reset_request_status(
    user_id: int,
    action: str,
    approved_by: str,
) -> Dict[str, Any]:
    """Approve or reject a pending password reset request."""
    normalized = str(action or "").strip().title()
    if normalized not in {"Approved", "Rejected"}:
        raise ValueError("Reset action must be Approved or Rejected.")

    with Database().connect() as connection:
        cursor = connection.cursor()
        cursor.execute(
            """
            UPDATE dbo.ApplicationUsers
            SET PasswordResetStatus=?,
                PasswordResetApprovedAt=CASE
                    WHEN ?=N'Approved' THEN SYSUTCDATETIME()
                    ELSE NULL
                END,
                PasswordResetApprovedBy=CASE
                    WHEN ?=N'Approved' THEN ?
                    ELSE ?
                END,
                UpdatedAt=SYSUTCDATETIME()
            WHERE UserID=?
              AND Status=N'Active'
              AND PasswordResetStatus=N'Pending'
              AND PasswordResetTokenHash IS NOT NULL
              AND PasswordResetExpiresAt>=SYSUTCDATETIME();
            """,
            (
                normalized,
                normalized,
                normalized,
                str(approved_by or "").strip(),
                str(approved_by or "").strip(),
                int(user_id),
            ),
        )
        if int(cursor.rowcount or 0) <= 0:
            connection.rollback()
            raise ValueError("No active pending password reset request was found for this user.")

        row = cursor.execute(
            """
            SELECT u.UserID,u.Email,u.Role,u.Status,u.ApprovedAt,u.CreatedAt,u.UpdatedAt,u.LastLoginAt,
                   u.WarehouseID,w.WarehouseName,w.WarehouseCode,u.RequestedWarehouseName,
                   u.PasswordResetStatus,u.PasswordResetRequestedAt,u.PasswordResetApprovedAt,
                   u.PasswordResetApprovedBy,u.PasswordResetExpiresAt
            FROM dbo.ApplicationUsers u
            LEFT JOIN dbo.Warehouses w ON w.WarehouseID=u.WarehouseID
            WHERE u.UserID=?;
            """,
            (int(user_id),),
        ).fetchone()
        result = _auth_row(cursor, row)
        connection.commit()
        return result


def reset_password_by_token_hash(
    token_hash: str,
    password_salt: str,
    password_hash: str,
) -> Optional[Dict[str, Any]]:
    """Consume an ADMIN-APPROVED reset token, replace password, revoke sessions."""
    with Database().connect() as connection:
        cursor = connection.cursor()
        row = cursor.execute(
            """
            SELECT TOP (1) UserID
            FROM dbo.ApplicationUsers WITH (UPDLOCK, HOLDLOCK)
            WHERE PasswordResetTokenHash=?
              AND PasswordResetStatus=N'Approved'
              AND PasswordResetExpiresAt>=SYSUTCDATETIME()
              AND Status=N'Active';
            """,
            (token_hash,),
        ).fetchone()
        if not row:
            connection.rollback()
            return None

        user_id = int(row[0])
        cursor.execute(
            """
            UPDATE dbo.ApplicationUsers
            SET PasswordSalt=?,
                PasswordHash=?,
                PasswordResetTokenHash=NULL,
                PasswordResetExpiresAt=NULL,
                PasswordResetStatus=N'Completed',
                UpdatedAt=SYSUTCDATETIME()
            WHERE UserID=?;
            """,
            (password_salt, password_hash, user_id),
        )
        cursor.execute(
            "DELETE FROM dbo.AuthSessions WHERE UserID=?;",
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
               u.WarehouseID,w.WarehouseName,w.WarehouseCode,u.RequestedWarehouseName,
               u.PasswordResetStatus,u.PasswordResetRequestedAt,u.PasswordResetApprovedAt,
               u.PasswordResetApprovedBy,u.PasswordResetExpiresAt
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
                ApprovedAt=CASE
                    WHEN ?=N'Active' AND ApprovedAt IS NULL THEN SYSUTCDATETIME()
                    ELSE ApprovedAt
                END,
                ApprovalTokenHash=CASE
                    WHEN ?=N'Active' THEN NULL
                    ELSE ApprovalTokenHash
                END,
                ApprovalExpiresAt=CASE
                    WHEN ?=N'Active' THEN NULL
                    ELSE ApprovalExpiresAt
                END,
                UpdatedAt=SYSUTCDATETIME()
            WHERE UserID=?;
            """,
            (status, status, status, status, int(user_id)),
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
