from __future__ import annotations

from pathlib import Path
from typing import Any, Dict

import pandas as pd

from engine.database import Database, initialize_database
from engine.normalizer import Normalizer
from engine.warehouse_context import current_warehouse_id, current_warehouse_name


DUMMY_GLN = "99999999999999"
LEGACY_MADINAH_WAREHOUSE_ID = 1


def _empty_gln() -> pd.DataFrame:
    return pd.DataFrame(columns=["To Address", "GLN"])


def _legacy_gln_path() -> Path:
    return Path(__file__).resolve().parent.parent / "config" / "gln.xlsx"


def _normalize_gln(frame: pd.DataFrame) -> pd.DataFrame:
    if frame is None or frame.empty:
        return _empty_gln()

    working = frame.copy()
    normalized = Normalizer.normalize_gln(working)
    if normalized.empty:
        return _empty_gln()

    return normalized[["To Address", "GLN"]].copy().reset_index(drop=True)


def load_current_warehouse_gln() -> pd.DataFrame:
    """Return the GLN mapping allowed for the current warehouse.

    Warehouse 1 (Madinah) intentionally remains on the legacy internal file.
    Every other warehouse reads only its own SQL mapping through the existing
    WarehouseID session context / RLS boundary. If no mapping exists, an empty
    dataframe is returned and the dispatch engines use the controlled dummy
    GLN fallback.
    """

    warehouse_id = int(current_warehouse_id())

    if warehouse_id == LEGACY_MADINAH_WAREHOUSE_ID:
        path = _legacy_gln_path()
        if not path.exists():
            raise RuntimeError(f"Legacy Madinah GLN file is missing: {path}")
        legacy = pd.read_excel(path, engine="openpyxl", dtype=object)
        return _normalize_gln(legacy)

    initialize_database()
    sql = """
        SELECT
            ToAddress AS [To Address],
            GLN
        FROM dbo.WarehouseGLNMapping
        WHERE WarehouseID = ?
        ORDER BY ToAddress;
    """

    try:
        with Database().connect() as connection:
            frame = pd.read_sql(sql, connection, params=[warehouse_id])
    except Exception as exc:
        if "WarehouseGLNMapping" in str(exc):
            raise RuntimeError(
                "Warehouse GLN database migration is not installed. "
                "Run sql/004_warehouse_gln_mapping.sql before using non-Madinah warehouses."
            ) from exc
        raise

    return _normalize_gln(frame)


def apply_current_warehouse_gln(frame: pd.DataFrame) -> pd.DataFrame:
    """Overlay the current warehouse GLN onto a dataframe with To Address.

    This intentionally does not update historical SQL quantities or movement.
    It only prevents a previously stored GLN from another warehouse from being
    reused after Multi-Warehouse isolation was introduced.
    """

    if frame is None:
        return pd.DataFrame()

    result = frame.copy()
    if result.empty:
        if "GLN" not in result.columns:
            result["GLN"] = pd.Series(dtype=object)
        return result

    if "To Address" not in result.columns:
        return result

    mapping = load_current_warehouse_gln()
    lookup = mapping.copy()
    lookup["_Address Key"] = Normalizer.text(lookup.get("To Address", ""))
    lookup["GLN"] = Normalizer.identifier(lookup.get("GLN", ""))
    lookup = (
        lookup.loc[lookup["_Address Key"].ne(""), ["_Address Key", "GLN"]]
        .drop_duplicates("_Address Key", keep="first")
    )

    result["_Address Key"] = Normalizer.text(result["To Address"])
    result = result.drop(columns=["GLN"], errors="ignore").merge(
        lookup,
        on="_Address Key",
        how="left",
        validate="many_to_one",
    )

    warehouse_id = int(current_warehouse_id())
    result["GLN"] = result["GLN"].fillna("").astype(str).str.strip()

    # Non-Madinah warehouses never inherit a GLN from stored legacy history.
    # No mapping means the controlled 14-nine fallback.
    if warehouse_id != LEGACY_MADINAH_WAREHOUSE_ID:
        result.loc[result["GLN"].eq(""), "GLN"] = DUMMY_GLN
    else:
        # Full Customer History historically used the 14-nine fallback for
        # unmapped Madinah customers, so preserve that existing behavior.
        result.loc[result["GLN"].eq(""), "GLN"] = DUMMY_GLN

    return result.drop(columns=["_Address Key"], errors="ignore")


def get_current_warehouse_gln_status() -> Dict[str, Any]:
    warehouse_id = int(current_warehouse_id())
    warehouse_name = str(current_warehouse_name() or f"Warehouse {warehouse_id}")

    if warehouse_id == LEGACY_MADINAH_WAREHOUSE_ID:
        mapping = load_current_warehouse_gln()
        return {
            "warehouse_id": warehouse_id,
            "warehouse_name": warehouse_name,
            "mode": "legacy_madinah",
            "configured": True,
            "mapping_rows": int(len(mapping)),
            "source": "config/gln.xlsx",
            "fallback_gln": DUMMY_GLN,
            "upload_allowed": False,
            "message": "Madinah continues to use the existing internal GLN file without any dispatch logic change.",
        }

    initialize_database()
    sql = """
        SELECT
            COUNT_BIG(*) AS MappingRows,
            MAX(SourceFileName) AS SourceFileName,
            MAX(UpdatedAt) AS UpdatedAt
        FROM dbo.WarehouseGLNMapping
        WHERE WarehouseID = ?;
    """

    try:
        with Database().connect() as connection:
            row = connection.cursor().execute(sql, warehouse_id).fetchone()
    except Exception as exc:
        if "WarehouseGLNMapping" in str(exc):
            raise RuntimeError(
                "Warehouse GLN database migration is not installed. "
                "Run sql/004_warehouse_gln_mapping.sql before using non-Madinah warehouses."
            ) from exc
        raise

    count = int((row[0] if row else 0) or 0)
    source_file = str((row[1] if row else "") or "").strip()
    updated_at = row[2] if row else None

    return {
        "warehouse_id": warehouse_id,
        "warehouse_name": warehouse_name,
        "mode": "warehouse_mapping" if count > 0 else "dummy_fallback",
        "configured": count > 0,
        "mapping_rows": count,
        "source": source_file,
        "updated_at": updated_at,
        "fallback_gln": DUMMY_GLN,
        "upload_allowed": True,
        "message": (
            "This warehouse uses only its own GLN mapping."
            if count > 0
            else f"No GLN mapping is configured. All unmatched customers will use {DUMMY_GLN}."
        ),
    }


def replace_current_warehouse_gln(
    frame: pd.DataFrame,
    source_file_name: str = "",
    updated_by: str = "",
) -> Dict[str, Any]:
    """Replace the current non-Madinah warehouse GLN mapping atomically."""

    warehouse_id = int(current_warehouse_id())
    warehouse_name = str(current_warehouse_name() or f"Warehouse {warehouse_id}")

    if warehouse_id == LEGACY_MADINAH_WAREHOUSE_ID:
        raise ValueError(
            "Madinah GLN mapping is intentionally locked to the legacy config/gln.xlsx file."
        )

    normalized = _normalize_gln(frame)
    if normalized.empty:
        raise ValueError(
            "The GLN file contains no valid rows. Required columns: To Address and GLN."
        )

    rows = [
        (
            warehouse_id,
            str(row.get("To Address") or "").strip(),
            str(row.get("GLN") or "").strip(),
            str(source_file_name or "").strip(),
            str(updated_by or "").strip(),
        )
        for row in normalized.to_dict(orient="records")
    ]

    initialize_database()
    with Database().connect() as connection:
        cursor = connection.cursor()
        try:
            # RLS plus the explicit predicate ensures this replacement can only
            # affect the current warehouse.
            cursor.execute(
                "DELETE FROM dbo.WarehouseGLNMapping WHERE WarehouseID = ?;",
                warehouse_id,
            )
            cursor.fast_executemany = True
            cursor.executemany(
                """
                INSERT INTO dbo.WarehouseGLNMapping
                (
                    WarehouseID,
                    ToAddress,
                    GLN,
                    SourceFileName,
                    UpdatedBy,
                    UpdatedAt
                )
                VALUES (?, ?, ?, ?, ?, SYSUTCDATETIME());
                """,
                rows,
            )
            connection.commit()
        except Exception:
            connection.rollback()
            raise

    return {
        "warehouse_id": warehouse_id,
        "warehouse_name": warehouse_name,
        "mapping_rows": int(len(rows)),
        "source": str(source_file_name or "").strip(),
        "fallback_gln": DUMMY_GLN,
        "message": "Warehouse GLN mapping replaced successfully.",
    }
