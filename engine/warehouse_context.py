from __future__ import annotations

import contextvars
from contextlib import contextmanager
from typing import Iterator

_DEFAULT_WAREHOUSE_ID = 1
_WAREHOUSE_ID = contextvars.ContextVar('sfda_warehouse_id', default=_DEFAULT_WAREHOUSE_ID)
_WAREHOUSE_NAME = contextvars.ContextVar('sfda_warehouse_name', default='Madinah Warehouse')


def current_warehouse_id() -> int:
    try:
        return max(1, int(_WAREHOUSE_ID.get() or _DEFAULT_WAREHOUSE_ID))
    except Exception:
        return _DEFAULT_WAREHOUSE_ID


def current_warehouse_name() -> str:
    return str(_WAREHOUSE_NAME.get() or 'Madinah Warehouse')


def set_current_warehouse(warehouse_id: int, warehouse_name: str = '') -> None:
    _WAREHOUSE_ID.set(max(1, int(warehouse_id or _DEFAULT_WAREHOUSE_ID)))
    if warehouse_name:
        _WAREHOUSE_NAME.set(str(warehouse_name).strip())


@contextmanager
def warehouse_scope(warehouse_id: int, warehouse_name: str = '') -> Iterator[None]:
    id_token = _WAREHOUSE_ID.set(max(1, int(warehouse_id or _DEFAULT_WAREHOUSE_ID)))
    name_token = _WAREHOUSE_NAME.set(str(warehouse_name or 'Madinah Warehouse').strip())
    try:
        yield
    finally:
        _WAREHOUSE_ID.reset(id_token)
        _WAREHOUSE_NAME.reset(name_token)
