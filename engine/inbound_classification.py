from __future__ import annotations

from typing import Any

import pandas as pd


# Official WMS Inbound Shipment prefixes supplied for the SFDA/RSD workflow.
TRK_CUSTOMER_RETURN = "TRK30"
TRK_RESERVATION = "TRK43"
TRK_STO_RETURN = "TRK49"
TRK_SUPPLIER = "TRK5060"
TRK_STO_INCOMING = "TRK800"
TRK_PRINCIPAL = "TRK74"

SUPPLIER = "Supplier"
STO_INCOMING = "STO Incoming"
STO_RETURN = "STO Return"
CUSTOMER_RETURN = "Customer Return"
RESERVATION = "Reservation"
PRINCIPAL = "Principal"
UNCLASSIFIED = "Unclassified"

# Receipt types that can contribute to physical historical Batch Master receipt totals.
BATCH_MASTER_TYPES = {SUPPLIER, STO_INCOMING, STO_RETURN}

# Receipt types that are allowed to produce an SFDA/RSD Accept quantity.
ACCEPT_TYPES = {SUPPLIER, STO_INCOMING}

# Supplier variance is intentionally supplier-only.
SUPPLIER_VARIANCE_TYPES = {SUPPLIER}

# These types never participate in SFDA/RSD calculations.
EXCLUDED_TYPES = {CUSTOMER_RETURN, RESERVATION, PRINCIPAL, UNCLASSIFIED}


def normalize_inbound_shipment(value: Any) -> str:
    if value is None:
        return ""
    try:
        if pd.isna(value):
            return ""
    except (TypeError, ValueError):
        pass
    return str(value).strip().upper()


def classify_inbound_shipment(value: Any) -> str:
    text = normalize_inbound_shipment(value)
    if text.startswith(TRK_CUSTOMER_RETURN):
        return CUSTOMER_RETURN
    if text.startswith(TRK_RESERVATION):
        return RESERVATION
    if text.startswith(TRK_STO_RETURN):
        return STO_RETURN
    if text.startswith(TRK_SUPPLIER):
        return SUPPLIER
    if text.startswith(TRK_STO_INCOMING):
        return STO_INCOMING
    if text.startswith(TRK_PRINCIPAL):
        return PRINCIPAL
    return UNCLASSIFIED


def classification_status(receipt_type: str) -> str:
    mapping = {
        SUPPLIER: "Supplier Receipt - SFDA Reconciliation",
        STO_INCOMING: "STO Incoming - Match RSD Receive Pending",
        STO_RETURN: "STO Return - Cancel Previous RSD Dispatch",
        CUSTOMER_RETURN: "Customer Return - Excluded from SFDA/RSD",
        RESERVATION: "Reservation - Excluded from SFDA/RSD",
        PRINCIPAL: "Principal - Excluded from SFDA/RSD",
        UNCLASSIFIED: "Unclassified TRK - Review Required",
    }
    return mapping.get(str(receipt_type or ""), "Unclassified TRK - Review Required")
