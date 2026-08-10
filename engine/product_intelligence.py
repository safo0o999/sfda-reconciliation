from __future__ import annotations

from typing import Any, Dict, List

import pandas as pd


class ProductIntelligenceEngine:
    """Build Product Intelligence datasets from persisted historical data and latest snapshots."""

    DEFAULT_GLN = "99999999999999"

    @staticmethod
    def _num(frame: pd.DataFrame, column: str) -> pd.Series:
        if column not in frame.columns:
            return pd.Series(0.0, index=frame.index)
        return pd.to_numeric(frame[column], errors="coerce").fillna(0)

    @staticmethod
    def _text(frame: pd.DataFrame, column: str) -> pd.Series:
        if column not in frame.columns:
            return pd.Series("", index=frame.index, dtype=object)
        return frame[column].fillna("").astype(str).str.strip()

    @staticmethod
    def _records(frame: pd.DataFrame, limit: int | None = None) -> List[Dict[str, Any]]:
        selected = frame.head(limit) if limit else frame
        records: List[Dict[str, Any]] = []
        for row in selected.to_dict(orient="records"):
            clean: Dict[str, Any] = {}
            for key, value in row.items():
                try:
                    if pd.isna(value):
                        clean[key] = None
                        continue
                except (TypeError, ValueError):
                    pass
                if isinstance(value, pd.Timestamp):
                    clean[key] = value.isoformat()
                elif hasattr(value, "item"):
                    try:
                        clean[key] = value.item()
                    except Exception:
                        clean[key] = value
                else:
                    clean[key] = value
            records.append(clean)
        return records

    def build(
        self,
        batch_master: pd.DataFrame,
        supplier_history: pd.DataFrame,
        customer_history: pd.DataFrame,
        inventory_snapshot: pd.DataFrame | None = None,
        sfda_snapshot: pd.DataFrame | None = None,
    ) -> Dict[str, Any]:
        master = batch_master.copy() if batch_master is not None else pd.DataFrame()
        suppliers = supplier_history.copy() if supplier_history is not None else pd.DataFrame()
        customers = customer_history.copy() if customer_history is not None else pd.DataFrame()
        inventory = inventory_snapshot.copy() if inventory_snapshot is not None else pd.DataFrame()
        sfda = sfda_snapshot.copy() if sfda_snapshot is not None else pd.DataFrame()

        if master.empty:
            return {
                "status": "Empty",
                "summary": {},
                "batches": [],
                "suppliers": [],
                "customers": [],
                "alerts": [],
                "metadata": {"has_inventory_snapshot": False, "has_sfda_snapshot": False},
            }

        for frame in (master, suppliers, customers, inventory, sfda):
            for column in ("BN", "Expiry Month Key", "Generic Item Number"):
                if column in frame.columns:
                    frame[column] = frame[column].fillna("").astype(str).str.strip().str.upper()

        base_columns = [
            "GTIN", "Drug Name", "BN", "Expiry Date", "Expiry Month Key",
            "Generic Item Number", "PackageSize", "Received Quantity Each",
            "Received Quantity Pack", "Total Dispatched Qty", "Total Dispatched Qty Pack",
            "First Received Date", "Last Received Date", "First Dispatch Date",
            "Last Dispatch Date", "Generic Exists in SFDA",
        ]
        for column in base_columns:
            if column not in master.columns:
                master[column] = None

        intelligence = master[base_columns].copy()
        intelligence = intelligence.rename(columns={
            "Received Quantity Each": "Historical Received Each",
            "Received Quantity Pack": "Historical Received Pack",
            "Total Dispatched Qty": "Historical Dispatched Each",
            "Total Dispatched Qty Pack": "Historical Dispatched Pack",
        })

        inventory_group = pd.DataFrame(columns=["BN", "Expiry Month Key", "Inventory Each"])
        if not inventory.empty:
            qty_column = "Available Quantity" if "Available Quantity" in inventory.columns else "Current Inventory Quantity Each"
            inventory[qty_column] = self._num(inventory, qty_column)
            inventory_group = inventory.groupby(["BN", "Expiry Month Key"], dropna=False)[qty_column].sum().reset_index()
            inventory_group = inventory_group.rename(columns={qty_column: "Inventory Each"})

        sfda_group = pd.DataFrame(columns=["BN", "Expiry Month Key", "SFDA Quantity", "SFDA Active", "Receive Pending", "Sent Pending"])
        if not sfda.empty:
            quantity_map = {
                "Quantity": "SFDA Quantity",
                "Active": "SFDA Active",
                "Quantity Receive Pending": "Receive Pending",
                "Quantity sent pending": "Sent Pending",
            }
            for source in quantity_map:
                sfda[source] = self._num(sfda, source)
            sfda_group = sfda.groupby(["BN", "Expiry Month Key"], dropna=False)[list(quantity_map)].sum().reset_index().rename(columns=quantity_map)

        intelligence = intelligence.merge(inventory_group, on=["BN", "Expiry Month Key"], how="left")
        intelligence = intelligence.merge(sfda_group, on=["BN", "Expiry Month Key"], how="left")

        for column in [
            "Historical Received Each", "Historical Received Pack", "Historical Dispatched Each",
            "Historical Dispatched Pack", "Inventory Each", "SFDA Quantity", "SFDA Active",
            "Receive Pending", "Sent Pending", "PackageSize",
        ]:
            intelligence[column] = self._num(intelligence, column)

        intelligence["Inventory Pack"] = 0.0
        valid_pack = intelligence["PackageSize"].gt(0)
        intelligence.loc[valid_pack, "Inventory Pack"] = intelligence.loc[valid_pack, "Inventory Each"] / intelligence.loc[valid_pack, "PackageSize"]

        intelligence["Remaining Accept"] = (
            intelligence["Historical Received Pack"]
            - intelligence["SFDA Active"]
            - intelligence["Sent Pending"]
        ).clip(lower=0)
        intelligence["Remaining Accept"] = intelligence[["Remaining Accept", "Receive Pending"]].min(axis=1).clip(lower=0)
        intelligence["Remaining Dispatch"] = (intelligence["SFDA Active"] - intelligence["Inventory Pack"]).clip(lower=0)

        intelligence["Status"] = "Balanced"
        intelligence.loc[intelligence["Generic Exists in SFDA"].astype(str).ne("Yes"), "Status"] = "Missing in SFDA"
        intelligence.loc[~valid_pack, "Status"] = "Package Size Missing"
        intelligence.loc[intelligence["Remaining Accept"].gt(0), "Status"] = "Accept Required"
        intelligence.loc[intelligence["Remaining Dispatch"].gt(0), "Status"] = "Dispatch Required"

        supplier_count = int(self._text(suppliers, "Supplier Name").replace("", pd.NA).nunique()) if not suppliers.empty else 0
        customer_count = int(self._text(customers, "To Address").replace("", pd.NA).nunique()) if not customers.empty else 0

        customer_with_gln_count = 0
        customer_dummy_gln_count = 0
        customer_unmapped_count = 0
        if not customers.empty:
            customer_names = self._text(customers, "To Address")
            customer_gln = self._text(customers, "GLN")
            customer_map = pd.DataFrame(
                {
                    "Customer": customer_names,
                    "GLN": customer_gln,
                }
            ).drop_duplicates()

            mapped_mask = (
                customer_map["GLN"].ne("")
                & customer_map["GLN"].str.upper().ne("DUMMY")
                & customer_map["GLN"].ne(self.DEFAULT_GLN)
            )
            dummy_mask = (
                customer_map["GLN"].str.upper().eq("DUMMY")
                | customer_map["GLN"].eq(self.DEFAULT_GLN)
            )
            unmapped_mask = customer_map["GLN"].eq("")

            customer_with_gln_count = int(
                customer_map.loc[mapped_mask, "Customer"]
                .replace("", pd.NA)
                .nunique()
            )
            customer_dummy_gln_count = int(
                customer_map.loc[dummy_mask, "Customer"]
                .replace("", pd.NA)
                .nunique()
            )
            customer_unmapped_count = int(
                customer_map.loc[unmapped_mask, "Customer"]
                .replace("", pd.NA)
                .nunique()
            )

        summary = {
            "historical_received_pack": float(intelligence["Historical Received Pack"].sum()),
            "historical_dispatched_pack": float(intelligence["Historical Dispatched Pack"].sum()),
            "current_inventory_pack": float(intelligence["Inventory Pack"].sum()),
            "sfda_active": float(intelligence["SFDA Active"].sum()),
            "receive_pending": float(intelligence["Receive Pending"].sum()),
            "sent_pending": float(intelligence["Sent Pending"].sum()),
            "remaining_accept": float(intelligence["Remaining Accept"].sum()),
            "remaining_dispatch": float(intelligence["Remaining Dispatch"].sum()),
            "batch_count": int(len(intelligence)),
            "generic_count": int(self._text(intelligence, "Generic Item Number").replace("", pd.NA).nunique()),
            "supplier_count": supplier_count,
            "customer_count": customer_count,
            "customer_with_gln_count": customer_with_gln_count,
            "customer_dummy_gln_count": customer_dummy_gln_count,
            "customer_unmapped_count": customer_unmapped_count,
            "last_received": pd.to_datetime(intelligence["Last Received Date"], errors="coerce").max(),
            "last_dispatch": pd.to_datetime(intelligence["Last Dispatch Date"], errors="coerce").max(),
        }

        supplier_table = pd.DataFrame()
        if not suppliers.empty:
            supplier_qty = "Received Quantity Pack"
            suppliers[supplier_qty] = self._num(suppliers, supplier_qty)
            supplier_table = suppliers.groupby(["Supplier Name", "Supplier Code"], dropna=False).agg(
                received_pack=(supplier_qty, "sum"),
                last_receipt=("Last Received Date", "max"),
            ).reset_index().sort_values("received_pack", ascending=False)

        customer_table = pd.DataFrame()
        if not customers.empty:
            customer_qty = "Dispatch Quantity Pack"
            customers[customer_qty] = self._num(customers, customer_qty)
            customer_table = customers.groupby(["To Address", "GLN"], dropna=False).agg(
                dispatched_pack=(customer_qty, "sum"),
                last_dispatch=("Last Dispatch Date", "max"),
            ).reset_index().sort_values("dispatched_pack", ascending=False)

        alerts = []
        alert_rules = [
            ("Missing in SFDA", "Batches Missing in SFDA"),
            ("Accept Required", "Accept Required"),
            ("Dispatch Required", "Dispatch Required"),
            ("Package Size Missing", "Package Size Missing"),
        ]
        for status, label in alert_rules:
            count = int(intelligence["Status"].eq(status).sum())
            alerts.append({"type": status, "label": label, "count": count})

        if not customers.empty and "GLN" in customers.columns:
            gln = self._text(customers, "GLN")
            missing_gln_count = int(gln.isin(["", "DUMMY", self.DEFAULT_GLN]).sum())
        else:
            missing_gln_count = 0
        alerts.append({"type": "GLN Missing", "label": "GLN Missing", "count": missing_gln_count})

        return {
            "status": "Completed",
            "summary": summary,
            "batches": self._records(intelligence.sort_values(["Status", "Generic Item Number", "BN"], kind="stable"), 500),
            "suppliers": self._records(supplier_table, 100),
            "customers": self._records(customer_table, 100),
            "alerts": alerts,
            "status_distribution": intelligence["Status"].value_counts().to_dict(),
            "metadata": {
                "has_inventory_snapshot": not inventory.empty,
                "has_sfda_snapshot": not sfda.empty,
            },
        }
