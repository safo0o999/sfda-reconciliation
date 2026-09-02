from __future__ import annotations

import hashlib
from typing import Any, Dict, List, Optional

import pandas as pd


class VarianceManagementEngine:
    """Build a live exception queue from persisted WMS history and latest SFDA data."""

    DEFAULT_GLN = "99999999999999"
    SFDA_REPORT_TYPES = {"Missing Registration", "Quantity Mismatch"}

    @staticmethod
    def _text(frame: pd.DataFrame, column: str) -> pd.Series:
        if column not in frame.columns:
            return pd.Series("", index=frame.index, dtype=object)
        return frame[column].fillna("").astype(str).str.strip()

    @staticmethod
    def _num(frame: pd.DataFrame, column: str) -> pd.Series:
        if column not in frame.columns:
            return pd.Series(0.0, index=frame.index)
        return pd.to_numeric(frame[column], errors="coerce").fillna(0.0)

    @staticmethod
    def _date(frame: pd.DataFrame, column: str) -> pd.Series:
        if column not in frame.columns:
            return pd.Series(pd.NaT, index=frame.index, dtype="datetime64[ns]")
        return pd.to_datetime(frame[column], errors="coerce", dayfirst=True)

    @staticmethod
    def _month_key(series: pd.Series) -> pd.Series:
        parsed = pd.to_datetime(series, errors="coerce", dayfirst=True)
        return parsed.dt.strftime("%Y-%m").fillna("")

    @staticmethod
    def _clean_number(value: Any) -> float:
        parsed = pd.to_numeric(pd.Series([value]), errors="coerce").iloc[0]
        return 0.0 if pd.isna(parsed) else float(parsed)

    @staticmethod
    def _variance_id(*parts: Any) -> str:
        raw = "|".join(str(part or "").strip().upper() for part in parts)
        return hashlib.sha256(raw.encode("utf-8")).hexdigest()[:24]

    @staticmethod
    def _json_records(frame: pd.DataFrame) -> List[Dict[str, Any]]:
        records: List[Dict[str, Any]] = []
        for row in frame.to_dict(orient="records"):
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

    def _supplier_variances(
        self,
        supplier_history: pd.DataFrame,
        sfda_snapshot: pd.DataFrame,
    ) -> pd.DataFrame:
        if supplier_history is None or supplier_history.empty:
            return pd.DataFrame()

        supplier = supplier_history.copy()
        sfda = sfda_snapshot.copy() if sfda_snapshot is not None else pd.DataFrame()

        supplier["Supplier Name"] = self._text(supplier, "Supplier Name")
        supplier["Supplier Code"] = self._text(supplier, "Supplier Code")
        supplier["BN"] = self._text(supplier, "BN").str.upper()
        supplier["Generic Item Number"] = self._text(supplier, "Generic Item Number")
        supplier["GTIN"] = self._text(supplier, "GTIN")
        supplier["Drug Name"] = self._text(supplier, "Drug Name")
        supplier["Description"] = self._text(supplier, "Description")
        supplier["Expiry Date"] = self._date(supplier, "Expiry Date")
        supplier["Expiry Month Key"] = self._text(supplier, "Expiry Month Key")
        missing_month = supplier["Expiry Month Key"].eq("")
        supplier.loc[missing_month, "Expiry Month Key"] = self._month_key(
            supplier.loc[missing_month, "Expiry Date"]
        )
        supplier["Received Quantity Each"] = self._num(
            supplier, "Received Quantity Each"
        )
        supplier["Received Quantity Pack"] = self._num(
            supplier, "Received Quantity Pack"
        )
        supplier["Last Received Date"] = self._date(supplier, "Last Received Date")

        supplier = (
            supplier.groupby(
                [
                    "Supplier Name",
                    "Supplier Code",
                    "BN",
                    "Expiry Month Key",
                    "Generic Item Number",
                ],
                dropna=False,
            )
            .agg(
                **{
                    "Expiry Date": ("Expiry Date", "max"),
                    "GTIN History": ("GTIN", "first"),
                    "Drug Name History": ("Drug Name", "first"),
                    "Description": ("Description", "first"),
                    "Received Quantity Each": ("Received Quantity Each", "sum"),
                    "Received Quantity Pack": ("Received Quantity Pack", "sum"),
                    "Last Received Date": ("Last Received Date", "max"),
                }
            )
            .reset_index()
        )

        if sfda.empty:
            sfda_group = pd.DataFrame(
                columns=[
                    "BN",
                    "Expiry Month Key",
                    "GTIN",
                    "Drug Name",
                    "SFDA Quantity",
                    "SFDA Active",
                    "Quantity Receive Pending",
                    "Quantity Sent Pending",
                ]
            )
        else:
            sfda["BN"] = self._text(sfda, "BN").str.upper()
            sfda["Expiry Date"] = self._date(sfda, "Expiry Date")
            sfda["Expiry Month Key"] = self._text(sfda, "Expiry Month Key")
            missing_month = sfda["Expiry Month Key"].eq("")
            sfda.loc[missing_month, "Expiry Month Key"] = self._month_key(
                sfda.loc[missing_month, "Expiry Date"]
            )
            sfda["GTIN"] = self._text(sfda, "GTIN")
            sfda["Drug Name"] = self._text(sfda, "Drug Name")
            sfda["Quantity"] = self._num(sfda, "Quantity")
            sfda["Active"] = self._num(sfda, "Active")
            sfda["Quantity Receive Pending"] = self._num(
                sfda, "Quantity Receive Pending"
            )
            sfda["Quantity sent pending"] = self._num(
                sfda, "Quantity sent pending"
            )
            sfda_group = (
                sfda.groupby(["BN", "Expiry Month Key"], dropna=False)
                .agg(
                    GTIN=("GTIN", "first"),
                    **{
                        "Drug Name": ("Drug Name", "first"),
                        "SFDA Quantity": ("Quantity", "sum"),
                        "SFDA Active": ("Active", "sum"),
                        "Quantity Receive Pending": (
                            "Quantity Receive Pending",
                            "sum",
                        ),
                        "Quantity Sent Pending": (
                            "Quantity sent pending",
                            "sum",
                        ),
                    },
                )
                .reset_index()
            )

        merged = supplier.merge(
            sfda_group,
            on=["BN", "Expiry Month Key"],
            how="left",
            indicator=True,
            validate="many_to_one",
        )
        merged["GTIN"] = self._text(merged, "GTIN")
        merged["Drug Name"] = self._text(merged, "Drug Name")
        merged["GTIN"] = merged["GTIN"].where(
            merged["GTIN"].ne(""), self._text(merged, "GTIN History")
        )
        merged["Drug Name"] = merged["Drug Name"].where(
            merged["Drug Name"].ne(""), self._text(merged, "Drug Name History")
        )
        for column in [
            "SFDA Quantity",
            "SFDA Active",
            "Quantity Receive Pending",
            "Quantity Sent Pending",
        ]:
            merged[column] = self._num(merged, column)

        merged["Difference Pack"] = (
            merged["Received Quantity Pack"] - merged["SFDA Quantity"]
        )

        records: List[Dict[str, Any]] = []
        for row in merged.to_dict(orient="records"):
            missing_registration = row.get("_merge") == "left_only"
            difference = self._clean_number(row.get("Difference Pack"))
            if not missing_registration and abs(difference) < 1e-9:
                continue

            if missing_registration:
                variance_type = "Missing Registration"
                severity = "Critical"
                description = "Received batch is not present in the latest SFDA report."
                required_action = "Register the batch in SFDA and confirm the reported quantity."
                report_difference = self._clean_number(row.get("Received Quantity Pack"))
                variance_status = "Batch Missing in SFDA"
            else:
                variance_type = "Quantity Mismatch"
                severity = "Warning"
                if difference > 0:
                    description = "SFDA quantity is lower than the cumulative WMS received quantity."
                    required_action = "Update the missing received quantity in SFDA."
                    variance_status = "SFDA Reported Less"
                else:
                    description = "SFDA quantity is higher than the cumulative WMS received quantity."
                    required_action = "Investigate the excess quantity reported in SFDA."
                    variance_status = "SFDA Reported More"
                report_difference = difference

            variance_id = self._variance_id(
                variance_type,
                row.get("Supplier Code"),
                row.get("BN"),
                row.get("Expiry Month Key"),
                row.get("Generic Item Number"),
            )
            records.append(
                {
                    "Variance ID": variance_id,
                    "Severity": severity,
                    "Variance Type": variance_type,
                    "Status": "Open",
                    "Supplier Name": row.get("Supplier Name") or "",
                    "Supplier Code": row.get("Supplier Code") or "",
                    "GTIN": row.get("GTIN") or "",
                    "Drug Name": row.get("Drug Name") or "",
                    "BN": row.get("BN") or "",
                    "Expiry Date": row.get("Expiry Date"),
                    "Expiry Month Key": row.get("Expiry Month Key") or "",
                    "Generic Item Number": row.get("Generic Item Number") or "",
                    "Description": description,
                    "Received Quantity Each": self._clean_number(
                        row.get("Received Quantity Each")
                    ),
                    "Received Quantity Pack": self._clean_number(
                        row.get("Received Quantity Pack")
                    ),
                    "SFDA Quantity": self._clean_number(row.get("SFDA Quantity")),
                    "SFDA Active": self._clean_number(row.get("SFDA Active")),
                    "Quantity Receive Pending": self._clean_number(
                        row.get("Quantity Receive Pending")
                    ),
                    "Quantity Sent Pending": self._clean_number(
                        row.get("Quantity Sent Pending")
                    ),
                    "Difference Pack": report_difference,
                    "Variance Status": variance_status,
                    "Required Action": required_action,
                    "Last Received Date": row.get("Last Received Date"),
                    "Report To SFDA": True,
                }
            )

        return pd.DataFrame(records)

    def _customer_variances(self, customer_history: pd.DataFrame) -> pd.DataFrame:
        if customer_history is None or customer_history.empty:
            return pd.DataFrame()
        customer = customer_history.copy()
        customer["To Address"] = self._text(customer, "To Address")
        customer["GLN"] = self._text(customer, "GLN")
        customer["Last Dispatch Date"] = self._date(customer, "Last Dispatch Date")
        missing = customer["GLN"].str.upper().isin(
            ["", "DUMMY", self.DEFAULT_GLN]
        )
        customer = customer.loc[missing].copy()
        if customer.empty:
            return pd.DataFrame()

        grouped = (
            customer.groupby(["To Address", "GLN"], dropna=False)
            .agg(**{"Last Dispatch Date": ("Last Dispatch Date", "max")})
            .reset_index()
        )
        records = []
        for row in grouped.to_dict(orient="records"):
            records.append(
                {
                    "Variance ID": self._variance_id(
                        "GLN Missing", row.get("To Address"), row.get("GLN")
                    ),
                    "Severity": "Info",
                    "Variance Type": "GLN Missing",
                    "Status": "Open",
                    "Supplier Name": "",
                    "Supplier Code": "",
                    "GTIN": "",
                    "Drug Name": "",
                    "BN": "",
                    "Expiry Date": None,
                    "Expiry Month Key": "",
                    "Generic Item Number": "",
                    "Description": f"Customer GLN is not mapped: {row.get('To Address') or ''}",
                    "Received Quantity Each": 0.0,
                    "Received Quantity Pack": 0.0,
                    "SFDA Quantity": 0.0,
                    "SFDA Active": 0.0,
                    "Quantity Receive Pending": 0.0,
                    "Quantity Sent Pending": 0.0,
                    "Difference Pack": 0.0,
                    "Variance Status": "GLN Missing",
                    "Required Action": "Map the customer to the correct SFDA GLN.",
                    "Last Received Date": row.get("Last Dispatch Date"),
                    "Report To SFDA": False,
                    "Customer": row.get("To Address") or "",
                    "GLN": row.get("GLN") or self.DEFAULT_GLN,
                }
            )
        return pd.DataFrame(records)

    def build(
        self,
        supplier_history: pd.DataFrame,
        customer_history: pd.DataFrame,
        sfda_snapshot: pd.DataFrame,
        inventory_snapshot: Optional[pd.DataFrame] = None,
    ) -> Dict[str, Any]:
        supplier_items = self._supplier_variances(supplier_history, sfda_snapshot)
        customer_items = self._customer_variances(customer_history)
        frames = [frame for frame in [supplier_items, customer_items] if not frame.empty]
        items = pd.concat(frames, ignore_index=True, sort=False) if frames else pd.DataFrame()

        if not items.empty:
            severity_rank = {"Critical": 0, "Warning": 1, "Info": 2}
            items["_severity_rank"] = items["Severity"].map(severity_rank).fillna(9)
            items = items.sort_values(
                ["_severity_rank", "Variance Type", "Supplier Name", "BN"],
                kind="stable",
            ).drop(columns=["_severity_rank"])

        receiving = int(
            items["Variance Type"].isin(["Missing Registration", "Quantity Mismatch"]).sum()
        ) if not items.empty else 0
        missing_registration = int(
            items["Variance Type"].eq("Missing Registration").sum()
        ) if not items.empty else 0
        quantity_mismatch = int(
            items["Variance Type"].eq("Quantity Mismatch").sum()
        ) if not items.empty else 0
        unmapped_customers = int(
            items["Variance Type"].eq("GLN Missing").sum()
        ) if not items.empty else 0
        reportable = int(items.get("Report To SFDA", pd.Series(dtype=bool)).fillna(False).sum()) if not items.empty else 0

        return {
            "status": "Completed",
            "summary": {
                "receiving_variance": receiving,
                "missing_registration": missing_registration,
                "quantity_mismatch": quantity_mismatch,
                "dispatch_variance": 0,
                "unmapped_customers": unmapped_customers,
                "reportable_to_sfda": reportable,
                "total_items": int(len(items)),
            },
            "items": self._json_records(items),
            "metadata": {
                "sfda_snapshot_available": sfda_snapshot is not None and not sfda_snapshot.empty,
                "inventory_snapshot_available": inventory_snapshot is not None and not inventory_snapshot.empty,
            },
        }

    def report_frame(
        self,
        result: Dict[str, Any],
        selected_ids: Optional[List[str]] = None,
    ) -> pd.DataFrame:
        items = pd.DataFrame(result.get("items") or [])
        if items.empty:
            return pd.DataFrame()
        reportable = items["Variance Type"].isin(self.SFDA_REPORT_TYPES)
        items = items.loc[reportable].copy()
        if selected_ids:
            selected = {str(value) for value in selected_ids}
            items = items.loc[items["Variance ID"].astype(str).isin(selected)].copy()

        columns = [
            "Severity",
            "Variance Type",
            "Supplier Name",
            "Supplier Code",
            "GTIN",
            "Drug Name",
            "BN",
            "Expiry Date",
            "Generic Item Number",
            "Received Quantity Each",
            "Received Quantity Pack",
            "SFDA Quantity",
            "Difference Pack",
            "Variance Status",
            "Required Action",
            "Last Received Date",
        ]
        for column in columns:
            if column not in items.columns:
                items[column] = None
        return items[columns].reset_index(drop=True)
