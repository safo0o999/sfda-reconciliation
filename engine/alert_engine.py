"""
Alert Engine for Missing Batch Detection

Detects batches that exist in Batch Master and SFDA but are missing from:
- Daily ASN (Accept reconciliation)
- Daily SFDA updates (Dispatch reconciliation)
"""

import pandas as pd
from typing import List, Dict, Any


class AlertEngine:
    """Generates alerts for missing batches in daily operations."""

    def __init__(
        self,
        batch_master_df: pd.DataFrame,
        sfda_df: pd.DataFrame,
        inventory_df: pd.DataFrame | None = None,
    ):
        self.batch_master = batch_master_df.copy()
        self.sfda = sfda_df.copy()
        self.inventory = inventory_df.copy() if inventory_df is not None else None
        self.alerts = []

    def detect_missing_in_daily_asn(
        self,
        asn_daily_df: pd.DataFrame,
    ) -> List[Dict[str, Any]]:
        """
        Alert: Batch exists in Batch Master + SFDA + Inventory
        BUT missing from today's ASN receipt.
        
        Meaning: The supplier didn't send this batch today,
        even though it was received before.
        """
        
        if asn_daily_df.empty:
            return []

        alerts = []

        # Normalize keys for matching
        try:
            asn_daily_df = asn_daily_df.copy()
            self.batch_master = self.batch_master.copy()

            # Extract BN from daily ASN
            asn_bns = set(
                asn_daily_df.get("BN", asn_daily_df.get("Batch No", pd.Series()))
                .fillna("")
                .astype(str)
                .str.strip()
                .unique()
            )

            # Check each batch in Batch Master
            for idx, row in self.batch_master.iterrows():
                bn = str(row.get("BN", "")).strip()
                generic = str(row.get("Generic Item Number", "")).strip()
                generic_exists = str(row.get("Generic Exists in SFDA", "")).strip()
                trade_name = str(row.get("Trade Name", "")).strip()
                total_received = row.get("Total Receive Qty", 0)

                # Skip if not in SFDA or generic not found
                if generic_exists == "Generic Not in SFDA" or generic_exists == "Missing Batch":
                    continue

                # Skip if batch was never received
                if total_received <= 0:
                    continue

                # Check if batch is missing from today's ASN
                if bn not in asn_bns:
                    alerts.append({
                        "type": "missing_in_daily_asn",
                        "severity": "warning",
                        "batch_no": bn,
                        "generic_no": generic,
                        "trade_name": trade_name,
                        "total_received_historically": total_received,
                        "message": (
                            f"Batch {bn} (Generic {generic}) was historically received "
                            f"and exists in SFDA, but is missing from today's ASN. "
                            f"Verify with the supplier."
                        ),
                    })

        except Exception as e:
            self.alerts.append({
                "type": "error",
                "severity": "error",
                "message": f"Error detecting missing ASN batches: {str(e)}",
            })

        return alerts

    def detect_missing_in_sfda(
        self,
        sfda_updated_df: pd.DataFrame,
    ) -> List[Dict[str, Any]]:
        """
        Alert: Batch exists in Batch Master + historically received + inventory
        BUT is missing/inactive in the updated SFDA report.
        
        Meaning: SFDA removed or deactivated this batch,
        even though it was in the system before.
        """

        if sfda_updated_df.empty:
            return []

        alerts = []

        try:
            sfda_updated_df = sfda_updated_df.copy()
            self.batch_master = self.batch_master.copy()

            # Extract BN from SFDA
            sfda_bns = set(
                sfda_updated_df.get("BN", sfda_updated_df.get("Batch No", pd.Series()))
                .fillna("")
                .astype(str)
                .str.strip()
                .unique()
            )

            # Check each batch in Batch Master
            for idx, row in self.batch_master.iterrows():
                bn = str(row.get("BN", "")).strip()
                generic = str(row.get("Generic Item Number", "")).strip()
                generic_exists = str(row.get("Generic Exists in SFDA", "")).strip()
                trade_name = str(row.get("Trade Name", "")).strip()
                total_dispatched = row.get("Total Dispatched Qty", 0)

                # Skip if not marked as existing in SFDA
                if generic_exists != "Yes":
                    continue

                # Skip if batch was never dispatched
                if total_dispatched <= 0:
                    continue

                # Check if batch is missing from updated SFDA
                if bn not in sfda_bns:
                    alerts.append({
                        "type": "missing_in_sfda",
                        "severity": "critical",
                        "batch_no": bn,
                        "generic_no": generic,
                        "trade_name": trade_name,
                        "total_dispatched_historically": total_dispatched,
                        "message": (
                            f"Batch {bn} (Generic {generic}) was in SFDA and dispatched, "
                            f"but is now missing from the updated SFDA report. "
                            f"Check with SFDA authority or verify the file."
                        ),
                    })

        except Exception as e:
            self.alerts.append({
                "type": "error",
                "severity": "error",
                "message": f"Error detecting missing SFDA batches: {str(e)}",
            })

        return alerts

    def generate_alerts_for_accept(
        self,
        asn_daily_df: pd.DataFrame,
    ) -> Dict[str, Any]:
        """Generate all alerts for Accept reconciliation step."""
        
        alerts = self.detect_missing_in_daily_asn(asn_daily_df)

        return {
            "alert_count": len(alerts),
            "alerts": alerts,
            "summary": {
                "missing_asn_count": len([a for a in alerts if a["type"] == "missing_in_daily_asn"]),
                "has_warnings": any(a["severity"] == "warning" for a in alerts),
            },
        }

    def generate_alerts_for_dispatch(
        self,
        sfda_updated_df: pd.DataFrame,
    ) -> Dict[str, Any]:
        """Generate all alerts for Dispatch reconciliation step."""
        
        alerts = self.detect_missing_in_sfda(sfda_updated_df)

        return {
            "alert_count": len(alerts),
            "alerts": alerts,
            "summary": {
                "missing_sfda_count": len([a for a in alerts if a["type"] == "missing_in_sfda"]),
                "has_critical": any(a["severity"] == "critical" for a in alerts),
            },
        }
