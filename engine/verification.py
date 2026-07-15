import io
from collections import Counter, defaultdict
from typing import Dict, Iterable, List, Optional, Tuple

import pandas as pd


class VerificationEngine:
    SUCCESS_CODE = "00000"

    @staticmethod
    def _clean_text(value) -> str:
        if value is None:
            return ""

        try:
            if pd.isna(value):
                return ""
        except Exception:
            pass

        text = str(value).strip()

        if text.endswith(".0"):
            text = text[:-2]

        return text

    @staticmethod
    def _normalize_gtin(value) -> str:
        text = (
            VerificationEngine
            ._clean_text(value)
            .replace(" ", "")
        )

        return text.zfill(14) if text else ""

    @staticmethod
    def _normalize_bn(value) -> str:
        return (
            VerificationEngine
            ._clean_text(value)
            .upper()
        )

    @staticmethod
    def _normalize_date(value) -> str:
        parsed = pd.to_datetime(
            value,
            errors="coerce",
            dayfirst=True,
        )

        if pd.isna(parsed):
            return ""

        return parsed.strftime("%Y-%m-%d")

    @staticmethod
    def _normalize_quantity(value) -> float:
        text = (
            VerificationEngine
            ._clean_text(value)
            .replace(",", "")
        )

        parsed = pd.to_numeric(
            pd.Series([text]),
            errors="coerce",
        ).fillna(0).iloc[0]

        return float(parsed)

    @staticmethod
    def _key(
        gtin,
        bn,
        expiry_date,
        quantity,
    ) -> Tuple[str, str, str, float]:
        return (
            VerificationEngine._normalize_gtin(
                gtin
            ),
            VerificationEngine._normalize_bn(
                bn
            ),
            VerificationEngine._normalize_date(
                expiry_date
            ),
            round(
                VerificationEngine
                ._normalize_quantity(quantity),
                4,
            ),
        )

    @staticmethod
    def _identity_key(
        gtin,
        bn,
        expiry_date,
    ) -> Tuple[str, str, str]:
        return (
            VerificationEngine._normalize_gtin(
                gtin
            ),
            VerificationEngine._normalize_bn(
                bn
            ),
            VerificationEngine._normalize_date(
                expiry_date
            ),
        )

    @staticmethod
    def _find_column(
        dataframe,
        candidates,
    ) -> Optional[str]:
        normalized = {
            str(column).strip().lower(): column
            for column in dataframe.columns
        }

        for candidate in candidates:
            match = normalized.get(
                str(candidate).strip().lower()
            )

            if match is not None:
                return match

        return None

    @staticmethod
    def read_tabular_bytes(
        file_name: str,
        file_bytes: bytes,
    ) -> pd.DataFrame:
        extension = (
            str(file_name or "")
            .lower()
            .rsplit(".", 1)[-1]
        )

        if extension == "xlsx":
            return pd.read_excel(
                io.BytesIO(file_bytes),
                engine="openpyxl",
                dtype=object,
            )

        if extension == "xls":
            return pd.read_excel(
                io.BytesIO(file_bytes),
                engine="xlrd",
                dtype=object,
            )

        if extension == "csv":
            text = file_bytes.decode(
                "utf-8-sig",
                errors="replace",
            )

            try:
                return pd.read_csv(
                    io.StringIO(text),
                    sep=None,
                    engine="python",
                    dtype=object,
                )
            except Exception:
                return pd.read_csv(
                    io.StringIO(text),
                    sep=";",
                    header=None,
                    dtype=object,
                )

        raise ValueError(
            f"Unsupported verification file type: "
            f"{file_name}"
        )

    @staticmethod
    def parse_generated_upload_file(
        file_name: str,
        file_bytes: bytes,
        notification_type: str,
    ) -> List[Dict]:
        dataframe = (
            VerificationEngine
            .read_tabular_bytes(
                file_name,
                file_bytes,
            )
        )

        gtin_column = (
            VerificationEngine._find_column(
                dataframe,
                ["GTIN"],
            )
        )
        quantity_column = (
            VerificationEngine._find_column(
                dataframe,
                [
                    "Quantity",
                    "To Be Accept",
                    "To Be Dispatch",
                ],
            )
        )
        bn_column = (
            VerificationEngine._find_column(
                dataframe,
                ["BN", "Batch Number"],
            )
        )
        expiry_column = (
            VerificationEngine._find_column(
                dataframe,
                [
                    "Expiry Date",
                    "Expiration Date",
                ],
            )
        )

        if not all([
            gtin_column,
            quantity_column,
            bn_column,
            expiry_column,
        ]):
            if dataframe.shape[1] < 4:
                raise ValueError(
                    "Generated upload file must contain "
                    "GTIN, Quantity, BN, and Expiry Date."
                )

            dataframe = dataframe.iloc[:, :4].copy()
            dataframe.columns = [
                "GTIN",
                "Quantity",
                "BN",
                "Expiry Date",
            ]

            gtin_column = "GTIN"
            quantity_column = "Quantity"
            bn_column = "BN"
            expiry_column = "Expiry Date"

        rows = []

        for row_number, (_, row) in enumerate(
            dataframe.iterrows(),
            start=1,
        ):
            gtin = (
                VerificationEngine
                ._normalize_gtin(
                    row.get(gtin_column)
                )
            )
            bn = (
                VerificationEngine
                ._normalize_bn(
                    row.get(bn_column)
                )
            )
            expiry_date = (
                VerificationEngine
                ._normalize_date(
                    row.get(expiry_column)
                )
            )
            quantity = (
                VerificationEngine
                ._normalize_quantity(
                    row.get(quantity_column)
                )
            )

            if (
                not gtin
                or not bn
                or not expiry_date
                or quantity <= 0
            ):
                continue

            rows.append({
                "notification_type":
                    notification_type,
                "source_file": file_name,
                "source_row": row_number,
                "gtin": gtin,
                "bn": bn,
                "expiry_date": expiry_date,
                "quantity": quantity,
                "key": VerificationEngine._key(
                    gtin,
                    bn,
                    expiry_date,
                    quantity,
                ),
                "identity_key":
                    VerificationEngine
                    ._identity_key(
                        gtin,
                        bn,
                        expiry_date,
                    ),
            })

        return rows

    @staticmethod
    def parse_notification_file(
        file_name: str,
        file_bytes: bytes,
    ) -> List[Dict]:
        dataframe = (
            VerificationEngine
            .read_tabular_bytes(
                file_name,
                file_bytes,
            )
        )

        required = {
            "gtin": ["GTIN"],
            "quantity": ["Quantity"],
            "bn": ["BN", "Batch Number"],
            "expiry_date": [
                "Expiry Date",
                "Expiration Date",
            ],
            "result": ["Result"],
            "description": ["Description"],
        }

        columns = {
            key:
                VerificationEngine._find_column(
                    dataframe,
                    candidates,
                )
            for key, candidates
            in required.items()
        }

        missing = [
            key
            for key, column
            in columns.items()
            if column is None
        ]

        if missing:
            raise ValueError(
                f"Notification file {file_name} "
                f"is missing columns: {missing}"
            )

        rows = []

        for row_number, (_, row) in enumerate(
            dataframe.iterrows(),
            start=1,
        ):
            gtin = (
                VerificationEngine
                ._normalize_gtin(
                    row.get(columns["gtin"])
                )
            )
            bn = (
                VerificationEngine
                ._normalize_bn(
                    row.get(columns["bn"])
                )
            )
            expiry_date = (
                VerificationEngine
                ._normalize_date(
                    row.get(
                        columns["expiry_date"]
                    )
                )
            )
            quantity = (
                VerificationEngine
                ._normalize_quantity(
                    row.get(
                        columns["quantity"]
                    )
                )
            )
            result_code = (
                VerificationEngine
                ._clean_text(
                    row.get(columns["result"])
                )
                .zfill(5)
            )
            description = (
                VerificationEngine
                ._clean_text(
                    row.get(
                        columns["description"]
                    )
                )
            )

            if (
                not gtin
                or not bn
                or not expiry_date
            ):
                continue

            rows.append({
                "notification_file": file_name,
                "notification_row": row_number,
                "gtin": gtin,
                "bn": bn,
                "expiry_date": expiry_date,
                "quantity": quantity,
                "result_code": result_code,
                "description": description,
                "portal_success": (
                    result_code
                    == VerificationEngine
                    .SUCCESS_CODE
                ),
                "key": VerificationEngine._key(
                    gtin,
                    bn,
                    expiry_date,
                    quantity,
                ),
                "identity_key":
                    VerificationEngine
                    ._identity_key(
                        gtin,
                        bn,
                        expiry_date,
                    ),
            })

        return rows

    @staticmethod
    def parse_sfda_snapshot(
        dataframe: pd.DataFrame,
    ) -> Dict[
        Tuple[str, str, str],
        Dict
    ]:
        gtin_column = (
            VerificationEngine._find_column(
                dataframe,
                ["GTIN"],
            )
        )
        bn_column = (
            VerificationEngine._find_column(
                dataframe,
                ["BN", "Batch Number"],
            )
        )
        expiry_column = (
            VerificationEngine._find_column(
                dataframe,
                [
                    "Expiry Date",
                    "Expiration Date",
                ],
            )
        )
        active_column = (
            VerificationEngine._find_column(
                dataframe,
                ["Active"],
            )
        )

        if not all([
            gtin_column,
            bn_column,
            expiry_column,
            active_column,
        ]):
            raise ValueError(
                "SFDA report must contain GTIN, BN, "
                "Expiry Date, and Active."
            )

        snapshot = defaultdict(
            lambda: {
                "active": 0.0,
                "rows": 0,
            }
        )

        for _, row in dataframe.iterrows():
            identity_key = (
                VerificationEngine
                ._identity_key(
                    row.get(gtin_column),
                    row.get(bn_column),
                    row.get(expiry_column),
                )
            )

            if not all(identity_key):
                continue

            snapshot[identity_key][
                "active"
            ] += (
                VerificationEngine
                ._normalize_quantity(
                    row.get(active_column)
                )
            )
            snapshot[identity_key][
                "rows"
            ] += 1

        return dict(snapshot)

    @staticmethod
    def classify_notifications(
        expected_rows: List[Dict],
        notification_rows: List[Dict],
    ) -> List[Dict]:
        exact_inventory = {
            "ACCEPT": Counter(),
            "DISPATCH": Counter(),
        }
        identity_types = defaultdict(set)

        for row in expected_rows:
            notification_type = (
                str(
                    row["notification_type"]
                )
                .strip()
                .upper()
            )
            exact_inventory[
                notification_type
            ][row["key"]] += 1
            identity_types[
                row["identity_key"]
            ].add(notification_type)

        classified = []

        for notification in notification_rows:
            matching_types = [
                notification_type
                for notification_type
                in ("ACCEPT", "DISPATCH")
                if exact_inventory[
                    notification_type
                ][notification["key"]] > 0
            ]

            if len(matching_types) == 1:
                notification_type = (
                    matching_types[0]
                )
                exact_inventory[
                    notification_type
                ][notification["key"]] -= 1
                classification_status = (
                    "Exact Match"
                )

            elif len(matching_types) > 1:
                notification_type = (
                    "UNCLASSIFIED"
                )
                classification_status = (
                    "Ambiguous Exact Match"
                )

            else:
                identity_matches = list(
                    identity_types.get(
                        notification[
                            "identity_key"
                        ],
                        set(),
                    )
                )

                if len(identity_matches) == 1:
                    notification_type = (
                        identity_matches[0]
                    )
                    classification_status = (
                        "Identity Match; "
                        "Quantity Mismatch"
                    )
                elif len(identity_matches) > 1:
                    notification_type = (
                        "UNCLASSIFIED"
                    )
                    classification_status = (
                        "Ambiguous Identity Match"
                    )
                else:
                    notification_type = (
                        "UNCLASSIFIED"
                    )
                    classification_status = (
                        "No Matching Generated Row"
                    )

            result = notification.copy()
            result[
                "notification_type"
            ] = notification_type
            result[
                "classification_status"
            ] = classification_status
            classified.append(result)

        return classified

    @staticmethod
    def verify(
        expected_rows: List[Dict],
        notification_rows: List[Dict],
        original_sfda: pd.DataFrame,
        latest_sfda: pd.DataFrame,
        tolerance: float = 0.01,
    ) -> Dict:
        classified = (
            VerificationEngine
            .classify_notifications(
                expected_rows,
                notification_rows,
            )
        )

        original_snapshot = (
            VerificationEngine
            .parse_sfda_snapshot(
                original_sfda
            )
        )
        latest_snapshot = (
            VerificationEngine
            .parse_sfda_snapshot(
                latest_sfda
            )
        )

        successful_delta = defaultdict(
            lambda: {
                "accept": 0.0,
                "dispatch": 0.0,
            }
        )

        for row in classified:
            if not row["portal_success"]:
                continue

            if (
                row["notification_type"]
                == "ACCEPT"
            ):
                successful_delta[
                    row["identity_key"]
                ]["accept"] += row["quantity"]

            elif (
                row["notification_type"]
                == "DISPATCH"
            ):
                successful_delta[
                    row["identity_key"]
                ]["dispatch"] += row["quantity"]

        verification_rows = []

        for row in classified:
            identity_key = row[
                "identity_key"
            ]
            original_active = (
                original_snapshot
                .get(identity_key, {})
                .get("active", 0.0)
            )
            latest_exists = (
                identity_key
                in latest_snapshot
            )
            latest_active = (
                latest_snapshot
                .get(identity_key, {})
                .get("active", 0.0)
            )
            delta = successful_delta[
                identity_key
            ]
            expected_active = (
                original_active
                + delta["accept"]
                - delta["dispatch"]
            )
            active_matches = (
                latest_exists
                and abs(
                    latest_active
                    - expected_active
                ) <= tolerance
            )

            if (
                row["notification_type"]
                == "UNCLASSIFIED"
            ):
                verification_status = (
                    "Investigation Required"
                )
            elif not row["portal_success"]:
                verification_status = (
                    "Portal Rejected"
                )
            elif not latest_exists:
                verification_status = (
                    "Latest SFDA Batch Missing"
                )
            elif active_matches:
                verification_status = (
                    "Verified"
                )
            else:
                verification_status = (
                    "Active Quantity Mismatch"
                )

            result = row.copy()
            result.update({
                "original_active":
                    original_active,
                "latest_active":
                    latest_active,
                "expected_active":
                    expected_active,
                "active_matches":
                    active_matches,
                "verification_status":
                    verification_status,
            })
            verification_rows.append(
                result
            )

        expected_counter = Counter(
            row["key"]
            for row in expected_rows
        )
        notification_counter = Counter(
            row["key"]
            for row in classified
            if row["notification_type"]
            in {"ACCEPT", "DISPATCH"}
        )

        missing_expected_rows = sum(
            max(
                0,
                count
                - notification_counter.get(
                    key,
                    0,
                ),
            )
            for key, count
            in expected_counter.items()
        )

        rejected_rows = sum(
            1
            for row in verification_rows
            if not row["portal_success"]
        )
        unclassified_rows = sum(
            1
            for row in verification_rows
            if row["notification_type"]
            == "UNCLASSIFIED"
        )
        mismatch_rows = sum(
            1
            for row in verification_rows
            if row["verification_status"]
            not in {
                "Verified",
                "Portal Rejected",
            }
        )
        verified_rows = sum(
            1
            for row in verification_rows
            if row["verification_status"]
            == "Verified"
        )

        overall_status = (
            "Verified"
            if (
                verification_rows
                and missing_expected_rows == 0
                and rejected_rows == 0
                and unclassified_rows == 0
                and mismatch_rows == 0
            )
            else "Investigation Required"
        )

        return {
            "status": overall_status,
            "summary": {
                "expected_rows":
                    len(expected_rows),
                "notification_rows":
                    len(notification_rows),
                "verified_rows":
                    verified_rows,
                "rejected_rows":
                    rejected_rows,
                "unclassified_rows":
                    unclassified_rows,
                "mismatch_rows":
                    mismatch_rows,
                "missing_expected_rows":
                    missing_expected_rows,
            },
            "rows": verification_rows,
        }
