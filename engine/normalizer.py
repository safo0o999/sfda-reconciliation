import re
from difflib import SequenceMatcher
from functools import lru_cache

import pandas as pd


class Normalizer:
    @staticmethod
    def _find_column(df, candidates):
        normalized = {
            str(column).strip().lower(): column
            for column in df.columns
        }

        for candidate in candidates:
            match = normalized.get(
                str(candidate).strip().lower()
            )
            if match is not None:
                return match

        return None

    @staticmethod
    def _required_series(df, candidates):
        column = Normalizer._find_column(
            df,
            candidates,
        )

        if column is None:
            raise ValueError(
                f"Required source column not found. "
                f"Expected one of: {candidates}"
            )

        return df[column]

    @staticmethod
    def _optional_series(
        df,
        candidates,
        default="",
    ):
        column = Normalizer._find_column(
            df,
            candidates,
        )

        if column is None:
            return pd.Series(
                [default] * len(df),
                index=df.index,
                dtype=object,
            )

        return df[column]

    @staticmethod
    def text(series):
        return (
            series.fillna("")
            .astype(str)
            .str.strip()
            .str.upper()
        )

    @staticmethod
    def drug_name_key(series):
        """Conservative drug-name normalization for identity/package matching.

        Keeps strength/volume tokens (25 MG, 100 ML, etc.) while removing only
        formatting noise. This is deliberately safer than aggressive fuzzy
        normalization because different strengths must remain distinguishable.
        """
        result = Normalizer.text(series)
        result = result.str.replace(r"(?<=\d)\s*(MG|MCG|UG|G|ML|L|IU|UNIT|UNITS)\b", r" \1", regex=True)
        result = result.str.replace(r"[^A-Z0-9]+", " ", regex=True)
        result = result.str.replace(r"\s+", " ", regex=True).str.strip()
        return result

    @staticmethod
    @lru_cache(maxsize=20000)
    def _drug_key_scalar(value):
        # Scalar normalization is used repeatedly during Historical matching.
        # Cache by input text so the same SFDA/WMS drug name is normalized once
        # per worker process rather than once per historical row.
        value = "" if value is None else str(value)
        return str(Normalizer.drug_name_key(pd.Series([value], dtype=object)).iloc[0])

    @staticmethod
    def _strength_tokens(value):
        key = Normalizer._drug_key_scalar(value)
        return set(re.findall(r"\b\d+(?:\.\d+)?\s*(?:MG|MCG|UG|G|ML|L|IU|UNIT|UNITS)\b", key))

    @staticmethod
    def drug_name_match_score(sfda_name, wms_trade_description):
        """Return a conservative 0..100 identity score for SFDA vs WMS names.

        Exact/contained normalized drug names score highest. Conflicting explicit
        strength/volume tokens are heavily penalized so RISPERDAL 25 MG cannot
        silently select RISPERDAL 50 MG.
        """
        sfda = Normalizer._drug_key_scalar(sfda_name)
        wms = Normalizer._drug_key_scalar(wms_trade_description)
        if not sfda or not wms:
            return 0.0

        sfda_strength = Normalizer._strength_tokens(sfda)
        wms_strength = Normalizer._strength_tokens(wms)
        if sfda_strength and wms_strength and sfda_strength.isdisjoint(wms_strength):
            return 0.0

        if sfda == wms:
            return 100.0
        if sfda in wms:
            return 98.0

        sfda_tokens = sfda.split()
        wms_tokens = set(wms.split())
        token_coverage = (sum(1 for token in sfda_tokens if token in wms_tokens) / max(1, len(sfda_tokens)))
        sequence = SequenceMatcher(None, sfda, wms).ratio()
        return round(100.0 * (0.72 * token_coverage + 0.28 * sequence), 2)

    @staticmethod
    def drug_name_validation_pass(sfda_name, wms_trade_description, threshold=60.0):
        """Validation gate AFTER BN+Expiry matching.

        BN + Expiry remains the regulatory matching key. This check does not
        discover or choose a Generic; it only rejects an exact batch match when
        the SFDA Drug Name and the WMS Trade Description are clearly unrelated.
        Explicit strength/volume conflicts already score zero in
        drug_name_match_score().
        """
        return Normalizer.drug_name_match_score(sfda_name, wms_trade_description) >= float(threshold)

    @staticmethod
    def identifier(series, length=None):
        result = (
            series.fillna("")
            .astype(str)
            .str.strip()
            .str.replace(r"\.0$", "", regex=True)
            .str.replace(r"\s+", "", regex=True)
        )

        if length is not None:
            result = result.str.zfill(length)

        return result

    @staticmethod
    def _safe_datetime(
        series,
        formats=None,
        fallback_dayfirst=True,
    ):
        """Parse dates safely while respecting each report's source format.

        Values already loaded by Excel as real date/datetime objects are
        preserved. Text values are first parsed using the supplied explicit
        formats, then a conservative mixed-format fallback is applied only to
        values that remain unresolved.
        """

        cleaned = series.copy()

        text_values = (
            cleaned.fillna("")
            .astype(str)
            .str.strip()
        )

        invalid_date_mask = (
            text_values.str.match(
                r"^9999[-/]",
                na=False,
            )
            | text_values.str.contains(
                r"[-/]9999(?:\s|$)",
                regex=True,
                na=False,
            )
        )

        cleaned = cleaned.mask(
            invalid_date_mask,
            None,
        )

        # Preserve values already interpreted by Excel/pandas as datetimes.
        result = pd.to_datetime(
            cleaned.where(
                cleaned.map(
                    lambda value: isinstance(
                        value,
                        (
                            pd.Timestamp,
                            __import__("datetime").datetime,
                            __import__("datetime").date,
                        ),
                    )
                )
            ),
            errors="coerce",
        )

        unresolved = result.isna() & cleaned.notna()

        for date_format in formats or []:
            if not unresolved.any():
                break

            parsed = pd.to_datetime(
                cleaned.where(unresolved),
                errors="coerce",
                format=date_format,
            )
            result = result.fillna(parsed)
            unresolved = result.isna() & cleaned.notna()

        if unresolved.any():
            parsed = pd.to_datetime(
                cleaned.where(unresolved),
                errors="coerce",
                dayfirst=fallback_dayfirst,
                format="mixed",
            )
            result = result.fillna(parsed)

        result = result.mask(
            (result > pd.Timestamp.max)
            | (result < pd.Timestamp.min),
            pd.NaT,
        )

        return result.astype(
            "datetime64[ns]"
        )

    @staticmethod
    def date(
        series,
        formats=None,
        fallback_dayfirst=True,
    ):
        return (
            Normalizer._safe_datetime(
                series,
                formats=formats,
                fallback_dayfirst=fallback_dayfirst,
            )
            .dt.normalize()
        )

    @staticmethod
    def datetime(
        series,
        formats=None,
        fallback_dayfirst=True,
    ):
        return Normalizer._safe_datetime(
            series,
            formats=formats,
            fallback_dayfirst=fallback_dayfirst,
        )

    @staticmethod
    def number(series):
        return (
            pd.to_numeric(
                series,
                errors="coerce",
            )
            .fillna(0)
        )

    @staticmethod
    def normalize_asn(df):
        df = df.copy()

        df["BN"] = Normalizer.text(
            Normalizer._required_series(
                df,
                ["Batch Number", "BN", "Batch/Lot"],
            )
        )
        df["Expiry Date"] = Normalizer.date(
            Normalizer._required_series(
                df,
                ["Expiration Date", "Expiry Date", "Best Before Date"],
            ),
            formats=[
                "%d/%m/%Y",
                "%d/%m/%Y %H:%M:%S",
            ],
            fallback_dayfirst=True,
        )
        df["Received Quantity"] = Normalizer.number(
            Normalizer._required_series(
                df,
                ["Received Qty", "Received Quantity"],
            )
        )
        df["Trade Name"] = Normalizer.text(
            Normalizer._required_series(
                df,
                ["Trade Description", "Trade Name"],
            )
        )
        df["Trade Item"] = Normalizer.identifier(
            Normalizer._optional_series(
                df,
                ["Trade Item", "Trade Item Number"],
            )
        )
        df["Inbound Shipment"] = Normalizer.identifier(
            Normalizer._optional_series(
                df,
                ["Inbound Shipment", "Shipment Reference"],
            )
        )
        df["ASN Line"] = Normalizer.identifier(
            Normalizer._optional_series(
                df,
                ["ASN Line", "Line Number"],
            )
        )
        df["Supplier Name"] = Normalizer.text(
            Normalizer._optional_series(
                df,
                ["Supplier Name", "Vendor Name"],
            )
        )
        df["Supplier Code"] = Normalizer.identifier(
            Normalizer._optional_series(
                df,
                ["Supplier Code", "Vendor Code"],
            )
        )
        df["Description"] = Normalizer.text(
            Normalizer._optional_series(
                df,
                [
                    "Description",
                    "Item Description",
                    "Generic Description",
                ],
            )
        )
        df["Item Family Group"] = Normalizer.text(
            Normalizer._optional_series(
                df,
                [
                    "Item Family Group",
                    "Item Family",
                    "Family Group",
                ],
            )
        )
        df["Received Date"] = Normalizer.datetime(
            Normalizer._optional_series(
                df,
                [
                    "Received Date",
                    "Receipt Date",
                    "Actual Receipt Date",
                    "Date Received",
                    "ASN Closed Date",
                    "Closed Date",
                    "Transaction Date",
                ],
            )
        )
        df["Generic Item Number"] = Normalizer.identifier(
            Normalizer._optional_series(
                df,
                [
                    "Generic Item Number",
                    "Item Number",
                    "Generic Number",
                ],
            )
        )

        return df

    @staticmethod
    def normalize_inventory(df):
        df = df.copy()

        df["BN"] = Normalizer.text(
            Normalizer._required_series(
                df,
                ["Lot No/Batch", "BN", "Batch Number"],
            )
        )
        df["Expiry Date"] = Normalizer.date(
            Normalizer._required_series(
                df,
                ["Expiry Date", "Expiration Date"],
            )
        )
        df["Available Quantity"] = Normalizer.number(
            Normalizer._required_series(
                df,
                ["Available Qty", "Available Quantity"],
            )
        )
        df["Trade Name"] = Normalizer.text(
            Normalizer._required_series(
                df,
                ["Trade Item Description", "Trade Name", "Trade Description"],
            )
        )
        df["Generic Item Number"] = Normalizer.identifier(
            Normalizer._optional_series(
                df,
                ["Generic Item Number", "Item Number"],
            )
        )
        df["Trade Item Number"] = Normalizer.identifier(
            Normalizer._optional_series(
                df,
                ["Trade Item Number", "Trade Item"],
            )
        )

        return df

    @staticmethod
    def normalize_dispatch(df):
        df = df.copy()

        df["BN"] = Normalizer.text(
            Normalizer._required_series(
                df,
                ["Batch/Lot", "BN", "Batch Number"],
            )
        )
        # Full Dispatch expiry is sourced primarily from "Best Before Date".
        # The fallback names are retained only for backward compatibility.
        df["Expiry Date"] = Normalizer.date(
            Normalizer._required_series(
                df,
                ["Best Before Date", "Expiry Date", "Expiration Date"],
            ),
            formats=[
                "%Y-%m-%d",
                "%Y-%m-%d %H:%M:%S",
            ],
            fallback_dayfirst=True,
        )
        df["Dispatched Quantity"] = Normalizer.number(
            Normalizer._required_series(
                df,
                ["Pick Qty", "Dispatched Quantity", "Dispatch Qty"],
            )
        )
        df["Trade Name"] = Normalizer.text(
            Normalizer._required_series(
                df,
                ["Trade Description", "Trade Name"],
            )
        )
        df["Trade Item Number"] = Normalizer.identifier(
            Normalizer._optional_series(
                df,
                ["Trade Item Number", "Trade Item"],
            )
        )
        df["To Address"] = Normalizer.text(
            Normalizer._required_series(
                df,
                ["To Address", "Customer Name"],
            )
        )
        df["Sales Order Number"] = Normalizer.identifier(
            Normalizer._required_series(
                df,
                ["Sales Order Number", "Sales Order"],
            )
        )
        df["Order Line"] = Normalizer.identifier(
            Normalizer._optional_series(
                df,
                ["order line", "Order Line", "Line Number"],
            )
        )
        df["Dispatch Date"] = Normalizer.datetime(
            Normalizer._optional_series(
                df,
                [
                    "Confirm Date",
                    "Confirmed Date",
                    "Confirmation Date",
                    "Dispatched Date",
                    "Dispatch Date",
                    "Actual Dispatch Date",
                    "Ship Date",
                    "Shipment Date",
                    "Date Dispatched",
                    "Transaction Date",
                ],
            )
        )
        df["Generic Item Number"] = Normalizer.identifier(
            Normalizer._optional_series(
                df,
                ["Generic Item Number", "Item Number"],
            )
        )
        df["Custody"] = Normalizer.text(
            Normalizer._optional_series(df, ["Custody"])
        )

        return df

    @staticmethod
    def normalize_sfda(df):
        df = df.copy()

        df["GTIN"] = Normalizer.identifier(
            Normalizer._required_series(
                df,
                ["GTIN"],
            ),
            length=14,
        )
        df["BN"] = Normalizer.text(
            Normalizer._required_series(
                df,
                ["BN", "Batch Number"],
            )
        )
        df["Expiry Date"] = Normalizer.date(
            Normalizer._required_series(
                df,
                ["Expiry Date", "Expiration Date"],
            ),
            formats=[
                "%d-%m-%Y",
                "%d-%m-%Y %H:%M:%S",
            ],
            fallback_dayfirst=True,
        )
        df["Drug Name"] = Normalizer.text(
            Normalizer._required_series(
                df,
                ["Drug Name"],
            )
        )

        for target, candidates in {
            "Quantity": ["Quantity"],
            "Active": ["Active"],
            "Quantity Receive Pending": [
                "Quantity Receive Pending",
                "Quantity ReceivePending",
            ],
            "Quantity sent pending": [
                "Quantity sent pending",
                "Quantity Send Pending",
            ],
        }.items():
            df[target] = Normalizer.number(
                Normalizer._required_series(
                    df,
                    candidates,
                )
            )

        return df

    @staticmethod
    def normalize_packsize(df):
        df = df.copy()

        df["Trade Name"] = Normalizer.text(
            Normalizer._required_series(
                df,
                ["Trade Name"],
            )
        )
        df["PackageSize"] = Normalizer.number(
            Normalizer._required_series(
                df,
                ["PackageSize", "Package Size"],
            )
        )
        # Keep the disambiguation fields from config/pack_size.xlsx.
        # Historical Pack Size resolution uses PharmaceuticalForm and, when
        # available, Size/SizeUnit/PackageTypes against the WMS Trade text.
        df["PharmaceuticalForm"] = Normalizer.text(
            Normalizer._optional_series(df, ["PharmaceuticalForm", "Pharmaceutical Form"])
        )
        df["Size"] = Normalizer.number(
            Normalizer._optional_series(df, ["Size"], default="")
        )
        df["SizeUnit"] = Normalizer.text(
            Normalizer._optional_series(df, ["SizeUnit", "Size Unit"])
        )
        df["PackageTypes"] = Normalizer.text(
            Normalizer._optional_series(df, ["PackageTypes", "Package Types"])
        )

        return df

    @staticmethod
    def normalize_gln(df):
        df = df.copy()

        df["To Address"] = Normalizer.text(
            Normalizer._required_series(
                df,
                ["To Address"],
            )
        )
        df["GLN"] = Normalizer.identifier(
            Normalizer._required_series(
                df,
                ["GLN"],
            )
        )

        df = df[
            (df["To Address"] != "")
            & (df["GLN"] != "")
        ].copy()

        return (
            df.drop_duplicates(
                subset=["To Address"],
                keep="first",
            )
            .reset_index(drop=True)
        )
