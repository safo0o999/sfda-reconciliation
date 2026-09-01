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
        # Keep it equivalent to drug_name_key(), but avoid constructing a pandas
        # Series for every scalar call. This materially reduces product-identity
        # setup time on the 13k-row Pack Size reference.
        text = "" if value is None else str(value).strip().upper()
        text = re.sub(
            r"(?<=\d)\s*(MG|MCG|UG|G|ML|L|IU|UNIT|UNITS)\b",
            r" \1",
            text,
        )
        text = re.sub(r"[^A-Z0-9]+", " ", text)
        return re.sub(r"\s+", " ", text).strip()

    _IDENTITY_STOP_TOKENS = {
        "TABLET", "TABLETS", "TAB", "TABS", "CAPSULE", "CAPSULES", "CAP", "CAPS",
        "SYRUP", "SYP", "SUSPENSION", "SUSP", "SOLUTION", "SOLN", "SOL",
        "INJECTION", "INJ", "INFUSION", "INF", "VIAL", "VIALS", "AMPOULE",
        "AMPOULES", "AMP", "POWDER", "SOLVENT", "FILM", "COATED", "MODIFIED",
        "RELEASE", "DISPERSABLE", "DISPERSIBLE", "ORAL", "EYE", "EAR", "DROPS",
        "DROP", "CREAM", "GEL", "OINTMENT", "SPRAY", "INHALER", "NASAL", "DOSE",
        "IV", "IM", "USE", "WATER", "FOR", "AND", "WITH", "OF", "THE", "USP",
        "BP", "BOTTLE", "BAG", "PHARMA", "PHARMACEUTICAL", "PHARMACEUTICALS",
        "JPI", "PSI", "JPM", "EIPICO", "TABUK", "JAMJOOM", "DALLAH", "SANOFI",
        "GLAXO", "SMITH", "KLINE", "SAUDI", "AMMAN", "MEDICAL", "UNION",
        "GLOBALPHARMA", "CIPLA", "AJA", "JULPHAR", "BATTERJEE", "JAZEERA",
        "BAXTER", "EFISA", "JUNIOR", "MG", "MCG", "UG", "G", "GM", "KG",
        "ML", "L", "IU", "UNIT", "UNITS", "PFU",
    }

    _IDENTITY_TOKEN_ALIASES = {
        "SOD": "SODIUM",
        "CHLOR": "CHLORIDE",
        "CHL": "CHLORIDE",
        "RINGERS": "RINGER",
        "LACTATED": "LACTATE",
        "NSS": "SALINE",
    }

    _COMMON_CHEMICAL_IDENTITY_TOKENS = {
        "SODIUM", "CHLORIDE", "POTASSIUM", "CALCIUM", "LACTATE", "DEXTROSE",
        "HYDROCHLORIDE", "ACID", "BICARBONATE",
    }

    @staticmethod
    def _strength_signature(value):
        """Return comparable strength/volume tokens without destroying decimals.

        The previous implementation normalized punctuation before parsing numeric
        strength.  That could turn ``1.5 MG`` into ``1 5 MG`` and incorrectly
        compare it with ``15MG``.  Parsing the raw text first preserves decimals
        and treats G/GM as the same mass unit.
        """
        raw = "" if value is None else str(value).upper().replace(",", ".")
        signature = {
            "mass_mg": set(),
            "volume_ml": set(),
            "activity_iu": set(),
            "percent": set(),
        }
        pattern = re.compile(
            r"(?<![A-Z0-9])(\d+(?:\.\d+)?)\s*(MCG|UG|MG|GM|G|KG|ML|L|IU|UNIT|UNITS|%)(?![A-Z])"
        )
        for number_text, unit in pattern.findall(raw):
            try:
                number = float(number_text)
            except (TypeError, ValueError):
                continue
            unit = unit.upper()
            if unit in {"MCG", "UG"}:
                signature["mass_mg"].add(round(number / 1000.0, 9))
            elif unit == "MG":
                signature["mass_mg"].add(round(number, 9))
            elif unit in {"G", "GM"}:
                signature["mass_mg"].add(round(number * 1000.0, 9))
            elif unit == "KG":
                signature["mass_mg"].add(round(number * 1_000_000.0, 9))
            elif unit == "ML":
                signature["volume_ml"].add(round(number, 9))
            elif unit == "L":
                signature["volume_ml"].add(round(number * 1000.0, 9))
            elif unit in {"IU", "UNIT", "UNITS"}:
                signature["activity_iu"].add(round(number, 9))
            elif unit == "%":
                signature["percent"].add(round(number, 9))
        return signature

    @staticmethod
    def _strength_tokens(value):
        signature = Normalizer._strength_signature(value)
        tokens = set()
        for dimension, values in signature.items():
            for number in values:
                tokens.add((dimension, number))
        return tokens

    @staticmethod
    def _has_conflicting_strength(sfda_name, wms_trade_description):
        sfda = Normalizer._strength_signature(sfda_name)
        wms = Normalizer._strength_signature(wms_trade_description)
        for dimension in sfda:
            left = sfda[dimension]
            right = wms[dimension]
            if left and right and left.isdisjoint(right):
                return True
        return False

    @staticmethod
    @lru_cache(maxsize=20000)
    def drug_identity_tokens(value):
        """Return stable product-identity tokens with manufacturer/form noise removed."""
        key = Normalizer._drug_key_scalar(value)
        result = []
        for token in re.findall(r"[A-Z][A-Z0-9]*", key):
            token = Normalizer._IDENTITY_TOKEN_ALIASES.get(token, token)
            if len(token) < 4 or token in Normalizer._IDENTITY_STOP_TOKENS:
                continue
            if token.isdigit():
                continue
            result.append(token)
        return tuple(result)

    @staticmethod
    def _is_distinctive_identity_token(token):
        token = str(token or "").upper()
        return bool(token) and token not in Normalizer._COMMON_CHEMICAL_IDENTITY_TOKENS

    @staticmethod
    def _has_shared_identity_token(sfda_name, wms_trade_description):
        left = Normalizer.drug_identity_tokens(sfda_name)
        right = Normalizer.drug_identity_tokens(wms_trade_description)
        if not left or not right:
            return False

        strong_pairs = []
        for a in left:
            for b in right:
                ratio = SequenceMatcher(None, a, b).ratio()
                if ratio >= 0.86:
                    strong_pairs.append((a, b, ratio))

        if not strong_pairs:
            return False

        # One strong brand/product token is enough.  For generic chemical words
        # (e.g. SODIUM/CHLORIDE), require at least two matching tokens to avoid
        # accepting unrelated products merely because they share one salt word.
        if any(
            Normalizer._is_distinctive_identity_token(a)
            or Normalizer._is_distinctive_identity_token(b)
            for a, b, _ in strong_pairs
        ):
            return True

        matched_left = {a for a, _, _ in strong_pairs}
        matched_right = {b for _, b, _ in strong_pairs}
        return min(len(matched_left), len(matched_right)) >= 2

    @staticmethod
    def drug_name_match_score(sfda_name, wms_trade_description):
        """Return a diagnostic 0..100 similarity score.

        The score is retained for logging/diagnostics, but it is no longer the
        sole hard gate for an exact BN + Expiry Month match.  Exact regulatory
        matches are rejected only when product-identity checks show a clear
        conflict.
        """
        sfda = Normalizer._drug_key_scalar(sfda_name)
        wms = Normalizer._drug_key_scalar(wms_trade_description)
        if not sfda or not wms:
            return 0.0

        if Normalizer._has_conflicting_strength(sfda_name, wms_trade_description):
            return 0.0

        if sfda == wms:
            return 100.0
        if sfda in wms or wms in sfda:
            return 98.0

        sfda_tokens = sfda.split()
        wms_tokens = set(wms.split())
        token_coverage = (
            sum(1 for token in sfda_tokens if token in wms_tokens)
            / max(1, len(sfda_tokens))
        )
        sequence = SequenceMatcher(None, sfda, wms).ratio()
        return round(100.0 * (0.72 * token_coverage + 0.28 * sequence), 2)

    @staticmethod
    def drug_name_validation_pass(
        sfda_name,
        wms_trade_description,
        threshold=60.0,
        reference_match=None,
    ):
        """Safety validation AFTER an exact BN + Expiry Month match.

        BN + Expiry Month remains the regulatory key.  This method is intentionally
        a *conflict detector*, not a fuzzy discovery engine:

        - missing/abbreviated WMS descriptions do not invalidate an exact key;
        - explicit conflicting strengths reject the candidate;
        - a trusted product-master/scientific-name bridge accepts known aliases;
        - a shared brand/product token accepts common WMS/SFDA naming differences;
        - the legacy score is retained only as a conservative final fallback.
        """
        sfda = Normalizer._drug_key_scalar(sfda_name)
        wms = Normalizer._drug_key_scalar(wms_trade_description)

        # No text evidence means there is nothing that can contradict the exact
        # BN+expiry regulatory key.  Do not silently drop the batch.
        if not sfda or not wms:
            return True

        if Normalizer._has_conflicting_strength(sfda_name, wms_trade_description):
            return False

        if sfda == wms or sfda in wms or wms in sfda:
            return True

        if reference_match is True:
            return True

        if Normalizer._has_shared_identity_token(sfda_name, wms_trade_description):
            return True

        if Normalizer.drug_name_match_score(sfda_name, wms_trade_description) >= float(threshold):
            return True

        # If the product master positively resolved both sides to different
        # scientific identities, or if no positive identity evidence exists,
        # treat the names as a conflict rather than weakening the threshold.
        return False

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
