import re
from difflib import SequenceMatcher
from functools import lru_cache

import pandas as pd


# Historical matching deployment signature. This value is written into every
# completed Historical Rebuild/Append SummaryJson so production SQL can prove
# exactly which matching logic the Azure worker executed.
HISTORICAL_MATCH_LOGIC_VERSION = "SFDA_IDENTITY_V4_CONCENTRATION_20260901"
HISTORICAL_MATCH_LEGACY_THRESHOLD = 60.0


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
        "DOSES", "VAGINAL", "OVULE", "OVULES", "UNIT",
        "IV", "IM", "USE", "WATER", "FOR", "AND", "WITH", "OF", "THE", "USP",
        "BP", "BOTTLE", "BAG", "PHARMA", "PHARMACEUTICAL", "PHARMACEUTICALS",
        "JPI", "PSI", "JPM", "EIPICO", "TABUK", "JAMJOOM", "DALLAH", "SANOFI",
        "GLAXO", "SMITH", "KLINE", "SAUDI", "AMMAN", "MEDICAL", "UNION",
        "GLOBALPHARMA", "CIPLA", "AJA", "JULPHAR", "BATTERJEE", "JAZEERA",
        "SANDOZ", "PFIZER", "MERCK", "SERONO", "GENZYME", "ABBOTT", "ARNET",
        "AGUETTANT", "MEDAC", "ASPEN", "BIOCON", "EBEWE", "ALTHEA", "UNITED",
        "SUDAIR", "REDDYS", "VIFOR", "HAUPT",
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
        "TAZO": "TAZOPACTAM",
    }

    _COMMON_CHEMICAL_IDENTITY_TOKENS = {
        "SODIUM", "CHLORIDE", "POTASSIUM", "CALCIUM", "LACTATE", "DEXTROSE",
        "HYDROCHLORIDE", "ACID", "BICARBONATE",
    }

    @staticmethod
    def _mass_to_mg(number, unit):
        unit = str(unit or "").upper()
        value = float(number)
        if unit in {"MCG", "UG"}:
            return value / 1000.0
        if unit == "MG":
            return value
        if unit in {"G", "GM"}:
            return value * 1000.0
        if unit == "KG":
            return value * 1_000_000.0
        raise ValueError(f"Unsupported mass unit: {unit}")

    @staticmethod
    def _volume_to_ml(number, unit):
        unit = str(unit or "").upper()
        value = float(number)
        if unit == "ML":
            return value
        if unit == "L":
            return value * 1000.0
        raise ValueError(f"Unsupported volume unit: {unit}")

    @staticmethod
    def _strength_signature(value):
        """Return normalized standalone strength tokens.

        Volume tokens are retained for diagnostics/package-size logic, but V4 no
        longer treats different standalone volumes as a drug-strength conflict.
        A 100 ML bottle and a 5 ML concentration denominator are different
        concepts and must not make an otherwise exact SFDA batch disappear.
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
            if unit in {"MCG", "UG", "MG", "G", "GM", "KG"}:
                signature["mass_mg"].add(round(Normalizer._mass_to_mg(number, unit), 9))
            elif unit in {"ML", "L"}:
                signature["volume_ml"].add(round(Normalizer._volume_to_ml(number, unit), 9))
            elif unit in {"IU", "UNIT", "UNITS"}:
                signature["activity_iu"].add(round(number, 9))
            elif unit == "%":
                signature["percent"].add(round(number, 9))
        return signature

    @staticmethod
    def _mass_measurements(value):
        raw = "" if value is None else str(value).upper().replace(",", ".")
        pattern = re.compile(
            r"(?<![A-Z0-9])(\d+(?:\.\d+)?)\s*(MCG|UG|MG|GM|G|KG)(?![A-Z])"
        )
        result = []
        for number_text, unit in pattern.findall(raw):
            try:
                result.append(
                    (
                        round(Normalizer._mass_to_mg(float(number_text), unit), 9),
                        number_text,
                        unit.upper(),
                    )
                )
            except (TypeError, ValueError):
                continue
        return result

    @staticmethod
    def _concentration_signature(value):
        """Return mass concentration values normalized to mg/ml.

        Supports the formats that occur in SFDA/WMS names, including:
        ``0.1MG/ML``, ``1MG/10ML``, ``20MG PER 2ML`` and ``500MG-10ML``.
        """
        raw = "" if value is None else str(value).upper().replace(",", ".")
        raw = re.sub(r"\bPER\b", "/", raw)
        pattern = re.compile(
            r"(?<![A-Z0-9])"
            r"(\d+(?:\.\d+)?)\s*(MCG|UG|MG|GM|G|KG)"
            r"\s*(?:/|-)\s*"
            r"(?:(\d+(?:\.\d+)?)\s*)?(ML|L)\b"
        )
        values = set()
        for mass_text, mass_unit, volume_text, volume_unit in pattern.findall(raw):
            try:
                mass_mg = Normalizer._mass_to_mg(float(mass_text), mass_unit)
                volume_ml = Normalizer._volume_to_ml(float(volume_text or "1"), volume_unit)
                if volume_ml > 0:
                    values.add(round(mass_mg / volume_ml, 9))
            except (TypeError, ValueError, ZeroDivisionError):
                continue
        return values

    @staticmethod
    def _close_numeric(left, right, rel_tol=0.015, abs_tol=1e-9):
        left = float(left)
        right = float(right)
        return abs(left - right) <= max(abs_tol, rel_tol * max(abs(left), abs(right), 1e-9))

    @staticmethod
    def _decimal_loss_equivalent(sfda_name, wms_trade_description):
        """Detect the WMS convention where decimal points disappear in strength.

        Examples observed in production include 0.25MG -> 025MG, 12.5MG ->
        125MG, 8.4G -> 84G and 16.8G -> 168G.  This repair is *never* used by
        itself; callers require positive product-identity evidence first.
        """
        left = Normalizer._mass_measurements(sfda_name)
        right = Normalizer._mass_measurements(wms_trade_description)
        if not left or not right:
            return False

        def digits(number_text):
            raw = str(number_text or "").strip()
            compact = raw.replace(".", "").lstrip("0")
            return compact or "0"

        for _, left_raw, left_unit in left:
            for _, right_raw, right_unit in right:
                if left_unit != right_unit:
                    continue
                ld = digits(left_raw)
                rd = digits(right_raw)
                if ld == rd:
                    if "." in left_raw or "." in right_raw or left_raw.startswith("0") or right_raw.startswith("0"):
                        return True
                if (ld + "0" == rd or rd + "0" == ld) and ("." in left_raw or "." in right_raw):
                    return True
        return False

    @staticmethod
    @lru_cache(maxsize=20000)
    def compact_identity_key(value):
        """Compact brand/product identity, ignoring separators and form noise."""
        key = Normalizer._drug_key_scalar(value)
        parts = []
        for token in re.findall(r"[A-Z][A-Z0-9]*", key):
            token = Normalizer._IDENTITY_TOKEN_ALIASES.get(token, token)
            if token in Normalizer._IDENTITY_STOP_TOKENS:
                continue
            if token.isdigit() or re.fullmatch(r"\d+(?:MG|MCG|UG|G|GM|ML|L)?", token):
                continue
            if len(token) < 3:
                continue
            parts.append(token)
        return "".join(parts)

    @staticmethod
    def _strength_tokens(value):
        signature = Normalizer._strength_signature(value)
        tokens = set()
        for dimension, values in signature.items():
            for number in values:
                tokens.add((dimension, number))
        return tokens

    @staticmethod
    def _shared_distinctive_identity_count(sfda_name, wms_trade_description):
        left = set(Normalizer.drug_identity_tokens(sfda_name))
        right = set(Normalizer.drug_identity_tokens(wms_trade_description))
        return len(
            {
                token
                for token in left.intersection(right)
                if Normalizer._is_distinctive_identity_token(token)
            }
        )

    @staticmethod
    def _strength_relation(sfda_name, wms_trade_description, product_evidence=False):
        """Return ``match``, ``conflict`` or ``unknown`` for dosage evidence.

        V4 distinguishes concentration from package volume.  It also recognizes
        mathematically equivalent concentration representations and, only when
        product identity is already proven, known decimal-loss formatting.
        """
        sfda_conc = Normalizer._concentration_signature(sfda_name)
        wms_conc = Normalizer._concentration_signature(wms_trade_description)

        if sfda_conc and wms_conc:
            if any(Normalizer._close_numeric(a, b) for a in sfda_conc for b in wms_conc):
                return "match"
            return "conflict"

        sfda_mass = {value for value, _, _ in Normalizer._mass_measurements(sfda_name)}
        wms_mass = {value for value, _, _ in Normalizer._mass_measurements(wms_trade_description)}

        if sfda_mass and wms_mass and any(
            Normalizer._close_numeric(a, b)
            for a in sfda_mass
            for b in wms_mass
        ):
            return "match"

        # One side may state a concentration while the other abbreviates it to
        # the per-ml mass (e.g. SURLEX 200MG/2ML vs SURLEX 100MG).
        if product_evidence:
            if sfda_conc and wms_mass and any(
                Normalizer._close_numeric(c, m)
                for c in sfda_conc
                for m in wms_mass
            ):
                return "match"
            if wms_conc and sfda_mass and any(
                Normalizer._close_numeric(c, m)
                for c in wms_conc
                for m in sfda_mass
            ):
                return "match"

            if Normalizer._decimal_loss_equivalent(sfda_name, wms_trade_description):
                return "match"

            # Combination drugs sometimes show total mass in SFDA but only the
            # primary component mass in WMS (e.g. piperacillin/tazobactam
            # 2.25GM vs piperacillin 2G + TAZO).  Two shared distinctive
            # ingredient identities plus a trusted product identity are enough
            # to avoid declaring a false strength conflict.
            if Normalizer._shared_distinctive_identity_count(sfda_name, wms_trade_description) >= 2:
                return "unknown"

        # Standalone package volume is intentionally not compared here.
        if sfda_mass and wms_mass:
            return "conflict"

        sfda = Normalizer._strength_signature(sfda_name)
        wms = Normalizer._strength_signature(wms_trade_description)
        for dimension in ("activity_iu", "percent"):
            left = sfda[dimension]
            right = wms[dimension]
            if left and right:
                if any(Normalizer._close_numeric(a, b) for a in left for b in right):
                    continue
                return "conflict"
        return "unknown"

    @staticmethod
    def _has_conflicting_strength(sfda_name, wms_trade_description, product_evidence=False):
        return Normalizer._strength_relation(
            sfda_name,
            wms_trade_description,
            product_evidence=product_evidence,
        ) == "conflict"

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
        left_compact = Normalizer.compact_identity_key(sfda_name)
        right_compact = Normalizer.compact_identity_key(wms_trade_description)
        if left_compact and right_compact and min(len(left_compact), len(right_compact)) >= 6:
            if left_compact in right_compact or right_compact in left_compact:
                return True

        left = Normalizer.drug_identity_tokens(sfda_name)
        right = Normalizer.drug_identity_tokens(wms_trade_description)
        if not left or not right:
            return False

        strong_pairs = []
        for a in left:
            for b in right:
                ratio = SequenceMatcher(None, a, b).ratio()
                # 0.84 deliberately captures small brand spelling variants such
                # as BIOVITIN/BIOTIN while still requiring a distinctive token.
                if ratio >= 0.84:
                    strong_pairs.append((a, b, ratio))

        if not strong_pairs:
            return False

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
        threshold=HISTORICAL_MATCH_LEGACY_THRESHOLD,
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

        shared_identity = Normalizer._has_shared_identity_token(
            sfda_name,
            wms_trade_description,
        )
        product_evidence = bool(reference_match is True or shared_identity)
        strength_relation = Normalizer._strength_relation(
            sfda_name,
            wms_trade_description,
            product_evidence=product_evidence,
        )

        if strength_relation == "conflict":
            return False

        if sfda == wms or sfda in wms or wms in sfda:
            return True

        if reference_match is True:
            return True

        if shared_identity:
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
