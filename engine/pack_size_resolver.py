from __future__ import annotations

import re
from functools import lru_cache
from pathlib import Path
from typing import Any

import pandas as pd

from engine.normalizer import Normalizer


class PackSizeResolver:
    """Resolve PackageSize from the configured product master plus WMS text.

    Trade Name is the primary product identity. When one Trade Name has more
    than one package configuration, PharmaceuticalForm and explicit WMS pack
    count/size tokens are used as disambiguators. If more than one positive
    PackageSize still remains, the FIRST positive PackageSize in config file
    order is used. If no Pack Size master match exists, PackageSize defaults
    to 1. This guarantees operational outputs never use zero as PackageSize.
    """

    _FORM_ALIASES = {
        "FILM COATED TABLET": ("FCT", "FC TAB", "FILM COATED TAB", "FILM COATED TABLET", "TABLET", " TAB "),
        "CAPLET": ("CAPLET", "CAPLETS"),
        "TABLET": ("TABLET", "TABLETS", " TAB ", " TABS "),
        "CAPSULE HARD": ("CAPSULE", "CAPSULES", " CAP ", " CAPS ", "HARD CAPSULE"),
        "CAPSULE": ("CAPSULE", "CAPSULES", " CAP ", " CAPS "),
        "CREAM": ("CREAM", " CRM "),
        "GEL": (" GEL ",),
        "OINTMENT": ("OINTMENT", " OINT "),
        "SYRUP": ("SYRUP", " SYP ", " SYR "),
        "SUSPENSION": ("SUSPENSION", " SUSP "),
        "SOLUTION FOR INJECTION": ("SOLUTION FOR INJECTION", "INJECTION", " INJ ", "VIAL", "AMPOULE", "AMP "),
        "INJECTION": ("INJECTION", " INJ ", "VIAL", "AMPOULE", "AMP "),
        "EYE DROPS": ("EYE DROP", "EYE DROPS", "OPHTHALMIC DROP"),
        "EAR DROPS": ("EAR DROP", "EAR DROPS", "OTIC DROP"),
        "TRANSDERMAL PATCH": ("TRANSDERMAL PATCH", "PATCH", " TTS "),
        "SUPPOSITORY": ("SUPPOSITORY", "SUPPOSITORIES", " SUPP "),
        "POWDER": ("POWDER", " PWD "),
        "GRANULES": ("GRANULE", "GRANULES"),
        "LOZENGE": ("LOZENGE", "LOZENGES"),
        "INHALATION": ("INHAL", "INHALER"),
        "SPRAY": ("SPRAY",),
        "SOLUTION": ("SOLUTION", " SOLN ", " SOL "),
    }

    def __init__(self, pack_frame: pd.DataFrame):
        frame = pack_frame.copy()
        for col in ["Trade Name", "PharmaceuticalForm", "Size", "SizeUnit", "PackageTypes", "PackageSize"]:
            if col not in frame.columns:
                frame[col] = ""
        frame["Trade Name"] = Normalizer.text(frame["Trade Name"])
        frame["PharmaceuticalForm"] = Normalizer.text(frame["PharmaceuticalForm"])
        frame["SizeUnit"] = Normalizer.text(frame["SizeUnit"])
        frame["PackageTypes"] = Normalizer.text(frame["PackageTypes"])
        frame["PackageSize"] = pd.to_numeric(frame["PackageSize"], errors="coerce")
        frame["Size"] = pd.to_numeric(frame["Size"], errors="coerce")
        frame["_TradeKey"] = Normalizer.drug_name_key(frame["Trade Name"])
        frame = frame.loc[frame["_TradeKey"].ne("") & frame["PackageSize"].gt(0)].copy()
        self.frame = frame.reset_index(drop=True)
        self.by_trade = {
            key: group.reset_index(drop=True)
            for key, group in self.frame.groupby("_TradeKey", sort=False)
        }

    @classmethod
    @lru_cache(maxsize=1)
    def from_config(cls) -> "PackSizeResolver":
        path = Path(__file__).resolve().parent.parent / "config" / "pack_size.xlsx"
        return cls(pd.read_excel(path, engine="openpyxl", dtype=object))

    @staticmethod
    def _clean(value: Any) -> str:
        text = Normalizer._drug_key_scalar(value)
        return f" {text} " if text else ""

    @classmethod
    def _form_matches(cls, pharmaceutical_form: str, wms_text: str) -> bool:
        form = Normalizer._drug_key_scalar(pharmaceutical_form)
        if not form or not wms_text:
            return False
        padded = f" {wms_text} "
        aliases = cls._FORM_ALIASES.get(form)
        if aliases:
            return any(alias in padded for alias in aliases)
        form_tokens = [t for t in form.split() if len(t) >= 4]
        return bool(form_tokens) and all(t in padded for t in form_tokens)

    @staticmethod
    def _explicit_pack_counts(wms_text: str) -> set[int]:
        if not wms_text:
            return set()
        values = set()
        patterns = [
            r"\b(\d{1,4})\s*(?:S|S\b|PCS?|PIECES?|TABLETS?|TABS?|CAPSULES?|CAPS?)\b",
            r"\bX\s*(\d{1,4})\b",
            r"\b(\d{1,4})\s*X\b",
        ]
        for pattern in patterns:
            for match in re.findall(pattern, wms_text, flags=re.IGNORECASE):
                try:
                    value = int(match)
                    if value > 0:
                        values.add(value)
                except Exception:
                    pass
        return values

    @staticmethod
    def _size_matches(row: pd.Series, wms_text: str) -> bool:
        size = row.get("Size")
        unit = str(row.get("SizeUnit") or "").strip().upper()
        if pd.isna(size) or not unit or not wms_text:
            return False
        number = f"{float(size):g}"
        unit_alias = {"MICROGRAM": "MCG", "MILLIGRAM": "MG", "GRAM": "G", "MILLILITER": "ML"}.get(unit, unit)
        return bool(re.search(rf"\b{re.escape(number)}\s*{re.escape(unit_alias)}\b", wms_text, flags=re.IGNORECASE))

    def resolve(self, drug_name: Any, wms_trade_description: Any = "") -> tuple[float, str]:
        drug_key = Normalizer._drug_key_scalar(drug_name)
        wms_key = Normalizer._drug_key_scalar(wms_trade_description)
        if not drug_key and not wms_key:
            return 1.0, "Default 1 - Missing Drug Identity"

        candidate_keys = []
        if drug_key in self.by_trade:
            candidate_keys.append(drug_key)
        if wms_key:
            for key in self.by_trade:
                if key in wms_key and key not in candidate_keys:
                    candidate_keys.append(key)
        if not candidate_keys:
            return 1.0, "Default 1 - Trade Name Not Matched"

        # Prefer the longest matching trade name to avoid short-brand collisions.
        trade_key = max(candidate_keys, key=len)
        candidates = self.by_trade[trade_key].copy()
        unique_packs = sorted(set(float(v) for v in candidates["PackageSize"].dropna() if float(v) > 0))
        if len(unique_packs) == 1:
            return unique_packs[0], "Mapped by Trade Name"

        if drug_key or wms_key:
            # Prefer form evidence embedded in the registered/SFDA drug name;
            # WMS Trade Description is the second source. This handles names
            # such as "ADOL 500MG CAPLET" even when WMS says TABLET.
            for form_source in (drug_key, wms_key):
                if not form_source:
                    continue
                form_mask = candidates["PharmaceuticalForm"].map(
                    lambda x: self._form_matches(str(x), form_source)
                )
                if form_mask.any():
                    narrowed = candidates.loc[form_mask].copy()
                    packs = sorted(set(float(v) for v in narrowed["PackageSize"].dropna() if float(v) > 0))
                    if len(packs) == 1:
                        return packs[0], "Mapped by Trade Name + PharmaceuticalForm"
                    candidates = narrowed
                    break

        if wms_key:
            explicit_counts = self._explicit_pack_counts(wms_key)
            if explicit_counts:
                count_match = candidates.loc[candidates["PackageSize"].map(lambda x: int(float(x)) in explicit_counts if pd.notna(x) else False)]
                packs = sorted(set(float(v) for v in count_match["PackageSize"].dropna() if float(v) > 0))
                if len(packs) == 1:
                    return packs[0], "Mapped by WMS Pack Count"

            size_mask = candidates.apply(lambda row: self._size_matches(row, wms_key), axis=1)
            if size_mask.any():
                narrowed = candidates.loc[size_mask]
                packs = sorted(set(float(v) for v in narrowed["PackageSize"].dropna() if float(v) > 0))
                if len(packs) == 1:
                    return packs[0], "Mapped by Trade Name + Form + Size"

        first_pack = next(
            (
                float(value)
                for value in candidates["PackageSize"].tolist()
                if pd.notna(value) and float(value) > 0
            ),
            1.0,
        )
        return first_pack, "Mapped by First Pack Size"

    def resolve_frame(self, frame: pd.DataFrame, *, drug_col: str = "Drug Name", wms_col: str = "Trade Description") -> pd.DataFrame:
        result = frame.copy()
        values = [
            self.resolve(row.get(drug_col, ""), row.get(wms_col, ""))
            for row in result.to_dict(orient="records")
        ]
        result["PackageSize"] = [v[0] for v in values]
        result["Package Size Status"] = [v[1] for v in values]
        return result
