"""
StridelyIQ — catalogue rules shared by the trainer API and the admin tools
==========================================================================

  - Which fields a trainer card needs before it can go Live
  - Canonical run types / terrain, and how spreadsheet values map onto them
  - Name matching between survey answers, the catalogue and retailer feeds

No database or web imports here, so it is safe to import from anywhere.
"""

from __future__ import annotations

import difflib
import os
import re
import unicodedata
from typing import Iterable, Optional


# ---------------------------------------------------------------------------
# Live = status 'active' AND visible_on_frontend
# ---------------------------------------------------------------------------
REQUIRED_FOR_LIVE: list[tuple[str, str]] = [
    ("display_name", "Display name"),
    ("terrain", "Terrain"),
    ("run_type", "Run type"),
    ("foot_width", "Foot width"),
    ("hero_image_url", "Hero image"),
    ("blurb", "Blurb"),
    ("drop_mm", "Drop (mm)"),
    ("stack_mm", "Stack (mm)"),
    ("weight_g", "Weight (g)"),
]
AFFILIATE_LABEL = "Affiliate link"
BLURB_MIN, BLURB_MAX = 30, 500


def affiliate_link_required() -> bool:
    """Affiliate links become mandatory once Awin is connected."""
    return bool(os.environ.get("AWIN_API_TOKEN"))


def is_live(t: dict) -> bool:
    return t.get("status") == "active" and bool(t.get("visible_on_frontend"))


def missing_for_live(t: dict, has_affiliate_link: bool) -> list[str]:
    """Labels of everything a trainer still needs before it can go Live."""
    missing = []
    for field, label in REQUIRED_FOR_LIVE:
        v = t.get(field)
        if v is None or (isinstance(v, str) and not v.strip()):
            missing.append(label)
    img = t.get("hero_image_url")
    if img and not str(img).startswith("https://"):
        missing.append("Hero image (must be https)")
    blurb = (t.get("blurb") or "").strip()
    if blurb and not (BLURB_MIN <= len(blurb) <= BLURB_MAX):
        missing.append(f"Blurb ({BLURB_MIN}–{BLURB_MAX} characters)")
    if affiliate_link_required() and not has_affiliate_link:
        missing.append(AFFILIATE_LABEL)
    return missing


# ---------------------------------------------------------------------------
# Run type / terrain vocabularies
# ---------------------------------------------------------------------------
RUN_TYPES = ["Daily", "Tempo", "Race", "Long run", "Recovery", "Trail"]

# Spreadsheet "PrimaryPurpose" values -> catalogue run_type
_RUN_TYPE_ALIASES = {
    "daily trainer": "Daily",
    "daily": "Daily",
    "easy run": "Daily",
    "tempo / speed": "Tempo",
    "tempo/speed": "Tempo",
    "speed": "Tempo",
    "race (road)": "Race",
    "road race": "Race",
    "trail racing": "Trail",
    # Kept as Daily so the existing run_type values stay valid; a separate
    # tag for these can come later
    "max cushion": "Daily",
    "stability": "Daily",
}
_TERRAIN_ALIASES = {
    "road": "Road",
    "trail": "Trail",
    "both": "Both",
    "road-to-trail": "Both",
    "road to trail": "Both",
    "mixed": "Both",
}
_FOOT_WIDTHS = {"narrow": "Narrow", "regular": "Regular", "standard": "Regular", "wide": "Wide", "all": "All"}


def normalise_run_type(value: Optional[str]) -> Optional[str]:
    if value is None or not str(value).strip():
        return None
    v = str(value).strip().lower()
    for rt in RUN_TYPES:
        if rt.lower() == v:
            return rt
    return _RUN_TYPE_ALIASES.get(v)


def normalise_terrain(value: Optional[str]) -> Optional[str]:
    if value is None or not str(value).strip():
        return None
    return _TERRAIN_ALIASES.get(str(value).strip().lower())


def normalise_foot_width(value: Optional[str]) -> Optional[str]:
    if value is None or not str(value).strip():
        return None
    return _FOOT_WIDTHS.get(str(value).strip().lower())


# ---------------------------------------------------------------------------
# Name matching
# ---------------------------------------------------------------------------
# Tally dropdown answers look like "Nike - Vomero"; "Other" answers are free
# text such as "On Cloudflow 4" or "Adizero Evo SL" (no brand).
OTHER_ANSWERS = {"other", "othernotlisted"}


def tokens(text: str) -> list[str]:
    s = unicodedata.normalize("NFKD", str(text)).encode("ascii", "ignore").decode().lower()
    return re.findall(r"[a-z0-9]+", s)


def name_key(text: str) -> str:
    return "".join(tokens(text))


def split_dropdown(text: str) -> Optional[tuple[str, str]]:
    """'Nike - Vomero' -> ('Nike', 'Vomero'); free text -> None."""
    parts = str(text).split(" - ", 1)
    if len(parts) == 2 and parts[0].strip() and parts[1].strip():
        return parts[0].strip(), parts[1].strip()
    return None


class CatalogueIndex:
    """
    Matches free-form trainer names to catalogue trainers.

    entries: dicts with id, brand, model (+ anything else, passed through)
    aliases: (alias_text, trainer_id) pairs
    """

    def __init__(self, entries: Iterable[dict], aliases: Iterable[tuple[str, object]] = ()):
        self.entries = list(entries)
        self.by_id = {e["id"]: e for e in self.entries}
        self.exact: dict[str, object] = {}
        for e in self.entries:
            self.exact[name_key(f"{e['brand']} {e['model']}")] = e["id"]
        self.alias: dict[str, object] = {name_key(a): tid for a, tid in aliases if tid in self.by_id}
        self.brands = sorted({e["brand"] for e in self.entries}, key=lambda b: -len(tokens(b)))

    def _strip_brand(self, toks: list[str]) -> tuple[Optional[str], list[str]]:
        for b in self.brands:
            bt = tokens(b)
            if bt and toks[: len(bt)] == bt:
                return b, toks[len(bt):]
        return None, toks

    @staticmethod
    def _subsequence_score(model_toks: list[str], text_toks: list[str]) -> Optional[tuple[int, int]]:
        """Model tokens must appear in order, each a prefix of a text token
        ('1080' matches '1080v13'). Returns (chars matched, -tokens skipped)."""
        i, skipped, chars, started = 0, 0, 0, False
        for t in text_toks:
            if i == len(model_toks):
                break
            if t.startswith(model_toks[i]):
                chars += len(model_toks[i])
                i += 1
                started = True
            elif started:
                skipped += 1
        if i < len(model_toks):
            return None
        return chars, -skipped

    def match(self, text: str) -> Optional[dict]:
        """
        Returns {"trainer_id", "via", "confident"} or None.
        via: exact | alias | name | similar
        """
        key = name_key(text)
        if not key or key in OTHER_ANSWERS:
            return None
        if key in self.alias:
            return {"trainer_id": self.alias[key], "via": "alias", "confident": True}
        if key in self.exact:
            return {"trainer_id": self.exact[key], "via": "exact", "confident": True}

        dd = split_dropdown(text)
        if dd:
            brand, toks = dd[0], tokens(dd[1])
        else:
            brand, toks = self._strip_brand(tokens(text))
        candidates = [e for e in self.entries if brand is None or name_key(e["brand"]) == name_key(brand)]

        best, best_score = None, None
        for e in candidates:
            score = self._subsequence_score(tokens(e["model"]), toks)
            if score and (best_score is None or score > best_score):
                best, best_score = e, score
        if best:
            return {"trainer_id": best["id"], "via": "name", "confident": False}

        keys = {name_key(f"{e['brand']} {e['model']}"): e["id"] for e in candidates}
        close = difflib.get_close_matches(key, list(keys), n=1, cutoff=0.8)
        if close:
            return {"trainer_id": keys[close[0]], "via": "similar", "confident": False}
        return None
