"""
StridelyIQ — Catalogue admin tools
==================================

Admin-only routes that take the manual work out of the dashboard:

  GET    /trainers/_readiness           -> per-trainer missing fields, aliases, Awin status
  POST   /trainers/_import              -> bulk import (dry run first, then commit)
  GET    /trainers/_coverage            -> reviewed models vs the catalogue
  POST   /trainers/{trainer_id}/aliases -> remember another name for a trainer
  DELETE /trainers/_aliases/{alias_id}
  POST   /trainers/_awin/preview        -> match Awin feed products to trainers (read-only)
  POST   /trainers/_awin/matches        -> confirm / reject feed matches

Include this router in api.py BEFORE the main trainers router, so the
/trainers/_xyz paths win over GET /trainers/{slug}.

Adds three tables (created on startup if missing, existing tables untouched):
  trainer_aliases       alternative names (survey free text, retailer names)
  trainer_feed_matches  Awin feed products confirmed/rejected per trainer
  trainer_sources       the product page each trainer was auto-filled from
"""

from __future__ import annotations

import asyncio
import html
import re
from typing import Optional
from uuid import UUID

import pandas as pd
from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel, Field
from starlette.concurrency import run_in_threadpool

from services.trainer_rules import (
    AFFILIATE_LABEL,
    BLURB_MAX,
    BLURB_MIN,
    REQUIRED_FOR_LIVE,
    CatalogueIndex,
    affiliate_link_required,
    is_live,
    missing_for_live,
    name_key,
    normalise_foot_width,
    normalise_run_type,
    normalise_terrain,
    split_dropdown,
    OTHER_ANSWERS,
    RUN_TYPES,
)
from services.trainers_api import (
    autofill_from_url,
    get_pool,
    require_admin,
    slugify,
    unique_slug,
)


# ---------------------------------------------------------------------------
# Schema (additive only)
# ---------------------------------------------------------------------------
SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS trainer_aliases (
    id          BIGSERIAL PRIMARY KEY,
    trainer_id  UUID NOT NULL REFERENCES trainers(id) ON DELETE CASCADE,
    alias       TEXT NOT NULL,
    alias_key   TEXT NOT NULL UNIQUE,
    source      TEXT NOT NULL DEFAULT 'manual',
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS trainer_aliases_trainer_id_idx ON trainer_aliases (trainer_id);

CREATE TABLE IF NOT EXISTS trainer_feed_matches (
    id               BIGSERIAL PRIMARY KEY,
    trainer_id       UUID NOT NULL REFERENCES trainers(id) ON DELETE CASCADE,
    feed             TEXT NOT NULL DEFAULT 'awin',
    merchant_id      TEXT NOT NULL,
    merchant_name    TEXT,
    feed_product_id  TEXT NOT NULL,
    product_name     TEXT,
    status           TEXT NOT NULL CHECK (status IN ('confirmed', 'rejected')),
    updated_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (feed, merchant_id, feed_product_id)
);
CREATE INDEX IF NOT EXISTS trainer_feed_matches_trainer_id_idx ON trainer_feed_matches (trainer_id);

CREATE TABLE IF NOT EXISTS trainer_sources (
    trainer_id  UUID PRIMARY KEY REFERENCES trainers(id) ON DELETE CASCADE,
    source_url  TEXT NOT NULL,
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);
"""


async def ensure_schema() -> None:
    """Never raises: the public routes must keep working even if this fails."""
    try:
        pool = get_pool()
        async with pool.acquire() as conn:
            async with conn.transaction():
                await conn.execute("SELECT pg_advisory_xact_lock(hashtext('stridely_catalogue_admin_schema'))")
                await conn.execute(SCHEMA_SQL)
    except Exception as e:
        print(f"WARNING: catalogue admin tables not created ({e}); admin tools may fail")


router = APIRouter(dependencies=[Depends(require_admin)])


# ---------------------------------------------------------------------------
# Shared loaders
# ---------------------------------------------------------------------------
async def _load_catalogue(conn) -> tuple[list[dict], list[tuple[str, UUID]]]:
    rows = await conn.fetch(
        """
        SELECT t.*, b.name AS brand_name,
               (t.best_price_link IS NOT NULL OR EXISTS (
                    SELECT 1 FROM trainer_listings l
                    WHERE l.trainer_id = t.id AND l.affiliate_url IS NOT NULL
               )) AS has_affiliate_link
        FROM trainers t JOIN brands b ON b.id = t.brand_id
        """
    )
    trainers = [{**dict(r), "brand": r["brand_name"]} for r in rows]
    aliases = await conn.fetch("SELECT alias, trainer_id FROM trainer_aliases")
    return trainers, [(a["alias"], a["trainer_id"]) for a in aliases]


# ---------------------------------------------------------------------------
# Readiness: completeness check + aliases + Awin status for every trainer
# ---------------------------------------------------------------------------
@router.get("/trainers/_readiness")
async def readiness():
    pool = get_pool()
    async with pool.acquire() as conn:
        trainers, _ = await _load_catalogue(conn)
        aliases = await conn.fetch("SELECT id, trainer_id, alias FROM trainer_aliases ORDER BY alias")
        sources = {str(r["trainer_id"]): r["source_url"] for r in await conn.fetch("SELECT * FROM trainer_sources")}
        matches = await conn.fetch(
            "SELECT trainer_id, count(*) AS n FROM trainer_feed_matches "
            "WHERE status = 'confirmed' GROUP BY trainer_id"
        )
    alias_map: dict = {}
    for a in aliases:
        alias_map.setdefault(str(a["trainer_id"]), []).append({"id": a["id"], "alias": a["alias"]})
    match_map = {str(m["trainer_id"]): m["n"] for m in matches}
    return {
        "required": [label for _, label in REQUIRED_FOR_LIVE]
        + ([AFFILIATE_LABEL] if affiliate_link_required() else []),
        "affiliate_required": affiliate_link_required(),
        "run_types": RUN_TYPES,
        "trainers": {
            str(t["id"]): {
                "missing": missing_for_live(t, t["has_affiliate_link"]),
                "live": is_live(t),
                "aliases": alias_map.get(str(t["id"]), []),
                "feed_matches": match_map.get(str(t["id"]), 0),
                "has_affiliate_link": t["has_affiliate_link"],
                "source_url": sources.get(str(t["id"])),
            }
            for t in trainers
        },
    }


# ---------------------------------------------------------------------------
# Bulk import
# ---------------------------------------------------------------------------
class ImportRow(BaseModel):
    brand: Optional[str] = None
    model: Optional[str] = None
    display_name: Optional[str] = None
    terrain: Optional[str] = None
    run_type: Optional[str] = None
    drop_mm: Optional[str] = None
    stack_mm: Optional[str] = None
    weight_g: Optional[str] = None
    foot_width: Optional[str] = None
    hero_image_url: Optional[str] = None
    blurb: Optional[str] = None
    source_url: Optional[str] = None
    aliases: Optional[str] = None  # semicolon-separated


class ImportIn(BaseModel):
    rows: list[ImportRow] = Field(..., max_length=200)
    dry_run: bool = True
    autofill: bool = False
    create_missing_brands: bool = False
    first_row_number: int = 1  # so chunked uploads report spreadsheet row numbers


TRAINER_FIELDS = [
    "display_name", "terrain", "run_type", "drop_mm", "stack_mm",
    "weight_g", "foot_width", "hero_image_url", "blurb",
]
_NUMBER_RANGES = {"drop_mm": (0, 16), "stack_mm": (10, 60), "weight_g": (100, 500)}


def _clean(v: Optional[str]) -> Optional[str]:
    if v is None:
        return None
    v = str(v).strip()
    return v or None


def _parse_row(raw: ImportRow) -> tuple[dict, list[str]]:
    """Normalise one spreadsheet row. Returns (fields, errors)."""
    errors: list[str] = []
    out: dict = {}

    brand, model = _clean(raw.brand), _clean(raw.model)
    if brand and not model and split_dropdown(brand):
        brand, model = split_dropdown(brand)  # single "Brand - Model" column
    out["brand"], out["model"] = brand, model
    if not brand:
        errors.append("Brand is missing")
    if not model:
        errors.append("Model is missing")

    out["display_name"] = _clean(raw.display_name)

    for field, fn, allowed in [
        ("terrain", normalise_terrain, "Road, Trail, Both"),
        ("run_type", normalise_run_type, ", ".join(RUN_TYPES)),
        ("foot_width", normalise_foot_width, "Narrow, Regular, Wide, All"),
    ]:
        value = _clean(getattr(raw, field))
        if value is None:
            out[field] = None
        elif fn(value) is None:
            errors.append(f"{field.replace('_', ' ').capitalize()} '{value}' not recognised (use {allowed})")
        else:
            out[field] = fn(value)

    for field, (lo, hi) in _NUMBER_RANGES.items():
        value = _clean(getattr(raw, field))
        if value is None:
            out[field] = None
            continue
        try:
            n = float(value.lower().replace("mm", "").replace("g", "").strip())
        except ValueError:
            errors.append(f"{field} '{value}' is not a number")
            continue
        if not lo <= n <= hi:
            errors.append(f"{field} {n:g} is outside {lo}–{hi}")
            continue
        out[field] = int(round(n)) if field == "weight_g" else n

    for field in ("hero_image_url", "source_url"):
        value = _clean(getattr(raw, field))
        if value and not value.lower().startswith(("http://", "https://")):
            errors.append(f"{field} must start with http:// or https://")
            value = None
        out[field] = value

    blurb = _clean(raw.blurb)
    if blurb and len(blurb) > BLURB_MAX:
        errors.append(f"Blurb is {len(blurb)} characters (max {BLURB_MAX})")
    out["blurb"] = blurb
    out["aliases"] = [a.strip() for a in (raw.aliases or "").split(";") if a.strip()]
    return out, errors


# Page descriptions that read like shop copy rather than a line about the shoe
_SHOP_TEXT = re.compile(r"^\s*(shop|buy)\b|free (delivery|shipping|returns)|\bat [\w.-]+\.(com|co\.uk)\b", re.I)


def _card_blurb(description: Optional[str]) -> Optional[str]:
    """A page description cleaned up for the trainer card, or None if unusable."""
    if not description:
        return None
    text = " ".join(html.unescape(description).split())
    if _SHOP_TEXT.search(text):
        return None
    if len(text) > BLURB_MAX:  # cut at the last full sentence that fits
        cut = text[:BLURB_MAX]
        end = max(cut.rfind(". "), cut.rfind("! "), cut.rfind("? "))
        text = cut[: end + 1] if end >= BLURB_MIN else cut.rstrip() + "…"
    return text if len(text) >= BLURB_MIN else None


async def _autofill_into(row: dict, sem: asyncio.Semaphore) -> tuple[list[str], list[str]]:
    """Fill fields from the row's source_url. Returns (fields filled, notes).

    The page's own description replaces a blurb supplied in the sheet (that
    one is the fallback); every other field is only filled when empty."""
    if not row.get("source_url"):
        return [], []
    async with sem:
        data = await autofill_from_url(row["source_url"])
    filled, notes = [], []
    page_blurb = _card_blurb(data.description)
    if page_blurb:
        row["blurb"] = page_blurb
        filled.append("blurb")
    elif data.description:
        notes.append("Page description read like shop text, so the list blurb was kept"
                     if row.get("blurb") else "Page description read like shop text, so it was skipped")
    for src, dst in [("image", "hero_image_url"), ("weight_g", "weight_g"),
                     ("drop_mm", "drop_mm"), ("stack_mm", "stack_mm")]:
        value = getattr(data, src)
        if value and row.get(dst) is None:
            row[dst] = value
            filled.append(dst)
    return filled, notes


@router.post("/trainers/_import")
async def bulk_import(payload: ImportIn):
    pool = get_pool()
    async with pool.acquire() as conn:
        brands = {name_key(b["name"]): dict(b) for b in await conn.fetch("SELECT id, name FROM brands")}
        existing = {
            (r["brand_id"], name_key(r["model"])): dict(r)
            for r in await conn.fetch("SELECT * FROM trainers")
        }
        sources = {r["trainer_id"]: r["source_url"] for r in await conn.fetch("SELECT * FROM trainer_sources")}

    parsed = [_parse_row(r) for r in payload.rows]
    sem = asyncio.Semaphore(4)
    autofilled = await asyncio.gather(*[
        _autofill_into(row, sem) if payload.autofill and not errs else asyncio.sleep(0, result=([], []))
        for row, errs in parsed
    ])

    results, seen, new_brands = [], set(), set()
    for i, ((row, errors), (filled, notes)) in enumerate(zip(parsed, autofilled)):
        res = {
            "row": payload.first_row_number + i,
            "brand": row["brand"], "model": row["model"],
            "errors": list(errors), "warnings": list(notes), "fill": {}, "autofilled": filled,
        }
        results.append(res)

        if row["brand"] and name_key(row["brand"]) in OTHER_ANSWERS:
            res["action"] = "skip"
            res["warnings"].append("'Other' is a survey option, not a trainer")
            continue
        brand = brands.get(name_key(row["brand"] or ""))
        if row["brand"] and not brand:
            if payload.create_missing_brands:
                new_brands.add(row["brand"])
                res["warnings"].append(f"New brand '{row['brand']}' will be created")
            else:
                res["errors"].append(f"Unknown brand '{row['brand']}' (tick 'create missing brands' or fix the spelling)")
        key = (name_key(row["brand"] or ""), name_key(row["model"] or ""))
        if row["brand"] and row["model"]:
            if key in seen:
                res["errors"].append("Duplicate of an earlier row in this file")
            seen.add(key)
        if res["errors"]:
            res["action"] = "error"
            continue

        current = existing.get((brand["id"], key[1])) if brand else None
        if current:
            res["trainer_id"] = str(current["id"])
            res["fill"] = {f: row[f] for f in TRAINER_FIELDS if row.get(f) is not None and current.get(f) is None}
            new_source = row["source_url"] and row["source_url"] != sources.get(current["id"])
            res["action"] = "update" if res["fill"] or row["aliases"] or new_source else "unchanged"
        else:
            res["fill"] = {f: row[f] for f in TRAINER_FIELDS if row.get(f) is not None}
            # Use the catalogue's spelling of the brand ("Adidas", not the sheet's "adidas")
            res["fill"].setdefault("display_name", f"{brand['name'] if brand else row['brand']} {row['model']}")
            res["action"] = "create"
        res["aliases"] = row["aliases"]
        res["source_url"] = row["source_url"]

    if not payload.dry_run:
        await _commit_import(results, new_brands)

    summary: dict = {}
    for r in results:
        summary[r["action"]] = summary.get(r["action"], 0) + 1
    return {"dry_run": payload.dry_run, "summary": summary, "rows": results}


async def _commit_import(results: list[dict], new_brands: set[str]) -> None:
    pool = get_pool()
    async with pool.acquire() as conn:
        brand_ids = {name_key(b["name"]): b["id"] for b in await conn.fetch("SELECT id, name FROM brands")}
        for name in sorted(new_brands):
            if name_key(name) not in brand_ids:
                brand_ids[name_key(name)] = await conn.fetchval(
                    "INSERT INTO brands (name, slug) VALUES ($1, $2) RETURNING id", name, slugify(name),
                )
        for res in results:
            if res["action"] not in ("create", "update"):
                continue
            try:
                async with conn.transaction():
                    if res["action"] == "create":
                        fill = res["fill"]
                        slug = await unique_slug(conn, slugify(fill["display_name"]))
                        res["trainer_id"] = str(await conn.fetchval(
                            """
                            INSERT INTO trainers (
                                brand_id, model, display_name, slug, terrain, run_type,
                                drop_mm, stack_mm, weight_g, foot_width,
                                hero_image_url, blurb, status, visible_on_frontend
                            ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,'draft',FALSE)
                            RETURNING id
                            """,
                            brand_ids[name_key(res["brand"])], res["model"], fill["display_name"], slug,
                            fill.get("terrain"), fill.get("run_type"), fill.get("drop_mm"),
                            fill.get("stack_mm"), fill.get("weight_g"), fill.get("foot_width"),
                            fill.get("hero_image_url"), fill.get("blurb"),
                        ))
                    elif res["fill"]:
                        # Only fills columns that are still empty — never overwrites
                        cols = list(res["fill"])
                        set_sql = ", ".join(f"{c} = COALESCE({c}, ${i + 2})" for i, c in enumerate(cols))
                        await conn.execute(
                            f"UPDATE trainers SET {set_sql} WHERE id = $1",
                            UUID(res["trainer_id"]), *res["fill"].values(),
                        )
                    for alias in res.get("aliases", []):
                        await _insert_alias(conn, UUID(res["trainer_id"]), alias, "import")
                    if res.get("source_url"):
                        await _save_source(conn, UUID(res["trainer_id"]), res["source_url"])
                res["saved"] = True
            except Exception as e:  # one bad row must not sink the rest
                res["saved"] = False
                res["errors"].append(f"Database rejected this row: {e}")


async def _insert_alias(conn, trainer_id: UUID, alias: str, source: str) -> Optional[int]:
    return await conn.fetchval(
        """
        INSERT INTO trainer_aliases (trainer_id, alias, alias_key, source)
        VALUES ($1, $2, $3, $4)
        ON CONFLICT (alias_key) DO UPDATE SET trainer_id = EXCLUDED.trainer_id, alias = EXCLUDED.alias
        RETURNING id
        """,
        trainer_id, alias, name_key(alias), source,
    )


async def _save_source(conn, trainer_id: UUID, url: str) -> None:
    await conn.execute(
        """
        INSERT INTO trainer_sources (trainer_id, source_url) VALUES ($1, $2)
        ON CONFLICT (trainer_id) DO UPDATE SET source_url = EXCLUDED.source_url, updated_at = now()
        """,
        trainer_id, url,
    )


class SourceIn(BaseModel):
    source_url: str = Field(..., pattern=r"^https?://", max_length=2000)


@router.put("/trainers/{trainer_id}/source")
async def set_source(trainer_id: UUID, payload: SourceIn):
    pool = get_pool()
    async with pool.acquire() as conn:
        if not await conn.fetchval("SELECT 1 FROM trainers WHERE id = $1", trainer_id):
            raise HTTPException(status_code=404, detail="Trainer not found")
        await _save_source(conn, trainer_id, payload.source_url.strip())
    return {"source_url": payload.source_url.strip()}


# ---------------------------------------------------------------------------
# Aliases
# ---------------------------------------------------------------------------
class AliasIn(BaseModel):
    alias: str = Field(..., min_length=1, max_length=200)


@router.post("/trainers/{trainer_id}/aliases")
async def add_alias(trainer_id: UUID, payload: AliasIn):
    if not name_key(payload.alias):
        raise HTTPException(status_code=400, detail="Alias needs at least one letter or number")
    pool = get_pool()
    async with pool.acquire() as conn:
        if not await conn.fetchval("SELECT 1 FROM trainers WHERE id = $1", trainer_id):
            raise HTTPException(status_code=404, detail="Trainer not found")
        alias_id = await _insert_alias(conn, trainer_id, payload.alias.strip(), "manual")
    return {"id": alias_id, "alias": payload.alias.strip()}


@router.delete("/trainers/_aliases/{alias_id}", status_code=204)
async def delete_alias(alias_id: int):
    pool = get_pool()
    async with pool.acquire() as conn:
        if await conn.execute("DELETE FROM trainer_aliases WHERE id = $1", alias_id) == "DELETE 0":
            raise HTTPException(status_code=404, detail="Alias not found")


# ---------------------------------------------------------------------------
# Coverage: reviewed models vs catalogue
# ---------------------------------------------------------------------------
def _read_reviews() -> pd.DataFrame:
    # Imported here so the admin tools still load if Sheets is misconfigured
    from services.sheets_service import SheetsService
    from config.settings import SHEET_CLEAN_DATA

    df = SheetsService().read_to_dataframe(SHEET_CLEAN_DATA)
    # Tally headers carry stray newlines and there are two brand/model
    # columns (one legacy, empty), so pick the one with the most answers.
    candidates = [c for c in df.columns if c and "brand" in c.lower() and "model" in c.lower()]
    if not candidates:
        raise HTTPException(status_code=502, detail="No brand/model column found in the review sheet")
    col = max(candidates, key=lambda c: (df[c].astype(str).str.strip() != "").sum())
    out = pd.DataFrame({"answer": df[col].astype(str).str.strip()})
    out["score"] = pd.to_numeric(df["Score"], errors="coerce") if "Score" in df.columns else None
    return out[out["answer"] != ""]


@router.get("/trainers/_coverage")
async def coverage():
    reviews = await run_in_threadpool(_read_reviews)
    pool = get_pool()
    async with pool.acquire() as conn:
        trainers, aliases = await _load_catalogue(conn)
    index = CatalogueIndex(trainers, aliases)

    rows, reviewed_ids = [], set()
    for key, grp in reviews.groupby(reviews["answer"].map(name_key)):
        answer = grp["answer"].mode().iloc[0]
        if key in OTHER_ANSWERS:
            continue
        m = index.match(answer)
        row = {
            "answer": answer,
            "kind": "dropdown" if split_dropdown(answer) else "free_text",
            "reviews": int(len(grp)),
            "avg_score": round(float(grp["score"].mean()), 1) if grp["score"].notna().any() else None,
            "match": None,
        }
        if m:
            t = index.by_id[m["trainer_id"]]
            row["match"] = {
                "trainer_id": str(t["id"]), "display_name": t["display_name"],
                "via": m["via"], "confident": m["confident"],
                "live": is_live(t), "status": t["status"],
                "missing": missing_for_live(t, t["has_affiliate_link"]),
            }
            if m["confident"]:
                reviewed_ids.add(t["id"])
            row["state"] = ("live" if is_live(t) else "not_live") if m["confident"] else "check"
        else:
            split = split_dropdown(answer)
            row["suggested"] = {"brand": split[0], "model": split[1]} if split else None
            row["state"] = "missing"
        rows.append(row)

    order = {"missing": 0, "check": 1, "not_live": 2, "live": 3}
    rows.sort(key=lambda r: (order[r["state"]], -r["reviews"], r["answer"].lower()))
    summary: dict = {}
    for r in rows:
        summary[r["state"]] = summary.get(r["state"], 0) + r["reviews"]
    return {
        "total_reviews": int(len(reviews)),
        "summary": summary,
        "rows": rows,
        "unreviewed": sorted(
            (t["display_name"] for t in trainers if t["id"] not in reviewed_ids), key=str.lower,
        ),
    }


# ---------------------------------------------------------------------------
# Awin product feed matching
# ---------------------------------------------------------------------------
# Feed rows use Awin's Create-a-Feed column names. The dashboard parses the
# CSV in the browser and sends products in batches; nothing here downloads
# the feed, so no Awin credentials are needed for matching.
class FeedProduct(BaseModel):
    aw_product_id: str
    merchant_id: str
    merchant_name: Optional[str] = None
    product_name: str
    brand_name: Optional[str] = None
    search_price: Optional[str] = None
    currency: Optional[str] = None
    in_stock: Optional[str] = None
    aw_deep_link: Optional[str] = None
    merchant_image_url: Optional[str] = None


class FeedPreviewIn(BaseModel):
    products: list[FeedProduct] = Field(..., max_length=2000)


@router.post("/trainers/_awin/preview")
async def awin_preview(payload: FeedPreviewIn):
    pool = get_pool()
    async with pool.acquire() as conn:
        trainers, aliases = await _load_catalogue(conn)
        decided = {
            (r["merchant_id"], r["feed_product_id"]): dict(r)
            for r in await conn.fetch("SELECT * FROM trainer_feed_matches WHERE feed = 'awin'")
        }
    index = CatalogueIndex(trainers, aliases)
    out = []
    for p in payload.products:
        brand = (p.brand_name or "").strip()
        text = p.product_name if name_key(p.product_name).startswith(name_key(brand)) else f"{brand} {p.product_name}"
        m = index.match(text)
        if not m:
            continue
        t = index.by_id[m["trainer_id"]]
        prior = decided.get((p.merchant_id, p.aw_product_id))
        out.append({
            **p.model_dump(),
            "trainer_id": str(t["id"]), "display_name": t["display_name"],
            # Retailer names always carry extra words ("Men's Road Running
            # Shoes"), so a brand + model name match counts as confident here.
            "via": m["via"], "confident": m["via"] != "similar",
            "decision": prior["status"] if prior and str(prior["trainer_id"]) == str(t["id"]) else None,
        })
    return {"scanned": len(payload.products), "matched": len(out), "products": out}


class FeedDecision(BaseModel):
    trainer_id: UUID
    merchant_id: str
    merchant_name: Optional[str] = None
    feed_product_id: str
    product_name: Optional[str] = None
    status: str = Field(..., pattern="^(confirmed|rejected)$")


class FeedDecisionsIn(BaseModel):
    matches: list[FeedDecision] = Field(..., max_length=2000)


@router.post("/trainers/_awin/matches")
async def awin_save_matches(payload: FeedDecisionsIn):
    pool = get_pool()
    async with pool.acquire() as conn:
        async with conn.transaction():
            await conn.executemany(
                """
                INSERT INTO trainer_feed_matches
                    (trainer_id, feed, merchant_id, merchant_name, feed_product_id, product_name, status)
                VALUES ($1, 'awin', $2, $3, $4, $5, $6)
                ON CONFLICT (feed, merchant_id, feed_product_id) DO UPDATE SET
                    trainer_id = EXCLUDED.trainer_id, status = EXCLUDED.status,
                    merchant_name = EXCLUDED.merchant_name, product_name = EXCLUDED.product_name,
                    updated_at = now()
                """,
                [(m.trainer_id, m.merchant_id, m.merchant_name, m.feed_product_id, m.product_name, m.status)
                 for m in payload.matches],
            )
    return {"saved": len(payload.matches)}
