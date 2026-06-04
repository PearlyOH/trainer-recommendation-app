"""
StridelyIQ — Trainer catalogue API (v2)
=======================================

Adds to the previous version:
  - GET  /api/brands               -> list of brands for the dashboard dropdown
  - GET  /api/trainers/_auth_check -> trivial endpoint to validate admin key
  - POST /api/trainers/_autofill   -> best-effort metadata fetch from a URL
  - GET  /admin                    -> serves dashboard.html

Mount the API router at prefix='/api' from main.py, and the admin_router with
no prefix so the dashboard is at /admin.
"""

from __future__ import annotations

import os
import re
from datetime import datetime
from pathlib import Path
from typing import Optional, Literal
from uuid import UUID

import asyncpg
import httpx
from fastapi import APIRouter, Depends, HTTPException, Header, Query
from fastapi.responses import FileResponse
from pydantic import BaseModel, HttpUrl


# ---------------------------------------------------------------------------
# DB pool
# ---------------------------------------------------------------------------
_pool: Optional[asyncpg.Pool] = None


async def init_db_pool() -> None:
    global _pool
    if _pool is None:
        _pool = await asyncpg.create_pool(
            dsn=os.environ["DATABASE_URL"],
            min_size=1,
            max_size=5,
            statement_cache_size=0,
        )


async def close_db_pool() -> None:
    global _pool
    if _pool is not None:
        await _pool.close()
        _pool = None


def get_pool() -> asyncpg.Pool:
    if _pool is None:
        raise RuntimeError("DB pool not initialised — call init_db_pool() at startup")
    return _pool


# ---------------------------------------------------------------------------
# Auth
# ---------------------------------------------------------------------------
def require_admin(x_admin_key: str = Header(...)) -> None:
    expected = os.environ.get("ADMIN_API_KEY")
    if not expected or x_admin_key != expected:
        raise HTTPException(status_code=401, detail="Invalid admin key")


# ---------------------------------------------------------------------------
# Pydantic models
# ---------------------------------------------------------------------------
Terrain = Literal["Road", "Trail", "Both"]
FootWidth = Literal["Narrow", "Regular", "Wide", "All"]
TrainerStatus = Literal["draft", "active", "archived"]


class BrandOut(BaseModel):
    id: int
    name: str
    slug: str


class TrainerListingOut(BaseModel):
    merchant_name: str
    price: Optional[float]
    currency: str
    in_stock: Optional[bool]
    affiliate_url: str
    last_refreshed_at: datetime


class TrainerOut(BaseModel):
    id: UUID
    brand: str
    model: str
    display_name: str
    slug: str
    terrain: Optional[Terrain]
    run_type: Optional[str]
    drop_mm: Optional[float]
    stack_mm: Optional[float]
    weight_g: Optional[int]
    foot_width: Optional[FootWidth]
    hero_image_url: Optional[str]
    blurb: Optional[str]
    best_price: Optional[float]
    best_price_currency: str
    best_price_link: Optional[str]
    best_price_merchant: Optional[str]
    price_last_refreshed_at: Optional[datetime]
    status: TrainerStatus
    visible_on_frontend: bool
    listings: list[TrainerListingOut] = []


class TrainerCreate(BaseModel):
    brand: str
    model: str
    display_name: Optional[str] = None
    terrain: Optional[Terrain] = None
    run_type: Optional[str] = None
    drop_mm: Optional[float] = None
    stack_mm: Optional[float] = None
    weight_g: Optional[int] = None
    foot_width: Optional[FootWidth] = None
    hero_image_url: Optional[HttpUrl] = None
    blurb: Optional[str] = None
    status: TrainerStatus = "draft"
    visible_on_frontend: bool = False


class TrainerUpdate(BaseModel):
    display_name: Optional[str] = None
    terrain: Optional[Terrain] = None
    run_type: Optional[str] = None
    drop_mm: Optional[float] = None
    stack_mm: Optional[float] = None
    weight_g: Optional[int] = None
    foot_width: Optional[FootWidth] = None
    hero_image_url: Optional[HttpUrl] = None
    blurb: Optional[str] = None
    status: Optional[TrainerStatus] = None
    visible_on_frontend: Optional[bool] = None


class AutofillIn(BaseModel):
    url: HttpUrl


class AutofillOut(BaseModel):
    title: Optional[str] = None
    image: Optional[str] = None
    description: Optional[str] = None
    weight_g: Optional[int] = None
    drop_mm: Optional[float] = None
    stack_mm: Optional[float] = None


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def _slugify(text: str) -> str:
    s = re.sub(r"[^a-z0-9]+", "-", text.lower()).strip("-")
    return s or "trainer"


def _row_to_trainer(row, listings) -> TrainerOut:
    return TrainerOut(
        id=row["id"],
        brand=row["brand_name"],
        model=row["model"],
        display_name=row["display_name"],
        slug=row["slug"],
        terrain=row["terrain"],
        run_type=row["run_type"],
        drop_mm=float(row["drop_mm"]) if row["drop_mm"] is not None else None,
        stack_mm=float(row["stack_mm"]) if row["stack_mm"] is not None else None,
        weight_g=row["weight_g"],
        foot_width=row["foot_width"],
        hero_image_url=row["hero_image_url"],
        blurb=row["blurb"],
        best_price=float(row["best_price"]) if row["best_price"] is not None else None,
        best_price_currency=row["best_price_currency"] or "GBP",
        best_price_link=row["best_price_link"],
        best_price_merchant=row["best_price_merchant"],
        price_last_refreshed_at=row["price_last_refreshed_at"],
        status=row["status"],
        visible_on_frontend=row["visible_on_frontend"],
        listings=[
            TrainerListingOut(
                merchant_name=l["merchant_name"],
                price=float(l["price"]) if l["price"] is not None else None,
                currency=l["currency"] or "GBP",
                in_stock=l["in_stock"],
                affiliate_url=l["affiliate_url"],
                last_refreshed_at=l["last_refreshed_at"],
            )
            for l in listings
        ],
    )


# ---------------------------------------------------------------------------
# API router — mount at /api from main.py
# ---------------------------------------------------------------------------
router = APIRouter()


# -------- Brands ----------------------------------------------------------
@router.get("/brands", response_model=list[BrandOut])
async def list_brands():
    pool = get_pool()
    async with pool.acquire() as conn:
        rows = await conn.fetch("SELECT id, name, slug FROM brands ORDER BY name")
    return [BrandOut(id=r["id"], name=r["name"], slug=r["slug"]) for r in rows]


# -------- Auth check ------------------------------------------------------
@router.get("/trainers/_auth_check", dependencies=[Depends(require_admin)])
async def auth_check():
    return {"ok": True}


# -------- Autofill --------------------------------------------------------
@router.post(
    "/trainers/_autofill",
    response_model=AutofillOut,
    dependencies=[Depends(require_admin)],
)
async def autofill(payload: AutofillIn):
    url = str(payload.url)
    headers = {
        "User-Agent": (
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/124.0.0.0 Safari/537.36"
        ),
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "en-GB,en;q=0.9",
    }
    try:
        async with httpx.AsyncClient(
            timeout=10, follow_redirects=True, headers=headers,
        ) as client:
            resp = await client.get(url)
            resp.raise_for_status()
            html = resp.text
    except Exception:
        return AutofillOut()

    return AutofillOut(
        title=_og_tag(html, "og:title"),
        image=_og_tag(html, "og:image"),
        description=_og_tag(html, "og:description"),
        weight_g=_extract_weight_g(html),
        drop_mm=_extract_drop_mm(html),
        stack_mm=_extract_stack_mm(html),
    )


def _og_tag(html: str, prop: str) -> Optional[str]:
    m = re.search(
        rf'<meta[^>]+property=["\']{re.escape(prop)}["\'][^>]+content=["\']([^"\']+)["\']',
        html, re.I,
    )
    if m:
        return m.group(1).strip()
    m = re.search(
        rf'<meta[^>]+content=["\']([^"\']+)["\'][^>]+property=["\']{re.escape(prop)}["\']',
        html, re.I,
    )
    return m.group(1).strip() if m else None


def _extract_weight_g(html: str) -> Optional[int]:
    for pattern in [
        r'weight[^0-9]{0,15}(\d{2,3})\s*g\b',
        r'\b(\d{2,3})\s*g\s*(?:</|<br|<\/?\w)',
    ]:
        m = re.search(pattern, html, re.I)
        if m:
            val = int(m.group(1))
            if 150 <= val <= 400:
                return val
    m = re.search(r'\b(\d+(?:\.\d+)?)\s*oz\b', html, re.I)
    if m:
        g = int(round(float(m.group(1)) * 28.35))
        if 150 <= g <= 400:
            return g
    return None


def _extract_drop_mm(html: str) -> Optional[float]:
    m = re.search(r'(?:heel.to.toe\s*)?drop[^0-9]{0,20}(\d{1,2}(?:\.\d)?)\s*mm', html, re.I)
    if m:
        v = float(m.group(1))
        if 0 <= v <= 14:
            return v
    return None


def _extract_stack_mm(html: str) -> Optional[float]:
    m = re.search(r'stack(?:\s*height)?[^0-9]{0,20}(\d{1,2}(?:\.\d)?)\s*mm', html, re.I)
    if m:
        v = float(m.group(1))
        if 10 <= v <= 60:
            return v
    return None


# -------- Public trainer reads --------------------------------------------
@router.get("/trainers", response_model=list[TrainerOut])
async def list_trainers(
    terrain: Optional[Terrain] = None,
    run_type: Optional[str] = None,
    foot_width: Optional[FootWidth] = None,
    include_drafts: bool = False,
    x_admin_key: Optional[str] = Header(None),
):
    if include_drafts:
        expected = os.environ.get("ADMIN_API_KEY")
        if not expected or x_admin_key != expected:
            raise HTTPException(status_code=401, detail="Admin key required for drafts")

    where, args = [], []
    if not include_drafts:
        where.append("t.status = 'active' AND t.visible_on_frontend = TRUE")
    if terrain:
        args.append(terrain); where.append(f"t.terrain = ${len(args)}")
    if run_type:
        args.append(run_type); where.append(f"t.run_type = ${len(args)}")
    if foot_width:
        args.append(foot_width); where.append(f"(t.foot_width = ${len(args)} OR t.foot_width = 'All')")
    where_sql = ("WHERE " + " AND ".join(where)) if where else ""

    pool = get_pool()
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            f"""
            SELECT t.*, b.name AS brand_name
            FROM trainers t JOIN brands b ON b.id = t.brand_id
            {where_sql}
            ORDER BY t.display_name
            """,
            *args,
        )
        if not rows:
            return []
        ids = [r["id"] for r in rows]
        listings = await conn.fetch(
            "SELECT * FROM trainer_listings WHERE trainer_id = ANY($1::uuid[])", ids,
        )
        by_trainer: dict = {}
        for l in listings:
            by_trainer.setdefault(l["trainer_id"], []).append(l)
        return [_row_to_trainer(r, by_trainer.get(r["id"], [])) for r in rows]


@router.get("/trainers/{slug}", response_model=TrainerOut)
async def get_trainer(slug: str):
    pool = get_pool()
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            """
            SELECT t.*, b.name AS brand_name
            FROM trainers t JOIN brands b ON b.id = t.brand_id
            WHERE t.slug = $1
            """,
            slug,
        )
        if not row:
            raise HTTPException(status_code=404, detail="Trainer not found")
        listings = await conn.fetch(
            "SELECT * FROM trainer_listings WHERE trainer_id = $1", row["id"],
        )
        return _row_to_trainer(row, listings)


# -------- Admin CRUD ------------------------------------------------------
@router.post("/trainers", response_model=TrainerOut, dependencies=[Depends(require_admin)])
async def create_trainer(payload: TrainerCreate):
    pool = get_pool()
    display_name = payload.display_name or f"{payload.brand} {payload.model}"
    base_slug = _slugify(display_name)
    async with pool.acquire() as conn:
        brand_id = await conn.fetchval("SELECT id FROM brands WHERE name = $1", payload.brand)
        if brand_id is None:
            raise HTTPException(status_code=400, detail=f"Unknown brand: {payload.brand}")
        slug = base_slug
        for i in range(2, 50):
            exists = await conn.fetchval("SELECT 1 FROM trainers WHERE slug = $1", slug)
            if not exists:
                break
            slug = f"{base_slug}-{i}"
        try:
            row = await conn.fetchrow(
                """
                INSERT INTO trainers (
                    brand_id, model, display_name, slug, terrain, run_type,
                    drop_mm, stack_mm, weight_g, foot_width,
                    hero_image_url, blurb, status, visible_on_frontend
                ) VALUES (
                    $1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14
                )
                RETURNING *, (SELECT name FROM brands WHERE id = $1) AS brand_name
                """,
                brand_id, payload.model, display_name, slug,
                payload.terrain, payload.run_type,
                payload.drop_mm, payload.stack_mm, payload.weight_g, payload.foot_width,
                str(payload.hero_image_url) if payload.hero_image_url else None,
                payload.blurb, payload.status, payload.visible_on_frontend,
            )
        except asyncpg.UniqueViolationError:
            raise HTTPException(status_code=409, detail="A trainer with this brand+model already exists")
        return _row_to_trainer(row, [])


@router.patch("/trainers/{trainer_id}", response_model=TrainerOut, dependencies=[Depends(require_admin)])
async def update_trainer(trainer_id: UUID, payload: TrainerUpdate):
    fields = payload.model_dump(exclude_unset=True)
    if not fields:
        raise HTTPException(status_code=400, detail="No fields to update")
    if "hero_image_url" in fields and fields["hero_image_url"] is not None:
        fields["hero_image_url"] = str(fields["hero_image_url"])
    set_sql = ", ".join(f"{k} = ${i+2}" for i, k in enumerate(fields))
    pool = get_pool()
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            f"""
            UPDATE trainers SET {set_sql}
            WHERE id = $1
            RETURNING *, (SELECT name FROM brands WHERE id = brand_id) AS brand_name
            """,
            trainer_id, *fields.values(),
        )
        if not row:
            raise HTTPException(status_code=404, detail="Trainer not found")
        listings = await conn.fetch(
            "SELECT * FROM trainer_listings WHERE trainer_id = $1", trainer_id,
        )
        return _row_to_trainer(row, listings)


@router.delete("/trainers/{trainer_id}", status_code=204, dependencies=[Depends(require_admin)])
async def delete_trainer(trainer_id: UUID):
    pool = get_pool()
    async with pool.acquire() as conn:
        result = await conn.execute("DELETE FROM trainers WHERE id = $1", trainer_id)
        if result == "DELETE 0":
            raise HTTPException(status_code=404, detail="Trainer not found")


# -------- Price refresh (stub until Awin) ---------------------------------
@router.post("/trainers/refresh", dependencies=[Depends(require_admin)])
async def refresh_prices(trainer_id: Optional[UUID] = Query(None)):
    if not os.environ.get("AWIN_API_TOKEN"):
        raise HTTPException(
            status_code=503,
            detail="Awin not connected yet. Set AWIN_API_TOKEN to enable.",
        )
    return {"refreshed": 0, "changed": 0, "trainers_updated": 0}


# ---------------------------------------------------------------------------
# Dashboard router — mount with no prefix
# ---------------------------------------------------------------------------
admin_router = APIRouter()


@admin_router.get("/admin")
async def admin_dashboard():
    here = Path(__file__).parent
    candidates = [
        here / "dashboard.html",
        here.parent / "admin" / "dashboard.html",
        Path("admin") / "dashboard.html",
    ]
    for p in candidates:
        if p.exists():
            return FileResponse(str(p))
    raise HTTPException(status_code=500, detail="dashboard.html not found")
