"""
main.py — FastAPI application entrypoint for Rubicr Caetis API.

Run:
    uvicorn backend.api.main:app --reload --port 8000
"""
from __future__ import annotations

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

import os
from typing import Optional
from fastapi import FastAPI, Query, HTTPException
from fastapi.middleware.cors import CORSMiddleware
import asyncio
import httpx

from backend.database.db import init_db
from backend.api.routers import auth, companies, pipeline, approvals, evidence, config, analytics
from backend.api.seed import seed_default_users

app = FastAPI(
    title="Rubicr Caetis API",
    description="Super Admin Console — Risk Intelligence Platform",
    version="1.0.0",
)

# ── CORS (allow Vite dev server + production) ─────────────────────────────────
_default_origins = [
    "http://localhost:5173",
    "http://localhost:5174",
    "http://localhost:3000",
    "http://127.0.0.1:5173",
]
_env_origins = [o.strip() for o in (os.getenv("FRONTEND_ORIGINS", "")).split(",") if o.strip()]

app.add_middleware(
    CORSMiddleware,
    allow_origins=sorted(set(_default_origins + _env_origins)),
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ── Routers ───────────────────────────────────────────────────────────────────
app.include_router(auth.router)
app.include_router(companies.router)
app.include_router(pipeline.router)
app.include_router(approvals.router)
app.include_router(evidence.router)
app.include_router(config.router)
app.include_router(analytics.router)


@app.on_event("startup")
async def on_startup():
    """Initialize DB tables and seed default users on first start."""
    init_db()
    seed_default_users()


@app.get("/api/health")
def health():
    return {"status": "ok", "service": "Rubicr Caetis API v1.0"}


@app.get("/api/search/companies")
async def search_companies(q: str = Query(..., min_length=1), per_page: int = 20):
    """Search companies using GLEIF LEI API (free, no key required)."""
    results = []

    async with httpx.AsyncClient(timeout=15) as client:
        # Step 1: fuzzycompletions → returns up to 10 name-matched suggestions with LEI IDs
        fuzzy_task = client.get(
            "https://api.gleif.org/api/v1/fuzzycompletions",
            params={"field": "entity.legalName", "q": q},
            headers={"Accept": "application/vnd.api+json"},
        )
        # Step 2: direct lei-records search by exact/partial name (up to per_page results)
        direct_task = client.get(
            "https://api.gleif.org/api/v1/lei-records",
            params={"filter[entity.legalName]": q.upper(), "page[size]": per_page},
            headers={"Accept": "application/vnd.api+json"},
        )
        fuzzy_resp, direct_resp = await asyncio.gather(fuzzy_task, direct_task)

        seen_leis: set = set()

        # Collect LEI IDs from fuzzycompletions
        lei_ids = []
        if fuzzy_resp.status_code == 200:
            for c in fuzzy_resp.json().get("data", []):
                lei_id = c.get("relationships", {}).get("lei-records", {}).get("data", {}).get("id", "")
                if lei_id and lei_id not in lei_ids:
                    lei_ids.append(lei_id)

        # Batch-fetch all fuzzy LEI IDs in one request
        if lei_ids:
            batch_resp = await client.get(
                "https://api.gleif.org/api/v1/lei-records",
                params={"filter[lei]": ",".join(lei_ids), "page[size]": len(lei_ids)},
                headers={"Accept": "application/vnd.api+json"},
            )
            if batch_resp.status_code == 200:
                for rec in batch_resp.json().get("data", []):
                    _append_record(rec, results, seen_leis)

        # Add direct name-match results (deduped)
        if direct_resp.status_code == 200:
            for rec in direct_resp.json().get("data", []):
                _append_record(rec, results, seen_leis)

    return {"results": {"companies": results, "total_count": len(results)}}


def _append_record(rec: dict, results: list, seen_leis: set) -> None:
    """Extract fields from a GLEIF lei-record and append to results if not seen."""
    attrs = rec.get("attributes", {})
    lei = attrs.get("lei", "")
    if not lei or lei in seen_leis:
        return
    seen_leis.add(lei)
    entity = attrs.get("entity", {})
    registration = attrs.get("registration", {})
    addr = entity.get("legalAddress", {})

    # Company registration number from local registry (more meaningful than LEI)
    company_number = entity.get("registeredAs") or ""

    # Registry authority name (e.g. MCA21 for India, Companies House for UK)
    authority_info = entity.get("registeredAt") or {}
    authority = authority_info.get("id", "") if isinstance(authority_info, dict) else ""

    # Country / jurisdiction
    jurisdiction = entity.get("jurisdiction") or addr.get("country", "")

    # Entity status — GLEIF uses ACTIVE / INACTIVE; map to human-readable
    raw_status = entity.get("status", "")
    current_status = {"ACTIVE": "Active", "INACTIVE": "Inactive"}.get(raw_status.upper(), raw_status.title()) if raw_status else ""

    # Legal form short name
    legal_form = (entity.get("legalForm") or {}).get("other") or ""

    # Registration (LEI record) status — ISSUED means live
    reg_status = registration.get("status", "")
    if reg_status == "LAPSED" and current_status == "Active":
        current_status = "Lapsed"

    results.append({
        "name": entity.get("legalName", {}).get("name", lei),
        "lei": lei,
        "address": ", ".join(filter(None, [
            ", ".join(addr.get("addressLines", [])),
            addr.get("city", ""),
            addr.get("country", ""),
        ])),
        "ticker": "",
        "region": _country_to_region(addr.get("country", "")),
        "sector": entity.get("category", "Unknown"),
        "jurisdiction": jurisdiction,
        "incorporation_date": (entity.get("creationDate") or "")[:10],  # strip time
        "company_number": company_number,
        "current_status": current_status,
        "legal_form": legal_form,
        "registry_authority": authority,
        "gleif_url": f"https://search.gleif.org/#/record/{lei}",
    })


@app.get("/api/search/gleif")
async def search_gleif(q: str = Query(..., min_length=1), per_page: int = 20):
    """Alias for /api/search/companies — GLEIF LEI Registry search."""
    return await search_companies(q=q, per_page=per_page)


@app.get("/api/search/nse-symbol")
async def search_nse_symbol(q: str = Query(..., min_length=1)):
    """
    Auto-discover stock ticker for a company name using Yahoo Finance search.
    Works for ANY globally listed company — returns NSE (.NS), BSE (.BO),
    NYSE, NASDAQ, LSE (.L), ASX (.AX), etc.
    e.g. GET /api/search/nse-symbol?q=Tata+Motors  →  {"symbol": "TATAMOTORS", "exchange": "NSE"}
         GET /api/search/nse-symbol?q=Apple        →  {"symbol": "AAPL",        "exchange": "NMS"}
         GET /api/search/nse-symbol?q=Shell        →  {"symbol": "SHEL",        "exchange": "NYQ"}
    """
    try:
        headers = {
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/120.0.0.0 Safari/537.36"
            )
        }
        async with httpx.AsyncClient(timeout=10) as client:
            resp = await client.get(
                "https://query2.finance.yahoo.com/v1/finance/search",
                params={"q": q, "quotesCount": 10, "newsCount": 0, "listsCount": 0},
                headers=headers,
            )
        if resp.status_code != 200:
            return {"symbol": None, "exchange": None, "matches": []}

        quotes = resp.json().get("quotes", [])
        matches = []
        # Priority order: NSE (.NS) > BSE (.BO) > any other equity
        nse_match = None
        bse_match = None
        global_match = None

        for quote in quotes:
            if quote.get("quoteType") not in ("EQUITY", "ETF", None):
                continue
            sym = quote.get("symbol", "")
            exch = quote.get("exchange", "")
            name_str = quote.get("longname") or quote.get("shortname") or ""
            if ".NS" in sym:
                clean = sym.replace(".NS", "")
                entry = {"symbol": clean, "exchange": "NSE", "full_symbol": sym, "name": name_str}
                matches.append(entry)
                if nse_match is None:
                    nse_match = entry
            elif ".BO" in sym:
                clean = sym.replace(".BO", "")
                entry = {"symbol": clean, "exchange": "BSE", "full_symbol": sym, "name": name_str}
                matches.append(entry)
                if bse_match is None:
                    bse_match = entry
            elif sym and exch:
                entry = {"symbol": sym, "exchange": exch, "full_symbol": sym, "name": name_str}
                matches.append(entry)
                if global_match is None:
                    global_match = entry

        best = nse_match or bse_match or global_match
        if best:
            return {"symbol": best["symbol"], "exchange": best["exchange"],
                    "full_symbol": best["full_symbol"], "matches": matches}
        return {"symbol": None, "exchange": None, "matches": matches}
    except Exception as exc:
        return {"symbol": None, "exchange": None, "matches": [], "error": str(exc)}


def _country_to_region(country: str) -> str:
    country = country.upper()
    if country in {"US", "CA", "MX"}: return "NA"
    if country in {"GB", "DE", "FR", "NL", "ES", "IT", "SE", "CH", "BE", "NO", "DK", "FI", "PL", "PT", "AT", "IE"}: return "EU"
    if country in {"IN", "CN", "JP", "AU", "SG", "HK", "KR", "MY", "TH", "ID", "NZ", "PH"}: return "APAC"
    if country in {"BR", "AR", "CL", "CO", "PE", "MX"}: return "LATAM"
    return "EMEA"


# ── GET /api/search/opencorporates ────────────────────────────────────────────

@app.get("/api/search/opencorporates")
def search_opencorporates(
    q: str = Query(..., min_length=1),
    jurisdiction: Optional[str] = None,
    per_page: int = Query(default=20, ge=1, le=100),
):
    """Search companies using the OpenCorporates API.

    Requires OPENCORPORATES_API_TOKEN to be set in the environment.
    Get your token at https://opencorporates.com
    """
    from backend.scraper.opencorporates import search_companies

    api_token = os.environ.get("OPENCORPORATES_API_TOKEN") or None
    if not api_token:
        raise HTTPException(
            status_code=422,
            detail=(
                "OpenCorporates API Token not configured. "
                "Get your token at https://opencorporates.com, then set:\n"
                "    export OPENCORPORATES_API_TOKEN=your_token\n"
                "and restart the backend."
            ),
        )
    try:
        results = search_companies(
            query=q,
            jurisdiction_code=jurisdiction.lower() if jurisdiction else None,
            per_page=per_page,
            api_token=api_token,
        )
        return {"results": {"companies": results, "total_count": len(results)}}
    except RuntimeError as exc:
        raise HTTPException(status_code=422, detail=str(exc))
