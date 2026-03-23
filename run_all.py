#!/usr/bin/env python3
"""
run_all.py
----------
Smart interactive runner for the Impactree backend.

Modes:
  1. INTERACTIVE (default) — enter a company name, choose current year OR all years,
     then the full pipeline runs automatically. Type 'done' when finished.
  2. BATCH — process the built-in COMPANIES list (or a subset) in one go.

Usage:
    python run_all.py                             # interactive: prompted for company + years
    python run_all.py --all-years                 # interactive: always fill all years
    python run_all.py --batch                     # batch: run all built-in companies
    python run_all.py --batch --all-years         # batch: every company, every year
    python run_all.py --batch --companies TCS     # batch: specific company
    python run_all.py --year 2026                 # override latest fiscal year
    python run_all.py --num-years 5               # how many past years to fill (default 5)
    python run_all.py --skip-download             # reuse previously downloaded PDFs

Examples:
    python run_all.py                             # interactive, prompts all options
    python run_all.py --all-years                 # interactive, fills all 5 years per company
    python run_all.py --batch --all-years         # batch all companies x all years
    python run_all.py --batch --companies "Reliance Industries" --all-years
"""

import sys
import os
import argparse
import datetime
import re
import requests
import sqlite3
import zipfile
from io import BytesIO
from pathlib import Path
from urllib.parse import quote, unquote, urlparse, parse_qs

# ── make backend importable ────────────────────────────────────────────────────
ROOT = Path(__file__).parent
sys.path.insert(0, str(ROOT))

# Avoid Windows cp1252 crashes when printing Unicode banner characters.
try:
    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    if hasattr(sys.stderr, "reconfigure"):
        sys.stderr.reconfigure(encoding="utf-8", errors="replace")
except Exception:
    pass

# ── Built-in company list (used in --batch mode) ───────────────────────────────
# "nse_symbol" is the NSE ticker used to look up the annual report PDF.
# "name"       is the name passed to the CLI scraper.

COMPANIES = [
    {"name": "Tata Consultancy Services",  "nse_symbol": "TCS"},
    {"name": "HCL Technologies",           "nse_symbol": "HCLTECH"},
    {"name": "Infosys",                    "nse_symbol": "INFY"},
    {"name": "Wipro",                      "nse_symbol": "WIPRO"},
    {"name": "Reliance Industries",        "nse_symbol": "RELIANCE"},
]

# Auto-detect fiscal year:
#   Indian FY runs April→March, so after April 1 the new FY begins.
#   e.g. running in Jan-Mar 2026 → FY 2026  |  Apr-Dec 2026 → FY 2027
_today = datetime.date.today()
FISCAL_YEAR = _today.year if _today.month >= 4 else _today.year
# Override here if you always want a specific year:
# FISCAL_YEAR = 2026

PDF_DIR = ROOT / "data" / "annual_reports"  # company-specific PDF storage root


def _slugify_company_name(name: str) -> str:
    """Create a filesystem-safe folder name from a company name."""
    slug = re.sub(r"[^A-Za-z0-9]+", "_", (name or "company").strip())
    slug = slug.strip("_")
    return slug or "company"

# ── NSE PDF download ───────────────────────────────────────────────────────────

NSE_BASE    = "https://www.nseindia.com/api/annual-reports"
NSE_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    ),
    "Accept":   "application/json",
    "Referer":  "https://www.nseindia.com/",
}


def _fetch_annual_report_entries(nse_symbol: str) -> list:
    """Fetch annual-report metadata from NSE for a given symbol."""
    print(f"  [NSE] Fetching annual report list for {nse_symbol} …")
    try:
        resp = requests.get(
            NSE_BASE,
            params={"index": "equities", "symbol": nse_symbol},
            headers=NSE_HEADERS,
            timeout=20,
        )
        resp.raise_for_status()
        data = resp.json()
    except Exception as exc:
        print(f"  [ERROR] NSE API call failed: {exc}")
        return []

    entries = data.get("data") or []
    if not entries:
        print(f"  [ERROR] No annual report entries found for {nse_symbol}")
        return []
    return entries


def _extract_entry_year(entry: dict) -> int | None:
    """Best-effort conversion of NSE `toYr` metadata to integer year."""
    raw = str(entry.get("toYr", "")).strip()
    if not raw:
        return None
    # Typical cases: "2026", "FY2026", "2025-26"
    m = re.search(r"(20\d{2})", raw)
    if m:
        return int(m.group(1))
    m = re.search(r"(\d{2})$", raw)
    if m:
        return 2000 + int(m.group(1))
    return None


def _download_pdf(nse_symbol: str, dest: Path, target_year: int | None = None) -> bool:
    """Download annual report PDF from NSE (specific FY when target_year is provided)."""
    entries = _fetch_annual_report_entries(nse_symbol)
    if not entries:
        return False

    selected = None
    if target_year is not None:
        for entry in entries:
            if _extract_entry_year(entry) == target_year:
                selected = entry
                break
        if not selected:
            print(f"  [WARN] No NSE annual report found for {nse_symbol} FY{target_year}")
            return False
    else:
        selected = entries[0]

    pdf_url = selected.get("fileName") or selected.get("fileUrl")
    year_label = selected.get("toYr", "?")
    if not pdf_url:
        print(f"  [ERROR] Could not find PDF URL in NSE response for {nse_symbol}")
        return False

    print(f"  [NSE] Downloading FY{year_label} report: {pdf_url}")
    dl_headers = {**NSE_HEADERS, "Accept": "application/pdf,application/zip,*/*", "Accept-Encoding": "identity"}
    try:
        r = requests.get(pdf_url, headers=dl_headers, timeout=300)
        r.raise_for_status()
        payload = r.content
        total = len(payload)
        ctype = (r.headers.get("Content-Type") or "").lower()
        is_zip = pdf_url.lower().endswith(".zip") or ("zip" in ctype)

        if is_zip:
            zf = zipfile.ZipFile(BytesIO(payload))
            pdf_names = [n for n in zf.namelist() if n.lower().endswith(".pdf")]
            if not pdf_names:
                print("  [WARN] ZIP downloaded but no PDF found inside")
                return False
            # Pick the largest PDF entry as primary report document.
            best_pdf = max(pdf_names, key=lambda n: zf.getinfo(n).file_size)
            with zf.open(best_pdf) as src, open(dest, "wb") as out:
                out.write(src.read())
            print(f"  [OK]   Extracted PDF from ZIP ({best_pdf}) → {dest}")
        else:
            with open(dest, "wb") as f:
                f.write(payload)
            print(f"  [OK]   Saved {total:,} bytes → {dest}")
        return True
    except Exception as exc:
        print(f"  [ERROR] Download failed: {exc}")
        return False


def _is_likely_report_pdf(url: str, context: str, target_year: int) -> bool:
    """Heuristic filter for annual/sustainability report PDF links."""
    text = f"{url} {context}".lower()
    if ".pdf" not in text:
        return False
    report_terms = [
        "annual report", "annual-report", "integrated report", "sustainability report",
        "ar_", "investor", "report",
    ]
    if not any(t in text for t in report_terms):
        return False
    if str(target_year) not in text and str(target_year - 1) not in text:
        # keep strict to improve year precision and avoid random PDFs
        return False
    return True


def _extract_real_url(candidate: str) -> str:
    """Resolve wrapped search-engine links to direct target URLs where possible."""
    try:
        parsed = urlparse(candidate)
        if "duckduckgo.com" in parsed.netloc and parsed.path.startswith("/l/"):
            qs = parse_qs(parsed.query)
            if "uddg" in qs and qs["uddg"]:
                return unquote(qs["uddg"][0])
    except Exception:
        pass
    return candidate


def _download_pdf_from_web(company_name: str, target_year: int, dest: Path) -> bool:
    """Fallback: search the web for a year-matching annual report PDF and download it."""
    try:
        from bs4 import BeautifulSoup
    except Exception:
        return False

    queries = [
        f'"{company_name}" "annual report" "{target_year}" filetype:pdf',
        f'"{company_name}" "integrated report" "{target_year}" filetype:pdf',
        f'"{company_name}" "sustainability report" "{target_year}" filetype:pdf',
    ]
    web_headers = {
        "User-Agent": NSE_HEADERS["User-Agent"],
        "Accept-Language": "en-US,en;q=0.9",
    }

    for q in queries:
        try:
            print(f"  [WEB]  Searching report links: {q}")
            search_url = f"https://duckduckgo.com/html/?q={quote(q)}"
            resp = requests.get(search_url, headers=web_headers, timeout=20)
            if resp.status_code != 200:
                continue
            soup = BeautifulSoup(resp.text, "lxml")

            # Collect candidate links from result anchors.
            candidates = []
            for a in soup.select("a"):
                href = (a.get("href") or "").strip()
                text = a.get_text(" ", strip=True)
                if not href:
                    continue
                real = _extract_real_url(href)
                if not real.startswith("http"):
                    continue
                if _is_likely_report_pdf(real, text, target_year):
                    candidates.append(real)

            # Try candidates in order and keep the first valid PDF.
            for url in candidates[:10]:
                try:
                    r = requests.get(url, headers=web_headers, timeout=120, stream=True)
                    if r.status_code != 200:
                        continue
                    ctype = (r.headers.get("Content-Type") or "").lower()
                    if "pdf" not in ctype and not url.lower().split("?")[0].endswith(".pdf"):
                        continue

                    total = 0
                    with open(dest, "wb") as f:
                        for chunk in r.iter_content(65536):
                            if chunk:
                                f.write(chunk)
                                total += len(chunk)
                    if total < 150_000:
                        # Tiny files are often not actual annual reports.
                        continue

                    # Validate PDF readability quickly.
                    try:
                        import pypdf
                        reader = pypdf.PdfReader(str(dest))
                        if len(reader.pages) < 5:
                            continue
                    except Exception:
                        continue

                    print(f"  [WEB]  Downloaded report PDF ({total:,} bytes): {url}")
                    return True
                except Exception:
                    continue
        except Exception:
            continue

    return False


# ── per-company pipeline (split into scrape + questionnaire) ──────────────────

def _scrape_company(name: str, nse_symbol: str, latest_year: int,
                    skip_download: bool, years_to_process: list[int] | None = None,
                    target_company_id: int | None = None) -> dict:
    """
    Step 1-3: Download PDF (if NSE-listed), then scrape Yahoo Finance data.
    PDF step is OPTIONAL — if it fails we log a warning and continue with
    Yahoo Finance / financial data, which works for ANY globally listed company.
    Returns dict with keys: company_name, official_name, ticker,
                            brsr_fields, history_years, error
    """
    company_folder = PDF_DIR / _slugify_company_name(name)
    company_folder.mkdir(parents=True, exist_ok=True)
    safe_name = nse_symbol.upper() if nse_symbol else "ANNUAL_REPORT"
    years_to_process = sorted(set(years_to_process or [latest_year]))
    result    = {"company_name": name, "official_name": name,
                 "brsr_fields": 0, "brsr_fields_by_year": {}, "history_years": [], "error": None}

    # ── 3. Scrape — always runs regardless of PDF availability ──────────────
    print(f"  [SCRAPE] Fetching company data from Yahoo Finance …")
    from backend.database.db import init_db, get_session
    from backend.database.models import Company, ScrapedData
    from backend.scraper.company_scraper import CompanyScraper
    from backend.scraper.financial_scraper import FinancialScraper
    from backend.scraper.brsr_scraper import BRSRScraper
    from sqlalchemy.dialects.sqlite import insert as sqlite_insert

    init_db()
    db = get_session()

    scraper       = CompanyScraper(name, ticker_hint=nse_symbol)
    info          = scraper.get_company_info() or {}
    # Fallback ticker when Yahoo search/profile is sparse.
    if not info.get("ticker") and nse_symbol:
        hint = nse_symbol.upper()
        info["ticker"] = hint if "." in hint else f"{hint}.NS"
    official_name = info.get("official_name") or name
    ticker_val    = info.get("ticker") or ""
    result["official_name"] = official_name
    result["ticker"]        = ticker_val

    company = None
    if target_company_id is not None:
        company = db.query(Company).filter(Company.id == int(target_company_id)).first()
    if company is None:
        company = (
            db.query(Company).filter(Company.name == official_name).first()
            or db.query(Company).filter(Company.ticker == ticker_val, ticker_val != "").first()
            or db.query(Company).filter(Company.name.ilike(f"%{name}%")).first()
        )
    if not company:
        company = Company(
            name=official_name,
            sector=info.get("sector"),
            industry=info.get("industry"),
            exchange=info.get("exchange"),
            ticker=info.get("ticker"),
            website=info.get("website"),
            headquarters=info.get("headquarters"),
            description=(info.get("description") or "")[:500],
        )
        db.add(company)
    else:
        if target_company_id is None:
            company.name = official_name
        company.sector       = info.get("sector")       or company.sector
        company.industry     = info.get("industry")     or company.industry
        company.exchange     = info.get("exchange")     or company.exchange
        company.ticker       = info.get("ticker")       or company.ticker
        company.website      = info.get("website")      or company.website
        company.headquarters = info.get("headquarters") or company.headquarters
    db.commit()

    def _upsert(key, value, source="yahoo", yr=latest_year):
        if value is None:
            return
        stmt = sqlite_insert(ScrapedData).values(
            company_id=company.id,
            year=yr,
            source=source,
            data_key=key,
            data_value=str(value),
            scraped_at=datetime.datetime.utcnow(),
        )
        stmt = stmt.on_conflict_do_update(
            index_elements=["company_id", "year", "source", "data_key"],
            set_={
                "data_value": str(value),
                "scraped_at": datetime.datetime.utcnow(),
            },
        )
        db.execute(stmt)

    for k, v in info.items():
        if v is not None:
            _upsert(k, str(v), source="yahoo")
    db.commit()

    # Historical financials (covers past 5 years automatically)
    ticker = info.get("ticker")
    history_years = []
    if ticker:
        fin     = FinancialScraper(ticker, name)
        history = fin.get_historical_financials(years=5)
        if history:
            for fy, metrics in history.items():
                for mk, mv in metrics.items():
                    if mv is not None:
                        _upsert(mk, str(mv), source="yahoo_historical", yr=fy)
            db.commit()
            history_years = sorted(history.keys())
            print(f"  [FIN]   Historical data saved for years: {history_years}")

        esg = fin.get_esg_scores()
        if esg:
            for k, v in esg.items():
                _upsert(k, v, source="yahoo_esg")
            db.commit()

    # BRSR PDF — process each selected year separately when available.
    brsr_fields_by_year: dict[int, int] = {}
    if nse_symbol:
        for yr in years_to_process:
            pdf_path = company_folder / f"{safe_name}_FY{yr}_annual.pdf"
            pdf_available = False

            if skip_download and pdf_path.exists():
                print(f"  [SKIP]  Using cached PDF for FY{yr}: {pdf_path}")
                pdf_available = True
            else:
                ok = _download_pdf(nse_symbol, pdf_path, target_year=yr)
                if not ok:
                    # Deep web fallback when exchange filing is missing/unavailable.
                    ok = _download_pdf_from_web(name, yr, pdf_path)
                if ok:
                    pdf_available = True

            if pdf_available:
                try:
                    import pypdf
                    reader = pypdf.PdfReader(str(pdf_path))
                    print(f"  [PDF]   FY{yr} verified OK ({len(reader.pages)} pages)")
                except Exception as exc:
                    print(f"  [WARN]  FY{yr} cached PDF unreadable: {exc} — retrying download")
                    pdf_available = False
                    try:
                        if pdf_path.exists():
                            pdf_path.unlink()
                    except Exception:
                        pass
                    ok = _download_pdf(nse_symbol, pdf_path, target_year=yr)
                    if not ok:
                        ok = _download_pdf_from_web(name, yr, pdf_path)
                    if ok:
                        try:
                            import pypdf
                            reader = pypdf.PdfReader(str(pdf_path))
                            print(f"  [PDF]   FY{yr} verified OK after retry ({len(reader.pages)} pages)")
                            pdf_available = True
                        except Exception as exc2:
                            print(f"  [WARN]  FY{yr} retry PDF unreadable: {exc2} — skipping this year")

            if not pdf_available:
                brsr_fields_by_year[yr] = 0
                continue

            brsr = BRSRScraper(name, ticker=ticker)
            brsr_data = brsr.parse_local_pdf(str(pdf_path))
            if brsr_data:
                for k, v in brsr_data.items():
                    _upsert(k, v, source="brsr_pdf", yr=yr)
                db.commit()
                brsr_fields_by_year[yr] = len(brsr_data)
                print(f"  [BRSR]  FY{yr}: {len(brsr_data)} fields extracted")
            else:
                brsr_fields_by_year[yr] = 0
                print(f"  [BRSR]  FY{yr}: no fields extracted")
    else:
        print(f"  [BRSR]  Skipped (no symbol available) — Yahoo Finance data only")
        for yr in years_to_process:
            brsr_fields_by_year[yr] = 0

    result["brsr_fields_by_year"] = brsr_fields_by_year
    result["brsr_fields"] = brsr_fields_by_year.get(latest_year, 0)

    result["history_years"] = history_years
    result["company_id"] = company.id
    result["company_name"] = company.name
    return result


def _fill_questionnaire(company_name: str, year: int, company_id: int | None = None) -> dict:
    """
    Step 4-5: Auto-fill questionnaire for one year and return answer counts.
    """
    print(f"  [QUEST] Year {year} — strict auto-fill from extracted data only …")
    from backend.questionnaire.engine import QuestionnaireEngine
    from backend.database.db import get_session
    from backend.database.models import Company as _Company
    company_name_for_engine = company_name
    if company_id is not None:
        _db_name = get_session()
        exact_name_row = _db_name.query(_Company).filter_by(id=company_id).first()
        if exact_name_row and exact_name_row.name:
            company_name_for_engine = exact_name_row.name
        _db_name.close()

    engine = QuestionnaireEngine(
        company_name_for_engine,
        year,
        standard="ALL",
        allow_historical_prefill=False,
        allow_smart_defaults=False,
        allow_cross_year_financial_fallback=False,
        allow_online_provisional_fallback=True,
    )
    engine.setup()
    if company_id is not None and (not engine.company or engine.company.id != company_id):
        _db = get_session()
        exact = _db.query(_Company).filter_by(id=company_id).first()
        if exact:
            engine.company = exact
        _db.close()
    engine.run_auto(module_filter=None)

    conn = sqlite3.connect(str(ROOT / "data" / "impactree.db"))
    cur  = conn.cursor()
    if company_id is not None:
        cur.execute("""
            SELECT ans.source, COUNT(*)
            FROM answers ans
            JOIN questionnaire_sessions qs ON ans.session_id = qs.id
            WHERE qs.year = ? AND qs.company_id = ?
            GROUP BY ans.source
        """, (year, company_id))
    else:
        cur.execute("""
            SELECT ans.source, COUNT(*)
            FROM answers ans
            JOIN questionnaire_sessions qs ON ans.session_id = qs.id
            JOIN companies co ON qs.company_id = co.id
            WHERE qs.year = ? AND co.name = ?
            GROUP BY ans.source
        """, (year, company_name))
    counts = dict(cur.fetchall())
    conn.close()

    total    = sum(counts.values())
    defaults = counts.get("smart_default", 0)
    real     = sum(v for k, v in counts.items() if k != "smart_default")
    status   = "✓"
    src_str  = "  |  ".join(f"{s}: {n}" for s, n in sorted(counts.items()))
    print(f"  {status} Year {year}: {total} answers  ({src_str})")

    return {"year": year, "answered": total, "real": real,
            "smart_defaults": defaults, "counts": counts}


def run_company(name: str, nse_symbol: str, year: int, skip_download: bool,
                target_company_id: int | None = None,
                fill_questionnaire: bool = True) -> dict:
    """Full pipeline for one company, one year."""
    print()
    print("=" * 65)
    print(f"  COMPANY : {name}  ({nse_symbol or 'no NSE symbol'})  |  Year: {year}")
    print("=" * 65)

    scrape = _scrape_company(
        name,
        nse_symbol,
        year,
        skip_download,
        years_to_process=[year],
        target_company_id=target_company_id,
    )
    # Never abort on PDF errors — Yahoo Finance data is still useful
    # Only abort on truly unexpected exceptions (scrape result will lack keys)
    if scrape.get("error") and scrape.get("_fatal"):
        return {"name": name, "year": year, "error": scrape["error"],
                "answered": 0, "smart_defaults": 0, "scraped": 0}

    if not fill_questionnaire:
        return {"name": name, "year": year, "error": None,
                "answered": 0, "smart_defaults": 0,
                "scraped_count": 0, "scraped": scrape["brsr_fields"]}

    q = _fill_questionnaire(scrape["official_name"], year, company_id=scrape.get("company_id"))
    return {"name": name, "year": year, "error": None,
            "answered": q["answered"], "smart_defaults": q["smart_defaults"],
            "scraped_count": q["real"], "scraped": scrape["brsr_fields"]}


def run_company_all_years(name: str, nse_symbol: str, latest_year: int,
                          num_years: int, skip_download: bool,
                          target_company_id: int | None = None,
                          fill_questionnaire: bool = True) -> list:
    """
    Scrape the company ONCE, then auto-fill the questionnaire for EACH year
    from (latest_year - num_years + 1) up to latest_year.
    Returns a list of per-year result dicts.
    """
    print()
    print("=" * 65)
    print(f"  COMPANY : {name}  ({nse_symbol})")
    print(f"  YEARS   : {latest_year - num_years + 1} → {latest_year}  ({num_years} years)")
    print("=" * 65)

    years_to_fill = list(range(latest_year - num_years + 1, latest_year + 1))
    return run_company_selected_years(
        name=name,
        nse_symbol=nse_symbol,
        years_to_fill=years_to_fill,
        skip_download=skip_download,
        target_company_id=target_company_id,
        fill_questionnaire=fill_questionnaire,
    )


def run_company_selected_years(name: str, nse_symbol: str,
                               years_to_fill: list[int], skip_download: bool,
                               target_company_id: int | None = None,
                               fill_questionnaire: bool = True) -> list:
    """Process the company for an explicit list of years (exact frontend selection)."""
    years_to_fill = sorted(set(years_to_fill))
    latest_year = max(years_to_fill)

    print()
    print("=" * 65)
    print(f"  COMPANY : {name}  ({nse_symbol})")
    print(f"  YEARS   : {years_to_fill}")
    print("=" * 65)

    scrape = _scrape_company(
        name,
        nse_symbol,
        latest_year,
        skip_download,
        years_to_process=years_to_fill,
        target_company_id=target_company_id,
    )
    if scrape.get("error") and scrape.get("_fatal"):
        return [{"name": name, "year": yr, "error": scrape["error"],
                 "answered": 0, "smart_defaults": 0, "scraped": 0}
                for yr in years_to_fill]

    official_name = scrape["official_name"]

    if not fill_questionnaire:
        return [
            {
                "name": name,
                "year": yr,
                "error": None,
                "answered": 0,
                "smart_defaults": 0,
                "scraped_count": 0,
                "scraped": (scrape.get("brsr_fields_by_year") or {}).get(yr, 0),
            }
            for yr in years_to_fill
        ]

    print()
    print(f"  Filling questionnaire year by year: {years_to_fill}")
    print()

    results = []
    for yr in years_to_fill:
        q = _fill_questionnaire(official_name, yr, company_id=scrape.get("company_id"))
        results.append({
            "name":           name,
            "year":           yr,
            "error":          None,
            "answered":       q["answered"],
            "smart_defaults": q["smart_defaults"],
            "scraped_count":  q["real"],
            # BRSR fields are now extracted per selected year when PDFs are available
            "scraped":        (scrape.get("brsr_fields_by_year") or {}).get(yr, 0),
        })

    return results


# ── summary table ──────────────────────────────────────────────────────────────

def print_summary(results: list, multi_year: bool = False):
    """
    results: flat list of dicts, each with keys:
             name, year, answered, real/scraped_count, smart_defaults, scraped, error
    """
    years = sorted({r["year"] for r in results})
    title = (f"ALL YEARS  ({min(years)} → {max(years)})"
             if multi_year and len(years) > 1
             else f"Fiscal Year {years[0]}")
    print()
    print("=" * 72)
    print(f"  FINAL SUMMARY  —  {title}")
    print("=" * 72)
    if multi_year and len(years) > 1:
        hdr = f"  {'Company':<30} {'Year':>6} {'Answered':>8}  {'Real':>6}  {'Default':>7}  {'BRSR':>5}"
    else:
        hdr = f"  {'Company':<35} {'Answered':>8}  {'Real':>6}  {'Default':>7}  {'BRSR':>5}"
    print(hdr)
    print("  " + "-" * 68)
    for r in results:
        if r.get("error"):
            if multi_year and len(years) > 1:
                print(f"  ✗ {r['name']:<29} {r['year']:>6}  ERROR: {r['error']}")
            else:
                print(f"  ✗ {r['name']:<34}  ERROR: {r['error']}")
        else:
            status = "✓" if r["smart_defaults"] == 0 else "⚠"
            if multi_year and len(years) > 1:
                print(
                    f"  {status} {r['name']:<29}"
                    f" {r['year']:>6}"
                    f" {r['answered']:>8}"
                    f"  {r.get('scraped_count', 0):>6}"
                    f"  {r['smart_defaults']:>7}"
                    f"  {r['scraped']:>5}"
                )
            else:
                print(
                    f"  {status} {r['name']:<34}"
                    f" {r['answered']:>8}"
                    f"  {r.get('scraped_count', 0):>6}"
                    f"  {r['smart_defaults']:>7}"
                    f"  {r['scraped']:>5}"
                )
    print("=" * 72)


# ── NSE symbol lookup helper ───────────────────────────────────────────────────

def _guess_nse_symbol(name: str) -> str:
    """
    Try to derive an NSE symbol from a company name by checking the NSE API.
    Returns the guessed symbol, or the uppercased first word as a fallback.
    """
    # Common known mappings to help users
    KNOWN = {
        "tcs": "TCS", "tata consultancy": "TCS",
        "hcl": "HCLTECH", "hcl tech": "HCLTECH",
        "infosys": "INFY",
        "wipro": "WIPRO",
        "reliance": "RELIANCE", "reliance industries": "RELIANCE",
        "hdfc bank": "HDFCBANK", "hdfc": "HDFCBANK",
        "icici": "ICICIBANK", "icici bank": "ICICIBANK",
        "sbi": "SBIN", "state bank": "SBIN",
        "ongc": "ONGC",
        "itc": "ITC",
        "bajaj": "BAJFINANCE",
        "maruti": "MARUTI",
        "tata motors": "TATAMOTORS",
        "tata steel": "TATASTEEL",
        "sun pharma": "SUNPHARMA",
        "asian paints": "ASIANPAINT",
        "kotak": "KOTAKBANK",
        "larsen": "LT", "l&t": "LT",
        "ultratech": "ULTRACEMCO",
        "titan": "TITAN",
        "nestlé": "NESTLEIND", "nestle": "NESTLEIND",
        "power grid": "POWERGRID",
        "ntpc": "NTPC",
        "tech mahindra": "TECHM",
        "m&m": "M&M", "mahindra": "M&M",
        "axis bank": "AXISBANK",
        "bharti airtel": "BHARTIARTL", "airtel": "BHARTIARTL",
        "adani": "ADANIENT",
        "jio": "JIOFIN",
    }
    key = name.strip().lower()
    for k, v in KNOWN.items():
        if k in key:
            return v
    # Fallback: first word of name uppercased
    return name.strip().split()[0].upper()


# ── interactive mode ───────────────────────────────────────────────────────────

def run_interactive(latest_year: int, num_years: int,
                    force_all_years: bool, skip_download: bool):
    """Prompt the user to enter companies one by one, process each immediately."""
    from backend.database.db import init_db
    init_db()

    print()
    print("╔══════════════════════════════════════════════════════════════╗")
    print("║       IMPACTREE  —  Smart Interactive Runner                 ║")
    print("╠══════════════════════════════════════════════════════════════╣")
    print("║  Enter a company name to process it automatically.           ║")
    print("║  Type  'list'  to see built-in companies.                    ║")
    print("║  Type  'done'  (or Ctrl+C) to finish and see summary.        ║")
    print("╚══════════════════════════════════════════════════════════════╝")
    years_range = f"{latest_year - num_years + 1}–{latest_year}"
    print(f"  Latest year : {latest_year}  |  Years to fill: {years_range}  |  PDF dir: {PDF_DIR}")
    print()

    all_results   = []   # flat list of per-year dicts
    multi_year    = False

    while True:
        try:
            raw = input("  Enter company name (or 'done'/'list'): ").strip()
        except (EOFError, KeyboardInterrupt):
            print()
            break

        if not raw:
            continue
        if raw.lower() == "done":
            break
        if raw.lower() == "list":
            print()
            print("  Built-in companies:")
            for i, c in enumerate(COMPANIES, 1):
                print(f"    {i}. {c['name']}  ({c['nse_symbol']})")
            print()
            continue

        # Pick from built-in list by number
        if raw.isdigit():
            idx = int(raw) - 1
            if 0 <= idx < len(COMPANIES):
                comp       = COMPANIES[idx]
                name       = comp["name"]
                nse_symbol = comp["nse_symbol"]
                print(f"  → Selected: {name}  ({nse_symbol})")
            else:
                print(f"  [ERROR] Number out of range. Type 'list' to see options.")
                continue
        else:
            name      = raw
            suggested = _guess_nse_symbol(name)
            try:
                sym_raw = input(f"  NSE symbol for '{name}' [{suggested}]: ").strip()
            except (EOFError, KeyboardInterrupt):
                print()
                break
            nse_symbol = sym_raw.upper() if sym_raw else suggested

        # Ask: current year only OR all years?
        do_all_years = force_all_years
        if not force_all_years:
            try:
                ay = input(
                    f"  Fill all years ({years_range})? [Y/n]: "
                ).strip().lower()
                do_all_years = ay not in ("n", "no")
            except (EOFError, KeyboardInterrupt):
                print()
                break

        # Confirm
        yr_display = years_range if do_all_years else str(latest_year)
        print(f"  ┌─────────────────────────────────────────────────┐")
        print(f"  │  Company   : {name:<36}│")
        print(f"  │  NSE Symbol: {nse_symbol:<36}│")
        print(f"  │  Years     : {yr_display:<36}│")
        print(f"  └─────────────────────────────────────────────────┘")
        try:
            confirm = input("  Proceed? [Y/n]: ").strip().lower()
        except (EOFError, KeyboardInterrupt):
            print()
            break
        if confirm in ("n", "no"):
            print("  Skipped.")
            continue

        if do_all_years:
            multi_year = True
            rs = run_company_all_years(
                name=name, nse_symbol=nse_symbol,
                latest_year=latest_year, num_years=num_years,
                skip_download=skip_download,
            )
            all_results.extend(rs)
        else:
            r = run_company(name=name, nse_symbol=nse_symbol,
                            year=latest_year, skip_download=skip_download)
            all_results.append(r)

        print()
        print(f"  ✓ '{name}' processed. Add another company or type 'done'.")
        print()

    if all_results:
        print_summary(all_results, multi_year=multi_year)
    else:
        print("  No companies were processed.")


# ── main ───────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(
        description=(
            "Impactree Smart Runner.\n"
            "Default: interactive — enter a company name, choose years, pipeline runs automatically.\n"
            "Use --batch to process the built-in COMPANIES list non-interactively."
        ),
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        "--batch", action="store_true",
        help="Batch mode: process all built-in companies (non-interactive)"
    )
    parser.add_argument(
        "--all-years", action="store_true",
        help="Fill questionnaire for every available year, not just the latest"
    )
    parser.add_argument(
        "--num-years", type=int, default=5,
        help="How many past years to fill when --all-years is used (default: 5)"
    )
    parser.add_argument(
        "--companies", nargs="*", metavar="NAME",
        help="(Batch mode) Names or NSE symbols to process. "
             'E.g. --companies TCS Wipro "Reliance Industries"'
    )
    parser.add_argument(
        "--year", type=int, default=FISCAL_YEAR,
        help=f"Latest fiscal year (default: {FISCAL_YEAR})"
    )
    parser.add_argument(
        "--years", nargs="*", metavar="YEAR", default=None,
        help="Exact years to process (overrides --all-years/--num-years). E.g. --years 2023 2025 2026"
    )
    parser.add_argument(
        "--skip-download", action="store_true",
        help="Skip PDF download if a company PDF exists in data/annual_reports/"
    )
    parser.add_argument(
        "--nse-symbol", metavar="SYMBOL", default=None,
        help="Explicit NSE stock symbol for PDF download (bypasses built-in list). "
             "E.g. --nse-symbol TATAMOTORS"
    )
    parser.add_argument(
        "--company-id", type=int, default=None,
        help="Pin ingestion and questionnaire fill to this exact Company ID."
    )
    parser.add_argument(
        "--skip-questionnaire", action="store_true",
        help="Scrape only; do not run questionnaire fill inside run_all.py"
    )
    args = parser.parse_args()

    # ── INTERACTIVE MODE (default) ─────────────────────────────────────────
    if not args.batch:
        run_interactive(
            latest_year=args.year,
            num_years=args.num_years,
            force_all_years=args.all_years,
            skip_download=args.skip_download,
        )
        return

    # ── BATCH MODE ─────────────────────────────────────────────────────────
    companies_to_run = COMPANIES
    if args.companies:
        filters = [c.upper() for c in args.companies]
        companies_to_run = [
            c for c in COMPANIES
            if c["nse_symbol"].upper() in filters
            # bidirectional: filter contains name OR name contains filter
            # handles "Tata Consultancy Services Ltd" matching "Tata Consultancy Services"
            or any(
                f in c["name"].upper() or c["name"].upper() in f
                for f in filters
            )
        ]
        if not companies_to_run:
            if args.nse_symbol:
                # Explicit NSE symbol provided — scrape ANY company directly
                company_label = args.companies[0] if args.companies else args.nse_symbol
                print(f"[INFO] Using explicit NSE symbol '{args.nse_symbol}' for '{company_label}'")
                companies_to_run = [{"name": company_label, "nse_symbol": args.nse_symbol}]
            else:
                # No built-in match and no explicit symbol — fallback to smart defaults
                print(f"[INFO] No built-in NSE company matches: {args.companies}")
                print(f"[INFO] Questionnaire will be filled by the API fallback.")
                sys.exit(0)  # exit cleanly so pipeline.py treats it as non-ERROR

    selected_years = sorted({
        int(y) for y in (args.years or [])
        if str(y).strip().isdigit()
    })
    if not selected_years:
        if args.all_years:
            selected_years = list(range(args.year - args.num_years + 1, args.year + 1))
        else:
            selected_years = [args.year]

    years_range = f"{min(selected_years)}–{max(selected_years)}" if len(selected_years) > 1 else str(selected_years[0])
    print()
    print("╔══════════════════════════════════════════════════════════════╗")
    print("║          IMPACTREE  —  Batch Pipeline Runner                 ║")
    print("╚══════════════════════════════════════════════════════════════╝")
    print(f"  Companies : {len(companies_to_run)}")
    print(f"  Years     : {years_range}")
    print(f"  PDF dir   : {PDF_DIR}")
    print(f"  DB        : {ROOT / 'data' / 'impactree.db'}")

    from backend.database.db import init_db
    init_db()

    all_results = []
    for comp in companies_to_run:
        if len(selected_years) > 1:
            rs = run_company_selected_years(
                name=comp["name"],
                nse_symbol=comp["nse_symbol"],
                years_to_fill=selected_years,
                skip_download=args.skip_download,
                target_company_id=args.company_id,
                fill_questionnaire=(not args.skip_questionnaire),
            )
            all_results.extend(rs)
        else:
            r = run_company(
                name=comp["name"],
                nse_symbol=comp["nse_symbol"],
                year=selected_years[0],
                skip_download=args.skip_download,
                target_company_id=args.company_id,
                fill_questionnaire=(not args.skip_questionnaire),
            )
            all_results.append(r)

    print_summary(all_results, multi_year=(len(selected_years) > 1))


if __name__ == "__main__":
    main()
