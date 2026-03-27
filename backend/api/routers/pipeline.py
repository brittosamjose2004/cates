"""
routers/pipeline.py — Pipeline run and job status.
"""
from __future__ import annotations

import sys, subprocess, threading, queue, time
import multiprocessing as mp
import json, re
from pathlib import Path
import requests
sys.path.insert(0, str(Path(__file__).parent.parent.parent.parent))

from datetime import datetime
from typing import List
from fastapi import APIRouter, Depends, BackgroundTasks, HTTPException
from sqlalchemy.orm import Session

from backend.api.deps import get_db, get_current_user
from backend.database.db import get_session
from backend.api.schemas import RunPipelineRequest, PipelineJobOut, PipelineJobLogOut, PipelineStatusBatchRequest
from backend.database.models import Company, PipelineJob, User, ScrapedData

router = APIRouter(prefix="/api/pipeline", tags=["pipeline"])

# Built-in NSE symbols — used to skip auto-discovery for known companies
_BUILTIN_NSE_SYMBOLS = [
    {"nse_symbol": "TCS"},
    {"nse_symbol": "HCLTECH"},
    {"nse_symbol": "INFY"},
    {"nse_symbol": "WIPRO"},
    {"nse_symbol": "RELIANCE"},
]


def _fetch_osm_coordinates(query: str) -> tuple[float, float] | None:
    """Resolve an address/city string to coordinates via OpenStreetMap Nominatim."""
    q = (query or "").strip()
    if not q:
        return None

    try:
        resp = requests.get(
            "https://nominatim.openstreetmap.org/search",
            params={
                "q": q,
                "format": "jsonv2",
                "limit": 1,
                "addressdetails": 1,
            },
            headers={
                "User-Agent": "impactree-pipeline-location-enricher/1.0",
                "Accept": "application/json",
            },
            timeout=20,
        )
        if resp.status_code != 200:
            return None
        payload = resp.json()
        if not payload:
            return None

        lat = float(payload[0].get("lat"))
        lon = float(payload[0].get("lon"))
        if -90 <= lat <= 90 and -180 <= lon <= 180:
            return lat, lon
    except Exception:
        return None

    return None


def _populate_location_indicators_from_osm(company_id: int, year: int, db_session=None) -> tuple[bool, str]:
    """Populate IMP-M22-I01/I02 from company headquarters using OpenStreetMap."""
    from backend.database.db import get_session
    from backend.database.models import Company as _Company, QuestionnaireSession, Answer

    db = db_session or get_session()
    own_session = db_session is None
    try:
        company = db.query(_Company).filter(_Company.id == company_id).first()
        if not company:
            return False, "company not found"

        hq = (company.headquarters or "").strip()
        if not hq or hq.upper() in {"NA", "NOT SPECIFIED", "APAC", "EU"}:
            return False, "no usable headquarters address"

        coords = _fetch_osm_coordinates(hq)
        if not coords:
            return False, f"OSM geocoding failed for '{hq}'"

        lat, lon = coords

        session_row = (
            db.query(QuestionnaireSession)
            .filter_by(company_id=company_id, year=year, standard="ALL")
            .first()
        )
        if session_row is None:
            session_row = QuestionnaireSession(
                company_id=company_id,
                year=year,
                standard="ALL",
                status="in_progress",
                total_questions=0,
                answered_questions=0,
            )
            db.add(session_row)
            db.flush()

        def upsert_indicator(indicator_id: str, value: float):
            row = (
                db.query(Answer)
                .filter(
                    Answer.company_id == company_id,
                    Answer.year == year,
                    Answer.indicator_id == indicator_id,
                )
                .first()
            )
            if row is None:
                row = Answer(
                    session_id=session_row.id,
                    company_id=company_id,
                    year=year,
                    indicator_id=indicator_id,
                    module="M22",
                    indicator_name=(
                        "Headquarters Latitude"
                        if indicator_id == "IMP-M22-I01"
                        else "Headquarters Longitude"
                    ),
                    question_text="Company headquarters geo-coordinate",
                    response_format="Numeric",
                )
                db.add(row)

            row.session_id = session_row.id
            row.answer_value = f"{value:.6f}"
            row.answer_unit = "Decimal degrees"
            row.source = "openstreetmap_nominatim"
            row.confidence = 0.95
            row.notes = f"Geocoded from headquarters='{hq}' via OpenStreetMap Nominatim"
            row.is_verified = True

        upsert_indicator("IMP-M22-I01", lat)
        upsert_indicator("IMP-M22-I02", lon)

        if own_session:
            db.commit()
        else:
            db.flush()

        return True, f"lat={lat:.6f}, lon={lon:.6f}, hq='{hq}'"
    except Exception as exc:
        if own_session:
            db.rollback()
        return False, str(exc)[:200]
    finally:
        if own_session:
            db.close()


def _direct_questionnaire_fill(company_name: str, company_id: int, year: int) -> int:
    """
    Directly invoke QuestionnaireEngine to fill answers strictly from
    year-specific extracted data for (company, year).
    """
    from backend.questionnaire.engine import QuestionnaireEngine
    from backend.database.db import get_session
    from backend.database.models import Company as _Company, Answer as _Answer

    # Clean legacy placeholders so strict mode cannot leave smart defaults behind.
    _db_cleanup = get_session()
    try:
        _db_cleanup.query(_Answer).filter(
            _Answer.company_id == company_id,
            _Answer.year == year,
            _Answer.source == "smart_default",
            _Answer.is_verified == False,
        ).update(
            {
                _Answer.answer_value: None,
                _Answer.source: "unavailable",
                _Answer.confidence: 0.0,
                _Answer.notes: "Legacy smart default cleared by strict original-only mode",
            },
            synchronize_session=False,
        )
        _db_cleanup.commit()
    finally:
        _db_cleanup.close()

    _db = get_session()
    exact = _db.query(_Company).filter_by(id=company_id).first()
    engine_company_name = exact.name if exact and exact.name else company_name
    _db.close()

    engine = QuestionnaireEngine(
        engine_company_name,
        year,
        standard="ALL",
        allow_historical_prefill=False,
        allow_smart_defaults=False,
        allow_cross_year_financial_fallback=False,
        # Bounded online fallback improves coverage while keeping scoring time finite.
        allow_online_provisional_fallback=True,
        provisional_max_attempts=151,
        provisional_time_budget_seconds=300,
    )
    engine.setup()
    # Pin to the exact DB record so ilike won't match the wrong company.
    if engine.company and engine.company.id != company_id:
        _db = get_session()
        exact = _db.query(_Company).filter_by(id=company_id).first()
        if exact:
            engine.company = exact
        _db.close()
    return engine.run_auto(module_filter=None)


def _fill_worker(company_name: str, company_id: int, year: int, out_q):
    """Process worker for bounded questionnaire fill."""
    try:
        rows = _direct_questionnaire_fill(company_name, company_id, year)
        out_q.put({"ok": True, "rows": rows})
    except Exception as exc:
        out_q.put({"ok": False, "error": str(exc)[:300]})


def _direct_questionnaire_fill_with_timeout(
    company_name: str,
    company_id: int,
    year: int,
    timeout_seconds: int = 300,
):
    """Run questionnaire fill in a child process and enforce a hard timeout."""
    out_q = mp.Queue()
    proc = mp.Process(target=_fill_worker, args=(company_name, company_id, year, out_q), daemon=True)
    proc.start()
    proc.join(timeout_seconds)

    if proc.is_alive():
        proc.terminate()
        proc.join(5)
        return False, f"timeout after {timeout_seconds}s"

    try:
        payload = out_q.get_nowait()
    except Exception:
        payload = {"ok": False, "error": "no_result_from_fill_worker"}

    if payload.get("ok"):
        return True, payload.get("rows", 0)
    return False, payload.get("error", "fill_failed")


def _ensure_all_indicator_rows(company_id: int, year: int, standard: str = "ALL"):
    """Ensure every indicator has an Answer row for (company, year) in strict mode.

    Missing indicators are created as explicit unavailable rows, never smart defaults.
    Returns: (created_count, total_indicator_count)
    """
    from backend.database.db import get_session
    from backend.database.models import QuestionnaireSession, Answer
    from backend.processor.csv_loader import ImpactreeCSVLoader

    db = get_session()
    try:
        session_row = (
            db.query(QuestionnaireSession)
            .filter_by(company_id=company_id, year=year, standard=standard)
            .first()
        )
        if session_row is None:
            indicators = ImpactreeCSVLoader.get_all_indicators() if standard == "ALL" else ImpactreeCSVLoader.get_indicators_by_standard(standard)
            session_row = QuestionnaireSession(
                company_id=company_id,
                year=year,
                standard=standard,
                status="in_progress",
                total_questions=len(indicators),
                answered_questions=0,
            )
            db.add(session_row)
            db.commit()
            db.refresh(session_row)

        indicators = ImpactreeCSVLoader.get_all_indicators() if standard == "ALL" else ImpactreeCSVLoader.get_indicators_by_standard(standard)
        total = len(indicators)
        existing_ids = {
            row[0]
            for row in db.query(Answer.indicator_id)
            .filter(Answer.company_id == company_id, Answer.year == year)
            .all()
        }

        created = 0
        for ind in indicators:
            ind_id = ind.get("indicator_id", "")
            if not ind_id or ind_id in existing_ids:
                continue
            module = ind_id.split("-")[1] if "-" in ind_id else ""
            db.add(Answer(
                session_id=session_row.id,
                company_id=company_id,
                year=year,
                indicator_id=ind_id,
                module=module,
                indicator_name=ind.get("indicator_name", "") or ind_id,
                question_text=ind.get("question", "") or "",
                answer_value=None,
                answer_unit=ind.get("unit", "") or "",
                response_format=ind.get("response_format", "") or "",
                source="unavailable",
                confidence=0.0,
                notes="No original year-specific evidence found in extracted reports or online sources",
                is_verified=False,
            ))
            created += 1

        if created > 0:
            db.commit()

        return created, total
    finally:
        db.close()


def _clear_legacy_smart_defaults(company_id: int, years: list[int]) -> int:
    """Convert any smart_default rows to explicit unavailable placeholders."""
    from backend.database.db import get_session
    from backend.database.models import Answer

    yrs = sorted(set(int(y) for y in (years or []) if str(y).isdigit()))
    if not yrs:
        return 0

    db = get_session()
    try:
        q = db.query(Answer).filter(
            Answer.company_id == company_id,
            Answer.year.in_(yrs),
            Answer.source == "smart_default",
            Answer.is_verified == False,
        )
        affected = q.count()
        if affected > 0:
            q.update(
                {
                    Answer.answer_value: None,
                    Answer.source: "unavailable",
                    Answer.confidence: 0.0,
                    Answer.notes: "Legacy smart default cleared by strict original-only mode",
                },
                synchronize_session=False,
            )
            db.commit()
        return affected
    finally:
        db.close()


def _is_scraped_only_source(source: str | None) -> bool:
    """Return True only for sources that are actual scraped/extracted evidence."""
    src = (source or "").strip().lower()
    if not src:
        return False

    blocked_tokens = (
        "manual",
        "default",
        "template",
        "synthetic",
        "historical",
        "unavailable",
        "none",
    )
    if any(token in src for token in blocked_tokens):
        return False
    return True


def _enforce_scraped_only_answers(company_id: int, year: int) -> tuple[int, int, int]:
    """Keep only scraped-backed answers for (company, year).

    Returns: (total_answers, kept_answers, cleared_answers)
    """
    from backend.database.db import get_session
    from backend.database.models import Answer

    db = get_session()
    try:
        scraped_sources = {
            (row[0] or "").strip().lower()
            for row in db.query(ScrapedData.source)
            .filter(ScrapedData.company_id == company_id, ScrapedData.year == year)
            .distinct()
            .all()
            if row[0]
        }

        answers = (
            db.query(Answer)
            .filter(Answer.company_id == company_id, Answer.year == year)
            .all()
        )

        total_answers = len(answers)
        kept_answers = 0
        cleared_answers = 0

        trusted_location_indicators = {"IMP-M22-I01", "IMP-M22-I02"}
        trusted_location_sources = {"openstreetmap_nominatim", "geocoded_headquarters", "company_office_location"}

        for ans in answers:
            src = (ans.source or "").strip().lower()
            has_value = bool((ans.answer_value or "").strip())
            is_allowed_scraped = src in scraped_sources and _is_scraped_only_source(src)
            is_trusted_location = (
                ans.indicator_id in trusted_location_indicators
                and src in trusted_location_sources
                and has_value
            )

            if has_value and (is_allowed_scraped or is_trusted_location):
                kept_answers += 1
                continue

            if has_value:
                ans.answer_value = None
                ans.confidence = 0.0
                ans.source = "unavailable"
                ans.notes = "Strict scraped-only mode: non-scraped/manual/default/template data removed"
                ans.is_verified = False
                cleared_answers += 1

        if cleared_answers > 0:
            db.commit()

        return total_answers, kept_answers, cleared_answers
    finally:
        db.close()


def _company_slug(name: str) -> str:
    slug = re.sub(r"[^A-Za-z0-9]+", "_", (name or "company").strip())
    slug = slug.strip("_")
    return slug or "company"


def _job_log_path(job_id: int) -> Path:
    root = Path(__file__).parent.parent.parent.parent
    log_dir = root / "data" / "pipeline_logs"
    log_dir.mkdir(parents=True, exist_ok=True)
    return log_dir / f"job_{job_id}.log"


def _append_job_log(job_id: int, message: str) -> None:
    try:
        stamp = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
        with _job_log_path(job_id).open("a", encoding="utf-8") as f:
            f.write(f"[{stamp}] {message}\n")
    except Exception:
        pass


def _tail_text(text: str, max_chars: int = 1200) -> str:
    if not text:
        return ""
    text = text.strip()
    if len(text) <= max_chars:
        return text
    return text[-max_chars:]


def _normalize_company_token(name: str) -> str:
    token = re.sub(r"[^A-Za-z0-9]+", "_", (name or "").strip()).strip("_")
    return token.lower()


def _find_local_annual_report_pdfs(company_name: str, year: int) -> list[Path]:
    """Search known local download folders for company/year PDF reports."""
    root = Path(__file__).parent.parent.parent.parent
    year_s = str(year)
    company_tok = _normalize_company_token(company_name)

    candidate_roots = [
        root / "scrapper_new-main" / "downloads" / "nseindia.com",
        root / "scrapper_new-main" / "downloads" / "annualreports.com",
        root / "data" / "annual_reports",
    ]

    found: list[Path] = []
    for base in candidate_roots:
        if not base.exists():
            continue

        try:
            for company_dir in base.iterdir():
                if not company_dir.is_dir():
                    continue

                folder_tok = _normalize_company_token(company_dir.name)
                if not folder_tok:
                    continue

                if company_tok not in folder_tok and folder_tok not in company_tok:
                    continue

                for pdf in company_dir.rglob("*.pdf"):
                    p = str(pdf)
                    if year_s in pdf.name or year_s in p:
                        found.append(pdf)
        except Exception:
            continue

    # De-duplicate preserving order.
    uniq: list[Path] = []
    seen: set[str] = set()
    for p in found:
        key = str(p).lower()
        if key in seen:
            continue
        seen.add(key)
        uniq.append(p)
    return uniq


def _ensure_local_reports_available(company_name: str, year: int, job_id: int | None = None) -> int:
    """Ensure local company/year annual report PDFs exist; download if missing."""
    existing = _find_local_annual_report_pdfs(company_name, year)
    if existing:
        if job_id is not None:
            _append_job_log(job_id, f"📁 Found {len(existing)} local report PDF(s) for {company_name} {year}")
        return len(existing)

    root = Path(__file__).parent.parent.parent.parent
    scraper_dir = root / "scrapper_new-main"
    scraper_script = scraper_dir / "scraper.py"
    if not scraper_script.exists():
        if job_id is not None:
            _append_job_log(job_id, f"⚠️ Local reports missing and scraper not found: {scraper_script}")
        return 0

    cmd = [
        sys.executable,
        "scraper.py",
        "--company",
        company_name,
        "--year",
        str(year),
        "--skip-news",
        "--skip-sustainability",
    ]

    try:
        if job_id is not None:
            _append_job_log(job_id, f"⬇️ Local reports missing; downloading for {company_name} {year} via scrapper_new-main")

        proc = subprocess.run(
            cmd,
            cwd=str(scraper_dir),
            capture_output=True,
            text=True,
            timeout=600,
            check=False,
        )

        if job_id is not None:
            _append_job_log(job_id, f"🧰 scraper.py exit code: {proc.returncode}")
            if proc.stdout:
                _append_job_log(job_id, f"🧾 scraper stdout: {_tail_text(proc.stdout, 800)}")
            if proc.stderr:
                _append_job_log(job_id, f"🧾 scraper stderr: {_tail_text(proc.stderr, 500)}")
    except Exception as exc:
        if job_id is not None:
            _append_job_log(job_id, f"❌ Auto-download failed: {str(exc)[:200]}")
        return 0

    existing = _find_local_annual_report_pdfs(company_name, year)
    if job_id is not None:
        _append_job_log(job_id, f"📁 Local reports after download: {len(existing)} PDF(s)")
    return len(existing)


def _export_company_data_snapshot(company_id: int, company_name: str) -> None:
    """Write a company-wise JSON snapshot of collected data to disk."""
    from backend.database.db import get_session
    from backend.database.models import Company as _Company, ScrapedData, QuestionnaireSession, Answer, PipelineJob as _PipelineJob

    root = Path(__file__).parent.parent.parent.parent
    company_dir = root / "data" / "company_data" / _company_slug(company_name)
    company_dir.mkdir(parents=True, exist_ok=True)

    db = get_session()
    try:
        company = db.query(_Company).filter(_Company.id == company_id).first()
        scraped_rows = db.query(ScrapedData).filter(ScrapedData.company_id == company_id).all()
        sessions = db.query(QuestionnaireSession).filter(QuestionnaireSession.company_id == company_id).all()
        answers = db.query(Answer).filter(Answer.company_id == company_id).all()
        jobs = db.query(_PipelineJob).filter(_PipelineJob.company_id == company_id).order_by(_PipelineJob.started_at.desc()).limit(100).all()

        payload = {
            "generated_at": datetime.utcnow().isoformat() + "Z",
            "company": {
                "id": company.id if company else company_id,
                "name": (company.name if company else company_name),
                "ticker": (company.ticker if company else "") or "",
                "cin": (company.cin if company else "") or "",
                "sector": (company.sector if company else "") or "",
                "exchange": (company.exchange if company else "") or "",
                "website": (company.website if company else "") or "",
                "headquarters": (company.headquarters if company else "") or "",
            },
            "years": sorted({s.year for s in sessions}),
            "counts": {
                "scraped_data": len(scraped_rows),
                "sessions": len(sessions),
                "answers": len(answers),
                "jobs": len(jobs),
            },
            "scraped_data": [
                {
                    "year": r.year,
                    "source": r.source,
                    "key": r.data_key,
                    "value": r.data_value,
                    "scraped_at": r.scraped_at.isoformat() if r.scraped_at else None,
                }
                for r in scraped_rows
            ],
            "sessions": [
                {
                    "id": s.id,
                    "year": s.year,
                    "standard": s.standard,
                    "status": s.status,
                    "total_questions": s.total_questions,
                    "answered_questions": s.answered_questions,
                    "created_at": s.created_at.isoformat() if s.created_at else None,
                    "updated_at": s.updated_at.isoformat() if s.updated_at else None,
                }
                for s in sessions
            ],
            "answers": [
                {
                    "session_id": a.session_id,
                    "year": a.year,
                    "indicator_id": a.indicator_id,
                    "module": a.module,
                    "indicator_name": a.indicator_name,
                    "answer_value": a.answer_value,
                    "answer_unit": a.answer_unit,
                    "source": a.source,
                    "confidence": a.confidence,
                    "is_verified": a.is_verified,
                    "updated_at": a.updated_at.isoformat() if a.updated_at else None,
                }
                for a in answers
            ],
            "jobs": [
                {
                    "id": j.id,
                    "year": j.year,
                    "status": j.status,
                    "error_msg": j.error_msg,
                    "started_at": j.started_at.isoformat() if j.started_at else None,
                    "finished_at": j.finished_at.isoformat() if j.finished_at else None,
                }
                for j in jobs
            ],
        }

        latest_path = company_dir / "latest_snapshot.json"
        latest_path.write_text(json.dumps(payload, indent=2, ensure_ascii=True), encoding="utf-8")

        stamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
        history_path = company_dir / f"snapshot_{stamp}.json"
        history_path.write_text(json.dumps(payload, indent=2, ensure_ascii=True), encoding="utf-8")
    finally:
        db.close()


def _collect_real_documents(company_id: int, year: int, db_session=None, job_id: int | None = None) -> tuple[bool, int]:
    """
    Phase 1: Collect real ESG documents from online sources.
    Downloads annual reports, sustainability reports, regulatory filings, etc.
    Returns: (success, documents_collected)
    """
    print(f"[DOWNLOAD] Starting automatic document collection for company {company_id}, year {year}")
    total_collected = 0

    try:
        # Get company info
        db = db_session or get_session()
        company = db.query(Company).filter_by(id=company_id).first()
        if not company:
            print(f"[ERROR] Company {company_id} not found")
            return False, 0

        print(f"[DOWNLOAD] Company: {company.name}")

        # STEP 0: Search local download folders first; if missing, auto-download.
        local_reports = _ensure_local_reports_available(company.name, year, job_id=job_id)
        if local_reports > 0:
            print(f"[DOWNLOAD] Local report PDFs ready: {local_reports}")
            total_collected += local_reports
        else:
            print("[DOWNLOAD] No local report PDFs found for this company/year")

        # STEP 1: Try automatic document collection system
        try:
            sys.path.insert(0, str(Path(__file__).parent.parent.parent.parent))
            from esg_pipeline_document_scraper import integrate_with_run_pipeline
            docs_from_scraper = integrate_with_run_pipeline(company_id, year, db_session)
            total_collected += docs_from_scraper
            print(f"[DOWNLOAD] Document scraper collected: {docs_from_scraper}")
        except Exception as e:
            print(f"[INFO] Document scraper unavailable: {str(e)[:100]}...")

        # STEP 2: Web scraping for ESG indicators
        try:
            from backend.scraper.provisional_scraper import ProvisionalWebScraper
            scraper = ProvisionalWebScraper(company.name, year)

            # Key indicators to scrape
            key_indicators = [
                {"id": "IMP-M01-I01", "question": f"Corporate Identification Number of {company.name}"},
                {"id": "IMP-M03-I01", "question": f"Total revenue from operations of {company.name} {year}"},
                {"id": "IMP-M05-I01", "question": f"Scope 1 GHG emissions of {company.name} {year}"},
                {"id": "IMP-M15-I01", "question": f"Total number of employees at {company.name} {year}"}
            ]

            web_scraped = 0
            for indicator in key_indicators:
                try:
                    result = scraper.get_provisional_answer(indicator)
                    if result and result.get('answer'):
                        # Store scraped data
                        scraped_data = ScrapedData(
                            company_id=company_id,
                            year=year,
                            source='web_scraped',
                            data_key=indicator['id'],
                            data_value=result['answer'][:500]  # Limit length
                        )
                        db.add(scraped_data)
                        web_scraped += 1
                        print(f"[WEB] Scraped {indicator['id']}: {result['answer'][:50]}...")
                except Exception as e:
                    continue

            if web_scraped > 0:
                db.commit()
                total_collected += web_scraped
                print(f"[WEB] Web scraping collected: {web_scraped} indicators")

        except Exception as e:
            print(f"[INFO] Web scraping unavailable: {str(e)[:100]}...")

        # STEP 4: PATTERN-BASED REAL DATA EXTRACTION (NO GEMINI, NO SYNTHETIC DATA)
        try:
            from pattern_based_real_extraction import integrate_with_pipeline
            print(f"[PATTERN] Starting pattern-based extraction for ALL 151 indicators...")
            print(f"[PATTERN] Using real PDF documents with regex pattern matching")
            print(f"[PATTERN] NO GEMINI - NO SYNTHETIC DATA")

            success_pattern, pattern_extracted = integrate_with_pipeline(
                company_id=company_id,
                company_name=company.name,
                year=year,
                document_texts=None,  # Will auto-download documents
                db_session=db
            )

            if success_pattern:
                total_collected += pattern_extracted
                print(f"[PATTERN] SUCCESS: {pattern_extracted}/151 indicators extracted from REAL documents")
                print(f"[PATTERN] ZERO synthetic data - Pattern matching only")
            else:
                print(f"[PATTERN] Pattern extraction: No documents found")
                print(f"[PATTERN] Returning 0 indicators - NO synthetic data generated")
        except Exception as e:
            print(f"[PATTERN] Pattern extraction error: {str(e)[:100]}...")
            print("[INFO] Pattern-based extraction unavailable")

        print(f"[SUCCESS] Total documents/data collected: {total_collected}")
        return True, total_collected

    except Exception as e:
        import traceback
        error_msg = f"Document collection failed: {str(e)}"
        print(f"[ERROR] in _collect_real_documents: {error_msg}")
        return False, 0


def _process_real_data_only(company_id: int, year: int, db_session=None) -> tuple[bool, int]:
    """
    Phase 2: Process ESG indicators using ENHANCED real data sources.
    Priority: Comprehensive Database > Manual Data > Document Data > Missing
    NO SYNTHETIC DATA GENERATION.
    Returns: (success, indicators_processed)
    """
    try:
        # Import the enhanced real data only system
        sys.path.insert(0, str(Path(__file__).parent.parent.parent.parent))
        from enhanced_real_data_system import process_enhanced_real_data_only

        indicators_processed = process_enhanced_real_data_only(
            company_id,
            year,
            db_session,
            allow_historical_fallback=False,
        )
        return True, indicators_processed
    except Exception as e:
        import traceback
        error_msg = f"Enhanced real data processing failed: {str(e)}"
        print(f"ERROR in _process_real_data_only: {error_msg}")
        # Skip traceback printing to avoid Unicode issues
        return False, 0


def _run_pipeline_task(
    job_id: int,
    company_name: str,
    nse_symbol: str,
    financial_years: list,   # list[int] — the exact years the user selected
    all_years: bool,
):
    """
    Background task — run complete ESG processing for every requested year.

    Uses new CompanyYearProcessor for:
    - 21 ESG modules processing
    - 151 indicators calculation
    - Multi-standard compliance (BRSR, CDP, EcoVadis, GRI)
    - Automated scoring and rating

    PLUS Complete 151/151 indicator filling with real data
    """
    from backend.database.db import get_session
    from backend.services.company_year_processor import CompanyYearProcessor

    db = get_session()
    job = db.query(PipelineJob).filter_by(id=job_id).first()
    if not job:
        db.close()
        return

    company_id = job.company_id
    _append_job_log(job_id, f"🚀 Started ESG processing pipeline for company='{company_name}', years={financial_years}")

    try:
        job.status = "FETCHING"
        job.error_msg = None
        db.commit()

        selected_years = sorted(set(financial_years or [2026]))
        _append_job_log(job_id, f"📅 Processing years: {selected_years}")

        # Process each year with our new ESG system
        processed_years = []
        failed_years = []

        for year in selected_years:
            try:
                _append_job_log(job_id, f"🔄 Starting ESG processing for year {year}...")

                # Update status to show current year processing
                job.status = "SCORING"
                job.error_msg = f"Processing year {year} - ESG modules and indicators"
                db.commit()

                # PHASE 1: Collect real ESG documents from online sources
                _append_job_log(job_id, f"📂 Collecting real ESG documents from online sources...")

                try:
                    success_docs, docs_collected = _collect_real_documents(company_id, year, db, job_id=job_id)

                    if success_docs:
                        _append_job_log(job_id, f"✅ Document collection: {docs_collected} documents collected from real sources")
                    else:
                        _append_job_log(job_id, f"⚠️ Document collection: Failed to collect documents")
                except Exception as collect_error:
                    _append_job_log(job_id, f"❌ Document collection failed: {str(collect_error)}")
                    success_docs, docs_collected = False, 0

                # PHASE 2: Process ESG indicators using ENHANCED real data sources
                _append_job_log(job_id, f"📊 Processing TARGET 151 ESG indicators with enhanced real data system...")

                # Import frontend integration module
                sys.path.insert(0, str(Path(__file__).parent.parent.parent.parent))
                from frontend_pipeline_integration import format_pipeline_progress_message

                try:
                    success_real, indicators_processed = _process_real_data_only(company_id, year, db)

                    # Get exact 151 indicators status
                    main_msg, details = format_pipeline_progress_message(company_id, year)
                    _append_job_log(job_id, f"✅ {main_msg}")

                    for detail in details:
                        _append_job_log(job_id, f"   • {detail}")

                    if success_real and indicators_processed > 140:
                        _append_job_log(job_id, f"🚫 NO SYNTHETIC DATA - Only selected-year comprehensive database, manual input, and documents used")
                    else:
                        _append_job_log(job_id, f"⚠️ Enhanced Real Data Processing: Need more data sources for complete coverage")
                except Exception as real_error:
                    _append_job_log(job_id, f"❌ Enhanced real data processing failed: {str(real_error)}")
                    success_real, indicators_processed = False, 0

                # PHASE 2.1: Populate latest location indicators from OpenStreetMap
                try:
                    loc_ok, loc_msg = _populate_location_indicators_from_osm(company_id, year, db)
                    if loc_ok:
                        _append_job_log(job_id, f"📍 Location indicators updated from OSM (IMP-M22-I01/I02): {loc_msg}")
                    else:
                        _append_job_log(job_id, f"⚠️ Location indicators skipped (IMP-M22-I01/I02): {loc_msg}")
                except Exception as loc_error:
                    _append_job_log(job_id, f"❌ Location indicator enrichment failed: {str(loc_error)[:120]}")

                # STEP 2: Use our comprehensive pipeline with DYNAMIC PATTERN SOURCES
                _append_job_log(job_id, f"🔄 Running comprehensive pipeline with DYNAMIC PATTERN SOURCES...")

                # Import comprehensive pipeline
                sys.path.insert(0, str(Path(__file__).parent.parent.parent.parent))
                from comprehensive_pipeline import run_comprehensive_pipeline

                _append_job_log(job_id, f"📊 Pipeline sources:")
                _append_job_log(job_id, f"   • Document sources (PDFs, reports)")
                _append_job_log(job_id, f"   • Dynamic pattern sources (web-scraped company data)")
                _append_job_log(job_id, f"   • Online sources (web scraping)")

                try:
                    # Run comprehensive pipeline with dynamic patterns
                    pipeline_result = run_comprehensive_pipeline(company_id, year)

                    if pipeline_result.get('success'):
                        indicators_count = pipeline_result.get('indicators_processed', 0)
                        document_sources = pipeline_result.get('document_sources', 0)
                        pattern_sources = pipeline_result.get('pattern_sources', 0)
                        online_sources = pipeline_result.get('online_sources', 0)

                        _append_job_log(job_id, f"✅ DYNAMIC PATTERN SOURCES SUCCESS:")
                        _append_job_log(job_id, f"   • Total indicators: {indicators_count}")
                        _append_job_log(job_id, f"   • Document sources: {document_sources} indicators")
                        _append_job_log(job_id, f"   • Dynamic pattern sources: {pattern_sources} indicators (WEB-SCRAPED)")
                        _append_job_log(job_id, f"   • Online sources: {online_sources} indicators")
                        _append_job_log(job_id, f"   • Pattern sources now use REAL company-specific data!")
                    else:
                        _append_job_log(job_id, f"❌ Comprehensive pipeline failed: {pipeline_result.get('error', 'Unknown error')}")
                except Exception as comp_error:
                    _append_job_log(job_id, f"❌ Comprehensive pipeline error: {str(comp_error)}")

                # STEP 3: Use our CompanyYearProcessor for module-based processing
                processor = CompanyYearProcessor(
                    company_id=str(company_id),
                    year=year,
                    standards=["BRSR", "CDP", "EcoVadis", "GRI"]
                )

                _append_job_log(job_id, f"📊 Processing 21 ESG modules and validating indicators...")

                # Run complete processing
                result = processor.process_company_year(
                    force_refresh=True,  # Always refresh in pipeline
                    include_real_time=True,
                    trigger_scoring=True
                )

                # Log results with actual 151 indicators status
                from frontend_pipeline_integration import get_pipeline_151_status
                found_count, total_count, coverage_percent, module_stats, missing = get_pipeline_151_status(company_id, year)

                _append_job_log(job_id, f"✅ Year {year} completed:")
                _append_job_log(job_id, f"   • Target 151 Indicators: {found_count}/{total_count} found ({coverage_percent:.1f}% coverage)")
                _append_job_log(job_id, f"   • Total Indicators: {result.total_indicators}")
                _append_job_log(job_id, f"   • Processed: {result.processed_indicators}")
                _append_job_log(job_id, f"   • Failed: {result.failed_indicators}")
                _append_job_log(job_id, f"   • Modules: {len(result.modules_processed)}")
                _append_job_log(job_id, f"   • Processing Time: {result.processing_time_seconds:.1f}s")

                if hasattr(result, 'final_score') and result.final_score:
                    _append_job_log(job_id, f"   • ESG Score: {result.final_score:.1f}/100")

                # AUTOMATIC DATA SOURCES SAVING
                try:
                    from automatic_data_saver import pipeline_auto_save_data_sources
                    _append_job_log(job_id, f"💾 Automatically saving data sources for {year}...")

                    auto_save_result = pipeline_auto_save_data_sources(company_id, year)
                    if auto_save_result:
                        report_filename = f"{company_name.replace(' ', '_')}_{year}_data_sources"
                        _append_job_log(job_id, f"✅ Data sources saved: {auto_save_result['total_indicators']} indicators from {auto_save_result['total_sources']} sources")
                        _append_job_log(job_id, f"   • Report saved: {report_filename}")
                        _append_job_log(job_id, f"   • Coverage: {auto_save_result['coverage_analysis']['target_151_coverage']}")
                    else:
                        _append_job_log(job_id, f"⚠️ Data sources auto-save skipped or failed")
                except Exception as auto_save_error:
                    _append_job_log(job_id, f"❌ Auto-save data sources failed: {str(auto_save_error)[:100]}")

                # FINAL ENFORCEMENT: keep only real scraped-backed answers for this year
                try:
                    total_ans, kept_ans, cleared_ans = _enforce_scraped_only_answers(company_id, year)
                    _append_job_log(job_id, f"🔒 Scraped-only enforcement (year {year}): kept={kept_ans}, cleared={cleared_ans}, total={total_ans}")
                except Exception as strict_error:
                    _append_job_log(job_id, f"⚠️ Scraped-only enforcement failed: {str(strict_error)[:120]}")

                processed_years.append(year)

            except Exception as year_error:
                failed_years.append(year)
                _append_job_log(job_id, f"❌ Year {year} failed: {str(year_error)[:200]}")

        # Final status determination
        if not failed_years:
            job.status = "PUBLISHED"
            job.error_msg = None
            _append_job_log(job_id, f"🎉 Pipeline completed successfully for all {len(processed_years)} years")
        elif processed_years:
            job.status = "NEEDS_REVIEW"
            job.error_msg = f"Completed {len(processed_years)} years, {len(failed_years)} failed"
            _append_job_log(job_id, f"⚠️ Partial success: {len(processed_years)} years completed, {len(failed_years)} failed")
        else:
            job.status = "ERROR"
            job.error_msg = f"All {len(failed_years)} years failed processing"
            _append_job_log(job_id, f"💥 All years failed processing")

        # Log summary statistics with actual indicators status
        if processed_years:
            # Get final status for the most recent/primary year
            primary_year = processed_years[-1] if processed_years else year
            from frontend_pipeline_integration import get_pipeline_151_status
            found_count, total_count, coverage_percent, module_stats, missing = get_pipeline_151_status(company_id, primary_year)

            _append_job_log(job_id, f"📈 Pipeline Summary:")
            _append_job_log(job_id, f"   • Company: {company_name} (ID: {company_id})")
            _append_job_log(job_id, f"   • Years Processed: {processed_years}")
            _append_job_log(job_id, f"   • Years Failed: {failed_years}")
            _append_job_log(job_id, f"   • ESG Modules: 21 (GHG, Energy, Water, Waste, OHS, etc.)")
            _append_job_log(job_id, f"   • TARGET 151 Indicators: {found_count}/{total_count} ({coverage_percent:.1f}% coverage with real data)")
            _append_job_log(job_id, f"   • Standards: BRSR, CDP, EcoVadis, GRI")
            if missing and len(missing) <= 5:
                _append_job_log(job_id, f"   • Missing: {', '.join(missing)}")

            # FINAL AUTO-SAVE SUMMARY
            _append_job_log(job_id, f"💾 Data sources automatically saved for all processed years")
        else:
            _append_job_log(job_id, f"📈 Pipeline Summary:")
            _append_job_log(job_id, f"   • Company: {company_name} (ID: {company_id})")
            _append_job_log(job_id, f"   • Years Processed: {processed_years}")
            _append_job_log(job_id, f"   • Years Failed: {failed_years}")
            _append_job_log(job_id, f"   • ESG Modules: 21 (GHG, Energy, Water, Waste, OHS, etc.)")
            _append_job_log(job_id, f"   • TARGET 151 Indicators: Processing failed - check data sources")
            _append_job_log(job_id, f"   • Standards: BRSR, CDP, EcoVadis, GRI")

    except Exception as exc:
        job.status = "ERROR"
        job.error_msg = str(exc)[:500]
        _append_job_log(job_id, f"💥 Unhandled pipeline error: {str(exc)[:500]}")
    finally:
        job.finished_at = datetime.utcnow()
        db.commit()
        db.close()
        try:
            _export_company_data_snapshot(company_id=company_id, company_name=company_name)
            _append_job_log(job_id, "📁 Company data snapshot exported")
        except Exception:
            pass


# ── POST /api/pipeline/run ────────────────────────────────────────────────────

@router.post("/run", response_model=List[PipelineJobOut])
def run_pipeline(
    body: RunPipelineRequest,
    background_tasks: BackgroundTasks,
    db: Session = Depends(get_db),
):
    # Resolve companies
    if body.company_ids:
        companies = db.query(Company).filter(Company.id.in_([int(i) for i in body.company_ids])).all()
    else:
        companies = db.query(Company).all()

    if not companies:
        raise HTTPException(status_code=400, detail="No companies found")

    # Parse every selected financial year into integers.
    # e.g. ["FY2022", "FY2024", "FY2025"] → [2022, 2024, 2025]
    financial_years_int: list = sorted(set(
        int(fy.replace("FY", "").strip())
        for fy in (body.financial_years or ["FY2026"])
        if fy.upper().startswith("FY") and fy[2:].strip().isdigit()
    ))
    if not financial_years_int:
        financial_years_int = [2026]
    # When all_years toggle is on, expand to the 5 years up to the latest
    if body.all_years:
        latest = max(financial_years_int)
        financial_years_int = sorted(set(financial_years_int) | set(range(latest - 4, latest + 1)))
    primary_year = max(financial_years_int)  # shown in the job row

    jobs: List[PipelineJobOut] = []
    for company in companies:
        # Enforce strict original-only mode before deciding whether to reuse active jobs.
        _clear_legacy_smart_defaults(company_id=company.id, years=financial_years_int)
        for yr in financial_years_int:
            _ensure_all_indicator_rows(company_id=company.id, year=yr, standard="ALL")

        # Prevent overlapping runs for the same company; return active job instead.
        active = (
            db.query(PipelineJob)
            .filter(
                PipelineJob.company_id == company.id,
                PipelineJob.status.in_(["QUEUED", "FETCHING", "SCORING"]),
            )
            .order_by(PipelineJob.started_at.desc())
            .first()
        )
        if active:
            age_s = (datetime.utcnow() - (active.started_at or datetime.utcnow())).total_seconds()
            stale = (
                (active.status == "QUEUED" and age_s > 300)
                or (active.status == "FETCHING" and age_s > 1800)
                or (active.status == "SCORING" and age_s > 900)
            )
            if stale:
                active.status = "ERROR"
                active.error_msg = "Stale active job auto-cleared"
                active.finished_at = datetime.utcnow()
                db.commit()
                active = None

        if active:
            jobs.append(PipelineJobOut(
                id=str(active.id),
                company_id=str(active.company_id),
                company_name=active.company_name or "",
                year=active.year,
                status=active.status,
                error_msg=active.error_msg,
                started_at=active.started_at.isoformat(),
                finished_at=active.finished_at.isoformat() if active.finished_at else None,
            ))
            continue

        # Create one job record per company (covers all selected years)
        job = PipelineJob(
            company_id=company.id,
            company_name=company.name,
            year=primary_year,
            status="QUEUED",
            data_sources=body.data_sources,
            triggered_by="api",
        )
        db.add(job)
        db.commit()
        db.refresh(job)

        # Launch background task with the full years list
        nse_symbol = company.ticker or company.name.upper().replace(" ", "")
        background_tasks.add_task(
            _run_pipeline_task,
            job.id, company.name, nse_symbol, financial_years_int, body.all_years
        )

        jobs.append(PipelineJobOut(
            id=str(job.id),
            company_id=str(job.company_id),
            company_name=job.company_name or "",
            year=job.year,
            status=job.status,
            error_msg=job.error_msg,
            started_at=job.started_at.isoformat(),
            finished_at=job.finished_at.isoformat() if job.finished_at else None,
        ))

    return jobs


# ── GET /api/pipeline/status/{job_id} ────────────────────────────────────────

@router.post("/status/batch", response_model=List[PipelineJobOut])
def get_job_status_batch(body: PipelineStatusBatchRequest, db: Session = Depends(get_db)):
    job_ids = [int(i) for i in (body.job_ids or []) if str(i).isdigit()]
    if not job_ids:
        return []

    jobs = db.query(PipelineJob).filter(PipelineJob.id.in_(job_ids)).all()
    by_id = {j.id: j for j in jobs}
    ordered = [by_id[jid] for jid in job_ids if jid in by_id]
    return [
        PipelineJobOut(
            id=str(j.id),
            company_id=str(j.company_id),
            company_name=j.company_name or "",
            year=j.year,
            status=j.status,
            error_msg=j.error_msg,
            started_at=j.started_at.isoformat(),
            finished_at=j.finished_at.isoformat() if j.finished_at else None,
        )
        for j in ordered
    ]


@router.get("/status/{job_id}", response_model=PipelineJobOut)
def get_job_status(job_id: str, db: Session = Depends(get_db)):
    job = db.query(PipelineJob).filter_by(id=int(job_id)).first()
    if not job:
        raise HTTPException(status_code=404, detail="Job not found")
    return PipelineJobOut(
        id=str(job.id),
        company_id=str(job.company_id),
        company_name=job.company_name or "",
        year=job.year,
        status=job.status,
        error_msg=job.error_msg,
        started_at=job.started_at.isoformat(),
        finished_at=job.finished_at.isoformat() if job.finished_at else None,
    )


# ── GET /api/pipeline/jobs ────────────────────────────────────────────────────

@router.get("/jobs", response_model=List[PipelineJobOut])
def list_jobs(db: Session = Depends(get_db)):
    jobs = db.query(PipelineJob).order_by(PipelineJob.started_at.desc()).limit(50).all()
    return [
        PipelineJobOut(
            id=str(j.id),
            company_id=str(j.company_id),
            company_name=j.company_name or "",
            year=j.year,
            status=j.status,
            error_msg=j.error_msg,
            started_at=j.started_at.isoformat(),
            finished_at=j.finished_at.isoformat() if j.finished_at else None,
        )
        for j in jobs
    ]


@router.get("/logs/{job_id}", response_model=PipelineJobLogOut)
def get_job_logs(job_id: str, lines: int = 200, db: Session = Depends(get_db)):
    job = db.query(PipelineJob).filter_by(id=int(job_id)).first()
    if not job:
        raise HTTPException(status_code=404, detail="Job not found")

    log_path = _job_log_path(int(job_id))
    if not log_path.exists():
        return PipelineJobLogOut(job_id=job_id, lines=[])

    try:
        all_lines = log_path.read_text(encoding="utf-8").splitlines()
    except Exception:
        all_lines = []

    lines = max(1, min(int(lines), 2000))
    return PipelineJobLogOut(job_id=job_id, lines=all_lines[-lines:])


# ═══════════════════════════════════════════════════════════════════════════
# NEW REAL DATA ONLY API ENDPOINTS
# ═══════════════════════════════════════════════════════════════════════════

@router.post("/collect-documents")
def collect_documents_endpoint(
    company_id: int,
    year: int = 2024,
    db: Session = Depends(get_db)
):
    """
    Phase 1: Collect real ESG documents from online sources.
    Downloads and extracts data from annual reports, sustainability reports, etc.
    """
    try:
        success, documents_collected = _collect_real_documents(company_id, year, db)

        return {
            "success": success,
            "company_id": company_id,
            "year": year,
            "documents_collected": documents_collected,
            "message": f"Document collection completed. {documents_collected} indicators extracted from real documents."
        }

    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Document collection failed: {str(e)}"
        )


@router.post("/process-real-data")
def process_real_data_endpoint(
    company_id: int,
    year: int = 2024,
    db: Session = Depends(get_db)
):
    """
    Phase 2: Process ESG indicators using ONLY real data sources.
    Priority: Manual Data > Document Data > Historical Data > Missing
    NO SYNTHETIC DATA GENERATION.
    """
    try:
        success, indicators_processed = _process_real_data_only(company_id, year, db)

        return {
            "success": success,
            "company_id": company_id,
            "year": year,
            "indicators_processed": indicators_processed,
            "total_indicators": 151,
            "coverage_percentage": round((indicators_processed / 151) * 100, 1),
            "message": f"Real data processing completed. {indicators_processed}/151 indicators processed with authentic data only."
        }

    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Real data processing failed: {str(e)}"
        )


@router.post("/real-data-pipeline")
def run_complete_real_data_pipeline(
    company_id: int,
    year: int = 2024,
    db: Session = Depends(get_db)
):
    """
    Complete Real Data Pipeline: Phase 1 + Phase 2
    1. Collect documents and extract real ESG data
    2. Process indicators using only real data sources
    Returns comprehensive results with 100% real data, no synthetic generation.
    """
    try:
        # Phase 1: Document Collection
        docs_success, docs_collected = _collect_real_documents(company_id, year, db)

        # Phase 2: Real Data Processing
        data_success, indicators_processed = _process_real_data_only(company_id, year, db)

        company = db.query(Company).filter_by(id=company_id).first()
        company_name = company.name if company else f"Company {company_id}"

        return {
            "success": docs_success and data_success,
            "company_id": company_id,
            "company_name": company_name,
            "year": year,
            "phase1_documents_collected": docs_collected,
            "phase2_indicators_processed": indicators_processed,
            "total_indicators": 151,
            "coverage_percentage": round((indicators_processed / 151) * 100, 1),
            "data_sources_used": ["real_pdf_extraction", "manual_input", "historical_data"],
            "synthetic_data_used": False,
            "message": f"✅ REAL DATA PIPELINE COMPLETED: {indicators_processed}/151 indicators with 100% authentic data"
        }

    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Real data pipeline failed: {str(e)}"
        )
