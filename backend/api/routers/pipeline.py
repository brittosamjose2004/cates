"""
routers/pipeline.py — Pipeline run and job status.
"""
from __future__ import annotations

import sys, subprocess, threading, queue, time
import multiprocessing as mp
import json, re
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent.parent.parent))

from datetime import datetime
from typing import List
from fastapi import APIRouter, Depends, BackgroundTasks, HTTPException
from sqlalchemy.orm import Session

from backend.api.deps import get_db, get_current_user
from backend.api.schemas import RunPipelineRequest, PipelineJobOut, PipelineJobLogOut, PipelineStatusBatchRequest
from backend.database.models import Company, PipelineJob, User

router = APIRouter(prefix="/api/pipeline", tags=["pipeline"])

# Built-in NSE symbols — used to skip auto-discovery for known companies
_BUILTIN_NSE_SYMBOLS = [
    {"nse_symbol": "TCS"},
    {"nse_symbol": "HCLTECH"},
    {"nse_symbol": "INFY"},
    {"nse_symbol": "WIPRO"},
    {"nse_symbol": "RELIANCE"},
]


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


def _run_pipeline_task(
    job_id: int,
    company_name: str,
    nse_symbol: str,
    financial_years: list,   # list[int] — the exact years the user selected
    all_years: bool,
):
    """Background task — run scrape + questionnaire fill for every requested year.

    Flow:
      1. Call run_all.py with the EXACT selected years from the frontend.
      2. For EVERY requested year, ensure a QuestionnaireSession exists.
         If any are missing, fill via QuestionnaireEngine so UI can always
         visualize analytics for each selected year.
    """
    from backend.database.db import get_session
    from backend.database.models import QuestionnaireSession
    db = get_session()
    job = db.query(PipelineJob).filter_by(id=job_id).first()
    if not job:
        db.close()
        return

    company_id = job.company_id
    _append_job_log(job_id, f"Started pipeline for company='{company_name}', years={financial_years}, all_years={all_years}")

    try:
        job.status = "FETCHING"
        job.error_msg = None
        db.commit()

        root = Path(__file__).parent.parent.parent.parent
        selected_years = sorted(set(financial_years or [2026]))
        latest_year = max(selected_years)

        # Attempt PDF scrape via run_all.py (works only for built-in NSE companies).
        # Non-matching companies cause run_all.py to exit 0 with a log message.
        cmd = [
            sys.executable, "-u", str(root / "run_all.py"),
            "--batch",
            "--skip-questionnaire",
            "--companies", company_name,
            "--year", str(latest_year),
            "--years", *[str(y) for y in selected_years],
        ]
        # Resolve symbol: if company.ticker looks like a fallback (e.g. "HCLTECHNOLOGIESFRANCE")
        # try auto-discovering the real ticker from Yahoo Finance (works for ANY global exchange).
        resolved_symbol = nse_symbol
        is_builtin = any(comp["nse_symbol"] == nse_symbol for comp in _BUILTIN_NSE_SYMBOLS)
        # Skip auto-discovery only for clean symbol tokens.
        # Values like "BAJAJ-AUTO" or long free-text names should still be resolved.
        looks_real = bool(nse_symbol) and bool(re.fullmatch(r"[A-Z0-9]{1,15}(\.(NS|BO))?", nse_symbol.upper()))
        if not is_builtin and not looks_real:
            try:
                import requests as _req
                _yf_resp = _req.get(
                    "https://query2.finance.yahoo.com/v1/finance/search",
                    params={"q": company_name, "quotesCount": 10, "newsCount": 0},
                    headers={"User-Agent": "Mozilla/5.0"},
                    timeout=10,
                )
                if _yf_resp.ok:
                    for _q in _yf_resp.json().get("quotes", []):
                        _sym = _q.get("symbol", "")
                        if _q.get("quoteType") not in ("EQUITY", None):
                            continue
                        if ".NS" in _sym:
                            resolved_symbol = _sym.replace(".NS", "")
                            break
                        elif ".BO" in _sym and resolved_symbol == nse_symbol:
                            resolved_symbol = _sym.replace(".BO", "")
                        elif resolved_symbol == nse_symbol and _sym:
                            # Global ticker (e.g. AAPL, SHEL, BHP)
                            resolved_symbol = _sym
            except Exception:
                pass  # keep original nse_symbol

        # Pass the resolved ticker. For NSE/BSE companies this triggers a real
        # PDF download + scrape. For global companies Yahoo Finance data is used.
        # If no valid ticker found, run_all.py falls back to smart defaults.
        symbol_for_runall = (resolved_symbol or "").strip()
        if symbol_for_runall.upper().endswith(".NS") or symbol_for_runall.upper().endswith(".BO"):
            symbol_for_runall = symbol_for_runall.rsplit(".", 1)[0]
        cmd += ["--nse-symbol", symbol_for_runall or resolved_symbol]
        cmd += ["--company-id", str(company_id)]

        _append_job_log(job_id, f"Running command: {' '.join(cmd)}")

        timeout_seconds = max(900, 300 * len(selected_years))
        _append_job_log(job_id, f"run_all timeout budget: {timeout_seconds}s")

        # Stream subprocess output live so UI logs show real progress while FETCHING.
        proc = subprocess.Popen(
            cmd,
            cwd=str(root),
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            bufsize=1,
        )

        output_q: queue.Queue[str] = queue.Queue()
        output_lines: list[str] = []

        def _reader() -> None:
            if not proc.stdout:
                return
            for line in iter(proc.stdout.readline, ""):
                output_q.put(line.rstrip("\r\n"))

        reader = threading.Thread(target=_reader, daemon=True)
        reader.start()

        start_ts = time.time()
        next_heartbeat = 60
        while True:
            try:
                line = output_q.get(timeout=1)
                if line:
                    output_lines.append(line)
                    _append_job_log(job_id, f"run_all> {line}")
            except queue.Empty:
                pass

            elapsed = int(time.time() - start_ts)
            if elapsed >= next_heartbeat:
                _append_job_log(job_id, f"run_all still running... elapsed={elapsed}s")
                next_heartbeat += 60

            if proc.poll() is not None:
                # Drain any remaining buffered output.
                while True:
                    try:
                        line = output_q.get_nowait()
                        if line:
                            output_lines.append(line)
                            _append_job_log(job_id, f"run_all> {line}")
                    except queue.Empty:
                        break
                break

            if elapsed > timeout_seconds:
                proc.kill()
                raise subprocess.TimeoutExpired(cmd=cmd, timeout=timeout_seconds)

        stdout_tail = _tail_text("\n".join(output_lines))
        stderr_tail = ""
        if stdout_tail:
            _append_job_log(job_id, f"run_all stdout (tail):\n{stdout_tail}")

        if proc.returncode != 0:
            job.status = "ERROR"
            err_msg = stderr_tail or stdout_tail or f"run_all.py failed with return code {proc.returncode}"
            job.error_msg = err_msg[:500]
            _append_job_log(job_id, f"Pipeline failed with return code {proc.returncode}")
            return

        job.status = "SCORING"
        db.commit()
        _append_job_log(job_id, "Fetch step completed; moving to scoring/fill step")

        # ── Recompute answers for every selected year for THIS company ────────
        # Always re-run questionnaire fill so a rerun truly refreshes year-wise
        # indicator values from newly scraped data (instead of leaving stale rows
        # when a session already exists from an older run).
        db.expire_all()
        fill_failures = 0
        for yr in selected_years:
            ok, detail = _direct_questionnaire_fill_with_timeout(
                company_name=company_name,
                company_id=company_id,
                year=yr,
                timeout_seconds=480,
            )
            created_missing, total_indicators = _ensure_all_indicator_rows(company_id=company_id, year=yr, standard="ALL")
            if ok:
                _append_job_log(
                    job_id,
                    f"Direct questionnaire fill completed for year={yr}; rows={detail}; coverage={total_indicators}; unavailable_created={created_missing}",
                )
            else:
                fill_failures += 1
                _append_job_log(
                    job_id,
                    f"Direct questionnaire fill failed for year={yr}: {detail}; coverage={total_indicators}; unavailable_created={created_missing}",
                )

        if fill_failures > 0:
            job.status = "NEEDS_REVIEW"
            job.error_msg = f"Year-wise fill failed for {fill_failures} year(s)."
            _append_job_log(job_id, f"Completed with review needed: fill failures={fill_failures}")
        else:
            job.status = "PUBLISHED"
            job.error_msg = None
            _append_job_log(job_id, "Pipeline completed successfully")

    except subprocess.TimeoutExpired:
        job.status = "ERROR"
        job.error_msg = "Pipeline timed out during scrape/download"
        _append_job_log(job_id, "Pipeline timed out during scrape/download")
    except Exception as exc:
        job.status = "ERROR"
        job.error_msg = str(exc)[:500]
        _append_job_log(job_id, f"Unhandled pipeline error: {str(exc)[:500]}")
    finally:
        job.finished_at = datetime.utcnow()
        db.commit()
        db.close()
        try:
            _export_company_data_snapshot(company_id=company_id, company_name=company_name)
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
