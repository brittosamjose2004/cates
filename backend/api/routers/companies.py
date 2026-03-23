"""
routers/companies.py — Company CRUD, risk scores, indicators, evidence.
"""
from __future__ import annotations

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent.parent.parent))

from datetime import datetime, timezone
from typing import List, Optional
from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy.orm import Session

from backend.api.deps import get_db, get_current_user
from backend.api.schemas import (
    CompanySummaryOut, CompanyDetailOut, AddCompanyRequest, CompanyCreatedOut,
    RiskScores, IndicatorOut, EvidenceItemOut, RiskPillarOut, DriverOut,
)
from backend.api.risk_engine import compute_risk_pillars, get_latest_year, get_pipeline_status
from backend.processor.csv_loader import ImpactreeCSVLoader
from backend.database.models import (
    Company, Answer, QuestionnaireSession, EvidenceSource, User, ScrapedData
)

router = APIRouter(prefix="/api/companies", tags=["companies"])

# ── Region heuristics ─────────────────────────────────────────────────────────

_REGION_MAP = {
    "india": "APAC", "in": "APAC", "apac": "APAC",
    "usa": "NA", "us": "NA", "united states": "NA", "na": "NA",
    "uk": "EU", "germany": "EU", "france": "EU", "eu": "EU",
    "brazil": "LATAM", "mexico": "LATAM", "latam": "LATAM",
    "uae": "EMEA", "saudi": "EMEA", "africa": "EMEA", "emea": "EMEA",
}

def _infer_region(company: Company) -> str:
    hq = (company.headquarters or "").lower()
    for key, region in _REGION_MAP.items():
        if key in hq:
            return region
    return "APAC"

def _time_ago(dt: Optional[datetime]) -> str:
    if not dt:
        return "Unknown"
    now = datetime.utcnow()
    diff = now - dt
    if diff.total_seconds() < 3600:
        return f"{int(diff.total_seconds() // 60)} mins ago"
    if diff.days == 0:
        return f"{int(diff.total_seconds() // 3600)} hours ago"
    if diff.days == 1:
        return "1 day ago"
    return f"{diff.days} days ago"


# ── GET /api/companies ───────────────────────────────────────────────────────

@router.get("", response_model=List[CompanySummaryOut])
def list_companies(db: Session = Depends(get_db)):
    companies = db.query(Company).order_by(Company.name).all()
    result = []
    for c in companies:
        year = get_latest_year(db, c.id)
        pillars = compute_risk_pillars(db, c, year)
        pipeline_status = get_pipeline_status(db, c.id)

        result.append(CompanySummaryOut(
            id=str(c.id),
            name=c.name,
            ticker=c.ticker or "",
            lei=c.cin or "",
            region=_infer_region(c),
            sector=c.sector or "Unknown",
            status=pipeline_status,
            riskScores=RiskScores(
                s=pillars["sustainability"]["score"],
                p=pillars["pchi"]["score"],
                o=pillars["operational"]["score"],
                f=pillars["financial"]["score"],
            ),
            financialYear=f"FY{year}",
            lastUpdated=_time_ago(c.created_at),
        ))
    return result


# ── POST /api/companies ──────────────────────────────────────────────────────

@router.post("", response_model=CompanyCreatedOut, status_code=status.HTTP_201_CREATED)
def add_company(body: AddCompanyRequest, db: Session = Depends(get_db)):
    existing = db.query(Company).filter(Company.name.ilike(f"%{body.name}%")).first()
    if existing:
        raise HTTPException(status_code=400, detail="Company already exists")
    company = Company(
        name=body.name,
        ticker=body.ticker or "",
        cin=body.lei or "",
        sector=body.sector or "Unknown",
        headquarters=body.region or "APAC",
    )
    db.add(company)
    db.commit()
    db.refresh(company)
    return CompanyCreatedOut(
        id=str(company.id),
        name=company.name,
        ticker=company.ticker or "",
        lei=company.cin or "",
        region=body.region or "APAC",
        sector=body.sector or "Unknown",
        status="QUEUED",
        riskScores=RiskScores(),
        financialYear=f"FY{body.financial_year or 2026}",
        lastUpdated="Just now",
    )


# ── GET /api/companies/{id} ──────────────────────────────────────────────────

@router.get("/{company_id}", response_model=CompanyDetailOut)
def get_company(company_id: str, year: Optional[int] = None, db: Session = Depends(get_db)):
    company = db.query(Company).filter(Company.id == int(company_id)).first()
    if not company:
        raise HTTPException(status_code=404, detail="Company not found")

    year = year or get_latest_year(db, company.id)
    pillars_raw = compute_risk_pillars(db, company, year)
    pipeline_status = get_pipeline_status(db, company.id)

    # ── Build pillars ──────────────────────────────────────────────────────
    pillars: dict = {}
    for key, p in pillars_raw.items():
        pillars[key] = RiskPillarOut(
            id=p["id"],
            name=p["name"],
            score=p["score"],
            trend=p["trend"],
            trendValue=p["trendValue"],
            drivers=[DriverOut(**d) for d in p["drivers"]],
        )

    # ── Build indicator list with real year-wise values only ────────────────
    answers = (
        db.query(Answer)
        .filter_by(company_id=company.id, year=year)
        .order_by(Answer.indicator_id)
        .all()
    )
    answer_map = {a.indicator_id: a for a in answers}

    indicator_defs = ImpactreeCSVLoader.get_all_indicators()
    indicators: List[IndicatorOut] = []

    for ind in indicator_defs:
        ind_id = ind.get("indicator_id", "")
        ind_name = ind.get("indicator_name") or ind_id
        ans = answer_map.get(ind_id)

        if ans and ans.answer_value:
            val = ans.answer_value
            if len(val) > 220:
                val = val[:217].rstrip() + "..."
            indicators.append(IndicatorOut(
                id=ind_id,
                name=ind_name,
                value=val,
                unit=ans.answer_unit or "",
                confidence=round((ans.confidence or 0.5) * 100, 0),
                source=ans.source or "scraped",
                isOverridden=ans.is_verified or False,
                overrideReason=ans.notes,
                lastUpdated=ans.updated_at.strftime("%Y-%m-%d") if ans.updated_at else str(year),
            ))
        else:
            indicators.append(IndicatorOut(
                id=ind_id,
                name=ind_name,
                value="",
                unit=ans.answer_unit if ans else "",
                confidence=0,
                source=(ans.source if ans and ans.source else "unavailable"),
                isOverridden=False,
                overrideReason=(ans.notes if ans else "No year-specific extracted data found"),
                lastUpdated=ans.updated_at.strftime("%Y-%m-%d") if ans and ans.updated_at else str(year),
            ))

    # ── Evidence ───────────────────────────────────────────────────────────
    evidence_rows = (
        db.query(EvidenceSource)
        .filter_by(company_id=company.id)
        .order_by(EvidenceSource.created_at.desc())
        .all()
    )
    evidence: List[EvidenceItemOut] = [
        EvidenceItemOut(
            id=str(e.id),
            type=e.type,
            name=e.name,
            date=e.date or "",
            status=e.status,
            tags=e.tags or [],
        )
        for e in evidence_rows
    ]

    # Determine version string
    session = (
        db.query(QuestionnaireSession)
        .filter_by(company_id=company.id, year=year)
        .first()
    )
    version = f"v{year}.{session.id:04d}" if session else f"v{year}.0001"

    return CompanyDetailOut(
        id=str(company.id),
        name=company.name,
        ticker=company.ticker or "",
        lei=company.cin or "",
        sector=company.sector or "Unknown",
        financialYear=f"FY{year}",
        status=pipeline_status,
        lastUpdated=_time_ago(company.created_at),
        version=version,
        pillars=pillars,
        indicators=indicators,
        evidence=evidence,
    )


# ── DELETE /api/companies/{id} ───────────────────────────────────────────────

@router.delete("/{company_id}", status_code=status.HTTP_204_NO_CONTENT)
def delete_company(company_id: str, db: Session = Depends(get_db)):
    company = db.query(Company).filter(Company.id == int(company_id)).first()
    if not company:
        raise HTTPException(status_code=404, detail="Company not found")
    db.delete(company)
    db.commit()


# ── GET /api/companies/{id}/years ─────────────────────────────────────────────

@router.get("/{company_id}/years")
def get_available_years(company_id: str, db: Session = Depends(get_db)):
    cid = int(company_id)

    session_years = {
        r.year for r in (
            db.query(QuestionnaireSession.year)
            .filter_by(company_id=cid)
            .distinct()
            .all()
        )
    }
    answer_years = {
        r.year for r in (
            db.query(Answer.year)
            .filter_by(company_id=cid)
            .distinct()
            .all()
        )
    }
    scraped_years = {
        r.year for r in (
            db.query(ScrapedData.year)
            .filter_by(company_id=cid)
            .distinct()
            .all()
        )
    }

    years = sorted(session_years | answer_years | scraped_years, reverse=True)
    return {"years": [f"FY{y}" for y in years]}
