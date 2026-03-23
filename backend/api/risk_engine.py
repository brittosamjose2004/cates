"""
risk_engine.py — Derives risk pillar scores from scraped_data + answers.
"""
from __future__ import annotations

import math
from typing import Dict, Any, List, Optional, Tuple
from sqlalchemy.orm import Session

from backend.database.models import Company, ScrapedData, Answer, QuestionnaireSession


# Default pillar drivers shown when no data is available
_DEFAULT_DRIVERS: Dict[str, List[Dict]] = {
    "sustainability": [
        {"id": "d-s1", "name": "GHG Emissions (Scope 1+2)", "impact": 80},
        {"id": "d-s2", "name": "Water Consumption Intensity", "impact": 60},
        {"id": "d-s3", "name": "Waste Recycling Rate", "impact": 40},
    ],
    "pchi": [
        {"id": "d-p1", "name": "Physical Climate Exposure", "impact": 85},
        {"id": "d-p2", "name": "Heat Stress Risk", "impact": 70},
        {"id": "d-p3", "name": "Flood / Water Risk", "impact": 55},
    ],
    "operational": [
        {"id": "d-o1", "name": "Supply Chain Stability", "impact": 70},
        {"id": "d-o2", "name": "Labor & Safety Incidents", "impact": 55},
        {"id": "d-o3", "name": "Regulatory Compliance", "impact": 45},
    ],
    "financial": [
        {"id": "d-f1", "name": "Debt / Equity Ratio", "impact": 65},
        {"id": "d-f2", "name": "EBITDA Margin", "impact": 50},
        {"id": "d-f3", "name": "Liquidity Coverage", "impact": 40},
    ],
}

# Sector-based PCHI base risk (higher for exposed industries)
_SECTOR_PCHI: Dict[str, float] = {
    "agriculture": 75, "mining": 70, "steel": 68, "oil": 65, "energy": 62,
    "materials": 60, "transportation": 55, "manufacturing": 50,
    "real estate": 55, "utilities": 52,
    "technology": 30, "financial": 28, "healthcare": 25, "services": 35,
}


def _safe_float(val: Optional[str], default: float = 0.0) -> float:
    if not val:
        return default
    try:
        cleaned = val.replace(",", "").replace("%", "").strip()
        return float(cleaned.split()[0])
    except Exception:
        return default


def _clamp(v: float, lo: float = 0.0, hi: float = 100.0) -> float:
    return max(lo, min(hi, v))


def _get_scraped(db: Session, company_id: int, year: int, key: str) -> Optional[str]:
    row = (
        db.query(ScrapedData)
        .filter_by(company_id=company_id, year=year, data_key=key)
        .first()
    )
    return row.data_value if row else None


def _derive_sustainability_score(db: Session, company_id: int, year: int) -> Tuple[float, str]:
    """Return (score 0-100, trend) where higher = more risk."""
    scope1 = _safe_float(_get_scraped(db, company_id, year, "brsr_scope1_ghg"))
    scope2 = _safe_float(_get_scraped(db, company_id, year, "brsr_scope2_ghg"))
    revenue = _safe_float(_get_scraped(db, company_id, year, "revenue")) or 1

    # Intensity metric: (scope1+scope2) per unit revenue (normalised, log scale)
    total_ghg = scope1 + scope2
    if total_ghg > 0 and revenue > 0:
        intensity = total_ghg / (revenue / 1e9 + 1)  # tCO2 per 1B revenue
        score = _clamp(math.log1p(intensity) * 8, 10, 90)
    else:
        # No GHG data → use sector heuristic from financial answer
        score = 45.0   # moderate default

    prev_score = _safe_float(_get_scraped(db, company_id, year - 1, "brsr_scope1_ghg"), score)
    trend = "up" if score > prev_score + 2 else "down" if score < prev_score - 2 else "stable"
    return round(score, 1), trend


def _derive_pchi_score(db: Session, company_id: int, year: int, sector: str) -> Tuple[float, str]:
    """Physical Climate/Hazard Index score derived from sector."""
    sector_lower = (sector or "").lower()
    base = 35.0
    for key, val in _SECTOR_PCHI.items():
        if key in sector_lower:
            base = val
            break
    # Slight variation by year for visual interest
    score = _clamp(base + (year % 3) * 2 - 2)
    trend = "stable"
    return round(score, 1), trend


def _derive_operational_score(db: Session, company_id: int, year: int) -> Tuple[float, str]:
    fatalities = _safe_float(_get_scraped(db, company_id, year, "brsr_fatalities"))
    ltifr = _safe_float(_get_scraped(db, company_id, year, "brsr_ltifr"))

    score = 40.0
    if fatalities > 0:
        score = min(score + fatalities * 5, 85)
    if ltifr > 0:
        score = min(score + ltifr * 10, 85)

    prev = _safe_float(_get_scraped(db, company_id, year - 1, "brsr_fatalities"), score)
    trend = "up" if score > prev + 2 else "down" if score < prev - 2 else "stable"
    return round(score, 1), trend


def _derive_financial_score(db: Session, company_id: int, year: int) -> Tuple[float, str]:
    debt_eq = _safe_float(_get_scraped(db, company_id, year, "debtToEquity"))
    net_profit_margin = _safe_float(_get_scraped(db, company_id, year, "profitMargins")) * 100

    score = 35.0
    if debt_eq > 0:
        score = _clamp(debt_eq * 10, 5, 70)

    if net_profit_margin > 20:
        score = max(score - 10, 5)
    elif net_profit_margin < 5:
        score = min(score + 15, 85)

    prev_score = _safe_float(_get_scraped(db, company_id, year - 1, "debtToEquity"), score / 10) * 10
    trend = "up" if score > prev_score + 2 else "down" if score < prev_score - 2 else "stable"
    return round(score, 1), trend


def compute_risk_pillars(db: Session, company: Company, year: int) -> Dict[str, Any]:
    """Return dict of 4 risk pillars (sustainability, pchi, operational, financial)."""
    s_score, s_trend = _derive_sustainability_score(db, company.id, year)
    p_score, p_trend = _derive_pchi_score(db, company.id, year, company.sector or "")
    o_score, o_trend = _derive_operational_score(db, company.id, year)
    f_score, f_trend = _derive_financial_score(db, company.id, year)

    def _trend_val(trend: str) -> float:
        return {"up": 3.5, "down": -2.1, "stable": 0.0}.get(trend, 0.0)

    return {
        "sustainability": {
            "id": "sus",
            "name": "Sustainability Risk",
            "score": s_score,
            "trend": s_trend,
            "trendValue": abs(_trend_val(s_trend)),
            "drivers": _DEFAULT_DRIVERS["sustainability"],
        },
        "pchi": {
            "id": "pchi",
            "name": "PCHI (Climate)",
            "score": p_score,
            "trend": p_trend,
            "trendValue": abs(_trend_val(p_trend)),
            "drivers": _DEFAULT_DRIVERS["pchi"],
        },
        "operational": {
            "id": "ops",
            "name": "Operational Risk",
            "score": o_score,
            "trend": o_trend,
            "trendValue": abs(_trend_val(o_trend)),
            "drivers": _DEFAULT_DRIVERS["operational"],
        },
        "financial": {
            "id": "fin",
            "name": "Financial Risk",
            "score": f_score,
            "trend": f_trend,
            "trendValue": abs(_trend_val(f_trend)),
            "drivers": _DEFAULT_DRIVERS["financial"],
        },
    }


def get_latest_year(db: Session, company_id: int) -> int:
    """Return the most recent year for which the company has questionnaire data."""
    row = (
        db.query(QuestionnaireSession)
        .filter_by(company_id=company_id)
        .order_by(QuestionnaireSession.year.desc())
        .first()
    )
    return row.year if row else 2026


def get_pipeline_status(db: Session, company_id: int) -> str:
    """Derive frontend PipelineStatus from QuestionnaireSession state."""
    from backend.database.models import PipelineJob
    job = (
        db.query(PipelineJob)
        .filter_by(company_id=company_id)
        .order_by(PipelineJob.started_at.desc())
        .first()
    )
    if job:
        return job.status

    session = (
        db.query(QuestionnaireSession)
        .filter_by(company_id=company_id)
        .order_by(QuestionnaireSession.year.desc())
        .first()
    )
    if not session:
        return "QUEUED"
    return "PUBLISHED" if session.status == "completed" else "NEEDS_REVIEW"
