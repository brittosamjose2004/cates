"""
routers/analytics.py — Multi-year analytics, trends, and comparisons.
"""
from __future__ import annotations

import sys
import time
from collections import defaultdict
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

sys.path.insert(0, str(Path(__file__).parent.parent.parent.parent))

from fastapi import APIRouter, Depends, Query
from sqlalchemy import func
from sqlalchemy.orm import Session

from backend.api.deps import get_db
from backend.api.risk_engine import compute_risk_pillars, get_latest_year
from backend.api.schemas import (
    AnalyticsMetaOut,
    AnalyticsSeriesResponse,
    AnalyticsSummaryOut,
    AnalyticsSummaryResponse,
    AnalyticsTableRowOut,
    ChartPointOut,
    ChartSeriesOut,
)
from backend.database.models import Answer, Company, ScrapedData

router = APIRouter(prefix="/api/analytics", tags=["analytics"])

_CACHE_TTL_SECONDS = 120
_CACHE: Dict[str, Tuple[float, Any]] = {}


def _cache_get(key: str) -> Optional[Any]:
    hit = _CACHE.get(key)
    if not hit:
        return None
    expiry_ts, payload = hit
    if time.time() > expiry_ts:
        _CACHE.pop(key, None)
        return None
    return payload


def _cache_set(key: str, payload: Any) -> None:
    _CACHE[key] = (time.time() + _CACHE_TTL_SECONDS, payload)


def _parse_csv_ints(value: Optional[str]) -> List[int]:
    if not value:
        return []
    out: List[int] = []
    for token in value.split(","):
        token = token.strip().replace("FY", "")
        if token.isdigit():
            out.append(int(token))
    return sorted(set(out))


def _parse_csv_strs(value: Optional[str]) -> List[str]:
    if not value:
        return []
    return [v.strip() for v in value.split(",") if v.strip()]


def _source_to_report_type(source: str) -> str:
    src = (source or "").lower()
    if src in {"manual"}:
        return "manual"
    if src in {"pdf", "annual_report", "report"}:
        return "annual_report"
    if src in {"url", "news", "csv"}:
        return src
    return "scraped"


def _detect_report_type(db: Session, company_id: int, year: int) -> str:
    source_counts = defaultdict(int)

    answer_rows = (
        db.query(Answer.source, func.count(Answer.id))
        .filter(Answer.company_id == company_id, Answer.year == year)
        .group_by(Answer.source)
        .all()
    )
    for source, count in answer_rows:
        source_counts[_source_to_report_type(source or "")] += count

    scraped_rows = (
        db.query(ScrapedData.source, func.count(ScrapedData.id))
        .filter(ScrapedData.company_id == company_id, ScrapedData.year == year)
        .group_by(ScrapedData.source)
        .all()
    )
    for source, count in scraped_rows:
        source_counts[_source_to_report_type(source or "scraped")] += count

    if not source_counts:
        return "unknown"
    return max(source_counts.items(), key=lambda x: x[1])[0]


def _compute_metric_value(
    db: Session,
    company: Company,
    year: int,
    metric: str,
) -> Optional[float]:
    metric_l = metric.lower().strip()
    pillars = compute_risk_pillars(db, company, year)

    if metric_l == "s":
        return float(pillars["sustainability"]["score"])
    if metric_l == "p":
        return float(pillars["pchi"]["score"])
    if metric_l == "o":
        return float(pillars["operational"]["score"])
    if metric_l == "f":
        return float(pillars["financial"]["score"])
    if metric_l == "overall":
        vals = [
            float(pillars["sustainability"]["score"]),
            float(pillars["pchi"]["score"]),
            float(pillars["operational"]["score"]),
            float(pillars["financial"]["score"]),
        ]
        return round(sum(vals) / len(vals), 2)

    if metric_l.startswith("indicator:"):
        indicator_id = metric.split(":", 1)[1].strip()
        ans = (
            db.query(Answer)
            .filter(
                Answer.company_id == company.id,
                Answer.year == year,
                Answer.indicator_id == indicator_id,
            )
            .first()
        )
        if not ans or not ans.answer_value:
            return None
        try:
            cleaned = str(ans.answer_value).replace(",", "").replace("%", "").strip().split()[0]
            return float(cleaned)
        except Exception:
            return None

    if metric_l.startswith("scraped:"):
        data_key = metric.split(":", 1)[1].strip()
        row = (
            db.query(ScrapedData)
            .filter(
                ScrapedData.company_id == company.id,
                ScrapedData.year == year,
                ScrapedData.data_key == data_key,
            )
            .first()
        )
        if not row or not row.data_value:
            return None
        try:
            cleaned = str(row.data_value).replace(",", "").replace("%", "").strip().split()[0]
            return float(cleaned)
        except Exception:
            return None

    return None


def _collect_rows(
    db: Session,
    company_ids: List[str],
    years: List[int],
    metric: str,
    report_type: str,
) -> List[AnalyticsTableRowOut]:
    query = db.query(Company)
    if company_ids:
        query = query.filter(Company.id.in_([int(i) for i in company_ids]))
    companies = query.order_by(Company.name.asc()).all()

    rows: List[AnalyticsTableRowOut] = []
    for company in companies:
        target_years = years or [get_latest_year(db, company.id)]
        for year in target_years:
            value = _compute_metric_value(db, company, year, metric)
            if value is None:
                continue
            row_report_type = _detect_report_type(db, company.id, year)
            if report_type != "all" and row_report_type != report_type:
                continue
            rows.append(
                AnalyticsTableRowOut(
                    company_id=str(company.id),
                    company_name=company.name,
                    year=year,
                    report_type=row_report_type,
                    metric=metric,
                    value=round(value, 2),
                )
            )
    return rows


def _sort_rows(rows: List[AnalyticsTableRowOut], sort_by: str, sort_order: str) -> List[AnalyticsTableRowOut]:
    reverse = sort_order.lower() == "desc"
    sort_key = sort_by.lower()

    if sort_key == "value":
        return sorted(rows, key=lambda r: r.value, reverse=reverse)
    if sort_key == "company":
        return sorted(rows, key=lambda r: r.company_name.lower(), reverse=reverse)
    return sorted(rows, key=lambda r: r.year, reverse=reverse)


def _group_rows(rows: List[AnalyticsTableRowOut], group_by: str) -> List[ChartSeriesOut]:
    grouped: Dict[str, List[AnalyticsTableRowOut]] = defaultdict(list)

    for row in rows:
        key = row.company_id
        label = row.company_name
        if group_by == "year":
            key = str(row.year)
            label = f"FY{row.year}"
        elif group_by == "report_type":
            key = row.report_type
            label = row.report_type.upper()
        grouped[f"{key}|{label}"].append(row)

    series: List[ChartSeriesOut] = []
    for key_label, group_rows in grouped.items():
        key, label = key_label.split("|", 1)
        points = [
            ChartPointOut(
                x=f"FY{r.year}",
                y=r.value,
                year=r.year,
                company_id=r.company_id,
                company_name=r.company_name,
                report_type=r.report_type,
            )
            for r in sorted(group_rows, key=lambda v: v.year)
        ]
        series.append(ChartSeriesOut(key=key, label=label, points=points))

    return series


def _build_meta(
    metric: str,
    years: List[int],
    company_ids: List[str],
    group_by: str,
    sort_by: str,
    sort_order: str,
    report_type: str,
) -> AnalyticsMetaOut:
    return AnalyticsMetaOut(
        metric=metric,
        years=years,
        company_ids=company_ids,
        group_by=group_by,
        sort_by=sort_by,
        sort_order=sort_order,
        report_type=report_type,
        generated_at=datetime.utcnow().isoformat() + "Z",
        cache_ttl_seconds=_CACHE_TTL_SECONDS,
    )


@router.get("/series", response_model=AnalyticsSeriesResponse)
def get_analytics_series(
    company_ids: Optional[str] = Query(default=None, description="Comma-separated company IDs"),
    years: Optional[str] = Query(default=None, description="Comma-separated years or FY labels"),
    metric: str = Query(default="overall", description="s|p|o|f|overall|indicator:<id>|scraped:<key>"),
    report_type: str = Query(default="all", description="all|annual_report|scraped|manual|url|news|csv"),
    group_by: str = Query(default="company", description="company|year|report_type"),
    sort_by: str = Query(default="year", description="year|company|value"),
    sort_order: str = Query(default="asc", description="asc|desc"),
    limit: int = Query(default=1000, ge=1, le=5000),
    db: Session = Depends(get_db),
):
    company_ids_list = _parse_csv_strs(company_ids)
    years_list = _parse_csv_ints(years)

    cache_key = "|".join(
        [
            "series",
            ",".join(company_ids_list),
            ",".join(str(y) for y in years_list),
            metric,
            report_type,
            group_by,
            sort_by,
            sort_order,
            str(limit),
        ]
    )
    cached = _cache_get(cache_key)
    if cached:
        return cached

    rows = _collect_rows(db, company_ids_list, years_list, metric, report_type)
    rows = _sort_rows(rows, sort_by, sort_order)[:limit]
    series = _group_rows(rows, group_by)

    payload = AnalyticsSeriesResponse(
        meta=_build_meta(metric, years_list, company_ids_list, group_by, sort_by, sort_order, report_type),
        series=series,
        table=rows,
    )
    _cache_set(cache_key, payload)
    return payload


@router.get("/compare", response_model=AnalyticsSeriesResponse)
def compare_companies(
    company_ids: str = Query(..., description="Comma-separated company IDs"),
    year: int = Query(..., ge=2000, le=2100),
    metric: str = Query(default="overall"),
    sort_by: str = Query(default="value"),
    sort_order: str = Query(default="desc"),
    db: Session = Depends(get_db),
):
    # Compare is a focused wrapper around the generic chart endpoint.
    return get_analytics_series(
        company_ids=company_ids,
        years=str(year),
        metric=metric,
        report_type="all",
        group_by="company",
        sort_by=sort_by,
        sort_order=sort_order,
        limit=500,
        db=db,
    )


@router.get("/summary", response_model=AnalyticsSummaryResponse)
def get_analytics_summary(
    company_ids: Optional[str] = Query(default=None),
    years: Optional[str] = Query(default=None),
    metric: str = Query(default="overall"),
    report_type: str = Query(default="all"),
    db: Session = Depends(get_db),
):
    company_ids_list = _parse_csv_strs(company_ids)
    years_list = _parse_csv_ints(years)

    cache_key = "|".join(
        [
            "summary",
            ",".join(company_ids_list),
            ",".join(str(y) for y in years_list),
            metric,
            report_type,
        ]
    )
    cached = _cache_get(cache_key)
    if cached:
        return cached

    rows = _collect_rows(db, company_ids_list, years_list, metric, report_type)
    values = [r.value for r in rows]

    summary = AnalyticsSummaryOut(
        metric=metric,
        count=len(values),
        average=round((sum(values) / len(values)), 2) if values else 0,
        minimum=round(min(values), 2) if values else 0,
        maximum=round(max(values), 2) if values else 0,
    )

    by_year_map: Dict[int, List[float]] = defaultdict(list)
    by_company_map: Dict[str, List[float]] = defaultdict(list)
    for row in rows:
        by_year_map[row.year].append(row.value)
        by_company_map[row.company_name].append(row.value)

    by_year = [
        {
            "year": year,
            "avg": round(sum(vals) / len(vals), 2),
            "count": len(vals),
        }
        for year, vals in sorted(by_year_map.items(), key=lambda x: x[0])
    ]
    by_company = [
        {
            "company_name": name,
            "avg": round(sum(vals) / len(vals), 2),
            "count": len(vals),
        }
        for name, vals in sorted(by_company_map.items(), key=lambda x: x[0].lower())
    ]

    payload = AnalyticsSummaryResponse(
        meta=_build_meta(metric, years_list, company_ids_list, "company", "year", "asc", report_type),
        summary=summary,
        by_year=by_year,
        by_company=by_company,
    )
    _cache_set(cache_key, payload)
    return payload
