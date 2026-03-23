"""
schemas.py  — Pydantic models (request/response) for the Rubicr Caetis API.
"""
from __future__ import annotations

from datetime import datetime
from typing import Any, Dict, List, Optional
from pydantic import BaseModel, Field


# ── Auth ──────────────────────────────────────────────────────────────────────

class LoginRequest(BaseModel):
    email: str
    password: str

class UserOut(BaseModel):
    id: str
    name: str
    email: str
    role: str   # ADMIN | OPERATIONS_MANAGER

class TokenResponse(BaseModel):
    access_token: str
    token_type: str = "bearer"
    user: UserOut


# ── Companies ─────────────────────────────────────────────────────────────────

class RiskScores(BaseModel):
    s: float = 0   # Sustainability
    p: float = 0   # PCHI (Physical Climate)
    o: float = 0   # Operational
    f: float = 0   # Financial

class CompanySummaryOut(BaseModel):
    id: str
    name: str
    ticker: str
    lei: str
    region: str
    sector: str
    status: str        # PipelineStatus
    riskScores: RiskScores
    financialYear: str
    lastUpdated: str

class DriverOut(BaseModel):
    id: str
    name: str
    impact: float

class RiskPillarOut(BaseModel):
    id: str
    name: str
    score: float
    trend: str         # up | down | stable
    trendValue: float
    drivers: List[DriverOut]

class IndicatorOut(BaseModel):
    id: str
    name: str
    value: Any
    unit: str
    confidence: float  # 0–100
    source: str
    isOverridden: bool
    overrideReason: Optional[str] = None
    lastUpdated: str

class EvidenceItemOut(BaseModel):
    id: str
    type: str          # PDF | URL | NEWS | CSV
    name: str
    date: str
    status: str
    tags: List[str]

class CompanyDetailOut(BaseModel):
    id: str
    name: str
    ticker: str
    lei: str
    sector: str
    financialYear: str
    status: str
    lastUpdated: str
    version: str
    pillars: Dict[str, RiskPillarOut]
    indicators: List[IndicatorOut]
    evidence: List[EvidenceItemOut]

class AddCompanyRequest(BaseModel):
    name: str
    lei: Optional[str] = ""
    ticker: Optional[str] = ""
    region: Optional[str] = "APAC"
    sector: Optional[str] = "Unknown"
    nse_symbol: Optional[str] = None
    financial_year: Optional[int] = None

class CompanyCreatedOut(BaseModel):
    id: str
    name: str
    ticker: str
    lei: str
    region: str
    sector: str
    status: str
    riskScores: RiskScores
    financialYear: str
    lastUpdated: str


# ── Pipeline ──────────────────────────────────────────────────────────────────

class RunPipelineRequest(BaseModel):
    company_ids: List[str] = Field(default_factory=list)
    data_sources: List[str] = Field(default_factory=lambda: ["Secondary"])
    financial_years: List[str] = Field(default_factory=lambda: ["FY2026"])
    all_years: bool = False


class PipelineStatusBatchRequest(BaseModel):
    job_ids: List[str] = Field(default_factory=list)

class PipelineJobOut(BaseModel):
    id: str
    company_id: str
    company_name: str
    year: Optional[int]
    status: str
    error_msg: Optional[str] = None
    started_at: str
    finished_at: Optional[str] = None


class PipelineJobLogOut(BaseModel):
    job_id: str
    lines: List[str]


# ── Approvals ─────────────────────────────────────────────────────────────────

class SubmitOverrideRequest(BaseModel):
    company_id: str
    indicator_id: str
    indicator_name: str
    current_value: Any
    new_value: Any
    justification: str
    evidence_type: Optional[str] = "URL"
    evidence_value: Optional[str] = ""
    submitted_by: Optional[str] = "System"

class SubmitSourceRequest(BaseModel):
    company_id: str
    source_type: str      # PDF | URL | CSV
    source_name: str
    source_tags: List[str] = Field(default_factory=list)
    justification: str
    submitted_by: Optional[str] = "System"

class ApprovalRequestOut(BaseModel):
    id: str
    type: str
    company_id: str
    company_name: str
    company_ticker: str
    submitted_by: str
    submitted_at: str
    justification: str
    status: str
    # Override fields
    indicator_name: Optional[str] = None
    current_value: Optional[Any] = None
    new_value: Optional[Any] = None
    # Source fields
    source_type: Optional[str] = None
    source_name: Optional[str] = None
    source_tags: Optional[List[str]] = None

class ReviewDecision(BaseModel):
    reason: Optional[str] = None
    reviewed_by: Optional[str] = "Admin"


# ── Evidence ──────────────────────────────────────────────────────────────────

class AddEvidenceRequest(BaseModel):
    type: str       # PDF | URL | NEWS | CSV
    name: str
    tags: List[str] = Field(default_factory=list)
    justification: Optional[str] = ""
    submitted_by: Optional[str] = "System"


# ── Risk Config ────────────────────────────────────────────────────────────────

class DriverWeightItem(BaseModel):
    id: str
    name: str
    category: str
    weight: float

class ThresholdsConfig(BaseModel):
    medium: float = 45
    high: float = 75

class DomainRuleOut(BaseModel):
    id: str
    domain: str
    type: str
    sub_type: Optional[str] = None
    status: str
    added_by: str
    date: str

class AddDomainRequest(BaseModel):
    domain: str
    type: str = "SECONDARY"
    sub_type: Optional[str] = None

class BlockedDomainOut(BaseModel):
    id: str
    url: str
    date: str

class BlockUrlsRequest(BaseModel):
    urls: List[str]


# ── Analytics ────────────────────────────────────────────────────────────────

class ChartPointOut(BaseModel):
    x: str
    y: float
    year: int
    company_id: str
    company_name: str
    report_type: str

class ChartSeriesOut(BaseModel):
    key: str
    label: str
    points: List[ChartPointOut]

class AnalyticsTableRowOut(BaseModel):
    company_id: str
    company_name: str
    year: int
    report_type: str
    metric: str
    value: float

class AnalyticsMetaOut(BaseModel):
    metric: str
    years: List[int]
    company_ids: List[str]
    group_by: str
    sort_by: str
    sort_order: str
    report_type: str
    generated_at: str
    cache_ttl_seconds: int

class AnalyticsSeriesResponse(BaseModel):
    meta: AnalyticsMetaOut
    series: List[ChartSeriesOut]
    table: List[AnalyticsTableRowOut]

class AnalyticsSummaryOut(BaseModel):
    metric: str
    count: int
    average: float
    minimum: float
    maximum: float

class AnalyticsSummaryResponse(BaseModel):
    meta: AnalyticsMetaOut
    summary: AnalyticsSummaryOut
    by_year: List[Dict[str, Any]]
    by_company: List[Dict[str, Any]]
