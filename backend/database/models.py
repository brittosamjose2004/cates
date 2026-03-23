from sqlalchemy import (
    Column, Integer, String, Text, Float, DateTime,
    ForeignKey, UniqueConstraint, Boolean, JSON
)
from sqlalchemy.orm import declarative_base, relationship
from datetime import datetime

Base = declarative_base()


class Company(Base):
    """One row per unique company."""
    __tablename__ = "companies"

    id            = Column(Integer, primary_key=True, autoincrement=True)
    name          = Column(String(255), nullable=False, unique=True)
    cin           = Column(String(50),  nullable=True)   # BSE/NSE company ID
    sector        = Column(String(100), nullable=True)
    industry      = Column(String(100), nullable=True)
    exchange      = Column(String(20),  nullable=True)
    ticker        = Column(String(20),  nullable=True)
    website       = Column(String(255), nullable=True)
    headquarters  = Column(String(255), nullable=True)
    description   = Column(Text,        nullable=True)
    created_at    = Column(DateTime, default=datetime.utcnow)

    scraped_data = relationship("ScrapedData",         back_populates="company", cascade="all, delete-orphan")
    sessions     = relationship("QuestionnaireSession", back_populates="company", cascade="all, delete-orphan")
    answers      = relationship("Answer",               back_populates="company", cascade="all, delete-orphan")
    evidence     = relationship("EvidenceSource",       foreign_keys="[EvidenceSource.company_id]")
    pipeline_jobs = relationship("PipelineJob",         foreign_keys="[PipelineJob.company_id]")


class ScrapedData(Base):
    """Raw key-value scraped data per company per year per source."""
    __tablename__ = "scraped_data"

    id         = Column(Integer, primary_key=True, autoincrement=True)
    company_id = Column(Integer, ForeignKey("companies.id"), nullable=False)
    year       = Column(Integer, nullable=False)
    source     = Column(String(50),  nullable=False)   # yahoo / screener / manual
    data_key   = Column(String(100), nullable=False)
    data_value = Column(Text,        nullable=True)
    scraped_at = Column(DateTime, default=datetime.utcnow)

    company = relationship("Company", back_populates="scraped_data")

    __table_args__ = (
        UniqueConstraint("company_id", "year", "source", "data_key", name="uix_scraped"),
    )


class QuestionnaireSession(Base):
    """One session per company × year × standard combination."""
    __tablename__ = "questionnaire_sessions"

    id                 = Column(Integer, primary_key=True, autoincrement=True)
    company_id         = Column(Integer, ForeignKey("companies.id"), nullable=False)
    year               = Column(Integer, nullable=False)
    standard           = Column(String(20), default="ALL")   # ALL / BRSR / CDP / EcoVadis / GRI
    status             = Column(String(20), default="in_progress")  # in_progress / completed
    total_questions    = Column(Integer, default=0)
    answered_questions = Column(Integer, default=0)
    created_at         = Column(DateTime, default=datetime.utcnow)
    updated_at         = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

    company = relationship("Company", back_populates="sessions")
    answers = relationship("Answer",  back_populates="session")

    __table_args__ = (
        UniqueConstraint("company_id", "year", "standard", name="uix_session"),
    )


class Answer(Base):
    """One answer per company × year × indicator (unique)."""
    __tablename__ = "answers"

    id              = Column(Integer, primary_key=True, autoincrement=True)
    session_id      = Column(Integer, ForeignKey("questionnaire_sessions.id"), nullable=False)
    company_id      = Column(Integer, ForeignKey("companies.id"),              nullable=False)
    year            = Column(Integer, nullable=False)
    indicator_id    = Column(String(30),  nullable=False)   # IMP-M01-I01
    module          = Column(String(100), nullable=True)
    indicator_name  = Column(String(255), nullable=True)
    question_text   = Column(Text,        nullable=True)
    answer_value    = Column(Text,        nullable=True)
    answer_unit     = Column(String(50),  nullable=True)
    response_format = Column(String(50),  nullable=True)
    source          = Column(String(30),  default="manual")   # manual / scraped / historical
    confidence      = Column(Float,       nullable=True)       # 0–1 confidence for auto-filled
    notes           = Column(Text,        nullable=True)
    is_verified     = Column(Boolean,     default=False)
    created_at      = Column(DateTime, default=datetime.utcnow)
    updated_at      = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

    company = relationship("Company", back_populates="answers")
    session = relationship("QuestionnaireSession", back_populates="answers")

    __table_args__ = (
        UniqueConstraint("company_id", "year", "indicator_id", name="uix_answer"),
    )


# ── User ──────────────────────────────────────────────────────────────────────

class User(Base):
    """Platform users — ADMIN or OPERATIONS_MANAGER."""
    __tablename__ = "users"

    id            = Column(Integer, primary_key=True, autoincrement=True)
    name          = Column(String(100), nullable=False)
    email         = Column(String(255), nullable=False, unique=True)
    password_hash = Column(String(255), nullable=False)
    role          = Column(String(30),  nullable=False, default="OPERATIONS_MANAGER")
    is_active     = Column(Boolean, default=True)
    created_at    = Column(DateTime, default=datetime.utcnow)

    approval_requests = relationship("ApprovalRequest", back_populates="submitter", foreign_keys="[ApprovalRequest.submitted_by_id]")


# ── ApprovalRequest ────────────────────────────────────────────────────────────

class ApprovalRequest(Base):
    """Override or Source approval requests (maker-checker workflow)."""
    __tablename__ = "approval_requests"

    id              = Column(Integer, primary_key=True, autoincrement=True)
    type            = Column(String(20), nullable=False)        # OVERRIDE | SOURCE
    company_id      = Column(Integer, ForeignKey("companies.id"), nullable=False)
    submitted_by_id = Column(Integer, ForeignKey("users.id"),    nullable=True)
    submitted_by    = Column(String(100), nullable=True)        # display name
    submitted_at    = Column(DateTime, default=datetime.utcnow)
    justification   = Column(Text, nullable=True)
    status          = Column(String(20), default="PENDING")     # PENDING | APPROVED | REJECTED
    rejection_reason = Column(Text, nullable=True)
    reviewed_by     = Column(String(100), nullable=True)
    reviewed_at     = Column(DateTime, nullable=True)

    # For OVERRIDE type
    indicator_id    = Column(String(30),  nullable=True)
    indicator_name  = Column(String(255), nullable=True)
    current_value   = Column(Text, nullable=True)
    new_value       = Column(Text, nullable=True)

    # For SOURCE type
    source_type     = Column(String(20),  nullable=True)       # PDF | URL | CSV
    source_name     = Column(String(500), nullable=True)
    source_tags     = Column(JSON, nullable=True)              # list of tag strings

    company   = relationship("Company",  foreign_keys=[company_id])
    submitter = relationship("User",     back_populates="approval_requests", foreign_keys=[submitted_by_id])


# ── EvidenceSource ─────────────────────────────────────────────────────────────

class EvidenceSource(Base):
    """Data sources / evidence attached to a company."""
    __tablename__ = "evidence_sources"

    id         = Column(Integer, primary_key=True, autoincrement=True)
    company_id = Column(Integer, ForeignKey("companies.id"), nullable=False)
    type       = Column(String(20), nullable=False)            # PDF | URL | NEWS | CSV
    name       = Column(String(500), nullable=False)
    date       = Column(String(20), nullable=True)
    status     = Column(String(30), default="processed")       # processed | processing | error | pending_review
    tags       = Column(JSON, nullable=True)                   # list of tag strings
    created_at = Column(DateTime, default=datetime.utcnow)

    company = relationship("Company", foreign_keys=[company_id], overlaps="evidence")


# ── PipelineJob ────────────────────────────────────────────────────────────────

class PipelineJob(Base):
    """Tracks pipeline runs per company."""
    __tablename__ = "pipeline_jobs"

    id           = Column(Integer, primary_key=True, autoincrement=True)
    company_id   = Column(Integer, ForeignKey("companies.id"), nullable=False)
    company_name = Column(String(255), nullable=True)
    year         = Column(Integer, nullable=True)
    status       = Column(String(30), default="QUEUED")        # QUEUED | FETCHING | SCORING | NEEDS_REVIEW | PUBLISHED | ERROR
    data_sources = Column(JSON, nullable=True)
    error_msg    = Column(Text, nullable=True)
    triggered_by = Column(String(100), nullable=True)
    started_at   = Column(DateTime, default=datetime.utcnow)
    finished_at  = Column(DateTime, nullable=True)

    company = relationship("Company", foreign_keys=[company_id], overlaps="pipeline_jobs")


# ── RiskConfig ─────────────────────────────────────────────────────────────────

class RiskConfig(Base):
    """JSON blob for global risk engine configuration."""
    __tablename__ = "risk_configs"

    id          = Column(Integer, primary_key=True, autoincrement=True)
    config_type = Column(String(50), nullable=False, unique=True)   # weights | thresholds
    config_data = Column(JSON, nullable=False)
    updated_at  = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    updated_by  = Column(String(100), nullable=True)


# ── DomainRule ─────────────────────────────────────────────────────────────────

class DomainRule(Base):
    """Trusted/allowed data source domains."""
    __tablename__ = "domain_rules"

    id        = Column(Integer, primary_key=True, autoincrement=True)
    domain    = Column(String(255), nullable=False, unique=True)
    type      = Column(String(20), nullable=False, default="SECONDARY")   # SECONDARY | TERTIARY
    sub_type  = Column(String(50), nullable=True)
    status    = Column(String(20), default="ACTIVE")                      # ACTIVE | INACTIVE
    added_by  = Column(String(100), nullable=True)
    added_at  = Column(DateTime, default=datetime.utcnow)


# ── BlockedDomain ──────────────────────────────────────────────────────────────

class BlockedDomain(Base):
    """Blocked/untrusted data source URLs or domains."""
    __tablename__ = "blocked_domains"

    id       = Column(Integer, primary_key=True, autoincrement=True)
    url      = Column(String(500), nullable=False, unique=True)
    added_at = Column(DateTime, default=datetime.utcnow)
    added_by = Column(String(100), nullable=True)
