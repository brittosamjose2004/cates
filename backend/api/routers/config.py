"""
routers/config.py — Risk engine configuration (weights, thresholds, domains, blocked).
"""
from __future__ import annotations

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent.parent.parent))

from datetime import datetime
from typing import List
from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy.orm import Session

from backend.api.deps import get_db, get_current_user
from backend.api.schemas import (
    DriverWeightItem, ThresholdsConfig,
    DomainRuleOut, AddDomainRequest,
    BlockedDomainOut, BlockUrlsRequest,
)
from backend.database.models import RiskConfig, DomainRule, BlockedDomain, User

router = APIRouter(prefix="/api/config", tags=["config"])

# ── Default data ──────────────────────────────────────────────────────────────

DEFAULT_WEIGHTS = [
    {"id": "w1", "name": "Carbon Emissions (Scope 1 & 2)", "category": "Sustainability", "weight": 20},
    {"id": "w2", "name": "Physical Climate Risk (Heat/Flood)", "category": "PCHI", "weight": 25},
    {"id": "w3", "name": "Supply Chain Stability", "category": "Operational", "weight": 15},
    {"id": "w4", "name": "Liquidity & Solvency", "category": "Financial", "weight": 20},
    {"id": "w5", "name": "Labor Practices & Human Rights", "category": "Operational", "weight": 10},
    {"id": "w6", "name": "Corporate Governance", "category": "Sustainability", "weight": 10},
]

DEFAULT_THRESHOLDS = {"medium": 45, "high": 75}

DEFAULT_DOMAINS = [
    {"id": "d1", "domain": "reuters.com",   "type": "SECONDARY", "sub_type": None,          "status": "ACTIVE", "added_by": "System", "date": "2024-01-15"},
    {"id": "d2", "domain": "bloomberg.com", "type": "SECONDARY", "sub_type": None,          "status": "ACTIVE", "added_by": "System", "date": "2023-11-20"},
    {"id": "d3", "domain": "twitter.com",   "type": "TERTIARY",  "sub_type": "Social Media","status": "ACTIVE", "added_by": "Admin",  "date": "2024-02-10"},
    {"id": "d4", "domain": "reddit.com",    "type": "TERTIARY",  "sub_type": "Forum",       "status": "INACTIVE","added_by": "Ops",  "date": "2024-03-01"},
    {"id": "d5", "domain": "ft.com",        "type": "SECONDARY", "sub_type": None,          "status": "ACTIVE", "added_by": "Admin",  "date": "2024-01-20"},
    {"id": "d6", "domain": "bbc.co.uk",     "type": "TERTIARY",  "sub_type": "News",        "status": "ACTIVE", "added_by": "System", "date": "2023-12-05"},
]


def _get_or_create_config(db: Session, config_type: str, default: dict) -> dict:
    row = db.query(RiskConfig).filter_by(config_type=config_type).first()
    if not row:
        row = RiskConfig(config_type=config_type, config_data=default)
        db.add(row)
        db.commit()
        db.refresh(row)
    return row.config_data


def _seed_domains(db: Session):
    if db.query(DomainRule).count() == 0:
        for d in DEFAULT_DOMAINS:
            db.add(DomainRule(
                domain=d["domain"], type=d["type"], sub_type=d.get("sub_type"),
                status=d["status"], added_by=d["added_by"],
            ))
        db.commit()


# ── Weights ────────────────────────────────────────────────────────────────────

@router.get("/weights", response_model=List[DriverWeightItem])
def get_weights(db: Session = Depends(get_db)):
    data = _get_or_create_config(db, "weights", {"items": DEFAULT_WEIGHTS})
    return [DriverWeightItem(**item) for item in data.get("items", DEFAULT_WEIGHTS)]


@router.put("/weights", response_model=List[DriverWeightItem])
def update_weights(items: List[DriverWeightItem], db: Session = Depends(get_db)):
    row = db.query(RiskConfig).filter_by(config_type="weights").first()
    data = {"items": [item.model_dump() for item in items]}
    if row:
        row.config_data = data
        row.updated_at = datetime.utcnow()
    else:
        row = RiskConfig(config_type="weights", config_data=data)
        db.add(row)
    db.commit()
    return items


# ── Thresholds ─────────────────────────────────────────────────────────────────

@router.get("/thresholds", response_model=ThresholdsConfig)
def get_thresholds(db: Session = Depends(get_db)):
    data = _get_or_create_config(db, "thresholds", DEFAULT_THRESHOLDS)
    return ThresholdsConfig(**data)


@router.put("/thresholds", response_model=ThresholdsConfig)
def update_thresholds(body: ThresholdsConfig, db: Session = Depends(get_db)):
    row = db.query(RiskConfig).filter_by(config_type="thresholds").first()
    data = body.model_dump()
    if row:
        row.config_data = data
        row.updated_at = datetime.utcnow()
    else:
        row = RiskConfig(config_type="thresholds", config_data=data)
        db.add(row)
    db.commit()
    return body


# ── Trusted Domains ────────────────────────────────────────────────────────────

@router.get("/domains", response_model=List[DomainRuleOut])
def list_domains(db: Session = Depends(get_db)):
    _seed_domains(db)
    rows = db.query(DomainRule).order_by(DomainRule.added_at.desc()).all()
    return [
        DomainRuleOut(
            id=str(r.id), domain=r.domain, type=r.type, sub_type=r.sub_type,
            status=r.status, added_by=r.added_by or "System",
            date=r.added_at.strftime("%Y-%m-%d") if r.added_at else "",
        )
        for r in rows
    ]


@router.post("/domains", response_model=DomainRuleOut, status_code=status.HTTP_201_CREATED)
def add_domain(body: AddDomainRequest, db: Session = Depends(get_db)):
    existing = db.query(DomainRule).filter_by(domain=body.domain).first()
    if existing:
        raise HTTPException(status_code=400, detail="Domain already exists")
    row = DomainRule(domain=body.domain, type=body.type, sub_type=body.sub_type, added_by="You", status="ACTIVE")
    db.add(row)
    db.commit()
    db.refresh(row)
    return DomainRuleOut(
        id=str(row.id), domain=row.domain, type=row.type, sub_type=row.sub_type,
        status=row.status, added_by=row.added_by or "You",
        date=row.added_at.strftime("%Y-%m-%d") if row.added_at else "",
    )


@router.delete("/domains/{domain_id}", status_code=status.HTTP_204_NO_CONTENT)
def delete_domain(domain_id: str, db: Session = Depends(get_db)):
    row = db.query(DomainRule).filter_by(id=int(domain_id)).first()
    if not row:
        raise HTTPException(status_code=404, detail="Domain not found")
    db.delete(row)
    db.commit()


@router.put("/domains/{domain_id}/toggle", response_model=DomainRuleOut)
def toggle_domain(domain_id: str, db: Session = Depends(get_db)):
    row = db.query(DomainRule).filter_by(id=int(domain_id)).first()
    if not row:
        raise HTTPException(status_code=404, detail="Domain not found")
    row.status = "INACTIVE" if row.status == "ACTIVE" else "ACTIVE"
    db.commit()
    return DomainRuleOut(
        id=str(row.id), domain=row.domain, type=row.type, sub_type=row.sub_type,
        status=row.status, added_by=row.added_by or "System",
        date=row.added_at.strftime("%Y-%m-%d") if row.added_at else "",
    )


# ── Blocked Domains ────────────────────────────────────────────────────────────

@router.get("/blocked", response_model=List[BlockedDomainOut])
def list_blocked(db: Session = Depends(get_db)):
    rows = db.query(BlockedDomain).order_by(BlockedDomain.added_at.desc()).all()
    return [
        BlockedDomainOut(
            id=str(r.id), url=r.url,
            date=r.added_at.strftime("%Y-%m-%d") if r.added_at else "",
        )
        for r in rows
    ]


@router.post("/blocked", response_model=List[BlockedDomainOut], status_code=status.HTTP_201_CREATED)
def block_urls(body: BlockUrlsRequest, db: Session = Depends(get_db)):
    added = []
    for url in body.urls:
        url = url.strip()
        if not url:
            continue
        existing = db.query(BlockedDomain).filter_by(url=url).first()
        if existing:
            continue
        row = BlockedDomain(url=url, added_by="You")
        db.add(row)
        db.flush()
        added.append(row)
    db.commit()
    return [
        BlockedDomainOut(
            id=str(r.id), url=r.url,
            date=r.added_at.strftime("%Y-%m-%d") if r.added_at else "",
        )
        for r in added
    ]


@router.delete("/blocked/{blocked_id}", status_code=status.HTTP_204_NO_CONTENT)
def unblock_url(blocked_id: str, db: Session = Depends(get_db)):
    row = db.query(BlockedDomain).filter_by(id=int(blocked_id)).first()
    if not row:
        raise HTTPException(status_code=404, detail="Blocked URL not found")
    db.delete(row)
    db.commit()
