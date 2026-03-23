"""
routers/evidence.py — Evidence/source management per company.
"""
from __future__ import annotations

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent.parent.parent))

import os
from datetime import datetime
from typing import List, Optional
from fastapi import APIRouter, Depends, HTTPException, status, UploadFile, File, Form
from sqlalchemy.orm import Session

from backend.api.deps import get_db
from backend.api.schemas import AddEvidenceRequest, EvidenceItemOut, SubmitSourceRequest, ApprovalRequestOut
from backend.database.models import EvidenceSource, ApprovalRequest, Company

router = APIRouter(prefix="/api/companies/{company_id}/evidence", tags=["evidence"])


# ── GET /api/companies/{id}/evidence ─────────────────────────────────────────

@router.get("", response_model=List[EvidenceItemOut])
def list_evidence(company_id: str, db: Session = Depends(get_db)):
    rows = (
        db.query(EvidenceSource)
        .filter_by(company_id=int(company_id))
        .order_by(EvidenceSource.created_at.desc())
        .all()
    )
    return [
        EvidenceItemOut(
            id=str(e.id), type=e.type, name=e.name,
            date=e.date or "", status=e.status, tags=e.tags or [],
        )
        for e in rows
    ]


# ── POST /api/companies/{id}/evidence ────────────────────────────────────────

@router.post("", response_model=EvidenceItemOut, status_code=status.HTTP_201_CREATED)
def add_evidence(company_id: str, body: AddEvidenceRequest, db: Session = Depends(get_db)):
    company = db.query(Company).filter_by(id=int(company_id)).first()
    if not company:
        raise HTTPException(status_code=404, detail="Company not found")

    # Create evidence with pending_review status + approval request
    ev = EvidenceSource(
        company_id=int(company_id),
        type=body.type,
        name=body.name,
        date=datetime.utcnow().strftime("%Y-%m-%d"),
        status="pending_review",
        tags=body.tags,
    )
    db.add(ev)

    # Create corresponding approval request
    req = ApprovalRequest(
        type="SOURCE",
        company_id=int(company_id),
        submitted_by=body.submitted_by or "Unknown",
        justification=body.justification or "",
        status="PENDING",
        source_type=body.type,
        source_name=body.name,
        source_tags=body.tags,
    )
    db.add(req)
    db.commit()
    db.refresh(ev)

    return EvidenceItemOut(
        id=str(ev.id), type=ev.type, name=ev.name,
        date=ev.date or "", status=ev.status, tags=ev.tags or [],
    )


# ── POST /api/companies/{id}/evidence/upload ─────────────────────────────────

@router.post("/upload", response_model=EvidenceItemOut, status_code=status.HTTP_201_CREATED)
async def upload_evidence_file(
    company_id: str,
    file: UploadFile = File(...),
    tag: str = Form(...),
    submitted_by: Optional[str] = Form("Unknown"),
    db: Session = Depends(get_db),
):
    company = db.query(Company).filter_by(id=int(company_id)).first()
    if not company:
        raise HTTPException(status_code=404, detail="Company not found")

    # Determine file type from extension
    ext = Path(file.filename or "file").suffix.lower()
    type_map = {".pdf": "PDF", ".xlsx": "EXCEL", ".xls": "EXCEL", ".csv": "CSV"}
    doc_type = type_map.get(ext, "PDF")

    # Save file to disk
    upload_dir = Path(__file__).parent.parent.parent.parent / "data" / "uploads" / company_id
    upload_dir.mkdir(parents=True, exist_ok=True)
    safe_name = f"{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}_{Path(file.filename or 'upload').name}"
    # Sanitize filename to prevent path traversal
    safe_name = os.path.basename(safe_name)
    dest = upload_dir / safe_name

    contents = await file.read()
    dest.write_bytes(contents)

    # Create evidence record + approval request
    ev = EvidenceSource(
        company_id=int(company_id),
        type=doc_type,
        name=file.filename or safe_name,
        date=datetime.utcnow().strftime("%Y-%m-%d"),
        status="pending_review",
        tags=[tag] if tag else [],
    )
    db.add(ev)

    req = ApprovalRequest(
        type="SOURCE",
        company_id=int(company_id),
        submitted_by=submitted_by or "Unknown",
        justification=f"File upload: {file.filename}",
        status="PENDING",
        source_type=doc_type,
        source_name=file.filename or safe_name,
        source_tags=[tag] if tag else [],
    )
    db.add(req)
    db.commit()
    db.refresh(ev)

    return EvidenceItemOut(
        id=str(ev.id), type=ev.type, name=ev.name,
        date=ev.date or "", status=ev.status, tags=ev.tags or [],
    )


# ── DELETE /api/companies/{id}/evidence/{ev_id} ───────────────────────────────

@router.delete("/{evidence_id}", status_code=status.HTTP_204_NO_CONTENT)
def delete_evidence(company_id: str, evidence_id: str, db: Session = Depends(get_db)):
    ev = db.query(EvidenceSource).filter_by(
        id=int(evidence_id), company_id=int(company_id)
    ).first()
    if not ev:
        raise HTTPException(status_code=404, detail="Evidence not found")
    db.delete(ev)
    db.commit()
