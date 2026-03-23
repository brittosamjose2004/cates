"""
routers/auth.py — Authentication endpoints.
"""
from __future__ import annotations

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent.parent.parent))

from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy.orm import Session

from backend.api.deps import get_db, get_current_user
from backend.api.schemas import LoginRequest, TokenResponse, UserOut
from backend.api.auth_utils import verify_password, create_token
from backend.database.models import User

router = APIRouter(prefix="/api/auth", tags=["auth"])


@router.post("/login", response_model=TokenResponse)
def login(body: LoginRequest, db: Session = Depends(get_db)):
    user = db.query(User).filter(User.email == body.email).first()
    if not user or not verify_password(body.password, user.password_hash):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid email or password",
        )
    token = create_token(user.id, user.role)
    return TokenResponse(
        access_token=token,
        user=UserOut(id=str(user.id), name=user.name, email=user.email, role=user.role),
    )


@router.get("/me", response_model=UserOut)
def me(current_user: User = Depends(get_current_user)):
    return UserOut(
        id=str(current_user.id),
        name=current_user.name,
        email=current_user.email,
        role=current_user.role,
    )
