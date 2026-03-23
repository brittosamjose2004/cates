"""
auth_utils.py — JWT creation/verification and password hashing.
"""
from __future__ import annotations

import os
import hashlib
import hmac
import secrets
from datetime import datetime, timedelta
from typing import Optional

from jose import JWTError, jwt

SECRET_KEY = os.getenv("SECRET_KEY", "rubicr-caetis-super-secret-key-2026-change-me")
ALGORITHM = "HS256"
ACCESS_TOKEN_EXPIRE_MINUTES = 60 * 24  # 24 hours

# Use PBKDF2-SHA256 (no external bcrypt dependency issues)
_HASH_ITERATIONS = 100_000


def hash_password(password: str) -> str:
    salt = secrets.token_hex(16)
    dk = hashlib.pbkdf2_hmac("sha256", password.encode(), salt.encode(), _HASH_ITERATIONS)
    return f"{salt}${dk.hex()}"


def verify_password(plain: str, stored: str) -> bool:
    try:
        salt, dk_hex = stored.split("$", 1)
        dk = hashlib.pbkdf2_hmac("sha256", plain.encode(), salt.encode(), _HASH_ITERATIONS)
        return hmac.compare_digest(dk.hex(), dk_hex)
    except Exception:
        return False


def create_token(user_id: int, role: str) -> str:
    expire = datetime.utcnow() + timedelta(minutes=ACCESS_TOKEN_EXPIRE_MINUTES)
    payload = {"sub": str(user_id), "role": role, "exp": expire}
    return jwt.encode(payload, SECRET_KEY, algorithm=ALGORITHM)


def decode_token(token: str) -> Optional[dict]:
    try:
        return jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
    except JWTError:
        return None
