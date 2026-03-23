"""
seed.py — Seeds default users and initial config on first run.
"""
from __future__ import annotations

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from backend.database.db import get_session, init_db
from backend.database.models import User
from backend.api.auth_utils import hash_password


DEFAULT_USERS = [
    {
        "name": "System Admin",
        "email": "admin@rubicr.com",
        "password": "admin123",
        "role": "ADMIN",
    },
    {
        "name": "Operations Manager",
        "email": "ops@rubicr.com",
        "password": "ops123",
        "role": "OPERATIONS_MANAGER",
    },
]


def seed_default_users():
    init_db()
    db = get_session()
    try:
        for u in DEFAULT_USERS:
            existing = db.query(User).filter(User.email == u["email"]).first()
            if not existing:
                user = User(
                    name=u["name"],
                    email=u["email"],
                    password_hash=hash_password(u["password"]),
                    role=u["role"],
                    is_active=True,
                )
                db.add(user)
        db.commit()
    finally:
        db.close()


if __name__ == "__main__":
    seed_default_users()
    print("✓ Default users seeded:")
    print("  admin@rubicr.com  / admin123  (ADMIN)")
    print("  ops@rubicr.com    / ops123    (OPERATIONS_MANAGER)")
