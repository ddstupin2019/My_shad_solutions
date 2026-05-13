import base64
import hashlib
import hmac
import os
import secrets
from datetime import datetime, timedelta, timezone

import jwt
from fastapi import Depends, HTTPException
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials

JWT_SECRET = os.getenv("JWT_SECRET", "dev-secret-change-me-32-bytes-min")
JWT_ALGO = "HS256"
TOKEN_EXPIRE_SECONDS = 86400

PASSWORD_ITERATIONS = 120_000

security = HTTPBearer(auto_error=False)


def _b64encode(data: bytes) -> str:
    return base64.urlsafe_b64encode(data).rstrip(b"=").decode("ascii")


def _b64decode(data: str) -> bytes:
    padding = "=" * (-len(data) % 4)
    return base64.urlsafe_b64decode(data + padding)


def hash_password(password: str) -> str:
    salt = secrets.token_bytes(16)
    digest = hashlib.pbkdf2_hmac(
        "sha256",
        password.encode("utf-8"),
        salt,
        PASSWORD_ITERATIONS,
    )
    return "$".join(
        (
            "pbkdf2_sha256",
            str(PASSWORD_ITERATIONS),
            _b64encode(salt),
            _b64encode(digest),
        )
    )


def verify_password(password: str, password_hash: str) -> bool:
    try:
        algorithm, iterations, salt, expected = password_hash.split("$", 3)
        if algorithm != "pbkdf2_sha256":
            return False
        digest = hashlib.pbkdf2_hmac(
            "sha256",
            password.encode("utf-8"),
            _b64decode(salt),
            int(iterations),
        )
    except (ValueError, TypeError):
        return False

    return hmac.compare_digest(_b64encode(digest), expected)


# --- jwt ---
def create_token(user_id: str) -> str:
    expires_at = datetime.now(timezone.utc) + timedelta(seconds=TOKEN_EXPIRE_SECONDS)
    payload = {
        "sub": user_id,
        "exp": expires_at,
    }
    return jwt.encode(payload, JWT_SECRET, algorithm=JWT_ALGO)


def decode_token(token: str) -> str:
    try:
        payload = jwt.decode(token, JWT_SECRET, algorithms=[JWT_ALGO])
        user_id = payload["sub"]
        if not isinstance(user_id, str) or not user_id:
            raise ValueError
        return user_id
    except jwt.ExpiredSignatureError:
        raise HTTPException(status_code=401, detail="Token expired") from None
    except (jwt.InvalidTokenError, ValueError, KeyError, TypeError):
        raise HTTPException(status_code=401, detail="Invalid token") from None


async def get_current_user(
    creds: HTTPAuthorizationCredentials | None = Depends(security),
) -> str:
    if creds is None or creds.scheme.lower() != "bearer":
        raise HTTPException(status_code=401, detail="Bearer token required")
    return decode_token(creds.credentials)
