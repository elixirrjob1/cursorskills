"""Simulated Bearer token authentication for API endpoints."""

import os
from fastapi import Header, HTTPException


async def require_bearer_token(authorization: str | None = Header(default=None)) -> None:
    """
    Dependency: require Authorization: Bearer <token> and validate against API_AUTH_TOKEN.
    Raises 401 if header is missing or token does not match.
    """
    token = os.environ.get("API_AUTH_TOKEN")
    if not token:
        raise HTTPException(
            status_code=503,
            detail="Server configuration error: API_AUTH_TOKEN not set",
        )
    if not authorization or not authorization.strip().lower().startswith("bearer "):
        raise HTTPException(status_code=401, detail="Unauthorized")
    value = authorization[7:].strip()
    if value != token:
        raise HTTPException(status_code=401, detail="Unauthorized")
