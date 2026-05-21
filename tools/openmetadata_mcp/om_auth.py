"""OpenMetadata authentication via OM_TOKEN (Bearer JWT). Vendored for standalone MCP repo."""

from __future__ import annotations

import os

API_VERSION_PREFIX = "v1"

_MISSING_TOKEN = (
    "Missing OpenMetadata token. Set OM_TOKEN in .env "
    "(OpenMetadata UI: Settings → Access Tokens, or Settings → Bots)."
)
_MISSING_BASE_URL = (
    "Missing OpenMetadata base URL. Set OM_BASE_URL in .env "
    "(e.g. https://<host>:8585, no trailing slash)."
)


def _first_env(*names: str) -> str:
    for name in names:
        value = os.getenv(name, "").strip()
        if value:
            return value
    return ""


def om_base_url_host() -> str:
    """OpenMetadata host URL without ``/api`` suffix."""
    base = _first_env("OM_BASE_URL", "OPENMETADATA_BASE_URL")
    if not base:
        raise RuntimeError(_MISSING_BASE_URL)
    base = base.rstrip("/")
    if base.endswith("/api"):
        return base[:-4]
    return base


def om_api_root() -> str:
    return f"{om_base_url_host()}/api"


def om_api_url(path: str) -> str:
    cleaned = path.lstrip("/")
    if not cleaned.startswith(f"{API_VERSION_PREFIX}/"):
        cleaned = f"{API_VERSION_PREFIX}/{cleaned}"
    return f"{om_api_root()}/{cleaned}"


def om_token() -> str:
    token = _first_env("OM_TOKEN", "OPENMETADATA_JWT_TOKEN")
    if not token:
        raise RuntimeError(_MISSING_TOKEN)
    return token


def om_bearer_headers(
    *,
    user_agent: str = "openmetadata-client",
    extra: dict[str, str] | None = None,
) -> dict[str, str]:
    headers = {
        "Accept": "application/json",
        "Content-Type": "application/json",
        "Authorization": f"Bearer {om_token()}",
        "User-Agent": user_agent,
    }
    if extra:
        headers.update(extra)
    return headers
