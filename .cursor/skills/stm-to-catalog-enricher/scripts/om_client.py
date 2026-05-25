"""Shared OpenMetadata REST helpers for the stm-to-catalog-enricher skill."""

from __future__ import annotations

import json
import sys
from pathlib import Path
from typing import Any
from urllib import error, request

_SCRIPT_DIR = Path(__file__).resolve().parent
if str(_SCRIPT_DIR) not in sys.path:
    sys.path.insert(0, str(_SCRIPT_DIR))

from om_auth import om_api_root, om_api_url, om_token  # noqa: E402


def _load_env_file(start: Path | None = None) -> None:
    """Populate os.environ with values from the nearest .env walking up from cwd."""
    import os

    cur = (start or Path.cwd()).resolve()
    for candidate in [cur, *cur.parents]:
        env_file = candidate / ".env"
        if env_file.is_file():
            for line in env_file.read_text().splitlines():
                line = line.strip()
                if not line or line.startswith("#") or "=" not in line:
                    continue
                key, _, val = line.partition("=")
                os.environ.setdefault(key.strip(), val.strip().strip('"').strip("'"))
            return


def login() -> tuple[str, str]:
    """Return (api_base_url, bearer_token) using OM_TOKEN from environment."""
    _load_env_file()
    return om_api_root(), om_token()


def _resolve_api_url(path: str) -> str:
    """Build a full OM REST URL from caller paths like ``/api/v1/tables/...``."""
    raw = path.strip()
    query = ""
    if "?" in raw:
        raw, query = raw.split("?", 1)
        query = f"?{query}"

    cleaned = raw.lstrip("/")
    if cleaned.startswith("api/"):
        cleaned = cleaned[len("api/") :]
    if cleaned.startswith("v1/"):
        cleaned = cleaned[len("v1/") :]

    return om_api_url(cleaned) + query


def api(method: str, path: str, token: str, *, body: Any = None, content_type: str | None = None) -> Any:
    """Call an OM REST endpoint. Returns parsed JSON or raises."""
    url = _resolve_api_url(path)
    payload = None
    headers = {"Authorization": f"Bearer {token}"}
    if body is not None:
        payload = json.dumps(body).encode() if not isinstance(body, (bytes, bytearray)) else bytes(body)
        headers["Content-Type"] = content_type or "application/json"
    req = request.Request(url, data=payload, headers=headers, method=method)
    try:
        with request.urlopen(req, timeout=60) as resp:
            raw = resp.read()
            return json.loads(raw) if raw else {}
    except error.HTTPError as exc:
        detail = exc.read().decode(errors="replace")[:400]
        sys.exit(f"error: OM API {method} {path} failed: {exc.code} {detail}")
