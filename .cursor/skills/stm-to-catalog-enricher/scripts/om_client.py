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

from om_auth import om_api_root, om_bearer_headers, om_token  # noqa: E402


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


def api(method: str, path: str, token: str, *, body: Any = None, content_type: str | None = None) -> Any:
    """Call an OM REST endpoint. Returns parsed JSON or raises."""
    import os

    base_url = os.environ.get("OM_BASE_URL", "") or os.environ.get("OPENMETADATA_BASE_URL", "")
    base_url = base_url.strip().rstrip("/")
    if base_url.endswith("/api"):
        base_url = base_url
    else:
        base_url = f"{base_url}/api" if base_url else om_api_root()
    url = f"{base_url}{path}" if path.startswith("/") else f"{base_url}/{path}"
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
