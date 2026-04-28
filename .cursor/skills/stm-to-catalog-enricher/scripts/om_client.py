"""Shared OpenMetadata REST helpers for the stm-to-openmetadata-enricher skill."""

from __future__ import annotations

import base64
import json
import os
import sys
from pathlib import Path
from typing import Any
from urllib import request, error


def _load_env_file(start: Path | None = None) -> None:
    """Populate os.environ with values from the nearest .env walking up from cwd."""
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


def _require_env(key: str) -> str:
    val = os.environ.get(key)
    if not val:
        sys.exit(f"error: required env var {key} is not set (expected in .env)")
    return val


def login() -> tuple[str, str]:
    """Return (base_url, bearer_token) after authenticating to OpenMetadata."""
    _load_env_file()
    base_url = _require_env("OPENMETADATA_BASE_URL").rstrip("/")
    email = _require_env("OPENMETADATA_EMAIL")
    password = _require_env("OPENMETADATA_PASSWORD")

    body = json.dumps(
        {"email": email, "password": base64.b64encode(password.encode()).decode()}
    ).encode()
    req = request.Request(
        f"{base_url}/api/v1/users/login",
        data=body,
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    try:
        with request.urlopen(req, timeout=30) as resp:
            data = json.load(resp)
    except error.HTTPError as exc:
        sys.exit(f"error: OM login failed: {exc.code} {exc.read().decode(errors='replace')[:400]}")
    token = data.get("accessToken")
    if not token:
        sys.exit(f"error: OM login response missing accessToken: {data}")
    return base_url, token


def api(method: str, path: str, token: str, *, body: Any = None, content_type: str | None = None) -> Any:
    """Call an OM REST endpoint. Returns parsed JSON or raises."""
    base_url, _tok = (os.environ["OPENMETADATA_BASE_URL"].rstrip("/"), token)
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
        detail = exc.read().decode(errors="replace")[:600]
        raise RuntimeError(f"{method} {path} → HTTP {exc.code}: {detail}") from exc
