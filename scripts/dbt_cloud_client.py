"""
Shared dbt Cloud Administrative API v2 client (token + GET).

Used by fetch_dbt_logs.py and sync_dbt_observability_to_snowflake.py.
"""

from __future__ import annotations

import base64
import json
import os
import re
import sys
import time
from pathlib import Path

try:
    import requests
except ImportError as e:  # pragma: no cover
    raise SystemExit("requests is required: pip install requests") from e


def load_env_from_repo_root() -> None:
    env_path = Path(__file__).resolve().parent.parent / ".env"
    if env_path.exists():
        for line in env_path.read_text(encoding="utf-8").splitlines():
            line = line.strip()
            if line and not line.startswith("#") and "=" in line:
                k, _, v = line.partition("=")
                os.environ.setdefault(k.strip(), v.strip())


def _jwt_expiry_unix(jwt: str) -> int | None:
    """Read JWT `exp` claim without verifying signature (local expiry check only)."""
    try:
        parts = jwt.split(".")
        if len(parts) < 2:
            return None
        payload = parts[1]
        pad = (-len(payload)) % 4
        if pad:
            payload += "=" * pad
        data = json.loads(base64.urlsafe_b64decode(payload.encode("ascii")))
        exp = data.get("exp")
        return int(exp) if exp is not None else None
    except Exception:
        return None


def _embedded_mcp_access_token(raw: str) -> str | None:
    """
    dbt-mcp writes `decoded_access_token.access_token_response.access_token` into mcp.yml.
    Same bearer token the Cursor dbt MCP server uses while it is still valid.
    """
    m = re.search(
        r"(?m)^\s+access_token:\s+(eyJ[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+)\s*$",
        raw,
    )
    if not m:
        return None
    jwt = m.group(1).strip()
    exp = _jwt_expiry_unix(jwt)
    if exp is None:
        return jwt
    if exp <= int(time.time()) + 120:
        return None
    return jwt


def get_access_token(repo_root: Path | None = None) -> str:
    """
    Auth order (aligned with dbt MCP):
    1) Valid embedded access_token in mcp.yml (OAuth cache written by dbt-mcp)
    2) OAuth refresh via refresh_token in mcp.yml
    3) DBT_PAT from env
    """
    root = repo_root or Path(__file__).resolve().parent.parent
    # Support two layouts:
    #   monorepo  (cursorskills): scripts/ sits two levels above dbt_project/drip_transformations/
    #   base repo (dbtproject):   scripts/ sits at repo root alongside mcp.yml
    mcp_yml = next(
        (p for p in [
            root / "dbt_project/drip_transformations/mcp.yml",
            root / "mcp.yml",
        ] if p.exists()),
        root / "dbt_project/drip_transformations/mcp.yml",  # fallback (may not exist)
    )
    if mcp_yml.exists():
        try:
            raw = mcp_yml.read_text(encoding="utf-8")
            embedded = _embedded_mcp_access_token(raw)
            if embedded:
                print("Auth: using access_token from mcp.yml (dbt MCP OAuth cache)", file=sys.stderr)
                return embedded

            refresh = re.search(r"refresh_token:\s*(\S+)", raw)
            client_id = re.search(r"client_id:\s*(\S+)", raw)
            host_pfx = re.search(r"host_prefix:\s*(\S+)", raw)
            if refresh and client_id and host_pfx:
                r = requests.post(
                    f"https://{host_pfx.group(1)}.us1.dbt.com/oauth/token",
                    data={
                        "grant_type": "refresh_token",
                        "refresh_token": refresh.group(1),
                        "client_id": client_id.group(1),
                    },
                    timeout=15,
                )
                if r.status_code == 200:
                    tok = r.json().get("access_token", "")
                    if tok:
                        print("Auth: refreshed OAuth token from mcp.yml", file=sys.stderr)
                        return str(tok)
                if r.status_code == 400:
                    print(
                        "Auth: mcp.yml refresh token expired — run: uvx dbt-mcp auth",
                        file=sys.stderr,
                    )
        except OSError as e:
            print(f"Auth: mcp.yml read failed ({e})", file=sys.stderr)

    pat = os.environ.get("DBT_PAT", "").strip()
    if not pat:
        print("ERROR: No valid dbt auth. Set DBT_PAT or refresh mcp.yml (uvx dbt-mcp auth).", file=sys.stderr)
        sys.exit(1)
    print("Auth: using DBT_PAT from .env", file=sys.stderr)
    return pat


_session: dict[str, object] = {}


def api_base() -> tuple[str, dict[str, str], str]:
    """Initialize HTTP session (cached)."""
    if _session.get("base"):
        return _session["base"], _session["headers"], _session["account_id"]  # type: ignore[return-value]

    load_env_from_repo_root()
    account_id = os.environ.get("DBT_ACCOUNT_ID", "").strip()
    host = os.environ.get("DBT_HOST", "").strip()
    if not account_id or not host:
        print("ERROR: Set DBT_ACCOUNT_ID and DBT_HOST in .env", file=sys.stderr)
        sys.exit(1)
    token = get_access_token()
    base = f"https://{host}/api/v2/accounts/{account_id}"
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
    _session["base"] = base
    _session["headers"] = headers
    _session["account_id"] = account_id
    return base, headers, account_id


def api_get(path: str, params: dict | list | None = None, timeout: int = 120) -> dict:
    base, headers, _ = api_base()
    path = path if path.startswith("/") else f"/{path}"
    r = requests.get(f"{base}{path}", headers=headers, params=params, timeout=timeout)
    r.raise_for_status()
    return r.json()


def api_get_run_with_steps(run_id: int) -> dict:
    """Single run including run_steps (dbt expects include_related JSON array)."""
    base, headers, _ = api_base()
    path = f"{base}/runs/{run_id}/"
    ir = json.dumps(["run_steps"])
    r = requests.get(path, headers=headers, params={"include_related": ir}, timeout=120)
    r.raise_for_status()
    return r.json()


def api_get_step_with_logs(step_id: int) -> dict:
    base, headers, _ = api_base()
    path = f"{base}/steps/{step_id}/"
    ir = json.dumps(["debug_logs"])
    r = requests.get(path, headers=headers, params={"include_related": ir}, timeout=120)
    r.raise_for_status()
    return r.json()
