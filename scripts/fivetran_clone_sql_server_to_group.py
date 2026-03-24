#!/usr/bin/env python3
"""
Clone an existing Fivetran sql_server connection into another group (same source, new destination).

The Fivetran API does not return the source DB password (masked). Password is taken from:
  - AZURE_MSSQL_URL (mssql+pyodbc://...) in .env / Key Vault (same pattern as fivetran_recreate_teleport.py), or
  - FIVETRAN_SQL_SERVER_PASSWORD (literal password for the source SQL user).

Does not copy config.public_key — a new key is issued; follow Fivetran SQL Server / Teleport docs if the source must trust the new key.

Usage:
  python scripts/fivetran_clone_sql_server_to_group.py --dry-run
  python scripts/fivetran_clone_sql_server_to_group.py \\
    --source-id paired_outnumbered --target-group-id picture_heav \\
    --schema-prefix bronze_erp_
"""
from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path
from typing import Any

import requests
from requests.auth import HTTPBasicAuth
from sqlalchemy.engine.url import make_url

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from scripts._envfile import load_env_file  # noqa: E402

try:
    from scripts.keyvault_loader import load_env as load_kv_env
except ImportError:
    load_kv_env = None  # type: ignore[misc, assignment]

API_BASE = "https://api.fivetran.com/v1"
HEADERS = {
    "Accept": "application/json;version=2",
    "Content-Type": "application/json",
    "User-Agent": "cursorskills-fivetran-clone-sql-server",
}


def _auth() -> HTTPBasicAuth:
    key = os.environ.get("FIVETRAN_API_KEY", "").strip()
    secret = os.environ.get("FIVETRAN_API_SECRET", "").strip()
    if not key or not secret:
        raise SystemExit("Set FIVETRAN_API_KEY and FIVETRAN_API_SECRET.")
    return HTTPBasicAuth(key, secret)


def _request(method: str, endpoint: str, payload: dict[str, Any] | None = None) -> dict[str, Any]:
    url = f"{API_BASE}/{endpoint}"
    auth = _auth()
    if method == "GET":
        r = requests.get(url, headers=HEADERS, auth=auth, timeout=(10, 120))
    elif method == "POST":
        r = requests.post(url, headers=HEADERS, auth=auth, json=payload, timeout=(10, 120))
    else:
        raise ValueError(method)
    r.raise_for_status()
    if r.status_code == 204 or not r.content:
        return {}
    return r.json()


def _source_password() -> str:
    explicit = os.environ.get("FIVETRAN_SQL_SERVER_PASSWORD", "").strip()
    if explicit:
        return explicit
    raw = os.environ.get("AZURE_MSSQL_URL", "").strip()
    if not raw:
        raise SystemExit(
            "Set FIVETRAN_SQL_SERVER_PASSWORD or AZURE_MSSQL_URL (same DB user as the source connector)."
        )
    if not raw.startswith("mssql"):
        raw = f"mssql+pyodbc://{raw}"
    u = make_url(raw)
    pw = u.password or ""
    if not pw:
        raise SystemExit("AZURE_MSSQL_URL has no password segment.")
    return pw


def _build_config_from_source(
    src_cfg: dict[str, Any],
    schema_prefix: str,
    password: str,
) -> dict[str, Any]:
    skip = frozenset({"public_key", "password"})
    out: dict[str, Any] = {}
    for k, v in src_cfg.items():
        if k in skip:
            continue
        out[k] = v
    out["schema_prefix"] = schema_prefix
    out["password"] = password
    return out


def main() -> None:
    p = argparse.ArgumentParser(description="Clone Fivetran sql_server connection to another group")
    p.add_argument("--source-id", default="paired_outnumbered", help="Existing connection id to copy from")
    p.add_argument("--target-group-id", default="picture_heav", help="Group id that already has the Snowflake destination")
    p.add_argument(
        "--schema-prefix",
        default="bronze_erp_",
        help="Destination schema prefix in config (per Fivetran; often ends with _)",
    )
    p.add_argument("--paused", action="store_true", help="Create paused (default: False, same as source often)")
    p.add_argument("--sync-frequency", type=int, default=1440)
    p.add_argument("--dry-run", action="store_true")
    args = p.parse_args()

    load_env_file(ROOT / ".env")
    if load_kv_env:
        load_kv_env()

    resp = _request("GET", f"connections/{args.source_id}")
    src = resp.get("data") or {}
    if src.get("service") != "sql_server":
        raise SystemExit(f"Source is not sql_server: {src.get('service')!r}")

    src_cfg = dict(src.get("config") or {})
    password = _source_password()
    new_cfg = _build_config_from_source(src_cfg, args.schema_prefix, password)

    body: dict[str, Any] = {
        "group_id": args.target_group_id,
        "service": "sql_server",
        "paused": args.paused,
        "sync_frequency": args.sync_frequency,
        "trust_certificates": True,
        "trust_fingerprints": True,
        "run_setup_tests": True,
        "config": new_cfg,
    }
    if src.get("service_version") is not None:
        body["service_version"] = src["service_version"]
    if src.get("networking_method"):
        body["networking_method"] = src["networking_method"]

    if args.dry_run:
        safe_cfg = {k: ("***" if k == "password" else v) for k, v in new_cfg.items()}
        print(json.dumps({**body, "config": safe_cfg}, indent=2))
        print("\nDry run: no POST. Omit --dry-run to create.")
        return

    out = _request("POST", "connections", body)
    data = out.get("data") or {}
    print("Created connection:", data.get("id"))
    print("setup_state:", (data.get("status") or {}).get("setup_state"))


if __name__ == "__main__":
    main()
