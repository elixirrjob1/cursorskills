#!/usr/bin/env python3
"""
Delete Fivetran connections + destination, then recreate PostgreSQL destination and
SQL Server source with update_method TELEPORT.

Secrets: loads DATABASE_URL and AZURE_MSSQL_URL via scripts/keyvault_loader (Key Vault
secret names DATABASE-URL and AZURE-MSSQL-URL when KEYVAULT_NAME is set).

Fivetran API: FIVETRAN_API_KEY and FIVETRAN_API_SECRET from .env (not in Key Vault by default).

Usage:
  python scripts/fivetran_recreate_teleport.py --dry-run    # print plan only
  python scripts/fivetran_recreate_teleport.py              # execute

See docs/FIVETRAN_TELEPORT_RECREATE.md for post-run steps (schema reload, HISTORY, hashing).
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

from scripts.keyvault_loader import load_env  # noqa: E402

API_BASE = "https://api.fivetran.com/v1"
HEADERS = {
    "Accept": "application/json;version=2",
    "Content-Type": "application/json",
    "User-Agent": "cursorskills-fivetran-recreate",
}


def _fivetran_auth() -> HTTPBasicAuth:
    key = os.environ.get("FIVETRAN_API_KEY", "").strip()
    secret = os.environ.get("FIVETRAN_API_SECRET", "").strip()
    if not key or not secret:
        raise SystemExit(
            "Set FIVETRAN_API_KEY and FIVETRAN_API_SECRET in .env (Fivetran API credentials)."
        )
    return HTTPBasicAuth(key, secret)


def _request(
    method: str,
    endpoint: str,
    payload: dict[str, Any] | None = None,
    params: dict[str, Any] | None = None,
) -> dict[str, Any]:
    url = f"{API_BASE}/{endpoint}"
    auth = _fivetran_auth()
    if method == "GET":
        r = requests.get(url, headers=HEADERS, auth=auth, params=params, timeout=(10, 60))
    elif method == "POST":
        r = requests.post(url, headers=HEADERS, auth=auth, json=payload, timeout=(10, 120))
    elif method == "DELETE":
        r = requests.delete(url, headers=HEADERS, auth=auth, timeout=(10, 60))
    else:
        raise ValueError(method)
    r.raise_for_status()
    if r.status_code == 204 or not r.content:
        return {"code": "Success", "message": "OK"}
    try:
        return r.json()
    except ValueError:
        return {"code": "Success", "message": "OK"}


def _load_snapshot(path: Path) -> dict[str, Any]:
    with open(path, encoding="utf-8") as f:
        return json.load(f)


def _merge_pg_config(snapshot_dest: dict[str, Any], database_url: str) -> dict[str, Any]:
    u = make_url(database_url)
    port = u.port or 5432
    cfg = dict(snapshot_dest["config"])
    cfg["host"] = u.host
    cfg["port"] = str(port)
    cfg["database"] = u.database or cfg.get("database", "postgres")
    cfg["user"] = u.username or cfg.get("user")
    cfg["password"] = u.password or ""
    return cfg


def _merge_sql_config(snapshot_sql: dict[str, Any], mssql_url: str) -> dict[str, Any]:
    u = make_url(mssql_url)
    port = int(u.port or 1433)
    cfg = dict(snapshot_sql["config"])
    cfg["host"] = u.host
    cfg["port"] = port
    cfg["database"] = u.database or cfg.get("database")
    cfg["user"] = u.username or cfg.get("user")
    cfg["password"] = u.password or ""
    cfg["update_method"] = "TELEPORT"
    return cfg


def main() -> None:
    parser = argparse.ArgumentParser(description="Recreate Fivetran destination + SQL Server TELEPORT")
    parser.add_argument(
        "--snapshot",
        type=Path,
        default=ROOT / "references" / "fivetran" / "reconnect_teleport_snapshot.json",
        help="Path to reconnect JSON template (no secrets)",
    )
    parser.add_argument("--dry-run", action="store_true", help="Print actions only")
    args = parser.parse_args()

    try:
        from dotenv import load_dotenv
        load_dotenv(ROOT / ".env")
    except ImportError:
        pass

    load_env()

    snapshot = _load_snapshot(args.snapshot)
    group_id = snapshot["group_id"]
    legacy_dest = snapshot["legacy_destination_id"]

    print("=== Fivetran TELEPORT recreate ===\n")
    print(f"Group: {group_id}")
    print(f"Snapshot: {args.snapshot}")

    if args.dry_run:
        print("\n[DRY RUN] Would DELETE all connections for group, then destination, then POST destination + POST sql_server (TELEPORT).")
        print(f"Legacy destination id: {legacy_dest}")
        print("Requires DATABASE_URL + AZURE_MSSQL_URL (Key Vault DATABASE-URL, AZURE-MSSQL-URL) for live run.")
        return

    database_url = os.environ.get("DATABASE_URL", "").strip()
    mssql_url = os.environ.get("AZURE_MSSQL_URL", "").strip()
    if not mssql_url.startswith("mssql"):
        mssql_url = f"mssql+pyodbc://{mssql_url}"
    if not database_url:
        raise SystemExit("DATABASE_URL not set (Key Vault DATABASE-URL or .env).")
    if not mssql_url:
        raise SystemExit("AZURE_MSSQL_URL not set (Key Vault AZURE-MSSQL-URL or .env).")

    dest_payload = snapshot["destination"]
    pg_cfg = _merge_pg_config(dest_payload, database_url)

    sql_snap = snapshot["sql_server_connector"]
    sql_cfg = _merge_sql_config(sql_snap, mssql_url)

    resp = _request("GET", "connections", params={"group_id": group_id})
    data = resp.get("data") or {}
    items = data.get("items", []) if isinstance(data, dict) else []
    if not items and isinstance(data, list):
        items = data
    conn_ids = [c.get("id") for c in items if c.get("id")]
    print(f"Connections to delete: {conn_ids}")
    for cid in conn_ids:
        print(f"  DELETE connection {cid}...")
        _request("DELETE", f"connections/{cid}")

    print(f"DELETE destination {legacy_dest}...")
    _request("DELETE", f"destinations/{legacy_dest}")

    d_body = {
        "group_id": group_id,
        "service": dest_payload["service"],
        "region": dest_payload["region"],
        "time_zone_offset": dest_payload["time_zone_offset"],
        "config": pg_cfg,
        "trust_certificates": True,
        "trust_fingerprints": True,
        "run_setup_tests": dest_payload.get("run_setup_tests", True),
        "networking_method": dest_payload.get("networking_method", "Directly"),
    }
    print("POST destination...")
    d_resp = _request("POST", "destinations", d_body)
    new_dest_id = (d_resp.get("data") or {}).get("id") or d_resp.get("id")
    print(f"  New destination id: {new_dest_id}")

    c_body: dict[str, Any] = {
        "group_id": group_id,
        "service": sql_snap["service"],
        "paused": sql_snap.get("paused", False),
        "sync_frequency": sql_snap.get("sync_frequency", 1440),
        "trust_certificates": True,
        "trust_fingerprints": True,
        "run_setup_tests": True,
        "config": sql_cfg,
    }
    print("POST connection (sql_server, TELEPORT)...")
    c_resp = _request("POST", "connections", c_body)
    new_conn_id = (c_resp.get("data") or {}).get("id") or c_resp.get("id")
    print(f"  New connection id: {new_conn_id}")
    print("\nDone. Update docs/scripts that referenced old connector ids.")
    print("Next: see docs/FIVETRAN_TELEPORT_RECREATE.md")


if __name__ == "__main__":
    main()
