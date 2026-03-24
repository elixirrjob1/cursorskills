#!/usr/bin/env python3
"""
Create a Fivetran Snowflake destination via REST API.

Uses the same config shape as the Fivetran Terraform provider: `config.auth` must be the
string `PASSWORD` or `KEY_PAIR` (not a nested object). See:
https://registry.terraform.io/providers/fivetran/fivetran/latest/docs/resources/destination

Secrets: load from repo `.env` (never print secret values).

Required:
  FIVETRAN_API_KEY, FIVETRAN_API_SECRET
  SNOWFLAKE_FIVETRAN_PASSWORD   Password for the Snowflake user (e.g. FIVETRAN_DRIP_USER)

Common optional (defaults shown):
  FIVETRAN_GROUP_ID             default: picture_heav
  FIVETRAN_REGION               default: AWS_US_WEST_2
  FIVETRAN_TIME_ZONE_OFFSET       default: -8
  SNOWFLAKE_HOST                default: ${SNOWFLAKE_ACCOUNT}.snowflakecomputing.com if SNOWFLAKE_ACCOUNT set
  SNOWFLAKE_FIVETRAN_USER         default: FIVETRAN_DRIP_USER
  SNOWFLAKE_FIVETRAN_DATABASE     default: DRIP_DATA_INTELLIGENCE
  SNOWFLAKE_FIVETRAN_WAREHOUSE    default: FIVETRAN_DRIP_WH  (included in API payload)
  SNOWFLAKE_FIVETRAN_ROLE         default: FIVETRAN_DRIP_ROLE

Usage:
  python scripts/fivetran_create_snowflake_destination.py --dry-run
  python scripts/fivetran_create_snowflake_destination.py
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path

import requests
from requests.auth import HTTPBasicAuth

REPO_ROOT = Path(__file__).resolve().parents[1]

API_BASE = "https://api.fivetran.com/v1"
HEADERS = {
    "Accept": "application/json;version=2",
    "Content-Type": "application/json",
    "User-Agent": "cursorskills-fivetran-snowflake-destination",
}


def _load_env() -> None:
    try:
        from dotenv import load_dotenv

        load_dotenv(REPO_ROOT / ".env")
    except ImportError:
        pass
    try:
        from _envfile import load_env_file

        load_env_file(REPO_ROOT / ".env")
    except ImportError:
        pass


def _auth() -> HTTPBasicAuth:
    key = os.environ.get("FIVETRAN_API_KEY", "").strip()
    secret = os.environ.get("FIVETRAN_API_SECRET", "").strip()
    if not key or not secret:
        raise SystemExit("Set FIVETRAN_API_KEY and FIVETRAN_API_SECRET.")
    return HTTPBasicAuth(key, secret)


def _host() -> str:
    h = os.environ.get("SNOWFLAKE_HOST", "").strip()
    if h:
        return h
    acct = os.environ.get("SNOWFLAKE_ACCOUNT", "").strip()
    if not acct:
        raise SystemExit(
            "Set SNOWFLAKE_HOST (full host, no https) or SNOWFLAKE_ACCOUNT "
            "(e.g. ZNA09333-IOLAP_PARTNER for ZNA09333-IOLAP_PARTNER.snowflakecomputing.com)."
        )
    return f"{acct}.snowflakecomputing.com"


def main() -> int:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--dry-run", action="store_true")
    args = p.parse_args()

    _load_env()

    pw = os.environ.get("SNOWFLAKE_FIVETRAN_PASSWORD", "").strip()
    group_id = os.environ.get("FIVETRAN_GROUP_ID", "picture_heav").strip()
    region = os.environ.get("FIVETRAN_REGION", "AWS_US_WEST_2").strip()
    tz = os.environ.get("FIVETRAN_TIME_ZONE_OFFSET", "-8").strip()
    user = os.environ.get("SNOWFLAKE_FIVETRAN_USER", "FIVETRAN_DRIP_USER").strip()
    database = os.environ.get("SNOWFLAKE_FIVETRAN_DATABASE", "DRIP_DATA_INTELLIGENCE").strip()
    warehouse = os.environ.get("SNOWFLAKE_FIVETRAN_WAREHOUSE", "FIVETRAN_DRIP_WH").strip()
    role = os.environ.get("SNOWFLAKE_FIVETRAN_ROLE", "FIVETRAN_DRIP_ROLE").strip()

    if args.dry_run:
        print("Fivetran Snowflake destination (dry run)")
        print(f"  FIVETRAN_GROUP_ID: {group_id!r}")
        print(f"  FIVETRAN_REGION: {region!r}")
        print(f"  SNOWFLAKE_FIVETRAN_PASSWORD: {'set' if pw else 'MISSING'}")
        try:
            hp = _host()
        except SystemExit:
            hp = "(set SNOWFLAKE_HOST or SNOWFLAKE_ACCOUNT)"
        print(f"  resolved host: {hp!r}")
        print(f"  user/database/warehouse/role: {user} / {database} / {warehouse} / {role}")
        return 0 if pw else 1

    if not pw:
        print("Set SNOWFLAKE_FIVETRAN_PASSWORD in the environment.", file=sys.stderr)
        return 1

    host = _host()
    payload = {
        "group_id": group_id,
        "service": "snowflake",
        "region": region,
        "time_zone_offset": tz,
        "config": {
            "host": host,
            "port": 443,
            "user": user,
            "password": pw,
            "database": database,
            "warehouse": warehouse,
            "role": role,
            "auth": "PASSWORD",
        },
        "trust_certificates": True,
        "trust_fingerprints": True,
        "run_setup_tests": True,
        "networking_method": "Directly",
        "daylight_saving_time_enabled": True,
    }

    try:
        r = requests.post(
            f"{API_BASE}/destinations",
            headers=HEADERS,
            auth=_auth(),
            json=payload,
            timeout=120,
        )
    except requests.RequestException as e:
        print(f"Request failed: {e}", file=sys.stderr)
        return 1

    try:
        data = r.json()
    except json.JSONDecodeError:
        print(r.text[:4000], file=sys.stderr)
        return 1

    if r.status_code >= 400:
        print(json.dumps(data, indent=2)[:4000], file=sys.stderr)
        return 1

    if data.get("code") != "Success":
        print(json.dumps(data, indent=2)[:4000], file=sys.stderr)
        return 1

    dest_id = (data.get("data") or {}).get("id")
    print("Destination created.")
    if dest_id:
        print(f"destination_id: {dest_id}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
