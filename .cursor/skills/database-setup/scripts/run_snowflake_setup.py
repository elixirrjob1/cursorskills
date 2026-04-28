#!/usr/bin/env python3
"""Validate and run Snowflake setup for Fivetran using workspace scripts."""

from __future__ import annotations

import argparse
import os
import subprocess
import sys
from pathlib import Path


def _workspace_root() -> Path:
    return Path.cwd()


def _workspace_script(*parts: str) -> Path:
    return _workspace_root().joinpath(*parts)


def _load_workspace_env() -> None:
    scripts_dir = _workspace_script("scripts")
    if str(scripts_dir) not in sys.path:
        sys.path.insert(0, str(scripts_dir))

    env_path = _workspace_script(".env")
    try:
        from _envfile import load_env_file

        load_env_file(env_path)
    except ImportError:
        pass

    try:
        from keyvault_loader import load_env

        load_env()
    except ImportError:
        pass


def _missing_required_env() -> list[str]:
    required = [
        "SNOWFLAKE_PAT",
        "SNOWFLAKE_SQL_API_EXECUTION_WAREHOUSE",
        "SNOWFLAKE_FIVETRAN_PASSWORD",
        "SNOWFLAKE_FIVETRAN_USER",
        "SNOWFLAKE_FIVETRAN_ROLE",
        "SNOWFLAKE_FIVETRAN_WAREHOUSE",
        "SNOWFLAKE_DRIP_DATABASE",
        "SNOWFLAKE_BRONZE_SCHEMA",
    ]
    missing = [key for key in required if not os.environ.get(key, "").strip()]
    if not (
        os.environ.get("SNOWFLAKE_SQL_API_HOST", "").strip()
        or os.environ.get("SNOWFLAKE_ACCOUNT", "").strip()
    ):
        missing.append("SNOWFLAKE_SQL_API_HOST or SNOWFLAKE_ACCOUNT")
    return missing


def _resolved_host() -> str:
    host = os.environ.get("SNOWFLAKE_HOST", "").strip()
    if host:
        return host
    api_host = os.environ.get("SNOWFLAKE_SQL_API_HOST", "").strip()
    if api_host:
        return api_host.removeprefix("https://").removeprefix("http://").rstrip("/")
    account = os.environ.get("SNOWFLAKE_ACCOUNT", "").strip()
    if account:
        return f"{account}.snowflakecomputing.com"
    return ""


def _print_handoff() -> None:
    print("Snowflake setup values for downstream Fivetran destination:")
    print(f"  host: {_resolved_host() or 'MISSING'}")
    print(f"  user: {os.environ.get('SNOWFLAKE_FIVETRAN_USER', '').strip() or 'MISSING'}")
    print("  password_source: SNOWFLAKE_FIVETRAN_PASSWORD")
    print(f"  role: {os.environ.get('SNOWFLAKE_FIVETRAN_ROLE', '').strip() or 'MISSING'}")
    print(
        "  warehouse: "
        f"{os.environ.get('SNOWFLAKE_FIVETRAN_WAREHOUSE', '').strip() or 'MISSING'}"
    )
    print(f"  database: {os.environ.get('SNOWFLAKE_DRIP_DATABASE', '').strip() or 'MISSING'}")
    print(f"  schema: {os.environ.get('SNOWFLAKE_BRONZE_SCHEMA', '').strip() or 'MISSING'}")
    print("  auth_for_fivetran: PASSWORD")
    print("  provisioning_auth: PROGRAMMATIC_ACCESS_TOKEN")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--check-only", action="store_true", help="Validate env and print handoff only")
    args = parser.parse_args()

    runner = _workspace_script("scripts", "snowflake_setup", "run_snowflake_sql_pat.py")
    sql_file = _workspace_script(
        "scripts", "snowflake_setup", "snowflake_fivetran_drip_bronze_erp.sql"
    )

    if not runner.is_file() or not sql_file.is_file():
        print(
            "Run this skill from the workspace root that contains scripts/snowflake_setup.",
            file=sys.stderr,
        )
        return 1

    _load_workspace_env()
    missing = _missing_required_env()
    if missing:
        print(
            "Missing required Snowflake setup values: " + ", ".join(missing),
            file=sys.stderr,
        )
        print("Set them in .env or in Azure Key Vault before rerunning.", file=sys.stderr)
        return 1

    if args.check_only:
        _print_handoff()
        return 0

    cmd = [
        sys.executable,
        str(runner),
        "--render-from-env",
        "--sql-file",
        str(sql_file),
    ]
    completed = subprocess.run(cmd, cwd=_workspace_root())
    if completed.returncode != 0:
        return completed.returncode

    _print_handoff()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
