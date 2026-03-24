#!/usr/bin/env python3
"""
Execute a Snowflake SQL file via the SQL REST API using a Programmatic Access Token (PAT).

Security:
  - Do not paste a PAT into chat or commit it. Set SNOWFLAKE_PAT in your environment or
    resolve it at runtime from a secret store (reference by name only).
  - This script does not log token values.

Required environment:
  SNOWFLAKE_ACCOUNT  Account identifier for the hostname
                     (see https://docs.snowflake.com/en/user-guide/admin-account-identifier )
  SNOWFLAKE_PAT      Programmatic access token for the Snowflake user that will run the SQL

Optional:
  SNOWFLAKE_WAREHOUSE   Default name for the dedicated warehouse created in the SQL file (FIVETRAN_DRIP_WH).
  SNOWFLAKE_SQL_API_EXECUTION_WAREHOUSE
                    Warehouse used only for this HTTP request — must already exist in the account
                    before the script runs (the SQL batch creates FIVETRAN_DRIP_WH; it cannot use it yet).
                    Defaults to COMPUTE_WH if unset; override if your account uses another default.
  SNOWFLAKE_SQL_API_HOST  Full API base URL (recommended). urllib3 lowercases hosts and can
                    break Snowflake TLS; when curl(1) is available, this script POSTs via curl
                    so the hostname keeps its case (e.g. https://ZNA09333-IOLAP_PARTNER.snowflakecomputing.com).
                    If unset, host is https://SNOWFLAKE_ACCOUNT.snowflakecomputing.com

The Snowflake user tied to the PAT must be allowed to execute the statements (e.g. role switches
to SECURITYADMIN / ACCOUNTADMIN as in scripts/snowflake_fivetran_drip_bronze_erp.sql).

Usage:
  python scripts/run_snowflake_sql_pat.py --sql-file scripts/snowflake_fivetran_drip_bronze_erp.sql
  python scripts/run_snowflake_sql_pat.py --render-from-env --sql-file scripts/snowflake_fivetran_drip_bronze_erp.sql
  python scripts/run_snowflake_sql_pat.py --dry-run
"""

from __future__ import annotations

import argparse
import json
import os
import shutil
import subprocess
import sys
from pathlib import Path

import requests

REPO_ROOT = Path(__file__).resolve().parents[1]


def _load_dotenv() -> None:
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


def _api_base() -> str:
    override = os.environ.get("SNOWFLAKE_SQL_API_HOST", "").strip()
    if override:
        return override.rstrip("/")
    account = os.environ["SNOWFLAKE_ACCOUNT"].strip()
    return f"https://{account}.snowflakecomputing.com"


def _post_sql_api(url: str, headers: dict[str, str], body: dict) -> tuple[int, dict]:
    """
    POST JSON. Prefer curl so the URL host is not lowercased (urllib3 breaks Snowflake TLS).
    """
    payload = json.dumps(body).encode("utf-8")
    curl = shutil.which("curl")
    if curl:
        cmd = [curl, "-sS", "-w", "\n%{http_code}", "-X", "POST", url]
        for key, val in headers.items():
            if key == "User-Agent":
                continue
            cmd.extend(["-H", f"{key}: {val}"])
        cmd.extend(["-d", "@-"])
        p = subprocess.run(
            cmd,
            input=payload,
            capture_output=True,
            timeout=400,
        )
        raw = p.stdout
        if p.stderr:
            sys.stderr.buffer.write(p.stderr)
        if not raw:
            return p.returncode or 599, {"message": "empty response from curl"}
        text, sep, code_b = raw.rpartition(b"\n")
        if sep and code_b.strip().isdigit():
            status = int(code_b.decode().strip())
            try:
                return status, json.loads(text.decode("utf-8"))
            except json.JSONDecodeError:
                return status, {"message": text.decode("utf-8", errors="replace")[:2000]}
        try:
            return 200, json.loads(raw.decode("utf-8"))
        except json.JSONDecodeError:
            return 599, {"message": raw.decode("utf-8", errors="replace")[:2000]}

    r = requests.post(url, headers=headers, json=body, timeout=400)
    try:
        data = r.json()
    except json.JSONDecodeError:
        data = {"message": r.text[:2000]}
    return r.status_code, data


def main() -> int:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument(
        "--sql-file",
        default=str(REPO_ROOT / "scripts/snowflake_fivetran_drip_bronze_erp.sql"),
        help="Path to SQL file to execute as one multi-statement request",
    )
    p.add_argument(
        "--dry-run",
        action="store_true",
        help="Print which env vars are set (not values) and exit",
    )
    p.add_argument(
        "--render-from-env",
        action="store_true",
        help=(
            "Treat --sql-file as a template with {{VAR}} placeholders; fill from repo .env "
            "(see scripts/snowflake_fivetran_template.py)"
        ),
    )
    p.add_argument(
        "--allow-empty-password",
        action="store_true",
        help="With --render-from-env, allow missing SNOWFLAKE_FIVETRAN_PASSWORD (debug only)",
    )
    args = p.parse_args()

    _load_dotenv()

    account = os.environ.get("SNOWFLAKE_ACCOUNT", "").strip()
    pat = os.environ.get("SNOWFLAKE_PAT", "").strip()
    warehouse = os.environ.get("SNOWFLAKE_WAREHOUSE", "FIVETRAN_DRIP_WH").strip()
    # Must be a warehouse that already exists (the DDL creates FIVETRAN_DRIP_WH; do not use that here).
    exec_wh = os.environ.get("SNOWFLAKE_SQL_API_EXECUTION_WAREHOUSE", "").strip() or "COMPUTE_WH"

    sql_host = os.environ.get("SNOWFLAKE_SQL_API_HOST", "").strip()

    if args.dry_run:
        print("Snowflake SQL API runner (dry run)")
        print(f"  SNOWFLAKE_ACCOUNT: {'set' if account else 'MISSING'}")
        print(f"  SNOWFLAKE_SQL_API_HOST: {'set' if sql_host else 'MISSING'}")
        print(f"  SNOWFLAKE_PAT: {'set' if pat else 'MISSING'}")
        print(f"  SNOWFLAKE_SQL_API_EXECUTION_WAREHOUSE (API session): {exec_wh!r}")
        print(f"  SNOWFLAKE_WAREHOUSE (DDL script / new WH name): {warehouse!r}")
        print(f"  sql file: {args.sql_file}")
        print(f"  render-from-env: {args.render_from_env}")
        try:
            base = _api_base()
        except KeyError:
            base = "(set SNOWFLAKE_SQL_API_HOST or SNOWFLAKE_ACCOUNT)"
        print(f"  API base: {base}")
        print(f"  HTTP client: {'curl' if shutil.which('curl') else 'requests (hosts may be lowercased)'}")
        ok = bool(pat) and bool(sql_host or account)
        return 0 if ok else 1

    if not pat:
        print("Set SNOWFLAKE_PAT in the environment (see --dry-run).", file=sys.stderr)
        return 1
    if not sql_host and not account:
        print(
            "Set SNOWFLAKE_SQL_API_HOST or SNOWFLAKE_ACCOUNT (see --dry-run).",
            file=sys.stderr,
        )
        return 1

    sql_path = Path(args.sql_file)
    if not sql_path.is_file():
        print(f"SQL file not found: {sql_path}", file=sys.stderr)
        return 1

    if args.render_from_env:
        sys.path.insert(0, str(REPO_ROOT / "scripts"))
        try:
            from snowflake_fivetran_template import render_template
        except ImportError as e:
            print(f"Import failed: {e}", file=sys.stderr)
            return 1
        try:
            sql_text = render_template(
                sql_path,
                require_password=not args.allow_empty_password,
            )
        except SystemExit as e:
            print(str(e), file=sys.stderr)
            return 1
    else:
        sql_text = sql_path.read_text(encoding="utf-8")

    url = f"{_api_base()}/api/v2/statements"
    headers = {
        "Authorization": f"Bearer {pat}",
        "X-Snowflake-Authorization-Token-Type": "PROGRAMMATIC_ACCESS_TOKEN",
        "Content-Type": "application/json",
        "Accept": "application/json",
        "User-Agent": "cursorskills-run-snowflake-sql-pat/1.0",
    }

    body: dict = {
        "statement": sql_text,
        "warehouse": exec_wh,
        "parameters": {"MULTI_STATEMENT_COUNT": "0"},
        "timeout": 300,
    }

    try:
        status, data = _post_sql_api(url, headers, body)
    except (OSError, subprocess.SubprocessError) as e:
        print(f"Request failed: {e}", file=sys.stderr)
        return 1

    if status >= 400:
        msg = data.get("message") or data.get("Message") or str(data)
        print(f"HTTP {status}: {msg}", file=sys.stderr)
        if isinstance(data, dict) and data.get("code"):
            print(f"Code: {data.get('code')}", file=sys.stderr)
        return 1

    handle = data.get("statementHandle")
    handles = data.get("statementHandles")
    print("OK")
    if handle:
        print(f"statementHandle: {handle}")
    if handles:
        print(f"statementHandles ({len(handles)}): first={handles[0] if handles else None}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
