#!/usr/bin/env python3
"""
Natural Language Data Query Engine — Snowflake backend
========================================================
Two subcommands:

  resolve  -- Query Snowflake INFORMATION_SCHEMA to return the exact
               case-sensitive names of tables and columns.
               OpenMetadata normalises all identifiers to UPPERCASE on
               ingestion; this command retrieves ground-truth casing so
               generated SQL won't fail with "invalid identifier" errors.

  execute  -- Run a SQL query on Snowflake and return results as JSON.

OpenMetadata metadata discovery is handled by the OpenMetadata native MCP
server (configured in %USERPROFILE%/.cursor/mcp.json). This script only
talks to Snowflake.

Credential resolution order (highest priority first):
  1. --secrets-provider + --secrets-url  → cloud secrets manager (production)
  2. .env file (auto-loaded)             → development — gitignored, single source of truth
  3. CLI args                             → optional override for one-off runs
"""

import argparse
import json
import os
import re
import shutil
import subprocess
import sys
import time

import requests


def _post_json(url: str, headers: dict, payload: dict, timeout: int = 30) -> tuple[int, dict]:
    """POST JSON — prefer curl to preserve hostname casing for PrivateLink accounts."""
    curl = shutil.which("curl")
    if curl:
        header_args = []
        for k, v in headers.items():
            header_args += ["-H", f"{k}: {v}"]
        cmd = [curl, "-sS", "-w", "\n%{http_code}", "-X", "POST", url] + header_args + \
              ["-d", json.dumps(payload), "--max-time", str(timeout)]
        p = subprocess.run(cmd, capture_output=True, text=True)
        lines = p.stdout.strip().splitlines()
        status = int(lines[-1]) if lines else 599
        body = "\n".join(lines[:-1])
        return status, json.loads(body) if body else {}
    r = requests.post(url, headers=headers, json=payload, timeout=timeout)
    return r.status_code, r.json() if r.text else {}


def _get_json(url: str, headers: dict, timeout: int = 15) -> tuple[int, dict]:
    """GET JSON — prefer curl."""
    curl = shutil.which("curl")
    if curl:
        header_args = []
        for k, v in headers.items():
            header_args += ["-H", f"{k}: {v}"]
        cmd = [curl, "-sS", "-w", "\n%{http_code}", url] + header_args + \
              ["--max-time", str(timeout)]
        p = subprocess.run(cmd, capture_output=True, text=True)
        lines = p.stdout.strip().splitlines()
        status = int(lines[-1]) if lines else 599
        body = "\n".join(lines[:-1])
        return status, json.loads(body) if body else {}
    r = requests.get(url, headers=headers, timeout=timeout)
    return r.status_code, r.json() if r.text else {}

# Auto-load .env from the workspace root (walks up from cwd)
try:
    from dotenv import load_dotenv, find_dotenv
    _dotenv_path = find_dotenv(usecwd=True)
    if _dotenv_path:
        load_dotenv(_dotenv_path, override=False)
except ImportError:
    pass

try:
    import snowflake.connector

    SNOWFLAKE_AVAILABLE = True
except ImportError:
    SNOWFLAKE_AVAILABLE = False

try:
    from azure.identity import DefaultAzureCredential
    from azure.keyvault.secrets import SecretClient

    AZURE_KEYVAULT_AVAILABLE = True
except ImportError:
    AZURE_KEYVAULT_AVAILABLE = False

try:
    import boto3

    AWS_AVAILABLE = True
except ImportError:
    AWS_AVAILABLE = False

try:
    import hvac

    VAULT_AVAILABLE = True
except ImportError:
    VAULT_AVAILABLE = False


# ---------------------------------------------------------------------------
# Credential key map — Snowflake only
# ---------------------------------------------------------------------------

CREDENTIAL_MAP = {
    # Snowflake — OAuth Bearer token (Snowflake PAT, works with REST API)
    "sf_bearer_token": ("SNOWFLAKE_BEARER_TOKEN",   "snowflake-bearer-token"),
    # Snowflake — legacy PAT (Snowflake Token= header format)
    "sf_pat":        ("SNOWFLAKE_PAT",              "snowflake-pat"),
    "sf_api_host":   ("SNOWFLAKE_SQL_API_HOST",     "snowflake-sql-api-host"),
    # Snowflake — classic connector (fallback)
    "sf_account":    ("SNOWFLAKE_ACCOUNT",          "snowflake-account"),
    "sf_user":       ("SNOWFLAKE_USERNAME",         "snowflake-username"),
    "sf_password":   ("SNOWFLAKE_PASSWORD",         "snowflake-password"),
    "sf_warehouse":  ("SNOWFLAKE_WAREHOUSE",        "snowflake-warehouse"),
    "sf_database":   ("SNOWFLAKE_DATABASE",         "snowflake-database"),
    "sf_schema":     ("SNOWFLAKE_SCHEMA",           "snowflake-schema"),
    "sf_role":       ("SNOWFLAKE_ROLE",             "snowflake-role"),
}


# ---------------------------------------------------------------------------
# Cloud-agnostic secrets providers
# ---------------------------------------------------------------------------

def _get_from_azure_keyvault(vault_url: str) -> dict:
    if not AZURE_KEYVAULT_AVAILABLE:
        raise RuntimeError(
            "azure-keyvault-secrets and azure-identity are required.\n"
            "Run: pip install azure-keyvault-secrets azure-identity"
        )
    credential = DefaultAzureCredential()
    client = SecretClient(vault_url=vault_url, credential=credential)
    result = {}
    for key, (_, secret_name) in CREDENTIAL_MAP.items():
        try:
            result[key] = client.get_secret(secret_name).value or ""
        except Exception:
            result[key] = ""
    return result


def _get_from_aws_secrets_manager(region: str, secret_name: str) -> dict:
    if not AWS_AVAILABLE:
        raise RuntimeError(
            "boto3 is required for AWS Secrets Manager.\n"
            "Run: pip install boto3"
        )
    client = boto3.client("secretsmanager", region_name=region)
    response = client.get_secret_value(SecretId=secret_name)
    secret_dict = json.loads(response.get("SecretString", "{}"))
    result = {}
    for key, (_, sm_key) in CREDENTIAL_MAP.items():
        result[key] = secret_dict.get(sm_key, "")
    return result


def _get_from_hashicorp_vault(vault_addr: str, vault_token: str, secret_path: str) -> dict:
    if not VAULT_AVAILABLE:
        raise RuntimeError(
            "hvac is required for HashiCorp Vault.\n"
            "Run: pip install hvac"
        )
    client = hvac.Client(url=vault_addr, token=vault_token)
    if "/data/" in secret_path:
        mount, _, path = secret_path.partition("/data/")
        resp = client.secrets.kv.v2.read_secret_version(path=path, mount_point=mount)
        secret_dict = resp["data"]["data"]
    else:
        resp = client.read(secret_path)
        secret_dict = resp["data"] if resp else {}
    result = {}
    for key, (_, sm_key) in CREDENTIAL_MAP.items():
        result[key] = secret_dict.get(sm_key, "")
    return result


def _get_from_env() -> dict:
    result = {key: "" for key in CREDENTIAL_MAP}
    for key, (env_name, _) in CREDENTIAL_MAP.items():
        val = os.environ.get(env_name, "")
        if val:
            result[key] = val
    return result


def resolve_credentials(args) -> dict:
    """Return a flat credentials dict. Priority: secrets-provider > .env/env vars > CLI args."""
    provider = getattr(args, "secrets_provider", None)
    secrets_url = getattr(args, "secrets_url", None)

    if provider and secrets_url:
        if provider == "azure-keyvault":
            creds = _get_from_azure_keyvault(secrets_url)
        elif provider == "aws-secrets-manager":
            region = getattr(args, "secrets_region", None) or "us-east-1"
            secret_name = getattr(args, "secrets_name", None) or "businesschat/creds"
            creds = _get_from_aws_secrets_manager(region, secret_name)
        elif provider == "hashicorp-vault":
            vault_token = getattr(args, "vault_token", None) or os.environ.get("VAULT_TOKEN", "")
            secret_path = getattr(args, "secrets_path", None) or "secret/data/businesschat/creds"
            creds = _get_from_hashicorp_vault(secrets_url, vault_token, secret_path)
        else:
            raise ValueError(f"Unknown secrets provider: {provider!r}. "
                             "Use: azure-keyvault, aws-secrets-manager, or hashicorp-vault")
        return creds

    creds = _get_from_env()
    for key in CREDENTIAL_MAP:
        cli_value = getattr(args, key, None)
        if cli_value:
            creds[key] = cli_value
    return creds


# ---------------------------------------------------------------------------
# Snowflake helpers
# ---------------------------------------------------------------------------

def _normalise_sf_account(account: str) -> str:
    return re.sub(r"\.snowflakecomputing\.com$", "", account, flags=re.IGNORECASE)


def _sf_api_base(creds: dict) -> str:
    api_host = creds.get("sf_api_host", "").strip()
    if api_host:
        api_host = re.sub(r"^https?://", "", api_host).rstrip("/")
        return f"https://{api_host}"
    account = _normalise_sf_account(creds.get("sf_account", ""))
    if account:
        return f"https://{account}.snowflakecomputing.com"
    return ""


def execute_snowflake_query_pat(creds: dict, sql: str) -> dict:
    """Execute SQL via the Snowflake SQL REST API using a PAT."""
    base = _sf_api_base(creds)
    if not base:
        raise RuntimeError("Missing Snowflake host: set SNOWFLAKE_SQL_API_HOST or SNOWFLAKE_ACCOUNT")

    pat = creds.get("sf_pat", "")
    if not pat:
        raise RuntimeError("Missing SNOWFLAKE_PAT for REST API authentication")

    headers = {
        "Content-Type":  "application/json",
        "Accept":        "application/json",
        "Authorization": f'Snowflake Token="{pat}"',
        "X-Snowflake-Authorization-Token-Type": "PROGRAMMATIC_ACCESS_TOKEN",
    }

    payload: dict = {"statement": sql, "timeout": 120}
    for key, param in (("sf_warehouse", "warehouse"), ("sf_database", "database"),
                       ("sf_schema", "schema"), ("sf_role", "role")):
        val = creds.get(key, "").strip()
        if val:
            payload[param] = val

    status, data = _post_json(f"{base}/api/v2/statements", headers, payload, timeout=30)

    if status in (200, 202):
        if status == 202:
            handle = data.get("statementHandle", "")
            poll_url = f"{base}/api/v2/statements/{handle}"
            for _ in range(30):
                time.sleep(2)
                poll_status, data = _get_json(poll_url, headers, timeout=15)
                if poll_status == 200:
                    break
                elif poll_status != 202:
                    raise RuntimeError(f"Snowflake poll error {poll_status}")
        col_names = [c["name"] for c in data.get("resultSetMetaData", {}).get("rowType", [])]
        rows = [dict(zip(col_names, row)) for row in data.get("data", [])]
        return {"columns": col_names, "rows": rows, "row_count": len(rows)}
    else:
        raise RuntimeError(f"Snowflake SQL API error {status}: {json.dumps(data)[:400]}")


def execute_snowflake_query_bearer(creds: dict, sql: str) -> dict:
    """Execute SQL via the Snowflake SQL REST API using an OAuth Bearer token (Snowflake PAT)."""
    base = _sf_api_base(creds)
    if not base:
        raise RuntimeError("Missing Snowflake host: set SNOWFLAKE_SQL_API_HOST or SNOWFLAKE_ACCOUNT")

    bearer = creds.get("sf_bearer_token", "")
    if not bearer:
        raise RuntimeError("Missing SNOWFLAKE_BEARER_TOKEN for Bearer authentication")

    headers = {
        "Content-Type":  "application/json",
        "Accept":        "application/json",
        "Authorization": f"Bearer {bearer}",
    }

    payload: dict = {"statement": sql, "timeout": 120}
    for key, param in (("sf_warehouse", "warehouse"), ("sf_database", "database"),
                       ("sf_schema", "schema"), ("sf_role", "role")):
        val = creds.get(key, "").strip()
        if val:
            payload[param] = val

    status, data = _post_json(f"{base}/api/v2/statements", headers, payload, timeout=30)

    if status in (200, 202):
        if status == 202:
            handle = data.get("statementHandle", "")
            poll_url = f"{base}/api/v2/statements/{handle}"
            for _ in range(30):
                time.sleep(2)
                poll_status, data = _get_json(poll_url, headers, timeout=15)
                if poll_status == 200:
                    break
                elif poll_status != 202:
                    raise RuntimeError(f"Snowflake poll error {poll_status}")
        col_names = [c["name"] for c in data.get("resultSetMetaData", {}).get("rowType", [])]
        rows = [dict(zip(col_names, row)) for row in data.get("data", [])]
        return {"columns": col_names, "rows": rows, "row_count": len(rows)}
    else:
        raise RuntimeError(f"Snowflake SQL API error {status}: {json.dumps(data)[:400]}")


def execute_snowflake_query(creds: dict, sql: str) -> dict:
    """Execute SQL on Snowflake using the Python connector."""
    if not SNOWFLAKE_AVAILABLE:
        raise RuntimeError(
            "snowflake-connector-python is required.\n"
            "Run: pip install snowflake-connector-python"
        )

    account = _normalise_sf_account(creds.get("sf_account", ""))
    conn_params: dict = {
        "account": account,
        "user":    creds["sf_user"],
        "password": creds["sf_password"],
        "session_parameters": {
            "QUERY_TAG": "cursor-nlq-skill",
            "STATEMENT_TIMEOUT_IN_SECONDS": "120",
        },
    }
    for opt_key, param_key in (("sf_warehouse", "warehouse"), ("sf_database", "database"),
                                ("sf_schema", "schema"), ("sf_role", "role")):
        val = creds.get(opt_key, "")
        if val:
            conn_params[param_key] = val

    conn = snowflake.connector.connect(**conn_params)
    try:
        cursor = conn.cursor(snowflake.connector.DictCursor)
        cursor.execute(sql)
        rows = cursor.fetchall()
        columns = [col[0] for col in (cursor.description or [])]
        return {"columns": columns, "rows": [dict(r) for r in rows], "row_count": len(rows)}
    finally:
        conn.close()


def _run_sf_query(creds: dict, sql: str) -> list:
    """Run a diagnostic query and return rows as list-of-dicts."""
    if creds.get("sf_bearer_token"):
        result = execute_snowflake_query_bearer(creds, sql)
    elif creds.get("sf_pat"):
        result = execute_snowflake_query_pat(creds, sql)
    else:
        required = ["sf_account", "sf_user", "sf_password"]
        missing = [k for k in required if not creds.get(k)]
        if missing:
            raise RuntimeError(
                f"Missing Snowflake credentials for resolve: {missing}. "
                "Provide --sf-bearer-token, --sf-pat, or --sf-account/user/password."
            )
        result = execute_snowflake_query(creds, sql)
    return result.get("rows", [])


# ---------------------------------------------------------------------------
# Subcommand: resolve
# ---------------------------------------------------------------------------

def cmd_resolve(args, creds: dict) -> None:
    """
    Query Snowflake INFORMATION_SCHEMA to return the exact case-sensitive names
    of tables and their columns.  OpenMetadata uppercases all identifiers on
    ingestion; this command retrieves ground-truth casing so the agent can
    generate SQL that won't fail with 'invalid identifier' errors.

    Accepts either:
      --tables  comma-separated list of database.schema.table triplets
      --schema  a single database.schema to resolve all tables in that schema
    """
    resolved: dict = {}

    targets: list = []
    if getattr(args, "tables", None):
        for t in args.tables.split(","):
            parts = t.strip().split(".")
            if len(parts) == 3:
                targets.append((parts[0].strip(), parts[1].strip(), parts[2].strip().upper()))
            else:
                print(f"[warn] Skipping malformed table ref: {t!r} (expected db.schema.table)",
                      file=sys.stderr)

    if getattr(args, "schema", None):
        schema_parts = args.schema.strip().split(".")
        if len(schema_parts) == 2:
            db, schema = schema_parts
            rows = _run_sf_query(
                creds,
                f"SELECT TABLE_NAME FROM {db}.INFORMATION_SCHEMA.TABLES "
                f"WHERE TABLE_SCHEMA = '{schema.upper()}' "
                f"ORDER BY TABLE_NAME"
            )
            for row in rows:
                tbl = row.get("TABLE_NAME", "")
                if tbl:
                    targets.append((db, schema, tbl.upper()))

    if not targets:
        print(json.dumps({"error": "Provide --tables db.schema.table[,...] or --schema db.schema"}),
              file=sys.stderr)
        sys.exit(1)

    for db, schema, table_upper in targets:
        print(f"[info] Resolving {db}.{schema}.{table_upper}", file=sys.stderr)

        tbl_rows = _run_sf_query(
            creds,
            f"SELECT TABLE_NAME FROM {db}.INFORMATION_SCHEMA.TABLES "
            f"WHERE TABLE_SCHEMA ILIKE '{schema}' "
            f"AND UPPER(TABLE_NAME) = '{table_upper}'"
        )
        if not tbl_rows:
            resolved[f"{db}.{schema}.{table_upper}"] = {
                "error": "Table not found in INFORMATION_SCHEMA — check permissions or spelling"
            }
            continue

        exact_table = tbl_rows[0]["TABLE_NAME"]

        col_rows = _run_sf_query(
            creds,
            f"SELECT COLUMN_NAME, DATA_TYPE, IS_NULLABLE, ORDINAL_POSITION "
            f"FROM {db}.INFORMATION_SCHEMA.COLUMNS "
            f"WHERE TABLE_SCHEMA ILIKE '{schema}' "
            f"AND TABLE_NAME = '{exact_table}' "
            f"ORDER BY ORDINAL_POSITION"
        )

        # mapping: OpenMetadata uppercase name → exact Snowflake name
        col_mapping = {row.get("COLUMN_NAME", "").upper(): row.get("COLUMN_NAME", "")
                       for row in col_rows}

        resolved[f"{db}.{schema}.{table_upper}"] = {
            "database":    db,
            "schema":      schema,
            "exact_table": exact_table,
            "quoted_ref":  f'{db}.{schema}."{exact_table}"',
            "columns":     col_mapping,
        }

    output = {
        "resolved_tables": resolved,
        "instruction": (
            "Use 'exact_table' and 'columns' values (not OpenMetadata uppercase names) "
            "when generating SQL. Always double-quote table names and column names: "
            "use the 'quoted_ref' field for the full table reference. "
            "Example: SELECT \"ExactColumnName\" FROM DB.SCHEMA.\"ExactTableName\""
        ),
    }
    print(json.dumps(output, indent=2, default=str))


# ---------------------------------------------------------------------------
# Subcommand: execute
# ---------------------------------------------------------------------------

def cmd_execute(args, creds: dict) -> None:
    """
    Execute SQL on Snowflake.
    Priority: Bearer token → legacy PAT → username/password connector.
    """
    if creds.get("sf_bearer_token"):
        base = _sf_api_base(creds)
        print(f"[info] Executing SQL via Snowflake REST API (Bearer): {base}", file=sys.stderr)
        result = execute_snowflake_query_bearer(creds, args.sql)
    elif creds.get("sf_pat"):
        base = _sf_api_base(creds)
        print(f"[info] Executing SQL via Snowflake REST API (PAT): {base}", file=sys.stderr)
        result = execute_snowflake_query_pat(creds, args.sql)
    else:
        required = ["sf_account", "sf_user", "sf_password"]
        missing = [k for k in required if not creds.get(k)]
        if missing:
            print(json.dumps({"error": f"Missing Snowflake credentials: {missing}. "
                              "Set SNOWFLAKE_BEARER_TOKEN + SNOWFLAKE_SQL_API_HOST in .env, "
                              "or provide username/password for connector mode."}),
                  file=sys.stderr)
            sys.exit(1)
        account_display = _normalise_sf_account(creds["sf_account"])
        print(f"[info] Executing SQL via Snowflake connector: {account_display}", file=sys.stderr)
        result = execute_snowflake_query(creds, args.sql)

    print(json.dumps(result, indent=2, default=str))


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def _add_credential_args(parser: argparse.ArgumentParser) -> None:
    sp = parser.add_argument_group("secrets provider (production)")
    sp.add_argument("--secrets-provider", dest="secrets_provider",
                    choices=["azure-keyvault", "aws-secrets-manager", "hashicorp-vault"],
                    metavar="PROVIDER",
                    help="Cloud-agnostic secrets provider (overrides all individual args). "
                         "Choices: azure-keyvault | aws-secrets-manager | hashicorp-vault")
    sp.add_argument("--secrets-url", dest="secrets_url", metavar="URL",
                    help="Vault URI (Azure KV vault URI or HashiCorp Vault address)")
    sp.add_argument("--secrets-region", dest="secrets_region", metavar="REGION",
                    help="AWS region (aws-secrets-manager only, default: us-east-1)")
    sp.add_argument("--secrets-name", dest="secrets_name", metavar="NAME",
                    help="AWS secret name (aws-secrets-manager only)")
    sp.add_argument("--secrets-path", dest="secrets_path", metavar="PATH",
                    help="HashiCorp Vault KV path (hashicorp-vault only)")
    sp.add_argument("--vault-token", dest="vault_token", metavar="TOKEN",
                    help="HashiCorp Vault token (falls back to VAULT_TOKEN env var)")

    sf = parser.add_argument_group("Snowflake credentials")
    sf.add_argument("--sf-bearer-token", dest="sf_bearer_token", metavar="TOKEN",
                    help="Snowflake OAuth Bearer token / PAT (SNOWFLAKE_BEARER_TOKEN)")
    sf.add_argument("--sf-pat", dest="sf_pat", metavar="PAT",
                    help="Snowflake legacy PAT using Snowflake Token= header (SNOWFLAKE_PAT)")
    sf.add_argument("--sf-api-host", dest="sf_api_host", metavar="URL",
                    help="Snowflake SQL API host (SNOWFLAKE_SQL_API_HOST)")
    sf.add_argument("--sf-account", dest="sf_account", metavar="ACCOUNT",
                    help="Snowflake account identifier (SNOWFLAKE_ACCOUNT)")
    sf.add_argument("--sf-user", dest="sf_user", metavar="USER",
                    help="Snowflake username (SNOWFLAKE_USERNAME)")
    sf.add_argument("--sf-password", dest="sf_password", metavar="PASS",
                    help="Snowflake password (SNOWFLAKE_PASSWORD)")
    sf.add_argument("--sf-warehouse", dest="sf_warehouse", metavar="WH",
                    help="Snowflake warehouse (SNOWFLAKE_WAREHOUSE)")
    sf.add_argument("--sf-database", dest="sf_database", metavar="DB",
                    help="Snowflake database (SNOWFLAKE_DATABASE)")
    sf.add_argument("--sf-schema", dest="sf_schema", metavar="SCHEMA",
                    help="Snowflake schema (SNOWFLAKE_SCHEMA)")
    sf.add_argument("--sf-role", dest="sf_role", metavar="ROLE",
                    help="Snowflake role (SNOWFLAKE_ROLE)")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Natural Language Data Query Engine — Snowflake backend",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    resolve_p = subparsers.add_parser(
        "resolve",
        help="Resolve exact Snowflake identifier casing via INFORMATION_SCHEMA",
    )
    resolve_p.add_argument("--tables", metavar="db.schema.TABLE[,...]",
                           help="Comma-separated list of fully-qualified table names to resolve")
    resolve_p.add_argument("--schema", metavar="db.schema",
                           help="Resolve all tables in a schema")
    _add_credential_args(resolve_p)

    execute_p = subparsers.add_parser(
        "execute",
        help="Execute a SQL query on Snowflake and return results as JSON",
    )
    execute_p.add_argument("--sql", required=True, metavar="SQL",
                           help="SQL query to execute")
    _add_credential_args(execute_p)

    return parser


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()
    creds = resolve_credentials(args)

    if args.command == "resolve":
        cmd_resolve(args, creds)
    elif args.command == "execute":
        cmd_execute(args, creds)
    else:
        parser.print_help()
        sys.exit(1)


if __name__ == "__main__":
    main()
