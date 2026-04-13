#!/usr/bin/env python3
"""Push table and column descriptions from analyzer JSON into OpenMetadata."""

from __future__ import annotations

import base64
import json
import os
import sys
import time
from typing import Any

import requests
from dotenv import load_dotenv

load_dotenv()

USER_AGENT = "openmetadata-cursor-mcp"
API_VERSION_PREFIX = "v1"
_TOKEN_CACHE: dict[str, Any] = {"token": None}


def _api_root() -> str:
    base = (os.getenv("OPENMETADATA_BASE_URL", "")).strip().rstrip("/")
    if not base:
        raise RuntimeError("Missing OPENMETADATA_BASE_URL")
    return f"{base}/api" if not base.endswith("/api") else base


def _api_url(path: str) -> str:
    cleaned = path.lstrip("/")
    if not cleaned.startswith(f"{API_VERSION_PREFIX}/"):
        cleaned = f"{API_VERSION_PREFIX}/{cleaned}"
    return f"{_api_root()}/{cleaned}"


def _login() -> str:
    jwt = os.getenv("OPENMETADATA_JWT_TOKEN", "").strip()
    if jwt:
        return jwt
    cached = _TOKEN_CACHE.get("token")
    if isinstance(cached, str) and cached.strip():
        return cached.strip()

    email = os.getenv("OPENMETADATA_EMAIL", "").strip()
    password = os.getenv("OPENMETADATA_PASSWORD", "")
    if not email or not password:
        raise RuntimeError("Missing OPENMETADATA_EMAIL / OPENMETADATA_PASSWORD")

    encoded = base64.b64encode(password.encode()).decode("ascii")
    for payload in [{"email": email, "password": encoded}, {"email": email, "password": password}]:
        try:
            r = requests.post(
                _api_url("users/login"),
                headers={"Accept": "application/json", "Content-Type": "application/json"},
                json=payload,
                timeout=(10, 30),
            )
            r.raise_for_status()
            data = r.json() if r.content else {}
            for key in ("accessToken", "jwtToken", "token", "id_token"):
                tok = data.get(key)
                if isinstance(tok, str) and tok.strip():
                    _TOKEN_CACHE["token"] = tok.strip()
                    return tok.strip()
        except Exception:
            continue
    raise RuntimeError("OpenMetadata login failed")


def _headers() -> dict[str, str]:
    return {
        "Accept": "application/json",
        "Content-Type": "application/json",
        "Authorization": f"Bearer {_login()}",
        "User-Agent": USER_AGENT,
    }


def _patch(endpoint: str, ops: list[dict[str, Any]]) -> Any:
    headers = dict(_headers())
    headers["Content-Type"] = "application/json-patch+json"
    for attempt in range(2):
        try:
            r = requests.patch(_api_url(endpoint), headers=headers, json=ops, timeout=(10, 30))
            if r.status_code == 401 and attempt < 1:
                _TOKEN_CACHE["token"] = None
                continue
            r.raise_for_status()
            return r.json() if r.content else {"success": True}
        except Exception as e:
            if attempt == 1:
                raise RuntimeError(f"PATCH failed: {e}") from e
            time.sleep(1)


def _get(endpoint: str, params: dict | None = None) -> Any:
    for attempt in range(2):
        try:
            r = requests.get(_api_url(endpoint), headers=_headers(), params=params, timeout=(10, 30))
            if r.status_code == 401 and attempt < 1:
                _TOKEN_CACHE["token"] = None
                continue
            r.raise_for_status()
            return r.json()
        except Exception as e:
            if attempt == 1:
                raise RuntimeError(f"GET failed: {e}") from e
            time.sleep(1)


def get_table_entity(table_fqn: str) -> dict[str, Any]:
    return _get(f"tables/name/{table_fqn}", params={"fields": "columns,tags"})


def update_table_description(table_fqn: str, description: str) -> dict[str, Any]:
    entity = get_table_entity(table_fqn)
    table_id = entity["id"]
    op = "replace" if entity.get("description") else "add"
    return _patch(f"tables/{table_id}", [{"op": op, "path": "/description", "value": description}])


def update_column_description(table_fqn: str, table_id: str, column_index: int, description: str, has_existing: bool) -> dict[str, Any]:
    op = "replace" if has_existing else "add"
    return _patch(f"tables/{table_id}", [{"op": op, "path": f"/columns/{column_index}/description", "value": description}])


def main():
    analyzer_path = sys.argv[1] if len(sys.argv) > 1 else "LATEST_SCHEMA/schema_azure_mssql_dbo.json"
    fqn_prefix = sys.argv[2] if len(sys.argv) > 2 else "snowflake_fivetran.DRIP_DATA_INTELLIGENCE.BRONZE_ERP__DBO"

    with open(analyzer_path) as f:
        data = json.load(f)

    tables = data.get("tables", [])
    print(f"Loaded {len(tables)} tables from analyzer JSON")
    print(f"Target FQN prefix: {fqn_prefix}")
    print()

    om_tables_resp = _get("tables", params={"databaseSchema": fqn_prefix, "fields": "columns,tags", "limit": 100})
    om_tables = om_tables_resp.get("data", [])
    om_lookup: dict[str, dict] = {}
    for t in om_tables:
        om_lookup[t["name"].upper()] = t
    print(f"Found {len(om_lookup)} tables in OpenMetadata schema")
    print()

    stats = {"tables_updated": 0, "columns_updated": 0, "tables_skipped": 0, "columns_skipped": 0, "errors": []}

    for analyzer_table in tables:
        table_name = analyzer_table.get("table", "")
        table_desc = analyzer_table.get("table_description", "")
        table_name_upper = table_name.upper()

        if table_name_upper not in om_lookup:
            print(f"  SKIP table '{table_name}' — not found in OpenMetadata")
            stats["tables_skipped"] += 1
            continue

        om_table = om_lookup[table_name_upper]
        table_fqn = om_table["fullyQualifiedName"]
        table_id = om_table["id"]

        if table_desc:
            try:
                op = "replace" if om_table.get("description") else "add"
                result = _patch(f"tables/{table_id}", [{"op": op, "path": "/description", "value": table_desc}])
                print(f"  OK table description: {table_name}")
                stats["tables_updated"] += 1
                om_table = result
            except Exception as e:
                msg = f"FAIL table description '{table_name}': {e}"
                print(f"  {msg}")
                stats["errors"].append(msg)

        om_columns = om_table.get("columns", [])
        om_col_index: dict[str, int] = {}
        for i, c in enumerate(om_columns):
            om_col_index[c["name"].upper()] = i

        for analyzer_col in analyzer_table.get("columns", []):
            col_name = analyzer_col.get("name", "")
            col_desc = analyzer_col.get("column_description", "")
            col_name_upper = col_name.upper()

            if not col_desc:
                continue

            if col_name_upper not in om_col_index:
                print(f"    SKIP column '{table_name}.{col_name}' — not found in OpenMetadata")
                stats["columns_skipped"] += 1
                continue

            idx = om_col_index[col_name_upper]
            has_existing = bool(om_columns[idx].get("description"))

            try:
                op = "replace" if has_existing else "add"
                _patch(f"tables/{table_id}", [{"op": op, "path": f"/columns/{idx}/description", "value": col_desc}])
                print(f"    OK column description: {table_name}.{col_name}")
                stats["columns_updated"] += 1
            except Exception as e:
                msg = f"FAIL column description '{table_name}.{col_name}': {e}"
                print(f"    {msg}")
                stats["errors"].append(msg)

    print()
    print("=" * 60)
    print(f"Tables updated:  {stats['tables_updated']}")
    print(f"Columns updated: {stats['columns_updated']}")
    print(f"Tables skipped:  {stats['tables_skipped']}")
    print(f"Columns skipped: {stats['columns_skipped']}")
    print(f"Errors:          {len(stats['errors'])}")
    if stats["errors"]:
        for err in stats["errors"]:
            print(f"  - {err}")


if __name__ == "__main__":
    main()
