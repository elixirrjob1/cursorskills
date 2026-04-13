#!/usr/bin/env python3
"""Export OpenMetadata table/column metadata into a flat data_catalogue CSV."""

from __future__ import annotations

import base64
import csv
import json
import os
import sys
import time
from datetime import datetime, timezone
from typing import Any

import requests
from dotenv import load_dotenv

load_dotenv()

USER_AGENT = "openmetadata-cursor-mcp"
API_VERSION_PREFIX = "v1"
_TOKEN_CACHE: dict[str, Any] = {"token": None}


def _api_root() -> str:
    base = os.getenv("OPENMETADATA_BASE_URL", "").strip().rstrip("/")
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
    encoded = base64.b64encode(password.encode()).decode("ascii")
    for payload in [{"email": email, "password": encoded}, {"email": email, "password": password}]:
        try:
            r = requests.post(
                _api_url("users/login"),
                headers={"Accept": "application/json", "Content-Type": "application/json"},
                json=payload, timeout=(10, 30),
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


_DATATYPE_LENGTHS: dict[str, int] = {
    "BOOLEAN": 1, "TINYINT": 1, "SMALLINT": 2, "INT": 4, "BIGINT": 8,
    "FLOAT": 8, "DOUBLE": 8, "DECIMAL": -1, "NUMBER": -1,
    "DATE": 3, "DATETIME": 8, "TIMESTAMP": 8,
}


def _infer_max_length(col: dict[str, Any]) -> str:
    data_length = col.get("dataLength")
    if data_length and data_length > 1:
        return str(data_length)
    dt = (col.get("dataType") or "").upper()
    mapped = _DATATYPE_LENGTHS.get(dt)
    if mapped and mapped > 0:
        return str(mapped)
    precision = col.get("precision")
    if precision:
        return str(precision)
    return ""


def _is_key(col: dict[str, Any], table_constraint_cols: set[str]) -> str:
    constraint = (col.get("constraint") or "").upper()
    col_name = col.get("name", "")
    if constraint in ("PRIMARY_KEY", "UNIQUE"):
        return "Yes"
    if col_name in table_constraint_cols:
        return "Yes"
    return ""


def main() -> None:
    schema_fqn = sys.argv[1] if len(sys.argv) > 1 else "snowflake_fivetran.DRIP_DATA_INTELLIGENCE.BRONZE_ERP__DBO"
    output_path = sys.argv[2] if len(sys.argv) > 2 else ".cursor/flat/data_catalogue.csv"

    parts = schema_fqn.split(".")
    schema_name = parts[-1] if parts else schema_fqn

    resp = _get("tables", params={"databaseSchema": schema_fqn, "fields": "columns,tags,tableConstraints", "limit": 500})
    tables = resp.get("data", [])
    print(f"Fetched {len(tables)} tables from {schema_fqn}")

    rows: list[dict[str, Any]] = []
    row_id = 0

    for table in sorted(tables, key=lambda t: t.get("name", "")):
        table_name = table.get("name", "")
        full_prefix = f"{schema_name}.{table_name}"
        updated_epoch = table.get("updatedAt")
        created_at = ""
        if updated_epoch:
            created_at = datetime.fromtimestamp(updated_epoch / 1000, tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S")

        pk_cols: set[str] = set()
        for constraint in table.get("tableConstraints", []):
            if (constraint.get("constraintType") or "").upper() == "PRIMARY_KEY":
                for c in constraint.get("columns", []):
                    pk_cols.add(c.upper())

        columns = table.get("columns", [])
        for col in columns:
            col_name = col.get("name", "")
            if col_name.startswith("_FIVETRAN"):
                continue

            row_id += 1
            data_type = col.get("dataTypeDisplay") or col.get("dataType") or ""
            nullable = col.get("constraint", "")
            is_nullable = "1" if nullable.upper() == "NULL" or nullable == "" else "0"
            if col_name.upper() in pk_cols:
                is_nullable = "0"

            rows.append({
                "id": row_id,
                "full_object_name": full_prefix,
                "column_name": col_name,
                "data_type": data_type,
                "max_length": _infer_max_length(col),
                "precision_value": col.get("precision") or "",
                "scale_value": col.get("scale") if col.get("scale") is not None else "",
                "is_nullable": is_nullable,
                "description": (col.get("description") or "").replace("\n", " ").strip(),
                "is_key": _is_key(col, pk_cols),
                "schema_name": schema_name,
                "table_name": table_name,
                "created_at": created_at,
            })

    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    fieldnames = [
        "id", "full_object_name", "column_name", "data_type", "max_length",
        "precision_value", "scale_value", "is_nullable", "description",
        "is_key", "schema_name", "table_name", "created_at",
    ]
    with open(output_path, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)

    print(f"Wrote {len(rows)} rows to {output_path}")


if __name__ == "__main__":
    main()
