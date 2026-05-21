#!/usr/bin/env python3
"""Export OpenMetadata table/column metadata into a flat data_catalogue CSV."""

from __future__ import annotations

import csv
import json
import os
import sys
import time
from datetime import datetime, timezone
from typing import Any

import requests
from dotenv import load_dotenv

from om_auth import om_api_url, om_bearer_headers

load_dotenv()


def _api_url(path: str) -> str:
    return om_api_url(path)


def _headers() -> dict[str, str]:
    return om_bearer_headers(user_agent="export-openmetadata-catalogue")


def _get(endpoint: str, params: dict | None = None) -> Any:
    for attempt in range(2):
        try:
            r = requests.get(_api_url(endpoint), headers=_headers(), params=params, timeout=(10, 30))
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
