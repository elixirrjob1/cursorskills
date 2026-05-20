#!/usr/bin/env python3
"""Sync Fivetran connector.py schema() from analyzer schema.json."""

from __future__ import annotations

import argparse
import json
import re
import sys
from pathlib import Path
from typing import Any


def db_type_to_fivetran_sdk(db_type: str) -> str:
    """Map destination DB type string to Fivetran Connector SDK type constant."""
    t = str(db_type or "").upper().strip()

    if any(x in t for x in ("BOOLEAN", "BIT")) and "VARCHAR" not in t:
        return "BOOLEAN"
    if "UUID" in t or "UNIQUEIDENTIFIER" in t:
        return "STRING"
    if any(x in t for x in ("TIMESTAMP_TZ", "DATETIMEOFFSET", "WITH TIME ZONE")):
        return "UTC_DATETIME"
    if any(x in t for x in ("TIMESTAMP", "DATETIME", "DATETIME2")):
        return "UTC_DATETIME"
    if t == "DATE" or t.startswith("DATE"):
        return "NAIVE_DATE"
    if any(x in t for x in ("FLOAT", "DOUBLE", "BINARY_DOUBLE", "REAL")):
        return "DOUBLE"
    if any(x in t for x in ("DECIMAL", "NUMERIC", "NUMBER")):
        if "," in t or re.search(r"NUMBER\s*\(\s*\d+\s*,\s*[1-9]", t):
            return "DOUBLE"
        return "LONG"
    if any(x in t for x in ("INT", "BIGINT", "INTEGER", "SMALLINT")):
        return "LONG"
    if any(x in t for x in ("JSON", "OBJECT", "ARRAY")):
        return "JSON"
    if any(x in t for x in ("BINARY", "BLOB", "BYTEA")):
        return "BINARY"
    return "STRING"


def _format_schema_entry(table: dict[str, Any]) -> str:
    table_name = str(table.get("table") or "")
    primary_keys = table.get("primary_keys") or []
    columns = table.get("columns") or []

    lines = ["        {"]
    lines.append(f'            "table": "{table_name}",')
    if primary_keys:
        pk_repr = ", ".join(f'"{pk}"' for pk in primary_keys)
        lines.append(f'            "primary_key": [{pk_repr}],')
    lines.append('            "columns": {')

    col_lines = []
    for col in columns:
        name = str(col.get("name") or "")
        if not name:
            continue
        sdk_type = db_type_to_fivetran_sdk(str(col.get("type") or "STRING"))
        col_lines.append(f'                "{name}": "{sdk_type}",')
    lines.extend(col_lines)
    lines.append("            },")
    lines.append("        },")
    return "\n".join(lines)


def generate_schema_function_body(tables: list[dict[str, Any]]) -> str:
    entries = [_format_schema_entry(t) for t in tables if t.get("table")]
    inner = "\n".join(entries)
    return (
        "def schema(configuration: dict):\n"
        "    return [\n"
        f"{inner}\n"
        "    ]\n"
    )


def replace_schema_function(source: str, new_schema_fn: str) -> str:
    """Replace schema() function in connector.py source."""
    pattern = re.compile(
        r"(# -+\n# Schema\n# -+\n)?def schema\(configuration: dict\):.*?(?=\n# -+\n# Update|\ndef update\(|\nconnector = )",
        re.DOTALL,
    )
    match = pattern.search(source)
    if not match:
        raise ValueError("Could not locate schema() function in connector.py")

    header = match.group(1) or (
        "# ---------------------------------------------------------------------------\n"
        "# Schema\n"
        "# ---------------------------------------------------------------------------\n\n"
    )
    replacement = header + new_schema_fn + "\n\n"
    return source[: match.start()] + replacement + source[match.end() :]


def sync_connector_schema(
    schema_json_path: Path,
    connector_py_path: Path,
    *,
    table_filter: set[str] | None = None,
) -> tuple[str, list[str]]:
    with open(schema_json_path, "r", encoding="utf-8") as f:
        schema_doc = json.load(f)

    tables = schema_doc.get("tables", []) or []
    changed_tables = (
        [str(t.get("table")) for t in tables if str(t.get("table")) in table_filter]
        if table_filter
        else [str(t.get("table")) for t in tables if t.get("table")]
    )

    new_fn = generate_schema_function_body(tables)
    with open(connector_py_path, "r", encoding="utf-8") as f:
        original = f.read()

    updated = replace_schema_function(original, new_fn)
    return updated, changed_tables


def main() -> int:
    parser = argparse.ArgumentParser(description="Sync connector.py schema() from schema.json")
    parser.add_argument("schema_json", help="Path to analyzer schema.json")
    parser.add_argument("connector_py", help="Path to connector.py")
    parser.add_argument(
        "--tables",
        help="Comma-separated table names that changed (for reporting only; full schema.json is synced)",
    )
    parser.add_argument("--dry-run", action="store_true", help="Print diff summary without writing")
    args = parser.parse_args()

    schema_path = Path(args.schema_json)
    connector_path = Path(args.connector_py)
    table_filter = None
    if args.tables:
        table_filter = {t.strip() for t in args.tables.split(",") if t.strip()}

    if not schema_path.exists():
        print(f"Error: schema not found: {schema_path}", file=sys.stderr)
        return 1
    if not connector_path.exists():
        print(f"Error: connector not found: {connector_path}", file=sys.stderr)
        return 1

    try:
        updated, tables = sync_connector_schema(schema_path, connector_path, table_filter=table_filter)
    except ValueError as exc:
        print(f"Error: {exc}", file=sys.stderr)
        return 1

    if args.dry_run:
        print(f"Would sync {len(tables)} table(s) to {connector_path}:")
        for name in tables:
            print(f"  - {name}")
        return 0

    with open(connector_path, "w", encoding="utf-8") as f:
        f.write(updated)

    print(f"Updated {connector_path}")
    print(f"  Tables synced: {len(tables)}")
    for name in tables:
        print(f"    - {name}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
