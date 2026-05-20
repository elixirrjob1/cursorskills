#!/usr/bin/env python3
"""Merge type enrichment checklist proposals back into schema.json."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any


def apply_type_checklist(schema: dict[str, Any], checklist: dict[str, Any]) -> dict[str, Any]:
    updates: dict[tuple[str, str, str], dict[str, Any]] = {}
    for item in checklist.get("items", []) or []:
        if item.get("phase") != "type_review":
            continue
        if item.get("status") != "completed":
            continue
        proposed = str(item.get("proposed_type") or "").strip()
        if not proposed:
            continue
        key = (
            str(item.get("schema") or ""),
            str(item.get("table") or ""),
            str(item.get("column") or ""),
        )
        updates[key] = {
            "type": proposed,
            "type_confidence": str(item.get("proposed_type_confidence") or "ai"),
        }

    updated_tables = []
    for table in schema.get("tables", []) or []:
        schema_name = str(table.get("schema") or "")
        table_name = str(table.get("table") or "")
        new_cols = []
        for col in table.get("columns", []) or []:
            col_copy = dict(col)
            key = (schema_name, table_name, str(col.get("name") or ""))
            if key in updates:
                col_copy.update(updates[key])
            new_cols.append(col_copy)
        table_copy = dict(table)
        table_copy["columns"] = new_cols
        updated_tables.append(table_copy)

    result = dict(schema)
    result["tables"] = updated_tables
    return result


def main() -> int:
    parser = argparse.ArgumentParser(description="Apply type enrichment checklist to schema.json")
    parser.add_argument("schema_json", help="Path to schema.json")
    parser.add_argument("checklist_json", help="Path to type enrichment checklist")
    parser.add_argument("--in-place", action="store_true", help="Overwrite schema.json")
    parser.add_argument("--output", "-o", help="Output path (default: overwrite schema.json if --in-place)")
    args = parser.parse_args()

    schema_path = Path(args.schema_json)
    checklist_path = Path(args.checklist_json)

    with open(schema_path, "r", encoding="utf-8") as f:
        schema = json.load(f)
    with open(checklist_path, "r", encoding="utf-8") as f:
        checklist = json.load(f)

    merged = apply_type_checklist(schema, checklist)
    out_path = schema_path if args.in_place or not args.output else Path(args.output)
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(merged, f, indent=2, default=str)

    applied = sum(
        1 for i in checklist.get("items", []) or []
        if i.get("phase") == "type_review" and i.get("status") == "completed" and i.get("proposed_type")
    )
    print(f"Applied {applied} type updates to {out_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
