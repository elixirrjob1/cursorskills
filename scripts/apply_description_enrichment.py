#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any


def apply_checklist(payload: dict[str, Any], checklist: dict[str, Any]) -> dict[str, Any]:
    table_lookup: dict[tuple[str, str], dict[str, Any]] = {}
    for table in payload.get("tables", []) or []:
        table_lookup[(str(table.get("schema") or ""), str(table.get("table") or ""))] = table

    for item in checklist.get("items", []) or []:
        proposed = str(item.get("proposed_description") or "").strip()
        if not proposed:
            continue

        schema_name = str(item.get("schema") or "")
        table_name = str(item.get("table") or "")
        column_name = item.get("column")
        field = str(item.get("field") or "")

        table = table_lookup.get((schema_name, table_name))
        if not table:
            continue

        if field == "table_description" and not column_name:
            table["table_description"] = proposed
            continue

        if field == "column_description" and column_name:
            for column in table.get("columns", []) or []:
                if str(column.get("name") or "") == str(column_name):
                    column["column_description"] = proposed
                    break

    return payload


def main() -> None:
    parser = argparse.ArgumentParser(description="Apply completed description checklist entries back into analyzer JSON.")
    parser.add_argument("schema_json", help="Path to analyzer schema JSON")
    parser.add_argument("checklist_json", help="Path to completed description checklist JSON")
    parser.add_argument("--output", "-o", default=None, help="Output path (defaults to in-place overwrite of schema_json)")
    args = parser.parse_args()

    schema_path = Path(args.schema_json)
    checklist_path = Path(args.checklist_json)

    payload = json.loads(schema_path.read_text(encoding="utf-8"))
    checklist = json.loads(checklist_path.read_text(encoding="utf-8"))
    updated = apply_checklist(payload, checklist)

    output_path = Path(args.output) if args.output else schema_path
    output_path.write_text(json.dumps(updated, indent=2), encoding="utf-8")
    print(str(output_path))


if __name__ == "__main__":
    main()
