#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


def build_checklist(payload: dict[str, Any], schema_json_path: str) -> dict[str, Any]:
    items: list[dict[str, Any]] = []
    missing_table_descriptions = 0
    missing_column_descriptions = 0

    for table in payload.get("tables", []) or []:
        schema_name = str(table.get("schema") or "").strip()
        table_name = str(table.get("table") or "").strip()
        table_description = str(table.get("table_description") or "").strip()
        missing_column_ids: list[str] = []

        for position, column in enumerate(table.get("columns", []) or [], start=1):
            column_name = str(column.get("name") or "").strip()
            column_description = str(column.get("column_description") or "").strip()
            if column_description:
                continue
            missing_column_descriptions += 1
            item_id = f"column:{schema_name}.{table_name}.{column_name}"
            missing_column_ids.append(item_id)
            items.append(
                {
                    "item_id": item_id,
                    "status": "pending",
                    "phase": "column_descriptions",
                    "schema": schema_name,
                    "table": table_name,
                    "column": column_name,
                    "field": "column_description",
                    "table_order": f"{schema_name}.{table_name}",
                    "work_order": len(items) + 1,
                    "column_position": position,
                    "current_description": "",
                    "proposed_description": "",
                    "sample_rows": [],
                }
            )

        if not table_description:
            missing_table_descriptions += 1
            items.append(
                {
                    "item_id": f"table:{schema_name}.{table_name}",
                    "status": "pending",
                    "phase": "table_description",
                    "schema": schema_name,
                    "table": table_name,
                    "column": None,
                    "field": "table_description",
                    "table_order": f"{schema_name}.{table_name}",
                    "work_order": len(items) + 1,
                    "depends_on_item_ids": missing_column_ids,
                    "current_description": "",
                    "proposed_description": "",
                    "sample_rows": [],
                }
            )

    return {
        "source_schema_json": schema_json_path,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "workflow": {
            "process_order": [
                "Work table by table in checklist order.",
                "For each table, complete all column_description items first.",
                "Query up to 3 sample rows per unresolved column when needed.",
                "Only after column descriptions are completed, write the table_description for that same table.",
                "After all checklist items are complete, merge descriptions back into the main analyzer JSON.",
            ]
        },
        "summary": {
            "missing_table_descriptions": missing_table_descriptions,
            "missing_column_descriptions": missing_column_descriptions,
            "total_items": len(items),
        },
        "items": items,
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="Build a checklist of missing table and column descriptions from analyzer JSON.")
    parser.add_argument("schema_json", help="Path to analyzer schema JSON")
    parser.add_argument("--output", "-o", default=None, help="Output checklist path")
    args = parser.parse_args()

    schema_path = Path(args.schema_json)
    payload = json.loads(schema_path.read_text(encoding="utf-8"))
    checklist = build_checklist(payload, str(schema_path))

    output_path = Path(args.output) if args.output else schema_path.with_name(f"{schema_path.stem}_description_checklist.json")
    output_path.write_text(json.dumps(checklist, indent=2), encoding="utf-8")
    print(str(output_path))


if __name__ == "__main__":
    main()
