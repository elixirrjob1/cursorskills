#!/usr/bin/env python3
"""Build a type enrichment checklist for AI review of ambiguous API column types."""

from __future__ import annotations

import argparse
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

LONG_TYPE_MARKERS = ("TEXT", "MAX)", "CLOB", "DOUBLE PRECISION")


def _needs_ai_review(col: dict[str, Any]) -> bool:
    confidence = col.get("type_confidence")
    if confidence in ("manual", "source", "ai_verified"):
        return False
    db_type = str(col.get("type") or "")
    if confidence == "rule" and not any(m in db_type for m in LONG_TYPE_MARKERS):
        return False
    return True


def build_type_checklist(
    payload: dict[str, Any],
    schema_json_path: str,
    dialect: str,
) -> dict[str, Any]:
    items: list[dict[str, Any]] = []

    for table in payload.get("tables", []) or []:
        schema_name = str(table.get("schema") or "").strip()
        table_name = str(table.get("table") or "").strip()
        table_items: list[dict[str, Any]] = []

        for col in table.get("columns", []) or []:
            if not _needs_ai_review(col):
                continue
            col_name = str(col.get("name") or "").strip()
            item_id = f"type:{schema_name}.{table_name}.{col_name}"
            table_items.append({
                "item_id": item_id,
                "status": "pending",
                "phase": "type_review",
                "schema": schema_name,
                "table": table_name,
                "column": col_name,
                "field": "type",
                "dialect": dialect,
                "current_type": str(col.get("type") or ""),
                "type_confidence": col.get("type_confidence"),
                "cardinality": col.get("cardinality"),
                "proposed_type": "",
                "proposed_type_confidence": "ai",
                "sample_values": [],
                "notes": "",
            })

        if table_items:
            items.append({
                "item_id": f"table_sweep:{schema_name}.{table_name}",
                "status": "pending",
                "phase": "table_type_sweep",
                "schema": schema_name,
                "table": table_name,
                "column": None,
                "dialect": dialect,
                "depends_on_item_ids": [i["item_id"] for i in table_items],
                "column_items": table_items,
                "notes": (
                    "Review all columns for this table. For uncertain columns, fetch up to 10 live API rows "
                    "and confirm or correct the destination DB type."
                ),
            })
            items.extend(table_items)

    pending = sum(1 for i in items if i.get("status") == "pending")

    return {
        "source_schema_json": schema_json_path,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "target_dialect": dialect,
        "workflow": {
            "process_order": [
                "Work table by table.",
                "For each table_sweep item, review all column_items.",
                "For uncertain columns, fetch live API sample rows before assigning proposed_type.",
                "Use dialect-native type names only.",
                "Merge checklist back with apply_type_enrichment.py.",
            ],
        },
        "summary": {
            "total_items": len(items),
            "pending_items": pending,
            "table_sweeps": sum(1 for i in items if i.get("phase") == "table_type_sweep"),
        },
        "items": items,
    }


def main() -> int:
    parser = argparse.ArgumentParser(description="Build type enrichment checklist from schema.json")
    parser.add_argument("schema_json", help="Path to schema.json")
    parser.add_argument("--dialect", required=True, choices=["snowflake", "sqlserver", "postgresql", "oracle"])
    parser.add_argument("--output", "-o", help="Output checklist path (default: schema_type_checklist.json)")
    args = parser.parse_args()

    schema_path = Path(args.schema_json)
    with open(schema_path, "r", encoding="utf-8") as f:
        payload = json.load(f)

    checklist = build_type_checklist(payload, str(schema_path), args.dialect)
    out_path = Path(args.output) if args.output else schema_path.with_name("schema_type_checklist.json")
    out_path.parent.mkdir(parents=True, exist_ok=True)
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(checklist, f, indent=2, default=str)

    print(f"Wrote type checklist: {out_path}")
    print(f"  Pending items: {checklist['summary']['pending_items']}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
