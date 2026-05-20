#!/usr/bin/env python3
"""
Analyze API data files and generate normalized schema.json output.

Reads JSON files downloaded by api_reader.py and produces schema.json
following the shared output schema contract.
"""

from __future__ import annotations

import argparse
import json
import os
import re
import sys
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Tuple
from urllib.parse import urljoin

import requests

SCRIPT_DIR = Path(__file__).resolve().parent
if str(SCRIPT_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPT_DIR))

from api_type_recommender import (  # noqa: E402
    SUPPORTED_DIALECTS,
    apply_type_recommendations,
    detect_tables_needing_types,
    is_crude_type,
    merge_table_entries,
    profile_column,
    recommend_db_type,
    refine_ambiguous_type,
)


def infer_type(value: Any) -> str:
    """Infer SQL-like type from Python value."""
    if value is None:
        return "text"
    if isinstance(value, bool):
        return "boolean"
    if isinstance(value, int):
        return "integer"
    if isinstance(value, float):
        return "numeric"
    if isinstance(value, str):
        if re.match(r"^\d{4}-\d{2}-\d{2}(\s+\d{2}:\d{2}:\d{2})?", value):
            return "timestamp"
        return "text"
    return "text"


def _normalize_discovery_tables(raw_tables: Any) -> Tuple[List[str], Dict[str, Dict[str, Any]]]:
    names: List[str] = []
    metadata_by_table: Dict[str, Dict[str, Any]] = {}

    if not isinstance(raw_tables, list):
        return names, metadata_by_table

    for entry in raw_tables:
        if isinstance(entry, str):
            table_name = entry.strip()
            if table_name and table_name not in metadata_by_table:
                names.append(table_name)
                metadata_by_table[table_name] = {}
            continue

        if isinstance(entry, dict):
            table_name = str(entry.get("table") or entry.get("name") or "").strip()
            if not table_name:
                continue
            if table_name not in metadata_by_table:
                names.append(table_name)
            metadata_by_table[table_name] = entry

    return names, metadata_by_table


def _load_table_payload(data_dir: Path, table_name: str) -> Optional[Dict[str, Any]]:
    candidates = [data_dir / table_name, data_dir / f"{table_name}.json"]
    for data_file in candidates:
        if not data_file.exists():
            continue
        with open(data_file, "r", encoding="utf-8") as f:
            payload = json.load(f)
        if isinstance(payload, dict):
            return payload
    return None


def _fetch_live_table_records(
    base_url: str,
    table_name: str,
    *,
    limit: int = 10,
    timeout: int = 30,
) -> List[dict]:
    """Fetch fresh sample rows from the live API for verification."""
    token = os.environ.get("API_BEARER_TOKEN") or os.environ.get("DBT_SERVICE_TOKEN")
    headers = {}
    if token:
        headers["Authorization"] = f"Bearer {token}"

    url = urljoin(base_url.rstrip("/") + "/", f"api/{table_name}")
    try:
        resp = requests.get(url, headers=headers, params={"limit": limit}, timeout=timeout)
        resp.raise_for_status()
        body = resp.json()
    except Exception:
        return []

    if isinstance(body, dict):
        data = body.get("data")
        if isinstance(data, list):
            return [r for r in data if isinstance(r, dict)]
    return []


def _verify_table_types(
    columns: List[Dict[str, Any]],
    records: List[dict],
    dialect: str,
    *,
    base_url: str,
    table_name: str,
    fetch_live: bool,
) -> List[Dict[str, Any]]:
    """Re-profile flagged columns; optionally fetch live API samples."""
    live_records = list(records)
    if fetch_live and base_url:
        fetched = _fetch_live_table_records(base_url, table_name)
        if fetched:
            live_records = fetched

    updated = []
    for col in columns:
        col_copy = dict(col)
        if col_copy.get("type_confidence") in ("source", "manual", "ai_verified"):
            updated.append(col_copy)
            continue

        col_name = str(col_copy.get("name") or "")
        coarse = str(col_copy.get("type") or "text").lower().split("(")[0]
        if is_crude_type(coarse):
            pass
        elif col_copy.get("type_confidence") == "rule":
            long_markers = ("TEXT", "MAX)", "CLOB", "DOUBLE PRECISION")
            if not any(m in str(col_copy.get("type") or "") for m in long_markers):
                updated.append(col_copy)
                continue

        profile = profile_column(col_name, live_records, coarse if is_crude_type(coarse) else "text")
        db_type, confidence = recommend_db_type(
            coarse if is_crude_type(coarse) else "text", profile, dialect, col_name
        )
        if confidence == "ambiguous":
            db_type, confidence = refine_ambiguous_type(
                coarse if is_crude_type(coarse) else "text",
                profile,
                dialect,
                col_name,
                col_copy.get("cardinality"),
            )

        if db_type != col_copy.get("type") or confidence == "ambiguous":
            col_copy["type"] = db_type
            col_copy["type_confidence"] = "ai_verified" if fetch_live else confidence
        updated.append(col_copy)

    return updated


def _process_table(
    table_name: str,
    table_data: Optional[Dict[str, Any]],
    discovered_table_meta: Dict[str, Any],
    schema_name: str,
    tables_list: List[str],
    *,
    recommend_dialect: Optional[str] = None,
    verify_types: bool = False,
    base_url: str = "",
    fetch_live: bool = False,
) -> Optional[Dict[str, Any]]:
    if not table_data:
        return None

    api_table_meta = table_data.get("metadata") if isinstance(table_data.get("metadata"), dict) else {}
    table_meta = api_table_meta or discovered_table_meta

    schema = table_data.get("schema") or table_meta.get("schema") or schema_name
    records = table_data.get("data", []) if isinstance(table_data.get("data"), list) else []

    columns: List[Dict[str, Any]] = []
    null_counts: Counter = Counter()
    value_samples: Dict[str, Set[str]] = {}

    metadata_columns = table_meta.get("columns") if isinstance(table_meta, dict) else None
    from_source_metadata = bool(isinstance(metadata_columns, list) and metadata_columns)

    if from_source_metadata:
        for col in metadata_columns:
            if not isinstance(col, dict):
                continue
            col_name = str(col.get("name") or "").strip()
            if not col_name:
                continue
            columns.append({
                "name": col_name,
                "type": str(col.get("type") or "text"),
                "nullable": bool(col.get("nullable", True)),
                "is_incremental": bool(col.get("is_incremental", False)),
                "cardinality": None,
                "null_count": 0,
                "data_range": {"min": None, "max": None},
                "data_category": None,
            })
            value_samples[col_name] = set()
    elif records:
        first_record = records[0]
        for col_name, col_value in first_record.items():
            columns.append({
                "name": col_name,
                "type": infer_type(col_value),
                "nullable": True,
                "is_incremental": col_name in ("created_at", "updated_at", "id"),
                "cardinality": None,
                "null_count": 0,
                "data_range": {"min": None, "max": None},
                "data_category": None,
            })
            value_samples[col_name] = set()

    if not columns:
        return None

    for record in records:
        for col in columns:
            col_name_str = col["name"]
            value = record.get(col_name_str)
            if value is None:
                null_counts[col_name_str] += 1
            elif isinstance(value, (str, int, float, bool)):
                value_samples[col_name_str].add(str(value))

    for col in columns:
        col_name = col["name"]
        col["null_count"] = null_counts[col_name]
        if records:
            col["nullable"] = null_counts[col_name] > 0
        unique_count = len(value_samples.get(col_name, set()))
        col["cardinality"] = unique_count if unique_count > 0 else None

        if col["type"] in ("integer", "numeric"):
            numeric_values = [
                float(r.get(col_name))
                for r in records
                if r.get(col_name) is not None and isinstance(r.get(col_name), (int, float))
            ]
            if numeric_values:
                col["data_range"]["min"] = str(min(numeric_values))
                col["data_range"]["max"] = str(max(numeric_values))

    meta_primary_keys = table_meta.get("primary_keys") if isinstance(table_meta, dict) else None
    if isinstance(meta_primary_keys, list) and meta_primary_keys:
        primary_keys = [str(pk) for pk in meta_primary_keys if str(pk).strip()]
    else:
        primary_keys = []
        for col in columns:
            col_name = col["name"]
            if col_name == "id" or (col_name.endswith("_id") and col_name != "id"):
                if col["cardinality"] == len(records):
                    primary_keys.append(col_name)
        if not primary_keys:
            for col in columns:
                col_name = col["name"]
                if col_name == f"{table_name}_id" or col_name == "id":
                    primary_keys.append(col_name)
                    break

    meta_foreign_keys = table_meta.get("foreign_keys") if isinstance(table_meta, dict) else None
    if isinstance(meta_foreign_keys, list) and meta_foreign_keys:
        foreign_keys = []
        for fk in meta_foreign_keys:
            if isinstance(fk, dict) and fk.get("column") and fk.get("references"):
                foreign_keys.append({
                    "column": str(fk["column"]),
                    "references": str(fk["references"]),
                })
    else:
        foreign_keys = []
        for col in columns:
            col_name = col["name"]
            if col_name.endswith("_id") and col_name not in primary_keys:
                ref_table = col_name.replace("_id", "")
                if ref_table in tables_list:
                    foreign_keys.append({
                        "column": col_name,
                        "references": f"{schema}.{ref_table}({ref_table}_id)",
                    })

    findings: List[Dict[str, Any]] = []

    if not primary_keys:
        findings.append({
            "severity": "warning",
            "check": "missing_primary_key",
            "table": table_name,
            "message": f"Table '{table_name}' has no identified primary key",
        })

    for col in columns:
        if col["cardinality"] and col["cardinality"] <= 10 and col["cardinality"] > 0:
            if col["cardinality"] < len(records) * 0.1:
                findings.append({
                    "severity": "info",
                    "check": "controlled_value_candidates",
                    "table": table_name,
                    "column": col["name"],
                    "message": (
                        f"Column '{col['name']}' has low cardinality ({col['cardinality']} distinct values), "
                        "may be a controlled value"
                    ),
                })

    for col in columns:
        if col["nullable"] and col["null_count"] == 0 and len(records) > 0:
            findings.append({
                "severity": "info",
                "check": "nullable_but_never_null",
                "table": table_name,
                "column": col["name"],
                "message": f"Column '{col['name']}' is nullable but contains no null values in sample",
            })

    for col in columns:
        col_name = col["name"]
        if "email" in col_name.lower():
            email_values = [str(r.get(col_name, "")) for r in records if r.get(col_name) is not None]
            invalid_emails = [e for e in email_values if e and not re.match(r"^[\w\.-]+@[\w\.-]+\.\w+$", e)]
            if invalid_emails:
                findings.append({
                    "severity": "warning",
                    "check": "format_inconsistency",
                    "table": table_name,
                    "column": col_name,
                    "message": f"Column '{col_name}' contains {len(invalid_emails)} invalid email format(s)",
                })

    delete_flags = [
        col["name"] for col in columns
        if "deleted" in col["name"].lower() or "active" in col["name"].lower()
    ]
    if not delete_flags:
        findings.append({
            "severity": "info",
            "check": "delete_management",
            "table": table_name,
            "message": f"Table '{table_name}' has no identified soft delete flag",
        })

    timestamp_cols = [col["name"] for col in columns if col["type"] == "timestamp"]
    if timestamp_cols:
        findings.append({
            "severity": "info",
            "check": "late_arriving_data",
            "table": table_name,
            "message": (
                f"Table '{table_name}' has timestamp columns: {', '.join(timestamp_cols)}. "
                "Monitor for late-arriving data."
            ),
        })

    if recommend_dialect:
        columns, _ambiguous = apply_type_recommendations(
            columns, records, recommend_dialect, from_source_metadata=from_source_metadata
        )
        if verify_types:
            columns = _verify_table_types(
                columns,
                records,
                recommend_dialect,
                base_url=base_url,
                table_name=table_name,
                fetch_live=fetch_live,
            )

    return {
        "table": table_name,
        "schema": schema,
        "columns": columns,
        "primary_keys": primary_keys,
        "foreign_keys": foreign_keys,
        "row_count": len(records),
        "data_quality": {
            "controlled_value_candidates": [f for f in findings if f["check"] == "controlled_value_candidates"],
            "nullable_but_never_null": [f for f in findings if f["check"] == "nullable_but_never_null"],
            "missing_primary_key": [f for f in findings if f["check"] == "missing_primary_key"],
            "missing_foreign_keys": [],
            "format_inconsistency": [f for f in findings if f["check"] == "format_inconsistency"],
            "range_violations": [],
            "delete_management": [f for f in findings if f["check"] == "delete_management"],
            "late_arriving_data": [f for f in findings if f["check"] == "late_arriving_data"],
            "timezone": [],
            "findings": findings,
        },
    }


def analyze_api_data(
    discovery_file: Path,
    data_dir: Path,
    base_url: str,
    *,
    table_filter: Optional[Set[str]] = None,
    recommend_dialect: Optional[str] = None,
    verify_types: bool = False,
    fetch_live: bool = False,
    merge_document: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    with open(discovery_file, "r", encoding="utf-8") as f:
        discovery = json.load(f)

    tables_list, discovery_meta_by_table = _normalize_discovery_tables(discovery.get("tables", []))
    schema_name = "dbo"

    if merge_document and table_filter:
        for t in merge_document.get("tables", []) or []:
            name = str(t.get("table") or "").strip()
            if name and name in table_filter and name not in tables_list:
                tables_list.append(name)

    if table_filter is not None:
        tables_list = [t for t in tables_list if t in table_filter]

    tables = []
    all_findings = []

    for table_name in tables_list:
        table_data = _load_table_payload(data_dir, table_name)
        entry = _process_table(
            table_name,
            table_data,
            discovery_meta_by_table.get(table_name, {}),
            schema_name,
            tables_list,
            recommend_dialect=recommend_dialect,
            verify_types=verify_types,
            base_url=base_url,
            fetch_live=fetch_live,
        )
        if not entry:
            continue
        tables.append(entry)
        all_findings.extend(entry["data_quality"]["findings"])

    severity_counts = Counter(f["severity"] for f in all_findings)
    check_counts = Counter(f["check"] for f in all_findings)
    total_rows = sum(t.get("row_count", 0) for t in tables)

    if merge_document:
        existing = merge_document.get("tables", []) or []
        replace_names = set(table_filter) if table_filter else {t["table"] for t in tables}
        tables = merge_table_entries(existing, tables, replace_table_names=replace_names)
        all_findings = []
        for t in tables:
            all_findings.extend(t.get("data_quality", {}).get("findings", []))
        severity_counts = Counter(f["severity"] for f in all_findings)
        check_counts = Counter(f["check"] for f in all_findings)
        total_rows = sum(t.get("row_count", 0) for t in tables)

    schema_document = {
        "metadata": {
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "database_url": base_url,
            "schema_filter": schema_name,
            "total_tables": len(tables),
            "total_rows": total_rows,
            "total_findings": len(all_findings),
        },
        "connection": {
            "host": None,
            "port": None,
            "database": None,
            "driver": "rest_api",
            "timezone": None,
        },
        "source_system_context": merge_document.get("source_system_context", {}) if merge_document else {
            "contacts": [],
            "delete_management_instruction": "",
            "restrictions": "",
            "late_arriving_data_manual": "",
            "volume_size_projection_manual": "",
        },
        "data_quality_summary": {
            "critical": severity_counts.get("critical", 0),
            "warning": severity_counts.get("warning", 0),
            "info": severity_counts.get("info", 0),
            "by_check": dict(check_counts),
            "constraints_found": {},
        },
        "tables": tables,
    }

    if merge_document:
        for key in ("metadata", "connection", "concept_registry"):
            if merge_document.get(key):
                merged = dict(schema_document.get(key, {}))
                merged.update({k: v for k, v in merge_document[key].items() if k != "generated_at"})
                if key == "metadata":
                    merged["generated_at"] = schema_document["metadata"]["generated_at"]
                    merged["total_tables"] = len(tables)
                    merged["total_rows"] = total_rows
                    merged["total_findings"] = len(all_findings)
                schema_document[key] = merged

    if recommend_dialect:
        schema_document.setdefault("metadata", {})["recommend_types_dialect"] = recommend_dialect

    return schema_document


def main() -> int:
    parser = argparse.ArgumentParser(description="Analyze API data and generate schema.json")
    parser.add_argument("--discovery", required=True, help="Path to API discovery JSON file")
    parser.add_argument("--data-dir", required=True, help="Directory containing downloaded API data files")
    parser.add_argument("--base-url", required=True, help="Base URL of the API")
    parser.add_argument("--output", required=True, help="Output schema.json file path")
    parser.add_argument(
        "--recommend-types",
        choices=sorted(SUPPORTED_DIALECTS),
        help="Overwrite column types with destination DB types for this dialect",
    )
    parser.add_argument(
        "--merge-into",
        metavar="PATH",
        help="Merge results into an existing schema.json instead of replacing it",
    )
    parser.add_argument(
        "--tables",
        help="Comma-separated table names to process (incremental mode)",
    )
    parser.add_argument(
        "--detect-changes",
        action="store_true",
        help="With --merge-into: auto-select tables needing type work",
    )
    parser.add_argument(
        "--verify-types",
        action="store_true",
        help="Run final verification pass per table (requires --recommend-types)",
    )
    parser.add_argument(
        "--fetch-live",
        action="store_true",
        help="Fetch live API samples during verification for uncertain columns",
    )
    parser.add_argument(
        "--type-checklist",
        metavar="PATH",
        help="Write type enrichment checklist for AI review of ambiguous columns",
    )
    args = parser.parse_args()

    discovery_file = Path(args.discovery)
    data_dir = Path(args.data_dir)
    output_file = Path(args.output)

    if not discovery_file.exists():
        print(f"Error: Discovery file not found: {discovery_file}")
        return 1
    if not data_dir.exists():
        print(f"Error: Data directory not found: {data_dir}")
        return 1

    merge_document = None
    table_filter: Optional[Set[str]] = None

    if args.merge_into:
        merge_path = Path(args.merge_into)
        if not merge_path.exists():
            print(f"Error: Merge target not found: {merge_path}")
            return 1
        with open(merge_path, "r", encoding="utf-8") as f:
            merge_document = json.load(f)

    if args.tables:
        table_filter = {t.strip() for t in args.tables.split(",") if t.strip()}
    elif args.detect_changes and merge_document:
        table_filter = set(detect_tables_needing_types(merge_document))
        if not table_filter:
            print("No tables need type work.")
            return 0
        print(f"Detected tables needing types: {', '.join(sorted(table_filter))}")

    if args.fetch_live and not args.verify_types:
        print("Warning: --fetch-live has no effect without --verify-types")

    schema_document = analyze_api_data(
        discovery_file,
        data_dir,
        args.base_url,
        table_filter=table_filter,
        recommend_dialect=args.recommend_types,
        verify_types=args.verify_types and bool(args.recommend_types),
        fetch_live=args.fetch_live and bool(args.recommend_types),
        merge_document=merge_document,
    )

    output_file.parent.mkdir(parents=True, exist_ok=True)
    with open(output_file, "w", encoding="utf-8") as f:
        json.dump(schema_document, f, indent=2, default=str)

    print(f"Generated schema.json: {output_file}")
    print(f"  Tables: {len(schema_document['tables'])}")
    print(f"  Total rows: {schema_document['metadata']['total_rows']}")
    print(f"  Findings: {schema_document['metadata']['total_findings']}")
    if args.recommend_types:
        print(f"  Recommend types dialect: {args.recommend_types}")

    if args.type_checklist and args.recommend_types:
        checklist_path = Path(args.type_checklist)
        checklist_path.parent.mkdir(parents=True, exist_ok=True)
        from build_type_enrichment_checklist import build_type_checklist  # noqa: WPS433

        checklist = build_type_checklist(schema_document, str(output_file), args.recommend_types)
        with open(checklist_path, "w", encoding="utf-8") as f:
            json.dump(checklist, f, indent=2, default=str)
        print(f"  Type checklist: {checklist_path} ({checklist['summary']['pending_items']} pending)")

    return 0


if __name__ == "__main__":
    sys.exit(main())
