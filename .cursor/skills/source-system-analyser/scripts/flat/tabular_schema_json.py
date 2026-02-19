#!/usr/bin/env python3
"""Convert CSV/XLSX tabular schema metadata to schema.json-like structure and back."""

from __future__ import annotations

import argparse
import csv
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


def _norm_header(name: str) -> str:
    return str(name or "").strip().lower().replace(" ", "_")


def _as_bool(value: Any, default: bool = False) -> bool:
    if value is None or value == "":
        return default
    if isinstance(value, bool):
        return value
    s = str(value).strip().lower()
    return s in {"1", "true", "t", "yes", "y"}


def _as_int(value: Any) -> int | None:
    if value is None or value == "":
        return None
    try:
        return int(float(str(value).strip()))
    except Exception:
        return None


def _validate_delimiter(delimiter: str | None) -> str | None:
    if delimiter is None:
        return None
    if len(delimiter) != 1:
        raise SystemExit("Delimiter must be a single character (for example ',', ';', '|', or '\\t').")
    return delimiter


def _detect_csv_delimiter(path: Path, fallback: str = ",") -> str:
    with path.open("r", encoding="utf-8-sig", newline="") as f:
        sample = f.read(8192)
    if not sample:
        return fallback
    try:
        dialect = csv.Sniffer().sniff(sample, delimiters=",;\t|")
        return dialect.delimiter
    except Exception:
        return fallback


def _resolve_csv_delimiter(path: Path, delimiter: str | None) -> str:
    validated = _validate_delimiter(delimiter)
    if validated is not None:
        return validated
    return _detect_csv_delimiter(path)


def _read_csv(path: Path, delimiter: str | None = None) -> list[dict[str, Any]]:
    resolved = _resolve_csv_delimiter(path, delimiter)
    with path.open("r", encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f, delimiter=resolved)
        out: list[dict[str, Any]] = []
        for row in reader:
            out.append({_norm_header(k): v for k, v in row.items()})
        return out


def _read_xlsx(path: Path, sheet_name: str | None) -> list[dict[str, Any]]:
    try:
        from openpyxl import load_workbook
    except ImportError as exc:
        raise SystemExit("Reading .xlsx requires openpyxl. Install with: pip install openpyxl") from exc

    wb = load_workbook(path, read_only=True, data_only=True)
    ws = wb[sheet_name] if sheet_name else wb[wb.sheetnames[0]]
    rows = ws.iter_rows(values_only=True)
    headers_raw = next(rows, None)
    if not headers_raw:
        return []
    headers = [_norm_header(h) for h in headers_raw]
    data: list[dict[str, Any]] = []
    for r in rows:
        if r is None:
            continue
        item = {headers[i]: r[i] for i in range(min(len(headers), len(r)))}
        if any(v not in (None, "") for v in item.values()):
            data.append(item)
    return data


def _read_tabular(path: str, sheet_name: str | None = None, delimiter: str | None = None) -> list[dict[str, Any]]:
    p = Path(path)
    if p.suffix.lower() == ".csv":
        return _read_csv(p, delimiter=delimiter)
    if p.suffix.lower() == ".xlsx":
        return _read_xlsx(p, sheet_name)
    raise SystemExit(f"Unsupported file type: {p.suffix}. Use .csv or .xlsx")


def _get(row: dict[str, Any], *keys: str, default: Any = None) -> Any:
    for k in keys:
        if k in row and row.get(k) not in (None, ""):
            return row.get(k)
    return default


def _headers(rows: list[dict[str, Any]]) -> list[str]:
    seen: set[str] = set()
    out: list[str] = []
    for r in rows:
        for k in r.keys():
            if k not in seen:
                seen.add(k)
                out.append(k)
    return out


def _suggest_role(headers: list[str]) -> dict[str, str | None]:
    s = set(headers)

    def pick(*opts: str) -> str | None:
        for o in opts:
            if o in s:
                return o
        return None

    return {
        "table": pick("table", "table_name", "entity", "object"),
        "column": pick("column", "column_name", "name", "field", "attribute"),
        "type": pick("type", "data_type", "dtype", "column_type"),
        "schema": pick("schema", "schema_name"),
        "primary_key": pick("primary_key", "pk"),
        "foreign_key": pick("foreign_key", "fk"),
    }


def _parse_columns(rows: list[dict[str, Any]], default_schema: str) -> dict[str, dict[str, Any]]:
    tables: dict[str, dict[str, Any]] = {}
    for r in rows:
        table = str(_get(r, "table", "table_name", "entity", "object", default="")).strip()
        column = str(_get(r, "column", "column_name", "name", "field", "attribute", default="")).strip()
        col_type = str(_get(r, "type", "data_type", "dtype", "column_type", default="text")).strip() or "text"
        if not table or not column:
            continue
        schema = str(_get(r, "schema", "schema_name", default=default_schema)).strip() or default_schema

        t = tables.setdefault(
            table,
            {
                "table": table,
                "schema": schema,
                "columns": [],
                "primary_keys": [],
                "foreign_keys": [],
                "row_count": _as_int(_get(r, "row_count")) or 0,
                "field_classifications": [],
                "sensitive_fields": [],
                "incremental_columns": [],
                "partition_columns": [],
                "join_candidates": [],
                "cdc_enabled": _as_bool(_get(r, "cdc_enabled"), False),
                "has_primary_key": False,
                "has_foreign_keys": False,
                "has_sensitive_fields": False,
                "data_quality": {
                    "findings": [],
                    "summary": {"critical": 0, "warning": 0, "info": 0},
                    "constraints_found": {},
                },
            },
        )

        is_incremental = _as_bool(_get(r, "is_incremental", "incremental"), False)
        col = {
            "name": column,
            "type": col_type,
            "nullable": _as_bool(_get(r, "nullable", "is_nullable"), True),
            "is_incremental": is_incremental,
            "cardinality": _as_int(_get(r, "cardinality", "distinct_count")),
            "null_count": _as_int(_get(r, "null_count", "nulls")),
            "data_range": {
                "min": None if _get(r, "min", "min_value") in (None, "") else str(_get(r, "min", "min_value")),
                "max": None if _get(r, "max", "max_value") in (None, "") else str(_get(r, "max", "max_value")),
            },
            "data_category": _get(r, "data_category", "category") or None,
        }
        t["columns"].append(col)

        if _as_bool(_get(r, "primary_key", "pk"), False):
            t["primary_keys"].append(column)
        if is_incremental:
            t["incremental_columns"].append(column)

        fk = str(_get(r, "foreign_key", "fk", default="")).strip()
        if fk:
            t["foreign_keys"].append({"column": column, "references": fk})

    for t in tables.values():
        t["primary_keys"] = sorted(set(t["primary_keys"]))
        t["incremental_columns"] = sorted(set(t["incremental_columns"]))
        # dedupe foreign keys by (column, references)
        seen = set()
        fks = []
        for fk in t["foreign_keys"]:
            key = (fk.get("column"), fk.get("references"))
            if key not in seen:
                seen.add(key)
                fks.append(fk)
        t["foreign_keys"] = fks
        t["has_primary_key"] = bool(t["primary_keys"])
        t["has_foreign_keys"] = bool(t["foreign_keys"])
    return tables


def _merge_table_rows(tables: dict[str, dict[str, Any]], rows: list[dict[str, Any]], default_schema: str) -> None:
    for r in rows:
        table = str(_get(r, "table", "table_name", "entity", "object", default="")).strip()
        if not table:
            continue
        t = tables.setdefault(
            table,
            {
                "table": table,
                "schema": str(_get(r, "schema", "schema_name", default=default_schema)).strip() or default_schema,
                "columns": [],
                "primary_keys": [],
                "foreign_keys": [],
                "row_count": 0,
                "field_classifications": [],
                "sensitive_fields": [],
                "incremental_columns": [],
                "partition_columns": [],
                "join_candidates": [],
                "cdc_enabled": False,
                "has_primary_key": False,
                "has_foreign_keys": False,
                "has_sensitive_fields": False,
                "data_quality": {
                    "findings": [],
                    "summary": {"critical": 0, "warning": 0, "info": 0},
                    "constraints_found": {},
                },
            },
        )
        if _get(r, "schema", "schema_name") not in (None, ""):
            t["schema"] = str(_get(r, "schema", "schema_name")).strip()
        if _as_int(_get(r, "row_count", "rows")) is not None:
            t["row_count"] = _as_int(_get(r, "row_count", "rows")) or 0
        if _get(r, "cdc_enabled", "cdc") not in (None, ""):
            t["cdc_enabled"] = _as_bool(_get(r, "cdc_enabled", "cdc"), False)


def cmd_inspect(args: argparse.Namespace) -> None:
    out: dict[str, Any] = {"files": []}

    def add_file(path: str, sheet: str | None, kind: str, delimiter: str | None = None) -> None:
        rows = _read_tabular(path, sheet, delimiter=delimiter)
        p = Path(path)
        headers = _headers(rows)
        entry = {
            "kind": kind,
            "path": path,
            "sheet": sheet,
            "row_count": len(rows),
            "headers": headers,
            "suggested_mapping": _suggest_role(headers),
            "sample_rows": rows[: args.sample_size],
        }
        if p.suffix.lower() == ".csv":
            entry["delimiter"] = _resolve_csv_delimiter(p, delimiter)
        out["files"].append(entry)

    add_file(args.columns_file, args.columns_sheet, "columns", delimiter=args.columns_delimiter)
    if args.tables_file:
        add_file(args.tables_file, args.tables_sheet, "tables", delimiter=args.tables_delimiter)

    payload = json.dumps(out, indent=2, default=str)
    if args.output:
        Path(args.output).write_text(payload, encoding="utf-8")
        print(f"Wrote {args.output}")
    else:
        print(payload)


def cmd_to_json(args: argparse.Namespace) -> None:
    col_rows = _read_tabular(args.columns_file, args.columns_sheet, delimiter=args.columns_delimiter)
    tables = _parse_columns(col_rows, args.default_schema)

    if args.tables_file:
        table_rows = _read_tabular(args.tables_file, args.tables_sheet, delimiter=args.tables_delimiter)
        _merge_table_rows(tables, table_rows, args.default_schema)

    ordered_tables = [tables[k] for k in sorted(tables.keys())]
    out = {
        "metadata": {
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "database_url": None,
            "schema_filter": args.default_schema,
            "total_tables": len(ordered_tables),
            "total_rows": sum(int(t.get("row_count") or 0) for t in ordered_tables),
            "total_findings": 0,
        },
        "connection": {
            "host": None,
            "port": None,
            "database": None,
            "driver": None,
            "timezone": None,
        },
        "data_quality_summary": {
            "critical": 0,
            "warning": 0,
            "info": 0,
            "by_check": {},
            "constraints_found": {},
        },
        "tables": ordered_tables,
    }

    Path(args.output).write_text(json.dumps(out, indent=2), encoding="utf-8")
    print(f"Wrote {args.output} (tables={len(ordered_tables)})")


def cmd_from_json(args: argparse.Namespace) -> None:
    src = json.loads(Path(args.schema_json).read_text(encoding="utf-8"))
    tables = src.get("tables", [])

    col_headers = [
        "table",
        "schema",
        "column",
        "type",
        "nullable",
        "is_incremental",
        "primary_key",
        "foreign_key",
        "cardinality",
        "null_count",
        "min",
        "max",
        "data_category",
    ]
    table_headers = ["table", "schema", "row_count", "cdc_enabled"]

    col_rows: list[dict[str, Any]] = []
    table_rows: list[dict[str, Any]] = []

    for t in tables:
        tname = t.get("table")
        schema = t.get("schema")
        pk_set = set(t.get("primary_keys") or [])
        fk_map = {fk.get("column"): fk.get("references") for fk in (t.get("foreign_keys") or [])}

        table_rows.append(
            {
                "table": tname,
                "schema": schema,
                "row_count": t.get("row_count"),
                "cdc_enabled": t.get("cdc_enabled"),
            }
        )

        for c in t.get("columns") or []:
            rng = c.get("data_range") or {}
            col_rows.append(
                {
                    "table": tname,
                    "schema": schema,
                    "column": c.get("name"),
                    "type": c.get("type"),
                    "nullable": c.get("nullable"),
                    "is_incremental": c.get("is_incremental"),
                    "primary_key": c.get("name") in pk_set,
                    "foreign_key": fk_map.get(c.get("name"), ""),
                    "cardinality": c.get("cardinality"),
                    "null_count": c.get("null_count"),
                    "min": rng.get("min"),
                    "max": rng.get("max"),
                    "data_category": c.get("data_category"),
                }
            )

    with Path(args.columns_out).open("w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=col_headers)
        w.writeheader()
        w.writerows(col_rows)

    with Path(args.tables_out).open("w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=table_headers)
        w.writeheader()
        w.writerows(table_rows)

    print(f"Wrote {args.columns_out} and {args.tables_out}")


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description=__doc__)
    sub = p.add_subparsers(dest="cmd", required=True)

    p_inspect = sub.add_parser("inspect", help="Inspect tabular file columns/rows before conversion")
    p_inspect.add_argument("--columns-file", required=True, help="Path to CSV/XLSX with column-level metadata")
    p_inspect.add_argument("--columns-sheet", default=None, help="Sheet name when --columns-file is .xlsx")
    p_inspect.add_argument("--columns-delimiter", default=None, help="CSV delimiter for --columns-file (auto-detected when omitted)")
    p_inspect.add_argument("--tables-file", default=None, help="Optional CSV/XLSX with table-level metadata")
    p_inspect.add_argument("--tables-sheet", default=None, help="Sheet name when --tables-file is .xlsx")
    p_inspect.add_argument("--tables-delimiter", default=None, help="CSV delimiter for --tables-file (auto-detected when omitted)")
    p_inspect.add_argument("--sample-size", type=int, default=5, help="Number of sample rows per file")
    p_inspect.add_argument("--output", default=None, help="Optional output JSON path")
    p_inspect.set_defaults(func=cmd_inspect)

    p_to = sub.add_parser("to-json", help="Convert tabular schema files to schema.json-like output")
    p_to.add_argument("--columns-file", required=True, help="Path to CSV/XLSX with column-level metadata")
    p_to.add_argument("--columns-sheet", default=None, help="Sheet name when --columns-file is .xlsx")
    p_to.add_argument("--columns-delimiter", default=None, help="CSV delimiter for --columns-file (auto-detected when omitted)")
    p_to.add_argument("--tables-file", default=None, help="Optional CSV/XLSX with table-level metadata")
    p_to.add_argument("--tables-sheet", default=None, help="Sheet name when --tables-file is .xlsx")
    p_to.add_argument("--tables-delimiter", default=None, help="CSV delimiter for --tables-file (auto-detected when omitted)")
    p_to.add_argument("--output", required=True, help="Output JSON path")
    p_to.add_argument("--default-schema", default="public", help="Schema name used when missing in source")
    p_to.set_defaults(func=cmd_to_json)

    p_from = sub.add_parser("from-json", help="Export schema.json to CSV templates")
    p_from.add_argument("--schema-json", required=True, help="Input schema.json path")
    p_from.add_argument("--columns-out", required=True, help="Output columns CSV path")
    p_from.add_argument("--tables-out", required=True, help="Output tables CSV path")
    p_from.set_defaults(func=cmd_from_json)

    return p


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()
    args.func(args)


if __name__ == "__main__":
    main()
