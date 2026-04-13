#!/usr/bin/env python3
"""Enrich generated STM markdown files with source-side metadata from OpenMetadata."""

from __future__ import annotations

import argparse
import base64
import csv
import json
import os
import re
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import requests
from dotenv import load_dotenv

load_dotenv()

_PROJECT_ROOT = Path(__file__).resolve().parents[4]
DEFAULT_STM_DIR = _PROJECT_ROOT / "stm" / "output"
DEFAULT_SCHEMA_FQN = "snowflake_fivetran.DRIP_DATA_INTELLIGENCE.BRONZE_ERP__DBO"
DEFAULT_CATALOGUE_OUT = _PROJECT_ROOT / ".cursor" / "flat" / "data_catalogue.csv"

API_VERSION_PREFIX = "v1"
_TOKEN_CACHE: dict[str, Any] = {"token": None}


# ---------------------------------------------------------------------------
# OpenMetadata API helpers (minimal, self-contained)
# ---------------------------------------------------------------------------

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
                raise RuntimeError(f"GET {endpoint} failed: {e}") from e
            time.sleep(1)


# ---------------------------------------------------------------------------
# Catalogue builder
# ---------------------------------------------------------------------------

@dataclass
class CatalogueColumn:
    table_name: str
    column_name: str
    data_type: str
    max_length: str
    precision_value: str
    scale_value: str
    is_nullable: str
    description: str
    is_key: str


@dataclass
class CatalogueTable:
    table_name: str
    columns: dict[str, CatalogueColumn] = field(default_factory=dict)
    has_incremental: bool = False
    incremental_column: str = ""


def fetch_catalogue(schema_fqn: str) -> dict[str, CatalogueTable]:
    resp = _get("tables", params={
        "databaseSchema": schema_fqn,
        "fields": "columns,tags,tableConstraints",
        "limit": 500,
    })
    tables_raw = resp.get("data", [])
    catalogue: dict[str, CatalogueTable] = {}

    for t in tables_raw:
        table_name = t.get("name", "")
        ct = CatalogueTable(table_name=table_name)

        pk_cols: set[str] = set()
        for constraint in t.get("tableConstraints", []):
            if (constraint.get("constraintType") or "").upper() == "PRIMARY_KEY":
                for c in constraint.get("columns", []):
                    pk_cols.add(c.upper())

        for col in t.get("columns", []):
            col_name = col.get("name", "")
            if col_name.startswith("_FIVETRAN"):
                continue
            data_type = col.get("dataTypeDisplay") or col.get("dataType") or ""
            nullable_constraint = (col.get("constraint") or "").upper()
            is_nullable = "1" if nullable_constraint in ("NULL", "") else "0"
            if col_name.upper() in pk_cols:
                is_nullable = "0"
            is_key = "Yes" if col_name.upper() in pk_cols or nullable_constraint in ("PRIMARY_KEY", "UNIQUE") else ""

            data_length = col.get("dataLength")
            max_length = str(data_length) if data_length and data_length > 1 else ""
            if not max_length and col.get("precision"):
                max_length = str(col["precision"])

            cc = CatalogueColumn(
                table_name=table_name,
                column_name=col_name,
                data_type=data_type,
                max_length=max_length,
                precision_value=str(col.get("precision") or ""),
                scale_value=str(col.get("scale")) if col.get("scale") is not None else "",
                is_nullable=is_nullable,
                description=(col.get("description") or "").replace("\n", " ").strip(),
                is_key=is_key,
            )
            ct.columns[col_name.upper()] = cc

            if col_name.upper() in ("UPDATED_AT", "MODIFIED_AT", "LAST_MODIFIED"):
                ct.has_incremental = True
                ct.incremental_column = col_name

        catalogue[table_name.upper()] = ct

    return catalogue


def export_catalogue_csv(catalogue: dict[str, CatalogueTable], schema_fqn: str, output_path: Path) -> None:
    parts = schema_fqn.split(".")
    schema_name = parts[-1] if parts else schema_fqn
    output_path.parent.mkdir(parents=True, exist_ok=True)

    fieldnames = [
        "id", "full_object_name", "column_name", "data_type", "max_length",
        "precision_value", "scale_value", "is_nullable", "description",
        "is_key", "schema_name", "table_name", "created_at",
    ]
    row_id = 0
    now_str = datetime.now(tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    with open(output_path, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for tname in sorted(catalogue):
            ct = catalogue[tname]
            for cname in sorted(ct.columns):
                cc = ct.columns[cname]
                row_id += 1
                writer.writerow({
                    "id": row_id,
                    "full_object_name": f"{schema_name}.{ct.table_name}",
                    "column_name": cc.column_name,
                    "data_type": cc.data_type,
                    "max_length": cc.max_length,
                    "precision_value": cc.precision_value,
                    "scale_value": cc.scale_value,
                    "is_nullable": cc.is_nullable,
                    "description": cc.description,
                    "is_key": cc.is_key,
                    "schema_name": schema_name,
                    "table_name": ct.table_name,
                    "created_at": now_str,
                })
    print(f"Exported {row_id} rows to {output_path}")


# ---------------------------------------------------------------------------
# Markdown table parsing / rewriting
# ---------------------------------------------------------------------------

_PIPE_RE = re.compile(r"(?<!\\)\|")


def _parse_md_row(line: str) -> list[str]:
    """Split a markdown table row into cell values."""
    stripped = line.strip()
    if stripped.startswith("|"):
        stripped = stripped[1:]
    if stripped.endswith("|"):
        stripped = stripped[:-1]
    return [c.strip() for c in _PIPE_RE.split(stripped)]


def _build_md_row(cells: list[str]) -> str:
    return "| " + " | ".join(cells) + " |"


def _is_separator_row(line: str) -> bool:
    return bool(re.match(r"^\|[\s\-:|]+\|$", line.strip()))


def _find_section(lines: list[str], header_pattern: str) -> int | None:
    for i, line in enumerate(lines):
        if re.search(header_pattern, line, re.IGNORECASE):
            return i
    return None


def _find_table_block(lines: list[str], start: int) -> tuple[int, int] | None:
    """Find the header row and end of a markdown table starting from `start`."""
    header_idx = None
    for i in range(start, min(start + 10, len(lines))):
        if lines[i].strip().startswith("|") and not _is_separator_row(lines[i]):
            header_idx = i
            break
    if header_idx is None:
        return None
    end_idx = header_idx + 1
    while end_idx < len(lines) and lines[end_idx].strip().startswith("|"):
        end_idx += 1
    return header_idx, end_idx


# ---------------------------------------------------------------------------
# STM enrichment logic
# ---------------------------------------------------------------------------

def _infer_table_name_from_stm(lines: list[str]) -> str:
    """Extract the target table name from the field-level mapping header row values."""
    sec = _find_section(lines, r"##\s+7\.\s+Field.Level Mapping")
    if sec is None:
        return ""
    block = _find_table_block(lines, sec)
    if block is None:
        return ""
    header_idx, end_idx = block
    if end_idx - header_idx < 3:
        return ""
    first_data = _parse_md_row(lines[header_idx + 2])
    if first_data:
        return first_data[0]
    return ""


def enrich_field_mapping(
    lines: list[str],
    catalogue: dict[str, CatalogueTable],
) -> list[str]:
    sec = _find_section(lines, r"##\s+7\.\s+Field.Level Mapping")
    if sec is None:
        return lines
    block = _find_table_block(lines, sec)
    if block is None:
        return lines

    header_idx, end_idx = block
    header_cells = _parse_md_row(lines[header_idx])

    col_idx: dict[str, int] = {}
    for i, h in enumerate(header_cells):
        normalized = h.lower().strip().replace("*", "")
        col_idx[normalized] = i

    src_table_i = col_idx.get("source table")
    src_col_i = col_idx.get("source column(s)")
    target_table_i = col_idx.get("target table")
    target_col_i = col_idx.get("target column")
    transform_i = col_idx.get("transformation / business rule")

    if src_table_i is None or src_col_i is None or target_col_i is None:
        return lines

    target_table_name = ""
    if target_table_i is not None:
        for row_i in range(header_idx + 2, end_idx):
            cells = _parse_md_row(lines[row_i])
            if _is_separator_row(lines[row_i]):
                continue
            if target_table_i < len(cells) and cells[target_table_i].strip():
                target_table_name = cells[target_table_i].strip()
                break

    cat_table = catalogue.get(target_table_name.upper())
    if cat_table is None:
        return lines

    new_lines = list(lines)
    for row_i in range(header_idx + 2, end_idx):
        if _is_separator_row(new_lines[row_i]):
            continue
        cells = _parse_md_row(new_lines[row_i])
        while len(cells) < len(header_cells):
            cells.append("")

        target_col = cells[target_col_i].strip() if target_col_i < len(cells) else ""
        cat_col = cat_table.columns.get(target_col.upper())
        if cat_col is None:
            continue

        if src_table_i < len(cells) and not cells[src_table_i].strip():
            cells[src_table_i] = cat_table.table_name
        if src_col_i < len(cells) and not cells[src_col_i].strip():
            cells[src_col_i] = cat_col.column_name

        if transform_i is not None and transform_i < len(cells) and not cells[transform_i].strip():
            target_dtype = cells[col_idx.get("data type", -1)].strip() if "data type" in col_idx else ""
            src_dtype = cat_col.data_type
            if target_dtype and src_dtype and target_dtype.lower() != src_dtype.lower():
                cells[transform_i] = f"Source type: {src_dtype}"

        new_lines[row_i] = _build_md_row(cells)

    return new_lines


def enrich_dq_rules(
    lines: list[str],
    catalogue: dict[str, CatalogueTable],
    target_table_name: str,
) -> list[str]:
    sec = _find_section(lines, r"##\s+9\.\s+Data Quality")
    if sec is None:
        return lines
    block = _find_table_block(lines, sec)
    if block is None:
        return lines

    header_idx, end_idx = block
    cat_table = catalogue.get(target_table_name.upper())
    if cat_table is None:
        return lines

    existing_data_rows = []
    for row_i in range(header_idx + 2, end_idx):
        if not _is_separator_row(lines[row_i]):
            cells = _parse_md_row(lines[row_i])
            has_content = any(c.strip() for c in cells)
            if has_content:
                existing_data_rows.append(lines[row_i])

    if existing_data_rows:
        return lines

    dq_rows: list[str] = []
    rule_id = 0

    for cname in sorted(cat_table.columns):
        cc = cat_table.columns[cname]
        if cc.is_key == "Yes":
            rule_id += 1
            dq_rows.append(_build_md_row([
                f"DQ{rule_id}",
                f"{cc.column_name} must not be NULL (primary key)",
                "NOT NULL",
                f"{cc.column_name} IS NOT NULL",
                "Reject record",
                "",
            ]))
            rule_id += 1
            dq_rows.append(_build_md_row([
                f"DQ{rule_id}",
                f"{cc.column_name} must be unique",
                "Uniqueness",
                f"COUNT(DISTINCT {cc.column_name}) = COUNT(*)",
                "Reject record",
                "",
            ]))

    fk_pattern = re.compile(r"referencing|foreign key|references", re.IGNORECASE)
    for cname in sorted(cat_table.columns):
        cc = cat_table.columns[cname]
        if cc.is_key != "Yes" and fk_pattern.search(cc.description):
            rule_id += 1
            dq_rows.append(_build_md_row([
                f"DQ{rule_id}",
                f"{cc.column_name} referential integrity check",
                "Referential Integrity",
                f"All {cc.column_name} values exist in referenced parent table",
                "Flag / quarantine",
                "",
            ]))

    if not dq_rows:
        return lines

    new_lines = list(lines[:end_idx - 1])
    for dr in dq_rows:
        new_lines.append(dr)
    new_lines.extend(lines[end_idx - 1:])
    return new_lines


def enrich_load_strategy(
    lines: list[str],
    catalogue: dict[str, CatalogueTable],
    target_table_name: str,
) -> list[str]:
    sec = _find_section(lines, r"##\s+10\.\s+Load Strategy")
    if sec is None:
        return lines
    block = _find_table_block(lines, sec)
    if block is None:
        return lines

    header_idx, end_idx = block
    cat_table = catalogue.get(target_table_name.upper())
    if cat_table is None:
        return lines

    existing_data_rows = []
    for row_i in range(header_idx + 2, end_idx):
        if not _is_separator_row(lines[row_i]):
            cells = _parse_md_row(lines[row_i])
            has_content = any(c.strip() for c in cells)
            if has_content:
                existing_data_rows.append(lines[row_i])

    if existing_data_rows:
        return lines

    if cat_table.has_incremental:
        load_row = _build_md_row([
            "Incremental",
            f"Delta load using {cat_table.incremental_column}",
            "",
            "",
            "",
            "",
        ])
    else:
        load_row = _build_md_row([
            "Full",
            "Full table refresh",
            "",
            "",
            "",
            "",
        ])

    new_lines = list(lines[:end_idx - 1])
    new_lines.append(load_row)
    new_lines.extend(lines[end_idx - 1:])
    return new_lines


def enrich_stm_file(filepath: Path, catalogue: dict[str, CatalogueTable]) -> bool:
    text = filepath.read_text(encoding="utf-8")
    lines = text.splitlines()

    target_table = _infer_table_name_from_stm(lines)
    if not target_table:
        return False

    if target_table.upper() not in catalogue:
        return False

    lines = enrich_field_mapping(lines, catalogue)
    lines = enrich_dq_rules(lines, catalogue, target_table)
    lines = enrich_load_strategy(lines, catalogue, target_table)

    new_text = "\n".join(lines) + "\n"
    if new_text != text:
        filepath.write_text(new_text, encoding="utf-8")
        return True
    return False


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    parser = argparse.ArgumentParser(description="Enrich STM files from OpenMetadata catalogue")
    parser.add_argument("--stm-dir", type=Path, default=DEFAULT_STM_DIR)
    parser.add_argument("--schema-fqn", default=DEFAULT_SCHEMA_FQN)
    parser.add_argument("--catalogue-out", type=Path, default=DEFAULT_CATALOGUE_OUT)
    args = parser.parse_args()

    print(f"Fetching catalogue from OpenMetadata: {args.schema_fqn}")
    catalogue = fetch_catalogue(args.schema_fqn)
    print(f"  {len(catalogue)} tables, {sum(len(t.columns) for t in catalogue.values())} columns")

    export_catalogue_csv(catalogue, args.schema_fqn, args.catalogue_out)

    stm_files = sorted(args.stm_dir.glob("*-stm.md"))
    print(f"\nEnriching {len(stm_files)} STM files in {args.stm_dir}")

    enriched = 0
    skipped = 0
    for stm_file in stm_files:
        if enrich_stm_file(stm_file, catalogue):
            print(f"  ENRICHED  {stm_file.name}")
            enriched += 1
        else:
            print(f"  SKIPPED   {stm_file.name}")
            skipped += 1

    print(f"\nDone: {enriched} enriched, {skipped} skipped")


if __name__ == "__main__":
    main()
