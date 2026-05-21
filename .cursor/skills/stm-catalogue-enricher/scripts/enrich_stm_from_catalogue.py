#!/usr/bin/env python3
"""Fetch table/column metadata from OpenMetadata and export as a flat CSV catalogue."""

from __future__ import annotations

import argparse
import csv
import os
import sys
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import requests
from dotenv import load_dotenv

_PROJECT_ROOT = Path(__file__).resolve().parents[4]
_SCRIPT_DIR = Path(__file__).resolve().parent
if str(_SCRIPT_DIR) not in sys.path:
    sys.path.insert(0, str(_SCRIPT_DIR))

from om_auth import om_api_url, om_bearer_headers  # noqa: E402

load_dotenv()

DEFAULT_SCHEMA_FQN = "snowflake_fivetran.DRIP_DATA_INTELLIGENCE.BRONZE_ERP__DBO"
DEFAULT_CATALOGUE_OUT = _PROJECT_ROOT / ".cursor" / "flat" / "data_catalogue.csv"


# ---------------------------------------------------------------------------
# OpenMetadata API helpers
# ---------------------------------------------------------------------------

def _api_url(path: str) -> str:
    return om_api_url(path)


def _headers() -> dict[str, str]:
    return om_bearer_headers(user_agent="enrich-stm-from-catalogue")


def _get(endpoint: str, params: dict | None = None) -> Any:
    for attempt in range(2):
        try:
            r = requests.get(_api_url(endpoint), headers=_headers(), params=params, timeout=(10, 30))
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
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    parser = argparse.ArgumentParser(description="Fetch OpenMetadata catalogue and export to CSV")
    parser.add_argument("--schema-fqn", default=DEFAULT_SCHEMA_FQN)
    parser.add_argument("--catalogue-out", type=Path, default=DEFAULT_CATALOGUE_OUT)
    args = parser.parse_args()

    print(f"Fetching catalogue from OpenMetadata: {args.schema_fqn}")
    catalogue = fetch_catalogue(args.schema_fqn)
    print(f"  {len(catalogue)} tables, {sum(len(t.columns) for t in catalogue.values())} columns")

    export_catalogue_csv(catalogue, args.schema_fqn, args.catalogue_out)
    print("Done.")


if __name__ == "__main__":
    main()
