#!/usr/bin/env python3
"""
Create MSSQL app schema from schema.json and copy data from PostgreSQL.
Ensures MSSQL has the same tables and data as schema.json (PostgreSQL source).
"""
import json
import os
import re
import sys
from pathlib import Path

from sqlalchemy import create_engine, text

_scripts = Path(__file__).resolve().parent
if str(_scripts) not in sys.path:
    sys.path.insert(0, str(_scripts))
from keyvault_loader import load_env

load_env()
DATABASE_URL = os.environ.get("DATABASE_URL")
MSSQL_URL = os.environ.get("MSSQL_URL")
if not DATABASE_URL or not MSSQL_URL:
    raise SystemExit("DATABASE_URL and MSSQL_URL must be set (in .env or Key Vault)")

SCHEMA_JSON = Path(__file__).resolve().parent.parent / "schema.json"
with open(SCHEMA_JSON) as f:
    schema = json.load(f)

# PostgreSQL type -> MSSQL type
PG_TO_MSSQL = {
    "bigint": "BIGINT",
    "integer": "INT",
    "smallint": "SMALLINT",
    "text": "NVARCHAR(MAX)",
    "boolean": "BIT",
    "date": "DATE",
    "timestamp": "DATETIME2",
    "timestamp without time zone": "DATETIME2",
    "timestamp with time zone": "DATETIME2",
}


def pg_type_to_mssql(pg_type: str) -> str:
    pg_type = (pg_type or "").lower().strip()
    if pg_type.startswith("numeric(") or pg_type.startswith("decimal("):
        m = re.match(r"numeric\((\d+),(\d+)\)", pg_type) or re.match(r"decimal\((\d+),(\d+)\)", pg_type)
        if m:
            return f"DECIMAL({m.group(1)},{m.group(2)})"
    return PG_TO_MSSQL.get(pg_type, "NVARCHAR(MAX)")


def build_ddl(unique_constraints: list) -> str:
    tables = schema["tables"]
    # Order: no-FK tables first, then by dependency
    order = ["stores", "suppliers", "customers", "products", "employees", "purchase_orders", "inventory", "purchase_order_items", "sales_orders", "sales_order_items"]
    ordered = {t["table"]: t for t in tables}
    tables_ordered = [ordered[n] for n in order if n in ordered]

    # Columns that are in UNIQUE constraints (NVARCHAR(MAX) cannot be indexed in MSSQL)
    unique_cols = set()
    for _, tname, col_list in unique_constraints:
        for col in col_list.split(","):
            unique_cols.add((tname, col.strip()))

    parts = [
        "-- MSSQL DDL from schema.json (same structure as PostgreSQL)",
        "IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = 'app') EXEC('CREATE SCHEMA [app]');",
        "",
    ]

    for t in tables_ordered:
        name = t["table"]
        cols = []
        pk = t.get("primary_keys", [])
        for c in t["columns"]:
            mssql_type = pg_type_to_mssql(c.get("type", "text"))
            # NVARCHAR(MAX) cannot be used in UNIQUE index; use NVARCHAR(450) for key columns
            if (name, c["name"]) in unique_cols and mssql_type == "NVARCHAR(MAX)":
                mssql_type = "NVARCHAR(450)"
            is_identity = c["name"] in pk and c.get("is_incremental", False)
            if is_identity and mssql_type in ("BIGINT", "INT", "SMALLINT"):
                mssql_type = f"{mssql_type} IDENTITY(1,1)"
            null = "NULL" if c.get("nullable", False) else "NOT NULL"
            cols.append(f"    [{c['name']}] {mssql_type} {null}")
        col_def = ",\n".join(cols)
        pk_constraint = f",\n    PRIMARY KEY ({', '.join('[' + k + ']' for k in pk)})" if pk else ""
        parts.append(f"IF OBJECT_ID('[app].[{name}]', 'U') IS NOT NULL DROP TABLE [app].[{name}];")
        parts.append(f"CREATE TABLE [app].[{name}] (\n{col_def}{pk_constraint}\n);")
        parts.append("")

    # Foreign keys
    for t in tables_ordered:
        for fk in t.get("foreign_keys", []):
            col = fk["column"]
            ref = fk["references"]
            ref_table, ref_col = ref.split(".")
            fk_name = f"fk_{t['table']}_{col}"
            parts.append(f"ALTER TABLE [app].[{t['table']}] ADD CONSTRAINT [{fk_name}] FOREIGN KEY ([{col}]) REFERENCES [app].[{ref_table}] ([{ref_col}]);")
        if t.get("foreign_keys"):
            parts.append("")

    # Unique constraints (from PostgreSQL)
    for cname, tname, col_list in unique_constraints:
        col_list_bracketed = ", ".join(f"[{c.strip()}]" for c in col_list.split(","))
        safe_name = re.sub(r"[^a-zA-Z0-9_]", "_", cname)[:128]
        parts.append(f"ALTER TABLE [app].[{tname}] ADD CONSTRAINT [uq_{tname}_{safe_name}] UNIQUE ({col_list_bracketed});")
    if unique_constraints:
        parts.append("")

    return "\n".join(parts)


def run_batches(engine, sql: str):
    batches = re.split(r"\s*GO\s*", sql, flags=re.IGNORECASE)
    for batch in batches:
        batch = re.sub(r"^(?:\s*--[^\n]*\n?)+", "", batch).strip()
        if not batch:
            continue
        with engine.connect() as conn:
            conn.execute(text(batch))
            conn.commit()


def fetch_pg_unique_constraints(pg_engine) -> list:
    """Fetch UNIQUE constraints from PostgreSQL public schema."""
    with pg_engine.connect() as conn:
        rows = conn.execute(text("""
            SELECT tc.constraint_name, tc.table_name,
                   string_agg(kcu.column_name, ',' ORDER BY kcu.ordinal_position) AS columns
            FROM information_schema.table_constraints tc
            JOIN information_schema.key_column_usage kcu
                ON tc.constraint_name = kcu.constraint_name AND tc.table_schema = kcu.table_schema
            WHERE tc.constraint_type = 'UNIQUE' AND tc.table_schema = 'public'
            GROUP BY tc.constraint_name, tc.table_name
            ORDER BY tc.table_name
        """)).fetchall()
        return [(r[0], r[1], r[2]) for r in rows]


def main():
    pg = create_engine(DATABASE_URL)
    mssql = create_engine(MSSQL_URL)

    unique_constraints = fetch_pg_unique_constraints(pg)

    # Drop all existing tables in app schema (drop UQ and FK first)
    with mssql.connect() as conn:
        # Drop UNIQUE constraints first
        uqs = conn.execute(text("""
            SELECT kc.name, OBJECT_SCHEMA_NAME(kc.parent_object_id), OBJECT_NAME(kc.parent_object_id)
            FROM sys.key_constraints kc
            WHERE kc.type = 'UQ' AND OBJECT_SCHEMA_NAME(kc.parent_object_id) = 'app'
        """)).fetchall()
        for row in uqs:
            conn.execute(text(f"ALTER TABLE [{row[1]}].[{row[2]}] DROP CONSTRAINT [{row[0]}]"))
            conn.commit()
        # Drop FK constraints
        fks = conn.execute(text("""
            SELECT fk.name, OBJECT_SCHEMA_NAME(fk.parent_object_id) AS sch, OBJECT_NAME(fk.parent_object_id) AS tbl
            FROM sys.foreign_keys fk
            WHERE OBJECT_SCHEMA_NAME(fk.parent_object_id) = 'app'
        """)).fetchall()
        for row in fks:
            conn.execute(text(f"ALTER TABLE [{row[1]}].[{row[2]}] DROP CONSTRAINT [{row[0]}]"))
            conn.commit()
        tables_to_drop = [r[0] for r in conn.execute(text("""
            SELECT t.name FROM sys.tables t
            JOIN sys.schemas s ON t.schema_id = s.schema_id
            WHERE s.name = 'app'
        """)).fetchall()]
        for name in tables_to_drop:
            conn.execute(text(f"DROP TABLE [app].[{name}]"))
            conn.commit()

    ddl = build_ddl(unique_constraints)
    stmts = [s.strip() for s in ddl.split(";") if s.strip() and not s.strip().startswith("--")]
    with mssql.connect() as conn:
        for stmt in stmts:
            if stmt:
                conn.execute(text(stmt + ";"))
                conn.commit()

    # Copy data from PostgreSQL to MSSQL
    tables = schema["tables"]
    order = ["stores", "suppliers", "customers", "products", "employees", "purchase_orders", "inventory", "purchase_order_items", "sales_orders", "sales_order_items"]
    ordered = {t["table"]: t for t in tables}
    tables_ordered = [ordered[n] for n in order if n in ordered]

    for t in tables_ordered:
        name = t["table"]
        cols = [c["name"] for c in t["columns"]]
        pk = t.get("primary_keys", [])
        has_identity = any(
            c["name"] in pk and c.get("is_incremental", False)
            for c in t["columns"]
        )
        col_list = ", ".join(f'"{c}"' for c in cols)
        with pg.connect() as pg_conn:
            rows = pg_conn.execute(text(f'SELECT {col_list} FROM public."{name}"')).fetchall()
        if not rows:
            continue
        col_list_mssql = ", ".join(f"[{c}]" for c in cols)
        placeholders = ", ".join([f":c{i}" for i in range(len(cols))])
        with mssql.connect() as mssql_conn:
            if has_identity:
                mssql_conn.execute(text(f"SET IDENTITY_INSERT [app].[{name}] ON"))
                mssql_conn.commit()
            for row in rows:
                params = {f"c{i}": v for i, v in enumerate(row)}
                mssql_conn.execute(
                    text(f"INSERT INTO [app].[{name}] ({col_list_mssql}) VALUES ({placeholders})"),
                    params,
                )
            if has_identity:
                mssql_conn.execute(text(f"SET IDENTITY_INSERT [app].[{name}] OFF"))
            mssql_conn.commit()
        print(f"  {name}: {len(rows)} rows")

    print("Done. Tables in app schema:")
    with mssql.connect() as conn:
        for row in conn.execute(text("""
            SELECT t.name, (SELECT SUM(p.rows) FROM sys.partitions p WHERE p.object_id = t.object_id AND p.index_id IN (0,1)) AS rows
            FROM sys.tables t
            JOIN sys.schemas s ON t.schema_id = s.schema_id
            WHERE s.name = 'app'
            ORDER BY t.name
        """)):
            print(f"  {row[0]}: {row[1]} rows")


if __name__ == "__main__":
    main()
