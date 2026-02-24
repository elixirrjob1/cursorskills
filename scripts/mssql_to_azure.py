#!/usr/bin/env python3
"""
Migrate app schema and data from local MSSQL to Azure SQL Database.
Uses schema_app.json (or schema.json) for structure, copies data from local MSSQL.

Usage:
  Add to .env:
    MSSQL_URL=mssql+pyodbc://sa:PASSWORD@localhost:1433/master?driver=ODBC+Driver+18+for+SQL+Server&TrustServerCertificate=yes
    AZURE_MSSQL_URL=mssql+pyodbc://USER:PASSWORD@pioneertest.database.windows.net:1433/free-sql-db-3300567?driver=ODBC+Driver+18+for+SQL+Server
  Run: python scripts/mssql_to_azure.py
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
LOCAL_MSSQL_URL = os.environ.get("MSSQL_URL")
AZURE_MSSQL_URL = os.environ.get("AZURE_MSSQL_URL")
AZURE_SCHEMA = os.environ.get("AZURE_MSSQL_SCHEMA", "dbo")  # Use dbo if app schema can't be created
if not LOCAL_MSSQL_URL or not AZURE_MSSQL_URL:
    raise SystemExit("MSSQL_URL and AZURE_MSSQL_URL must be set (in .env or Key Vault)")

# Prefer schema_app.json (MSSQL types) if it exists, else schema.json (PostgreSQL types)
REPO_ROOT = Path(__file__).resolve().parent.parent
LATEST_SCHEMA = REPO_ROOT / "LATEST_SCHEMA" / "schema_public_postgresql.json"
SCHEMA_APP = REPO_ROOT / "schema_app.json"
SCHEMA_JSON = REPO_ROOT / "schema.json"
SCHEMA_CANDIDATES = [LATEST_SCHEMA, SCHEMA_APP, SCHEMA_JSON]
SCHEMA_PATH = next((p for p in SCHEMA_CANDIDATES if p.exists()), SCHEMA_JSON)
with open(SCHEMA_PATH) as f:
    schema = json.load(f)

# Map PostgreSQL types to MSSQL (used when schema.json is source)
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
    "nvarchar collate \"sql_latin1_general_cp1_ci_as\"": "NVARCHAR(MAX)",
    "nvarchar(450)": "NVARCHAR(450)",
    "datetime2": "DATETIME2",
    "numeric(10,2)": "DECIMAL(10,2)",
}


def type_to_mssql(col_type: str) -> str:
    """Convert column type to MSSQL (handles PG, MSSQL, Oracle schema sources)."""
    ct = (col_type or "").strip().lower()
    if "numeric(" in ct or "decimal(" in ct:
        m = re.search(r"numeric\((\d+),(\d+)\)", ct) or re.search(r"decimal\((\d+),(\d+)\)", ct) or re.search(r"number\((\d+),(\d+)\)", ct)
        if m:
            return f"DECIMAL({m.group(1)},{m.group(2)})"
    if "nvarchar" in ct:
        return "NVARCHAR(450)" if "450" in ct else "NVARCHAR(MAX)"
    if "varchar" in ct and "number" not in ct:
        return "NVARCHAR(450)" if "450" in ct else "NVARCHAR(MAX)"
    if "datetime2" in ct or "timestamp" in ct:
        return "DATETIME2"
    if "bit" in ct or "number(1,0)" in ct:
        return "BIT"
    if "number(19,0)" in ct or "bigint" in ct:
        return "BIGINT"
    if "number(10,0)" in ct or "integer" in ct:
        return "INT"
    if "date" in ct and "time" not in ct:
        return "DATE"
    for pg, mssql in PG_TO_MSSQL.items():
        if ct == pg or (len(pg) > 3 and pg in ct):
            return mssql
    return "NVARCHAR(MAX)"


def fetch_mssql_unique_constraints(engine, schema_name: str = "app") -> list:
    """Fetch UNIQUE constraints from MSSQL schema."""
    with engine.connect() as conn:
        rows = conn.execute(text("""
            SELECT kc.name, OBJECT_NAME(kc.parent_object_id),
                   STRING_AGG(c.name, ',') WITHIN GROUP (ORDER BY ic.key_ordinal)
            FROM sys.key_constraints kc
            JOIN sys.index_columns ic ON kc.parent_object_id = ic.object_id AND kc.unique_index_id = ic.index_id
            JOIN sys.columns c ON ic.object_id = c.object_id AND ic.column_id = c.column_id
            WHERE kc.type = 'UQ' AND OBJECT_SCHEMA_NAME(kc.parent_object_id) = :schema
            GROUP BY kc.name, kc.parent_object_id
            ORDER BY OBJECT_NAME(kc.parent_object_id)
        """), {"schema": schema_name}).fetchall()
        return [(r[0], r[1], r[2]) for r in rows]


def build_ddl(unique_constraints: list, target_schema: str) -> str:
    tables = schema["tables"]
    order = ["stores", "suppliers", "customers", "products", "employees", "purchase_orders", "inventory", "purchase_order_items", "sales_orders", "sales_order_items"]
    ordered = {t["table"]: t for t in tables}
    tables_ordered = [ordered[n] for n in order if n in ordered]

    unique_cols = set()
    for _, tname, col_list in unique_constraints:
        for col in (col_list or "").split(","):
            unique_cols.add((tname, col.strip()))

    sch = target_schema
    parts = [
        f"-- MSSQL DDL for Azure (schema: {sch})",
    ]
    if sch != "dbo":
        parts.append(f"IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = '{sch}') EXEC('CREATE SCHEMA [{sch}]');")
    parts.append("")

    for t in tables_ordered:
        name = t["table"]
        cols = []
        pk = t.get("primary_keys", [])
        for c in t["columns"]:
            mssql_type = type_to_mssql(c.get("type", "text"))
            if (name, c["name"]) in unique_cols and mssql_type == "NVARCHAR(MAX)":
                mssql_type = "NVARCHAR(450)"
            is_identity = c["name"] in pk and c.get("is_incremental", False)
            if is_identity and mssql_type in ("BIGINT", "INT", "SMALLINT"):
                mssql_type = f"{mssql_type} IDENTITY(1,1)"
            null = "NULL" if c.get("nullable", False) else "NOT NULL"
            cols.append(f"    [{c['name']}] {mssql_type} {null}")
        col_def = ",\n".join(cols)
        pk_constraint = f",\n    PRIMARY KEY ({', '.join('[' + k + ']' for k in pk)})" if pk else ""
        parts.append(f"IF OBJECT_ID('[{sch}].[{name}]', 'U') IS NOT NULL DROP TABLE [{sch}].[{name}];")
        parts.append(f"CREATE TABLE [{sch}].[{name}] (\n{col_def}{pk_constraint}\n);")
        parts.append("")

    for t in tables_ordered:
        for fk in t.get("foreign_keys", []):
            col = fk["column"]
            ref = fk["references"]
            ref_table, ref_col = ref.split(".")
            fk_name = f"fk_{t['table']}_{col}"
            parts.append(f"ALTER TABLE [{sch}].[{t['table']}] ADD CONSTRAINT [{fk_name}] FOREIGN KEY ([{col}]) REFERENCES [{sch}].[{ref_table}] ([{ref_col}]);")
        if t.get("foreign_keys"):
            parts.append("")

    for cname, tname, col_list in unique_constraints:
        if not col_list:
            continue
        col_list_bracketed = ", ".join(f"[{c.strip()}]" for c in col_list.split(","))
        safe_name = re.sub(r"[^a-zA-Z0-9_]", "_", cname)[:128]
        parts.append(f"ALTER TABLE [{sch}].[{tname}] ADD CONSTRAINT [uq_{tname}_{safe_name}] UNIQUE ({col_list_bracketed});")
    if unique_constraints:
        parts.append("")

    return "\n".join(parts)


def _table_exists_mssql(conn, schema_name: str, table_name: str) -> bool:
    row = conn.execute(
        text(
            """
            SELECT 1
            FROM sys.tables t
            JOIN sys.schemas s ON s.schema_id = t.schema_id
            WHERE s.name = :schema_name AND t.name = :table_name
            """
        ),
        {"schema_name": schema_name, "table_name": table_name},
    ).fetchone()
    return bool(row)


def _column_exists_mssql(conn, schema_name: str, table_name: str, column_name: str) -> bool:
    row = conn.execute(
        text(
            """
            SELECT 1
            FROM sys.columns c
            JOIN sys.tables t ON t.object_id = c.object_id
            JOIN sys.schemas s ON s.schema_id = t.schema_id
            WHERE s.name = :schema_name AND t.name = :table_name AND c.name = :column_name
            """
        ),
        {"schema_name": schema_name, "table_name": table_name, "column_name": column_name},
    ).fetchone()
    return bool(row)


def _fk_exists_mssql(conn, schema_name: str, constraint_name: str) -> bool:
    row = conn.execute(
        text(
            """
            SELECT 1
            FROM sys.foreign_keys fk
            WHERE fk.name = :constraint_name
              AND OBJECT_SCHEMA_NAME(fk.parent_object_id) = :schema_name
            """
        ),
        {"constraint_name": constraint_name, "schema_name": schema_name},
    ).fetchone()
    return bool(row)


def _ensure_column_mssql(conn, schema_name: str, table_name: str, column_name: str, column_type: str) -> None:
    if not _table_exists_mssql(conn, schema_name, table_name):
        return
    if _column_exists_mssql(conn, schema_name, table_name, column_name):
        return
    conn.execute(text(f"ALTER TABLE [{schema_name}].[{table_name}] ADD [{column_name}] {column_type} NULL"))


def _ensure_fk_mssql(
    conn,
    schema_name: str,
    table_name: str,
    constraint_name: str,
    column_name: str,
    ref_table: str,
    ref_column: str,
) -> None:
    if not _table_exists_mssql(conn, schema_name, table_name) or not _table_exists_mssql(conn, schema_name, ref_table):
        return
    if not _column_exists_mssql(conn, schema_name, table_name, column_name) or not _column_exists_mssql(conn, schema_name, ref_table, ref_column):
        return
    if _fk_exists_mssql(conn, schema_name, constraint_name):
        return
    conn.execute(
        text(
            f"""
            ALTER TABLE [{schema_name}].[{table_name}]
            ADD CONSTRAINT [{constraint_name}]
            FOREIGN KEY ([{column_name}]) REFERENCES [{schema_name}].[{ref_table}] ([{ref_column}])
            """
        )
    )


def _set_mssql_description(conn, schema_name: str, table_name: str, description: str, column_name: str | None = None) -> None:
    try:
        if column_name:
            conn.execute(
                text(
                    """
                    EXEC sys.sp_updateextendedproperty
                        @name = N'MS_Description',
                        @value = :description,
                        @level0type = N'SCHEMA', @level0name = :schema_name,
                        @level1type = N'TABLE',  @level1name = :table_name,
                        @level2type = N'COLUMN', @level2name = :column_name
                    """
                ),
                {
                    "description": description,
                    "schema_name": schema_name,
                    "table_name": table_name,
                    "column_name": column_name,
                },
            )
        else:
            conn.execute(
                text(
                    """
                    EXEC sys.sp_updateextendedproperty
                        @name = N'MS_Description',
                        @value = :description,
                        @level0type = N'SCHEMA', @level0name = :schema_name,
                        @level1type = N'TABLE',  @level1name = :table_name
                    """
                ),
                {"description": description, "schema_name": schema_name, "table_name": table_name},
            )
    except Exception:
        if column_name:
            conn.execute(
                text(
                    """
                    EXEC sys.sp_addextendedproperty
                        @name = N'MS_Description',
                        @value = :description,
                        @level0type = N'SCHEMA', @level0name = :schema_name,
                        @level1type = N'TABLE',  @level1name = :table_name,
                        @level2type = N'COLUMN', @level2name = :column_name
                    """
                ),
                {
                    "description": description,
                    "schema_name": schema_name,
                    "table_name": table_name,
                    "column_name": column_name,
                },
            )
        else:
            conn.execute(
                text(
                    """
                    EXEC sys.sp_addextendedproperty
                        @name = N'MS_Description',
                        @value = :description,
                        @level0type = N'SCHEMA', @level0name = :schema_name,
                        @level1type = N'TABLE',  @level1name = :table_name
                    """
                ),
                {"description": description, "schema_name": schema_name, "table_name": table_name},
            )


def enrich_for_analyzer_testing_mssql(engine, schema_name: str) -> None:
    with engine.connect() as conn:
        _ensure_column_mssql(conn, schema_name, "products", "weight_value", "DECIMAL(10,2)")
        _ensure_column_mssql(conn, schema_name, "products", "weight_unit", "NVARCHAR(16)")
        _ensure_column_mssql(conn, schema_name, "products", "length_value", "DECIMAL(10,2)")
        _ensure_column_mssql(conn, schema_name, "products", "length_unit", "NVARCHAR(16)")
        _ensure_column_mssql(conn, schema_name, "products", "product_description", "NVARCHAR(MAX)")

        _ensure_column_mssql(conn, schema_name, "purchase_order_items", "ordered_qty_value", "DECIMAL(10,2)")
        _ensure_column_mssql(conn, schema_name, "purchase_order_items", "ordered_qty_unit", "NVARCHAR(16)")

        _ensure_column_mssql(conn, schema_name, "sales_order_items", "sold_qty_value", "DECIMAL(10,2)")
        _ensure_column_mssql(conn, schema_name, "sales_order_items", "sold_qty_unit", "NVARCHAR(16)")

        _ensure_column_mssql(conn, schema_name, "inventory", "stock_value", "DECIMAL(10,2)")
        _ensure_column_mssql(conn, schema_name, "inventory", "stock_unit", "NVARCHAR(16)")

        _ensure_column_mssql(conn, schema_name, "purchase_orders", "approver_employee_id", "BIGINT")
        _ensure_column_mssql(conn, schema_name, "sales_orders", "sales_rep_employee_id", "BIGINT")
        _ensure_column_mssql(conn, schema_name, "products", "primary_supplier_id", "BIGINT")

        if _table_exists_mssql(conn, schema_name, "products"):
            conn.execute(
                text(
                    f"""
                    UPDATE [{schema_name}].[products]
                    SET [weight_value] = CAST(ROUND(0.50 + (([product_id] % 20) * 0.25), 2) AS DECIMAL(10,2)),
                        [weight_unit] = CASE WHEN [product_id] % 2 = 0 THEN 'kg' ELSE 'lb' END,
                        [length_value] = CAST(ROUND(10.0 + (([product_id] % 30) * 1.5), 2) AS DECIMAL(10,2)),
                        [length_unit] = CASE WHEN [product_id] % 2 = 0 THEN 'cm' ELSE 'in' END,
                        [product_description] = CONCAT('Product ', COALESCE([name], 'unknown'), ' in ', COALESCE([category], 'general'), ' category.'),
                        [primary_supplier_id] = COALESCE([primary_supplier_id], [supplier_id])
                    """
                )
            )
            if _table_exists_mssql(conn, schema_name, "suppliers"):
                conn.execute(
                    text(
                        f"""
                        UPDATE p
                        SET [primary_supplier_id] = s.[supplier_id]
                        FROM [{schema_name}].[products] p
                        CROSS APPLY (
                            SELECT TOP 1 [supplier_id]
                            FROM [{schema_name}].[suppliers]
                            ORDER BY [supplier_id]
                        ) s
                        WHERE p.[primary_supplier_id] IS NULL
                        """
                    )
                )

        if _table_exists_mssql(conn, schema_name, "purchase_order_items"):
            conn.execute(
                text(
                    f"""
                    UPDATE [{schema_name}].[purchase_order_items]
                    SET [ordered_qty_value] = CAST(COALESCE([quantity], 0) AS DECIMAL(10,2)),
                        [ordered_qty_unit] = CASE WHEN [po_item_id] % 3 = 0 THEN 'box' ELSE 'ea' END
                    """
                )
            )

        if _table_exists_mssql(conn, schema_name, "sales_order_items"):
            conn.execute(
                text(
                    f"""
                    UPDATE [{schema_name}].[sales_order_items]
                    SET [sold_qty_value] = CAST(COALESCE([quantity], 0) AS DECIMAL(10,2)),
                        [sold_qty_unit] = CASE WHEN [sales_order_item_id] % 3 = 0 THEN 'box' ELSE 'ea' END
                    """
                )
            )

        if _table_exists_mssql(conn, schema_name, "inventory"):
            conn.execute(
                text(
                    f"""
                    UPDATE [{schema_name}].[inventory]
                    SET [stock_value] = CAST(COALESCE([quantity_on_hand], 0) AS DECIMAL(10,2)),
                        [stock_unit] = 'ea'
                    """
                )
            )

        if _table_exists_mssql(conn, schema_name, "purchase_orders") and _table_exists_mssql(conn, schema_name, "employees"):
            conn.execute(
                text(
                    f"""
                    UPDATE po
                    SET [approver_employee_id] = e.[employee_id]
                    FROM [{schema_name}].[purchase_orders] po
                    CROSS APPLY (
                        SELECT TOP 1 [employee_id]
                        FROM [{schema_name}].[employees]
                        WHERE [store_id] = po.[store_id]
                        ORDER BY [employee_id]
                    ) e
                    WHERE po.[approver_employee_id] IS NULL
                    """
                )
            )
            conn.execute(
                text(
                    f"""
                    UPDATE po
                    SET [approver_employee_id] = e.[employee_id]
                    FROM [{schema_name}].[purchase_orders] po
                    CROSS APPLY (
                        SELECT TOP 1 [employee_id]
                        FROM [{schema_name}].[employees]
                        ORDER BY [employee_id]
                    ) e
                    WHERE po.[approver_employee_id] IS NULL
                    """
                )
            )

        if _table_exists_mssql(conn, schema_name, "sales_orders"):
            conn.execute(
                text(
                    f"""
                    UPDATE [{schema_name}].[sales_orders]
                    SET [sales_rep_employee_id] = COALESCE([sales_rep_employee_id], [employee_id])
                    """
                )
            )
            if _table_exists_mssql(conn, schema_name, "employees"):
                conn.execute(
                    text(
                        f"""
                        UPDATE so
                        SET [sales_rep_employee_id] = e.[employee_id]
                        FROM [{schema_name}].[sales_orders] so
                        CROSS APPLY (
                            SELECT TOP 1 [employee_id]
                            FROM [{schema_name}].[employees]
                            WHERE [store_id] = so.[store_id]
                            ORDER BY [employee_id]
                        ) e
                        WHERE so.[sales_rep_employee_id] IS NULL
                        """
                    )
                )

        _ensure_fk_mssql(
            conn,
            schema_name,
            "purchase_orders",
            "fk_purchase_orders_approver_employee",
            "approver_employee_id",
            "employees",
            "employee_id",
        )
        _ensure_fk_mssql(
            conn,
            schema_name,
            "sales_orders",
            "fk_sales_orders_sales_rep_employee",
            "sales_rep_employee_id",
            "employees",
            "employee_id",
        )
        _ensure_fk_mssql(
            conn,
            schema_name,
            "products",
            "fk_products_primary_supplier",
            "primary_supplier_id",
            "suppliers",
            "supplier_id",
        )

        table_descriptions = {
            "products": "Master product catalog including physical units for analyzer unit-context testing.",
            "purchase_order_items": "Line items with order quantities and explicit unit labels.",
            "sales_order_items": "Sales line items with sold quantity and explicit unit labels.",
            "inventory": "Current stock levels with normalized stock unit representation.",
            "purchase_orders": "Procurement headers including approver employee relationship.",
            "sales_orders": "Sales headers including assigned sales representative relationship.",
        }
        column_descriptions = {
            ("products", "weight_value"): "Measured product weight value.",
            ("products", "weight_unit"): "Source weight unit (kg/lb) used for unit inference testing.",
            ("products", "length_value"): "Measured product length value.",
            ("products", "length_unit"): "Source length unit (cm/in) used for unit inference testing.",
            ("products", "product_description"): "Human-readable product description for text semantics.",
            ("products", "primary_supplier_id"): "Primary supplier relationship used for join candidate detection.",
            ("purchase_order_items", "ordered_qty_value"): "Ordered quantity as numeric value.",
            ("purchase_order_items", "ordered_qty_unit"): "Ordered quantity unit (ea/box).",
            ("sales_order_items", "sold_qty_value"): "Sold quantity as numeric value.",
            ("sales_order_items", "sold_qty_unit"): "Sold quantity unit (ea/box).",
            ("inventory", "stock_value"): "Current stock quantity as numeric value.",
            ("inventory", "stock_unit"): "Current stock unit label.",
            ("purchase_orders", "approver_employee_id"): "Approver employee foreign key for procurement workflow.",
            ("sales_orders", "sales_rep_employee_id"): "Sales representative foreign key for order ownership.",
        }

        for table_name, description in table_descriptions.items():
            if _table_exists_mssql(conn, schema_name, table_name):
                _set_mssql_description(conn, schema_name, table_name, description)
        for (table_name, column_name), description in column_descriptions.items():
            if _column_exists_mssql(conn, schema_name, table_name, column_name):
                _set_mssql_description(conn, schema_name, table_name, description, column_name=column_name)

        conn.commit()



def main():
    local_mssql = create_engine(LOCAL_MSSQL_URL)
    azure = create_engine(AZURE_MSSQL_URL)
    sch = AZURE_SCHEMA

    unique_constraints = fetch_mssql_unique_constraints(local_mssql, "app")

    # Drop existing tables on Azure
    with azure.connect() as conn:
        uqs = conn.execute(text("""
            SELECT kc.name, OBJECT_SCHEMA_NAME(kc.parent_object_id), OBJECT_NAME(kc.parent_object_id)
            FROM sys.key_constraints kc
            WHERE kc.type = 'UQ' AND OBJECT_SCHEMA_NAME(kc.parent_object_id) = :schema
        """), {"schema": sch}).fetchall()
        for row in uqs:
            conn.execute(text(f"ALTER TABLE [{row[1]}].[{row[2]}] DROP CONSTRAINT [{row[0]}]"))
            conn.commit()
        fks = conn.execute(text("""
            SELECT fk.name, OBJECT_SCHEMA_NAME(fk.parent_object_id), OBJECT_NAME(fk.parent_object_id)
            FROM sys.foreign_keys fk
            WHERE OBJECT_SCHEMA_NAME(fk.parent_object_id) = :schema
        """), {"schema": sch}).fetchall()
        for row in fks:
            conn.execute(text(f"ALTER TABLE [{row[1]}].[{row[2]}] DROP CONSTRAINT [{row[0]}]"))
            conn.commit()
        tables_to_drop = [r[0] for r in conn.execute(text("""
            SELECT t.name FROM sys.tables t
            JOIN sys.schemas s ON t.schema_id = s.schema_id
            WHERE s.name = :schema
        """), {"schema": sch}).fetchall()]
        for name in tables_to_drop:
            conn.execute(text(f"DROP TABLE [{sch}].[{name}]"))
            conn.commit()

    ddl = build_ddl(unique_constraints, sch)
    stmts = [s.strip() for s in ddl.split(";") if s.strip() and not s.strip().startswith("--")]
    with azure.connect() as conn:
        for stmt in stmts:
            if stmt:
                conn.execute(text(stmt + ";"))
                conn.commit()

    # Copy data from local MSSQL to Azure
    tables = schema["tables"]
    order = ["stores", "suppliers", "customers", "products", "employees", "purchase_orders", "inventory", "purchase_order_items", "sales_orders", "sales_order_items"]
    ordered = {t["table"]: t for t in tables}
    tables_ordered = [ordered[n] for n in order if n in ordered]

    for t in tables_ordered:
        name = t["table"]
        cols = [c["name"] for c in t["columns"]]
        pk = t.get("primary_keys", [])
        has_identity = any(c["name"] in pk and c.get("is_incremental", False) for c in t["columns"])
        col_list_mssql = ", ".join(f"[{c}]" for c in cols)
        with local_mssql.connect() as src:
            rows = src.execute(text(f"SELECT {col_list_mssql} FROM [app].[{name}]")).fetchall()
        if not rows:
            continue
        placeholders = ", ".join([f":c{i}" for i in range(len(cols))])
        with azure.connect() as dst:
            if has_identity:
                dst.execute(text(f"SET IDENTITY_INSERT [{sch}].[{name}] ON"))
                dst.commit()
            for row in rows:
                params = {f"c{i}": v for i, v in enumerate(row)}
                dst.execute(text(f"INSERT INTO [{sch}].[{name}] ({col_list_mssql}) VALUES ({placeholders})"), params)
            if has_identity:
                dst.execute(text(f"SET IDENTITY_INSERT [{sch}].[{name}] OFF"))
            dst.commit()
        print(f"  {name}: {len(rows)} rows")

    enrich_for_analyzer_testing_mssql(azure, sch)

    print(f"Done. Tables in Azure [{sch}] schema:")
    with azure.connect() as conn:
        for row in conn.execute(text("""
            SELECT t.name, (SELECT SUM(p.rows) FROM sys.partitions p WHERE p.object_id = t.object_id AND p.index_id IN (0,1))
            FROM sys.tables t
            JOIN sys.schemas s ON t.schema_id = s.schema_id
            WHERE s.name = :schema
            ORDER BY t.name
        """), {"schema": sch}).fetchall():
            print(f"  {row[0]}: {row[1]} rows")


if __name__ == "__main__":
    main()
