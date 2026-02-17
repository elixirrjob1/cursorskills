#!/usr/bin/env python3
"""Run schema_public_mssql.ddl against MSSQL and populate with sample data."""
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
MSSQL_URL = os.environ.get("MSSQL_URL")
if not MSSQL_URL:
    raise SystemExit("MSSQL_URL not set (in .env or Key Vault)")

ddl_path = Path(__file__).resolve().parent.parent / "schema_public_mssql.ddl"
ddl = ddl_path.read_text()

engine = create_engine(MSSQL_URL)

def run_batches(sql: str):
    """Split by GO and run each batch."""
    batches = re.split(r"\s*GO\s*", sql, flags=re.IGNORECASE)
    for batch in batches:
        # Strip all leading comment lines
        batch = re.sub(r"^(?:\s*--[^\n]*\n?)+", "", batch).strip()
        if not batch:
            continue
        with engine.connect() as conn:
            try:
                conn.execute(text(batch))
                conn.commit()
            except Exception as e:
                conn.rollback()
                raise

print("Creating public schema and tables...")
run_batches(ddl)

print("Inserting sample data...")
with engine.connect() as conn:
    # MSreplication_options: 3 rows (matches original row_count)
    conn.execute(text("""
        INSERT INTO [app].[MSreplication_options] (optname, value, major_version, minor_version, revision, install_failures)
        VALUES
            ('merge', 1, 16, 0, 1, 0),
            ('publish', 1, 16, 0, 1, 0),
            ('subscribe', 0, 16, 0, 1, 0)
    """))
    conn.commit()

    # spt_monitor: 1 row (matches original)
    conn.execute(text("""
        INSERT INTO [app].[spt_monitor] (lastrun, cpu_busy, io_busy, idle, pack_received, pack_sent, connections, pack_errors, total_read, total_write, total_errors)
        VALUES ('2025-09-09 18:16:47.810', 31, 19, 5838, 39, 39, 70, 0, 0, 0, 0)
    """))
    conn.commit()

    # spt_fallback_db: 1 sample row
    conn.execute(text("""
        INSERT INTO [app].[spt_fallback_db] (xserver_name, xdttm_ins, xdttm_last_ins_upd, xfallback_dbid, name, dbid, status, version)
        VALUES ('.', GETDATE(), GETDATE(), NULL, 'master', 1, 0, 1)
    """))
    conn.commit()

    # spt_fallback_dev: 1 sample row
    conn.execute(text("""
        INSERT INTO [app].[spt_fallback_dev] (xserver_name, xdttm_ins, xdttm_last_ins_upd, xfallback_low, xfallback_drive, low, high, status, name, phyname)
        VALUES ('.', GETDATE(), GETDATE(), NULL, 'C:', 0, 0, 0, 'master', 'C:\\Program Files\\Microsoft SQL Server\\...')
    """))
    conn.commit()

    # spt_fallback_usg: 1 sample row
    conn.execute(text("""
        INSERT INTO [app].[spt_fallback_usg] (xserver_name, xdttm_ins, xdttm_last_ins_upd, xfallback_vstart, dbid, segmap, lstart, sizepg, vstart)
        VALUES ('.', GETDATE(), GETDATE(), NULL, 1, 1, 0, 8192, 0)
    """))
    conn.commit()

print("Done. Tables in app schema:")
with engine.connect() as conn:
    for row in conn.execute(text("""
        SELECT t.name, (SELECT SUM(p.rows) FROM sys.partitions p WHERE p.object_id = t.object_id AND p.index_id IN (0,1)) AS rows
        FROM sys.tables t
        JOIN sys.schemas s ON t.schema_id = s.schema_id
        WHERE s.name = 'app'
        ORDER BY t.name
    """)):
        print(f"  {row[0]}: {row[1]} rows")
