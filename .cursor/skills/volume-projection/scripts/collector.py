#!/usr/bin/env python3
"""
Volume Projection Collector
Collects table sizes, churn metrics, and growth history for capacity planning.
Stores data in the prediction schema for use by the predictor.
"""

import os
import argparse
import logging
from pathlib import Path
from datetime import datetime, timezone, timedelta
from typing import Optional

from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)


def _load_env_file() -> None:
    """Load .env from current working directory or script directory."""
    for base in (Path.cwd(), Path(__file__).resolve().parent.parent.parent.parent.parent):
        env_path = base / ".env"
        if env_path.exists():
            try:
                with open(env_path, encoding="utf-8") as f:
                    for line in f:
                        line = line.strip()
                        if line and not line.startswith("#") and "=" in line:
                            key, _, value = line.partition("=")
                            key = key.strip()
                            value = value.strip().strip('"').strip("'")
                            if key and key not in os.environ:
                                os.environ[key] = value
            except Exception:
                pass
            break


def get_engine(database_url: str) -> Engine:
    """Create a SQLAlchemy engine from a database URL."""
    return create_engine(
        database_url,
        pool_size=2,
        max_overflow=5,
        pool_pre_ping=True,
        connect_args={"connect_timeout": 10},
        echo=False,
    )


# ============================================================================
# Setup: Create prediction schema and tables
# ============================================================================

SETUP_SQL = """
-- Create schema
CREATE SCHEMA IF NOT EXISTS prediction;

-- collection_runs: tracks each collection run
CREATE TABLE IF NOT EXISTS prediction.collection_runs (
    run_id SERIAL PRIMARY KEY,
    started_at TIMESTAMPTZ NOT NULL,
    completed_at TIMESTAMPTZ,
    status TEXT NOT NULL DEFAULT 'running',
    tables_analyzed INT DEFAULT 0
);

-- table_size_snapshots: point-in-time size and churn per table
CREATE TABLE IF NOT EXISTS prediction.table_size_snapshots (
    snapshot_id SERIAL PRIMARY KEY,
    run_id INT NOT NULL REFERENCES prediction.collection_runs(run_id),
    table_name TEXT NOT NULL,
    schema_name TEXT NOT NULL,
    snapshot_date DATE NOT NULL,
    row_count BIGINT NOT NULL,
    avg_row_size_bytes NUMERIC,
    table_data_size_bytes BIGINT,
    index_size_bytes BIGINT,
    toast_size_bytes BIGINT,
    total_size_bytes BIGINT,
    bloat_ratio NUMERIC,
    n_live_tup BIGINT,
    n_dead_tup BIGINT,
    n_tup_ins BIGINT,
    n_tup_upd BIGINT,
    n_tup_del BIGINT,
    n_tup_hot_upd BIGINT,
    stats_reset_at TIMESTAMPTZ
);

-- growth_history: monthly row growth from created_at columns
CREATE TABLE IF NOT EXISTS prediction.growth_history (
    growth_id SERIAL PRIMARY KEY,
    run_id INT NOT NULL REFERENCES prediction.collection_runs(run_id),
    table_name TEXT NOT NULL,
    schema_name TEXT NOT NULL,
    source_column TEXT NOT NULL,
    period_start DATE NOT NULL,
    period_end DATE NOT NULL,
    rows_added BIGINT NOT NULL,
    cumulative_rows BIGINT NOT NULL
);

-- database_snapshots: database-level metrics
CREATE TABLE IF NOT EXISTS prediction.database_snapshots (
    db_snapshot_id SERIAL PRIMARY KEY,
    run_id INT NOT NULL REFERENCES prediction.collection_runs(run_id),
    snapshot_date DATE NOT NULL,
    total_database_size_bytes BIGINT,
    shared_buffers TEXT,
    work_mem TEXT,
    temp_buffers TEXT,
    max_connections INT,
    temp_files_count BIGINT,
    temp_bytes BIGINT
);

-- Indexes for efficient queries
CREATE INDEX IF NOT EXISTS idx_table_size_snapshots_run ON prediction.table_size_snapshots(run_id);
CREATE INDEX IF NOT EXISTS idx_table_size_snapshots_table ON prediction.table_size_snapshots(table_name, schema_name);
CREATE INDEX IF NOT EXISTS idx_growth_history_run ON prediction.growth_history(run_id);
CREATE INDEX IF NOT EXISTS idx_growth_history_table_period ON prediction.growth_history(table_name, schema_name, period_start);
"""


def run_setup(engine: Engine) -> None:
    """Create prediction schema and all tables."""
    logger.info("Creating prediction schema and tables...")
    with engine.connect() as conn:
        conn.execute(text(SETUP_SQL))
        conn.commit()
    logger.info("Setup complete.")


# ============================================================================
# Collect: Gather metrics and store in prediction schema
# ============================================================================

def run_collect(engine: Engine, schema: str = "public") -> None:
    """Run full collection: table sizes, churn, growth history, database metrics."""
    dialect = engine.dialect.name
    if dialect != "postgresql":
        raise RuntimeError("Volume projection collector supports PostgreSQL only")

    started_at = datetime.now(timezone.utc)
    snapshot_date = started_at.date()

    with engine.connect() as conn:
        # 1. Insert collection_runs row
        result = conn.execute(
            text("""
                INSERT INTO prediction.collection_runs (started_at, status)
                VALUES (:started_at, 'running')
                RETURNING run_id
            """),
            {"started_at": started_at},
        )
        run_id = result.scalar()
        conn.commit()

        logger.info(f"Started collection run_id={run_id}")

        try:
            # 2. Get list of tables in schema
            tables_result = conn.execute(
                text("""
                    SELECT table_name
                    FROM information_schema.tables
                    WHERE table_schema = :schema
                      AND table_type = 'BASE TABLE'
                    ORDER BY table_name
                """),
                {"schema": schema},
            )
            tables = [row[0] for row in tables_result.fetchall()]

            if not tables:
                logger.warning(f"No tables found in schema {schema}")
                tables_analyzed = 0
            else:
                # 3. For each table: sizes and churn
                for table_name in tables:
                    _collect_table_snapshot(conn, run_id, schema, table_name, snapshot_date)

                # 4. Growth history from created_at columns (2 years back)
                cutoff = snapshot_date - timedelta(days=730)  # ~2 years
                for table_name in tables:
                    _collect_growth_history(conn, run_id, schema, table_name, cutoff)

                # 5. Database-level metrics
                _collect_database_snapshot(conn, run_id, snapshot_date)

                tables_analyzed = len(tables)

            # 6. Update collection_runs
            conn.execute(
                text("""
                    UPDATE prediction.collection_runs
                    SET completed_at = :completed_at, status = 'success', tables_analyzed = :tables_analyzed
                    WHERE run_id = :run_id
                """),
                {
                    "completed_at": datetime.now(timezone.utc),
                    "tables_analyzed": tables_analyzed,
                    "run_id": run_id,
                },
            )
            conn.commit()
            logger.info(f"Collection complete. Analyzed {tables_analyzed} tables.")

        except Exception as e:
            logger.error(f"Collection failed: {e}")
            conn.execute(
                text("""
                    UPDATE prediction.collection_runs
                    SET completed_at = :completed_at, status = 'failed'
                    WHERE run_id = :run_id
                """),
                {"completed_at": datetime.now(timezone.utc), "run_id": run_id},
            )
            conn.commit()
            raise


def _collect_table_snapshot(conn, run_id: int, schema: str, table_name: str, snapshot_date) -> None:
    """Collect size and churn metrics for one table."""
    try:
        # Physical sizes
        size_result = conn.execute(
            text("""
                SELECT
                    pg_total_relation_size(:qualified) AS total_size,
                    pg_table_size(:qualified) AS table_size,
                    pg_indexes_size(:qualified) AS index_size,
                    pg_relation_size(:qualified, 'main') AS main_size
            """),
            {"qualified": f'"{schema}"."{table_name}"'},
        )
        size_row = size_result.fetchone()

        total_size = size_row[0] or 0
        table_size = size_row[1] or 0
        index_size = size_row[2] or 0
        main_size = size_row[3] or 0
        toast_size = total_size - table_size - index_size if total_size else 0
        if toast_size < 0:
            toast_size = 0

        # Row count
        count_result = conn.execute(
            text(f'SELECT COUNT(*) FROM "{schema}"."{table_name}"'),
        )
        row_count = count_result.scalar() or 0

        # Avg row size (from pg_stats if available, else table_size/row_count)
        avg_row_size = None
        if row_count > 0 and main_size > 0:
            avg_row_size = round(main_size / row_count, 2)

        # Bloat ratio: actual vs theoretical (simplified)
        bloat_ratio = None
        if avg_row_size and row_count > 0:
            theoretical = row_count * avg_row_size
            if theoretical > 0:
                bloat_ratio = round(table_size / theoretical, 4)

        # Churn from pg_stat_user_tables (stats_reset is in pg_stat_database, not here)
        stat_result = conn.execute(
            text("""
                SELECT
                    n_live_tup, n_dead_tup,
                    n_tup_ins, n_tup_upd, n_tup_del, n_tup_hot_upd
                FROM pg_stat_user_tables
                WHERE schemaname = :schema AND relname = :table
            """),
            {"schema": schema, "table": table_name},
        )
        stat_row = stat_result.fetchone()

        n_live_tup = n_dead_tup = n_tup_ins = n_tup_upd = n_tup_del = n_tup_hot_upd = 0
        stats_reset_at = None
        if stat_row:
            n_live_tup = stat_row[0] or 0
            n_dead_tup = stat_row[1] or 0
            n_tup_ins = stat_row[2] or 0
            n_tup_upd = stat_row[3] or 0
            n_tup_del = stat_row[4] or 0
            n_tup_hot_upd = stat_row[5] or 0

        conn.execute(
            text("""
                INSERT INTO prediction.table_size_snapshots (
                    run_id, table_name, schema_name, snapshot_date,
                    row_count, avg_row_size_bytes,
                    table_data_size_bytes, index_size_bytes, toast_size_bytes, total_size_bytes,
                    bloat_ratio,
                    n_live_tup, n_dead_tup, n_tup_ins, n_tup_upd, n_tup_del, n_tup_hot_upd,
                    stats_reset_at
                ) VALUES (
                    :run_id, :table_name, :schema_name, :snapshot_date,
                    :row_count, :avg_row_size_bytes,
                    :table_data_size_bytes, :index_size_bytes, :toast_size_bytes, :total_size_bytes,
                    :bloat_ratio,
                    :n_live_tup, :n_dead_tup, :n_tup_ins, :n_tup_upd, :n_tup_del, :n_tup_hot_upd,
                    :stats_reset_at
                )
            """),
            {
                "run_id": run_id,
                "table_name": table_name,
                "schema_name": schema,
                "snapshot_date": snapshot_date,
                "row_count": row_count,
                "avg_row_size_bytes": avg_row_size,
                "table_data_size_bytes": table_size,
                "index_size_bytes": index_size,
                "toast_size_bytes": toast_size,
                "total_size_bytes": total_size,
                "bloat_ratio": bloat_ratio,
                "n_live_tup": n_live_tup,
                "n_dead_tup": n_dead_tup,
                "n_tup_ins": n_tup_ins,
                "n_tup_upd": n_tup_upd,
                "n_tup_del": n_tup_del,
                "n_tup_hot_upd": n_tup_hot_upd,
                "stats_reset_at": stats_reset_at,
            },
        )
    except Exception as e:
        logger.warning(f"Failed to collect snapshot for {schema}.{table_name}: {e}")
        conn.rollback()


def _collect_growth_history(conn, run_id: int, schema: str, table_name: str, cutoff_date) -> None:
    """Collect monthly growth from created_at column for one table."""
    try:
        # Check if table has created_at (or similar) column
        col_result = conn.execute(
            text("""
                SELECT column_name
                FROM information_schema.columns
                WHERE table_schema = :schema AND table_name = :table
                  AND column_name IN ('created_at', 'created_date', 'inserted_at')
                ORDER BY CASE column_name WHEN 'created_at' THEN 1 WHEN 'created_date' THEN 2 ELSE 3 END
                LIMIT 1
            """),
            {"schema": schema, "table": table_name},
        )
        col_row = col_result.fetchone()
        if not col_row:
            return

        source_column = col_row[0]

        # Get monthly counts for last 2 years
        growth_result = conn.execute(
            text(f"""
                SELECT
                    date_trunc('month', "{source_column}")::date AS period_start,
                    COUNT(*) AS rows_added
                FROM "{schema}"."{table_name}"
                WHERE "{source_column}" >= :cutoff
                GROUP BY date_trunc('month', "{source_column}")
                ORDER BY period_start
            """),
            {"cutoff": cutoff_date},
        )
        rows = growth_result.fetchall()

        if not rows:
            return

        # Compute cumulative totals (ordered by period)
        cumulative = 0
        for period_start, rows_added in rows:
            cumulative += rows_added
            period_end = period_start + timedelta(days=32)  # end of month
            conn.execute(
                text("""
                    INSERT INTO prediction.growth_history (
                        run_id, table_name, schema_name, source_column,
                        period_start, period_end, rows_added, cumulative_rows
                    ) VALUES (
                        :run_id, :table_name, :schema_name, :source_column,
                        :period_start, :period_end, :rows_added, :cumulative_rows
                    )
                """),
                {
                    "run_id": run_id,
                    "table_name": table_name,
                    "schema_name": schema,
                    "source_column": source_column,
                    "period_start": period_start,
                    "period_end": period_end,
                    "rows_added": rows_added,
                    "cumulative_rows": cumulative,
                },
            )
    except Exception as e:
        logger.warning(f"Failed to collect growth history for {schema}.{table_name}: {e}")
        conn.rollback()


def _collect_database_snapshot(conn, run_id: int, snapshot_date) -> None:
    """Collect database-level metrics."""
    try:
        # Total database size
        size_result = conn.execute(text("SELECT pg_database_size(current_database())"))
        total_size = size_result.scalar() or 0

        # Config values
        config_result = conn.execute(
            text("""
                SELECT name, setting
                FROM pg_settings
                WHERE name IN ('shared_buffers', 'work_mem', 'temp_buffers', 'max_connections')
            """),
        )
        config = {row[0]: row[1] for row in config_result.fetchall()}
        shared_buffers = config.get("shared_buffers")
        work_mem = config.get("work_mem")
        temp_buffers = config.get("temp_buffers")
        max_connections = int(config.get("max_connections", 0) or 0)

        # Temp file usage
        temp_result = conn.execute(
            text("""
                SELECT temp_files, temp_bytes
                FROM pg_stat_database
                WHERE datname = current_database()
            """),
        )
        temp_row = temp_result.fetchone()
        temp_files_count = temp_row[0] if temp_row else 0
        temp_bytes = temp_row[1] if temp_row else 0

        conn.execute(
            text("""
                INSERT INTO prediction.database_snapshots (
                    run_id, snapshot_date, total_database_size_bytes,
                    shared_buffers, work_mem, temp_buffers, max_connections,
                    temp_files_count, temp_bytes
                ) VALUES (
                    :run_id, :snapshot_date, :total_database_size_bytes,
                    :shared_buffers, :work_mem, :temp_buffers, :max_connections,
                    :temp_files_count, :temp_bytes
                )
            """),
            {
                "run_id": run_id,
                "snapshot_date": snapshot_date,
                "total_database_size_bytes": total_size,
                "shared_buffers": shared_buffers,
                "work_mem": work_mem,
                "temp_buffers": temp_buffers,
                "max_connections": max_connections,
                "temp_files_count": temp_files_count,
                "temp_bytes": temp_bytes,
            },
        )
    except Exception as e:
        logger.warning(f"Failed to collect database snapshot: {e}")
        conn.rollback()


# ============================================================================
# Main
# ============================================================================

def main() -> None:
    _load_env_file()
    parser = argparse.ArgumentParser(description="Volume projection collector")
    parser.add_argument("database_url", nargs="?", default=os.environ.get("DATABASE_URL"))
    parser.add_argument("--setup", action="store_true", help="Create prediction schema and tables")
    parser.add_argument("--collect", action="store_true", help="Run data collection")
    parser.add_argument("--schema", default="public", help="Schema to analyze (default: public)")
    args = parser.parse_args()

    if not args.database_url:
        parser.error("database_url required (argument or DATABASE_URL env)")

    if not args.setup and not args.collect:
        parser.error("Specify --setup and/or --collect")

    engine = get_engine(args.database_url)

    if args.setup:
        run_setup(engine)
    if args.collect:
        run_collect(engine, schema=args.schema)


if __name__ == "__main__":
    main()
