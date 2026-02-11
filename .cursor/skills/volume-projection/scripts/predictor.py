#!/usr/bin/env python3
"""
Volume Projection Predictor
Reads data from the prediction schema and generates capacity forecasts.
Uses pure Python (no numpy) for linear regression and trend detection.
"""

import os
import json
import argparse
import logging
from pathlib import Path
from datetime import datetime, timezone, date, timedelta
from typing import Optional, Dict, List, Any

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


def _format_bytes(n: int) -> str:
    """Format byte count as human-readable string."""
    if n is None or n < 0:
        return "0 B"
    for unit in ("B", "KB", "MB", "GB", "TB"):
        if n < 1024:
            return f"{n:.1f} {unit}"
        n /= 1024
    return f"{n:.1f} PB"


def _linear_regression_slope(x: List[float], y: List[float]) -> float:
    """Pure Python least-squares slope. Returns growth rate per x unit."""
    n = len(x)
    if n < 2:
        return 0.0
    x_mean = sum(x) / n
    y_mean = sum(y) / n
    numer = sum((x[i] - x_mean) * (y[i] - y_mean) for i in range(n))
    denom = sum((x[i] - x_mean) ** 2 for i in range(n))
    if denom == 0:
        return 0.0
    return numer / denom


def _classify_write_profile(ins: int, upd: int, dele: int) -> str:
    """Classify table write profile from churn counters."""
    total = ins + upd + dele
    if total == 0:
        return "unknown"
    ins_pct = ins / total
    upd_pct = upd / total
    del_pct = dele / total
    if ins_pct > 0.8:
        return "append_only"
    if upd_pct > 0.5:
        return "update_heavy"
    if del_pct > 0.3:
        return "delete_heavy"
    return "mixed"


def run_predict(engine: Engine, output_path: str) -> None:
    """Read prediction schema, compute forecasts, write capacity_report.json."""
    dialect = engine.dialect.name
    if dialect != "postgresql":
        raise RuntimeError("Volume projection predictor supports PostgreSQL only")

    report = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "summary": {},
        "tables": [],
        "database": {},
    }

    with engine.connect() as conn:
        # Get latest run
        run_result = conn.execute(
            text("""
                SELECT run_id, started_at, tables_analyzed, status
                FROM prediction.collection_runs
                WHERE status = 'success'
                ORDER BY run_id DESC
                LIMIT 1
            """),
        )
        run_row = run_result.fetchone()
        if not run_row:
            report["error"] = "No successful collection run found. Run the collector first."
            with open(output_path, "w", encoding="utf-8") as f:
                json.dump(report, f, indent=2, default=str)
            return

        run_id = run_row[0]
        report["collection_run_id"] = run_id
        report["collection_run_at"] = str(run_row[1]) if run_row[1] else None

        # Database snapshot
        db_result = conn.execute(
            text("""
                SELECT total_database_size_bytes, shared_buffers, work_mem,
                       max_connections, temp_files_count, temp_bytes
                FROM prediction.database_snapshots
                WHERE run_id = :run_id
                ORDER BY db_snapshot_id DESC
                LIMIT 1
            """),
            {"run_id": run_id},
        )
        db_row = db_result.fetchone()
        if db_row:
            report["database"] = {
                "total_size_bytes": db_row[0],
                "total_size_human": _format_bytes(db_row[0] or 0),
                "shared_buffers": db_row[1],
                "work_mem": db_row[2],
                "max_connections": db_row[3],
                "temp_files_count": db_row[4],
                "temp_bytes": db_row[5],
                "temp_size_human": _format_bytes(db_row[5] or 0),
            }

        # Table snapshots
        snap_result = conn.execute(
            text("""
                SELECT table_name, schema_name, row_count, avg_row_size_bytes,
                       total_size_bytes, table_data_size_bytes, index_size_bytes,
                       bloat_ratio, n_tup_ins, n_tup_upd, n_tup_del
                FROM prediction.table_size_snapshots
                WHERE run_id = :run_id
                ORDER BY total_size_bytes DESC NULLS LAST
            """),
            {"run_id": run_id},
        )
        snapshots = snap_result.fetchall()

        current_total_bytes = 0
        table_projections = []

        for row in snapshots:
            table_name = row[0]
            schema_name = row[1]
            row_count = row[2] or 0
            avg_row_size = float(row[3]) if row[3] is not None else None
            total_size = row[4] or 0
            table_data_size = row[5] or 0
            index_size = row[6] or 0
            bloat_ratio = float(row[7]) if row[7] is not None else 1.0
            n_tup_ins = row[8] or 0
            n_tup_upd = row[9] or 0
            n_tup_del = row[10] or 0

            current_total_bytes += total_size

            write_profile = _classify_write_profile(n_tup_ins, n_tup_upd, n_tup_del)

            # Growth history for this table
            growth_result = conn.execute(
                text("""
                    SELECT period_start, rows_added, cumulative_rows
                    FROM prediction.growth_history
                    WHERE run_id = :run_id AND table_name = :table AND schema_name = :schema
                    ORDER BY period_start
                """),
                {"run_id": run_id, "table": table_name, "schema": schema_name},
            )
            growth_rows = growth_result.fetchall()

            avg_monthly_growth = 0.0
            trend_direction = "stable"
            projected_6m = row_count
            projected_12m = row_count
            projected_24m = row_count

            if growth_rows and len(growth_rows) >= 2:
                x = [i for i in range(len(growth_rows))]
                y = [r[2] for r in growth_rows]  # cumulative_rows
                slope = _linear_regression_slope(x, y)
                avg_monthly_growth = sum(r[1] for r in growth_rows) / len(growth_rows)
                if slope > 0.1 * avg_monthly_growth:
                    trend_direction = "increasing"
                elif slope < -0.1 * avg_monthly_growth:
                    trend_direction = "decreasing"

                months_per_point = 1
                projected_6m = int(row_count + avg_monthly_growth * 6)
                projected_12m = int(row_count + avg_monthly_growth * 12)
                projected_24m = int(row_count + avg_monthly_growth * 24)
                projected_6m = max(0, projected_6m)
                projected_12m = max(0, projected_12m)
                projected_24m = max(0, projected_24m)

            # Storage projection
            index_overhead = (index_size / table_data_size) if table_data_size else 1.0
            if index_overhead < 0.1:
                index_overhead = 1.0
            bloat_factor = bloat_ratio if bloat_ratio and bloat_ratio > 0 else 1.0

            def _est_size(proj_rows: int) -> int:
                if avg_row_size and proj_rows > 0:
                    base = int(proj_rows * avg_row_size)
                    return int(base * index_overhead * bloat_factor)
                return total_size

            size_6m = _est_size(projected_6m)
            size_12m = _est_size(projected_12m)
            size_24m = _est_size(projected_24m)

            table_entry = {
                "table": table_name,
                "schema": schema_name,
                "current": {
                    "row_count": row_count,
                    "total_size_bytes": total_size,
                    "total_size_human": _format_bytes(total_size),
                    "avg_row_size_bytes": avg_row_size,
                    "bloat_ratio": bloat_ratio,
                    "write_profile": write_profile,
                },
                "growth": {
                    "avg_monthly_growth_rows": round(avg_monthly_growth, 2),
                    "trend_direction": trend_direction,
                    "data_points": len(growth_rows),
                },
                "projections": {
                    "6_month": {
                        "estimated_rows": projected_6m,
                        "estimated_size_bytes": size_6m,
                        "estimated_size_human": _format_bytes(size_6m),
                    },
                    "12_month": {
                        "estimated_rows": projected_12m,
                        "estimated_size_bytes": size_12m,
                        "estimated_size_human": _format_bytes(size_12m),
                    },
                    "24_month": {
                        "estimated_rows": projected_24m,
                        "estimated_size_bytes": size_24m,
                        "estimated_size_human": _format_bytes(size_24m),
                    },
                },
            }
            table_projections.append(table_entry)

        report["tables"] = table_projections

        # Summary
        total_current = current_total_bytes
        total_6m = sum(t["projections"]["6_month"]["estimated_size_bytes"] for t in table_projections)
        total_12m = sum(t["projections"]["12_month"]["estimated_size_bytes"] for t in table_projections)
        total_24m = sum(t["projections"]["24_month"]["estimated_size_bytes"] for t in table_projections)

        fastest = sorted(
            table_projections,
            key=lambda t: t["growth"]["avg_monthly_growth_rows"],
            reverse=True,
        )[:5]
        largest = sorted(
            table_projections,
            key=lambda t: t["current"]["total_size_bytes"] or 0,
            reverse=True,
        )[:5]

        report["summary"] = {
            "current_total_size_bytes": total_current,
            "current_total_size_human": _format_bytes(total_current),
            "projected_6_month_size_bytes": total_6m,
            "projected_6_month_size_human": _format_bytes(total_6m),
            "projected_12_month_size_bytes": total_12m,
            "projected_12_month_size_human": _format_bytes(total_12m),
            "projected_24_month_size_bytes": total_24m,
            "projected_24_month_size_human": _format_bytes(total_24m),
            "tables_analyzed": len(table_projections),
            "fastest_growing_tables": [
                {"table": t["table"], "avg_monthly_growth": t["growth"]["avg_monthly_growth_rows"]}
                for t in fastest
            ],
            "largest_tables": [
                {"table": t["table"], "size_human": t["current"]["total_size_human"]}
                for t in largest
            ],
        }

    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(report, f, indent=2, default=str)

    logger.info(f"Capacity report written to {output_path}")


def main() -> None:
    _load_env_file()
    parser = argparse.ArgumentParser(description="Volume projection predictor")
    parser.add_argument("database_url", nargs="?", default=os.environ.get("DATABASE_URL"))
    parser.add_argument("output_path", nargs="?", default="capacity_report.json")
    args = parser.parse_args()

    if not args.database_url:
        parser.error("database_url required (argument or DATABASE_URL env)")

    engine = get_engine(args.database_url)
    run_predict(engine, args.output_path)


if __name__ == "__main__":
    main()
