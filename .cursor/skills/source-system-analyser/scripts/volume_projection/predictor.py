#!/usr/bin/env python3
"""Volume projection predictor for PostgreSQL, MSSQL, and Oracle."""

from __future__ import annotations

import argparse
import json
import logging
import os
from datetime import datetime, timezone
from pathlib import Path

from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)


def _load_env_file() -> None:
    for base in (Path.cwd(), Path(__file__).resolve().parents[4]):
        env_path = base / ".env"
        if env_path.exists():
            try:
                with open(env_path, encoding="utf-8") as f:
                    for line in f:
                        line = line.strip()
                        if line and not line.startswith("#") and "=" in line:
                            k, _, v = line.partition("=")
                            k = k.strip()
                            v = v.strip().strip('"').strip("'")
                            if k and k not in os.environ:
                                os.environ[k] = v
            except Exception:
                pass
            break


def get_engine(database_url: str) -> Engine:
    connect_args = {}
    if database_url.startswith(("postgresql://", "postgresql+")):
        connect_args = {"connect_timeout": 10}
    return create_engine(database_url, pool_pre_ping=True, connect_args=connect_args, echo=False)


def _pred_table(dialect: str, name: str) -> str:
    if dialect == "oracle":
        return f"prediction_{name}"
    return f"prediction.{name}"


def _format_bytes(n: int | None) -> str:
    if not n or n < 0:
        return "0 B"
    val = float(n)
    for unit in ("B", "KB", "MB", "GB", "TB"):
        if val < 1024:
            return f"{val:.1f} {unit}"
        val /= 1024
    return f"{val:.1f} PB"


def _linear_slope(x: list[float], y: list[float]) -> float:
    n = len(x)
    if n < 2:
        return 0.0
    xm = sum(x) / n
    ym = sum(y) / n
    num = sum((x[i] - xm) * (y[i] - ym) for i in range(n))
    den = sum((x[i] - xm) ** 2 for i in range(n))
    return 0.0 if den == 0 else num / den


def _write_profile(ins: int, upd: int, dele: int) -> str:
    total = ins + upd + dele
    if total <= 0:
        return "unknown"
    if ins / total > 0.8:
        return "append_only"
    if upd / total > 0.5:
        return "update_heavy"
    if dele / total > 0.3:
        return "delete_heavy"
    return "mixed"


def run_predict(engine: Engine, output_path: str) -> None:
    d = engine.dialect.name
    cr = _pred_table(d, "collection_runs")
    ts = _pred_table(d, "table_size_snapshots")
    gh = _pred_table(d, "growth_history")
    ds = _pred_table(d, "database_snapshots")

    report = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "dialect": d,
        "summary": {},
        "tables": [],
        "database": {},
    }

    with engine.connect() as conn:
        run_row = conn.execute(
            text(f"SELECT run_id, started_at FROM {cr} WHERE status = 'success' ORDER BY run_id DESC")
        ).fetchone()

        if not run_row:
            report["error"] = "No successful collection run found. Run collector first."
            with open(output_path, "w", encoding="utf-8") as f:
                json.dump(report, f, indent=2, default=str)
            return

        run_id = int(run_row[0])
        report["collection_run_id"] = run_id
        report["collection_run_at"] = str(run_row[1]) if run_row[1] else None

        db_row = conn.execute(
            text(
                f"""
                SELECT total_database_size_bytes, shared_buffers, work_mem,
                       max_connections, temp_files_count, temp_bytes
                FROM {ds}
                WHERE run_id = :run_id
                ORDER BY db_snapshot_id DESC
                """
            ),
            {"run_id": run_id},
        ).fetchone()

        if db_row:
            report["database"] = {
                "total_size_bytes": db_row[0],
                "total_size_human": _format_bytes(db_row[0]),
                "shared_buffers": db_row[1],
                "work_mem": db_row[2],
                "max_connections": db_row[3],
                "temp_files_count": db_row[4],
                "temp_bytes": db_row[5],
                "temp_size_human": _format_bytes(db_row[5]),
            }

        snapshots = conn.execute(
            text(
                f"""
                SELECT table_name, schema_name, row_count, avg_row_size_bytes,
                       total_size_bytes, table_data_size_bytes, index_size_bytes,
                       bloat_ratio, n_tup_ins, n_tup_upd, n_tup_del
                FROM {ts}
                WHERE run_id = :run_id
                ORDER BY total_size_bytes DESC
                """
            ),
            {"run_id": run_id},
        ).fetchall()

        tables = []
        current_total = 0

        for row in snapshots:
            table_name = row[0]
            schema_name = row[1]
            row_count = int(row[2] or 0)
            avg_row_size = float(row[3]) if row[3] is not None else None
            total_size = int(row[4] or 0)
            table_data_size = int(row[5] or 0)
            index_size = int(row[6] or 0)
            bloat_ratio = float(row[7]) if row[7] is not None else 1.0
            ins = int(row[8] or 0)
            upd = int(row[9] or 0)
            dele = int(row[10] or 0)

            current_total += total_size
            write_profile = _write_profile(ins, upd, dele)

            growth_rows = conn.execute(
                text(
                    f"""
                    SELECT period_start, rows_added, cumulative_rows
                    FROM {gh}
                    WHERE run_id = :run_id AND table_name = :table_name AND schema_name = :schema_name
                    ORDER BY period_start
                    """
                ),
                {"run_id": run_id, "table_name": table_name, "schema_name": schema_name},
            ).fetchall()

            avg_monthly_growth = 0.0
            trend = "stable"
            r6 = r12 = r24 = row_count

            if len(growth_rows) >= 2:
                x = [float(i) for i in range(len(growth_rows))]
                y = [float(g[2] or 0) for g in growth_rows]
                slope = _linear_slope(x, y)
                avg_monthly_growth = sum(float(g[1] or 0) for g in growth_rows) / len(growth_rows)

                if slope > 0.1 * avg_monthly_growth:
                    trend = "increasing"
                elif slope < -0.1 * avg_monthly_growth:
                    trend = "decreasing"

                r6 = max(0, int(row_count + avg_monthly_growth * 6))
                r12 = max(0, int(row_count + avg_monthly_growth * 12))
                r24 = max(0, int(row_count + avg_monthly_growth * 24))

            index_overhead = (index_size / table_data_size) if table_data_size else 1.0
            if index_overhead < 0.1:
                index_overhead = 1.0
            bloat_factor = bloat_ratio if bloat_ratio > 0 else 1.0

            def est_size(rows_est: int) -> int:
                if avg_row_size and rows_est > 0:
                    return int(rows_est * avg_row_size * index_overhead * bloat_factor)
                return total_size

            s6 = est_size(r6)
            s12 = est_size(r12)
            s24 = est_size(r24)

            tables.append(
                {
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
                        "trend_direction": trend,
                        "data_points": len(growth_rows),
                    },
                    "projections": {
                        "6_month": {"estimated_rows": r6, "estimated_size_bytes": s6, "estimated_size_human": _format_bytes(s6)},
                        "12_month": {"estimated_rows": r12, "estimated_size_bytes": s12, "estimated_size_human": _format_bytes(s12)},
                        "24_month": {"estimated_rows": r24, "estimated_size_bytes": s24, "estimated_size_human": _format_bytes(s24)},
                    },
                }
            )

        report["tables"] = tables

        total_6 = sum(t["projections"]["6_month"]["estimated_size_bytes"] for t in tables)
        total_12 = sum(t["projections"]["12_month"]["estimated_size_bytes"] for t in tables)
        total_24 = sum(t["projections"]["24_month"]["estimated_size_bytes"] for t in tables)

        fastest = sorted(tables, key=lambda t: t["growth"]["avg_monthly_growth_rows"], reverse=True)[:5]
        largest = sorted(tables, key=lambda t: t["current"]["total_size_bytes"], reverse=True)[:5]

        report["summary"] = {
            "current_total_size_bytes": current_total,
            "current_total_size_human": _format_bytes(current_total),
            "projected_6_month_size_bytes": total_6,
            "projected_6_month_size_human": _format_bytes(total_6),
            "projected_12_month_size_bytes": total_12,
            "projected_12_month_size_human": _format_bytes(total_12),
            "projected_24_month_size_bytes": total_24,
            "projected_24_month_size_human": _format_bytes(total_24),
            "tables_analyzed": len(tables),
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

    logger.info("Wrote capacity report: %s", output_path)


def main() -> None:
    _load_env_file()
    parser = argparse.ArgumentParser(description="Volume projection predictor")
    parser.add_argument("database_url", nargs="?", default=os.environ.get("DATABASE_URL"))
    parser.add_argument("output", nargs="?", default="capacity_report.json")
    args = parser.parse_args()

    if not args.database_url:
        parser.error("database_url required (argument or DATABASE_URL env)")

    run_predict(get_engine(args.database_url), args.output)


if __name__ == "__main__":
    main()
