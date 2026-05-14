"""PostgreSQL dialect adapter."""

import logging
from typing import Any, Dict, List, Set

from sqlalchemy import text
from sqlalchemy.engine import Engine

from .base import DialectAdapter

logger = logging.getLogger(__name__)


class PostgresqlAdapter(DialectAdapter):
    """PostgreSQL dialect adapter."""

    def quote_identifier(self, name: str) -> str:
        return f'"{name}"'

    def default_schema(self) -> str:
        return "public"

    def fetch_database_timezone(self, engine: Engine) -> str:
        try:
            with engine.connect() as conn:
                return conn.execute(text("SHOW timezone")).scalar() or "Unknown"
        except Exception:
            return "Unknown"

    def fetch_check_constraints(self, engine: Engine, schema: str) -> Dict[str, List[Dict]]:
        result = {}
        query = text("""
            SELECT tc.table_name, ccu.column_name, tc.constraint_name, cc.check_clause
            FROM information_schema.table_constraints tc
            JOIN information_schema.check_constraints cc ON tc.constraint_name = cc.constraint_name AND tc.constraint_schema = cc.constraint_schema
            JOIN information_schema.constraint_column_usage ccu ON tc.constraint_name = ccu.constraint_name AND tc.constraint_schema = ccu.constraint_schema
            WHERE tc.constraint_type = 'CHECK' AND tc.table_schema = :schema AND tc.constraint_name NOT LIKE '%_not_null'
        """)
        try:
            with engine.connect() as conn:
                for row in conn.execute(query, {"schema": schema}).fetchall():
                    result.setdefault(row[0], []).append({
                        "column": row[1], "constraint_name": row[2], "check_clause": row[3]
                    })
        except Exception as e:
            logger.warning(f"Could not fetch CHECK constraints: {e}")
        return result

    def fetch_enum_columns(self, engine: Engine, schema: str) -> Dict[str, Dict[str, List[str]]]:
        result = {}
        query = text("""
            SELECT c.table_name, c.column_name, c.udt_name, array_agg(e.enumlabel ORDER BY e.enumsortorder) AS enum_values
            FROM information_schema.columns c
            JOIN pg_type t ON t.typname = c.udt_name
            JOIN pg_enum e ON e.enumtypid = t.oid
            WHERE c.table_schema = :schema AND c.data_type = 'USER-DEFINED'
            GROUP BY c.table_name, c.column_name, c.udt_name
        """)
        try:
            with engine.connect() as conn:
                for row in conn.execute(query, {"schema": schema}).fetchall():
                    result.setdefault(row[0], {})[row[1]] = list(row[3])
        except Exception as e:
            logger.warning(f"Could not fetch ENUM columns: {e}")
        return result

    def fetch_unique_constraints(self, engine: Engine, schema: str) -> Dict[str, Set[str]]:
        result = {}
        query = text("""
            SELECT tc.table_name, kcu.column_name
            FROM information_schema.table_constraints tc
            JOIN information_schema.key_column_usage kcu ON tc.constraint_name = kcu.constraint_name AND tc.table_schema = kcu.table_schema
            WHERE tc.constraint_type = 'UNIQUE' AND tc.table_schema = :schema
        """)
        try:
            with engine.connect() as conn:
                for row in conn.execute(query, {"schema": schema}).fetchall():
                    result.setdefault(row[0], set()).add(row[1])
        except Exception as e:
            logger.warning(f"Could not fetch UNIQUE constraints: {e}")
        return result

    def detect_cdc_enabled(self, engine: Engine, table_name: str, schema: str) -> bool:
        try:
            with engine.connect() as conn:
                row = conn.execute(text(
                    "SELECT c.relreplident FROM pg_class c "
                    "JOIN pg_namespace n ON n.oid = c.relnamespace "
                    "WHERE n.nspname = :schema AND c.relname = :table"
                ), {"schema": schema, "table": table_name}).fetchone()
                return row and row[0] in ('f', 'i')
        except Exception:
            return False

    def fetch_table_descriptions(self, engine: Engine, schema: str) -> Dict[str, str]:
        result: Dict[str, str] = {}
        query = text(
            """
            SELECT c.relname AS table_name, obj_description(c.oid, 'pg_class') AS description
            FROM pg_class c
            JOIN pg_namespace n ON n.oid = c.relnamespace
            WHERE n.nspname = :schema AND c.relkind = 'r'
            """
        )
        try:
            with engine.connect() as conn:
                for row in conn.execute(query, {"schema": schema}).fetchall():
                    if row[1]:
                        result[str(row[0])] = str(row[1])
        except Exception as e:
            logger.warning(f"Could not fetch table descriptions: {e}")
        return result

    def fetch_column_descriptions(self, engine: Engine, schema: str) -> Dict[str, Dict[str, str]]:
        result: Dict[str, Dict[str, str]] = {}
        query = text(
            """
            SELECT c.relname AS table_name, a.attname AS column_name, col_description(a.attrelid, a.attnum) AS description
            FROM pg_class c
            JOIN pg_namespace n ON n.oid = c.relnamespace
            JOIN pg_attribute a ON a.attrelid = c.oid
            WHERE n.nspname = :schema
              AND c.relkind = 'r'
              AND a.attnum > 0
              AND NOT a.attisdropped
            """
        )
        try:
            with engine.connect() as conn:
                for row in conn.execute(query, {"schema": schema}).fetchall():
                    if row[2]:
                        result.setdefault(str(row[0]), {})[str(row[1])] = str(row[2])
        except Exception as e:
            logger.warning(f"Could not fetch column descriptions: {e}")
        return result

    def detect_partition_columns(
        self, engine: Engine, table_name: str, schema: str, columns: List[Dict]
    ) -> List[str]:
        try:
            q = text("""
                SELECT a.attname FROM pg_partitioned_table pt
                JOIN pg_class c ON c.oid = pt.partrelid
                JOIN pg_namespace n ON n.oid = c.relnamespace
                JOIN pg_attribute a ON a.attrelid = c.oid AND a.attnum = ANY(pt.partattrs::smallint[])
                WHERE c.relname = :tbl AND n.nspname = :sch
                ORDER BY a.attnum
            """)
            with engine.connect() as conn:
                rows = conn.execute(q, {"tbl": table_name, "sch": schema}).fetchall()
            if rows:
                return [r[0] for r in rows]
        except Exception:
            pass
        # Fallback to heuristic from column names/types
        return super().detect_partition_columns(engine, table_name, schema, columns)

    def limit_clause(self, limit: int) -> str:
        return f"LIMIT {limit}"

    def build_select_limit_query(self, schema: str, table: str, limit: int) -> tuple:
        qt = self.quote_table(schema, table) if schema else self.quote_identifier(table)
        return (f"SELECT * FROM {qt} LIMIT :limit", {"limit": limit})

    def build_late_arriving_query(
        self,
        table_name: str,
        schema: str,
        biz_col: str,
        sys_col: str,
        biz_expr: str,
    ) -> str:
        qt = self.quote_table(schema, table_name)
        q_sys = self.quote_column(sys_col)
        q_biz = self.quote_column(biz_col)
        return f"""
            SELECT COUNT(*) AS total, COUNT(*) FILTER (WHERE lh > 24) AS late_1d, COUNT(*) FILTER (WHERE lh > 168) AS late_7d,
                   ROUND(MIN(lh)::numeric, 2) AS min_h, ROUND(AVG(lh)::numeric, 2) AS avg_h,
                   ROUND(PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY lh)::numeric, 2) AS p95_h, ROUND(MAX(lh)::numeric, 2) AS max_h
            FROM (SELECT EXTRACT(EPOCH FROM ({q_sys} - {biz_expr}))/3600.0 AS lh FROM {qt} WHERE {q_sys} IS NOT NULL AND {q_biz} IS NOT NULL) sub
            WHERE lh >= 0
        """

    def supports_late_arriving_check(self) -> bool:
        return True

    def supports_nulls_first(self) -> bool:
        return True
