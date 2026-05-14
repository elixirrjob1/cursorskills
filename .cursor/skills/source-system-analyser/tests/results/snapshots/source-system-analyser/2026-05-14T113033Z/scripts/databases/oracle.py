"""Oracle dialect adapter."""

import logging
from typing import Dict, List, Set

from sqlalchemy import text
from sqlalchemy.engine import Engine

from .base import DialectAdapter

logger = logging.getLogger(__name__)


class OracleAdapter(DialectAdapter):
    """Oracle dialect adapter."""

    def quote_identifier(self, name: str) -> str:
        return f'"{name}"'

    def default_schema(self) -> str:
        return "USER"

    def resolve_default_schema(self, engine: Engine) -> str:
        try:
            with engine.connect() as conn:
                return conn.execute(text("SELECT USER FROM DUAL")).scalar() or "USER"
        except Exception:
            return "USER"

    def fetch_database_timezone(self, engine: Engine) -> str:
        try:
            with engine.connect() as conn:
                tz = conn.execute(text("SELECT SESSIONTIMEZONE FROM DUAL")).scalar()
                if tz:
                    return str(tz)
                tz = conn.execute(text("SELECT DBTIMEZONE FROM DUAL")).scalar()
                return str(tz) if tz else "Unknown"
        except Exception:
            return "Unknown"

    def fetch_check_constraints(self, engine: Engine, schema: str) -> Dict[str, List[Dict]]:
        result = {}
        query = text("""
            SELECT ac.TABLE_NAME, acc.COLUMN_NAME, ac.CONSTRAINT_NAME, ac.SEARCH_CONDITION_VC
            FROM ALL_CONSTRAINTS ac
            JOIN ALL_CONS_COLUMNS acc ON ac.CONSTRAINT_NAME = acc.CONSTRAINT_NAME
                AND ac.OWNER = acc.OWNER
            WHERE ac.CONSTRAINT_TYPE = 'C'
                AND ac.OWNER = :schema
                AND ac.TABLE_NAME NOT LIKE 'BIN$%'
                AND ac.SEARCH_CONDITION_VC IS NOT NULL
                AND ac.CONSTRAINT_NAME NOT LIKE 'SYS_%'
                AND ac.SEARCH_CONDITION_VC NOT LIKE '%IS NOT NULL%'
        """)
        try:
            with engine.connect() as conn:
                for row in conn.execute(query, {"schema": schema.upper()}).fetchall():
                    result.setdefault(row[0], []).append({
                        "column": row[1], "constraint_name": row[2], "check_clause": row[3] or ""
                    })
        except Exception as e:
            logger.warning(f"Could not fetch CHECK constraints: {e}")
        return result

    def fetch_enum_columns(self, engine: Engine, schema: str) -> Dict[str, Dict[str, List[str]]]:
        return {}

    def fetch_unique_constraints(self, engine: Engine, schema: str) -> Dict[str, Set[str]]:
        result = {}
        query = text("""
            SELECT ac.TABLE_NAME, acc.COLUMN_NAME
            FROM ALL_CONSTRAINTS ac
            JOIN ALL_CONS_COLUMNS acc ON ac.CONSTRAINT_NAME = acc.CONSTRAINT_NAME
                AND ac.OWNER = acc.OWNER
            WHERE ac.CONSTRAINT_TYPE = 'U' AND ac.OWNER = :schema
                AND ac.TABLE_NAME NOT LIKE 'BIN$%'
        """)
        try:
            with engine.connect() as conn:
                for row in conn.execute(query, {"schema": schema.upper()}).fetchall():
                    result.setdefault(row[0], set()).add(row[1])
        except Exception as e:
            logger.warning(f"Could not fetch UNIQUE constraints: {e}")
        return result

    def detect_cdc_enabled(self, engine: Engine, table_name: str, schema: str) -> bool:
        return False

    def fetch_table_descriptions(self, engine: Engine, schema: str) -> Dict[str, str]:
        result: Dict[str, str] = {}
        query = text(
            """
            SELECT TABLE_NAME, COMMENTS
            FROM ALL_TAB_COMMENTS
            WHERE OWNER = :schema
            """
        )
        try:
            with engine.connect() as conn:
                for row in conn.execute(query, {"schema": schema.upper()}).fetchall():
                    if row[1]:
                        result[str(row[0])] = str(row[1])
        except Exception as e:
            logger.warning(f"Could not fetch table descriptions: {e}")
        return result

    def fetch_column_descriptions(self, engine: Engine, schema: str) -> Dict[str, Dict[str, str]]:
        result: Dict[str, Dict[str, str]] = {}
        query = text(
            """
            SELECT TABLE_NAME, COLUMN_NAME, COMMENTS
            FROM ALL_COL_COMMENTS
            WHERE OWNER = :schema
            """
        )
        try:
            with engine.connect() as conn:
                for row in conn.execute(query, {"schema": schema.upper()}).fetchall():
                    if row[2]:
                        result.setdefault(str(row[0]), {})[str(row[1])] = str(row[2])
        except Exception as e:
            logger.warning(f"Could not fetch column descriptions: {e}")
        return result

    def detect_partition_columns(
        self, engine: Engine, table_name: str, schema: str, columns: List[Dict]
    ) -> List[str]:
        try:
            with engine.connect() as conn:
                rows = conn.execute(text("""
                    SELECT COLUMN_NAME FROM ALL_PART_KEY_COLUMNS
                    WHERE OWNER = :schema AND NAME = :table AND OBJECT_TYPE = 'TABLE'
                    ORDER BY COLUMN_POSITION
                """), {"schema": schema.upper(), "table": table_name.upper()}).fetchall()
                if rows:
                    return [r[0] for r in rows]
        except Exception:
            pass
        return super().detect_partition_columns(engine, table_name, schema, columns)

    def limit_clause(self, limit: int) -> str:
        return f"FETCH FIRST {limit} ROWS ONLY"

    def build_select_limit_query(self, schema: str, table: str, limit: int) -> tuple:
        qt = self.quote_table(schema, table) if schema else self.quote_identifier(table)
        return (f"SELECT * FROM {qt} FETCH FIRST {limit} ROWS ONLY", {})

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
        # Use CAST to DATE so (ts - date) yields numeric days; * 24 = hours.
        # Avoids ORA-00932 when mixing TIMESTAMP and DATE (which yields INTERVAL).
        return f"""
            SELECT COUNT(*) AS total,
                SUM(CASE WHEN lh > 24 THEN 1 ELSE 0 END) AS late_1d,
                SUM(CASE WHEN lh > 168 THEN 1 ELSE 0 END) AS late_7d,
                ROUND(MIN(lh), 2) AS min_h, ROUND(AVG(lh), 2) AS avg_h,
                ROUND(PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY lh), 2) AS p95_h,
                ROUND(MAX(lh), 2) AS max_h
            FROM (
                SELECT (CAST({q_sys} AS DATE) - CAST({q_biz} AS DATE)) * 24 AS lh
                FROM {qt}
                WHERE {q_sys} IS NOT NULL AND {q_biz} IS NOT NULL
            ) sub
            WHERE lh >= 0
        """

    def supports_late_arriving_check(self) -> bool:
        return True

    def supports_nulls_first(self) -> bool:
        return True
