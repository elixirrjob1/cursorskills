"""Microsoft SQL Server / Azure SQL dialect adapter."""

import logging
from typing import Dict, List, Optional, Set

from sqlalchemy import text
from sqlalchemy.engine import Engine

from .base import DialectAdapter

logger = logging.getLogger(__name__)


class MssqlAdapter(DialectAdapter):
    """Microsoft SQL Server / Azure SQL dialect adapter."""

    def quote_identifier(self, name: str) -> str:
        return f"[{name}]"

    def default_schema(self) -> str:
        return "dbo"

    def fetch_database_timezone(self, engine: Engine) -> str:
        try:
            with engine.connect() as conn:
                return conn.execute(text("SELECT CURRENT_TIMEZONE()")).scalar() or "Unknown"
        except Exception:
            return "Unknown"

    def fetch_check_constraints(self, engine: Engine, schema: str) -> Dict[str, List[Dict]]:
        result = {}
        query = text("""
            SELECT tc.TABLE_NAME, ccu.COLUMN_NAME, tc.CONSTRAINT_NAME, cc.CHECK_CLAUSE
            FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS tc
            JOIN INFORMATION_SCHEMA.CHECK_CONSTRAINTS cc
                ON tc.CONSTRAINT_NAME = cc.CONSTRAINT_NAME
                AND tc.CONSTRAINT_SCHEMA = cc.CONSTRAINT_SCHEMA
            JOIN INFORMATION_SCHEMA.CONSTRAINT_COLUMN_USAGE ccu
                ON tc.CONSTRAINT_NAME = ccu.CONSTRAINT_NAME
                AND tc.TABLE_SCHEMA = ccu.TABLE_SCHEMA
            WHERE tc.CONSTRAINT_TYPE = 'CHECK'
                AND tc.TABLE_SCHEMA = :schema
                AND tc.CONSTRAINT_NAME NOT LIKE '%_not_null'
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
        return {}

    def fetch_unique_constraints(self, engine: Engine, schema: str) -> Dict[str, Set[str]]:
        result = {}
        query = text("""
            SELECT tc.TABLE_NAME, kcu.COLUMN_NAME
            FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS tc
            JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE kcu
                ON tc.CONSTRAINT_NAME = kcu.CONSTRAINT_NAME
                AND tc.TABLE_SCHEMA = kcu.TABLE_SCHEMA
            WHERE tc.CONSTRAINT_TYPE = 'UNIQUE' AND tc.TABLE_SCHEMA = :schema
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
                row = conn.execute(text("""
                    SELECT 1 FROM sys.change_tracking_tables ct
                    JOIN sys.tables t ON ct.object_id = t.object_id
                    JOIN sys.schemas s ON t.schema_id = s.schema_id
                    WHERE s.name = :schema AND t.name = :table
                """), {"schema": schema, "table": table_name}).fetchone()
                if row:
                    return True
                row = conn.execute(text("""
                    SELECT 1 FROM cdc.change_tables ct
                    JOIN sys.tables t ON ct.source_object_id = t.object_id
                    JOIN sys.schemas s ON t.schema_id = s.schema_id
                    WHERE s.name = :schema AND t.name = :table
                """), {"schema": schema, "table": table_name}).fetchone()
                return row is not None
        except Exception:
            return False

    def fetch_table_descriptions(self, engine: Engine, schema: str) -> Dict[str, str]:
        result: Dict[str, str] = {}
        query = text(
            """
            SELECT t.name AS table_name, CAST(ep.value AS nvarchar(max)) AS description
            FROM sys.tables t
            JOIN sys.schemas s ON s.schema_id = t.schema_id
            LEFT JOIN sys.extended_properties ep
              ON ep.major_id = t.object_id
             AND ep.minor_id = 0
             AND ep.name = 'MS_Description'
            WHERE s.name = :schema
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
            SELECT t.name AS table_name, c.name AS column_name, CAST(ep.value AS nvarchar(max)) AS description
            FROM sys.tables t
            JOIN sys.schemas s ON s.schema_id = t.schema_id
            JOIN sys.columns c ON c.object_id = t.object_id
            LEFT JOIN sys.extended_properties ep
              ON ep.major_id = c.object_id
             AND ep.minor_id = c.column_id
             AND ep.name = 'MS_Description'
            WHERE s.name = :schema
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
            with engine.connect() as conn:
                row = conn.execute(text("""
                    SELECT c.name
                    FROM sys.indexes i
                    JOIN sys.index_columns ic ON i.object_id = ic.object_id AND i.index_id = ic.index_id
                    JOIN sys.columns c ON ic.object_id = c.object_id AND ic.column_id = c.column_id
                    JOIN sys.tables t ON i.object_id = t.object_id
                    JOIN sys.schemas s ON t.schema_id = s.schema_id
                    WHERE s.name = :schema AND t.name = :table
                        AND i.type = 1
                        AND i.data_space_id IN (SELECT data_space_id FROM sys.data_spaces WHERE type = 'P')
                    ORDER BY ic.key_ordinal
                """), {"schema": schema, "table": table_name}).fetchall()
                if row:
                    return [r[0] for r in row]
        except Exception:
            pass
        return super().detect_partition_columns(engine, table_name, schema, columns)

    def limit_clause(self, limit: int) -> str:
        return f"TOP {limit}"

    def build_select_limit_query(self, schema: str, table: str, limit: int) -> tuple:
        qt = self.quote_table(schema, table) if schema else self.quote_identifier(table)
        return (f"SELECT TOP {limit} * FROM {qt}", {})

    def get_late_arriving_biz_expr(self, biz_name: str, biz_type: str) -> Optional[str]:
        """MSSQL TIMESTAMP is rowversion, not datetime. Use column directly; DATEDIFF works with DATE/DATETIME2."""
        return self.quote_column(biz_name)

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
            SELECT COUNT(*) AS total,
                SUM(CASE WHEN lh > 24 THEN 1 ELSE 0 END) AS late_1d,
                SUM(CASE WHEN lh > 168 THEN 1 ELSE 0 END) AS late_7d,
                ROUND(MIN(lh), 2) AS min_h, ROUND(AVG(lh), 2) AS avg_h,
                ROUND((SELECT MIN(p95) FROM (
                    SELECT PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY lh) OVER () AS p95
                    FROM (SELECT DATEDIFF(SECOND, {biz_expr}, {q_sys}) / 3600.0 AS lh
                          FROM {qt} WHERE {q_sys} IS NOT NULL AND {q_biz} IS NOT NULL) sub
                    WHERE lh >= 0
                ) p), 2) AS p95_h,
                ROUND(MAX(lh), 2) AS max_h
            FROM (
                SELECT DATEDIFF(SECOND, {biz_expr}, {q_sys}) / 3600.0 AS lh
                FROM {qt}
                WHERE {q_sys} IS NOT NULL AND {q_biz} IS NOT NULL
            ) sub
            WHERE lh >= 0
        """

    def supports_late_arriving_check(self) -> bool:
        return True

    def supports_nulls_first(self) -> bool:
        return False
