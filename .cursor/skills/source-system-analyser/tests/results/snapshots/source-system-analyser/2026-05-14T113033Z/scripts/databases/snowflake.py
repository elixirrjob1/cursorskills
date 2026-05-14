"""Snowflake dialect adapter (minimal; supports introspection + basic constraints)."""

from __future__ import annotations

import logging
from typing import Dict, List, Set

from sqlalchemy import text
from sqlalchemy.engine import Engine

from .base import DialectAdapter

logger = logging.getLogger(__name__)


class SnowflakeAdapter(DialectAdapter):
    """Snowflake — INFORMATION_SCHEMA + session metadata."""

    def quote_identifier(self, name: str) -> str:
        return f'"{name}"'

    def default_schema(self) -> str:
        return "PUBLIC"

    def resolve_default_schema(self, engine: Engine) -> str:
        try:
            with engine.connect() as conn:
                cur = conn.execute(text("SELECT CURRENT_SCHEMA()")).scalar()
                return str(cur or "PUBLIC")
        except Exception:
            return "PUBLIC"

    def fetch_database_timezone(self, engine: Engine) -> str:
        try:
            with engine.connect() as conn:
                return str(conn.execute(text("SELECT CURRENT_TIMEZONE()")).scalar() or "UTC")
        except Exception:
            return "UTC"

    def fetch_check_constraints(self, engine: Engine, schema: str) -> Dict[str, List[Dict]]:
        result: Dict[str, List[Dict]] = {}
        query = text(
            """
            SELECT tc.table_name, ccu.column_name, tc.constraint_name, cc.check_clause
            FROM information_schema.table_constraints tc
            JOIN information_schema.check_constraints cc
              ON tc.constraint_catalog = cc.constraint_catalog
             AND tc.constraint_schema = cc.constraint_schema
             AND tc.constraint_name = cc.constraint_name
            JOIN information_schema.constraint_column_usage ccu
              ON tc.constraint_catalog = ccu.constraint_catalog
             AND tc.constraint_schema = ccu.constraint_schema
             AND tc.constraint_name = ccu.constraint_name
            WHERE tc.constraint_type = 'CHECK'
              AND UPPER(tc.table_schema) = UPPER(:schema)
              AND tc.table_catalog = CURRENT_DATABASE()
              AND tc.constraint_name NOT ILIKE '%_NOT_NULL'
            """
        )
        try:
            with engine.connect() as conn:
                for row in conn.execute(query, {"schema": schema}).fetchall():
                    result.setdefault(str(row[0]), []).append(
                        {
                            "column": row[1],
                            "constraint_name": row[2],
                            "check_clause": row[3],
                        }
                    )
        except Exception as e:
            logger.warning("Could not fetch CHECK constraints: %s", e)
        return result

    def fetch_enum_columns(self, engine: Engine, schema: str) -> Dict[str, Dict[str, List[str]]]:
        return {}

    def fetch_unique_constraints(self, engine: Engine, schema: str) -> Dict[str, Set[str]]:
        result: Dict[str, Set[str]] = {}
        query = text(
            """
            SELECT tc.table_name, kcu.column_name
            FROM information_schema.table_constraints tc
            JOIN information_schema.key_column_usage kcu
              ON tc.constraint_catalog = kcu.constraint_catalog
             AND tc.constraint_schema = kcu.constraint_schema
             AND tc.constraint_name = kcu.constraint_name
            WHERE tc.constraint_type = 'UNIQUE'
              AND UPPER(tc.table_schema) = UPPER(:schema)
              AND tc.table_catalog = CURRENT_DATABASE()
            """
        )
        try:
            with engine.connect() as conn:
                for row in conn.execute(query, {"schema": schema}).fetchall():
                    result.setdefault(str(row[0]), set()).add(str(row[1]))
        except Exception as e:
            logger.warning("Could not fetch UNIQUE constraints: %s", e)
        return result

    def detect_cdc_enabled(self, engine: Engine, table_name: str, schema: str) -> bool:
        return False

    def fetch_table_descriptions(self, engine: Engine, schema: str) -> Dict[str, str]:
        result: Dict[str, str] = {}
        query = text(
            """
            SELECT table_name, comment
            FROM information_schema.tables
            WHERE table_catalog = CURRENT_DATABASE()
              AND UPPER(table_schema) = UPPER(:schema)
              AND table_type = 'BASE TABLE'
            """
        )
        try:
            with engine.connect() as conn:
                for row in conn.execute(query, {"schema": schema}).fetchall():
                    if row[1]:
                        result[str(row[0])] = str(row[1])
        except Exception as e:
            logger.warning("Could not fetch table descriptions: %s", e)
        return result

    def fetch_column_descriptions(self, engine: Engine, schema: str) -> Dict[str, Dict[str, str]]:
        result: Dict[str, Dict[str, str]] = {}
        query = text(
            """
            SELECT table_name, column_name, comment
            FROM information_schema.columns
            WHERE table_catalog = CURRENT_DATABASE()
              AND UPPER(table_schema) = UPPER(:schema)
            """
        )
        try:
            with engine.connect() as conn:
                for row in conn.execute(query, {"schema": schema}).fetchall():
                    if row[2]:
                        result.setdefault(str(row[0]), {})[str(row[1])] = str(row[2])
        except Exception as e:
            logger.warning("Could not fetch column descriptions: %s", e)
        return result

    def limit_clause(self, limit: int) -> str:
        return f"LIMIT {int(limit)}"

    def supports_late_arriving_check(self) -> bool:
        return False

    def supports_nulls_first(self) -> bool:
        return True
