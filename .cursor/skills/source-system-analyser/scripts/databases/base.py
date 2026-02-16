"""
Dialect adapter base class for multi-database support.

Each database (PostgreSQL, MSSQL, Oracle) implements this interface to provide
dialect-specific SQL generation and introspection.
"""

from abc import ABC, abstractmethod
from typing import Any, Dict, List, Optional, Set

from sqlalchemy.engine import Engine


class DialectAdapter(ABC):
    """Abstract base for database dialect adapters."""

    @abstractmethod
    def quote_identifier(self, name: str) -> str:
        """Quote a single identifier (table, column, schema)."""
        pass

    def quote_table(self, schema: str, table: str) -> str:
        """Quote schema.table for use in FROM/JOIN clauses."""
        if schema:
            return f"{self.quote_identifier(schema)}.{self.quote_identifier(table)}"
        return self.quote_identifier(table)

    def quote_column(self, col: str) -> str:
        """Quote a column name."""
        return self.quote_identifier(col)

    @abstractmethod
    def default_schema(self) -> str:
        """Return the default schema name for this dialect."""
        pass

    def resolve_default_schema(self, engine: Engine) -> str:
        """Resolve default schema at runtime (e.g. Oracle USER). Override if needed."""
        return self.default_schema()

    @abstractmethod
    def fetch_database_timezone(self, engine: Engine) -> str:
        """Fetch the database server timezone."""
        pass

    @abstractmethod
    def fetch_check_constraints(self, engine: Engine, schema: str) -> Dict[str, List[Dict]]:
        """Fetch CHECK constraints grouped by table. Returns {table: [{column, constraint_name, check_clause}]}."""
        pass

    @abstractmethod
    def fetch_enum_columns(self, engine: Engine, schema: str) -> Dict[str, Dict[str, List[str]]]:
        """Fetch columns using ENUM types. Returns {table: {column: [values]}}. Empty for dialects without ENUM."""
        pass

    @abstractmethod
    def fetch_unique_constraints(self, engine: Engine, schema: str) -> Dict[str, Set[str]]:
        """Fetch columns with UNIQUE constraints. Returns {table: {col1, col2, ...}}."""
        pass

    @abstractmethod
    def detect_cdc_enabled(self, engine: Engine, table_name: str, schema: str) -> bool:
        """Check if the table has CDC-friendly settings."""
        pass

    _PARTITION_NAME_HINTS = (
        "order_date", "event_time", "event_date", "payment_date", "transaction_date",
        "created_at", "changed_at", "log_date", "partition_date", "report_date",
    )
    _PARTITION_TYPE_PREFIXES = ("date", "timestamp", "timestamptz")

    def detect_partition_columns(
        self, engine: Engine, table_name: str, schema: str, columns: List[Dict]
    ) -> List[str]:
        """Detect partition key columns. Falls back to heuristic from columns if not supported."""
        candidates = []
        for col in columns:
            name_lower = col["name"].lower()
            col_type = col.get("type", "").lower()
            if any(col_type.startswith(p) for p in self._PARTITION_TYPE_PREFIXES):
                if name_lower in self._PARTITION_NAME_HINTS or any(
                    h in name_lower for h in ["_date", "_time", "_at"]
                ):
                    candidates.append(col["name"])
        return candidates

    @abstractmethod
    def limit_clause(self, limit: int) -> str:
        """Return the LIMIT clause for raw SQL (e.g. 'LIMIT 25' or 'TOP 25')."""
        pass

    def build_select_limit_query(self, schema: str, table: str, limit: int) -> tuple:
        """Build SELECT * FROM table LIMIT n. Returns (query_str, params_dict).
        params_dict is empty for literal limit, or {'limit': n} for parameterized."""
        qt = self.quote_table(schema, table) if schema else self.quote_identifier(table)
        lc = self.limit_clause(limit)
        if "TOP " in lc:
            return (f"SELECT {lc} * FROM {qt}", {})
        return (f"SELECT * FROM {qt} {lc}", {})

    def get_late_arriving_biz_expr(self, biz_name: str, biz_type: str) -> Optional[str]:
        """Return dialect-specific expression for business date in lag computation.
        Return None to use default (PostgreSQL-style CAST AS TIMESTAMP)."""
        return None

    def build_late_arriving_query(
        self,
        table_name: str,
        schema: str,
        biz_col: str,
        sys_col: str,
        biz_expr: str,
    ) -> str:
        """Build the SQL for late-arriving data check. Returns full query string."""
        raise NotImplementedError("Late-arriving data check not implemented for this dialect")

    def supports_late_arriving_check(self) -> bool:
        """Whether this dialect supports the late-arriving data check."""
        return False

    def supports_nulls_first(self) -> bool:
        """Whether ORDER BY supports NULLS FIRST."""
        return False

    def order_by_nullable_first(self, column: str) -> str:
        """Return ORDER BY clause that puts NULLs first. Used for delete_management."""
        quoted = self.quote_column(column)
        if self.supports_nulls_first():
            return f"{quoted} NULLS FIRST"
        return f"CASE WHEN {quoted} IS NULL THEN 0 ELSE 1 END, {quoted}"
