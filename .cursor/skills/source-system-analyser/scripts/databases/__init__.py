"""Database dialect adapters for multi-database support."""

from typing import Optional

from sqlalchemy.engine import Engine

from .base import DialectAdapter
from .mssql import MssqlAdapter
from .oracle import OracleAdapter
from .postgresql import PostgresqlAdapter

_ADAPTERS = {
    "postgresql": PostgresqlAdapter,
    "mssql": MssqlAdapter,
    "oracle": OracleAdapter,
}


def get_adapter(dialect_name: str) -> Optional[DialectAdapter]:
    """Get the dialect adapter for the given dialect name.

    Args:
        dialect_name: SQLAlchemy dialect name (e.g. postgresql, mssql, oracle).

    Returns:
        DialectAdapter instance or None if dialect is not supported.
    """
    adapter_cls = _ADAPTERS.get(dialect_name)
    if adapter_cls is None:
        return None
    return adapter_cls()


def get_adapter_for_engine(engine: Engine) -> Optional[DialectAdapter]:
    """Get the dialect adapter for the given engine."""
    return get_adapter(engine.dialect.name)


def supported_dialects() -> tuple:
    """Return tuple of supported dialect names."""
    return tuple(_ADAPTERS.keys())
