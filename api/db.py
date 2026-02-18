"""Database engine and helpers: table listing and row data as JSON-serializable dicts."""

from typing import Any

from sqlalchemy import MetaData, Table, inspect, select
from sqlalchemy.engine import Engine


_engine: Engine | None = None


def set_engine(engine: Engine) -> None:
    """Set the global engine (called from app lifespan)."""
    global _engine
    _engine = engine


def get_engine() -> Engine:
    """Return the global engine. Raises RuntimeError if not set."""
    if _engine is None:
        raise RuntimeError("Database engine not initialized")
    return _engine


def get_tables(schema: str) -> list[str]:
    """Return list of table names in the given schema."""
    eng = get_engine()
    inspector = inspect(eng)
    return inspector.get_table_names(schema=(schema or "public"))


def get_table_data(
    table: str,
    schema: str,
    limit: int = 100,
    offset: int = 0,
) -> list[dict[str, Any]]:
    """
    Return rows from the table as list of dicts (JSON-serializable).
    Table must exist in the schema (caller should validate via get_tables).
    """
    eng = get_engine()
    metadata = MetaData()
    table_obj = Table(table, metadata, schema=(schema or None), autoload_with=eng)
    stmt = select(table_obj).limit(limit)

    # SQL Server requires ORDER BY when OFFSET is used.
    if offset:
        if eng.dialect.name == "mssql":
            pk_cols = list(table_obj.primary_key.columns)
            order_cols = pk_cols if pk_cols else [next(iter(table_obj.columns))]
            stmt = stmt.order_by(*order_cols)
        stmt = stmt.offset(offset)

    with eng.connect() as conn:
        rows = conn.execute(stmt).mappings().fetchall()
    return [dict(r) for r in rows]
