"""Database engine and helpers: table listing and row data as JSON-serializable dicts."""

from typing import Any

from sqlalchemy import inspect, text
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
    # Use identifier quoting to avoid injection; schema/table validated by being in get_tables()
    schema_q = _quote_ident(schema) if schema else "public"
    table_q = _quote_ident(table)
    stmt = text(
        f"SELECT * FROM {schema_q}.{table_q} LIMIT :limit OFFSET :offset"
    ).bindparams(limit=limit, offset=offset)
    with eng.connect() as conn:
        rows = conn.execute(stmt).mappings().fetchall()
    return [dict(r) for r in rows]


def _quote_ident(name: str) -> str:
    """Quote a PostgreSQL identifier (double-quote). Reject invalid chars."""
    if not name or not name.replace("_", "").isalnum():
        raise ValueError("Invalid identifier")
    return f'"{name}"'
