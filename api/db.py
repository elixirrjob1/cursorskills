"""Database engine and helpers for table data and schema-style metadata."""

from __future__ import annotations

from threading import Lock
from time import monotonic
from typing import Any

from sqlalchemy import MetaData, Table, func as sa_func, inspect, select, text
from sqlalchemy.engine import Engine


_engine: Engine | None = None
_METADATA_CACHE_TTL_SECONDS = 300
_metadata_cache_lock = Lock()
_metadata_cache: dict[str, dict[str, Any]] = {}


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


def _is_temporal_type(type_name: str) -> bool:
    lowered = type_name.lower()
    return any(token in lowered for token in ("date", "time", "timestamp", "datetime"))


def _infer_incremental_columns(columns: list[dict[str, Any]], primary_keys: list[str]) -> list[str]:
    pk_lower = {c.lower() for c in primary_keys}
    common_incremental = {
        "created_at",
        "updated_at",
        "modified_at",
        "last_updated_at",
    }
    result: list[str] = []
    for col in columns:
        name = str(col["name"])
        lowered = name.lower()
        if lowered in pk_lower or lowered in common_incremental:
            result.append(name)
    return result


def _infer_partition_columns(columns: list[dict[str, Any]]) -> list[str]:
    preferred = {
        "created_at",
        "updated_at",
        "event_time",
        "event_at",
        "created_on",
        "updated_on",
    }
    result: list[str] = []
    for col in columns:
        name = str(col["name"])
        lowered = name.lower()
        type_name = str(col["type"])
        if lowered in preferred or _is_temporal_type(type_name):
            result.append(name)
    return result


def _quote_mssql_ident(name: str) -> str:
    return "[" + str(name).replace("]", "]]") + "]"


def _get_table_row_count(schema: str, table_name: str) -> int | None:
    eng = get_engine()
    try:
        with eng.connect() as conn:
            if eng.dialect.name == "mssql":
                sch = _quote_mssql_ident(schema or "dbo")
                tbl = _quote_mssql_ident(table_name)
                stmt = text(f"SELECT COUNT(*) FROM {sch}.{tbl}")
                count = conn.execute(stmt).scalar_one()
            else:
                table_obj = Table(table_name, MetaData(), schema=(schema or None), autoload_with=eng)
                stmt = select(sa_func.count()).select_from(table_obj)
                count = conn.execute(stmt).scalar_one()
        return int(count)
    except Exception:
        return None


def _build_table_metadata(schema: str, table_name: str, inspector: Any) -> dict[str, Any]:
    eng = get_engine()
    columns_raw = inspector.get_columns(table_name, schema=schema)
    pk_constraint = inspector.get_pk_constraint(table_name, schema=schema) or {}
    pk_columns = [c for c in (pk_constraint.get("constrained_columns") or []) if c]

    foreign_keys: list[dict[str, str]] = []
    for fk in inspector.get_foreign_keys(table_name, schema=schema):
        constrained = fk.get("constrained_columns") or []
        referred_cols = fk.get("referred_columns") or []
        referred_table = fk.get("referred_table")
        if not referred_table:
            continue
        if constrained and referred_cols and len(constrained) == len(referred_cols):
            for source_col, target_col in zip(constrained, referred_cols):
                foreign_keys.append(
                    {
                        "column": source_col,
                        "references": f"{referred_table}.{target_col}",
                    }
                )
            continue
        if constrained:
            target_col = referred_cols[0] if referred_cols else "id"
            foreign_keys.append(
                {
                    "column": constrained[0],
                    "references": f"{referred_table}.{target_col}",
                }
            )

    columns: list[dict[str, Any]] = []
    for col in columns_raw:
        columns.append(
            {
                "name": col["name"],
                "type": str(col["type"]).lower(),
                "nullable": bool(col.get("nullable", True)),
            }
        )

    incremental_columns = _infer_incremental_columns(columns, pk_columns)
    incremental_lower = {c.lower() for c in incremental_columns}
    for col in columns:
        col["is_incremental"] = col["name"].lower() in incremental_lower

    partition_columns = _infer_partition_columns(columns)
    return {
        "table": table_name,
        "schema": schema,
        "columns": columns,
        "primary_keys": pk_columns,
        "foreign_keys": foreign_keys,
        "row_count": _get_table_row_count(schema, table_name),
        "incremental_columns": incremental_columns,
        "partition_columns": partition_columns,
        "join_candidates": [],
        "cdc_enabled": False,
        "has_primary_key": bool(pk_columns),
        "has_foreign_keys": bool(foreign_keys),
        "has_sensitive_fields": False,
    }


def _build_metadata_cache_entry(schema: str) -> dict[str, Any]:
    eng = get_engine()
    inspector = inspect(eng)
    table_names = inspector.get_table_names(schema=(schema or "public"))
    tables: list[dict[str, Any]] = []
    by_lower: dict[str, dict[str, Any]] = {}

    for table_name in table_names:
        try:
            metadata = _build_table_metadata(schema, table_name, inspector)
            tables.append(metadata)
            by_lower[table_name.lower()] = metadata
        except Exception:
            # Keep /api/tables available even if one table has problematic metadata.
            continue

    return {
        "tables": tables,
        "by_lower": by_lower,
        "expires_at": monotonic() + _METADATA_CACHE_TTL_SECONDS,
    }


def _get_metadata_cache_entry(schema: str) -> dict[str, Any]:
    schema_key = schema or "public"
    now = monotonic()
    with _metadata_cache_lock:
        existing = _metadata_cache.get(schema_key)
        if existing and now < float(existing["expires_at"]):
            return existing
        refreshed = _build_metadata_cache_entry(schema_key)
        _metadata_cache[schema_key] = refreshed
        return refreshed


def get_tables_metadata(schema: str) -> list[dict[str, Any]]:
    """Return schema.json-compatible structural metadata for tables."""
    entry = _get_metadata_cache_entry(schema)
    return entry["tables"]


def get_table_metadata(schema: str, table: str) -> dict[str, Any] | None:
    """Return metadata for one table (case-insensitive lookup)."""
    entry = _get_metadata_cache_entry(schema)
    return entry["by_lower"].get((table or "").lower())


def resolve_table_name(schema: str, table: str) -> str | None:
    """Resolve requested table to the canonical DB table name (case-insensitive)."""
    metadata = get_table_metadata(schema, table)
    if not metadata:
        return None
    return str(metadata["table"])


def get_table_data(
    table: str,
    schema: str,
    limit: int = 100,
    offset: int = 0,
) -> list[dict[str, Any]]:
    """
    Return rows from the table as list of dicts (JSON-serializable).
    Table must exist in the schema (caller should validate via metadata helpers).
    """
    eng = get_engine()
    if eng.dialect.name != "mssql":
        metadata = MetaData()
        table_obj = Table(table, metadata, schema=(schema or None), autoload_with=eng)
        stmt = select(table_obj).limit(limit)
        if offset:
            stmt = stmt.offset(offset)
        with eng.connect() as conn:
            rows = conn.execute(stmt).mappings().fetchall()
        return [dict(r) for r in rows]

    inspector = inspect(eng)
    cols_raw = inspector.get_columns(table, schema=schema)
    col_names = [str(c["name"]) for c in cols_raw if c.get("name")]
    if not col_names:
        return []

    pk_constraint = inspector.get_pk_constraint(table, schema=schema) or {}
    pk_columns = [str(c) for c in (pk_constraint.get("constrained_columns") or []) if c]
    order_columns = [c for c in pk_columns if c in col_names] or [col_names[0]]

    sch = _quote_mssql_ident(schema or "dbo")
    tbl = _quote_mssql_ident(table)
    order_sql = ", ".join(_quote_mssql_ident(c) for c in order_columns)
    plain_cols_sql = ", ".join(_quote_mssql_ident(c) for c in col_names)
    cast_cols_sql = ", ".join(
        f"CONVERT(nvarchar(max), {_quote_mssql_ident(c)}) AS {_quote_mssql_ident(c)}" for c in col_names
    )

    query = text(
        f"""
        SELECT {plain_cols_sql}
        FROM {sch}.{tbl}
        ORDER BY {order_sql}
        OFFSET :offset ROWS FETCH NEXT :limit ROWS ONLY
        """
    )
    fallback_query = text(
        f"""
        SELECT {cast_cols_sql}
        FROM {sch}.{tbl}
        ORDER BY {order_sql}
        OFFSET :offset ROWS FETCH NEXT :limit ROWS ONLY
        """
    )

    with eng.connect() as conn:
        try:
            rows = conn.execute(query, {"offset": offset, "limit": limit}).mappings().fetchall()
        except Exception:
            rows = conn.execute(fallback_query, {"offset": offset, "limit": limit}).mappings().fetchall()
    return [dict(r) for r in rows]
