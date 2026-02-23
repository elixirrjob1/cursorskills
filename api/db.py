"""Database engine and helpers for table data and schema-style metadata."""

from __future__ import annotations

import re
from pathlib import Path
from threading import Lock
from time import monotonic
from typing import Any

from sqlalchemy import MetaData, Table, func as sa_func, inspect, select, text
from sqlalchemy.engine import Engine

try:
    import yaml  # type: ignore
except Exception:
    yaml = None


_engine: Engine | None = None
_METADATA_CACHE_TTL_SECONDS = 300
_metadata_cache_lock = Lock()
_metadata_cache: dict[str, dict[str, Any]] = {}

_SEMANTIC_PATTERNS: list[tuple[str, str]] = [
    (r"(length|height|width|depth|distance|diameter|radius|thickness)", "length"),
    (r"(volume|capacity|cubic|cbm|ft3|m3|liter|litre|gallon)", "volume"),
    (r"(pressure|press|psi|bar|kpa|mpa)", "pressure"),
    (r"(temperature|temp|celsius|fahrenheit|kelvin)", "temperature"),
    (r"(duration|latency|elapsed|runtime|ttl|age|timeout)", "duration"),
    (r"(sku|product_code|product_id)", "product_identifier"),
]
_UNITFUL_SEMANTICS = {"length", "area", "volume", "mass", "pressure", "temperature", "duration", "speed", "flow_rate", "force", "energy", "power", "density"}
_UNIT_ALIASES = {
    "ft": "ft",
    "feet": "ft",
    "foot": "ft",
    "in": "in",
    "inch": "in",
    "inches": "in",
    "m": "m",
    "meter": "m",
    "meters": "m",
    "cm": "cm",
    "mm": "mm",
    "ft3": "ft3",
    "cubic_ft": "ft3",
    "m3": "m3",
    "cubic_m": "m3",
    "psi": "psi",
    "bar": "bar",
    "kpa": "kpa",
    "mpa": "mpa",
    "s": "s",
    "sec": "s",
    "second": "s",
    "min": "min",
    "minute": "min",
    "h": "h",
    "hour": "h",
    "c": "c",
    "celsius": "c",
    "f": "f",
    "fahrenheit": "f",
    "k": "k",
    "kelvin": "k",
}
_UNIT_CONVERSION: dict[str, dict[str, Any]] = {
    "ft": {"canonical_unit": "m", "unit_system": "imperial", "factor_to_canonical": 0.3048, "offset_to_canonical": 0.0},
    "in": {"canonical_unit": "m", "unit_system": "imperial", "factor_to_canonical": 0.0254, "offset_to_canonical": 0.0},
    "m": {"canonical_unit": "m", "unit_system": "metric", "factor_to_canonical": 1.0, "offset_to_canonical": 0.0},
    "cm": {"canonical_unit": "m", "unit_system": "metric", "factor_to_canonical": 0.01, "offset_to_canonical": 0.0},
    "mm": {"canonical_unit": "m", "unit_system": "metric", "factor_to_canonical": 0.001, "offset_to_canonical": 0.0},
    "ft3": {"canonical_unit": "m3", "unit_system": "imperial", "factor_to_canonical": 0.028316846592, "offset_to_canonical": 0.0},
    "m3": {"canonical_unit": "m3", "unit_system": "metric", "factor_to_canonical": 1.0, "offset_to_canonical": 0.0},
    "psi": {"canonical_unit": "bar", "unit_system": "imperial", "factor_to_canonical": 0.0689475729, "offset_to_canonical": 0.0},
    "bar": {"canonical_unit": "bar", "unit_system": "metric", "factor_to_canonical": 1.0, "offset_to_canonical": 0.0},
    "kpa": {"canonical_unit": "bar", "unit_system": "metric", "factor_to_canonical": 0.01, "offset_to_canonical": 0.0},
    "mpa": {"canonical_unit": "bar", "unit_system": "metric", "factor_to_canonical": 10.0, "offset_to_canonical": 0.0},
    "s": {"canonical_unit": "s", "unit_system": "metric", "factor_to_canonical": 1.0, "offset_to_canonical": 0.0},
    "min": {"canonical_unit": "s", "unit_system": "metric", "factor_to_canonical": 60.0, "offset_to_canonical": 0.0},
    "h": {"canonical_unit": "s", "unit_system": "metric", "factor_to_canonical": 3600.0, "offset_to_canonical": 0.0},
    "c": {"canonical_unit": "c", "unit_system": "metric", "factor_to_canonical": 1.0, "offset_to_canonical": 0.0},
    "f": {"canonical_unit": "c", "unit_system": "imperial", "factor_to_canonical": 0.5555555556, "offset_to_canonical": -17.7777777778},
    "k": {"canonical_unit": "c", "unit_system": "metric", "factor_to_canonical": 1.0, "offset_to_canonical": -273.15},
}
_RULES_LOADED = False


def _load_context_rules() -> None:
    global _RULES_LOADED, _SEMANTIC_PATTERNS, _UNIT_ALIASES, _UNIT_CONVERSION
    if _RULES_LOADED:
        return
    _RULES_LOADED = True
    if yaml is None:
        return
    shared_dir = (Path(__file__).resolve().parent.parent / ".cursor" / "skills" / "source-system-analyser" / "references" / "shared")
    semantic_path = shared_dir / "semantic_mappings.yaml"
    unit_path = shared_dir / "unit_mappings.yaml"
    try:
        if semantic_path.exists():
            with open(semantic_path, encoding="utf-8") as f:
                data = yaml.safe_load(f) or {}
            patterns = data.get("semantic_patterns")
            if isinstance(patterns, list):
                parsed: list[tuple[str, str]] = []
                for item in patterns:
                    if not isinstance(item, dict):
                        continue
                    pat = str(item.get("pattern") or "").strip()
                    cls = str(item.get("semantic_class") or "").strip()
                    if pat and cls:
                        parsed.append((pat, cls))
                if parsed:
                    _SEMANTIC_PATTERNS = parsed
    except Exception:
        pass
    try:
        if unit_path.exists():
            with open(unit_path, encoding="utf-8") as f:
                data = yaml.safe_load(f) or {}
            aliases = data.get("unit_aliases")
            if isinstance(aliases, dict):
                _UNIT_ALIASES.update({str(k): str(v) for k, v in aliases.items()})
            conversions = data.get("unit_conversion")
            if isinstance(conversions, dict):
                _UNIT_CONVERSION.update(conversions)
    except Exception:
        pass


def _infer_semantic_class(col_name: str) -> str | None:
    _load_context_rules()
    lower = col_name.lower()
    for pattern, semantic_class in _SEMANTIC_PATTERNS:
        if re.search(pattern, lower):
            return semantic_class
    return None


def _extract_unit_from_name(col_name: str) -> str | None:
    _load_context_rules()
    lower = re.sub(r"[^a-z0-9]+", "_", col_name.lower()).strip("_")
    if not lower:
        return None
    for alias in sorted(_UNIT_ALIASES.keys(), key=len, reverse=True):
        norm_alias = re.sub(r"[^a-z0-9]+", "_", alias).strip("_")
        if re.search(rf"(?:^|_){re.escape(norm_alias)}(?:$|_)", lower):
            return _UNIT_ALIASES[alias]
    return None


def _build_unit_context(col_name: str, semantic_class: str | None) -> dict[str, Any] | None:
    _load_context_rules()
    detected = _extract_unit_from_name(col_name)
    if not detected:
        if semantic_class in _UNITFUL_SEMANTICS:
            return {
                "detected_unit": None,
                "canonical_unit": None,
                "unit_system": "unknown",
                "conversion": None,
                "detection_confidence": "low",
                "detection_source": "name",
                "notes": "Semantic class suggests units, but no explicit source unit token was detected.",
            }
        return None
    conversion_cfg = _UNIT_CONVERSION.get(detected)
    if not conversion_cfg:
        return {
            "detected_unit": detected,
            "canonical_unit": None,
            "unit_system": "unknown",
            "conversion": None,
            "detection_confidence": "medium",
            "detection_source": "name",
            "notes": "Detected unit alias is not configured for canonical conversion.",
        }
    factor = conversion_cfg.get("factor_to_canonical")
    offset = conversion_cfg.get("offset_to_canonical", 0.0)
    canonical = conversion_cfg.get("canonical_unit")
    return {
        "detected_unit": detected,
        "canonical_unit": canonical,
        "unit_system": conversion_cfg.get("unit_system", "unknown"),
        "conversion": {
            "factor_to_canonical": factor,
            "offset_to_canonical": offset,
            "formula": f"canonical = value * {factor} + {offset}",
        },
        "detection_confidence": "medium",
        "detection_source": "name",
        "notes": None if detected == canonical else f"Normalize from '{detected}' to canonical '{canonical}'.",
    }


def _build_unit_summary(columns: list[dict[str, Any]]) -> dict[str, Any]:
    with_units = 0
    unknown_cols: list[str] = []
    groups: dict[str, set[str]] = {}
    for col in columns:
        semantic_class = col.get("semantic_class")
        unit_ctx = col.get("unit_context")
        if isinstance(unit_ctx, dict) and unit_ctx.get("detected_unit"):
            with_units += 1
            if semantic_class:
                groups.setdefault(str(semantic_class), set()).add(str(unit_ctx.get("detected_unit")))
        elif semantic_class in _UNITFUL_SEMANTICS:
            unknown_cols.append(str(col.get("name")))
    mixed_groups = [
        {"semantic_class": semantic_class, "detected_units": sorted(units)}
        for semantic_class, units in groups.items()
        if len(units) > 1
    ]
    return {
        "columns_with_units": with_units,
        "columns_without_units": max(len(columns) - with_units, 0),
        "mixed_unit_groups": mixed_groups,
        "unknown_unit_columns": sorted(unknown_cols),
    }


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


def _get_table_description(inspector: Any, schema: str, table_name: str) -> str | None:
    eng = get_engine()
    try:
        raw = inspector.get_table_comment(table_name, schema=schema)
        if isinstance(raw, dict):
            text_val = raw.get("text")
            if text_val:
                return str(text_val)
        elif raw:
            return str(raw)
    except Exception:
        pass

    try:
        with eng.connect() as conn:
            if eng.dialect.name == "mssql":
                row = conn.execute(
                    text(
                        """
                        SELECT CAST(ep.value AS nvarchar(max))
                        FROM sys.tables t
                        JOIN sys.schemas s ON s.schema_id = t.schema_id
                        LEFT JOIN sys.extended_properties ep
                          ON ep.major_id = t.object_id
                         AND ep.minor_id = 0
                         AND ep.name = 'MS_Description'
                        WHERE s.name = :schema AND t.name = :table
                        """
                    ),
                    {"schema": schema, "table": table_name},
                ).fetchone()
                if row and row[0]:
                    return str(row[0])
            if eng.dialect.name == "oracle":
                row = conn.execute(
                    text(
                        """
                        SELECT COMMENTS
                        FROM ALL_TAB_COMMENTS
                        WHERE OWNER = :schema AND TABLE_NAME = :table
                        """
                    ),
                    {"schema": schema.upper(), "table": table_name.upper()},
                ).fetchone()
                if row and row[0]:
                    return str(row[0])
    except Exception:
        pass
    return None


def _get_column_description_map(schema: str, table_name: str, columns_raw: list[dict[str, Any]]) -> dict[str, str]:
    desc: dict[str, str] = {}
    eng = get_engine()

    for col in columns_raw:
        name = col.get("name")
        comment = col.get("comment")
        if name and comment:
            desc[str(name)] = str(comment)
    if desc:
        return desc

    try:
        with eng.connect() as conn:
            if eng.dialect.name == "mssql":
                rows = conn.execute(
                    text(
                        """
                        SELECT c.name AS column_name, CAST(ep.value AS nvarchar(max)) AS description
                        FROM sys.tables t
                        JOIN sys.schemas s ON s.schema_id = t.schema_id
                        JOIN sys.columns c ON c.object_id = t.object_id
                        LEFT JOIN sys.extended_properties ep
                          ON ep.major_id = c.object_id
                         AND ep.minor_id = c.column_id
                         AND ep.name = 'MS_Description'
                        WHERE s.name = :schema AND t.name = :table
                        """
                    ),
                    {"schema": schema, "table": table_name},
                ).fetchall()
                for row in rows:
                    if row[1]:
                        desc[str(row[0])] = str(row[1])
            if eng.dialect.name == "oracle":
                rows = conn.execute(
                    text(
                        """
                        SELECT COLUMN_NAME, COMMENTS
                        FROM ALL_COL_COMMENTS
                        WHERE OWNER = :schema AND TABLE_NAME = :table
                        """
                    ),
                    {"schema": schema.upper(), "table": table_name.upper()},
                ).fetchall()
                for row in rows:
                    if row[1]:
                        desc[str(row[0])] = str(row[1])
    except Exception:
        pass
    return desc


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
    table_description = _get_table_description(inspector, schema, table_name)
    column_descriptions = _get_column_description_map(schema, table_name, columns_raw)

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

    join_candidates = [
        {
            "column": fk["column"],
            "target_table": str(fk["references"]).split(".", 1)[0],
            "target_column": str(fk["references"]).split(".", 1)[1] if "." in str(fk["references"]) else None,
            "confidence": "high",
        }
        for fk in foreign_keys
    ]

    columns: list[dict[str, Any]] = []
    for col in columns_raw:
        col_name = str(col["name"])
        semantic_class = _infer_semantic_class(col_name)
        columns.append(
            {
                "name": col_name,
                "type": str(col["type"]).lower(),
                "nullable": bool(col.get("nullable", True)),
                "semantic_class": semantic_class,
                "unit_context": _build_unit_context(col_name, semantic_class),
                "column_description": column_descriptions.get(col_name),
            }
        )

    incremental_columns = _infer_incremental_columns(columns, pk_columns)
    incremental_lower = {c.lower() for c in incremental_columns}
    for col in columns:
        col["is_incremental"] = col["name"].lower() in incremental_lower

    partition_columns = _infer_partition_columns(columns)
    unit_summary = _build_unit_summary(columns)
    return {
        "table": table_name,
        "schema": schema,
        "table_description": table_description,
        "columns": columns,
        "primary_keys": pk_columns,
        "foreign_keys": foreign_keys,
        "row_count": _get_table_row_count(schema, table_name),
        "incremental_columns": incremental_columns,
        "partition_columns": partition_columns,
        "join_candidates": join_candidates,
        "unit_summary": unit_summary,
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
