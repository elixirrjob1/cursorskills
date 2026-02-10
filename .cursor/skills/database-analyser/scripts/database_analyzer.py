#!/usr/bin/env python3
"""
Database Schema Analyzer
Simple module that analyzes a database schema and produces a fully enriched JSON file.

This module provides:
- analyze_database_to_json(): Single function that analyzes database and saves enriched schema to JSON
- Metadata detection functions for sensitive fields, partitions, incremental columns, CDC

This is a self-contained module with no external dependencies beyond SQLAlchemy.
"""

import os
import json
import logging
import warnings
from pathlib import Path
from typing import Dict, List, Optional, Any
from datetime import datetime, timezone


def _load_env_file() -> None:
    """Load .env from current working directory or script directory."""
    for base in (Path.cwd(), Path(__file__).resolve().parent):
        env_path = base / ".env"
        if env_path.exists():
            try:
                with open(env_path, encoding="utf-8") as f:
                    for line in f:
                        line = line.strip()
                        if line and not line.startswith("#") and "=" in line:
                            key, _, value = line.partition("=")
                            key = key.strip()
                            value = value.strip().strip('"').strip("'")
                            if key and key not in os.environ:
                                os.environ[key] = value
            except Exception:
                pass
            break


# Configure logging
logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

from sqlalchemy import create_engine, inspect, text, MetaData, Table, select
from sqlalchemy.engine import Engine
from sqlalchemy.exc import SAWarning


# ============================================================================
# Database Connection Functions (Self-contained implementations)
# ============================================================================

def get_engine(database_url: str) -> Engine:
    """Create a SQLAlchemy engine from a database URL with connection pooling."""
    return create_engine(
        database_url,
        pool_size=5,
        max_overflow=10,
        pool_pre_ping=True,
        connect_args={
            "connect_timeout": 10,
        },
        echo=False,
    )


def format_type(col_type: str, col_info: dict) -> str:
    """Format column type string from SQLAlchemy column info, including precision/scale."""
    if hasattr(col_type, '__name__'):
        base_type = col_type.__name__.lower()
    else:
        base_type = str(col_type).lower()
        if '(' in base_type:
            base_type = base_type.split('(')[0].strip()
    
    if hasattr(col_type, 'length') and col_type.length is not None:
        length = col_type.length
        if 'varchar' in base_type or 'char' in base_type:
            return f"{base_type}({length})"
        elif 'text' in base_type and length:
            return f"{base_type}({length})"
    
    if hasattr(col_type, 'precision') and col_type.precision is not None:
        precision = col_type.precision
        scale = col_type.scale if hasattr(col_type, 'scale') and col_type.scale is not None else 0
        
        if 'numeric' in base_type or 'decimal' in base_type:
            return f"numeric({precision},{scale})"
        elif 'float' in base_type or 'double' in base_type or 'real' in base_type:
            if scale == 0:
                return f"{base_type}({precision})"
            else:
                return f"{base_type}({precision},{scale})"
        else:
            return f"{base_type}({precision},{scale})"
    
    type_str = str(col_type)
    if '(' in type_str and ')' in type_str:
        return type_str
    
    return base_type


def is_incremental_column(col_info: dict, col_type: str) -> bool:
    """Detect if a column is an incremental/auto-increment column."""
    col_type_str = str(col_type).lower()
    default_val = str(col_info.get('default', '')).lower() if col_info.get('default') else ''
    
    if any(serial_type in col_type_str for serial_type in ['serial', 'bigserial', 'smallserial']):
        return True
    
    if 'auto_increment' in default_val or 'nextval' in default_val:
        return True
    
    if 'identity' in col_type_str or col_info.get('identity', False):
        return True
    
    if col_info.get('autoincrement', False):
        return True
    
    return False


# Date/time type keywords for detection
_DATETIME_TYPE_KEYWORDS = ("timestamp", "datetime", "date", "time", "smalldatetime", "datetimeoffset")

# TZ-aware types by dialect: these store timezone info or are internally UTC
_TZ_AWARE_TYPES = {
    "postgresql": ("timestamptz", "timestamp with time zone", "timetz", "time with time zone"),
    "mysql": ("timestamp",),       # MySQL TIMESTAMP is stored as UTC internally
    "mssql": ("datetimeoffset",),  # datetimeoffset carries its own offset per row
}

# For TZ-aware types, what timezone the stored values represent
_TZ_AWARE_INTERPRETATION = {
    "postgresql": "UTC",          # Postgres stores timestamptz as UTC internally
    "mysql": "UTC",               # MySQL stores TIMESTAMP as UTC internally
    "mssql": "offset_embedded",   # datetimeoffset: each value carries its own offset
}


def get_column_timezone(col_type_str: str, dialect: str, server_timezone: str) -> Optional[str]:
    """Determine the effective timezone of a date/timestamp column.

    Returns:
        - The timezone string for timestamp/datetime columns (e.g. "UTC", "Europe/Warsaw", "offset_embedded")
        - None for non-date/time columns or pure date columns (field should be omitted from output)

    Rules:
        - Pure date columns (DATE with no time component): Returns None — timezone
          is not meaningful without a time-of-day.
        - TZ-aware types (timestamptz, MySQL TIMESTAMP, datetimeoffset):
          Returns how the DB stores them ("UTC" or "offset_embedded").
        - TZ-naive types (timestamp, datetime):
          Returns the server timezone as the assumed interpretation.
        - SQLite: Returns "unknown" (no native TZ support).
        - Non-date/time columns: Returns None.
    """
    col_type_lower = col_type_str.lower().strip()

    # Check if this is a date/time type at all
    is_datetime = any(kw in col_type_lower for kw in _DATETIME_TYPE_KEYWORDS)
    if not is_datetime:
        return None

    # Pure date columns have no time component — timezone is not meaningful
    if col_type_lower == "date":
        return None

    # Check if it's a TZ-aware type for this dialect
    aware_types = _TZ_AWARE_TYPES.get(dialect, ())
    is_tz_aware = any(tz_type in col_type_lower for tz_type in aware_types)

    if is_tz_aware:
        return _TZ_AWARE_INTERPRETATION.get(dialect, server_timezone)

    # TZ-naive date/time column: assume server timezone
    if dialect == "sqlite":
        return "unknown"

    return server_timezone


def fetch_tables(database_url: str, schema: Optional[str] = None) -> List[str]:
    """Fetch all table names from the database, optionally filtered by schema."""
    engine = get_engine(database_url)
    inspector = inspect(engine)
    schemas = inspector.get_schema_names()
    
    tables = []
    if schema:
        if schema in schemas:
            schema_tables = inspector.get_table_names(schema=schema)
            tables.extend(schema_tables)
    else:
        for schema_name in schemas:
            schema_tables = inspector.get_table_names(schema=schema_name)
            tables.extend(schema_tables)
    
    return sorted(tables)


def fetch_columns(database_url: str, table_names: List[str] = None, schema: str = None) -> Dict[str, List[Dict[str, object]]]:
    """Fetch column information for specified tables (or all if table_names is None)."""
    engine = get_engine(database_url)
    inspector = inspect(engine)
    
    columns_by_table: Dict[str, List[Dict[str, object]]] = {}
    
    if table_names:
        target_tables = {t: None for t in table_names}
        schemas_to_check = [schema] if schema else inspector.get_schema_names()
        
        for sch in schemas_to_check:
            schema_tables = inspector.get_table_names(schema=sch)
            for table_name in schema_tables:
                if table_name in target_tables:
                    target_tables[table_name] = sch
    else:
        schemas_to_check = [schema] if schema else inspector.get_schema_names()
        target_tables = {}
        for sch in schemas_to_check:
            schema_tables = inspector.get_table_names(schema=sch)
            for table_name in schema_tables:
                target_tables[table_name] = sch
    
    with warnings.catch_warnings():
        warnings.filterwarnings('ignore', category=SAWarning, message='Did not recognize type')
        
        for table_name, table_schema in target_tables.items():
            if table_schema is None:
                continue
            try:
                columns = inspector.get_columns(table_name, schema=table_schema)
                columns_by_table[table_name] = []
                for col in columns:
                    col_type_str = format_type(col['type'], col)
                    is_incremental = is_incremental_column(col, col['type'])
                    columns_by_table[table_name].append(
                        {
                            "name": col['name'],
                            "type": col_type_str,
                            "nullable": col.get('nullable', True),
                            "default": str(col.get('default', '')) if col.get('default') is not None else None,
                            "is_incremental": is_incremental,
                        }
                    )
            except Exception:
                continue
    
    return columns_by_table


def fetch_primary_keys(database_url: str, table_names: List[str] = None, schema: str = None) -> Dict[str, List[str]]:
    """Fetch primary key information for specified tables (or all if table_names is None)."""
    engine = get_engine(database_url)
    inspector = inspect(engine)
    
    pk_by_table: Dict[str, List[str]] = {}
    
    if table_names:
        target_tables = {t: None for t in table_names}
        schemas_to_check = [schema] if schema else inspector.get_schema_names()
        
        for sch in schemas_to_check:
            schema_tables = inspector.get_table_names(schema=sch)
            for table_name in schema_tables:
                if table_name in target_tables:
                    target_tables[table_name] = sch
    else:
        schemas_to_check = [schema] if schema else inspector.get_schema_names()
        target_tables = {}
        for sch in schemas_to_check:
            schema_tables = inspector.get_table_names(schema=sch)
            for table_name in schema_tables:
                target_tables[table_name] = sch
    
    for table_name, table_schema in target_tables.items():
        if table_schema is None:
            continue
        try:
            pk_constraint = inspector.get_pk_constraint(table_name, schema=table_schema)
            if pk_constraint and pk_constraint.get('constrained_columns'):
                pk_by_table[table_name] = pk_constraint['constrained_columns']
        except Exception:
            continue
    
    return pk_by_table


def fetch_foreign_keys(database_url: str, table_names: List[str] = None, schema: str = None) -> Dict[str, List[Dict[str, str]]]:
    """Fetch foreign key information for specified tables (or all if table_names is None)."""
    engine = get_engine(database_url)
    inspector = inspect(engine)
    
    fk_by_table: Dict[str, List[Dict[str, str]]] = {}
    
    if table_names:
        target_tables = {t: None for t in table_names}
        schemas_to_check = [schema] if schema else inspector.get_schema_names()
        
        for sch in schemas_to_check:
            schema_tables = inspector.get_table_names(schema=sch)
            for table_name in schema_tables:
                if table_name in target_tables:
                    target_tables[table_name] = sch
    else:
        schemas_to_check = [schema] if schema else inspector.get_schema_names()
        target_tables = {}
        for sch in schemas_to_check:
            schema_tables = inspector.get_table_names(schema=sch)
            for table_name in schema_tables:
                target_tables[table_name] = sch
    
    for table_name, table_schema in target_tables.items():
        if table_schema is None:
            continue
        try:
            foreign_keys = inspector.get_foreign_keys(table_name, schema=table_schema)
            fk_by_table[table_name] = []
            for fk in foreign_keys:
                for local_col, ref_col in zip(fk['constrained_columns'], fk['referred_columns']):
                    ref_table = fk['referred_table']
                    fk_by_table[table_name].append(
                        {"column": local_col, "references": f"{ref_table}.{ref_col}"}
                    )
        except Exception:
            continue
    
    return fk_by_table


def fetch_sample_rows(database_url: str, table: str, limit: int):
    """Fetch sample rows from a table with fallback for problematic tables."""
    engine = get_engine(database_url)
    with engine.connect() as conn:
        try:
            metadata = MetaData()
            table_obj = Table(table, metadata, autoload_with=engine)
            stmt = select(table_obj).limit(limit)
            result = conn.execute(stmt)
            colnames = list(result.keys())
            rows = result.fetchall()
        except Exception:
            result = conn.execute(text(f'SELECT * FROM "{table}" LIMIT :limit'), {"limit": limit})
            colnames = list(result.keys())
            rows = result.fetchall()
    return colnames, rows


def fetch_row_counts(database_url: str, table_names: List[str], schema: str = None) -> Dict[str, int]:
    """Fetch row counts for all specified tables."""
    row_counts = {}
    engine = get_engine(database_url)
    with engine.connect() as conn:
        for table_name in table_names:
            try:
                if schema:
                    result = conn.execute(text(f'SELECT COUNT(*) FROM "{schema}"."{table_name}"'))
                else:
                    result = conn.execute(text(f'SELECT COUNT(*) FROM "{table_name}"'))
                count = result.scalar()
                row_counts[table_name] = count if count is not None else 0
            except Exception:
                row_counts[table_name] = 0
    return row_counts


def fetch_database_timezone(database_url: str) -> str:
    """Fetch the database server timezone."""
    engine = get_engine(database_url)
    dialect = engine.dialect.name
    
    with engine.connect() as conn:
        try:
            if dialect == "postgresql":
                result = conn.execute(text("SHOW timezone"))
                return result.scalar() or "Unknown"
            elif dialect == "mysql":
                result = conn.execute(text("SELECT @@global.time_zone"))
                return result.scalar() or "Unknown"
            elif dialect == "sqlite":
                return "UTC (SQLite default)"
            elif dialect == "mssql":
                result = conn.execute(text("SELECT CURRENT_TIMEZONE()"))
                return result.scalar() or "Unknown"
            else:
                return f"Unknown ({dialect})"
        except Exception:
            return "Unknown"


# Types where MIN/MAX is meaningless or unsupported
_RANGE_SKIP_TYPES = (
    "json", "jsonb", "bytea", "xml", "tsvector", "tsquery",
    "point", "line", "lseg", "box", "path", "polygon", "circle",
    "array", "user-defined", "bool",  # PostgreSQL has no min(boolean)
)


def fetch_column_statistics(
    engine, table_name: str, columns: List[Dict],
    schema: str = None, row_count: int = 0,
) -> Dict[str, Dict]:
    """Fetch cardinality, null count, and data range for all columns in a table.

    Returns:
        Dict mapping column name to stats dict:
        - cardinality (int): number of distinct values
        - null_count (int): number of NULL values
        - data_range (dict, optional): {"min": str, "max": str} for range-compatible types
    """
    empty_stats = {
        col["name"]: {"cardinality": 0, "null_count": 0}
        for col in columns
    }
    if not columns or row_count == 0:
        return empty_stats

    # Build a single query that computes all column stats in one table scan
    stats_parts = []
    range_columns = set()

    for col in columns:
        col_name = col["name"]
        col_type = col.get("type", "").lower()
        quoted = f'"{col_name}"'

        stats_parts.append(f'COUNT(DISTINCT {quoted}) AS "{col_name}__card"')
        stats_parts.append(
            f'SUM(CASE WHEN {quoted} IS NULL THEN 1 ELSE 0 END) AS "{col_name}__nulls"'
        )

        # MIN/MAX only for types that support comparison
        skip = any(s in col_type for s in _RANGE_SKIP_TYPES)
        if not skip:
            stats_parts.append(f'MIN({quoted}) AS "{col_name}__min"')
            stats_parts.append(f'MAX({quoted}) AS "{col_name}__max"')
            range_columns.add(col_name)

    select_clause = ", ".join(stats_parts)
    if schema:
        from_clause = f'"{schema}"."{table_name}"'
    else:
        from_clause = f'"{table_name}"'

    query = f"SELECT {select_clause} FROM {from_clause}"

    try:
        with engine.connect() as conn:
            result = conn.execute(text(query))
            row = result.fetchone()

        if not row:
            return empty_stats

        row_dict = dict(row._mapping)
        stats = {}

        for col in columns:
            col_name = col["name"]
            col_stats = {
                "cardinality": int(row_dict.get(f"{col_name}__card", 0) or 0),
                "null_count": int(row_dict.get(f"{col_name}__nulls", 0) or 0),
            }

            if col_name in range_columns:
                min_val = row_dict.get(f"{col_name}__min")
                max_val = row_dict.get(f"{col_name}__max")
                if min_val is not None or max_val is not None:
                    col_stats["data_range"] = {
                        "min": str(min_val) if min_val is not None else None,
                        "max": str(max_val) if max_val is not None else None,
                    }

            stats[col_name] = col_stats

        return stats

    except Exception as e:
        logger.warning(f"Could not fetch column statistics for '{table_name}': {e}")
        return empty_stats


# ============================================================================
# Metadata Detection Functions
# ============================================================================


# Sensitive field patterns -> category
_SENSITIVE_PATTERNS: List[tuple] = [
    ("ssn", "government_id"),
    ("social_security", "government_id"),
    ("tax_id", "government_id"),
    ("passport", "government_id"),
    ("national_id", "government_id"),
    ("driver_license", "government_id"),
    ("credit_card", "financial"),
    ("card_number", "financial"),
    ("card_last", "financial"),
    ("bank_account", "financial"),
    ("routing_number", "financial"),
    ("iban", "financial"),
    ("salary", "financial"),
    ("compensation", "financial"),
    ("email", "pii_contact"),
    ("phone", "pii_contact"),
    ("mobile", "pii_contact"),
    ("fax", "pii_contact"),
    ("date_of_birth", "pii_personal"),
    ("dob", "pii_personal"),
    ("birth_date", "pii_personal"),
    ("gender", "pii_personal"),
    ("ethnicity", "pii_personal"),
    ("religion", "pii_personal"),
    ("address", "pii_address"),
    ("street", "pii_address"),
    ("postal_code", "pii_address"),
    ("zip_code", "pii_address"),
    ("ip_address", "network_identity"),
    ("user_ip", "network_identity"),
    ("mac_address", "network_identity"),
    ("password", "credential"),
    ("secret", "credential"),
    ("token", "credential"),
    ("api_key", "credential"),
]

_SENSITIVE_TYPES = {"inet": "network_identity", "cidr": "network_identity", "macaddr": "network_identity"}

_PARTITION_NAME_HINTS = [
    "order_date", "event_time", "event_date", "payment_date", "transaction_date",
    "created_at", "changed_at", "log_date", "partition_date", "report_date",
    "ship_date", "last_counted_at",
]
_PARTITION_TYPE_PREFIXES = ("date", "timestamp", "timestamptz")


def detect_sensitive_fields(columns: List[Dict]) -> Dict[str, str]:
    """Return {col_name: sensitivity_category} for columns that look sensitive."""
    result: Dict[str, str] = {}
    for col in columns:
        name_lower = col["name"].lower()
        col_type = col.get("type", "").lower()
        # Check type-based sensitivity first
        for sens_type, cat in _SENSITIVE_TYPES.items():
            if sens_type in col_type:
                result[col["name"]] = cat
                break
        else:
            # Check name-based patterns
            for pattern, cat in _SENSITIVE_PATTERNS:
                if pattern in name_lower:
                    result[col["name"]] = cat
                    break
    return result


def detect_partition_columns(
    columns: List[Dict],
    table_name: Optional[str] = None,
    schema: str = "public",
    engine=None,
) -> List[str]:
    """Detect partition key columns for a table.

    Strategy:
      1. If a SQLAlchemy engine is provided, query Postgres system catalogs
         for the real partition key columns (works for RANGE, LIST, HASH).
      2. Fall back to heuristic (date-typed columns with suggestive names)
         for non-Postgres databases or when the query fails.
    """
    # Try real detection via Postgres catalog
    if engine is not None and table_name:
        try:
            from sqlalchemy import text as sa_text
            _query = sa_text("""
                SELECT a.attname
                FROM   pg_partitioned_table pt
                JOIN   pg_class c ON c.oid = pt.partrelid
                JOIN   pg_namespace n ON n.oid = c.relnamespace
                JOIN   pg_attribute a ON a.attrelid = c.oid
                       AND a.attnum = ANY(pt.partattrs::smallint[])
                WHERE  c.relname   = :tbl
                  AND  n.nspname   = :sch
                ORDER  BY a.attnum;
            """)
            with engine.connect() as conn:
                rows = conn.execute(_query, {"tbl": table_name, "sch": schema}).fetchall()
            if rows:
                return [r[0] for r in rows]
            return []
        except Exception:
            pass  # non-Postgres or insufficient privileges

    # Heuristic fallback: date-typed columns with suggestive names
    candidates = []
    for col in columns:
        name_lower = col["name"].lower()
        col_type = col.get("type", "").lower()
        is_date_type = any(col_type.startswith(p) for p in _PARTITION_TYPE_PREFIXES)
        if not is_date_type:
            continue
        if name_lower in _PARTITION_NAME_HINTS or any(hint in name_lower for hint in ["_date", "_time", "_at"]):
            candidates.append(col["name"])
    return candidates


def detect_incremental_columns(columns: List[Dict], pk_columns: List[str]) -> List[str]:
    """Identify columns suitable for incremental/watermark loads.

    Good candidates:
      - Auto-increment / serial PKs
      - updated_at / modified_at timestamps
      - created_at timestamps (append-only tables)
    """
    inc_cols = []
    for col in columns:
        name_lower = col["name"].lower()
        # Already flagged by SQLAlchemy-level detection
        if col.get("is_incremental"):
            inc_cols.append(col["name"])
            continue
        # Timestamp columns commonly used for watermarks
        if any(kw in name_lower for kw in ["updated_at", "modified_at", "changed_at", "last_modified"]):
            inc_cols.append(col["name"])
        elif name_lower == "created_at":
            inc_cols.append(col["name"])
    return inc_cols


def detect_cdc_enabled(database_url: str, table_name: str, schema: str = "public", engine=None) -> bool:
    """Check if a table has CDC-friendly settings (Postgres: REPLICA IDENTITY != DEFAULT)."""
    if engine is None:
        engine = get_engine(database_url)
    dialect = engine.dialect.name
    if dialect != "postgresql":
        return False
    try:
        with engine.connect() as conn:
            result = conn.execute(text(
                "SELECT c.relreplident FROM pg_class c "
                "JOIN pg_namespace n ON n.oid = c.relnamespace "
                "WHERE n.nspname = :schema AND c.relname = :table"
            ), {"schema": schema, "table": table_name})
            row = result.fetchone()
            if row:
                # 'd' = default (only PK), 'f' = full, 'i' = index, 'n' = nothing
                # 'f' and 'i' mean CDC is intentionally configured
                return row[0] in ('f', 'i')
    except Exception:
        pass
    return False


def parse_connection_info(database_url: str) -> Dict[str, str]:
    """Extract host, port, database name, and driver from a database URL."""
    engine = get_engine(database_url)
    url = engine.url
    return {
        "host": str(url.host or ""),
        "port": str(url.port or ""),
        "database": str(url.database or ""),
        "driver": str(engine.dialect.name or ""),
    }


def classify_field(col_name: str) -> Optional[str]:
    """Classify a field based on its name."""
    col_name_lower = col_name.lower()
    if any(kw in col_name_lower for kw in ["price", "cost", "amount", "total", "subtotal"]):
        return "pricing"
    elif any(kw in col_name_lower for kw in ["quantity", "qty"]):
        return "quantity"
    elif any(kw in col_name_lower for kw in ["category", "type", "status"]):
        return "categorical"
    elif "created" in col_name_lower:
        return "temporal"
    elif any(kw in col_name_lower for kw in ["updated", "modified"]):
        return "temporal"
    elif any(kw in col_name_lower for kw in ["email", "phone"]):
        return "contact"
    return None


# ============================================================================
# Join Candidate and Data Category Detection
# ============================================================================

_JOIN_CANDIDATE_SUFFIXES = ("_id", "_key", "_code", "_ref", "_fk")

# Columns ending with a join-candidate suffix but are NOT FK candidates
_JOIN_CANDIDATE_EXCLUDE = {
    "postal_code", "zip_code", "area_code", "country_code", "currency_code",
    "language_code", "phone_code", "dialing_code", "iban_code", "swift_code",
    "barcode", "qr_code", "hash_code", "auth_code", "verification_code",
    "access_code", "promo_code", "discount_code", "coupon_code", "voucher_code",
    "error_code", "status_code", "exit_code", "response_code",
}

_ORDINAL_NAME_PATTERNS = [
    "priority", "grade", "rank", "rating", "severity",
    "score", "stage", "phase", "tier", "step", "order_num",
    "sequence", "position",
]

# Only match "level" when it's the whole name or a standalone word boundary
# (e.g. "priority_level", "access_level") but not "reorder_level"
_ORDINAL_LEVEL_EXCLUDE_PREFIXES = ("reorder", "stock", "inventory", "fill", "min", "max")


def _is_ordinal_by_name(col_name_lower: str) -> bool:
    """Check if a column name suggests ordinal data, with guards against false positives."""
    # Direct pattern match (non-level patterns)
    if any(p in col_name_lower for p in _ORDINAL_NAME_PATTERNS):
        return True

    # Special "level" handling: match only when not preceded by quantity-like prefixes
    if "level" in col_name_lower:
        for prefix in _ORDINAL_LEVEL_EXCLUDE_PREFIXES:
            if col_name_lower.startswith(prefix):
                return False
        return True

    return False


def detect_join_candidates(
    table_name: str,
    columns: List[Dict],
    pk_columns: List[str],
    fk_columns: List[Dict],
    all_tables_pks: Dict[str, List[str]],
) -> List[Dict[str, str]]:
    """Detect columns that are candidates for JOIN operations.

    Looks for columns following FK naming patterns (e.g. user_id, product_code)
    and tries to match them to other tables' primary keys.
    Skips columns that already have explicit FK constraints.

    Returns:
        List of dicts with keys: column, target_table, target_column, confidence
    """
    explicit_fk_cols = {fk["column"] for fk in fk_columns}

    candidates = []
    for col in columns:
        name = col["name"]
        name_lower = name.lower()

        # Skip own PK columns, columns with existing FK constraints, and known non-FK names
        if name in pk_columns or name in explicit_fk_cols:
            continue
        if name_lower in _JOIN_CANDIDATE_EXCLUDE:
            continue

        # Check for join-candidate suffixes
        matched_suffix = None
        for suffix in _JOIN_CANDIDATE_SUFFIXES:
            if name_lower.endswith(suffix):
                matched_suffix = suffix
                break

        if not matched_suffix:
            continue

        prefix = name_lower[: -len(matched_suffix)]
        if not prefix:
            continue

        # Try to find a matching table by name
        matched = False
        for other_table, other_pks in all_tables_pks.items():
            if other_table == table_name:
                continue
            other_lower = other_table.lower()

            # Match patterns: user_id -> users / user, customer_id -> customers
            if (
                other_lower == prefix
                or other_lower == prefix + "s"
                or other_lower == prefix + "es"
                or other_lower.rstrip("s") == prefix
                or other_lower.rstrip("es") == prefix
            ):
                # Find the matching PK column
                suffix_base = matched_suffix.lstrip("_")  # "id", "key", "code"
                target_col = None
                for pk in other_pks:
                    if pk.lower() == suffix_base or pk.lower() == name_lower:
                        target_col = pk
                        break
                if target_col is None and other_pks:
                    target_col = other_pks[0]

                candidates.append({
                    "column": name,
                    "target_table": other_table,
                    "target_column": target_col,
                    "confidence": "high",
                })
                matched = True
                break

        if not matched:
            candidates.append({
                "column": name,
                "target_table": None,
                "target_column": None,
                "confidence": "low",
            })

    return candidates


def classify_data_category(
    col_type_str: str, col_name: str,
    cardinality: int = 0, row_count: int = 0,
) -> Optional[str]:
    """Classify a column into a statistical data category.

    Categories:
        - continuous: Measurable values (float, decimal, money, timestamps)
        - discrete: Countable values (integer types, boolean)
        - ordinal: Ordered categories (detected by name patterns + type)
        - nominal: Unordered categories (text/varchar, uuid, enum)
        - None: Cannot classify (complex types like json, binary)
    """
    col_type = col_type_str.lower().strip()
    col_name_lower = col_name.lower()

    # Skip complex types
    if any(t in col_type for t in ("json", "jsonb", "bytea", "xml", "tsvector")):
        return None

    # --- Continuous ---
    if any(t in col_type for t in ("float", "double", "real", "money")):
        return "continuous"
    if "numeric" in col_type or "decimal" in col_type:
        # Has fractional scale -> continuous
        if "," in col_type:
            try:
                scale = int(col_type.split(",")[-1].rstrip(")").strip())
                if scale > 0:
                    return "continuous"
            except ValueError:
                pass
        return "continuous"
    # Date/time types are continuous
    if any(t in col_type for t in ("timestamp", "datetime", "date", "time", "interval")):
        return "continuous"

    # --- Discrete / Ordinal for integers ---
    if "bool" in col_type:
        return "discrete"
    if any(t in col_type for t in ("int", "serial")):
        if _is_ordinal_by_name(col_name_lower):
            return "ordinal"
        return "discrete"

    # --- Nominal / Ordinal for text ---
    if any(t in col_type for t in ("varchar", "char", "text", "citext", "name")):
        if _is_ordinal_by_name(col_name_lower):
            return "ordinal"
        return "nominal"

    # UUID, inet, macaddr, enum
    if any(t in col_type for t in ("uuid", "inet", "macaddr")):
        return "nominal"
    if "enum" in col_type:
        if _is_ordinal_by_name(col_name_lower):
            return "ordinal"
        return "nominal"

    return None


def analyze_database_to_json(
    database_url: str,
    output_path: str,
    schema: Optional[str] = None,
    include_sample_data: bool = False
) -> Dict[str, Any]:
    """Analyze database schema and save fully enriched schema to JSON file.

    This is the main function - it does everything:
    1. Connects to database
    2. Fetches all table metadata
    3. Enriches with sensitive fields, partitions, incremental columns, CDC status
    4. Saves to JSON file

    Args:
        database_url: Database connection URL
        output_path: Path to save JSON file (e.g., "schema.json")
        schema: Optional schema name to filter tables
        include_sample_data: If True, include sample data rows (default: False)

    Returns:
        Dict containing the full enriched schema (same as what's saved to JSON)

    Example:
        >>> schema = analyze_database_to_json(
        ...     "postgresql://user:pass@host/db",
        ...     "database_schema.json",
        ...     schema="public"
        ... )
    """
    logger.info(f"Starting database analysis for: {database_url}")
    
    # Use a single shared engine for all DB access
    engine = get_engine(database_url)
    dialect = engine.dialect.name

    try:
        # Fetch basic metadata
        tables = fetch_tables(database_url, schema=schema)
        total_tables = len(tables)

        if total_tables == 0:
            logger.warning("No tables found to analyze.")
            return {"error": "No tables found"}

        logger.info(f"Found {total_tables} tables")

        # Connection-level metadata
        connection_info = parse_connection_info(database_url)
        try:
            db_timezone = fetch_database_timezone(database_url)
        except Exception:
            db_timezone = "unknown"

        # Bulk-fetch schema for ALL tables
        try:
            row_counts = fetch_row_counts(database_url, tables, schema=schema)
        except Exception:
            row_counts = {}

        try:
            all_columns = fetch_columns(database_url, table_names=tables, schema=schema)
        except Exception:
            all_columns = {}

        try:
            all_pks = fetch_primary_keys(database_url, table_names=tables, schema=schema)
        except Exception:
            all_pks = {}

        try:
            all_fks = fetch_foreign_keys(database_url, table_names=tables, schema=schema)
        except Exception:
            all_fks = {}

        # Build enriched schema
        enriched_tables = []
        total_rows = 0

        for idx, table_name in enumerate(tables):
            logger.info(f"Analyzing table {idx + 1}/{total_tables}: {table_name}")
            
            try:
                table_columns = all_columns.get(table_name, [])
                pk_columns = all_pks.get(table_name, [])
                fk_columns = all_fks.get(table_name, [])
                row_count = row_counts.get(table_name, 0)
                total_rows += row_count

                # Get sample data if requested
                sample_data = None
                if include_sample_data:
                    try:
                        colnames, rows = fetch_sample_rows(database_url, table_name, limit=10)
                        sample_data = {col: [row[i] for row in rows] for i, col in enumerate(colnames)}
                    except Exception:
                        pass

                # Field classification
                field_classifications = {}
                for col in table_columns:
                    classification = classify_field(col["name"])
                    if classification:
                        field_classifications[col["name"]] = classification

                # Enriched metadata
                sensitive_fields = detect_sensitive_fields(table_columns)
                partition_columns = detect_partition_columns(
                    table_columns,
                    table_name=table_name,
                    schema=schema or "public",
                    engine=engine,
                )
                incremental_columns = detect_incremental_columns(table_columns, pk_columns)
                table_schema = schema or "public"

                # CDC check
                try:
                    cdc_enabled = detect_cdc_enabled(database_url, table_name, schema=table_schema, engine=engine)
                except Exception:
                    cdc_enabled = False

                # Column-level statistics: cardinality, null counts, data range
                try:
                    col_statistics = fetch_column_statistics(
                        engine, table_name, table_columns,
                        schema=table_schema, row_count=row_count,
                    )
                except Exception:
                    col_statistics = {}

                # Join candidates (implicit FKs not covered by explicit constraints)
                join_candidates = detect_join_candidates(
                    table_name, table_columns, pk_columns, fk_columns, all_pks,
                )

                # Build enriched columns with all column-level metadata
                enriched_columns = []
                for col in table_columns:
                    col_dict = {
                        "name": col["name"],
                        "type": col["type"],
                        "nullable": col.get("nullable", True),
                        "is_incremental": col.get("is_incremental", False),
                    }

                    # Timezone for date/time columns
                    col_tz = get_column_timezone(col["type"], dialect, db_timezone)
                    if col_tz is not None:
                        col_dict["column_timezone"] = col_tz

                    # Column statistics (cardinality, null_count, data_range)
                    stats = col_statistics.get(col["name"], {})
                    if stats:
                        col_dict["cardinality"] = stats.get("cardinality", 0)
                        col_dict["null_count"] = stats.get("null_count", 0)
                        if "data_range" in stats:
                            col_dict["data_range"] = stats["data_range"]

                    # Data category (nominal, ordinal, discrete, continuous)
                    data_cat = classify_data_category(
                        col["type"], col["name"],
                        cardinality=stats.get("cardinality", 0),
                        row_count=row_count,
                    )
                    if data_cat:
                        col_dict["data_category"] = data_cat

                    enriched_columns.append(col_dict)

                # Build table entry
                table_entry = {
                    "table": table_name,
                    "schema": table_schema,
                    "columns": enriched_columns,
                    "primary_keys": pk_columns,
                    "foreign_keys": [
                        {
                            "column": fk["column"],
                            "references": fk["references"],
                        }
                        for fk in fk_columns
                    ],
                    "row_count": row_count,
                    "field_classifications": field_classifications,
                    "sensitive_fields": sensitive_fields,
                    "incremental_columns": incremental_columns,
                    "partition_columns": partition_columns,
                    "join_candidates": join_candidates,
                    "cdc_enabled": cdc_enabled,
                    "has_primary_key": len(pk_columns) > 0,
                    "has_foreign_keys": len(fk_columns) > 0,
                    "has_sensitive_fields": len(sensitive_fields) > 0,
                }

                if sample_data:
                    table_entry["sample_data"] = sample_data

                enriched_tables.append(table_entry)

            except Exception as e:
                logger.warning(f"Skipped table '{table_name}': {e}")
                continue

        # Build final schema document
        schema_document = {
            "metadata": {
                "generated_at": datetime.now(timezone.utc).isoformat(),
                "database_url": database_url.split("@")[-1] if "@" in database_url else database_url,  # Hide credentials
                "schema_filter": schema,
                "total_tables": len(enriched_tables),
                "total_rows": total_rows,
            },
            "connection": {
                **connection_info,
                "timezone": db_timezone,
            },
            "tables": enriched_tables,
        }

        # Save to JSON file
        logger.info(f"Saving enriched schema to: {output_path}")
        with open(output_path, 'w', encoding='utf-8') as f:
            json.dump(schema_document, f, indent=2, default=str)

        logger.info(f"Successfully analyzed {len(enriched_tables)} tables and saved to {output_path}")
        return schema_document

    except Exception as e:
        logger.error(f"Error analyzing database: {e}")
        raise


if __name__ == "__main__":
    import sys

    _load_env_file()

    if len(sys.argv) < 3:
        print("Usage: python database_analyzer.py <database_url> <output_json_path> [schema]")
        print("  schema can also be set via DATABASE_SCHEMA or SCHEMA in .env")
        sys.exit(1)

    database_url = sys.argv[1]
    output_path = sys.argv[2]
    schema = (
        sys.argv[3]
        if len(sys.argv) > 3
        else os.environ.get("DATABASE_SCHEMA") or os.environ.get("SCHEMA") or None
    )

    analyze_database_to_json(database_url, output_path, schema=schema)
