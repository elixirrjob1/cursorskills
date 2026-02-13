#!/usr/bin/env python3
"""
Source System Analyzer

Analyzes a source database schema and assesses data quality in a single pass.
Produces a combined schema.json with:
- Schema metadata (tables, columns, PKs, FKs, enrichments)
- Data quality findings (9 checks: controlled values, constraints, format, etc.)

Usage:
    python source_system_analyzer.py <database_url> <output_json_path> [schema]
"""

import math
import os
import json
import logging
import re
import warnings
from pathlib import Path
from typing import Dict, List, Optional, Any, Set
from datetime import datetime, timezone
from collections import Counter

from sqlalchemy import create_engine, inspect, text, MetaData, Table, select
from sqlalchemy.engine import Engine
from sqlalchemy.exc import SAWarning


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


logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")


# ============================================================================
# Database connection
# ============================================================================

def get_engine(database_url: str) -> Engine:
    """Create a SQLAlchemy engine from a database URL with connection pooling."""
    return create_engine(
        database_url,
        pool_size=5,
        max_overflow=10,
        pool_pre_ping=True,
        connect_args={"connect_timeout": 10},
        echo=False,
    )


# ============================================================================
# Schema analysis (from database_analyzer)
# ============================================================================

def format_type(col_type: str, col_info: dict) -> str:
    """Format column type string from SQLAlchemy column info."""
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
            return f"{base_type}({precision},{scale})" if scale else f"{base_type}({precision})"
        return f"{base_type}({precision},{scale})"

    type_str = str(col_type)
    if '(' in type_str and ')' in type_str:
        return type_str
    return base_type


def is_incremental_column(col_info: dict, col_type: str) -> bool:
    """Detect if a column is an incremental/auto-increment column."""
    col_type_str = str(col_type).lower()
    default_val = str(col_info.get('default', '')).lower() if col_info.get('default') else ''
    if any(t in col_type_str for t in ['serial', 'bigserial', 'smallserial']):
        return True
    if 'auto_increment' in default_val or 'nextval' in default_val:
        return True
    if 'identity' in col_type_str or col_info.get('identity', False):
        return True
    if col_info.get('autoincrement', False):
        return True
    return False


_DATETIME_TYPE_KEYWORDS = ("timestamp", "datetime", "date", "time", "smalldatetime", "datetimeoffset")
_TZ_AWARE_TYPES = {
    "postgresql": ("timestamptz", "timestamp with time zone", "timetz", "time with time zone"),
    "mysql": ("timestamp",),
    "mssql": ("datetimeoffset",),
}
_TZ_AWARE_INTERPRETATION = {
    "postgresql": "UTC",
    "mysql": "UTC",
    "mssql": "offset_embedded",
}


def get_column_timezone(col_type_str: str, dialect: str, server_timezone: str) -> Optional[str]:
    """Determine the effective timezone of a date/timestamp column."""
    col_type_lower = col_type_str.lower().strip()
    is_datetime = any(kw in col_type_lower for kw in _DATETIME_TYPE_KEYWORDS)
    if not is_datetime or col_type_lower == "date":
        return None
    aware_types = _TZ_AWARE_TYPES.get(dialect, ())
    if any(t in col_type_lower for t in aware_types):
        return _TZ_AWARE_INTERPRETATION.get(dialect, server_timezone)
    if dialect == "sqlite":
        return "unknown"
    return server_timezone


def fetch_schema_metadata(engine: Engine, schema: Optional[str] = None) -> Dict[str, Any]:
    """Fetch tables, columns, primary keys, and foreign keys in a single inspector pass.

    Returns a dict with keys: 'tables', 'columns', 'primary_keys', 'foreign_keys'.
    """
    inspector = inspect(engine)
    schemas_to_check = [schema] if schema else inspector.get_schema_names()

    target_tables: Dict[str, str] = {}
    for sch in schemas_to_check:
        if schema and sch != schema:
            continue
        for table_name in inspector.get_table_names(schema=sch):
            target_tables[table_name] = sch

    table_names = sorted(target_tables.keys())
    columns_by_table: Dict[str, List[Dict]] = {}
    pk_by_table: Dict[str, List[str]] = {}
    fk_by_table: Dict[str, List[Dict]] = {}

    with warnings.catch_warnings():
        warnings.filterwarnings('ignore', category=SAWarning, message='Did not recognize type')
        for table_name in table_names:
            table_schema = target_tables[table_name]
            try:
                columns = inspector.get_columns(table_name, schema=table_schema)
                columns_by_table[table_name] = [
                    {
                        "name": col['name'],
                        "type": format_type(col['type'], col),
                        "nullable": col.get('nullable', True),
                        "default": str(col.get('default', '')) if col.get('default') is not None else None,
                        "is_incremental": is_incremental_column(col, col['type']),
                    }
                    for col in columns
                ]
            except Exception:
                columns_by_table[table_name] = []

            try:
                pk_constraint = inspector.get_pk_constraint(table_name, schema=table_schema)
                if pk_constraint and pk_constraint.get('constrained_columns'):
                    pk_by_table[table_name] = pk_constraint['constrained_columns']
            except Exception:
                pass

            try:
                foreign_keys = inspector.get_foreign_keys(table_name, schema=table_schema)
                fk_by_table[table_name] = [
                    {"column": local_col, "references": f"{fk['referred_table']}.{ref_col}"}
                    for fk in foreign_keys
                    for local_col, ref_col in zip(fk['constrained_columns'], fk['referred_columns'])
                ]
            except Exception:
                pass

    return {
        "tables": table_names,
        "columns": columns_by_table,
        "primary_keys": pk_by_table,
        "foreign_keys": fk_by_table,
    }


def fetch_sample_rows(engine: Engine, table: str, limit: int):
    """Fetch sample rows from a table."""
    with engine.connect() as conn:
        try:
            metadata = MetaData()
            table_obj = Table(table, metadata, autoload_with=engine)
            result = conn.execute(select(table_obj).limit(limit))
            rows = result.fetchall()
            return list(result.keys()), rows
        except Exception:
            result = conn.execute(text(f'SELECT * FROM "{table}" LIMIT :limit'), {"limit": limit})
            return list(result.keys()), result.fetchall()


def fetch_row_counts(engine: Engine, table_names: List[str], schema: str = None) -> Dict[str, int]:
    """Fetch row counts for all specified tables."""
    row_counts = {}
    with engine.connect() as conn:
        for table_name in table_names:
            try:
                q = f'SELECT COUNT(*) FROM "{schema}"."{table_name}"' if schema else f'SELECT COUNT(*) FROM "{table_name}"'
                count = conn.execute(text(q)).scalar()
                row_counts[table_name] = count if count is not None else 0
            except Exception:
                row_counts[table_name] = 0
    return row_counts


def fetch_database_timezone(engine: Engine) -> str:
    """Fetch the database server timezone."""
    dialect = engine.dialect.name
    with engine.connect() as conn:
        try:
            if dialect == "postgresql":
                return conn.execute(text("SHOW timezone")).scalar() or "Unknown"
            elif dialect == "mysql":
                return conn.execute(text("SELECT @@global.time_zone")).scalar() or "Unknown"
            elif dialect == "sqlite":
                return "UTC (SQLite default)"
            elif dialect == "mssql":
                return conn.execute(text("SELECT CURRENT_TIMEZONE()")).scalar() or "Unknown"
            return f"Unknown ({dialect})"
        except Exception:
            return "Unknown"


_RANGE_SKIP_TYPES = (
    "json", "jsonb", "bytea", "xml", "tsvector", "tsquery",
    "point", "line", "lseg", "box", "path", "polygon", "circle",
    "array", "user-defined", "bool",
)


def fetch_column_statistics(engine, table_name: str, columns: List[Dict], schema: str = None, row_count: int = 0) -> Dict[str, Dict]:
    """Fetch cardinality, null count, and data range for all columns in a table."""
    empty_stats = {col["name"]: {"cardinality": 0, "null_count": 0} for col in columns}
    if not columns or row_count == 0:
        return empty_stats

    stats_parts = []
    range_columns = set()
    for col in columns:
        col_name = col["name"]
        col_type = col.get("type", "").lower()
        quoted = f'"{col_name}"'
        stats_parts.append(f'COUNT(DISTINCT {quoted}) AS "{col_name}__card"')
        stats_parts.append(f'SUM(CASE WHEN {quoted} IS NULL THEN 1 ELSE 0 END) AS "{col_name}__nulls"')
        if not any(s in col_type for s in _RANGE_SKIP_TYPES):
            stats_parts.append(f'MIN({quoted}) AS "{col_name}__min"')
            stats_parts.append(f'MAX({quoted}) AS "{col_name}__max"')
            range_columns.add(col_name)

    from_clause = f'"{schema}"."{table_name}"' if schema else f'"{table_name}"'
    query = f"SELECT {', '.join(stats_parts)} FROM {from_clause}"

    try:
        with engine.connect() as conn:
            row = conn.execute(text(query)).fetchone()
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
                mn, mx = row_dict.get(f"{col_name}__min"), row_dict.get(f"{col_name}__max")
                if mn is not None or mx is not None:
                    col_stats["data_range"] = {
                        "min": str(mn) if mn is not None else None,
                        "max": str(mx) if mx is not None else None,
                    }
            stats[col_name] = col_stats
        return stats
    except Exception as e:
        logger.warning(f"Could not fetch column statistics for '{table_name}': {e}")
        return empty_stats


# ============================================================================
# Metadata detection (from database_analyzer)
# ============================================================================

_SENSITIVE_PATTERNS: List[tuple] = [
    ("ssn", "government_id"), ("social_security", "government_id"), ("tax_id", "government_id"),
    ("passport", "government_id"), ("national_id", "government_id"), ("driver_license", "government_id"),
    ("credit_card", "financial"), ("card_number", "financial"), ("bank_account", "financial"),
    ("routing_number", "financial"), ("iban", "financial"), ("salary", "financial"),
    ("email", "pii_contact"), ("phone", "pii_contact"), ("mobile", "pii_contact"),
    ("date_of_birth", "pii_personal"), ("dob", "pii_personal"), ("gender", "pii_personal"),
    ("address", "pii_address"), ("street", "pii_address"), ("postal_code", "pii_address"),
    ("ip_address", "network_identity"), ("password", "credential"), ("secret", "credential"),
    ("token", "credential"), ("api_key", "credential"),
]
_SENSITIVE_TYPES = {"inet": "network_identity", "cidr": "network_identity", "macaddr": "network_identity"}
_PARTITION_NAME_HINTS = [
    "order_date", "event_time", "event_date", "payment_date", "transaction_date",
    "created_at", "changed_at", "log_date", "partition_date", "report_date",
]
_PARTITION_TYPE_PREFIXES = ("date", "timestamp", "timestamptz")


def detect_sensitive_fields(columns: List[Dict]) -> Dict[str, str]:
    """Return {col_name: sensitivity_category} for columns that look sensitive."""
    result = {}
    for col in columns:
        name_lower = col["name"].lower()
        col_type = col.get("type", "").lower()
        for sens_type, cat in _SENSITIVE_TYPES.items():
            if sens_type in col_type:
                result[col["name"]] = cat
                break
        else:
            for pattern, cat in _SENSITIVE_PATTERNS:
                if pattern in name_lower:
                    result[col["name"]] = cat
                    break
    return result


def detect_partition_columns(columns: List[Dict], table_name: Optional[str] = None, schema: str = "public", engine=None) -> List[str]:
    """Detect partition key columns for a table."""
    if engine and table_name:
        try:
            q = text("""
                SELECT a.attname FROM pg_partitioned_table pt
                JOIN pg_class c ON c.oid = pt.partrelid
                JOIN pg_namespace n ON n.oid = c.relnamespace
                JOIN pg_attribute a ON a.attrelid = c.oid AND a.attnum = ANY(pt.partattrs::smallint[])
                WHERE c.relname = :tbl AND n.nspname = :sch
                ORDER BY a.attnum
            """)
            with engine.connect() as conn:
                rows = conn.execute(q, {"tbl": table_name, "sch": schema}).fetchall()
            if rows:
                return [r[0] for r in rows]
        except Exception:
            pass
    candidates = []
    for col in columns:
        name_lower = col["name"].lower()
        col_type = col.get("type", "").lower()
        if any(col_type.startswith(p) for p in _PARTITION_TYPE_PREFIXES):
            if name_lower in _PARTITION_NAME_HINTS or any(h in name_lower for h in ["_date", "_time", "_at"]):
                candidates.append(col["name"])
    return candidates


def detect_incremental_columns(columns: List[Dict], pk_columns: List[str]) -> List[str]:
    """Identify columns suitable for incremental/watermark loads."""
    inc_cols = []
    for col in columns:
        name_lower = col["name"].lower()
        if col.get("is_incremental"):
            inc_cols.append(col["name"])
        elif any(kw in name_lower for kw in ["updated_at", "modified_at", "changed_at", "last_modified"]):
            inc_cols.append(col["name"])
        elif name_lower == "created_at":
            inc_cols.append(col["name"])
    return inc_cols


def detect_cdc_enabled(engine: Engine, table_name: str, schema: str = "public") -> bool:
    """Check if a table has CDC-friendly settings (Postgres: REPLICA IDENTITY != DEFAULT)."""
    if engine.dialect.name != "postgresql":
        return False
    try:
        with engine.connect() as conn:
            row = conn.execute(text(
                "SELECT c.relreplident FROM pg_class c "
                "JOIN pg_namespace n ON n.oid = c.relnamespace "
                "WHERE n.nspname = :schema AND c.relname = :table"
            ), {"schema": schema, "table": table_name}).fetchone()
            return row and row[0] in ('f', 'i')
    except Exception:
        return False


def parse_connection_info(engine: Engine) -> Dict[str, str]:
    """Extract host, port, database name, and driver from a database URL."""
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
    elif "created" in col_name_lower or any(kw in col_name_lower for kw in ["updated", "modified"]):
        return "temporal"
    elif any(kw in col_name_lower for kw in ["email", "phone"]):
        return "contact"
    return None


_JOIN_CANDIDATE_SUFFIXES = ("_id", "_key", "_code", "_ref", "_fk")
_JOIN_CANDIDATE_EXCLUDE = {
    "postal_code", "zip_code", "area_code", "country_code", "currency_code",
    "language_code", "phone_code", "dialing_code", "iban_code", "swift_code",
    "barcode", "qr_code", "hash_code", "auth_code", "verification_code",
    "access_code", "promo_code", "discount_code", "coupon_code", "voucher_code",
    "error_code", "status_code", "exit_code", "response_code",
}
_ORDINAL_NAME_PATTERNS = ["priority", "grade", "rank", "rating", "severity", "score", "stage", "phase", "tier", "step", "order_num", "sequence", "position"]
_ORDINAL_LEVEL_EXCLUDE_PREFIXES = ("reorder", "stock", "inventory", "fill", "min", "max")


def _is_ordinal_by_name(col_name_lower: str) -> bool:
    if any(p in col_name_lower for p in _ORDINAL_NAME_PATTERNS):
        return True
    if "level" in col_name_lower:
        for prefix in _ORDINAL_LEVEL_EXCLUDE_PREFIXES:
            if col_name_lower.startswith(prefix):
                return False
        return True
    return False


def detect_join_candidates(table_name: str, columns: List[Dict], pk_columns: List[str], fk_columns: List[Dict], all_tables_pks: Dict[str, List[str]]) -> List[Dict]:
    """Detect columns that are candidates for JOIN operations."""
    explicit_fk_cols = {fk["column"] for fk in fk_columns}
    candidates = []
    for col in columns:
        name = col["name"]
        name_lower = name.lower()
        if name in pk_columns or name in explicit_fk_cols or name_lower in _JOIN_CANDIDATE_EXCLUDE:
            continue
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
        for other_table, other_pks in all_tables_pks.items():
            if other_table == table_name:
                continue
            other_lower = other_table.lower()
            if (other_lower == prefix or other_lower == prefix + "s" or other_lower == prefix + "es"
                    or other_lower.rstrip("s") == prefix or other_lower.rstrip("es") == prefix):
                suffix_base = matched_suffix.lstrip("_")
                target_col = next((pk for pk in other_pks if pk.lower() == suffix_base or pk.lower() == name_lower), None)
                target_col = target_col or (other_pks[0] if other_pks else None)
                candidates.append({"column": name, "target_table": other_table, "target_column": target_col, "confidence": "high"})
                break
        else:
            candidates.append({"column": name, "target_table": None, "target_column": None, "confidence": "low"})
    return candidates


def classify_data_category(col_type_str: str, col_name: str, cardinality: int = 0, row_count: int = 0) -> Optional[str]:
    """Classify a column into a statistical data category."""
    col_type = col_type_str.lower().strip()
    col_name_lower = col_name.lower()
    if any(t in col_type for t in ("json", "jsonb", "bytea", "xml", "tsvector")):
        return None
    if any(t in col_type for t in ("float", "double", "real", "money")):
        return "continuous"
    if "numeric" in col_type or "decimal" in col_type:
        return "continuous"
    if any(t in col_type for t in ("timestamp", "datetime", "date", "time", "interval")):
        return "continuous"
    if "bool" in col_type:
        return "discrete"
    if any(t in col_type for t in ("int", "serial")):
        return "ordinal" if _is_ordinal_by_name(col_name_lower) else "discrete"
    if any(t in col_type for t in ("varchar", "char", "text", "citext", "name")):
        return "ordinal" if _is_ordinal_by_name(col_name_lower) else "nominal"
    if any(t in col_type for t in ("uuid", "inet", "macaddr")):
        return "nominal"
    if "enum" in col_type:
        return "ordinal" if _is_ordinal_by_name(col_name_lower) else "nominal"
    return None


# ============================================================================
# Data quality: constraint introspection (PostgreSQL)
# ============================================================================

def fetch_check_constraints(engine: Engine, schema: str) -> Dict[str, List[Dict]]:
    """Fetch CHECK constraints grouped by table."""
    result = {}
    query = text("""
        SELECT tc.table_name, ccu.column_name, tc.constraint_name, cc.check_clause
        FROM information_schema.table_constraints tc
        JOIN information_schema.check_constraints cc ON tc.constraint_name = cc.constraint_name AND tc.constraint_schema = cc.constraint_schema
        JOIN information_schema.constraint_column_usage ccu ON tc.constraint_name = ccu.constraint_name AND tc.constraint_schema = ccu.constraint_schema
        WHERE tc.constraint_type = 'CHECK' AND tc.table_schema = :schema AND tc.constraint_name NOT LIKE '%_not_null'
    """)
    try:
        with engine.connect() as conn:
            for row in conn.execute(query, {"schema": schema}).fetchall():
                result.setdefault(row[0], []).append({"column": row[1], "constraint_name": row[2], "check_clause": row[3]})
    except Exception as e:
        logger.warning(f"Could not fetch CHECK constraints: {e}")
    return result


def fetch_enum_columns(engine: Engine, schema: str) -> Dict[str, Dict[str, List[str]]]:
    """Fetch columns using ENUM types and their allowed values."""
    result = {}
    query = text("""
        SELECT c.table_name, c.column_name, c.udt_name, array_agg(e.enumlabel ORDER BY e.enumsortorder) AS enum_values
        FROM information_schema.columns c
        JOIN pg_type t ON t.typname = c.udt_name
        JOIN pg_enum e ON e.enumtypid = t.oid
        WHERE c.table_schema = :schema AND c.data_type = 'USER-DEFINED'
        GROUP BY c.table_name, c.column_name, c.udt_name
    """)
    try:
        with engine.connect() as conn:
            for row in conn.execute(query, {"schema": schema}).fetchall():
                result.setdefault(row[0], {})[row[1]] = list(row[3])
    except Exception as e:
        logger.warning(f"Could not fetch ENUM columns: {e}")
    return result


def fetch_unique_constraints(engine: Engine, schema: str) -> Dict[str, Set[str]]:
    """Fetch columns that have UNIQUE constraints."""
    result = {}
    query = text("""
        SELECT tc.table_name, kcu.column_name
        FROM information_schema.table_constraints tc
        JOIN information_schema.key_column_usage kcu ON tc.constraint_name = kcu.constraint_name AND tc.table_schema = kcu.table_schema
        WHERE tc.constraint_type = 'UNIQUE' AND tc.table_schema = :schema
    """)
    try:
        with engine.connect() as conn:
            for row in conn.execute(query, {"schema": schema}).fetchall():
                result.setdefault(row[0], set()).add(row[1])
    except Exception as e:
        logger.warning(f"Could not fetch UNIQUE constraints: {e}")
    return result


# ============================================================================
# Data quality: pattern constants and helpers
# ============================================================================

_TEXT_TYPES = ("text", "varchar", "char", "citext", "name", "character varying", "character")
_FREEFORM_EXACT: Set[str] = {
    "name", "description", "desc", "comment", "note", "notes", "title", "body", "content", "message", "summary", "detail",
    "first_name", "last_name", "full_name", "display_name", "contact_name", "username", "email", "phone", "mobile", "fax",
    "address", "street", "url", "uri", "path", "filename", "password", "token", "secret", "api_key", "sku", "barcode", "code", "uuid",
}
_FREEFORM_SUFFIXES = ("_name", "_description", "_desc", "_comment", "_email", "_phone", "_address", "_url", "_password")
_CONTROLLED_VALUE_MAX_CARDINALITY = 20
_PRICING_PATTERNS = ("price", "cost", "amount", "total", "subtotal", "fee", "charge", "rate")
_QUANTITY_PATTERNS = ("quantity", "qty", "count", "quantity_on_hand")
_JOIN_SUFFIXES = ("_id", "_key", "_code", "_ref", "_fk")
_JOIN_EXCLUDE = {
    "postal_code", "zip_code", "area_code", "country_code", "currency_code", "language_code", "phone_code",
    "iban_code", "swift_code", "barcode", "qr_code", "hash_code", "auth_code", "verification_code",
    "access_code", "promo_code", "discount_code", "coupon_code", "error_code", "status_code", "exit_code", "response_code",
}
_EMAIL_RE = re.compile(r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$')
_PHONE_RE = re.compile(r'^[+]?[\d\s\-().]{7,20}$')
_DATE_TEXT_RE = re.compile(r'^\d{4}[-/]\d{2}[-/]\d{2}')
_URL_RE = re.compile(r'^https?://')
_NUMERIC_TEXT_RE = re.compile(r'^-?\d+\.?\d*$')


def _is_text_type(col_type: str) -> bool:
    return any(t in col_type.lower() for t in _TEXT_TYPES)


def _is_numeric_type(col_type: str) -> bool:
    return any(t in col_type.lower() for t in ("int", "numeric", "decimal", "float", "double", "real", "money", "serial"))


def _is_freeform_column(col_name: str) -> bool:
    lower = col_name.lower()
    if lower in _FREEFORM_EXACT:
        return True
    return any(lower.endswith(s) for s in _FREEFORM_SUFFIXES)


# ============================================================================
# Data quality: 9 check functions
# ============================================================================

def check_controlled_value_candidates(engine: Engine, tables: List[Dict], check_constraints: Dict, enum_columns: Dict, unique_constraints: Dict[str, Set[str]], schema: str) -> List[Dict]:
    findings = []
    for tbl in tables:
        table_name = tbl["table"]
        if tbl.get("row_count", 0) == 0:
            continue
        pk_set = set(tbl.get("primary_keys", []))
        fk_set = {fk["column"] for fk in tbl.get("foreign_keys", [])}
        check_set = {c["column"] for c in check_constraints.get(table_name, [])}
        enum_set = set(enum_columns.get(table_name, {}).keys())
        unique_set = unique_constraints.get(table_name, set())

        for col in tbl.get("columns", []):
            col_name = col["name"]
            col_type = col.get("type", "")
            cardinality = col.get("cardinality", 0)
            if not _is_text_type(col_type) or cardinality == 0 or cardinality > _CONTROLLED_VALUE_MAX_CARDINALITY:
                continue
            if col_name in pk_set | fk_set | check_set | enum_set | unique_set or _is_freeform_column(col_name):
                continue

            distinct_values = []
            try:
                q = text(f'SELECT DISTINCT "{col_name}" FROM "{schema}"."{table_name}" WHERE "{col_name}" IS NOT NULL ORDER BY "{col_name}" LIMIT 25')
                with engine.connect() as conn:
                    distinct_values = [str(r[0]) for r in conn.execute(q).fetchall()]
            except Exception:
                pass

            values_display = ", ".join(repr(v) for v in distinct_values[:10])
            findings.append({
                "table": table_name, "column": col_name, "check": "controlled_value_candidate", "severity": "warning",
                "detail": f"Text column with {cardinality} distinct value(s) ({values_display}) but no CHECK, ENUM, or FK constraint",
                "recommendation": "Add a CHECK constraint, convert to an ENUM type, or create a lookup/reference table to prevent invalid values",
                "distinct_values": distinct_values, "cardinality": cardinality,
            })
    return findings


def check_nullable_but_never_null(tables: List[Dict]) -> List[Dict]:
    findings = []
    for tbl in tables:
        row_count = tbl.get("row_count", 0)
        if row_count == 0:
            continue
        for col in tbl.get("columns", []):
            if col.get("nullable") and col.get("null_count", 0) == 0:
                findings.append({
                    "table": tbl["table"], "column": col["name"], "check": "nullable_but_never_null", "severity": "info",
                    "detail": f"Column is nullable but has 0 NULLs across {row_count} row(s)",
                    "recommendation": "Consider adding a NOT NULL constraint if the column should always have a value",
                })
    return findings


def check_missing_primary_keys(tables: List[Dict]) -> List[Dict]:
    findings = []
    for tbl in tables:
        if not tbl.get("has_primary_key", True):
            findings.append({
                "table": tbl["table"], "column": None, "check": "missing_primary_key", "severity": "critical",
                "detail": "Table has no primary key defined",
                "recommendation": "Add a primary key to ensure row uniqueness and enable efficient lookups",
            })
    return findings


def check_missing_foreign_keys(engine: Engine, tables: List[Dict], all_pks: Dict[str, List[str]], schema: str) -> List[Dict]:
    findings = []
    for tbl in tables:
        table_name = tbl["table"]
        row_count = tbl.get("row_count", 0)
        pk_set = set(tbl.get("primary_keys", []))
        fk_set = {fk["column"] for fk in tbl.get("foreign_keys", [])}

        for col in tbl.get("columns", []):
            col_name = col["name"]
            name_lower = col_name.lower()
            if col_name in pk_set | fk_set or name_lower in _JOIN_EXCLUDE:
                continue
            matched_suffix = next((s for s in _JOIN_SUFFIXES if name_lower.endswith(s)), None)
            if not matched_suffix:
                continue
            prefix = name_lower[: -len(matched_suffix)]
            if not prefix:
                continue

            target_table = target_column = None
            for other_table, other_pks in all_pks.items():
                if other_table == table_name:
                    continue
                ol = other_table.lower()
                if ol in (prefix, prefix + "s", prefix + "es") or ol.rstrip("s") == prefix or ol.rstrip("es") == prefix:
                    target_table = other_table
                    suffix_base = matched_suffix.lstrip("_")
                    target_column = next((pk for pk in other_pks if pk.lower() in (suffix_base, name_lower)), None)
                    target_column = target_column or (other_pks[0] if other_pks else None)
                    break

            if not target_table:
                continue

            orphan_sample = []
            if row_count > 0 and target_column:
                try:
                    q = text(f'''
                        SELECT DISTINCT s."{col_name}" FROM "{schema}"."{table_name}" s
                        LEFT JOIN "{schema}"."{target_table}" t ON s."{col_name}" = t."{target_column}"
                        WHERE s."{col_name}" IS NOT NULL AND t."{target_column}" IS NULL LIMIT 10
                    ''')
                    with engine.connect() as conn:
                        orphan_sample = [str(r[0]) for r in conn.execute(q).fetchall()]
                except Exception:
                    pass

            detail = f"Column follows FK naming pattern and matches {target_table}.{target_column} but has no FK constraint"
            severity = "critical" if orphan_sample else "warning"
            if orphan_sample:
                detail += f". Found {len(orphan_sample)} orphaned value(s): {', '.join(orphan_sample)}"

            finding = {
                "table": table_name, "column": col_name, "check": "missing_foreign_key", "severity": severity,
                "detail": detail,
                "recommendation": f"Add FOREIGN KEY constraint referencing {target_table}({target_column}) to enforce referential integrity",
                "target_table": target_table, "target_column": target_column,
            }
            if orphan_sample:
                finding["orphaned_values"] = orphan_sample
            findings.append(finding)
    return findings


def check_format_inconsistency(engine: Engine, tables: List[Dict], schema: str, sample_size: int = 200) -> List[Dict]:
    findings = []
    patterns = {"email": _EMAIL_RE, "phone": _PHONE_RE, "date_as_text": _DATE_TEXT_RE, "url": _URL_RE, "numeric_as_text": _NUMERIC_TEXT_RE}
    for tbl in tables:
        table_name = tbl["table"]
        if tbl.get("row_count", 0) == 0:
            continue
        for col in tbl.get("columns", []):
            if not _is_text_type(col.get("type", "")) or col.get("cardinality", 0) <= _CONTROLLED_VALUE_MAX_CARDINALITY:
                continue
            try:
                q = text(f'SELECT "{col["name"]}" FROM "{schema}"."{table_name}" WHERE "{col["name"]}" IS NOT NULL LIMIT :lim')
                with engine.connect() as conn:
                    values = [str(r[0]) for r in conn.execute(q, {"lim": sample_size}).fetchall() if r[0] is not None]
            except Exception:
                continue
            if not values:
                continue
            for pat_name, pat_re in patterns.items():
                matches = sum(1 for v in values if pat_re.match(v))
                ratio = matches / len(values)
                if 0.5 < ratio < 1.0:
                    non_matching = [v for v in values if not pat_re.match(v)][:5]
                    findings.append({
                        "table": table_name, "column": col["name"], "check": "format_inconsistency", "severity": "warning",
                        "detail": f"{matches}/{len(values)} sampled values match {pat_name} format, but {len(values) - matches} do not. Non-matching samples: {non_matching}",
                        "recommendation": f"Add validation to ensure consistent {pat_name} format, or separate non-conforming values",
                        "pattern": pat_name, "match_ratio": round(ratio, 3),
                    })
    return findings


def check_range_violations(engine: Engine, tables: List[Dict], schema: str) -> List[Dict]:
    findings = []
    for tbl in tables:
        table_name = tbl["table"]
        if tbl.get("row_count", 0) == 0:
            continue
        for col in tbl.get("columns", []):
            col_name = col["name"]
            col_type = col.get("type", "")
            data_range = col.get("data_range", {})
            min_val_str = data_range.get("min")
            if min_val_str is None:
                continue
            name_lower = col_name.lower()
            try:
                if any(p in name_lower for p in _PRICING_PATTERNS) and _is_numeric_type(col_type) and float(min_val_str) < 0:
                    with engine.connect() as conn:
                        neg_count = conn.execute(text(f'SELECT COUNT(*) FROM "{schema}"."{table_name}" WHERE "{col_name}" < 0')).scalar() or 0
                    if neg_count > 0:
                        findings.append({
                            "table": table_name, "column": col_name, "check": "range_violation", "severity": "warning",
                            "detail": f"Pricing/amount column has {neg_count} negative value(s) (min: {min_val_str})",
                            "recommendation": "Add CHECK constraint (value >= 0) or verify negatives represent valid adjustments",
                            "violation_type": "negative_pricing", "violation_count": neg_count,
                        })
                if any(p in name_lower for p in _QUANTITY_PATTERNS) and _is_numeric_type(col_type) and float(min_val_str) < 0:
                    with engine.connect() as conn:
                        neg_count = conn.execute(text(f'SELECT COUNT(*) FROM "{schema}"."{table_name}" WHERE "{col_name}" < 0')).scalar() or 0
                    if neg_count > 0:
                        findings.append({
                            "table": table_name, "column": col_name, "check": "range_violation", "severity": "warning",
                            "detail": f"Quantity column has {neg_count} negative value(s) (min: {min_val_str})",
                            "recommendation": "Add CHECK constraint (value >= 0) if negative quantities are not expected",
                            "violation_type": "negative_quantity", "violation_count": neg_count,
                        })
            except (ValueError, TypeError):
                pass
    return findings


_SOFT_DELETE_TIMESTAMP = ("deleted_at", "deleted_date", "removed_at", "archived_at", "archived_date", "deactivated_at", "purged_at")
_SOFT_DELETE_BOOLEAN = ("is_deleted", "deleted", "is_removed", "removed", "is_archived", "archived", "is_deactivated", "deactivated")
_ACTIVE_FLAG = ("is_active", "active", "enabled", "is_enabled")
_AUDIT_TRAIL_SUFFIXES = ("_history", "_audit", "_log", "_archive", "_changelog")


def check_delete_management(engine: Engine, tables: List[Dict], schema: str) -> List[Dict]:
    findings = []
    all_table_names = {t["table"].lower() for t in tables}
    for tbl in tables:
        table_name = tbl["table"]
        row_count = tbl.get("row_count", 0)
        columns = tbl.get("columns", [])

        soft_col = soft_type = None
        for col in columns:
            cn = col["name"].lower()
            ct = col.get("type", "").lower()
            if cn in _SOFT_DELETE_TIMESTAMP:
                soft_col, soft_type = col["name"], "timestamp"
                break
            if cn in _SOFT_DELETE_BOOLEAN:
                soft_col, soft_type = col["name"], "boolean"
                break
            if cn in _ACTIVE_FLAG and "bool" in ct:
                soft_col, soft_type = col["name"], "active_flag"
                break

        cdc_enabled = tbl.get("cdc_enabled", False)
        has_audit = False
        audit_table = None
        for sfx in _AUDIT_TRAIL_SUFFIXES:
            if table_name.lower() + sfx in all_table_names:
                has_audit = True
                audit_table = table_name.lower() + sfx
                break

        value_info = ""
        if soft_col and row_count > 0:
            try:
                q = text(f'SELECT "{soft_col}", COUNT(*) FROM "{schema}"."{table_name}" GROUP BY "{soft_col}" ORDER BY "{soft_col}" NULLS FIRST LIMIT 10')
                with engine.connect() as conn:
                    rows = conn.execute(q).fetchall()
                value_info = f" Current distribution: {', '.join(f'{r[0]}={r[1]}' for r in rows)}."
            except Exception:
                pass

        if soft_col:
            strategy, severity = "soft_delete", "info"
            if soft_type == "active_flag":
                detail = f"Active-flag column '{soft_col}' (boolean) detected — rows with {soft_col}=false are logically deleted.{value_info}"
                recommendation = f'Filter on "{soft_col}" = true for current records during ingestion.'
            elif soft_type == "timestamp":
                detail = f"Soft-delete column '{soft_col}' (timestamp) detected — deleted rows are preserved with a deletion timestamp.{value_info}"
                recommendation = f'Use "{soft_col}" IS NULL for active records. This column can serve as a watermark for incremental delete detection.'
            else:
                detail = f"Soft-delete column '{soft_col}' (boolean) detected — deleted rows are flagged in the source table.{value_info}"
                recommendation = f'Filter on "{soft_col}" = false for active records, or ingest all rows for full history.'
        elif cdc_enabled:
            strategy, severity = "hard_delete_with_cdc", "info"
            detail = "No soft-delete column found, but CDC is enabled. Hard deletes can be captured via change data capture."
            recommendation = "Use CDC (e.g. Debezium, pgoutput) to capture DELETE events."
        else:
            strategy, severity = "hard_delete", "warning"
            detail = "No soft-delete column detected and CDC is not enabled. Table likely uses hard deletes invisible to incremental ingestion."
            recommendation = "Consider: (1) Add soft-delete column, (2) Enable CDC via ALTER TABLE … REPLICA IDENTITY FULL, or (3) Plan periodic full-load syncs."

        if has_audit:
            detail += f" Audit-trail table '{audit_table}' exists."

        finding = {"table": table_name, "column": soft_col, "check": "delete_management", "severity": severity, "detail": detail, "recommendation": recommendation,
                   "delete_strategy": strategy, "soft_delete_column": soft_col, "soft_delete_type": soft_type, "has_audit_trail": has_audit}
        if audit_table:
            finding["audit_trail_table"] = audit_table
        findings.append(finding)
    return findings


_BUSINESS_DATE_PATTERNS = ("order_date", "transaction_date", "payment_date", "event_date", "event_time", "ship_date", "delivery_date", "invoice_date", "booking_date", "sale_date", "purchase_date", "effective_date", "activity_date", "record_date", "entry_date", "posting_date", "trade_date", "settlement_date", "value_date", "hire_date")
_SYSTEM_TS_PATTERNS = ("created_at", "inserted_at", "created_date", "record_created_at", "insert_date", "insert_timestamp", "ingested_at")


def check_late_arriving_data(engine: Engine, tables: List[Dict], schema: str) -> List[Dict]:
    findings = []
    for tbl in tables:
        table_name = tbl["table"]
        row_count = tbl.get("row_count", 0)
        columns = tbl.get("columns", [])
        if row_count == 0:
            continue
        col_names = {c["name"].lower(): c for c in columns}
        biz_col = next((col_names[p] for p in _BUSINESS_DATE_PATTERNS if p in col_names), None)
        if biz_col is None:
            continue
        sys_col = next((col_names[p] for p in _SYSTEM_TS_PATTERNS if p in col_names), None)
        if sys_col is None:
            findings.append({"table": table_name, "column": biz_col["name"], "check": "late_arriving_data", "severity": "info",
                            "detail": f"Table has business-date column '{biz_col['name']}' but no system-insertion timestamp (created_at, etc.). Cannot measure arrival lag.",
                            "recommendation": "Add a created_at / inserted_at column to track when rows actually land.", "business_date_column": biz_col["name"], "system_ts_column": None})
            continue

        biz_name = biz_col["name"]
        sys_name = sys_col["name"]
        biz_type = biz_col.get("type", "").lower()
        biz_expr = f'"{biz_name}"'
        if "date" in biz_type and "timestamp" not in biz_type:
            biz_expr = f'"{biz_name}"::timestamp'

        lag_query = text(f"""
            SELECT COUNT(*) AS total, COUNT(*) FILTER (WHERE lh > 24) AS late_1d, COUNT(*) FILTER (WHERE lh > 168) AS late_7d,
                   ROUND(MIN(lh)::numeric, 2) AS min_h, ROUND(AVG(lh)::numeric, 2) AS avg_h,
                   ROUND(PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY lh)::numeric, 2) AS p95_h, ROUND(MAX(lh)::numeric, 2) AS max_h
            FROM (SELECT EXTRACT(EPOCH FROM ("{sys_name}" - {biz_expr}))/3600.0 AS lh FROM "{schema}"."{table_name}" WHERE "{sys_name}" IS NOT NULL AND "{biz_name}" IS NOT NULL) sub
            WHERE lh >= 0
        """)
        try:
            with engine.connect() as conn:
                row = conn.execute(lag_query).fetchone()
        except Exception as e:
            logger.warning(f"Could not compute arrival lag for {table_name}.{biz_name}: {e}")
            continue
        if not row or row[0] == 0:
            continue

        total, late_1d, late_7d = int(row[0]), int(row[1]), int(row[2])
        min_h = float(row[3] or 0)
        avg_h = float(row[4] or 0)
        p95_h = float(row[5] or 0)
        max_h = float(row[6] or 0)
        max_days = round(max_h / 24, 1)
        lookback_days = max(1, math.ceil(max_h / 24) + 1)
        lag_stats = {"total_rows_compared": total, "min_lag_hours": min_h, "avg_lag_hours": avg_h, "p95_lag_hours": p95_h,
                     "max_lag_hours": max_h, "max_lag_days": max_days, "rows_late_over_1d": late_1d, "rows_late_over_7d": late_7d}

        if max_h <= 1:
            severity, detail = "info", f"Data arrives promptly — max lag between '{biz_name}' and '{sys_name}' is {max_h:.1f}h. Standard watermarking on '{sys_name}' is safe."
            recommendation = f"Use '{sys_name}' as the incremental watermark. No special lookback window needed."
        elif max_h <= 24:
            severity, detail = "info", f"Minor arrival delay — max lag between '{biz_name}' and '{sys_name}' is {max_h:.1f}h (avg {avg_h:.1f}h, P95 {p95_h:.1f}h)."
            recommendation = f"Use '{sys_name}' as the watermark (preferred). If using '{biz_name}', add a 1–2 day lookback buffer."
        elif max_h <= 168:
            severity = "warning"
            detail = f"Late-arriving data detected — max lag between '{biz_name}' and '{sys_name}' is {max_days} day(s). {late_1d} of {total} row(s) arrived >24h late."
            recommendation = f"Do NOT use '{biz_name}' as the incremental watermark. Use '{sys_name}' instead, or add a lookback window of at least {lookback_days} day(s)."
        else:
            severity = "warning"
            detail = f"Significant late-arriving data — max lag {max_days} day(s). {late_7d} of {total} row(s) arrived >7 days late."
            recommendation = f"'{biz_name}' is NOT safe as a watermark. Use '{sys_name}' for incremental loads. If '{biz_name}' must be used, apply a {lookback_days}-day lookback window."

        findings.append({
            "table": table_name, "column": biz_name, "check": "late_arriving_data", "severity": severity,
            "detail": detail, "recommendation": recommendation,
            "business_date_column": biz_name, "system_ts_column": sys_name,
            "lag_stats": lag_stats, "recommended_lookback_days": lookback_days,
        })
    return findings


_TZ_DATETIME_KEYWORDS = ("timestamp", "datetime", "date", "time", "smalldatetime", "datetimeoffset")
_TZ_AWARE_PG = ("timestamptz", "timestamp with time zone", "timetz", "time with time zone")


def _classify_column_tz(col_type_str: str, server_tz: str) -> Optional[str]:
    ct = col_type_str.lower().strip()
    if not any(kw in ct for kw in _TZ_DATETIME_KEYWORDS) or ct == "date":
        return None
    if any(t in ct for t in _TZ_AWARE_PG):
        return "UTC"
    return server_tz


def _fetch_server_timezone(engine: Engine) -> str:
    try:
        with engine.connect() as conn:
            return conn.execute(text("SHOW timezone")).scalar() or "Unknown"
    except Exception:
        return "Unknown"


def check_timezone(engine: Engine, tables: List[Dict], schema: str) -> List[Dict]:
    findings = []
    server_tz = _fetch_server_timezone(engine)
    all_tz_profiles = []

    for tbl in tables:
        table_name = tbl["table"]
        columns = tbl.get("columns", [])
        tz_columns = []
        tz_set = set()
        for col in columns:
            col_type = col.get("type", "")
            eff_tz = col.get("column_timezone") or _classify_column_tz(col_type, server_tz)
            if eff_tz is None:
                continue
            is_aware = any(t in col_type.lower() for t in _TZ_AWARE_PG)
            tz_columns.append({"column": col["name"], "type": col_type, "effective_timezone": eff_tz, "is_tz_aware": is_aware})
            tz_set.add(eff_tz)
        if not tz_columns:
            continue

        aware_count = sum(1 for c in tz_columns if c["is_tz_aware"])
        naive_count = len(tz_columns) - aware_count
        has_mixed = len(tz_set) > 1
        all_tz_profiles.append({"table": table_name, "timezones": sorted(tz_set), "aware_count": aware_count, "naive_count": naive_count})

        if has_mixed:
            severity = "warning"
            detail = f"Mixed timezones within table — date/time columns use multiple effective timezones ({', '.join(sorted(tz_set))}). {aware_count} TZ-aware, {naive_count} TZ-naive."
            recommendation = "Standardize date/time columns to a single timezone (preferably UTC with timestamptz)."
        elif naive_count > 0 and server_tz != "UTC":
            severity = "info"
            detail = f"All {len(tz_columns)} date/time column(s) are TZ-naive — stored values are implicitly in server timezone '{server_tz}'."
            recommendation = f"During ingestion, treat all timestamps as '{server_tz}' and convert to UTC."
        elif aware_count == len(tz_columns):
            severity = "info"
            detail = f"All {len(tz_columns)} date/time column(s) are TZ-aware (timestamptz) — values are stored as UTC internally."
            recommendation = "Timestamps are in UTC. No special timezone handling needed during ingestion."
        else:
            severity = "info"
            tz_val = next(iter(tz_set))
            detail = f"All {len(tz_columns)} date/time column(s) use timezone '{tz_val}'."
            recommendation = f"Treat all timestamps as '{tz_val}' during ingestion."

        findings.append({
            "table": table_name, "column": None, "check": "timezone", "severity": severity,
            "detail": detail, "recommendation": recommendation,
            "server_timezone": server_tz, "columns": tz_columns,
            "distinct_timezones": sorted(tz_set), "tz_aware_count": aware_count, "tz_naive_count": naive_count,
        })

    all_tzs = set()
    for p in all_tz_profiles:
        all_tzs.update(p["timezones"])
    if len(all_tzs) > 1:
        findings.append({
            "table": "(database-wide)", "column": None, "check": "timezone", "severity": "warning",
            "detail": f"Multiple effective timezones detected across the database: {', '.join(sorted(all_tzs))}.",
            "recommendation": "Establish a single source timezone convention. Preferably migrate all columns to timestamptz (UTC).",
            "server_timezone": server_tz, "all_timezones": sorted(all_tzs),
        })
    return findings


# ============================================================================
# Data quality: per-table grouping helper
# ============================================================================


def _build_table_data_quality(findings: List[Dict]) -> Dict[str, Any]:
    """Build a per-table data_quality object from that table's findings.

    Returns a single flat findings list — the canonical representation.
    """
    return {"findings": findings}


# ============================================================================
# Main entry: analyze_source_system
# ============================================================================

def analyze_source_system(
    database_url: str,
    output_path: str,
    schema: Optional[str] = None,
    include_sample_data: bool = False,
) -> Dict[str, Any]:
    """Analyze source database schema and data quality, save combined output to schema.json."""
    schema = schema or "public"
    logger.info(f"Starting source system analysis for schema: {schema}")

    engine = get_engine(database_url)
    dialect = engine.dialect.name

    try:
        schema_meta = fetch_schema_metadata(engine, schema=schema)
        tables = schema_meta["tables"]
        all_columns = schema_meta["columns"]
        all_pks = schema_meta["primary_keys"]
        all_fks = schema_meta["foreign_keys"]

        if not tables:
            logger.warning("No tables found to analyze.")
            return {"error": "No tables found"}

        logger.info(f"Found {len(tables)} tables")

        connection_info = parse_connection_info(engine)
        db_timezone = fetch_database_timezone(engine)
        row_counts = fetch_row_counts(engine, tables, schema=schema)

        enriched_tables = []
        total_rows = 0

        for idx, table_name in enumerate(tables):
            logger.info(f"Analyzing table {idx + 1}/{len(tables)}: {table_name}")
            try:
                table_columns = all_columns.get(table_name, [])
                pk_columns = all_pks.get(table_name, [])
                fk_columns = all_fks.get(table_name, [])
                row_count = row_counts.get(table_name, 0)
                total_rows += row_count
                table_schema = schema or "public"

                sample_data = None
                if include_sample_data:
                    try:
                        colnames, rows = fetch_sample_rows(engine, table_name, limit=10)
                        sample_data = {col: [row[i] for row in rows] for i, col in enumerate(colnames)}
                    except Exception:
                        pass

                field_classifications = {col["name"]: c for col in table_columns if (c := classify_field(col["name"]))}
                sensitive_fields = detect_sensitive_fields(table_columns)
                partition_columns = detect_partition_columns(table_columns, table_name=table_name, schema=table_schema, engine=engine)
                incremental_columns = detect_incremental_columns(table_columns, pk_columns)
                cdc_enabled = detect_cdc_enabled(engine, table_name, schema=table_schema)
                col_statistics = fetch_column_statistics(engine, table_name, table_columns, schema=table_schema, row_count=row_count)
                join_candidates = detect_join_candidates(table_name, table_columns, pk_columns, fk_columns, all_pks)

                enriched_columns = []
                for col in table_columns:
                    col_dict = {"name": col["name"], "type": col["type"], "nullable": col.get("nullable", True), "is_incremental": col.get("is_incremental", False)}
                    col_tz = get_column_timezone(col["type"], dialect, db_timezone)
                    if col_tz is not None:
                        col_dict["column_timezone"] = col_tz
                    stats = col_statistics.get(col["name"], {})
                    if stats:
                        col_dict["cardinality"] = stats.get("cardinality", 0)
                        col_dict["null_count"] = stats.get("null_count", 0)
                        if "data_range" in stats:
                            col_dict["data_range"] = stats["data_range"]
                    data_cat = classify_data_category(col["type"], col["name"], cardinality=stats.get("cardinality", 0), row_count=row_count)
                    if data_cat:
                        col_dict["data_category"] = data_cat
                    enriched_columns.append(col_dict)

                table_entry = {
                    "table": table_name, "schema": table_schema, "columns": enriched_columns,
                    "primary_keys": pk_columns,
                    "foreign_keys": [{"column": fk["column"], "references": fk["references"]} for fk in fk_columns],
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

        # ---- Data quality checks (PostgreSQL only) ----
        data_quality_summary = {}
        database_wide_findings = []
        if dialect == "postgresql":
            logger.info("Running data quality checks…")
            check_constraints = fetch_check_constraints(engine, schema)
            enum_cols = fetch_enum_columns(engine, schema)
            unique_constraints = fetch_unique_constraints(engine, schema)
            all_pks_dict = {t["table"]: t.get("primary_keys", []) for t in enriched_tables}

            all_findings = []
            all_findings.extend(check_controlled_value_candidates(engine, enriched_tables, check_constraints, enum_cols, unique_constraints, schema))
            all_findings.extend(check_nullable_but_never_null(enriched_tables))
            all_findings.extend(check_missing_primary_keys(enriched_tables))
            all_findings.extend(check_missing_foreign_keys(engine, enriched_tables, all_pks_dict, schema))
            all_findings.extend(check_format_inconsistency(engine, enriched_tables, schema))
            all_findings.extend(check_range_violations(engine, enriched_tables, schema))
            all_findings.extend(check_delete_management(engine, enriched_tables, schema))
            all_findings.extend(check_late_arriving_data(engine, enriched_tables, schema))
            all_findings.extend(check_timezone(engine, enriched_tables, schema))

            severity_counts = Counter(f["severity"] for f in all_findings)
            check_counts = Counter(f["check"] for f in all_findings)

            # Group findings by table name and nest into each table entry
            findings_by_table: Dict[str, List[Dict]] = {}
            database_wide_findings = []
            for f in all_findings:
                finding_copy = {k: v for k, v in f.items() if k != "table"}
                table_name = f["table"]
                if table_name == "(database-wide)":
                    database_wide_findings.append(finding_copy)
                else:
                    findings_by_table.setdefault(table_name, []).append(finding_copy)

            for tbl in enriched_tables:
                table_findings = findings_by_table.get(tbl["table"], [])
                tbl["data_quality"] = _build_table_data_quality(table_findings)

            data_quality_summary = {
                "critical": severity_counts.get("critical", 0),
                "warning": severity_counts.get("warning", 0),
                "info": severity_counts.get("info", 0),
                "by_check": {
                    check: check_counts.get(check, 0)
                    for check in (
                        "controlled_value_candidate", "nullable_but_never_null",
                        "missing_primary_key", "missing_foreign_key",
                        "format_inconsistency", "range_violation",
                        "delete_management", "late_arriving_data", "timezone",
                    )
                },
                "constraints_found": {
                    "check_constraints": sum(len(v) for v in check_constraints.values()),
                    "enum_columns": sum(len(v) for v in enum_cols.values()),
                    "unique_constraints": sum(len(v) for v in unique_constraints.values()),
                },
            }
            if database_wide_findings:
                data_quality_summary["database_wide_findings"] = database_wide_findings

            logger.info(f"Data quality: {len(all_findings)} finding(s) (critical: {severity_counts.get('critical', 0)}, warning: {severity_counts.get('warning', 0)}, info: {severity_counts.get('info', 0)})")
        else:
            logger.info(f"Data quality checks skipped (PostgreSQL only; dialect={dialect})")

        # Build final document
        total_findings = sum(len(tbl.get("data_quality", {}).get("findings", [])) for tbl in enriched_tables)
        if database_wide_findings:
            total_findings += len(database_wide_findings)
        schema_document = {
            "metadata": {
                "generated_at": datetime.now(timezone.utc).isoformat(),
                "database_url": database_url.split("@")[-1] if "@" in database_url else database_url,
                "schema_filter": schema,
                "total_tables": len(enriched_tables),
                "total_rows": total_rows,
                "total_findings": total_findings,
            },
            "connection": {**connection_info, "timezone": db_timezone},
            "data_quality_summary": data_quality_summary,
            "tables": enriched_tables,
        }

        logger.info(f"Saving to {output_path}")
        with open(output_path, "w", encoding="utf-8") as f:
            json.dump(schema_document, f, indent=2, default=str)

        logger.info(f"Done — {len(enriched_tables)} tables, {total_findings} data quality findings")
        return schema_document

    except Exception as e:
        logger.error(f"Error analyzing source system: {e}")
        raise


if __name__ == "__main__":
    import sys
    _load_env_file()
    if len(sys.argv) < 3:
        print("Usage: python source_system_analyzer.py <database_url> <output_json_path> [schema]")
        print("  schema can also be set via DATABASE_SCHEMA or SCHEMA in .env")
        sys.exit(1)
    database_url = sys.argv[1]
    output_path = sys.argv[2]
    schema = sys.argv[3] if len(sys.argv) > 3 else os.environ.get("DATABASE_SCHEMA") or os.environ.get("SCHEMA") or None
    analyze_source_system(database_url, output_path, schema=schema)
