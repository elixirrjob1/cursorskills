#!/usr/bin/env python3
"""
Data Quality Analyzer

Assesses data integrity and quality of a source database, producing a JSON
report with actionable findings and recommendations.

Key checks:
- Controlled value list candidates (low-cardinality text without constraints)
- Nullable columns that are never null
- Missing primary keys
- Missing foreign key constraints (implicit FK patterns without enforcement)
- Orphaned references (FK-like values with no matching target row)
- Format inconsistencies (mixed patterns in text columns)
- Range/domain violations (negative prices, negative quantities)
- Delete management (soft-delete vs hard-delete strategy per table)
- Late-arriving data (lag between business dates and insertion timestamps)
- Timezone assessment (TZ-aware vs TZ-naive columns, server timezone, mixed-TZ warnings)

Usage:
    python data_quality.py <database_url> <output_json_path> [schema] [schema_json_path]
"""

import os
import json
import logging
import re
from pathlib import Path
from typing import Dict, List, Optional, Any, Set
from datetime import datetime, timezone
from collections import Counter

from sqlalchemy import create_engine, inspect, text
from sqlalchemy.engine import Engine


def _load_env_file() -> None:
    """Load .env from current working directory or project root."""
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


# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")


# ---------------------------------------------------------------------------
# Database helpers
# ---------------------------------------------------------------------------

def get_engine(database_url: str) -> Engine:
    """Create a SQLAlchemy engine with sensible defaults."""
    return create_engine(
        database_url,
        pool_size=5,
        max_overflow=10,
        pool_pre_ping=True,
        connect_args={"connect_timeout": 10},
        echo=False,
    )


# ---------------------------------------------------------------------------
# Constraint introspection (PostgreSQL)
# ---------------------------------------------------------------------------

def fetch_check_constraints(engine: Engine, schema: str) -> Dict[str, List[Dict]]:
    """Fetch CHECK constraints grouped by table.

    Returns: {table_name: [{column, constraint_name, check_clause}]}
    """
    result: Dict[str, List[Dict]] = {}
    query = text("""
        SELECT
            tc.table_name,
            ccu.column_name,
            tc.constraint_name,
            cc.check_clause
        FROM information_schema.table_constraints tc
        JOIN information_schema.check_constraints cc
            ON  tc.constraint_name   = cc.constraint_name
            AND tc.constraint_schema = cc.constraint_schema
        JOIN information_schema.constraint_column_usage ccu
            ON  tc.constraint_name   = ccu.constraint_name
            AND tc.constraint_schema = ccu.constraint_schema
        WHERE tc.constraint_type = 'CHECK'
          AND tc.table_schema    = :schema
          AND tc.constraint_name NOT LIKE '%_not_null'
        ORDER BY tc.table_name, ccu.column_name
    """)
    try:
        with engine.connect() as conn:
            for row in conn.execute(query, {"schema": schema}).fetchall():
                table = row[0]
                result.setdefault(table, []).append({
                    "column": row[1],
                    "constraint_name": row[2],
                    "check_clause": row[3],
                })
    except Exception as e:
        logger.warning(f"Could not fetch CHECK constraints: {e}")
    return result


def fetch_enum_columns(engine: Engine, schema: str) -> Dict[str, Dict[str, List[str]]]:
    """Fetch columns using ENUM types and their allowed values.

    Returns: {table_name: {column_name: [enum_values]}}
    """
    result: Dict[str, Dict[str, List[str]]] = {}
    query = text("""
        SELECT
            c.table_name,
            c.column_name,
            c.udt_name,
            array_agg(e.enumlabel ORDER BY e.enumsortorder) AS enum_values
        FROM information_schema.columns c
        JOIN pg_type t ON t.typname = c.udt_name
        JOIN pg_enum e ON e.enumtypid = t.oid
        WHERE c.table_schema = :schema
          AND c.data_type    = 'USER-DEFINED'
        GROUP BY c.table_name, c.column_name, c.udt_name
        ORDER BY c.table_name, c.column_name
    """)
    try:
        with engine.connect() as conn:
            for row in conn.execute(query, {"schema": schema}).fetchall():
                table = row[0]
                result.setdefault(table, {})[row[1]] = list(row[3])
    except Exception as e:
        logger.warning(f"Could not fetch ENUM columns: {e}")
    return result


def fetch_unique_constraints(engine: Engine, schema: str) -> Dict[str, Set[str]]:
    """Fetch columns that have UNIQUE constraints.

    Returns: {table_name: set(column_names)}
    """
    result: Dict[str, Set[str]] = {}
    query = text("""
        SELECT
            tc.table_name,
            kcu.column_name
        FROM information_schema.table_constraints tc
        JOIN information_schema.key_column_usage kcu
            ON  tc.constraint_name = kcu.constraint_name
            AND tc.table_schema    = kcu.table_schema
        WHERE tc.constraint_type = 'UNIQUE'
          AND tc.table_schema    = :schema
        ORDER BY tc.table_name
    """)
    try:
        with engine.connect() as conn:
            for row in conn.execute(query, {"schema": schema}).fetchall():
                result.setdefault(row[0], set()).add(row[1])
    except Exception as e:
        logger.warning(f"Could not fetch UNIQUE constraints: {e}")
    return result


# ---------------------------------------------------------------------------
# Pattern constants
# ---------------------------------------------------------------------------

# Text column types
_TEXT_TYPES = ("text", "varchar", "char", "citext", "name",
               "character varying", "character")

# Columns whose *name* indicates free-form content (never a controlled list)
_FREEFORM_EXACT: Set[str] = {
    # Generic descriptive text
    "name", "description", "desc", "comment", "note", "notes",
    "title", "body", "content", "message", "summary", "detail",
    "details", "remarks", "text", "label",
    # Person names
    "first_name", "last_name", "full_name", "display_name",
    "contact_name", "middle_name", "maiden_name", "nickname",
    "username", "login_name", "user_name",
    # Contact info
    "email", "phone", "mobile", "fax",
    # Street-level address parts
    "address", "street", "address_line_1", "address_line_2",
    # URLs / paths
    "url", "uri", "path", "filename", "filepath", "href", "link",
    # Secrets
    "password", "token", "secret", "api_key", "hash", "salt",
    # Unique identifiers
    "sku", "barcode", "code", "serial_number", "uuid", "guid",
}

_FREEFORM_SUFFIXES = (
    "_name",         # product_name, company_name …
    "_description", "_desc",
    "_comment", "_note", "_notes",
    "_email", "_phone", "_mobile", "_fax",
    "_address", "_street",
    "_url", "_uri", "_path",
    "_password", "_token", "_secret", "_hash",
)

# Max distinct values to consider as a controlled-value-list candidate
_CONTROLLED_VALUE_MAX_CARDINALITY = 20

# Pricing / quantity column-name patterns (for range checks)
_PRICING_PATTERNS = ("price", "cost", "amount", "total", "subtotal",
                     "fee", "charge", "rate")
_QUANTITY_PATTERNS = ("quantity", "qty", "count", "quantity_on_hand")

# FK-candidate suffixes and exclusion list
_JOIN_SUFFIXES = ("_id", "_key", "_code", "_ref", "_fk")
_JOIN_EXCLUDE = {
    "postal_code", "zip_code", "area_code", "country_code", "currency_code",
    "language_code", "phone_code", "iban_code", "swift_code",
    "barcode", "qr_code", "hash_code", "auth_code", "verification_code",
    "access_code", "promo_code", "discount_code", "coupon_code",
    "error_code", "status_code", "exit_code", "response_code",
}

# Regex patterns for format-consistency check
_EMAIL_RE = re.compile(r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$')
_PHONE_RE = re.compile(r'^[+]?[\d\s\-().]{7,20}$')
_DATE_TEXT_RE = re.compile(r'^\d{4}[-/]\d{2}[-/]\d{2}')
_URL_RE = re.compile(r'^https?://')
_NUMERIC_TEXT_RE = re.compile(r'^-?\d+\.?\d*$')


# ---------------------------------------------------------------------------
# Helper predicates
# ---------------------------------------------------------------------------

def _is_text_type(col_type: str) -> bool:
    return any(t in col_type.lower() for t in _TEXT_TYPES)


def _is_numeric_type(col_type: str) -> bool:
    return any(t in col_type.lower()
               for t in ("int", "numeric", "decimal", "float",
                          "double", "real", "money", "serial"))


def _is_freeform_column(col_name: str) -> bool:
    """Return True if the column name suggests free-form content."""
    lower = col_name.lower()
    if lower in _FREEFORM_EXACT:
        return True
    return any(lower.endswith(s) for s in _FREEFORM_SUFFIXES)


# ---------------------------------------------------------------------------
# Quality checks
# ---------------------------------------------------------------------------

def check_controlled_value_candidates(
    engine: Engine,
    tables: List[Dict],
    check_constraints: Dict[str, List[Dict]],
    enum_columns: Dict[str, Dict[str, List[str]]],
    unique_constraints: Dict[str, Set[str]],
    schema: str = "public",
) -> List[Dict]:
    """Find text columns that should use controlled value lists.

    Flags text columns with low cardinality that lack CHECK, ENUM, FK,
    or UNIQUE constraints.
    """
    findings: List[Dict] = []

    for tbl in tables:
        table_name = tbl["table"]
        row_count = tbl.get("row_count", 0)
        if row_count == 0:
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

            if not _is_text_type(col_type):
                continue
            if cardinality == 0 or cardinality > _CONTROLLED_VALUE_MAX_CARDINALITY:
                continue
            # Skip columns already constrained
            if col_name in pk_set | fk_set | check_set | enum_set | unique_set:
                continue
            # Skip known free-form columns
            if _is_freeform_column(col_name):
                continue

            # Fetch actual distinct values
            distinct_values: List[str] = []
            try:
                q = text(
                    f'SELECT DISTINCT "{col_name}" '
                    f'FROM "{schema}"."{table_name}" '
                    f'WHERE "{col_name}" IS NOT NULL '
                    f'ORDER BY "{col_name}" LIMIT 25'
                )
                with engine.connect() as conn:
                    distinct_values = [str(r[0]) for r in conn.execute(q).fetchall()]
            except Exception:
                pass

            values_display = ", ".join(repr(v) for v in distinct_values[:10])
            findings.append({
                "table": table_name,
                "column": col_name,
                "check": "controlled_value_candidate",
                "severity": "warning",
                "detail": (
                    f"Text column with {cardinality} distinct value(s) "
                    f"({values_display}) but no CHECK, ENUM, or FK constraint"
                ),
                "recommendation": (
                    "Add a CHECK constraint, convert to an ENUM type, "
                    "or create a lookup/reference table to prevent invalid values"
                ),
                "distinct_values": distinct_values,
                "cardinality": cardinality,
            })

    return findings


def check_nullable_but_never_null(tables: List[Dict]) -> List[Dict]:
    """Find columns that are nullable but contain zero NULLs."""
    findings: List[Dict] = []
    for tbl in tables:
        row_count = tbl.get("row_count", 0)
        if row_count == 0:
            continue
        for col in tbl.get("columns", []):
            if col.get("nullable") and col.get("null_count", 0) == 0:
                findings.append({
                    "table": tbl["table"],
                    "column": col["name"],
                    "check": "nullable_but_never_null",
                    "severity": "info",
                    "detail": (
                        f"Column is nullable but has 0 NULLs "
                        f"across {row_count} row(s)"
                    ),
                    "recommendation": (
                        "Consider adding a NOT NULL constraint if "
                        "the column should always have a value"
                    ),
                })
    return findings


def check_missing_primary_keys(tables: List[Dict]) -> List[Dict]:
    """Find tables without a primary key."""
    findings: List[Dict] = []
    for tbl in tables:
        if not tbl.get("has_primary_key", True):
            findings.append({
                "table": tbl["table"],
                "column": None,
                "check": "missing_primary_key",
                "severity": "critical",
                "detail": "Table has no primary key defined",
                "recommendation": (
                    "Add a primary key to ensure row uniqueness "
                    "and enable efficient lookups"
                ),
            })
    return findings


def check_missing_foreign_keys(
    engine: Engine,
    tables: List[Dict],
    all_pks: Dict[str, List[str]],
    schema: str = "public",
) -> List[Dict]:
    """Find FK-patterned columns without FK constraints; check for orphans."""
    findings: List[Dict] = []

    for tbl in tables:
        table_name = tbl["table"]
        row_count = tbl.get("row_count", 0)
        pk_set = set(tbl.get("primary_keys", []))
        fk_set = {fk["column"] for fk in tbl.get("foreign_keys", [])}

        for col in tbl.get("columns", []):
            col_name = col["name"]
            name_lower = col_name.lower()

            if col_name in pk_set | fk_set:
                continue
            if name_lower in _JOIN_EXCLUDE:
                continue

            # Suffix match
            matched_suffix = None
            for suffix in _JOIN_SUFFIXES:
                if name_lower.endswith(suffix):
                    matched_suffix = suffix
                    break
            if not matched_suffix:
                continue

            prefix = name_lower[: -len(matched_suffix)]
            if not prefix:
                continue

            # Find matching target table
            target_table = target_column = None
            for other_table, other_pks in all_pks.items():
                if other_table == table_name:
                    continue
                ol = other_table.lower()
                if ol in (prefix, prefix + "s", prefix + "es") or \
                   ol.rstrip("s") == prefix or ol.rstrip("es") == prefix:
                    target_table = other_table
                    suffix_base = matched_suffix.lstrip("_")
                    for pk in other_pks:
                        if pk.lower() in (suffix_base, name_lower):
                            target_column = pk
                            break
                    if target_column is None and other_pks:
                        target_column = other_pks[0]
                    break

            if not target_table:
                continue

            # Check for orphaned references
            orphan_sample: List[str] = []
            if row_count > 0 and target_column:
                try:
                    q = text(
                        f'SELECT DISTINCT s."{col_name}" '
                        f'FROM "{schema}"."{table_name}" s '
                        f'LEFT JOIN "{schema}"."{target_table}" t '
                        f'  ON s."{col_name}" = t."{target_column}" '
                        f'WHERE s."{col_name}" IS NOT NULL '
                        f'  AND t."{target_column}" IS NULL '
                        f'LIMIT 10'
                    )
                    with engine.connect() as conn:
                        orphan_sample = [str(r[0]) for r in conn.execute(q).fetchall()]
                except Exception:
                    pass

            detail = (
                f"Column follows FK naming pattern and matches "
                f"{target_table}.{target_column} but has no FK constraint"
            )
            severity = "warning"

            if orphan_sample:
                detail += (
                    f". Found {len(orphan_sample)} orphaned value(s): "
                    f"{', '.join(orphan_sample)}"
                )
                severity = "critical"

            finding: Dict[str, Any] = {
                "table": table_name,
                "column": col_name,
                "check": "missing_foreign_key",
                "severity": severity,
                "detail": detail,
                "recommendation": (
                    f"Add FOREIGN KEY constraint referencing "
                    f"{target_table}({target_column}) to enforce "
                    f"referential integrity"
                ),
                "target_table": target_table,
                "target_column": target_column,
            }
            if orphan_sample:
                finding["orphaned_values"] = orphan_sample

            findings.append(finding)

    return findings


def check_format_inconsistency(
    engine: Engine,
    tables: List[Dict],
    schema: str = "public",
    sample_size: int = 200,
) -> List[Dict]:
    """Detect text columns with inconsistent format patterns.

    Samples values and checks whether a dominant pattern (email, phone, date,
    URL, numeric) is present but not consistently followed.
    """
    findings: List[Dict] = []
    patterns = {
        "email": _EMAIL_RE,
        "phone": _PHONE_RE,
        "date_as_text": _DATE_TEXT_RE,
        "url": _URL_RE,
        "numeric_as_text": _NUMERIC_TEXT_RE,
    }

    for tbl in tables:
        table_name = tbl["table"]
        row_count = tbl.get("row_count", 0)
        if row_count == 0:
            continue

        for col in tbl.get("columns", []):
            col_type = col.get("type", "")
            cardinality = col.get("cardinality", 0)
            if not _is_text_type(col_type):
                continue
            # Only check columns with enough diversity to matter
            if cardinality <= _CONTROLLED_VALUE_MAX_CARDINALITY:
                continue

            try:
                q = text(
                    f'SELECT "{col["name"]}" '
                    f'FROM "{schema}"."{table_name}" '
                    f'WHERE "{col["name"]}" IS NOT NULL '
                    f'LIMIT :lim'
                )
                with engine.connect() as conn:
                    values = [
                        str(r[0]) for r in
                        conn.execute(q, {"lim": sample_size}).fetchall()
                        if r[0] is not None
                    ]
            except Exception:
                continue

            if not values:
                continue

            for pat_name, pat_re in patterns.items():
                matches = sum(1 for v in values if pat_re.match(v))
                ratio = matches / len(values)
                # Dominant pattern exists but some values break it
                if 0.5 < ratio < 1.0:
                    non_matching = [v for v in values if not pat_re.match(v)][:5]
                    findings.append({
                        "table": table_name,
                        "column": col["name"],
                        "check": "format_inconsistency",
                        "severity": "warning",
                        "detail": (
                            f"{matches}/{len(values)} sampled values match "
                            f"{pat_name} format, but {len(values) - matches} "
                            f"do not. Non-matching samples: {non_matching}"
                        ),
                        "recommendation": (
                            f"Add validation to ensure consistent {pat_name} "
                            f"format, or separate non-conforming values"
                        ),
                        "pattern": pat_name,
                        "match_ratio": round(ratio, 3),
                    })

    return findings


def check_range_violations(
    engine: Engine,
    tables: List[Dict],
    schema: str = "public",
) -> List[Dict]:
    """Detect values outside expected domains.

    - Negative prices / amounts
    - Negative quantities
    """
    findings: List[Dict] = []

    for tbl in tables:
        table_name = tbl["table"]
        row_count = tbl.get("row_count", 0)
        if row_count == 0:
            continue

        for col in tbl.get("columns", []):
            col_name = col["name"]
            col_type = col.get("type", "")
            name_lower = col_name.lower()
            data_range = col.get("data_range", {})
            if not data_range:
                continue

            min_val_str = data_range.get("min")
            if min_val_str is None:
                continue

            # Negative pricing
            is_pricing = any(p in name_lower for p in _PRICING_PATTERNS)
            if is_pricing and _is_numeric_type(col_type):
                try:
                    if float(min_val_str) < 0:
                        q = text(
                            f'SELECT COUNT(*) FROM "{schema}"."{table_name}" '
                            f'WHERE "{col_name}" < 0'
                        )
                        with engine.connect() as conn:
                            neg_count = conn.execute(q).scalar() or 0
                        if neg_count > 0:
                            findings.append({
                                "table": table_name,
                                "column": col_name,
                                "check": "range_violation",
                                "severity": "warning",
                                "detail": (
                                    f"Pricing/amount column has {neg_count} "
                                    f"negative value(s) (min: {min_val_str})"
                                ),
                                "recommendation": (
                                    "Add CHECK constraint (value >= 0) or "
                                    "verify negatives represent valid "
                                    "adjustments (refunds, credits)"
                                ),
                                "violation_type": "negative_pricing",
                                "violation_count": neg_count,
                            })
                except (ValueError, TypeError):
                    pass

            # Negative quantities
            is_quantity = any(p in name_lower for p in _QUANTITY_PATTERNS)
            if is_quantity and _is_numeric_type(col_type):
                try:
                    if float(min_val_str) < 0:
                        q = text(
                            f'SELECT COUNT(*) FROM "{schema}"."{table_name}" '
                            f'WHERE "{col_name}" < 0'
                        )
                        with engine.connect() as conn:
                            neg_count = conn.execute(q).scalar() or 0
                        if neg_count > 0:
                            findings.append({
                                "table": table_name,
                                "column": col_name,
                                "check": "range_violation",
                                "severity": "warning",
                                "detail": (
                                    f"Quantity column has {neg_count} "
                                    f"negative value(s) (min: {min_val_str})"
                                ),
                                "recommendation": (
                                    "Add CHECK constraint (value >= 0) if "
                                    "negative quantities are not expected"
                                ),
                                "violation_type": "negative_quantity",
                                "violation_count": neg_count,
                            })
                except (ValueError, TypeError):
                    pass

    return findings


# ---------------------------------------------------------------------------
# Delete management assessment
# ---------------------------------------------------------------------------

# Soft-delete column name patterns
_SOFT_DELETE_TIMESTAMP = (
    "deleted_at", "deleted_date", "removed_at", "removed_date",
    "archived_at", "archived_date", "deactivated_at", "purged_at",
)
_SOFT_DELETE_BOOLEAN = (
    "is_deleted", "deleted", "is_removed", "removed",
    "is_archived", "archived", "is_deactivated", "deactivated",
)
_ACTIVE_FLAG = (
    "is_active", "active", "enabled", "is_enabled",
)
_AUDIT_TRAIL_SUFFIXES = (
    "_history", "_audit", "_log", "_archive", "_changelog",
)


def _check_cdc(engine: Engine, table_name: str, schema: str) -> bool:
    """Check if a table has CDC-friendly REPLICA IDENTITY (PostgreSQL)."""
    try:
        with engine.connect() as conn:
            row = conn.execute(text(
                "SELECT c.relreplident FROM pg_class c "
                "JOIN pg_namespace n ON n.oid = c.relnamespace "
                "WHERE n.nspname = :schema AND c.relname = :table"
            ), {"schema": schema, "table": table_name}).fetchone()
            if row:
                return row[0] in ('f', 'i')
    except Exception:
        pass
    return False


def check_delete_management(
    engine: Engine,
    tables: List[Dict],
    schema: str = "public",
) -> List[Dict]:
    """Assess how each table handles deletions.

    Detects per table:
    - Soft-delete columns  (deleted_at, is_deleted, active, …)
    - CDC configuration    (REPLICA IDENTITY for PostgreSQL)
    - Audit-trail tables   (*_history, *_audit, *_log)

    Classifies each table's delete strategy as:
    - soft_delete:           Has a soft-delete or active-flag column
    - hard_delete_with_cdc:  No soft-delete but CDC is enabled
    - hard_delete:           No soft-delete, no CDC (requires full loads)
    """
    findings: List[Dict] = []
    all_table_names = {t["table"].lower() for t in tables}

    for tbl in tables:
        table_name = tbl["table"]
        row_count = tbl.get("row_count", 0)
        columns = tbl.get("columns", [])

        # --- detect soft-delete column ---
        soft_col: Optional[str] = None
        soft_type: Optional[str] = None

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

        # --- CDC status ---
        cdc_enabled = tbl.get("cdc_enabled", None)
        if cdc_enabled is None:
            cdc_enabled = _check_cdc(engine, table_name, schema)

        # --- audit trail table ---
        has_audit = False
        audit_table: Optional[str] = None
        for sfx in _AUDIT_TRAIL_SUFFIXES:
            candidate = table_name.lower() + sfx
            if candidate in all_table_names:
                has_audit = True
                audit_table = candidate
                break

        # --- value distribution for soft-delete column ---
        value_info = ""
        if soft_col and row_count > 0:
            try:
                q = text(
                    f'SELECT "{soft_col}", COUNT(*) '
                    f'FROM "{schema}"."{table_name}" '
                    f'GROUP BY "{soft_col}" '
                    f'ORDER BY "{soft_col}" NULLS FIRST '
                    f'LIMIT 10'
                )
                with engine.connect() as conn:
                    rows = conn.execute(q).fetchall()
                dist = ", ".join(
                    f"{r[0]}={r[1]}" for r in rows
                )
                value_info = f" Current distribution: {dist}."
            except Exception:
                pass

        # --- classify strategy ---
        if soft_col:
            strategy = "soft_delete"
            severity = "info"

            if soft_type == "active_flag":
                detail = (
                    f"Active-flag column '{soft_col}' (boolean) detected — "
                    f"rows with {soft_col}=false are logically deleted.{value_info}"
                )
                recommendation = (
                    f"Filter on \"{soft_col}\" = true for current records "
                    f"during ingestion. Ingest all rows if you need "
                    f"deletion history downstream."
                )
            elif soft_type == "timestamp":
                detail = (
                    f"Soft-delete column '{soft_col}' (timestamp) detected — "
                    f"deleted rows are preserved with a deletion timestamp.{value_info}"
                )
                recommendation = (
                    f"Use \"{soft_col}\" IS NULL for active records. "
                    f"This column can also serve as a watermark for "
                    f"incremental delete detection."
                )
            else:  # boolean
                detail = (
                    f"Soft-delete column '{soft_col}' (boolean) detected — "
                    f"deleted rows are flagged in the source table.{value_info}"
                )
                recommendation = (
                    f"Filter on \"{soft_col}\" = false for active records, "
                    f"or ingest all rows for full history."
                )

        elif cdc_enabled:
            strategy = "hard_delete_with_cdc"
            severity = "info"
            detail = (
                "No soft-delete column found, but CDC is enabled "
                "(REPLICA IDENTITY FULL or INDEX). Hard deletes can "
                "be captured via change data capture."
            )
            recommendation = (
                "Use CDC (e.g. Debezium, pgoutput) to capture DELETE "
                "events. This avoids periodic full loads to detect "
                "removed rows."
            )

        else:
            strategy = "hard_delete"
            severity = "warning"
            detail = (
                "No soft-delete column detected and CDC is not enabled. "
                "Table likely uses hard deletes that are invisible to "
                "incremental ingestion."
            )
            recommendation = (
                "Consider one of: (1) Add a soft-delete column "
                "(e.g. deleted_at TIMESTAMPTZ), (2) Enable CDC via "
                "ALTER TABLE … REPLICA IDENTITY FULL, or (3) Plan "
                "periodic full-load syncs to detect deletions."
            )

        if has_audit:
            detail += f" Audit-trail table '{audit_table}' exists."

        finding: Dict[str, Any] = {
            "table": table_name,
            "column": soft_col,
            "check": "delete_management",
            "severity": severity,
            "detail": detail,
            "recommendation": recommendation,
            "delete_strategy": strategy,
            "soft_delete_column": soft_col,
            "soft_delete_type": soft_type,
            "cdc_enabled": cdc_enabled,
            "has_audit_trail": has_audit,
        }
        if audit_table:
            finding["audit_trail_table"] = audit_table

        findings.append(finding)

    return findings


# ---------------------------------------------------------------------------
# Late-arriving data assessment
# ---------------------------------------------------------------------------

# Business-event date columns (when did the real-world event happen?)
_BUSINESS_DATE_PATTERNS = (
    "order_date", "transaction_date", "payment_date", "event_date",
    "event_time", "ship_date", "delivery_date", "invoice_date",
    "booking_date", "sale_date", "purchase_date", "effective_date",
    "activity_date", "record_date", "entry_date", "posting_date",
    "trade_date", "settlement_date", "value_date", "hire_date",
)

# System insertion timestamps (when did the row actually land?)
_SYSTEM_TS_PATTERNS = (
    "created_at", "inserted_at", "created_date", "record_created_at",
    "insert_date", "insert_timestamp", "ingested_at",
)

# Future-oriented columns — skip (not late-arriving candidates)
_FUTURE_DATE_PATTERNS = (
    "expected_date", "due_date", "expiry_date", "expiration_date",
    "target_date", "scheduled_date", "planned_date", "estimated_date",
)

import math  # noqa: E402 (for ceil)


def check_late_arriving_data(
    engine: Engine,
    tables: List[Dict],
    schema: str = "public",
) -> List[Dict]:
    """Assess per table how far back data can land after the business event.

    For each table that has both a business-date column (order_date,
    transaction_date, …) and a system-insertion column (created_at, …),
    computes the lag between the two.  This tells you:
      - How far back late-arriving data can appear
      - What lookback window incremental loads need
      - Whether business-date watermarks are safe to use

    Tables without a detectable pair get a brief note instead.
    """
    findings: List[Dict] = []

    for tbl in tables:
        table_name = tbl["table"]
        row_count = tbl.get("row_count", 0)
        columns = tbl.get("columns", [])

        if row_count == 0:
            continue

        col_names = {c["name"].lower(): c for c in columns}

        # --- find business-date column ---
        biz_col: Optional[Dict] = None
        for pat in _BUSINESS_DATE_PATTERNS:
            if pat in col_names:
                biz_col = col_names[pat]
                break

        # --- find system-insertion column ---
        sys_col: Optional[Dict] = None
        for pat in _SYSTEM_TS_PATTERNS:
            if pat in col_names:
                sys_col = col_names[pat]
                break

        # Skip tables with no business-date column — nothing to compare
        if biz_col is None:
            continue

        if sys_col is None:
            # Has business date but no system timestamp
            findings.append({
                "table": table_name,
                "column": biz_col["name"],
                "check": "late_arriving_data",
                "severity": "info",
                "detail": (
                    f"Table has business-date column '{biz_col['name']}' "
                    f"but no system-insertion timestamp (created_at, etc.). "
                    f"Cannot measure arrival lag."
                ),
                "recommendation": (
                    "Add a created_at / inserted_at column to track when "
                    "rows actually land. Without it, late-arriving data "
                    "cannot be detected or measured."
                ),
                "business_date_column": biz_col["name"],
                "system_ts_column": None,
            })
            continue

        # --- compute lag stats ---
        biz_name = biz_col["name"]
        sys_name = sys_col["name"]
        biz_type = biz_col.get("type", "").lower()

        # Cast date → timestamp if needed so subtraction works
        biz_expr = f'"{biz_name}"'
        if "date" in biz_type and "timestamp" not in biz_type:
            biz_expr = f'"{biz_name}"::timestamp'

        lag_query = text(f"""
            SELECT
                COUNT(*)                                        AS total,
                COUNT(*) FILTER (WHERE lh > 24)                 AS late_1d,
                COUNT(*) FILTER (WHERE lh > 168)                AS late_7d,
                ROUND(MIN(lh)::numeric, 2)                      AS min_h,
                ROUND(AVG(lh)::numeric, 2)                      AS avg_h,
                ROUND(
                    PERCENTILE_CONT(0.95)
                        WITHIN GROUP (ORDER BY lh)::numeric, 2) AS p95_h,
                ROUND(MAX(lh)::numeric, 2)                      AS max_h
            FROM (
                SELECT EXTRACT(EPOCH FROM ("{sys_name}" - {biz_expr}))
                       / 3600.0 AS lh
                FROM  "{schema}"."{table_name}"
                WHERE "{sys_name}" IS NOT NULL
                  AND "{biz_name}" IS NOT NULL
            ) sub
            WHERE lh >= 0
        """)

        try:
            with engine.connect() as conn:
                row = conn.execute(lag_query).fetchone()
        except Exception as e:
            logger.warning(
                f"Could not compute arrival lag for "
                f"{table_name}.{biz_name}: {e}"
            )
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

        lag_stats = {
            "total_rows_compared": total,
            "min_lag_hours": min_h,
            "avg_lag_hours": avg_h,
            "p95_lag_hours": p95_h,
            "max_lag_hours": max_h,
            "max_lag_days": max_days,
            "rows_late_over_1d": late_1d,
            "rows_late_over_7d": late_7d,
        }

        # --- severity ---
        if max_h <= 1:
            severity = "info"
            detail = (
                f"Data arrives promptly — max lag between "
                f"'{biz_name}' and '{sys_name}' is "
                f"{max_h:.1f}h (avg {avg_h:.1f}h). "
                f"Standard watermarking on '{sys_name}' is safe."
            )
            recommendation = (
                f"Use '{sys_name}' as the incremental watermark. "
                f"No special lookback window needed."
            )
        elif max_h <= 24:
            severity = "info"
            detail = (
                f"Minor arrival delay — max lag between "
                f"'{biz_name}' and '{sys_name}' is "
                f"{max_h:.1f}h (avg {avg_h:.1f}h, P95 {p95_h:.1f}h)."
            )
            recommendation = (
                f"Use '{sys_name}' as the watermark (preferred). "
                f"If using '{biz_name}', add a 1–2 day lookback buffer."
            )
        elif max_h <= 168:  # 7 days
            severity = "warning"
            detail = (
                f"Late-arriving data detected — max lag between "
                f"'{biz_name}' and '{sys_name}' is "
                f"{max_days} day(s) (avg {avg_h:.1f}h, P95 {p95_h:.1f}h). "
                f"{late_1d} of {total} row(s) arrived >24h late."
            )
            recommendation = (
                f"Do NOT use '{biz_name}' as the incremental watermark. "
                f"Use '{sys_name}' instead, or add a lookback window of "
                f"at least {lookback_days} day(s) to catch late arrivals."
            )
        else:
            severity = "warning"
            detail = (
                f"Significant late-arriving data — max lag between "
                f"'{biz_name}' and '{sys_name}' is "
                f"{max_days} day(s) (avg {avg_h:.1f}h, P95 {p95_h:.1f}h). "
                f"{late_7d} of {total} row(s) arrived >7 days late."
            )
            recommendation = (
                f"'{biz_name}' is NOT safe as a watermark. "
                f"Use '{sys_name}' for incremental loads. "
                f"If '{biz_name}' must be used, apply a {lookback_days}-day "
                f"lookback window, or implement a reconciliation process."
            )

        findings.append({
            "table": table_name,
            "column": biz_name,
            "check": "late_arriving_data",
            "severity": severity,
            "detail": detail,
            "recommendation": recommendation,
            "business_date_column": biz_name,
            "system_ts_column": sys_name,
            "lag_stats": lag_stats,
            "recommended_lookback_days": lookback_days,
        })

    return findings


# ---------------------------------------------------------------------------
# Timezone assessment
# ---------------------------------------------------------------------------

# Date/time type keywords
_TZ_DATETIME_KEYWORDS = (
    "timestamp", "datetime", "date", "time",
    "smalldatetime", "datetimeoffset",
)

# TZ-aware types by PostgreSQL
_TZ_AWARE_PG = ("timestamptz", "timestamp with time zone",
                "timetz", "time with time zone")


def _classify_column_tz(col_type_str: str, server_tz: str) -> Optional[str]:
    """Determine the effective timezone of a date/time column.

    Returns None for non-date/time columns or pure DATE columns.
    """
    ct = col_type_str.lower().strip()

    if not any(kw in ct for kw in _TZ_DATETIME_KEYWORDS):
        return None
    if ct == "date":
        return None

    if any(t in ct for t in _TZ_AWARE_PG):
        return "UTC"

    return server_tz


def _fetch_server_timezone(engine: Engine) -> str:
    """Get the server timezone from the database."""
    try:
        with engine.connect() as conn:
            result = conn.execute(text("SHOW timezone"))
            return result.scalar() or "Unknown"
    except Exception:
        return "Unknown"


def check_timezone(
    engine: Engine,
    tables: List[Dict],
    schema: str = "public",
) -> List[Dict]:
    """Assess timezone handling across all date/time columns.

    For each table, reports:
    - Server timezone
    - Per-column effective timezone (UTC, server TZ, unknown)
    - TZ-aware vs TZ-naive column usage
    - Mixed-timezone warnings within a table
    - Recommendations for ingestion timezone handling

    Also produces a database-wide summary finding when mixed timezones
    are detected across tables.
    """
    findings: List[Dict] = []
    server_tz = _fetch_server_timezone(engine)

    # Collect per-table timezone profiles for cross-table analysis
    all_tz_profiles: List[Dict] = []

    for tbl in tables:
        table_name = tbl["table"]
        columns = tbl.get("columns", [])

        tz_columns: List[Dict] = []
        tz_set: set = set()

        for col in columns:
            col_type = col.get("type", "")
            # Use column_timezone from schema.json if available,
            # otherwise classify from the type string
            eff_tz = col.get("column_timezone")
            if eff_tz is None:
                eff_tz = _classify_column_tz(col_type, server_tz)
            if eff_tz is None:
                continue  # Not a date/time column

            is_aware = any(t in col_type.lower() for t in _TZ_AWARE_PG)
            tz_columns.append({
                "column": col["name"],
                "type": col_type,
                "effective_timezone": eff_tz,
                "is_tz_aware": is_aware,
            })
            tz_set.add(eff_tz)

        if not tz_columns:
            continue

        aware_count = sum(1 for c in tz_columns if c["is_tz_aware"])
        naive_count = len(tz_columns) - aware_count
        has_mixed = len(tz_set) > 1

        all_tz_profiles.append({
            "table": table_name,
            "timezones": sorted(tz_set),
            "aware_count": aware_count,
            "naive_count": naive_count,
        })

        # --- build per-table finding ---
        if has_mixed:
            severity = "warning"
            tz_list = ", ".join(sorted(tz_set))
            detail = (
                f"Mixed timezones within table — date/time columns use "
                f"multiple effective timezones ({tz_list}). "
                f"{aware_count} TZ-aware column(s), "
                f"{naive_count} TZ-naive column(s)."
            )
            recommendation = (
                "Standardize date/time columns to a single timezone "
                "(preferably UTC with timestamptz). Mixed timezones "
                "cause silent conversion errors during ingestion."
            )
        elif naive_count > 0 and server_tz != "UTC":
            severity = "info"
            detail = (
                f"All {len(tz_columns)} date/time column(s) are TZ-naive "
                f"— stored values are implicitly in server timezone "
                f"'{server_tz}'. No TZ-aware (timestamptz) columns."
            )
            recommendation = (
                f"During ingestion, treat all timestamps as "
                f"'{server_tz}' and convert to UTC. Consider migrating "
                f"to timestamptz for explicit timezone handling."
            )
        elif aware_count == len(tz_columns):
            severity = "info"
            detail = (
                f"All {len(tz_columns)} date/time column(s) are TZ-aware "
                f"(timestamptz) — values are stored as UTC internally."
            )
            recommendation = (
                "Timestamps are in UTC. No special timezone handling "
                "needed during ingestion."
            )
        else:
            severity = "info"
            tz_val = next(iter(tz_set))
            detail = (
                f"All {len(tz_columns)} date/time column(s) use "
                f"timezone '{tz_val}'. "
                f"{aware_count} TZ-aware, {naive_count} TZ-naive."
            )
            recommendation = (
                f"Treat all timestamps as '{tz_val}' during ingestion."
            )

        findings.append({
            "table": table_name,
            "column": None,
            "check": "timezone",
            "severity": severity,
            "detail": detail,
            "recommendation": recommendation,
            "server_timezone": server_tz,
            "columns": tz_columns,
            "distinct_timezones": sorted(tz_set),
            "tz_aware_count": aware_count,
            "tz_naive_count": naive_count,
        })

    # --- cross-table summary ---
    all_tzs: set = set()
    for p in all_tz_profiles:
        all_tzs.update(p["timezones"])

    if len(all_tzs) > 1:
        tz_list = ", ".join(sorted(all_tzs))
        findings.append({
            "table": "(database-wide)",
            "column": None,
            "check": "timezone",
            "severity": "warning",
            "detail": (
                f"Multiple effective timezones detected across the "
                f"database: {tz_list}. This increases the risk of "
                f"timezone conversion errors during ingestion."
            ),
            "recommendation": (
                "Establish a single source timezone convention. "
                "Preferably migrate all columns to timestamptz (UTC). "
                "Document per-table timezone assumptions for the "
                "ingestion pipeline."
            ),
            "server_timezone": server_tz,
            "all_timezones": sorted(all_tzs),
        })

    return findings


# ---------------------------------------------------------------------------
# Inline schema analysis (when no schema.json provided)
# ---------------------------------------------------------------------------

# Types where MIN/MAX is unsupported
_RANGE_SKIP = (
    "json", "jsonb", "bytea", "xml", "tsvector", "tsquery",
    "point", "line", "lseg", "box", "path", "polygon", "circle",
    "array", "user-defined", "bool",
)


def _inline_schema_analysis(engine: Engine, schema: str) -> List[Dict]:
    """Lightweight schema analysis when no schema.json is available."""
    inspector = inspect(engine)
    tables: List[Dict] = []

    for table_name in sorted(inspector.get_table_names(schema=schema)):
        try:
            columns_raw = inspector.get_columns(table_name, schema=schema)
            pk_info = inspector.get_pk_constraint(table_name, schema=schema)
            pk_columns = pk_info.get("constrained_columns", []) if pk_info else []

            fk_raw = inspector.get_foreign_keys(table_name, schema=schema)
            fk_list = []
            for fk in fk_raw:
                for lc, rc in zip(fk["constrained_columns"],
                                  fk["referred_columns"]):
                    fk_list.append({
                        "column": lc,
                        "references": f"{fk['referred_table']}.{rc}",
                    })

            # Row count
            try:
                with engine.connect() as conn:
                    row_count = conn.execute(
                        text(f'SELECT COUNT(*) FROM "{schema}"."{table_name}"')
                    ).scalar() or 0
            except Exception:
                row_count = 0

            # Column statistics in a single scan
            stats_parts: List[str] = []
            range_cols: Set[str] = set()
            for c in columns_raw:
                cname = c["name"]
                ctype = str(c["type"]).lower()
                quoted = f'"{cname}"'
                stats_parts.append(
                    f'COUNT(DISTINCT {quoted}) AS "{cname}__card"'
                )
                stats_parts.append(
                    f'SUM(CASE WHEN {quoted} IS NULL THEN 1 ELSE 0 END) '
                    f'AS "{cname}__nulls"'
                )
                if not any(s in ctype for s in _RANGE_SKIP):
                    stats_parts.append(f'MIN({quoted}) AS "{cname}__min"')
                    stats_parts.append(f'MAX({quoted}) AS "{cname}__max"')
                    range_cols.add(cname)

            col_stats: Dict[str, Dict] = {}
            if stats_parts and row_count > 0:
                try:
                    q = (
                        f"SELECT {', '.join(stats_parts)} "
                        f'FROM "{schema}"."{table_name}"'
                    )
                    with engine.connect() as conn:
                        row = conn.execute(text(q)).fetchone()
                    if row:
                        rd = dict(row._mapping)
                        for c in columns_raw:
                            cn = c["name"]
                            st: Dict[str, Any] = {
                                "cardinality": int(rd.get(f"{cn}__card", 0) or 0),
                                "null_count": int(rd.get(f"{cn}__nulls", 0) or 0),
                            }
                            if cn in range_cols:
                                mn = rd.get(f"{cn}__min")
                                mx = rd.get(f"{cn}__max")
                                if mn is not None or mx is not None:
                                    st["data_range"] = {
                                        "min": str(mn) if mn is not None else None,
                                        "max": str(mx) if mx is not None else None,
                                    }
                            col_stats[cn] = st
                except Exception:
                    pass

            columns: List[Dict] = []
            for c in columns_raw:
                entry: Dict[str, Any] = {
                    "name": c["name"],
                    "type": str(c["type"]).lower(),
                    "nullable": c.get("nullable", True),
                }
                st = col_stats.get(c["name"], {})
                entry["cardinality"] = st.get("cardinality", 0)
                entry["null_count"] = st.get("null_count", 0)
                if "data_range" in st:
                    entry["data_range"] = st["data_range"]
                columns.append(entry)

            tables.append({
                "table": table_name,
                "schema": schema,
                "columns": columns,
                "primary_keys": pk_columns,
                "foreign_keys": fk_list,
                "row_count": row_count,
                "has_primary_key": len(pk_columns) > 0,
                "has_foreign_keys": len(fk_list) > 0,
            })
        except Exception as e:
            logger.warning(f"Skipped table '{table_name}': {e}")
    return tables


# ---------------------------------------------------------------------------
# Main entry point
# ---------------------------------------------------------------------------

def analyze_data_quality(
    database_url: str,
    output_path: str,
    schema: Optional[str] = None,
    schema_json_path: Optional[str] = None,
) -> Dict[str, Any]:
    """Analyze data quality and produce a findings report.

    Args:
        database_url:      Database connection URL
        output_path:       Path to save the quality report JSON
        schema:            Schema name to analyze (default: public)
        schema_json_path:  Optional path to existing schema.json from
                           the database-analyser skill (avoids re-scanning)

    Returns:
        Dict containing the full quality report (same as saved to JSON).
    """
    schema = schema or "public"
    logger.info(f"Starting data quality analysis for schema: {schema}")

    engine = get_engine(database_url)

    # ---- Load or build table metadata ----
    if schema_json_path and os.path.exists(schema_json_path):
        logger.info(f"Loading schema metadata from {schema_json_path}")
        with open(schema_json_path, "r", encoding="utf-8") as f:
            schema_doc = json.load(f)
        tables = schema_doc.get("tables", [])
    else:
        logger.info("No schema.json provided — running inline schema scan")
        tables = _inline_schema_analysis(engine, schema)

    if not tables:
        logger.warning("No tables found to analyze.")
        return {"error": "No tables found"}

    # PK lookup for FK checks
    all_pks: Dict[str, List[str]] = {
        t["table"]: t.get("primary_keys", []) for t in tables
    }

    # ---- Fetch constraint metadata ----
    logger.info("Fetching constraint metadata…")
    check_constraints = fetch_check_constraints(engine, schema)
    enum_cols = fetch_enum_columns(engine, schema)
    unique_constraints = fetch_unique_constraints(engine, schema)

    # ---- Run quality checks ----
    all_findings: List[Dict] = []

    logger.info("Check 1/9: Controlled value list candidates…")
    all_findings.extend(check_controlled_value_candidates(
        engine, tables, check_constraints, enum_cols,
        unique_constraints, schema,
    ))

    logger.info("Check 2/9: Nullable but never-null columns…")
    all_findings.extend(check_nullable_but_never_null(tables))

    logger.info("Check 3/9: Missing primary keys…")
    all_findings.extend(check_missing_primary_keys(tables))

    logger.info("Check 4/9: Missing foreign keys & orphaned references…")
    all_findings.extend(check_missing_foreign_keys(
        engine, tables, all_pks, schema,
    ))

    logger.info("Check 5/9: Format inconsistencies…")
    all_findings.extend(check_format_inconsistency(engine, tables, schema))

    logger.info("Check 6/9: Range / domain violations…")
    all_findings.extend(check_range_violations(engine, tables, schema))

    logger.info("Check 7/9: Delete management assessment…")
    all_findings.extend(check_delete_management(engine, tables, schema))

    logger.info("Check 8/9: Late-arriving data assessment…")
    all_findings.extend(check_late_arriving_data(engine, tables, schema))

    logger.info("Check 9/9: Timezone assessment…")
    all_findings.extend(check_timezone(engine, tables, schema))

    # ---- Build report ----
    severity_counts = Counter(f["severity"] for f in all_findings)
    check_counts = Counter(f["check"] for f in all_findings)

    controlled_candidates = [
        {
            "table": f["table"],
            "column": f["column"],
            "distinct_values": f.get("distinct_values", []),
            "cardinality": f.get("cardinality", 0),
            "has_constraint": False,
        }
        for f in all_findings
        if f["check"] == "controlled_value_candidate"
    ]

    delete_management = [
        {
            "table": f["table"],
            "delete_strategy": f.get("delete_strategy"),
            "soft_delete_column": f.get("soft_delete_column"),
            "soft_delete_type": f.get("soft_delete_type"),
            "cdc_enabled": f.get("cdc_enabled"),
            "has_audit_trail": f.get("has_audit_trail"),
        }
        for f in all_findings
        if f["check"] == "delete_management"
    ]

    late_arriving = [
        {
            "table": f["table"],
            "business_date_column": f.get("business_date_column"),
            "system_ts_column": f.get("system_ts_column"),
            "lag_stats": f.get("lag_stats"),
            "recommended_lookback_days": f.get("recommended_lookback_days"),
        }
        for f in all_findings
        if f["check"] == "late_arriving_data" and f.get("lag_stats")
    ]

    timezone_summary = [
        {
            "table": f["table"],
            "server_timezone": f.get("server_timezone"),
            "distinct_timezones": f.get("distinct_timezones"),
            "tz_aware_count": f.get("tz_aware_count"),
            "tz_naive_count": f.get("tz_naive_count"),
            "columns": f.get("columns"),
        }
        for f in all_findings
        if f["check"] == "timezone" and f["table"] != "(database-wide)"
    ]

    report: Dict[str, Any] = {
        "metadata": {
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "database_url": (
                database_url.split("@")[-1]
                if "@" in database_url else database_url
            ),
            "schema_filter": schema,
            "total_tables_analyzed": len(tables),
            "total_findings": len(all_findings),
        },
        "summary": {
            "critical": severity_counts.get("critical", 0),
            "warning": severity_counts.get("warning", 0),
            "info": severity_counts.get("info", 0),
            "by_check": dict(check_counts),
        },
        "findings": all_findings,
        "controlled_value_candidates": controlled_candidates,
        "delete_management": delete_management,
        "late_arriving_data": late_arriving,
        "timezone_summary": timezone_summary,
        "constraints_found": {
            "check_constraints": sum(len(v) for v in check_constraints.values()),
            "enum_columns": sum(len(v) for v in enum_cols.values()),
            "unique_constraints": sum(len(v) for v in unique_constraints.values()),
        },
    }

    # Save
    logger.info(f"Saving quality report to {output_path}")
    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(report, f, indent=2, default=str)

    logger.info(
        f"Done — {len(all_findings)} finding(s) across "
        f"{len(tables)} table(s)"
    )
    logger.info(f"  Critical : {severity_counts.get('critical', 0)}")
    logger.info(f"  Warning  : {severity_counts.get('warning', 0)}")
    logger.info(f"  Info     : {severity_counts.get('info', 0)}")

    return report


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    import sys

    _load_env_file()

    if len(sys.argv) < 3:
        print(
            "Usage: python data_quality.py <database_url> <output_json_path> "
            "[schema] [schema_json_path]"
        )
        print("  schema can also be set via DATABASE_SCHEMA or SCHEMA in .env")
        print(
            "  schema_json_path: optional path to existing schema.json "
            "from database-analyser"
        )
        sys.exit(1)

    db_url = sys.argv[1]
    out_path = sys.argv[2]
    db_schema = (
        sys.argv[3] if len(sys.argv) > 3
        else os.environ.get("DATABASE_SCHEMA")
        or os.environ.get("SCHEMA")
        or None
    )
    schema_json = sys.argv[4] if len(sys.argv) > 4 else None

    analyze_data_quality(
        db_url, out_path, schema=db_schema, schema_json_path=schema_json,
    )
