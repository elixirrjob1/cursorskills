#!/usr/bin/env python3
"""Dialect-aware DB type recommendation from API column profiles."""

from __future__ import annotations

import re
from typing import Any, Dict, List, Optional, Tuple

SUPPORTED_DIALECTS = frozenset({"snowflake", "sqlserver", "postgresql", "oracle"})

CRUDE_TYPES = frozenset({"text", "integer", "numeric", "boolean", "timestamp"})

_UUID_RE = re.compile(
    r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$",
    re.I,
)
_EMAIL_RE = re.compile(r"^[\w\.-]+@[\w\.-]+\.\w+$")
_PHONE_RE = re.compile(r"^\+?\d[\d\s\-\(\)]+$")
_ISO_DATE_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")
_ISO_DATETIME_RE = re.compile(r"^\d{4}-\d{2}-\d{2}[T\s]\d{2}:\d{2}:\d{2}")
_NUMERIC_STRING_RE = re.compile(r"^-?\d+(\.\d+)?$")


def is_crude_type(type_str: str) -> bool:
    base = str(type_str or "").strip().lower().split("(")[0]
    return base in CRUDE_TYPES


def is_source_db_type(type_str: str) -> bool:
    """True when type already looks like a destination DB type, not crude inference."""
    lowered = str(type_str or "").strip().lower()
    if not lowered or is_crude_type(lowered):
        return False
    hints = (
        "varchar", "nvarchar", "varchar2", "char", "nchar", "text", "clob",
        "bigint", "int", "integer", "number", "numeric", "decimal", "float",
        "double", "binary_double", "boolean", "bit", "date", "time",
        "timestamp", "datetime", "uuid", "uniqueidentifier",
    )
    return any(h in lowered for h in hints)


def detect_pattern_hint(value: Any, col_name: str) -> Optional[str]:
    if value is None:
        return None
    if isinstance(value, bool):
        return "boolean"
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        return "numeric"
    text = str(value).strip()
    if not text:
        return None
    name = col_name.lower()
    if _UUID_RE.match(text):
        return "uuid"
    if _EMAIL_RE.match(text) or "email" in name:
        return "email"
    if _PHONE_RE.match(text) and ("phone" in name or "mobile" in name):
        return "phone"
    if _ISO_DATE_RE.match(text):
        return "iso_date"
    if _ISO_DATETIME_RE.match(text) or "at" in name or "date" in name:
        return "iso_datetime"
    if _NUMERIC_STRING_RE.match(text):
        return "numeric_string"
    return "free_text"


def profile_column(col_name: str, records: List[dict], coarse_type: str) -> Dict[str, Any]:
    """Profile one column across all sample rows."""
    max_observed_len = 0
    min_value: Optional[float] = None
    max_value: Optional[float] = None
    all_integers = True
    timezone_aware = True
    pattern_counts: Dict[str, int] = {}
    sample_values: List[Any] = []
    non_null = 0

    for record in records:
        value = record.get(col_name)
        if value is None:
            continue
        non_null += 1
        if len(sample_values) < 10:
            sample_values.append(value)

        hint = detect_pattern_hint(value, col_name)
        if hint:
            pattern_counts[hint] = pattern_counts.get(hint, 0) + 1

        if isinstance(value, bool):
            continue
        if isinstance(value, (int, float)):
            num = float(value)
            min_value = num if min_value is None else min(min_value, num)
            max_value = num if max_value is None else max(max_value, num)
            if not float(value).is_integer():
                all_integers = False
            continue
        text = str(value)
        max_observed_len = max(max_observed_len, len(text))
        if coarse_type == "timestamp" and not _has_timezone(text):
            timezone_aware = False

    pattern_hint = None
    if pattern_counts:
        pattern_hint = max(pattern_counts, key=pattern_counts.get)

    return {
        "max_observed_len": max_observed_len or None,
        "min_value": min_value,
        "max_value": max_value,
        "all_integers": all_integers if non_null else None,
        "timezone_aware": timezone_aware if non_null and coarse_type == "timestamp" else None,
        "pattern_hint": pattern_hint,
        "sample_values": sample_values[:3],
        "non_null_count": non_null,
    }


def _has_timezone(text: str) -> bool:
    return bool(re.search(r"(Z|[+-]\d{2}:?\d{2})$", text.strip()))


def _varchar(dialect: str, length: int) -> str:
    if dialect == "snowflake":
        return f"VARCHAR({length})"
    if dialect == "sqlserver":
        return f"NVARCHAR({length})"
    if dialect == "postgresql":
        return f"VARCHAR({length})"
    return f"VARCHAR2({length})"


def _long_text(dialect: str) -> str:
    if dialect == "snowflake":
        return "TEXT"
    if dialect == "sqlserver":
        return "NVARCHAR(MAX)"
    if dialect == "postgresql":
        return "TEXT"
    return "CLOB"


def recommend_db_type(
    coarse_type: str,
    profile: Dict[str, Any],
    dialect: str,
    col_name: str = "",
) -> Tuple[str, str]:
    """Return (db_type, confidence) where confidence is 'rule' or 'ambiguous'."""
    dialect = dialect.lower()
    if dialect not in SUPPORTED_DIALECTS:
        raise ValueError(f"Unsupported dialect: {dialect}")

    base = coarse_type.lower()
    hint = profile.get("pattern_hint")
    max_len = profile.get("max_observed_len") or 0
    max_val = profile.get("max_value")
    tz = profile.get("timezone_aware")

    if base == "boolean":
        mapping = {
            "snowflake": "BOOLEAN",
            "sqlserver": "BIT",
            "postgresql": "BOOLEAN",
            "oracle": "NUMBER(1)",
        }
        return mapping[dialect], "rule"

    if base == "integer" or (base == "numeric" and profile.get("all_integers")):
        big = max_val is not None and max_val > 2_147_483_647
        if dialect == "snowflake":
            return ("NUMBER(19,0)" if big else "NUMBER(10,0)"), "rule"
        if dialect == "sqlserver":
            return ("BIGINT" if big else "INT"), "rule"
        if dialect == "postgresql":
            return ("BIGINT" if big else "INTEGER"), "rule"
        return ("NUMBER(19)" if big else "NUMBER(10)"), "rule"

    if base == "numeric":
        mapping = {
            "snowflake": "FLOAT",
            "sqlserver": "FLOAT",
            "postgresql": "DOUBLE PRECISION",
            "oracle": "BINARY_DOUBLE",
        }
        return mapping[dialect], "rule"

    if base == "timestamp":
        if tz:
            mapping = {
                "snowflake": "TIMESTAMP_TZ(9)",
                "sqlserver": "DATETIMEOFFSET",
                "postgresql": "TIMESTAMP WITH TIME ZONE",
                "oracle": "TIMESTAMP WITH TIME ZONE",
            }
        else:
            mapping = {
                "snowflake": "TIMESTAMP_NTZ(9)",
                "sqlserver": "DATETIME2",
                "postgresql": "TIMESTAMP",
                "oracle": "TIMESTAMP",
            }
        return mapping[dialect], "rule"

    if hint == "uuid":
        mapping = {
            "snowflake": "VARCHAR(36)",
            "sqlserver": "UNIQUEIDENTIFIER",
            "postgresql": "UUID",
            "oracle": "VARCHAR2(36)",
        }
        return mapping[dialect], "rule"

    if hint == "email":
        return _varchar(dialect, 254), "rule"

    if hint == "iso_date":
        return "DATE", "rule"

    if max_len <= 50 and max_len > 0:
        return _varchar(dialect, max(max_len * 2, 16)), "rule"
    if max_len <= 255:
        return _varchar(dialect, 255), "rule"
    if max_len <= 1000:
        return _varchar(dialect, 1000), "rule"
    if max_len == 0 and hint in ("numeric_string",):
        return _varchar(dialect, 64), "ambiguous"

    name = col_name.lower()
    if max_len <= 64 and any(k in name for k in ("status", "code", "type", "state")):
        return _varchar(dialect, max(max_len * 2, 32)), "rule"

    return _long_text(dialect), "ambiguous"


def refine_ambiguous_type(
    coarse_type: str,
    profile: Dict[str, Any],
    dialect: str,
    col_name: str,
    cardinality: Optional[int],
) -> Tuple[str, str]:
    """Heuristic refinement for ambiguous columns (pre-AI checklist)."""
    max_len = profile.get("max_observed_len") or 0
    if cardinality and cardinality <= 20 and max_len <= 64:
        length = max(int(max_len * 2), 16)
        return _varchar(dialect, length), "rule"
    db_type, _ = recommend_db_type(coarse_type, profile, dialect, col_name)
    return db_type, "rule"


def apply_type_recommendations(
    columns: List[Dict[str, Any]],
    records: List[dict],
    dialect: str,
    *,
    from_source_metadata: bool,
) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    """Overwrite column type fields. Returns (updated_columns, ambiguous_items)."""
    ambiguous: List[Dict[str, Any]] = []
    updated: List[Dict[str, Any]] = []

    for col in columns:
        col_copy = dict(col)
        col_name = str(col_copy.get("name") or "")
        current_type = str(col_copy.get("type") or "text")

        if from_source_metadata and is_source_db_type(current_type):
            col_copy["type_confidence"] = "source"
            updated.append(col_copy)
            continue

        coarse = current_type.lower().split("(")[0]
        if not is_crude_type(coarse):
            coarse = "text"

        profile = profile_column(col_name, records, coarse)
        db_type, confidence = recommend_db_type(coarse, profile, dialect, col_name)

        if confidence == "ambiguous":
            db_type, confidence = refine_ambiguous_type(
                coarse, profile, dialect, col_name, col_copy.get("cardinality")
            )
            long_types = ("TEXT", "MAX)", "CLOB")
            if any(db_type.endswith(t) or t in db_type for t in long_types):
                ambiguous.append(
                    {
                        "column": col_name,
                        "coarse_type": coarse,
                        "current_type": db_type,
                        "profile": profile,
                        "cardinality": col_copy.get("cardinality"),
                    }
                )

        col_copy["type"] = db_type
        col_copy["type_confidence"] = confidence
        updated.append(col_copy)

    return updated, ambiguous


def detect_tables_needing_types(schema_document: Dict[str, Any]) -> List[str]:
    """Tables missing type_confidence or with crude types."""
    names: List[str] = []
    for table in schema_document.get("tables", []) or []:
        table_name = str(table.get("table") or "").strip()
        if not table_name:
            continue
        needs = False
        for col in table.get("columns", []) or []:
            if col.get("type_confidence") == "manual":
                continue
            if not col.get("type_confidence") or is_crude_type(str(col.get("type") or "")):
                needs = True
                break
        if needs:
            names.append(table_name)
    return names


def merge_table_entries(
    existing_tables: List[Dict[str, Any]],
    new_tables: List[Dict[str, Any]],
    *,
    replace_table_names: Optional[set] = None,
) -> List[Dict[str, Any]]:
    """Merge analyser output into existing schema, preserving untouched tables."""
    by_name = {str(t.get("table")): t for t in existing_tables if t.get("table")}
    replace = replace_table_names or {str(t.get("table")) for t in new_tables if t.get("table")}

    for table in new_tables:
        name = str(table.get("table") or "")
        if not name:
            continue
        if name not in by_name:
            by_name[name] = table
            continue
        if name not in replace:
            continue
        old = by_name[name]
        merged = dict(table)
        for key in (
            "table_description",
            "classification_tags",
            "glossary_terms",
            "classification_summary",
            "field_classifications",
            "sensitive_fields",
        ):
            if old.get(key):
                merged[key] = old[key]
        old_cols = {str(c.get("name")): c for c in old.get("columns", []) or []}
        new_cols = []
        for col in merged.get("columns", []) or []:
            col_name = str(col.get("name") or "")
            old_col = old_cols.get(col_name)
            if not old_col:
                new_cols.append(col)
                continue
            merged_col = dict(col)
            if old_col.get("type_confidence") == "manual":
                merged_col["type"] = old_col.get("type")
                merged_col["type_confidence"] = "manual"
            for key in (
                "column_description",
                "concept_id",
                "concept_confidence",
                "concept_evidence",
                "classification_tags",
                "glossary_terms",
                "semantic_class",
                "unit_context",
            ):
                if old_col.get(key):
                    merged_col[key] = old_col[key]
            new_cols.append(merged_col)
        merged["columns"] = new_cols
        by_name[name] = merged

    return list(by_name.values())
