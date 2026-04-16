#!/usr/bin/env python3
from __future__ import annotations

import argparse
import base64
import getpass
import json
import os
import re
import time
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import requests


_PROJECT_ROOT = Path(__file__).resolve().parents[3]
DEFAULT_INPUT_DIR = _PROJECT_ROOT / "stm" / "input"
DEFAULT_OUTPUT_DIR = _PROJECT_ROOT / "stm" / "output"
SOURCE_SYSTEM = "Snowflake"
SOURCE_DATABASE_SCHEMA = "DRIP_DATA_INTELLIGENCE.BRONZE_ERP__DBO"
SOURCE_TABLE_FILE = "See field-level mapping"
SOURCE_NOTES = "Immediate technical source is Snowflake bronze; original lineage comes from the analyzer source system."
TARGET_DATABASE = "DRIP_DATA_INTELLIGENCE"
TARGET_SCHEMA = "GOLD"
OPENMETADATA_API_VERSION_PREFIX = "v1"
OPENMETADATA_LOGIN_ENDPOINT = "users/login"
OPENMETADATA_TIMEOUT = (10, 30)
OPENMETADATA_USER_AGENT = "stm-from-data-model"
_OPENMETADATA_TOKEN_CACHE: dict[str, str | None] = {"token": None}


@dataclass
class ColumnDef:
    name: str
    data_type: str
    nullable: str
    description: str


@dataclass
class TableDef:
    name: str
    meta: dict[str, str] = field(default_factory=dict)
    columns: list[ColumnDef] = field(default_factory=list)
    business_rules: list[str] = field(default_factory=list)
    measures: list[str] = field(default_factory=list)


@dataclass
class ClassificationDefinition:
    classification_name: str = ""
    classification_description: str = ""
    tag_name: str = ""
    tag_description: str = ""


@dataclass
class GlossaryDefinition:
    term_name: str = ""
    definition: str = ""


@dataclass
class AnalyzerColumnMetadata:
    glossary_terms: list[str] = field(default_factory=list)
    classification_tags: list[str] = field(default_factory=list)


@dataclass
class AnalyzerTableMetadata:
    glossary_terms: list[str] = field(default_factory=list)
    classification_tags: list[str] = field(default_factory=list)
    columns: dict[str, AnalyzerColumnMetadata] = field(default_factory=dict)


@dataclass
class AnalyzerMetadata:
    source_path: Path
    tables: dict[str, AnalyzerTableMetadata] = field(default_factory=dict)
    classification_definitions: dict[str, ClassificationDefinition] = field(default_factory=dict)
    glossary_definitions: dict[str, GlossaryDefinition] = field(default_factory=dict)


def _slug_index_name(index: int, table_name: str) -> str:
    return f"{index:02d}-{table_name}-stm.md"


def _extract_first(pattern: str, text: str) -> str:
    match = re.search(pattern, text, re.MULTILINE)
    return match.group(1).strip() if match else ""


def _table_section_pattern() -> re.Pattern[str]:
    return re.compile(
        r"^###\s+([A-Za-z0-9_]+)\s*$([\s\S]*?)(?=^###\s+[A-Za-z0-9_]+\s*$|^##\s+|\Z)",
        re.MULTILINE,
    )


def _split_bullets(raw: str) -> list[str]:
    values: list[str] = []
    for line in raw.splitlines():
        stripped = line.strip()
        if stripped.startswith("- "):
            values.append(stripped[2:].strip())
    return values


def _extract_labeled_block(label: str, block: str) -> str:
    lines = block.splitlines()
    capture = False
    captured: list[str] = []
    label_prefix = f"**{label}**:"
    for line in lines:
        stripped = line.strip()
        if stripped.startswith("**") and not stripped.startswith(label_prefix) and capture:
            break
        if stripped.startswith("### ") and capture:
            break
        if stripped.startswith(label_prefix):
            capture = True
            remainder = stripped[len(label_prefix):].strip()
            if remainder:
                captured.append(remainder)
            continue
        if capture:
            if stripped.startswith("|"):
                break
            captured.append(line.rstrip())
    return "\n".join(captured).strip()


def _parse_markdown_table(block: str) -> list[ColumnDef]:
    rows: list[ColumnDef] = []
    lines = block.splitlines()
    in_table = False
    for line in lines:
        stripped = line.strip()
        if stripped.startswith("| Column | Data Type | Nullable | Description |"):
            in_table = True
            continue
        if not in_table:
            continue
        if not stripped.startswith("|"):
            break
        if set(stripped.replace("|", "").replace("-", "").replace(" ", "")) == set():
            continue
        parts = [part.strip() for part in stripped.strip("|").split("|")]
        if len(parts) != 4:
            continue
        if parts[0] == "Column":
            continue
        rows.append(
            ColumnDef(
                name=parts[0],
                data_type=parts[1],
                nullable=parts[2],
                description=parts[3],
            )
        )
    return rows


def _parse_labeled_value(label: str, block: str) -> str:
    pattern = rf"\*\*{re.escape(label)}\*\*:\s*(.+)$"
    return _extract_first(pattern, block)


def parse_model_markdown(text: str) -> tuple[dict[str, str], list[TableDef]]:
    doc = {
        "title": _extract_first(r"^#\s+(.+)$", text),
        "generated": _extract_first(r"^\*\*Generated\*\*:\s*(.+)$", text),
        "framework": _extract_first(r"^\*\*Framework\*\*:\s*(.+)$", text),
        "naming_convention": _extract_first(r"^\*\*Naming Convention\*\*:\s*(.+)$", text),
        "summary": _extract_first(r"^## Summary\s*$([\s\S]*?)(?=^### Key Design Decisions|^## )", text),
        "author": "",
    }

    version_history_match = re.search(
        r"^## Version History\s*$([\s\S]*?)(?=^## |\Z)",
        text,
        re.MULTILINE,
    )
    if version_history_match:
        lines = [line.strip() for line in version_history_match.group(1).splitlines() if line.strip().startswith("|")]
        for line in lines:
            parts = [part.strip() for part in line.strip("|").split("|")]
            if len(parts) != 4:
                continue
            if parts[0] in {"Version", "---------"}:
                continue
            if parts[2]:
                doc["author"] = parts[2]
                break

    tables: list[TableDef] = []
    for match in _table_section_pattern().finditer(text):
        name = match.group(1).strip()
        block = match.group(2)
        table = TableDef(name=name)
        for label in ("Type", "Description", "Primary Key", "Business Key", "SCD Type", "Grain"):
            value = _parse_labeled_value(label, block)
            if value:
                table.meta[label] = value

        foreign_keys_raw = _extract_labeled_block("Foreign Keys", block)
        if foreign_keys_raw:
            table.meta["Foreign Keys"] = foreign_keys_raw

        measures_raw = _extract_labeled_block("Measures", block)
        if measures_raw:
            first_line = measures_raw.splitlines()[0].strip()
            if first_line and not first_line.startswith("- "):
                table.measures.append(first_line)
            table.measures.extend(_split_bullets(measures_raw))

        business_rules_raw = _extract_labeled_block("Business Rules", block)
        if business_rules_raw:
            first_line = business_rules_raw.splitlines()[0].strip()
            if first_line and not first_line.startswith("- "):
                table.business_rules.append(first_line)
            table.business_rules.extend(_split_bullets(business_rules_raw))

        table.columns = _parse_markdown_table(block)
        tables.append(table)

    return doc, tables


def _infer_system_module(doc_title: str) -> str:
    return doc_title.replace(" Data Model", "").strip()


def _field_type(column: ColumnDef, table: TableDef) -> str:
    name = column.name
    lower = name.lower()
    desc = column.description.lower()
    if name.endswith("HashPK"):
        return "Primary Key"
    if name.endswith("HashFK"):
        return "Foreign Key"
    if name.endswith("HashBK"):
        return "Business Key"
    if lower in {"loadtimestamp", "etlbatchid"} or "etl" in desc or "load timestamp" in desc:
        return "Audit/Metadata"
    if table.meta.get("Type", "").lower().startswith("fact"):
        if any(token in lower for token in ("amount", "hours", "rate", "percent", "cost", "revenue", "margin", "variance")):
            return "Measure"
    return "Attribute"


def _normalize_formula(text: str) -> str:
    value = text.strip()
    if not value:
        return ""
    value = re.sub(r"\s+", " ", value)
    return value


def _column_transformation_logic(column: ColumnDef, table: TableDef) -> str:
    """Return a Snowflake SQL expression skeleton for the target column.

    Uses {SOURCE_COL} as a placeholder when the actual source column name
    is not yet known (filled later by the enrichment subagent).
    """
    desc = column.description
    lower = desc.lower()
    name = column.name
    data_type = column.data_type

    if name.endswith("HashPK"):
        return "CAST(SHA2(COALESCE(CAST({SOURCE_COL} AS VARCHAR), '#@#@#@#@#'), 256) AS BINARY(32))"
    if name.endswith("HashBK"):
        return "CAST(SHA2(COALESCE(CAST({SOURCE_COL} AS VARCHAR), '#@#@#@#@#'), 256) AS BINARY(32))"
    if name.endswith("HashFK"):
        nullable = column.nullable.upper() == "YES"
        if nullable:
            return "IFF({SOURCE_COL} IS NULL, NULL, CAST(SHA2(COALESCE(CAST({SOURCE_COL} AS VARCHAR), '#@#@#@#@#'), 256) AS BINARY(32)))"
        return "CAST(SHA2(COALESCE(CAST({SOURCE_COL} AS VARCHAR), '#@#@#@#@#'), 256) AS BINARY(32))"

    if "scd type 2 row effective start date" in lower:
        return "CAST({SOURCE_COL} AS DATE)"
    if "scd type 2 row expiration date" in lower:
        return "CAST({SOURCE_COL} AS DATE)"
    if "scd type 2 current row flag" in lower:
        return "IFF({SOURCE_COL} IS NULL, TRUE, FALSE)"

    if "degenerate dimension" in lower:
        return f"CAST({{SOURCE_COL}} AS {data_type})"

    if "foreign key to" in lower:
        return "CAST(SHA2(COALESCE(CAST({SOURCE_COL} AS VARCHAR), '#@#@#@#@#'), 256) AS BINARY(32))"

    normalized = desc.replace(" ", "").lower()
    if "calculated billable amount" in lower and "billablehours*billrate" in normalized:
        return "CAST(BILLABLE_HOURS * BILL_RATE AS DECIMAL(19,4))"
    if "calculated cost amount" in lower and "hoursworked*costrate" in normalized:
        return "CAST(HOURS_WORKED * COST_RATE AS DECIMAL(19,4))"
    if "calculated margin" in lower and "billedamount-costamount" in normalized:
        return "CAST(BILLED_AMOUNT - COST_AMOUNT AS DECIMAL(19,4))"
    if "calculated" in lower and "(" in desc and ")" in desc:
        match = re.search(r"\(([^)]+)\)", desc)
        if match:
            formula = _normalize_formula(match.group(1))
            return f"CAST({formula} AS {data_type})"

    for business_rule in table.business_rules:
        rule = business_rule.strip().removeprefix("-").strip()
        if rule.startswith(f"{name} ="):
            formula = rule.split("=", 1)[1].strip()
            return f"CAST({formula} AS {data_type})"

    return ""


def _table_rule_rows(table: TableDef) -> list[tuple[str, str, str, str]]:
    rows: list[tuple[str, str, str, str]] = []
    next_id = 1

    for business_rule in table.business_rules:
        cleaned = business_rule.strip().removeprefix("-").strip()
        if not cleaned:
            continue
        rows.append(
            (
                f"BR{next_id}",
                "Business Rule",
                cleaned,
                "",
            )
        )
        next_id += 1

    for column in table.columns:
        formula = _column_transformation_logic(column, table)
        if not formula:
            continue
        rows.append(
            (
                f"TX{next_id}",
                f"{column.name} Transformation",
                column.description,
                formula,
            )
        )
        next_id += 1

    seen: set[tuple[str, str, str, str]] = set()
    unique_rows: list[tuple[str, str, str, str]] = []
    for row in rows:
        if row in seen:
            continue
        seen.add(row)
        unique_rows.append(row)
    return unique_rows


def _markdown_escape(value: Any) -> str:
    text = str(value or "")
    text = text.replace("\r\n", "\n").replace("\r", "\n")
    text = text.replace("\n", "<br/><br/>")
    return text.replace("|", "\\|")


def _list_of_strings(value: Any) -> list[str]:
    if not isinstance(value, list):
        return []
    cleaned: list[str] = []
    for item in value:
        text = str(item).strip()
        if text:
            cleaned.append(text)
    return cleaned


def _normalize_identifier(value: str) -> str:
    normalized = str(value or "").strip().strip("`").strip('"').strip("'")
    normalized = normalized.replace("[", "").replace("]", "")
    return normalized.lower()


def _identifier_candidates(value: str) -> list[str]:
    normalized = _normalize_identifier(value)
    if not normalized:
        return []
    candidates = [normalized]
    if "." in normalized:
        last_part = normalized.split(".")[-1]
        if last_part and last_part not in candidates:
            candidates.append(last_part)
    return candidates


def _parse_tag_fqn(fqn: str) -> tuple[str, str]:
    if "." not in fqn:
        return "", fqn
    prefix, suffix = fqn.split(".", 1)
    return prefix, suffix


def _extract_glossary_definition_map(document: dict[str, Any]) -> dict[str, GlossaryDefinition]:
    definition_map: dict[str, GlossaryDefinition] = {}
    metadata = document.get("metadata")
    if not isinstance(metadata, dict):
        return definition_map

    raw_sources: list[Any] = []
    for key in ("openmetadata_glossary_terms", "glossary_term_definitions", "glossary_terms_catalog"):
        raw_value = metadata.get(key)
        if isinstance(raw_value, list):
            raw_sources.extend(raw_value)
        elif isinstance(raw_value, dict):
            raw_sources.extend(raw_value.values())

    for item in raw_sources:
        if not isinstance(item, dict):
            continue
        fqn = str(item.get("fqn") or item.get("fullyQualifiedName") or "").strip()
        definition = str(item.get("definition") or item.get("description") or "").strip()
        if not fqn:
            continue
        _, term_name = _parse_tag_fqn(fqn)
        definition_map[fqn] = GlossaryDefinition(term_name=term_name, definition=definition)
    return definition_map


def _normalize_openmetadata_base_url(value: str) -> str:
    base = (value or "").strip().rstrip("/")
    if not base:
        raise RuntimeError("Missing OpenMetadata base URL. Set OPENMETADATA_BASE_URL.")
    return base if base.endswith("/api") else f"{base}/api"


def _openmetadata_api_root() -> str:
    return _normalize_openmetadata_base_url(os.getenv("OPENMETADATA_BASE_URL", ""))


def _openmetadata_api_url(path: str) -> str:
    cleaned = path.lstrip("/")
    if not cleaned.startswith(f"{OPENMETADATA_API_VERSION_PREFIX}/"):
        cleaned = f"{OPENMETADATA_API_VERSION_PREFIX}/{cleaned}"
    return f"{_openmetadata_api_root()}/{cleaned}"


def _openmetadata_login_payloads() -> list[dict[str, str]]:
    email = os.getenv("OPENMETADATA_EMAIL", "").strip()
    password = os.getenv("OPENMETADATA_PASSWORD", "")
    if not email or not password:
        raise RuntimeError(
            "Missing OpenMetadata credentials. Set OPENMETADATA_EMAIL and OPENMETADATA_PASSWORD."
        )
    encoded_password = base64.b64encode(password.encode("utf-8")).decode("ascii")
    return [
        {"email": email, "password": encoded_password},
        {"email": email, "password": password},
    ]


def _extract_openmetadata_token(payload: Any) -> str | None:
    if isinstance(payload, str) and payload.strip():
        return payload.strip()
    if isinstance(payload, dict):
        for key in ("accessToken", "jwtToken", "token", "id_token"):
            value = payload.get(key)
            if isinstance(value, str) and value.strip():
                return value.strip()
        for nested_key in ("data", "response"):
            token = _extract_openmetadata_token(payload.get(nested_key))
            if token:
                return token
    return None


def _openmetadata_login() -> str:
    jwt_token = os.getenv("OPENMETADATA_JWT_TOKEN", "").strip()
    if jwt_token:
        return jwt_token

    cached = _OPENMETADATA_TOKEN_CACHE.get("token")
    if isinstance(cached, str) and cached.strip():
        return cached.strip()

    last_error: Exception | None = None
    for payload in _openmetadata_login_payloads():
        try:
            response = requests.post(
                _openmetadata_api_url(OPENMETADATA_LOGIN_ENDPOINT),
                headers={
                    "Accept": "application/json",
                    "Content-Type": "application/json",
                    "User-Agent": OPENMETADATA_USER_AGENT,
                },
                json=payload,
                timeout=OPENMETADATA_TIMEOUT,
            )
            response.raise_for_status()
            data = response.json() if response.content else {}
            token = _extract_openmetadata_token(data)
            if token:
                _OPENMETADATA_TOKEN_CACHE["token"] = token
                return token
            last_error = RuntimeError("OpenMetadata login succeeded but no JWT token was returned.")
        except requests.exceptions.RequestException as exc:
            last_error = exc

    raise RuntimeError(f"OpenMetadata login failed: {last_error}") from last_error


def _openmetadata_headers() -> dict[str, str]:
    return {
        "Accept": "application/json",
        "Content-Type": "application/json",
        "Authorization": f"Bearer {_openmetadata_login()}",
        "User-Agent": OPENMETADATA_USER_AGENT,
    }


def _openmetadata_request(method: str, endpoint: str, params: dict[str, Any] | None = None) -> Any:
    last_error: Exception | None = None
    for attempt in range(2):
        try:
            response = requests.request(
                method=method,
                url=_openmetadata_api_url(endpoint),
                headers=_openmetadata_headers(),
                params=params,
                timeout=OPENMETADATA_TIMEOUT,
            )
            if response.status_code == 401 and not os.getenv("OPENMETADATA_JWT_TOKEN"):
                _OPENMETADATA_TOKEN_CACHE["token"] = None
                if attempt < 1:
                    continue
            response.raise_for_status()
            if response.status_code == 204 or not response.content:
                return {}
            return response.json()
        except requests.exceptions.RequestException as exc:
            last_error = exc
            if attempt == 1:
                break
            time.sleep(2**attempt)

    raise RuntimeError(f"OpenMetadata API request failed: {last_error}") from last_error


def fetch_openmetadata_glossary_definitions() -> dict[str, GlossaryDefinition]:
    payload = _openmetadata_request("GET", "glossaryTerms", params={"limit": 1000})
    terms = payload.get("data")
    if not isinstance(terms, list):
        return {}

    definitions: dict[str, GlossaryDefinition] = {}
    for term in terms:
        if not isinstance(term, dict):
            continue
        fqn = str(term.get("fullyQualifiedName") or "").strip()
        if not fqn:
            continue
        term_name = str(term.get("name") or term.get("displayName") or _parse_tag_fqn(fqn)[1]).strip()
        definition = str(term.get("description") or "").strip()
        definitions[fqn] = GlossaryDefinition(term_name=term_name, definition=definition)
    return definitions


def load_analyzer_metadata(path: Path) -> AnalyzerMetadata:
    document = json.loads(path.read_text(encoding="utf-8"))
    metadata = document.get("metadata")
    classification_definitions: dict[str, ClassificationDefinition] = {}
    if isinstance(metadata, dict):
        raw_classifications = metadata.get("openmetadata_classifications")
        if isinstance(raw_classifications, list):
            for classification in raw_classifications:
                if not isinstance(classification, dict):
                    continue
                classification_name = str(classification.get("name") or "").strip()
                classification_description = str(classification.get("description") or "").strip()
                options = classification.get("options")
                if not isinstance(options, list):
                    continue
                for option in options:
                    if not isinstance(option, dict):
                        continue
                    fqn = str(option.get("fqn") or "").strip()
                    if not fqn:
                        continue
                    classification_definitions[fqn] = ClassificationDefinition(
                        classification_name=classification_name,
                        classification_description=classification_description,
                        tag_name=str(option.get("name") or "").strip(),
                        tag_description=str(option.get("description") or "").strip(),
                    )

    analyzer = AnalyzerMetadata(
        source_path=path,
        classification_definitions=classification_definitions,
        glossary_definitions=_extract_glossary_definition_map(document),
    )

    raw_tables = document.get("tables")
    if not isinstance(raw_tables, list):
        return analyzer

    for raw_table in raw_tables:
        if not isinstance(raw_table, dict):
            continue
        table_name = str(raw_table.get("table") or "").strip()
        if not table_name:
            continue

        table_metadata = AnalyzerTableMetadata(
            glossary_terms=_list_of_strings(raw_table.get("glossary_terms")),
            classification_tags=_list_of_strings(raw_table.get("classification_tags")),
        )

        raw_columns = raw_table.get("columns")
        if isinstance(raw_columns, list):
            for raw_column in raw_columns:
                if not isinstance(raw_column, dict):
                    continue
                column_name = str(raw_column.get("name") or "").strip()
                if not column_name:
                    continue
                column_metadata = AnalyzerColumnMetadata(
                    glossary_terms=_list_of_strings(raw_column.get("glossary_terms")),
                    classification_tags=_list_of_strings(raw_column.get("classification_tags")),
                )
                for candidate in _identifier_candidates(column_name):
                    table_metadata.columns[candidate] = column_metadata

        for candidate in _identifier_candidates(table_name):
            analyzer.tables[candidate] = table_metadata

    return analyzer


def _find_analyzer_table(table_name: str, analyzer_metadata: AnalyzerMetadata) -> AnalyzerTableMetadata:
    for candidate in _identifier_candidates(table_name):
        table_metadata = analyzer_metadata.tables.get(candidate)
        if table_metadata is not None:
            return table_metadata
    return AnalyzerTableMetadata()


def _find_analyzer_column(table_metadata: AnalyzerTableMetadata, column_name: str) -> AnalyzerColumnMetadata:
    for candidate in _identifier_candidates(column_name):
        column_metadata = table_metadata.columns.get(candidate)
        if column_metadata is not None:
            return column_metadata
    return AnalyzerColumnMetadata()


_TAG_TIER: dict[str, int] = {
    "Architecture.Enriched": 3, "Architecture.Curated": 2, "Architecture.Raw": 1,
    "Certification.Gold": 3, "Certification.Silver": 2, "Certification.Bronze": 1,
    "PII.Sensitive": 3, "PII.NonSensitive": 2, "PII.None": 1,
    "Tier.Tier1": 3, "Tier.Tier2": 2, "Tier.Tier3": 1,
}

_TARGET_SCHEMA_LAYER_OVERRIDES: dict[str, dict[str, str]] = {
    "GOLD": {"Architecture": "Architecture.Enriched", "Certification": "Certification.Gold"},
    "SILVER": {"Architecture": "Architecture.Enriched", "Certification": "Certification.Silver"},
    "ENRICHED": {"Architecture": "Architecture.Enriched", "Certification": "Certification.Gold"},
    "CURATED": {"Architecture": "Architecture.Curated", "Certification": "Certification.Gold"},
    "BRONZE": {"Architecture": "Architecture.Raw", "Certification": "Certification.Bronze"},
    "RAW": {"Architecture": "Architecture.Raw", "Certification": "Certification.Bronze"},
}


def _deduplicate_tags(
    rows: list[tuple[str, str, str, str]],
) -> list[tuple[str, str, str, str]]:
    """Deduplicate (scope, column, tag_fqn, classification) rows.

    For table-level: one tag per classification, highest tier wins.
    For column-level: one tag per (column, classification), highest tier wins.
    """
    table_best: dict[str, tuple[str, str, str, str]] = {}
    col_best: dict[tuple[str, str], tuple[str, str, str, str]] = {}
    table_order: list[str] = []
    col_order: list[tuple[str, str]] = []

    for row in rows:
        scope, column, tag_fqn, classification = row
        tier = _TAG_TIER.get(tag_fqn, 0)

        if scope == "Table":
            existing = table_best.get(classification)
            if existing is None:
                table_best[classification] = row
                table_order.append(classification)
            else:
                existing_tier = _TAG_TIER.get(existing[2], 0)
                if tier > existing_tier:
                    table_best[classification] = row
        else:
            key = (column, classification)
            existing = col_best.get(key)
            if existing is None:
                col_best[key] = row
                col_order.append(key)
            else:
                existing_tier = _TAG_TIER.get(existing[2], 0)
                if tier > existing_tier:
                    col_best[key] = row

    result: list[tuple[str, str, str, str]] = []
    seen_t: set[str] = set()
    for cls in table_order:
        if cls not in seen_t:
            result.append(table_best[cls])
            seen_t.add(cls)
    seen_c: set[tuple[str, str]] = set()
    for key in col_order:
        if key not in seen_c:
            result.append(col_best[key])
            seen_c.add(key)
    return result


def _append_classification_section(
    lines: list[str],
    table: TableDef,
    analyzer_table: AnalyzerTableMetadata,
    analyzer_metadata: AnalyzerMetadata,
) -> None:
    lines.append("## 5. Classification Tags")
    lines.append("| Scope | Column | Tag FQN | Classification |")
    lines.append("|-------|--------|---------|----------------|")

    raw_rows: list[tuple[str, str, str, str]] = []
    for tag in analyzer_table.classification_tags:
        definition = analyzer_metadata.classification_definitions.get(tag, ClassificationDefinition())
        classification_name = definition.classification_name or _parse_tag_fqn(tag)[0]
        raw_rows.append(("Table", "", tag, classification_name))

    for column in table.columns:
        column_metadata = _find_analyzer_column(analyzer_table, column.name)
        for tag in column_metadata.classification_tags:
            definition = analyzer_metadata.classification_definitions.get(tag, ClassificationDefinition())
            classification_name = definition.classification_name or _parse_tag_fqn(tag)[0]
            raw_rows.append(("Column", column.name, tag, classification_name))

    deduped = _deduplicate_tags(raw_rows)

    layer_overrides = _TARGET_SCHEMA_LAYER_OVERRIDES.get(TARGET_SCHEMA.upper(), {})
    if layer_overrides:
        deduped = [
            (scope, col, layer_overrides[classification] if scope == "Table" and classification in layer_overrides else fqn, classification)
            for scope, col, fqn, classification in deduped
        ]
        for cls, override_fqn in layer_overrides.items():
            if not any(c == cls for _, _, _, c in deduped):
                deduped.insert(0, ("Table", "", override_fqn, cls))

    if not deduped:
        lines.append("|  |  |  |  |")
    else:
        for scope, column, tag_fqn, classification in deduped:
            lines.append(
                f"| {scope} | {_markdown_escape(column)} | {_markdown_escape(tag_fqn)} | {_markdown_escape(classification)} |"
            )

    lines.append("")
    lines.append("---")
    lines.append("")


def _append_glossary_section(
    lines: list[str],
    table: TableDef,
    analyzer_table: AnalyzerTableMetadata,
    analyzer_metadata: AnalyzerMetadata,
) -> None:
    lines.append("## 6. Glossary Terms")
    lines.append("Definitions are included only when they are present in the analyzer JSON.")
    lines.append("")
    lines.append("| Scope | Column | Term FQN | Term Name | Definition |")
    lines.append("|-------|--------|----------|-----------|------------|")

    section_has_rows = False
    for term in analyzer_table.glossary_terms:
        definition = analyzer_metadata.glossary_definitions.get(term, GlossaryDefinition(term_name=_parse_tag_fqn(term)[1]))
        lines.append(
            f"| Table |  | {_markdown_escape(term)} | {_markdown_escape(definition.term_name)} | {_markdown_escape(definition.definition)} |"
        )
        section_has_rows = True

    for column in table.columns:
        column_metadata = _find_analyzer_column(analyzer_table, column.name)
        for term in column_metadata.glossary_terms:
            definition = analyzer_metadata.glossary_definitions.get(term, GlossaryDefinition(term_name=_parse_tag_fqn(term)[1]))
            lines.append(
                f"| Column | {_markdown_escape(column.name)} | {_markdown_escape(term)} | {_markdown_escape(definition.term_name)} | {_markdown_escape(definition.definition)} |"
            )
            section_has_rows = True

    if not section_has_rows:
        lines.append("|  |  |  |  |  |")

    lines.append("")
    lines.append("---")
    lines.append("")


def render_stm(
    doc: dict[str, str],
    table: TableDef,
    model_path: Path,
    analyzer_metadata: AnalyzerMetadata,
    *,
    author: str,
    generated_version: str = "1.0",
) -> str:
    project_name = doc.get("title", "").replace(" Data Model", "").strip()
    system_module = _infer_system_module(project_name)
    table_type = table.meta.get("Type", "")
    description = table.meta.get("Description", "")
    primary_key = table.meta.get("Primary Key", "")
    grain = table.meta.get("Grain", "")
    scd_type = table.meta.get("SCD Type", "")
    grain_pk = " / ".join(part for part in (grain, primary_key) if part)
    generated_date = doc.get("generated", "")
    today = datetime.now(UTC).date().isoformat()
    analyzer_table = _find_analyzer_table(table.name, analyzer_metadata)

    lines: list[str] = []
    lines.append("## 1. Document Information")
    lines.append("| Field | Description |")
    lines.append("|-------|-------------|")
    lines.append(f"| **Project Name** | {_markdown_escape(project_name)} |")
    lines.append(f"| **System / Module** | {_markdown_escape(system_module)} |")
    lines.append(f"| **STM Version** | {_markdown_escape(generated_version)} |")
    lines.append(f"| **Author** | {_markdown_escape(author)} |")
    lines.append(f"| **Date Created** | {_markdown_escape(generated_date)} |")
    lines.append("| **Last Updated** |  |")
    lines.append("| **Approved By** |  |")
    lines.append("")
    lines.append("---")
    lines.append("")
    lines.append("## 2. Business Context")
    lines.append("**Purpose / Use Case:**  ")
    lines.append(f"> {_markdown_escape(description)}")
    lines.append("")
    lines.append("**Stakeholders:**  ")
    lines.append("- **Business Owner(s):**  ")
    lines.append("- **Technical Owner(s):**  ")
    lines.append("- **Data Consumer(s):**  ")
    lines.append("")
    lines.append("**Dependencies / Related Documentation:**  ")
    lines.append("- Requirements Document:  ")
    lines.append(f"- ERD / Data Model:  {_markdown_escape(model_path.name)}  ")
    lines.append(f"- Analyzer Schema JSON:  {_markdown_escape(analyzer_metadata.source_path.name)}  ")
    lines.append("- Job Orchestration Diagram:  ")
    lines.append("")
    lines.append("---")
    lines.append("")
    lines.append("## 3. Source System Inventory")
    lines.append("| Source System | Database / Schema | Table / File | Frequency | Owner | Notes |")
    lines.append("|---------------|-------------------|--------------|-----------|-------|-------|")
    lines.append(
        f"| {_markdown_escape(SOURCE_SYSTEM)} | {_markdown_escape(SOURCE_DATABASE_SCHEMA)} | "
        f"{_markdown_escape(SOURCE_TABLE_FILE)} |  |  | {_markdown_escape(SOURCE_NOTES)} |"
    )
    lines.append("")
    lines.append("---")
    lines.append("")
    lines.append("## 4. Target Schema Definition")
    lines.append("| Target Database | Schema | Table Name | SCD Type | Grain / Primary Key | Distribution | Table Type | Notes |")
    lines.append("|-----------------|--------|------------|----------|----------------------|-------------|------------|-------|")
    lines.append(
        f"| {_markdown_escape(TARGET_DATABASE)} | {_markdown_escape(TARGET_SCHEMA)} | {_markdown_escape(table.name)} | {_markdown_escape(scd_type)} | "
        f"{_markdown_escape(grain_pk)} |  | {_markdown_escape(table_type)} | {_markdown_escape(description)} |"
    )
    lines.append("")
    lines.append("---")
    lines.append("")
    _append_classification_section(lines, table, analyzer_table, analyzer_metadata)
    _append_glossary_section(lines, table, analyzer_table, analyzer_metadata)
    lines.append("## 7. Field-Level Mapping Matrix")
    lines.append("| Target Table | Target Column | Data Type | Field Type | Source System | Source Table | Source Column(s) | Transformation / Business Rule | Nullable? | Default / Fallback | Description |")
    lines.append("|--------------|---------------|-----------|------------|---------------|--------------|------------------|--------------------------------|-----------|--------------------|-------------|")
    for column in table.columns:
        lines.append(
            f"| {_markdown_escape(table.name)} | {_markdown_escape(column.name)} | {_markdown_escape(column.data_type)} | "
            f"{_markdown_escape(_field_type(column, table))} | {_markdown_escape(SOURCE_SYSTEM)} |  |  | {_markdown_escape(_column_transformation_logic(column, table))} | "
            f"{_markdown_escape(column.nullable)} |  | {_markdown_escape(column.description)} |"
        )
    lines.append("")
    lines.append("---")
    lines.append("")
    lines.append("## 8. Load Strategy")
    lines.append("| Load Type | Method | Frequency | Dependencies | Error Handling / Recovery | Orchestration Tool |")
    lines.append("|-----------|--------|-----------|--------------|---------------------------|--------------------|")
    lines.append("|  |  |  |  |  |  |")
    lines.append("")
    lines.append("---")
    lines.append("")
    lines.append("## 9. Version Control & Governance")
    lines.append("| Version | Date | Author | Changes | Approved By |")
    lines.append("|---------|------|--------|---------|-------------|")
    lines.append(
        f"| {_markdown_escape(generated_version)} | {_markdown_escape(today)} | "
        f"{_markdown_escape(author)} | Initial generation from target data model and analyzer schema JSON |  |"
    )
    lines.append("")
    lines.append("---")
    lines.append("")
    lines.append("## 10. Sign-Off")
    lines.append("- **Business Owner Approval:** _____________________  ")
    lines.append("- **Data Engineering Lead Approval:** _____________________  ")
    lines.append("- **QA / Testing Approval:** _____________________  ")
    lines.append("")
    return "\n".join(lines)


def _has_glossary_terms(analyzer_metadata: AnalyzerMetadata) -> bool:
    for table in analyzer_metadata.tables.values():
        if table.glossary_terms:
            return True
        for column in table.columns.values():
            if column.glossary_terms:
                return True
    return False


def render_index(model_path: Path, analyzer_path: Path, output_dir: Path, tables: list[TableDef]) -> str:
    lines = [
        "# STM Output Index",
        "",
        f"- Source model: `{model_path}`",
        f"- Analyzer schema: `{analyzer_path}`",
        f"- Generated at: `{datetime.now(UTC).isoformat()}`",
        f"- Output directory: `{output_dir}`",
        "",
        "## Generated STM Documents",
        "",
        "| Order | Target Table | File |",
        "|-------|--------------|------|",
    ]
    for idx, table in enumerate(tables, start=1):
        lines.append(f"| {idx} | {table.name} | `{_slug_index_name(idx, table.name)}` |")
    lines.append("")
    return "\n".join(lines)


def resolve_input_path(explicit_input: str | None) -> Path:
    if explicit_input:
        path = Path(explicit_input).expanduser()
        if not path.exists():
            raise FileNotFoundError(f"Input markdown file not found: {path}")
        return path

    if not DEFAULT_INPUT_DIR.exists():
        raise FileNotFoundError(f"Default input directory not found: {DEFAULT_INPUT_DIR}")
    candidates = sorted(DEFAULT_INPUT_DIR.glob("*.md"))
    if not candidates:
        raise FileNotFoundError(f"No markdown files found in {DEFAULT_INPUT_DIR}")
    if len(candidates) > 1:
        names = ", ".join(path.name for path in candidates)
        raise ValueError(f"Multiple markdown files found in {DEFAULT_INPUT_DIR}; use --input explicitly. Found: {names}")
    return candidates[0]


def resolve_analyzer_path(explicit_analyzer_path: str | None) -> Path:
    if explicit_analyzer_path:
        path = Path(explicit_analyzer_path).expanduser()
        if not path.exists():
            raise FileNotFoundError(f"Analyzer JSON file not found: {path}")
        return path

    if not DEFAULT_INPUT_DIR.exists():
        raise FileNotFoundError(f"Default input directory not found: {DEFAULT_INPUT_DIR}")
    candidates = sorted(DEFAULT_INPUT_DIR.glob("*.json"))
    if not candidates:
        raise FileNotFoundError(
            f"No analyzer JSON files found in {DEFAULT_INPUT_DIR}; use --analyzer-json explicitly."
        )
    if len(candidates) > 1:
        names = ", ".join(path.name for path in candidates)
        raise ValueError(
            f"Multiple analyzer JSON files found in {DEFAULT_INPUT_DIR}; use --analyzer-json explicitly. Found: {names}"
        )
    return candidates[0]


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Generate STM markdown documents from a target data-model markdown file and analyzer schema JSON."
    )
    parser.add_argument("--input", default=None, help="Path to the input markdown model file.")
    parser.add_argument("--analyzer-json", default=None, help="Path to the analyzer schema JSON file.")
    parser.add_argument("--output-dir", default=str(DEFAULT_OUTPUT_DIR), help="Directory where STM markdown files will be written.")
    parser.add_argument(
        "--author",
        default=None,
        help="Author name to write into generated STM documents when the input model does not provide one.",
    )
    args = parser.parse_args()

    input_path = resolve_input_path(args.input)
    analyzer_path = resolve_analyzer_path(args.analyzer_json)
    output_dir = Path(args.output_dir).expanduser()
    output_dir.mkdir(parents=True, exist_ok=True)
    doc, tables = parse_model_markdown(input_path.read_text(encoding="utf-8"))
    if not tables:
        raise ValueError(f"No table sections detected in {input_path}")
    analyzer_metadata = load_analyzer_metadata(analyzer_path)
    if _has_glossary_terms(analyzer_metadata):
        try:
            analyzer_metadata.glossary_definitions.update(fetch_openmetadata_glossary_definitions())
        except RuntimeError:
            print("OpenMetadata unavailable — using glossary definitions from analyzer JSON only.")
    author = str(doc.get("author") or args.author or getpass.getuser()).strip()

    for idx, table in enumerate(tables, start=1):
        output_path = output_dir / _slug_index_name(idx, table.name)
        output_path.write_text(
            render_stm(doc, table, input_path, analyzer_metadata, author=author),
            encoding="utf-8",
        )

    readme_path = output_dir / "README.md"
    readme_path.write_text(render_index(input_path, analyzer_path, output_dir, tables), encoding="utf-8")
    print(f"Generated {len(tables)} STM files in {output_dir}")


if __name__ == "__main__":
    main()
