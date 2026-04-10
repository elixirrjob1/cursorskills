#!/usr/bin/env python3
from __future__ import annotations

import argparse
import getpass
import json
import re
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from typing import Any


_PROJECT_ROOT = Path(__file__).resolve().parents[3]
DEFAULT_INPUT_DIR = _PROJECT_ROOT / "stm" / "input"
DEFAULT_OUTPUT_DIR = _PROJECT_ROOT / "stm" / "output"


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
    desc = column.description
    lower = desc.lower()
    normalized = desc.replace(" ", "").lower()
    if "calculated billable amount" in lower and "billablehours*billrate" in normalized:
        return "BillableHours * BillRate"
    if "calculated cost amount" in lower and "hoursworked*costrate" in normalized:
        return "HoursWorked * CostRate"
    if "calculated margin" in lower and "billedamount-costamount" in normalized:
        return "BilledAmount - CostAmount"
    if "calculated" in lower and "(" in desc and ")" in desc:
        match = re.search(r"\(([^)]+)\)", desc)
        return _normalize_formula(match.group(1)) if match else ""
    if "scd type 2 row effective start date" in lower:
        return "Populate when a new SCD Type 2 version becomes effective."
    if "scd type 2 row expiration date" in lower:
        return "Populate with the end date of the current SCD Type 2 version."
    if "scd type 2 current row flag" in lower:
        return "Set to indicate whether the row is the current SCD Type 2 version."
    if "degenerate dimension" in lower:
        return "Carry forward the source transaction identifier without a separate dimension lookup."
    if "foreign key to" in lower:
        return "Lookup and populate the referenced target dimension key."
    for business_rule in table.business_rules:
        rule = business_rule.strip().removeprefix("-").strip()
        if rule.startswith(f"{column.name} ="):
            return rule.split("=", 1)[1].strip()
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
    return str(value or "").replace("|", "\\|")


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


def _append_classification_section(
    lines: list[str],
    table: TableDef,
    analyzer_table: AnalyzerTableMetadata,
    analyzer_metadata: AnalyzerMetadata,
) -> None:
    lines.append("## 5. Classification Tags")
    lines.append("| Scope | Column | Tag FQN | Classification |")
    lines.append("|-------|--------|---------|----------------|")

    section_has_rows = False
    for tag in analyzer_table.classification_tags:
        definition = analyzer_metadata.classification_definitions.get(tag, ClassificationDefinition())
        classification_name = definition.classification_name or _parse_tag_fqn(tag)[0]
        lines.append(
            f"| Table |  | {_markdown_escape(tag)} | {_markdown_escape(classification_name)} |"
        )
        section_has_rows = True

    for column in table.columns:
        column_metadata = _find_analyzer_column(analyzer_table, column.name)
        for tag in column_metadata.classification_tags:
            definition = analyzer_metadata.classification_definitions.get(tag, ClassificationDefinition())
            classification_name = definition.classification_name or _parse_tag_fqn(tag)[0]
            lines.append(
                f"| Column | {_markdown_escape(column.name)} | {_markdown_escape(tag)} | {_markdown_escape(classification_name)} |"
            )
            section_has_rows = True

    if not section_has_rows:
        lines.append("|  |  |  |  |")

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
    rules = _table_rule_rows(table)
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
    lines.append("|  |  |  |  |  |  |")
    lines.append("")
    lines.append("---")
    lines.append("")
    lines.append("## 4. Target Schema Definition")
    lines.append("| Target Database | Schema | Table Name | SCD Type | Grain / Primary Key | Distribution | Table Type | Notes |")
    lines.append("|-----------------|--------|------------|----------|----------------------|-------------|------------|-------|")
    lines.append(
        f"|  |  | {_markdown_escape(table.name)} | {_markdown_escape(scd_type)} | "
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
            f"{_markdown_escape(_field_type(column, table))} |  |  |  | {_markdown_escape(_column_transformation_logic(column, table))} | "
            f"{_markdown_escape(column.nullable)} |  | {_markdown_escape(column.description)} |"
        )
    lines.append("")
    lines.append("---")
    lines.append("")
    lines.append("## 8. Transformation & Business Rules")
    lines.append("| Rule ID | Name | Description | Example / Formula | Notes |")
    lines.append("|---------|------|-------------|-------------------|-------|")
    if rules:
        for rule_id, name, desc, formula in rules:
            lines.append(
                f"| {_markdown_escape(rule_id)} | {_markdown_escape(name)} | {_markdown_escape(desc)} | {_markdown_escape(formula)} |  |"
            )
    else:
        lines.append("|  |  |  |  |  |")
    lines.append("")
    lines.append("---")
    lines.append("")
    lines.append("## 9. Data Quality & Validation Rules")
    lines.append("| Rule ID | Description | Check Type | Threshold / Condition | Action on Failure | Owner |")
    lines.append("|---------|-------------|------------|-----------------------|-------------------|-------|")
    lines.append("|  |  |  |  |  |  |")
    lines.append("")
    lines.append("---")
    lines.append("")
    lines.append("## 10. Load Strategy")
    lines.append("| Load Type | Method | Frequency | Dependencies | Error Handling / Recovery | Orchestration Tool |")
    lines.append("|-----------|--------|-----------|--------------|---------------------------|--------------------|")
    lines.append("|  |  |  |  |  |  |")
    lines.append("")
    lines.append("---")
    lines.append("")
    lines.append("## 11. Version Control & Governance")
    lines.append("| Version | Date | Author | Changes | Approved By |")
    lines.append("|---------|------|--------|---------|-------------|")
    lines.append(
        f"| {_markdown_escape(generated_version)} | {_markdown_escape(today)} | "
        f"{_markdown_escape(author)} | Initial generation from target data model and analyzer schema JSON |  |"
    )
    lines.append("")
    lines.append("---")
    lines.append("")
    lines.append("## 12. Sign-Off")
    lines.append("- **Business Owner Approval:** _____________________  ")
    lines.append("- **Data Engineering Lead Approval:** _____________________  ")
    lines.append("- **QA / Testing Approval:** _____________________  ")
    lines.append("")
    return "\n".join(lines)


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
