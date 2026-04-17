#!/usr/bin/env python3
"""Generate dbt scaffolding (sources, schema YAML) from STM markdown files.

Produces:
  - _sources.yml  — source definitions for {{ source() }} refs
  - _schema.yml   — model entries for both the view (_v) and incremental models

Enriched model SQL is NOT generated here — that is handled by parallel
subagents that can reason about transformation logic in natural language.
"""

import argparse
import os
import re
import sys
from pathlib import Path
from dataclasses import dataclass, field


# ---------------------------------------------------------------------------
# Markdown table parser
# ---------------------------------------------------------------------------

_ESCAPED_PIPE_SENTINEL = "\x00ESCPIPE\x00"


def _split_md_row(line: str) -> list[str]:
    """Split a markdown table row on '|', honouring '\\|' escape sequences inside cells."""
    protected = line.replace("\\|", _ESCAPED_PIPE_SENTINEL)
    parts = [p.strip().replace(_ESCAPED_PIPE_SENTINEL, "|") for p in protected.strip("|").split("|")]
    return parts


def parse_md_table(text: str) -> list[dict[str, str]]:
    lines = [l.strip() for l in text.strip().splitlines() if l.strip()]
    if len(lines) < 2:
        return []
    header_line = lines[0]
    headers = [h.strip("*") for h in _split_md_row(header_line)]
    rows = []
    for line in lines[2:]:
        if set(line.replace("|", "").replace("-", "").strip()) <= {""}:
            continue
        vals = _split_md_row(line)
        row = {}
        for i, h in enumerate(headers):
            row[h] = vals[i] if i < len(vals) else ""
        rows.append(row)
    return rows


# ---------------------------------------------------------------------------
# STM parser
# ---------------------------------------------------------------------------

@dataclass
class ColumnMapping:
    target_table: str = ""
    target_column: str = ""
    data_type: str = ""
    field_type: str = ""
    source_table: str = ""
    source_columns: str = ""
    transformation: str = ""
    nullable: str = ""
    default: str = ""
    description: str = ""


@dataclass
class STM:
    filename: str = ""
    filepath: str = ""
    target_database: str = ""
    target_schema: str = ""
    target_table: str = ""
    scd_type: str = ""
    table_type: str = ""
    grain: str = ""
    purpose: str = ""
    source_tables: list = field(default_factory=list)
    source_database_schema: str = ""
    columns: list = field(default_factory=list)
    load_type: str = ""
    load_method: str = ""


_BOGUS_TABLE_PATTERNS = re.compile(
    r"(?i)(see field|no matching|n/a|none|tbd|not applicable|—|--|unknown)"
)


def _is_real_table(name: str) -> bool:
    return bool(name) and not _BOGUS_TABLE_PATTERNS.search(name) and " " not in name


def extract_section(md: str, section_num: int) -> str:
    pattern = rf"##\s*{section_num}\.\s.*?\n(.*?)(?=\n##\s*\d+\.|$)"
    m = re.search(pattern, md, re.DOTALL)
    return m.group(1).strip() if m else ""


def parse_stm(filepath: str) -> STM:
    md = Path(filepath).read_text(encoding="utf-8")
    stm = STM(filename=os.path.basename(filepath), filepath=filepath)

    s2 = extract_section(md, 2)
    purpose_m = re.search(r">\s*(.+)", s2)
    if purpose_m:
        stm.purpose = purpose_m.group(1).strip()

    s3 = extract_section(md, 3)
    src_rows = parse_md_table(s3)
    seen = set()
    for r in src_rows:
        tbl = r.get("Table / File", "").strip()
        db_schema = r.get("Database / Schema", "").strip()
        if tbl and tbl not in seen and _is_real_table(tbl):
            stm.source_tables.append(tbl)
            seen.add(tbl)
        if db_schema:
            stm.source_database_schema = db_schema

    s4 = extract_section(md, 4)
    tgt_rows = parse_md_table(s4)
    if tgt_rows:
        r = tgt_rows[0]
        stm.target_database = r.get("Target Database", "").strip()
        stm.target_schema = r.get("Schema", "").strip()
        stm.target_table = r.get("Table Name", "").strip()
        stm.scd_type = r.get("SCD Type", "").strip()
        stm.grain = r.get("Grain / Primary Key", "").strip()
        stm.table_type = r.get("Table Type", "").strip()

    s7 = extract_section(md, 7)
    col_rows = parse_md_table(s7)
    for r in col_rows:
        stm.columns.append(ColumnMapping(
            target_table=r.get("Target Table", "").strip(),
            target_column=r.get("Target Column", "").strip(),
            data_type=r.get("Data Type", "").strip(),
            field_type=r.get("Field Type", "").strip(),
            source_table=r.get("Source Table", "").strip(),
            source_columns=r.get("Source Column(s)", "").strip(),
            transformation=r.get("Transformation / Business Rule", "").strip(),
            nullable=r.get("Nullable?", "").strip(),
            default=r.get("Default / Fallback", "").strip(),
            description=r.get("Description", "").strip(),
        ))

    s8 = extract_section(md, 8)
    load_rows = parse_md_table(s8)
    if load_rows:
        stm.load_type = load_rows[0].get("Load Type", "").strip()
        stm.load_method = load_rows[0].get("Method", "").strip()

    return stm


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def to_snake(name: str) -> str:
    s1 = re.sub(r"([A-Z]+)([A-Z][a-z])", r"\1_\2", name)
    return re.sub(r"([a-z0-9])([A-Z])", r"\1_\2", s1).lower()


# ---------------------------------------------------------------------------
# Generators — scaffolding only
# ---------------------------------------------------------------------------

def generate_sources_yml(stms: list[STM]) -> str:
    db_schema_tables: dict[str, set[str]] = {}
    db_name = ""
    for stm in stms:
        key = stm.source_database_schema
        if key not in db_schema_tables:
            db_schema_tables[key] = set()
        for t in stm.source_tables:
            db_schema_tables[key].add(t)
        if stm.target_database:
            db_name = stm.target_database

    lines = ["version: 2", "", "sources:"]
    for db_schema, tables in db_schema_tables.items():
        parts = db_schema.split(".")
        schema_name = parts[-1] if len(parts) > 1 else parts[0]
        database = parts[0] if len(parts) > 1 else db_name
        real_tables = sorted(t for t in tables if _is_real_table(t))
        if not real_tables:
            continue
        lines.append(f"  - name: {schema_name.lower()}")
        if database:
            lines.append(f"    database: {database}")
        lines.append(f"    schema: {schema_name}")
        lines.append("    tables:")
        for t in real_tables:
            lines.append(f"      - name: {t}")
    lines.append("")
    return "\n".join(lines)


# ---------------------------------------------------------------------------
# File writers
# ---------------------------------------------------------------------------

def write_file(path: str, content: str):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    Path(path).write_text(content, encoding="utf-8")
    print(f"  wrote {path}")


def _view_model_name(stm: "STM") -> str:
    """Generate view model name: vw_<Entity> with Entity verbatim from STM (PascalCase)."""
    return f"vw_{stm.target_table}"


def _yaml_quote(value: str) -> str:
    """Escape a string for use inside a YAML double-quoted scalar."""
    return value.replace("\\", "\\\\").replace("\"", "\\\"")


def generate_view_schema_yml(stms: list[STM]) -> str:
    """Generate _schema.yml for the view models only.

    Both model names (vw_<Entity>) and column names are preserved verbatim from the STM (PascalCase).
    """
    lines = ["version: 2", "", "models:"]
    for stm in stms:
        view_name = _view_model_name(stm)
        lines.append(f"  - name: {view_name}")
        view_desc = f"View layer for {stm.target_table} — transformation logic from bronze source."
        if stm.purpose:
            view_desc += f" {stm.purpose}"
        lines.append(f"    description: \"{_yaml_quote(view_desc)}\"")
        lines.append("    columns:")
        for col in stm.columns:
            col_name = col.target_column
            lines.append(f"      - name: {col_name}")
            if col.description:
                lines.append(f"        description: \"{_yaml_quote(col.description)}\"")
            tests = []
            if col.field_type == "Primary Key":
                tests.extend(["unique", "not_null"])
            elif col.nullable.upper() == "NO" and col.field_type != "Primary Key":
                tests.append("not_null")
            if tests:
                lines.append("        tests:")
                for t in tests:
                    lines.append(f"          - {t}")
    lines.append("")
    return "\n".join(lines)


def generate_enriched_schema_yml(stms: list[STM]) -> str:
    """Generate _schema.yml for the incremental models only.

    Column names AND model names are preserved verbatim from the STM (PascalCase).
    """
    lines = ["version: 2", "", "models:"]
    for stm in stms:
        model_name = stm.target_table
        lines.append(f"  - name: {model_name}")
        base_desc = stm.purpose or f"{stm.target_table} dimension/fact table."
        view_name = _view_model_name(stm)
        base_desc += f" Incrementally materialized from {view_name}."
        lines.append(f"    description: \"{_yaml_quote(base_desc)}\"")
        lines.append("    columns:")
        for col in stm.columns:
            col_name = col.target_column
            lines.append(f"      - name: {col_name}")
            if col.description:
                lines.append(f"        description: \"{_yaml_quote(col.description)}\"")
            tests = []
            if col.field_type == "Primary Key":
                tests.extend(["unique", "not_null"])
            elif col.nullable.upper() == "NO" and col.field_type != "Primary Key":
                tests.append("not_null")
            if tests:
                lines.append("        tests:")
                for t in tests:
                    lines.append(f"          - {t}")
    lines.append("")
    return "\n".join(lines)


def run(stm_paths: list[str], dbt_project: str):
    stms = [parse_stm(p) for p in stm_paths]

    staging_dir = os.path.join(dbt_project, "models", "staging")
    views_dir = os.path.join(dbt_project, "models", "views")
    enriched_dir = os.path.join(dbt_project, "models", "enriched")

    sources_yml = generate_sources_yml(stms)
    write_file(os.path.join(staging_dir, "_sources.yml"), sources_yml)

    view_schema_yml = generate_view_schema_yml(stms)
    write_file(os.path.join(views_dir, "_schema.yml"), view_schema_yml)

    enriched_schema_yml = generate_enriched_schema_yml(stms)
    write_file(os.path.join(enriched_dir, "_schema.yml"), enriched_schema_yml)

    print(f"\nScaffolding done for {len(stms)} STM(s).")
    print(f"  Sources:        {staging_dir}/_sources.yml")
    print(f"  View schema:    {views_dir}/_schema.yml")
    print(f"  Enriched schema: {enriched_dir}/_schema.yml")
    print(f"\nModel SQL NOT generated — use subagents next.")
    print("Each STM produces TWO models: a vw_<Entity> view (models/views/) + an incremental table (models/enriched/) — both PascalCase from STM Target Table.")

    print("\n--- STM summary for subagent dispatch ---")
    for stm in stms:
        if not stm.target_table:
            continue
        model_name = stm.target_table
        view_name = _view_model_name(stm)
        src_refs = ", ".join(stm.source_tables)
        print(f"  views/{view_name} + enriched/{model_name} <- sources: [{src_refs}] (STM: {stm.filepath})")


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(description="Generate dbt scaffolding from STM markdown files")
    parser.add_argument("--stm", help="Single STM markdown file")
    parser.add_argument("--stm-dir", help="Directory containing STM markdown files")
    parser.add_argument("--dbt-project", required=True, help="Path to dbt project root")
    args = parser.parse_args()

    paths = []
    if args.stm:
        paths = [args.stm]
    elif args.stm_dir:
        paths = sorted(
            str(p) for p in Path(args.stm_dir).glob("*-stm.md")
        )
    else:
        print("Error: provide --stm or --stm-dir", file=sys.stderr)
        sys.exit(1)

    if not paths:
        print("Error: no STM files found", file=sys.stderr)
        sys.exit(1)

    print(f"Processing {len(paths)} STM file(s)...")
    run(paths, args.dbt_project)


if __name__ == "__main__":
    main()
