#!/usr/bin/env python3
"""Post-enrichment: populate STM Sections 5 (Classification Tags) and 6 (Glossary Terms)
from OpenMetadata source-table metadata.

Run AFTER the subagent enrichment pass has filled Source Table / Source Column(s)
in Section 7.  This script reads each STM, collects the matched source pairs,
fetches tags and glossary assignments from OpenMetadata once per unique source table,
resolves glossary term definitions, and writes the results back into each STM file.
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path
from typing import Any

from dotenv import load_dotenv

load_dotenv()

_SCRIPT_DIR = Path(__file__).resolve().parent
sys.path.insert(0, str(_SCRIPT_DIR))
from enrich_stm_from_catalogue import _get  # noqa: E402  — reuse existing API helpers

_PROJECT_ROOT = Path(__file__).resolve().parents[4]
DEFAULT_SCHEMA_FQN = "snowflake_fivetran.DRIP_DATA_INTELLIGENCE.BRONZE_ERP__DBO"
DEFAULT_STM_DIR = _PROJECT_ROOT / "stm" / "output"


# ---------------------------------------------------------------------------
# OpenMetadata fetch helpers
# ---------------------------------------------------------------------------

def _fetch_table(table_fqn: str) -> dict[str, Any]:
    return _get(f"tables/name/{table_fqn}", params={"fields": "columns,tags"})


def _fetch_glossary_term(term_fqn: str) -> dict[str, Any]:
    return _get(f"glossaryTerms/name/{term_fqn}")


# ---------------------------------------------------------------------------
# STM parsing
# ---------------------------------------------------------------------------

def _parse_table_rows(lines: list[str], section_prefix: str) -> list[list[str]]:
    """Return cell-lists for every data row inside the first markdown table of *section_prefix*."""
    in_section = False
    header_seen = False
    rows: list[list[str]] = []

    for line in lines:
        stripped = line.strip()
        if stripped.startswith(section_prefix):
            in_section = True
            continue
        if in_section and stripped.startswith("## "):
            break
        if not in_section or not stripped.startswith("|"):
            continue

        cells = [c.strip() for c in stripped.split("|")[1:-1]]
        if not header_seen:
            header_seen = True
            rows.append(cells)  # header as first row
            continue
        if all(set(c) <= {"-", " ", ":"} for c in cells):
            continue
        rows.append(cells)

    return rows


def _extract_source_mappings(stm_path: Path) -> list[dict[str, str]]:
    """Return [{target_column, source_table, source_column}, ...] from Section 7."""
    lines = stm_path.read_text().splitlines()
    rows = _parse_table_rows(lines, "## 7.")
    if len(rows) < 2:
        return []

    header = [h.lower() for h in rows[0]]
    try:
        idx_tgt = header.index("target column")
        idx_src_t = header.index("source table")
        idx_src_c = header.index("source column(s)")
    except ValueError:
        return []

    mappings: list[dict[str, str]] = []
    for cells in rows[1:]:
        src_table_raw = cells[idx_src_t].strip() if idx_src_t < len(cells) else ""
        src_col_raw = cells[idx_src_c].strip() if idx_src_c < len(cells) else ""
        tgt_col = cells[idx_tgt].strip() if idx_tgt < len(cells) else ""
        if not src_table_raw or not src_col_raw:
            continue
        # Handle comma-separated multi-table references (e.g. "SALES_ORDER_ITEMS, PRODUCTS")
        for src_table in (t.strip() for t in src_table_raw.split(",")):
            if src_table:
                mappings.append({
                    "target_column": tgt_col,
                    "source_table": src_table,
                    "source_column": src_col_raw,
                })
    return mappings


# ---------------------------------------------------------------------------
# Tag / glossary extraction from a table entity
# ---------------------------------------------------------------------------

def _classification_from_fqn(tag_fqn: str) -> str:
    return tag_fqn.split(".")[0] if "." in tag_fqn else tag_fqn


def _collect_from_table(
    table_entity: dict[str, Any],
    matched_cols_upper: set[str],
) -> tuple[list[dict[str, str]], list[dict[str, str]]]:
    """Return (classification_rows, glossary_refs) for one source table."""
    class_rows: list[dict[str, str]] = []
    glossary_refs: list[dict[str, str]] = []

    for tag in table_entity.get("tags", []):
        fqn = tag.get("tagFQN", "")
        src = tag.get("source", "")
        if src == "Classification":
            class_rows.append({"scope": "Table", "column": "", "tag_fqn": fqn,
                               "classification": _classification_from_fqn(fqn)})
        elif src == "Glossary":
            glossary_refs.append({"scope": "Table", "column": "", "term_fqn": fqn})

    for col in table_entity.get("columns", []):
        col_name = col.get("name", "")
        if col_name.upper() not in matched_cols_upper:
            continue
        for tag in col.get("tags", []):
            fqn = tag.get("tagFQN", "")
            src = tag.get("source", "")
            if src == "Classification":
                class_rows.append({"scope": "Column", "column": col_name,
                                   "tag_fqn": fqn,
                                   "classification": _classification_from_fqn(fqn)})
            elif src == "Glossary":
                glossary_refs.append({"scope": "Column", "column": col_name, "term_fqn": fqn})

    return class_rows, glossary_refs


# ---------------------------------------------------------------------------
# Markdown builders
# ---------------------------------------------------------------------------

_S3_HEADER = "| Source System | Database / Schema | Table / File | Frequency | Owner | Notes |"
_S3_SEP    = "|---------------|-------------------|--------------|-----------|-------|-------|"

_S5_HEADER = "| Scope | Column | Tag FQN | Classification |"
_S5_SEP    = "|-------|--------|---------|----------------|"
_S5_EMPTY  = "|  |  |  |  |"

_S6_HEADER = "| Scope | Column | Term FQN | Term Name | Definition |"
_S6_SEP    = "|-------|--------|----------|-----------|------------|"
_S6_EMPTY  = "|  |  |  |  |  |"


def _build_section3_table(source_tables: list[str], schema_fqn: str) -> str:
    """Build Section 3 with one row per matched source table."""
    parts = schema_fqn.split(".", 1)
    db_schema = parts[1] if len(parts) > 1 else schema_fqn

    lines = [_S3_HEADER, _S3_SEP]
    if not source_tables:
        lines.append(f"| Snowflake | {db_schema} | See field-level mapping |  |  | No source tables matched in catalogue. |")
    else:
        for i, tbl in enumerate(source_tables):
            notes = "Bronze replica via Fivetran." if i == 0 else ""
            lines.append(f"| Snowflake | {db_schema} | {tbl} |  |  | {notes} |")
    return "\n".join(lines)


def _build_section5_table(rows: list[dict[str, str]]) -> str:
    lines = [_S5_HEADER, _S5_SEP]
    if not rows:
        lines.append(_S5_EMPTY)
    else:
        for r in rows:
            lines.append(f"| {r['scope']} | {r['column']} | {r['tag_fqn']} | {r['classification']} |")
    return "\n".join(lines)


def _build_section6_table(rows: list[dict[str, str]]) -> str:
    lines = [_S6_HEADER, _S6_SEP]
    if not rows:
        lines.append(_S6_EMPTY)
    else:
        for r in rows:
            defn = (r.get("definition") or "").replace("\n", " ").replace("|", "—").strip()
            lines.append(
                f"| {r['scope']} | {r['column']} | {r['term_fqn']} | {r['term_name']} | {defn} |"
            )
    return "\n".join(lines)


# ---------------------------------------------------------------------------
# Section replacement in STM text
# ---------------------------------------------------------------------------

def _replace_section_table(text: str, section_prefix: str, new_table: str) -> str:
    """Replace only the markdown table (| lines) inside *section_prefix*, keeping any preamble."""
    lines = text.split("\n")
    out: list[str] = []
    in_section = False
    table_emitted = False

    for line in lines:
        stripped = line.strip()

        if stripped.startswith(section_prefix):
            in_section = True
            table_emitted = False
            out.append(line)
            continue

        if in_section and stripped.startswith("## "):
            in_section = False

        if in_section and stripped.startswith("|"):
            if not table_emitted:
                out.append(new_table)
                table_emitted = True
            continue  # skip ALL old table lines in this section

        out.append(line)

    return "\n".join(out)


# ---------------------------------------------------------------------------
# Per-STM processing
# ---------------------------------------------------------------------------

def _process_stm(
    stm_path: Path,
    schema_fqn: str,
    table_cache: dict[str, dict[str, Any]],
    term_cache: dict[str, dict[str, Any]],
) -> dict[str, Any]:
    mappings = _extract_source_mappings(stm_path)
    if not mappings:
        return {"file": stm_path.name, "status": "skipped", "reason": "no source mappings",
                "tags": 0, "terms": 0}

    table_cols: dict[str, set[str]] = {}
    target_to_source: dict[str, tuple[str, str]] = {}
    for m in mappings:
        table_cols.setdefault(m["source_table"], set()).add(m["source_column"].upper())
        target_to_source[m["target_column"]] = (m["source_table"], m["source_column"])

    all_class: list[dict[str, str]] = []
    all_gloss_refs: list[dict[str, str]] = []

    for src_table, matched_upper in table_cols.items():
        fqn = f"{schema_fqn}.{src_table}"
        if fqn not in table_cache:
            try:
                table_cache[fqn] = _fetch_table(fqn)
            except Exception as exc:
                print(f"    ⚠ Could not fetch {fqn}: {exc}")
                continue

        c_rows, g_refs = _collect_from_table(table_cache[fqn], matched_upper)

        src_to_target = {sc.upper(): tc for tc, (st, sc) in target_to_source.items() if st == src_table}
        for row in c_rows:
            if row["column"]:
                row["column"] = src_to_target.get(row["column"].upper(), row["column"])
            all_class.append(row)
        for ref in g_refs:
            if ref["column"]:
                ref["column"] = src_to_target.get(ref["column"].upper(), ref["column"])
            all_gloss_refs.append(ref)

    glossary_rows: list[dict[str, str]] = []
    for ref in all_gloss_refs:
        tfqn = ref["term_fqn"]
        if tfqn not in term_cache:
            try:
                term_cache[tfqn] = _fetch_glossary_term(tfqn)
            except Exception as exc:
                print(f"    ⚠ Could not fetch glossary term {tfqn}: {exc}")
                term_cache[tfqn] = {}
        term = term_cache[tfqn]
        glossary_rows.append({
            "scope": ref["scope"],
            "column": ref["column"],
            "term_fqn": tfqn,
            "term_name": term.get("displayName") or term.get("name") or tfqn.rsplit(".", 1)[-1],
            "definition": term.get("description", ""),
        })

    unique_source_tables = list(dict.fromkeys(
        m["source_table"] for m in mappings
    ))

    new_s3 = _build_section3_table(unique_source_tables, schema_fqn)
    new_s5 = _build_section5_table(all_class)
    new_s6 = _build_section6_table(glossary_rows)

    text = stm_path.read_text()
    text = _replace_section_table(text, "## 3.", new_s3)
    text = _replace_section_table(text, "## 5.", new_s5)
    text = _replace_section_table(text, "## 6.", new_s6)
    stm_path.write_text(text)

    return {"file": stm_path.name, "status": "enriched",
            "tags": len(all_class), "terms": len(glossary_rows),
            "source_tables": unique_source_tables}


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    parser = argparse.ArgumentParser(
        description="Post-enrich STMs with classification tags and glossary terms from OpenMetadata",
    )
    parser.add_argument("--schema-fqn", default=DEFAULT_SCHEMA_FQN)
    parser.add_argument("--stm-dir", type=Path, default=DEFAULT_STM_DIR)
    args = parser.parse_args()

    stm_files = sorted(args.stm_dir.glob("*-stm.md"))
    if not stm_files:
        print(f"No STM files found in {args.stm_dir}")
        return

    print(f"Post-enriching {len(stm_files)} STMs from {args.schema_fqn}")

    table_cache: dict[str, dict[str, Any]] = {}
    term_cache: dict[str, dict[str, Any]] = {}
    results: list[dict[str, Any]] = []

    for stm in stm_files:
        print(f"  {stm.name} …")
        r = _process_stm(stm, args.schema_fqn, table_cache, term_cache)
        results.append(r)
        print(f"    → {r['status']}: {r['tags']} tags, {r['terms']} glossary terms")

    enriched = sum(1 for r in results if r["status"] == "enriched")
    skipped = len(results) - enriched
    total_tags = sum(r["tags"] for r in results)
    total_terms = sum(r["terms"] for r in results)

    print(f"\nDone: {enriched} enriched, {skipped} skipped")
    print(f"Totals: {total_tags} classification tags, {total_terms} glossary terms")
    print(f"API calls: {len(table_cache)} table fetches, {len(term_cache)} glossary term fetches")


if __name__ == "__main__":
    main()
