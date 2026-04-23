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

import re

from dotenv import load_dotenv

load_dotenv()

_BOGUS_TABLE = re.compile(
    r"(?i)^(n/a|none|tbd|not applicable|unknown|—|--|see field|no matching)\s*$"
)

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
        if _BOGUS_TABLE.match(src_table_raw):
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

_TARGET_SCHEMA_LAYER_OVERRIDES: dict[str, dict[str, str]] = {
    "GOLD": {"Architecture": "Architecture.Enriched", "Certification": "Certification.Gold"},
    "SILVER": {"Architecture": "Architecture.Enriched", "Certification": "Certification.Silver"},
    "ENRICHED": {"Architecture": "Architecture.Enriched", "Certification": "Certification.Gold"},
    "CURATED": {"Architecture": "Architecture.Curated", "Certification": "Certification.Gold"},
    "BRONZE": {"Architecture": "Architecture.Raw", "Certification": "Certification.Bronze"},
    "RAW": {"Architecture": "Architecture.Raw", "Certification": "Certification.Bronze"},
}

_TAG_TIER = {
    "Architecture.Enriched": 3, "Architecture.Curated": 2, "Architecture.Raw": 1,
    "Certification.Gold": 3, "Certification.Silver": 2, "Certification.Bronze": 1,
    "PII.Sensitive": 3, "PII.NonSensitive": 2, "PII.None": 1,
    "Tier.Tier1": 3, "Tier.Tier2": 2, "Tier.Tier3": 1,
}


def _deduplicate_classification_tags(rows: list[dict[str, str]]) -> list[dict[str, str]]:
    """Deduplicate table-level tags: one tag per classification, highest tier wins.

    Column-level tags are deduplicated per (column, classification).
    """
    table_best: dict[str, dict[str, str]] = {}
    col_best: dict[tuple[str, str], dict[str, str]] = {}
    col_order: list[tuple[str, str]] = []
    table_order: list[str] = []

    for row in rows:
        cls = row["classification"]
        fqn = row["tag_fqn"]
        tier = _TAG_TIER.get(fqn, 0)

        if row["scope"] == "Table":
            existing = table_best.get(cls)
            if existing is None:
                table_best[cls] = row
                table_order.append(cls)
            else:
                existing_tier = _TAG_TIER.get(existing["tag_fqn"], 0)
                if tier > existing_tier:
                    table_best[cls] = row
        else:
            key = (row["column"], cls)
            existing = col_best.get(key)
            if existing is None:
                col_best[key] = row
                col_order.append(key)
            else:
                existing_tier = _TAG_TIER.get(existing["tag_fqn"], 0)
                if tier > existing_tier:
                    col_best[key] = row

    deduped: list[dict[str, str]] = []
    seen_table: set[str] = set()
    for cls in table_order:
        if cls not in seen_table:
            deduped.append(table_best[cls])
            seen_table.add(cls)
    seen_col: set[tuple[str, str]] = set()
    for key in col_order:
        if key not in seen_col:
            deduped.append(col_best[key])
            seen_col.add(key)
    return deduped


def _extract_target_schema(stm_path: Path) -> str:
    """Read the target schema from Section 4 of the STM."""
    lines = stm_path.read_text().splitlines()
    rows = _parse_table_rows(lines, "## 4.")
    if len(rows) < 2:
        return ""
    header = [h.lower() for h in rows[0]]
    try:
        idx = header.index("schema")
    except ValueError:
        return ""
    cells = rows[1]
    return cells[idx].strip().upper() if idx < len(cells) else ""


def _parse_all_tables_in_section(lines: list[str], section_prefix: str) -> list[list[list[str]]]:
    """Return every markdown table (as rows of cells) found inside *section_prefix*.

    Unlike ``_parse_table_rows`` (which stops at the first table), this walks
    the whole section and collects every pipe-table it finds. Tables are
    separated from each other by any non-pipe line (heading, blank, prose).
    """
    in_section = False
    current: list[list[str]] = []
    out: list[list[list[str]]] = []

    for line in lines:
        stripped = line.strip()
        if stripped.startswith(section_prefix):
            in_section = True
            continue
        if in_section and stripped.startswith("## "):
            break
        if not in_section:
            continue
        if stripped.startswith("|"):
            cells = [c.strip() for c in stripped.split("|")[1:-1]]
            if all(set(c) <= {"-", " ", ":"} for c in cells):
                continue
            current.append(cells)
        elif current:
            out.append(current)
            current = []
    if current:
        out.append(current)
    return out


def _pick_column_mapping_table(tables: list[list[list[str]]]) -> list[list[str]]:
    """Pick the Section 7 table whose header has both ``target column`` and
    ``description`` — that's the Field-Level Mapping (or the "Final" sub-table
    in Type-2 STMs). Falls back to the first table if nothing matches."""
    for tbl in tables:
        if not tbl:
            continue
        hdr = " ".join(c.lower() for c in tbl[0])
        if "target column" in hdr and "description" in hdr:
            return tbl
    return tables[0] if tables else []


def _collect_unclassified_dbt_columns(
    stm_path: Path,
    existing_col_classification_keys: set[tuple[str, str]],
    source_mapped_columns: set[str],
) -> list[dict[str, str]]:
    """Return the Section 7 rows whose target column has NO column-scoped
    classification tag in Section 5 and is NOT mapped to a bronze source
    column. These are the dbt-invented columns that need LLM-driven
    classification against the OpenMetadata vocabulary."""
    lines = stm_path.read_text().splitlines()
    tables = _parse_all_tables_in_section(lines, "## 7.")
    tbl = _pick_column_mapping_table(tables)
    if len(tbl) < 2:
        return []

    hdr_lower = [c.lower() for c in tbl[0]]

    def _find(name: str) -> int:
        try:
            return hdr_lower.index(name)
        except ValueError:
            return -1

    idx_col = _find("target column")
    if idx_col < 0:
        return []
    idx_dt = _find("data type")
    idx_ft = _find("field type")
    idx_desc = _find("description")

    already_tagged_cols = {col for col, _cls in existing_col_classification_keys}

    out: list[dict[str, str]] = []
    for cells in tbl[1:]:
        col = cells[idx_col].strip() if idx_col < len(cells) else ""
        if not col:
            continue
        if col.upper() in already_tagged_cols:
            continue
        if col.upper() in {c.upper() for c in source_mapped_columns}:
            continue
        out.append({
            "column": col,
            "data_type": cells[idx_dt].strip() if 0 <= idx_dt < len(cells) else "",
            "field_type": cells[idx_ft].strip() if 0 <= idx_ft < len(cells) else "",
            "description": cells[idx_desc].strip() if 0 <= idx_desc < len(cells) else "",
        })
    return out


def _apply_layer_overrides(
    rows: list[dict[str, str]], target_schema: str,
) -> list[dict[str, str]]:
    """Override layer-derived tags (Architecture, Certification) to match the
    target schema. Applies at both Table and Column scope because the target
    layer is authoritative: a bronze column mapping propagating
    ``Architecture.Raw`` onto an enriched-layer column would violate
    OpenMetadata's mutual-exclusion constraint once the LLM step assigns
    ``Architecture.Enriched``."""
    overrides = _TARGET_SCHEMA_LAYER_OVERRIDES.get(target_schema)
    if not overrides:
        return rows

    result: list[dict[str, str]] = []
    for row in rows:
        if row["classification"] in overrides:
            result.append({**row, "tag_fqn": overrides[row["classification"]]})
        else:
            result.append(row)

    for classification, tag_fqn in overrides.items():
        if not any(
            r["scope"] == "Table" and r["classification"] == classification
            for r in result
        ):
            result.insert(0, {
                "scope": "Table", "column": "", "tag_fqn": tag_fqn, "classification": classification,
            })
    return result


def _process_stm(
    stm_path: Path,
    schema_fqn: str,
    table_cache: dict[str, dict[str, Any]],
    term_cache: dict[str, dict[str, Any]],
) -> dict[str, Any]:
    mappings = _extract_source_mappings(stm_path)
    if not mappings:
        # No bronze propagation possible, but Section 7 target columns still
        # need LLM classification against the OM vocabulary in step 4b.
        unclassified_no_src = _collect_unclassified_dbt_columns(
            stm_path, existing_col_classification_keys=set(), source_mapped_columns=set(),
        )
        return {"file": stm_path.name, "status": "skipped", "reason": "no source mappings",
                "tags": 0, "terms": 0,
                "unclassified_dbt_columns": unclassified_no_src}

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

    all_class = _deduplicate_classification_tags(all_class)
    target_schema = _extract_target_schema(stm_path)
    all_class = _apply_layer_overrides(all_class, target_schema)

    # Identify dbt-invented columns (no bronze match, no column-scoped tag yet).
    # These are emitted to the work file for the LLM classification step (4b).
    tagged_col_keys: set[tuple[str, str]] = {
        (r["column"].upper(), r["classification"])
        for r in all_class
        if r["scope"] == "Column" and r["column"]
    }
    source_mapped_cols = {m["target_column"] for m in mappings}
    unclassified = _collect_unclassified_dbt_columns(
        stm_path, tagged_col_keys, source_mapped_cols,
    )

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
            "unclassified_dbt_columns": unclassified,
            "source_tables": unique_source_tables}


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def _fetch_om_vocabulary() -> dict[str, Any]:
    """Fetch every non-disabled OM classification tag + glossary term so the
    LLM classification subagent can pick from a bounded, authoritative list."""
    try:
        classifications_page = _get("classifications", params={"limit": 200})
    except Exception as exc:
        print(f"    ⚠ could not fetch classifications: {exc}")
        return {"classifications": [], "glossary_terms": []}

    classifications: list[dict[str, Any]] = []
    for cls in classifications_page.get("data", []):
        if cls.get("disabled"):
            continue
        name = cls.get("name") or cls.get("fullyQualifiedName", "")
        try:
            tags_page = _get("tags", params={"parent": name, "limit": 200})
        except Exception as exc:
            print(f"    ⚠ could not fetch tags under {name}: {exc}")
            continue
        options = []
        for tag in tags_page.get("data", []):
            if tag.get("disabled"):
                continue
            options.append({
                "fqn": tag.get("fullyQualifiedName", ""),
                "name": tag.get("name", ""),
                "description": (tag.get("description") or "").strip(),
            })
        classifications.append({
            "name": name,
            "description": (cls.get("description") or "").strip(),
            "mutually_exclusive": cls.get("mutuallyExclusive", False),
            "options": options,
        })

    glossary_terms: list[dict[str, Any]] = []
    try:
        terms_page = _get("glossaryTerms", params={"limit": 500})
    except Exception as exc:
        print(f"    ⚠ could not fetch glossary terms: {exc}")
        terms_page = {"data": []}
    for term in terms_page.get("data", []):
        glossary_terms.append({
            "fqn": term.get("fullyQualifiedName", ""),
            "name": term.get("name", ""),
            "display_name": term.get("displayName", ""),
            "description": (term.get("description") or "").strip(),
        })

    return {"classifications": classifications, "glossary_terms": glossary_terms}


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Post-enrich STMs with classification tags and glossary terms from OpenMetadata",
    )
    parser.add_argument("--schema-fqn", default=DEFAULT_SCHEMA_FQN)
    parser.add_argument("--stm-dir", type=Path, default=DEFAULT_STM_DIR)
    parser.add_argument(
        "--work-file",
        type=Path,
        default=None,
        help="Where to write the LLM classification work file "
             "(default: <stm-dir>/_dbt_column_classification_work.json)",
    )
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
        unclassified_n = len(r.get("unclassified_dbt_columns", []))
        extra = f" ({unclassified_n} unclassified dbt cols)" if unclassified_n else ""
        print(f"    → {r['status']}: {r['tags']} tags, {r['terms']} glossary terms{extra}")

    enriched = sum(1 for r in results if r["status"] == "enriched")
    skipped = len(results) - enriched
    total_tags = sum(r["tags"] for r in results)
    total_terms = sum(r["terms"] for r in results)
    total_unclassified = sum(len(r.get("unclassified_dbt_columns", [])) for r in results)

    print(f"\nDone: {enriched} enriched, {skipped} skipped")
    print(f"Totals: {total_tags} classification tags, {total_terms} glossary terms")
    print(f"API calls: {len(table_cache)} table fetches, {len(term_cache)} glossary term fetches")

    if total_unclassified == 0:
        print("No unclassified dbt-invented columns — skipping work-file emit.")
        return

    import json

    print(f"\nFetching OM vocabulary for {total_unclassified} unclassified dbt columns …")
    vocab = _fetch_om_vocabulary()
    print(
        f"  → {len(vocab['classifications'])} classifications, "
        f"{sum(len(c['options']) for c in vocab['classifications'])} tags, "
        f"{len(vocab['glossary_terms'])} glossary terms"
    )

    work = {
        "schema_fqn": args.schema_fqn,
        "om_vocabulary": vocab,
        "items": [
            {
                "stm_file": str((args.stm_dir / r["file"]).resolve()),
                "target_table": r["file"].replace("-stm.md", ""),
                "unclassified_columns": r["unclassified_dbt_columns"],
            }
            for r in results
            if r.get("unclassified_dbt_columns")
        ],
    }
    work_file = args.work_file or (args.stm_dir / "_dbt_column_classification_work.json")
    work_file.write_text(json.dumps(work, indent=2))
    print(f"\nWork file written: {work_file}")
    print(
        "Next: launch the LLM classification subagent per step 4b of "
        "stm-catalogue-enricher/SKILL.md."
    )


if __name__ == "__main__":
    main()
