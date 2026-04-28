#!/usr/bin/env python3
"""Apply STM markdown content to OpenMetadata tables via direct REST PATCH.

For each STM, parses Sections 2 (description), 5 (tags), 6 (glossary), 7
(column descriptions), fetches the target table with columns+tags, and issues
ONE JSON-patch call that sets descriptions + merges tags/glossary on the
table and each column.

Anything ambiguous (STM column not found in OM, unknown tag FQN, duplicate
label conflict, etc.) is recorded in the report for an agent to handle.

Usage:
  apply_stm_to_openmetadata.py --stm-dir stm/output \
      --service snowflake_fivetran --database DRIP_DATA_INTELLIGENCE \
      --schema DBT_PROD_ENRICHED

  apply_stm_to_openmetadata.py --stm stm/output/05-DimCustomer-stm.md \
      --table-fqn snowflake_fivetran.DRIP_DATA_INTELLIGENCE.DBT_PROD_ENRICHED.DIMCUSTOMER
"""

from __future__ import annotations

import argparse
import json
import re
import sys
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

SCRIPT_DIR = Path(__file__).resolve().parent
sys.path.insert(0, str(SCRIPT_DIR))
from om_client import api, login  # noqa: E402


ESC_PIPE = "\x00ESCPIPE\x00"


def _split_row(line: str) -> list[str]:
    """Split a markdown table row into cells, respecting escaped pipes (\\|)."""
    if not line.strip().startswith("|"):
        return []
    raw = line.strip()
    # drop leading/trailing pipe
    if raw.startswith("|"):
        raw = raw[1:]
    if raw.endswith("|"):
        raw = raw[:-1]
    raw = raw.replace("\\|", ESC_PIPE)
    cells = [c.replace(ESC_PIPE, "|").strip() for c in raw.split("|")]
    return cells


def _is_separator(cells: list[str]) -> bool:
    return bool(cells) and all(re.fullmatch(r":?-+:?", c or "") for c in cells)


@dataclass
class StmSection:
    header: list[str]
    rows: list[list[str]]


@dataclass
class StmContent:
    table_description: str = ""
    target_db: str = ""
    target_schema: str = ""
    target_table: str = ""  # PascalCase from STM
    table_tags: list[str] = field(default_factory=list)
    column_tags: dict[str, list[str]] = field(default_factory=dict)
    table_glossary: list[str] = field(default_factory=list)
    column_glossary: dict[str, list[str]] = field(default_factory=dict)
    column_descriptions: dict[str, str] = field(default_factory=dict)  # keyed by PascalCase


def _parse_stm(path: Path) -> StmContent:
    text = path.read_text()
    lines = text.splitlines()
    stm = StmContent()

    sections = _split_sections(lines)

    # Section 2: description
    sec2 = sections.get("2", [])
    for ln in sec2:
        s = ln.lstrip()
        if s.startswith("> "):
            stm.table_description = s[2:].strip()
            break
        if s.startswith(">"):
            stm.table_description = s[1:].strip()
            if stm.table_description:
                break

    # Section 4: target schema/table
    sec4 = _parse_table(sections.get("4", []))
    if sec4 and sec4.rows:
        row = sec4.rows[0]
        hdr = [h.lower() for h in sec4.header]
        def col(name_like: str) -> str:
            for i, h in enumerate(hdr):
                if name_like in h:
                    return row[i] if i < len(row) else ""
            return ""
        stm.target_db = col("target database") or col("database")
        stm.target_schema = col("schema")
        stm.target_table = col("table name") or col("table")

    # Section 5: classification tags
    sec5 = _parse_table(sections.get("5", []))
    if sec5:
        hdr = [h.lower() for h in sec5.header]
        i_scope = _find(hdr, "scope")
        i_col = _find(hdr, "column")
        i_fqn = _find(hdr, "tag fqn") if "tag fqn" in " ".join(hdr) else _find(hdr, "fqn")
        for row in sec5.rows:
            scope = _safe(row, i_scope).lower()
            colname = _safe(row, i_col)
            fqn = _safe(row, i_fqn)
            if not fqn:
                continue
            if scope == "table":
                stm.table_tags.append(fqn)
            elif scope == "column" and colname:
                stm.column_tags.setdefault(colname, []).append(fqn)

    # Section 6: glossary terms
    sec6 = _parse_table(sections.get("6", []))
    if sec6:
        hdr = [h.lower() for h in sec6.header]
        i_scope = _find(hdr, "scope")
        i_col = _find(hdr, "column")
        i_fqn = _find(hdr, "term fqn")
        for row in sec6.rows:
            scope = _safe(row, i_scope).lower()
            colname = _safe(row, i_col)
            fqn = _safe(row, i_fqn)
            if not fqn:
                continue
            if scope == "table":
                stm.table_glossary.append(fqn)
            elif scope == "column" and colname:
                stm.column_glossary.setdefault(colname, []).append(fqn)

    # Section 7: column descriptions — Type-2 STMs have multiple tables
    # (Data Condition + Final); pick the one with both "target column" and
    # "description" headers (the Final mapping).
    sec7_lines = sections.get("7", [])
    sec7 = _find_matching_table(sec7_lines, required_headers=["target column", "description"])
    if sec7 is None:
        sec7 = _parse_table(sec7_lines)
    if sec7:
        hdr = [h.lower() for h in sec7.header]
        i_col = _find(hdr, "target column")
        i_desc = _find(hdr, "description")
        for row in sec7.rows:
            colname = _safe(row, i_col)
            desc = _safe(row, i_desc)
            if colname and desc:
                stm.column_descriptions[colname] = desc

    # Deduplicate tags while preserving order
    stm.table_tags = _dedup(stm.table_tags)
    stm.table_glossary = _dedup(stm.table_glossary)
    stm.column_tags = {k: _dedup(v) for k, v in stm.column_tags.items()}
    stm.column_glossary = {k: _dedup(v) for k, v in stm.column_glossary.items()}

    return stm


def _dedup(xs: list[str]) -> list[str]:
    seen: set[str] = set()
    out: list[str] = []
    for x in xs:
        if x not in seen:
            seen.add(x)
            out.append(x)
    return out


def _find(headers: list[str], needle: str) -> int:
    for i, h in enumerate(headers):
        if needle in h:
            return i
    return -1


def _safe(row: list[str], idx: int) -> str:
    return row[idx] if 0 <= idx < len(row) else ""


def _split_sections(lines: list[str]) -> dict[str, list[str]]:
    sections: dict[str, list[str]] = {}
    current: str | None = None
    for ln in lines:
        m = re.match(r"^##\s+(\d+)\.", ln)
        if m:
            current = m.group(1)
            sections[current] = []
            continue
        if current:
            sections[current].append(ln)
    return sections


def _iter_tables(lines: list[str]):
    """Yield every markdown table (StmSection) found in lines."""
    header: list[str] | None = None
    rows: list[list[str]] = []
    for ln in lines:
        if not ln.strip().startswith("|"):
            if header is not None:
                if rows:
                    yield StmSection(header=header, rows=rows)
                header = None
                rows = []
            continue
        cells = _split_row(ln)
        if not cells:
            continue
        if _is_separator(cells):
            continue
        if header is None:
            header = cells
        else:
            rows.append(cells)
    if header is not None and rows:
        yield StmSection(header=header, rows=rows)


def _find_matching_table(lines: list[str], required_headers: list[str]) -> StmSection | None:
    for tbl in _iter_tables(lines):
        hdr_joined = " ".join(h.lower() for h in tbl.header)
        if all(req in hdr_joined for req in required_headers):
            return tbl
    return None


def _parse_table(lines: list[str]) -> StmSection | None:
    header: list[str] | None = None
    rows: list[list[str]] = []
    for ln in lines:
        if not ln.strip().startswith("|"):
            if header is not None and rows:
                break
            continue
        cells = _split_row(ln)
        if not cells:
            continue
        if _is_separator(cells):
            continue
        if header is None:
            header = cells
        else:
            rows.append(cells)
    if header is None:
        return None
    return StmSection(header=header, rows=rows)


def _tag_label(fqn: str, source: str) -> dict[str, Any]:
    return {
        "tagFQN": fqn,
        "source": source,
        "labelType": "Manual",
        "state": "Confirmed",
    }


def _merge_labels(existing: list[dict[str, Any]], new: list[dict[str, Any]]) -> list[dict[str, Any]]:
    by_key: dict[tuple[str, str], dict[str, Any]] = {}
    order: list[tuple[str, str]] = []
    for lbl in list(existing) + list(new):
        key = (lbl.get("tagFQN", ""), lbl.get("source", ""))
        if key not in by_key:
            by_key[key] = lbl
            order.append(key)
        else:
            # keep existing, but prefer Manual labelType if either is Manual
            if lbl.get("labelType") == "Manual":
                by_key[key] = {**by_key[key], **lbl}
    return [by_key[k] for k in order]


@dataclass
class ApplyReport:
    stm: str
    table_fqn: str
    ok: bool = True
    table_description_updated: bool = False
    table_tags_added: int = 0
    table_glossary_added: int = 0
    column_descriptions_updated: int = 0
    column_tags_added: int = 0
    column_glossary_added: int = 0
    missing_stm_columns_in_om: list[str] = field(default_factory=list)
    rest_errors: list[str] = field(default_factory=list)
    skipped: str = ""


def _norm(s: str) -> str:
    return re.sub(r"[^A-Z0-9]", "", s.upper())


def _apply(stm: StmContent, table_fqn: str, token: str) -> ApplyReport:
    report = ApplyReport(stm="", table_fqn=table_fqn)
    try:
        table = api(
            "GET",
            f"/api/v1/tables/name/{table_fqn}?fields=tags,columns",
            token,
        )
    except RuntimeError as exc:
        report.ok = False
        report.rest_errors.append(f"GET table failed: {exc}")
        return report

    patches: list[dict[str, Any]] = []

    # Table description
    if stm.table_description and table.get("description") != stm.table_description:
        op = "replace" if table.get("description") else "add"
        patches.append({"op": op, "path": "/description", "value": stm.table_description})
        report.table_description_updated = True

    # Table tags + glossary (stored together in /tags)
    existing_tags = list(table.get("tags") or [])
    new_table_labels: list[dict[str, Any]] = []
    for fqn in stm.table_tags:
        new_table_labels.append(_tag_label(fqn, "Classification"))
    for fqn in stm.table_glossary:
        new_table_labels.append(_tag_label(fqn, "Glossary"))
    merged = _merge_labels(existing_tags, new_table_labels)
    if merged != existing_tags:
        patches.append({"op": "add" if not existing_tags else "replace", "path": "/tags", "value": merged})
        added = {(l["tagFQN"], l["source"]) for l in merged} - {(l.get("tagFQN",""), l.get("source","")) for l in existing_tags}
        report.table_tags_added = sum(1 for _, s in added if s == "Classification")
        report.table_glossary_added = sum(1 for _, s in added if s == "Glossary")

    # Columns: build PascalCase -> OM column info (name + index)
    om_columns = table.get("columns") or []
    # OM names for Snowflake are UPPERCASE. Match by normalized form (strip _ and such).
    om_by_norm: dict[str, tuple[int, dict[str, Any]]] = {}
    for i, col in enumerate(om_columns):
        om_by_norm[_norm(col["name"])] = (i, col)

    stm_cols: set[str] = set()
    stm_cols.update(stm.column_descriptions.keys())
    stm_cols.update(stm.column_tags.keys())
    stm_cols.update(stm.column_glossary.keys())

    for stm_col in sorted(stm_cols):
        key = _norm(stm_col)
        match = om_by_norm.get(key)
        if not match:
            report.missing_stm_columns_in_om.append(stm_col)
            continue
        idx, col = match

        # description
        desc = stm.column_descriptions.get(stm_col)
        if desc and col.get("description") != desc:
            op = "replace" if col.get("description") else "add"
            patches.append({"op": op, "path": f"/columns/{idx}/description", "value": desc})
            report.column_descriptions_updated += 1

        # tags + glossary merged into /columns/{i}/tags
        col_existing = list(col.get("tags") or [])
        col_new: list[dict[str, Any]] = []
        for fqn in stm.column_tags.get(stm_col, []):
            col_new.append(_tag_label(fqn, "Classification"))
        for fqn in stm.column_glossary.get(stm_col, []):
            col_new.append(_tag_label(fqn, "Glossary"))
        if col_new:
            col_merged = _merge_labels(col_existing, col_new)
            if col_merged != col_existing:
                patches.append({
                    "op": "add" if not col_existing else "replace",
                    "path": f"/columns/{idx}/tags",
                    "value": col_merged,
                })
                added = {(l["tagFQN"], l["source"]) for l in col_merged} - {(l.get("tagFQN",""), l.get("source","")) for l in col_existing}
                report.column_tags_added += sum(1 for _, s in added if s == "Classification")
                report.column_glossary_added += sum(1 for _, s in added if s == "Glossary")

    if not patches:
        report.skipped = "no changes"
        return report

    try:
        api(
            "PATCH",
            f"/api/v1/tables/{table['id']}",
            token,
            body=patches,
            content_type="application/json-patch+json",
        )
    except RuntimeError as exc:
        report.ok = False
        report.rest_errors.append(f"PATCH failed: {exc}")
    return report


def _target_fqn_from_stm(stm: StmContent, service: str, database: str, schema_override: str | None) -> str:
    schema = (schema_override or stm.target_schema or "").upper()
    db = (database or stm.target_db or "").upper()
    tbl = (stm.target_table or "").upper()
    if not (service and db and schema and tbl):
        raise SystemExit(
            f"error: unable to compose FQN — service={service!r} database={db!r} schema={schema!r} table={tbl!r}"
        )
    return f"{service}.{db}.{schema}.{tbl}"


def _print_report(reports: list[ApplyReport]) -> None:
    print(f"{'STM':<38} {'tbl desc':<8} {'tbl tags':<9} {'tbl gloss':<10} {'col desc':<9} {'col tags':<9} {'col gloss':<10} {'missing':<8}")
    print("-" * 110)
    any_fail = False
    for r in reports:
        stm_name = Path(r.stm).name if r.stm else r.table_fqn.split(".")[-1]
        status = "ok" if r.ok else "FAIL"
        print(
            f"{stm_name:<38} "
            f"{('yes' if r.table_description_updated else '-'):<8} "
            f"{r.table_tags_added:<9} "
            f"{r.table_glossary_added:<10} "
            f"{r.column_descriptions_updated:<9} "
            f"{r.column_tags_added:<9} "
            f"{r.column_glossary_added:<10} "
            f"{len(r.missing_stm_columns_in_om):<8} "
            f"[{status}]"
        )
        if r.missing_stm_columns_in_om:
            print(f"    missing in OM: {', '.join(r.missing_stm_columns_in_om)}")
        for err in r.rest_errors:
            print(f"    error: {err}")
        if r.skipped:
            print(f"    note: {r.skipped}")
        if not r.ok:
            any_fail = True
    print()
    if any_fail:
        sys.exit(1)


def main() -> None:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--stm", help="single STM markdown file")
    p.add_argument("--stm-dir", help="directory of STM markdown files (skips README.md)")
    p.add_argument("--table-fqn", help="target table FQN (only with --stm)")
    p.add_argument("--service", help="OM database service name (required with --stm-dir)")
    p.add_argument("--database", help="OM database name (overrides STM Section 4)")
    p.add_argument("--schema", help="OM schema name (overrides STM Section 4)")
    args = p.parse_args()

    if not args.stm and not args.stm_dir:
        p.error("must pass --stm or --stm-dir")

    stm_paths: list[Path] = []
    if args.stm:
        stm_paths = [Path(args.stm)]
    else:
        stm_dir = Path(args.stm_dir)
        stm_paths = sorted(
            [p for p in stm_dir.glob("*.md") if p.name.lower() != "readme.md"]
        )

    _, token = login()

    reports: list[ApplyReport] = []
    for stm_path in stm_paths:
        stm = _parse_stm(stm_path)
        if args.table_fqn:
            fqn = args.table_fqn
        else:
            if not args.service:
                sys.exit("error: --service is required when using --stm-dir")
            fqn = _target_fqn_from_stm(stm, args.service, args.database or "", args.schema)
        rep = _apply(stm, fqn, token)
        rep.stm = str(stm_path)
        rep.table_fqn = fqn
        reports.append(rep)

    _print_report(reports)


if __name__ == "__main__":
    main()
