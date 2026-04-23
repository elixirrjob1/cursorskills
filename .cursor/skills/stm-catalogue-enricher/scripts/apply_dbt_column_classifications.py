#!/usr/bin/env python3
"""Merge LLM-produced classifications for dbt-invented columns into STM files.

Consumes the work file produced by ``post_enrich_tags_glossary.py``, which a
subagent has filled in (per the STM catalogue enricher skill, step 4b). For
each item, appends rows to Section 5 (Classification Tags) and Section 6
(Glossary Terms) of the corresponding STM without touching existing rows.

Work-file item shape (after subagent completion):

    {
      "stm_file": "/abs/path/to/02-DimProduct-stm.md",
      "target_table": "02-DimProduct",
      "classified_columns": [
        {
          "column": "ProductHashPK",
          "classification_tags": ["Criticality.TransactionalCore", "PII.None"],
          "glossary_terms": []
        },
        ...
      ]
    }
"""
from __future__ import annotations

import argparse
import json
import re
import sys
from pathlib import Path

_SCRIPT_DIR = Path(__file__).resolve().parent
sys.path.insert(0, str(_SCRIPT_DIR))
from post_enrich_tags_glossary import _get  # noqa: E402  — OM helper


_S5_HEADER_LINE = "| Scope | Column | Tag FQN | Classification |"
_S5_SEP_LINE = "|-------|--------|---------|----------------|"
_S5_EMPTY_LINE = "|  |  |  |  |"

_S6_HEADER_LINE = "| Scope | Column | Term FQN | Term Name | Definition |"
_S6_SEP_LINE = "|-------|--------|----------|-----------|------------|"
_S6_EMPTY_LINE = "|  |  |  |  |  |"


def _classification_from_fqn(fqn: str) -> str:
    return fqn.split(".")[0] if "." in fqn else fqn


def _existing_s5_rows(stm_text: str) -> tuple[list[str], tuple[int, int]]:
    """Return (non-empty data rows, (start_line_idx, end_line_idx)) for the
    Section 5 markdown table so we can append new rows at end_line_idx."""
    return _existing_section_rows(stm_text, "## 5.", _S5_HEADER_LINE, _S5_EMPTY_LINE)


def _existing_s6_rows(stm_text: str) -> tuple[list[str], tuple[int, int]]:
    return _existing_section_rows(stm_text, "## 6.", _S6_HEADER_LINE, _S6_EMPTY_LINE)


def _existing_section_rows(
    stm_text: str,
    section_prefix: str,
    header_line: str,
    empty_line: str,
) -> tuple[list[str], tuple[int, int]]:
    lines = stm_text.split("\n")
    in_section = False
    header_idx = -1
    sep_idx = -1
    first_data_idx = -1
    last_data_idx = -1

    for i, line in enumerate(lines):
        s = line.strip()
        if s.startswith(section_prefix):
            in_section = True
            continue
        if in_section and s.startswith("## "):
            break
        if not in_section:
            continue
        if s.startswith("|"):
            if header_idx < 0:
                header_idx = i
                continue
            if sep_idx < 0 and all(ch in "-| :" for ch in s):
                sep_idx = i
                continue
            if first_data_idx < 0:
                first_data_idx = i
            last_data_idx = i

    if header_idx < 0 or sep_idx < 0:
        raise ValueError(f"could not locate {section_prefix} markdown table")

    if first_data_idx < 0:
        return [], (sep_idx + 1, sep_idx + 1)

    existing_rows = []
    for j in range(first_data_idx, last_data_idx + 1):
        s = lines[j].strip()
        if s and s != empty_line:
            existing_rows.append(s)
    return existing_rows, (first_data_idx, last_data_idx + 1)


def _row_key_s5(row: str) -> tuple[str, str, str]:
    cells = [c.strip() for c in row.split("|")[1:-1]]
    while len(cells) < 4:
        cells.append("")
    return (cells[0].lower(), cells[1].upper(), cells[2])


def _row_key_s6(row: str) -> tuple[str, str, str]:
    cells = [c.strip() for c in row.split("|")[1:-1]]
    while len(cells) < 5:
        cells.append("")
    return (cells[0].lower(), cells[1].upper(), cells[2])


_term_cache: dict[str, dict] = {}


def _fetch_term(fqn: str) -> dict:
    if fqn in _term_cache:
        return _term_cache[fqn]
    try:
        t = _get(f"glossaryTerms/name/{fqn}")
    except Exception:
        t = {}
    _term_cache[fqn] = t
    return t


def _apply_item(stm_text: str, item: dict) -> tuple[str, int, int, list[str]]:
    """Return (new_text, rows_added_s5, rows_added_s6, warnings)."""
    warnings: list[str] = []
    cols = item.get("classified_columns") or []
    if not cols:
        return stm_text, 0, 0, warnings

    s5_existing, (s5_start, s5_end) = _existing_s5_rows(stm_text)
    s6_existing, (s6_start, s6_end) = _existing_s6_rows(stm_text)

    s5_keys = {_row_key_s5(r) for r in s5_existing}
    s6_keys = {_row_key_s6(r) for r in s6_existing}

    new_s5: list[str] = []
    new_s6: list[str] = []

    for entry in cols:
        col = entry.get("column", "").strip()
        if not col:
            continue
        for fqn in entry.get("classification_tags", []) or []:
            fqn = fqn.strip()
            if not fqn:
                continue
            key = ("column", col.upper(), fqn)
            if key in s5_keys:
                continue
            s5_keys.add(key)
            new_s5.append(f"| Column | {col} | {fqn} | {_classification_from_fqn(fqn)} |")
        for term_fqn in entry.get("glossary_terms", []) or []:
            term_fqn = term_fqn.strip()
            if not term_fqn:
                continue
            key = ("column", col.upper(), term_fqn)
            if key in s6_keys:
                continue
            s6_keys.add(key)
            term = _fetch_term(term_fqn)
            term_name = term.get("displayName") or term.get("name") or term_fqn.rsplit(".", 1)[-1]
            definition = (term.get("description") or "").replace("\n", " ").replace("|", "—").strip()
            new_s6.append(f"| Column | {col} | {term_fqn} | {term_name} | {definition} |")

    text = stm_text
    if new_s5:
        text = _insert_rows(text, "## 5.", _S5_EMPTY_LINE, new_s5)
    if new_s6:
        text = _insert_rows(text, "## 6.", _S6_EMPTY_LINE, new_s6)
    return text, len(new_s5), len(new_s6), warnings


def _insert_rows(stm_text: str, section_prefix: str, empty_line: str, new_rows: list[str]) -> str:
    """Append *new_rows* at the end of the section's markdown table. If the
    existing table only has an empty placeholder row, replace it."""
    lines = stm_text.split("\n")
    in_section = False
    header_idx = -1
    sep_idx = -1
    first_data_idx = -1
    last_data_idx = -1

    for i, line in enumerate(lines):
        s = line.strip()
        if s.startswith(section_prefix):
            in_section = True
            continue
        if in_section and s.startswith("## "):
            break
        if not in_section:
            continue
        if s.startswith("|"):
            if header_idx < 0:
                header_idx = i
                continue
            if sep_idx < 0 and all(ch in "-| :" for ch in s):
                sep_idx = i
                continue
            if first_data_idx < 0:
                first_data_idx = i
            last_data_idx = i

    if header_idx < 0 or sep_idx < 0:
        return stm_text  # shouldn't happen — caller already succeeded

    if first_data_idx >= 0 and lines[first_data_idx].strip() == empty_line and first_data_idx == last_data_idx:
        lines[first_data_idx:first_data_idx + 1] = new_rows
    else:
        insert_at = last_data_idx + 1 if first_data_idx >= 0 else sep_idx + 1
        lines[insert_at:insert_at] = new_rows
    return "\n".join(lines)


def main() -> None:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--work-file", type=Path, required=True)
    p.add_argument(
        "--dry-run",
        action="store_true",
        help="Print what would change without writing any STM file",
    )
    args = p.parse_args()

    work = json.loads(args.work_file.read_text())
    items = work.get("items", [])
    if not items:
        print("work file has no items")
        return

    total_s5 = 0
    total_s6 = 0
    for item in items:
        stm_path = Path(item["stm_file"])
        if not stm_path.is_file():
            print(f"  skip: {stm_path} not found")
            continue
        text = stm_path.read_text()
        new_text, n5, n6, warns = _apply_item(text, item)
        total_s5 += n5
        total_s6 += n6
        status = "would update" if args.dry_run else "updated"
        print(f"  {stm_path.name}: {status} +{n5} tag rows, +{n6} glossary rows")
        for w in warns:
            print(f"    warn: {w}")
        if not args.dry_run and new_text != text:
            stm_path.write_text(new_text)

    print(f"\nTotal rows added: section 5 +{total_s5}, section 6 +{total_s6}")


if __name__ == "__main__":
    main()
