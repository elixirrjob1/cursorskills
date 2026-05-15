"""Lightweight regression tests for the metadata/history touch-up helpers in
``post_enrich_tags_glossary.py``.

These tests intentionally avoid any OpenMetadata or network access — they
exercise only the pure-Python text-manipulation helpers that drive the STM
Preservation Contract.

Run from the repo root:

    pytest -q tests/test_post_enrich_helpers.py
"""
from __future__ import annotations

import os
import sys
from datetime import date
from pathlib import Path

_REPO_ROOT = Path(__file__).resolve().parents[1]
_SCRIPTS = _REPO_ROOT / "scripts"
sys.path.insert(0, str(_SCRIPTS))

# Importing the module triggers ``load_dotenv()`` which is harmless when no
# ``.env`` exists. We also rely on its side-effect-free helpers only.
from post_enrich_tags_glossary import (  # noqa: E402
    _extract_stm_version,
    _increment_stm_version,
    _is_doc_info_heading,
    _is_version_history_heading,
    _merge_section5_rows,
    _merge_section6_rows,
    _parse_all_tables_in_section,
    _pick_column_mapping_table,
    _rows_from_section5,
    _rows_from_section6,
    _touch_document_metadata_and_history,
)


_STM_TEMPLATE = """# 05 — DimCustomer STM

## 1. Document Information

| Field | Value |
|-------|-------|
| **STM Version** | 2.1 |
| **Author** | fillip |
| **Last Updated** | 2026-04-21 |

## 2. Business Context

(prose)

## 5. Classification Tags

| Scope | Column | Tag FQN | Classification |
|-------|--------|---------|----------------|
| Column | EmailAddress | PII.Sensitive | PII |

## 6. Glossary Terms

| Scope | Column | Term FQN | Term Name | Definition |
|-------|--------|----------|-----------|------------|
| Column | EmailAddress | RetailDomainGlossary.Email | Email | Customer email address |

## 9. Version Control & Governance

| Version | Date | Author | Note | Approver |
|---------|------|--------|------|----------|
| 1.0 | 2026-01-01 | fillip | initial draft |  |
| 2.0 | 2026-04-13 | fillip | bronze enrichment pass |  |
"""


def _force_operator(monkeypatch) -> str:
    operator = "test-bot"
    monkeypatch.setenv("STM_UPDATED_BY", operator)
    return operator


def test_extract_stm_version_handles_extra_whitespace():
    text = "|   **STM Version**   |   3.4   |\n"
    assert _extract_stm_version(text) == "3.4"


def test_extract_stm_version_falls_back_to_default():
    text = "no version line here"
    assert _extract_stm_version(text) == "1.0"


def test_is_doc_info_heading_matches_unnumbered_variant():
    assert _is_doc_info_heading("## 1. Document Information")
    assert _is_doc_info_heading("## Document Information")
    assert _is_doc_info_heading("## 1 Document Information")
    assert not _is_doc_info_heading("## 2. Business Context")


def test_is_version_history_heading_matches_common_variants():
    assert _is_version_history_heading("## 9. Version Control & Governance")
    assert _is_version_history_heading("## Version History")
    assert _is_version_history_heading("## Version Control")
    assert not _is_version_history_heading("## 7. Field-Level Mapping Matrix")


def test_increment_stm_version_patch_and_digits():
    assert _increment_stm_version("1.0") == "1.1"
    assert _increment_stm_version("2.10") == "2.11"
    assert _increment_stm_version("1.0.3") == "1.0.4"
    assert _increment_stm_version("3") == "4"
    assert _increment_stm_version("draft") == "draft.1"


def test_touch_noop_when_not_substantive(monkeypatch):
    """No Section 5/6 delta → no edits to version, dates, operator, or history."""
    _force_operator(monkeypatch)
    out = _touch_document_metadata_and_history(
        _STM_TEMPLATE, "sections 5 and 6", substantive_stm_change=False,
    )
    assert out == _STM_TEMPLATE


def test_touch_bumps_stm_version_when_substantive(monkeypatch):
    operator = _force_operator(monkeypatch)
    out = _touch_document_metadata_and_history(
        _STM_TEMPLATE, "sections 5 and 6", substantive_stm_change=True,
    )
    assert "| **STM Version** | 2.2 |" in out
    assert "| **Author** | fillip |" in out
    today = date.today().isoformat()
    assert f"| **Last Updated** | {today} |" in out
    assert f"| **Last Updated By** | {operator} |" in out
    assert f"| 2.2 | {today} | {operator} | stm-catalogue-enricher-v2: updated sections 5 and 6 |  |" in out


def test_touch_is_idempotent_for_same_day_same_operator(monkeypatch):
    _force_operator(monkeypatch)
    once = _touch_document_metadata_and_history(
        _STM_TEMPLATE, "sections 5 and 6", substantive_stm_change=True,
    )
    twice = _touch_document_metadata_and_history(
        once, "sections 5 and 6", substantive_stm_change=False,
    )
    today = date.today().isoformat()
    # First pass adds one governance row for (2.2, today, operator); second is no-op
    assert once.count(f"| 2.2 | {today} |") == 1
    assert twice == once


def test_touch_does_not_touch_other_last_updated_lines(monkeypatch):
    """If a `Last Updated` row appears outside Section 1 (e.g. in a Sign-Off
    table), the helper must leave it alone."""
    operator = _force_operator(monkeypatch)
    polluted = _STM_TEMPLATE + (
        "\n## 10. Sign-Off\n\n"
        "| Field | Value |\n"
        "|-------|-------|\n"
        "| **Last Updated** | should-stay |\n"
    )
    out = _touch_document_metadata_and_history(
        polluted, "sections 5 and 6", substantive_stm_change=True,
    )
    assert "| **Last Updated** | should-stay |" in out
    # Section 1 Document Information updated
    today = date.today().isoformat()
    assert f"| **Last Updated** | {today} |" in out
    assert f"| **Last Updated By** | {operator} |" in out


def test_touch_supports_unnumbered_document_information(monkeypatch):
    operator = _force_operator(monkeypatch)
    template = _STM_TEMPLATE.replace("## 1. Document Information", "## Document Information")
    out = _touch_document_metadata_and_history(
        template, "sections 5 and 6", substantive_stm_change=True,
    )
    today = date.today().isoformat()
    assert f"| **Last Updated** | {today} |" in out
    assert f"| **Last Updated By** | {operator} |" in out


def test_merge_section5_rows_dedupes_on_full_key():
    existing = _rows_from_section5(_STM_TEMPLATE)
    new = [
        # exact duplicate of existing
        {"scope": "Column", "column": "EmailAddress",
         "tag_fqn": "PII.Sensitive", "classification": "PII"},
        # genuine new row
        {"scope": "Column", "column": "PhoneNumber",
         "tag_fqn": "PII.Sensitive", "classification": "PII"},
    ]
    merged = _merge_section5_rows(existing, new)
    cols = [r["column"] for r in merged]
    assert cols == ["EmailAddress", "PhoneNumber"]


def test_pick_column_mapping_table_handles_constraints_column():
    """Section 7 matrices gain a Constraints column before Description."""
    lines = """
## 7. Field-Level Mapping Matrix

| Target Table | Target Column | Data Type | Field Type | Source System | Source Table | Source Column(s) | Transformation / Business Rule | Nullable? | Default / Fallback | Constraints | Description |
|--------------|---------------|-----------|------------|---------------|--------------|------------------|--------------------------------|-----------|--------------------|-------------|-------------|
| DimExample | CustomerHashPK | NUMBER(19,0) | Primary Key | Snowflake | CUSTOMERS | CUSTOMER_ID |  | NO | | PK on CUSTOMER_ID | surrogate hash |
""".splitlines()
    tables = _parse_all_tables_in_section(lines, "## 7.")
    tbl = _pick_column_mapping_table(tables)
    hdr_lower = [h.lower() for h in tbl[0]]
    assert "constraints" in hdr_lower
    idx_c = hdr_lower.index("constraints")
    assert tbl[1][idx_c] == "PK on CUSTOMER_ID"


def test_merge_section6_rows_dedupes_on_scope_column_termfqn():
    existing = _rows_from_section6(_STM_TEMPLATE)
    new = [
        {"scope": "Column", "column": "EmailAddress",
         "term_fqn": "RetailDomainGlossary.Email", "term_name": "Email",
         "definition": "different wording but same term"},
        {"scope": "Column", "column": "FullName",
         "term_fqn": "RetailDomainGlossary.Name", "term_name": "Name",
         "definition": "customer name"},
    ]
    merged = _merge_section6_rows(existing, new)
    fqns = [r["term_fqn"] for r in merged]
    assert fqns == ["RetailDomainGlossary.Email", "RetailDomainGlossary.Name"]
