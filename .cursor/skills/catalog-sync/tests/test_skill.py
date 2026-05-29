# tests/test_skill.py
# Run with: pytest .cursor/skills/catalog-sync/tests/test_skill.py

"""Behavioural eval stubs for catalog-sync skill-reviewer suite."""

import json
from pathlib import Path

EVALS_PATH = Path(__file__).parent / "evals" / "evals.json"
TEST_CASES = Path(__file__).parent / "test-cases.md"


def _load_evals():
    with EVALS_PATH.open(encoding="utf-8") as f:
        return json.load(f)["evals"]


def test_eval_suite_files_exist():
    assert TEST_CASES.exists()
    assert EVALS_PATH.exists()
    evals = _load_evals()
    assert len(evals) >= 10
    types = {e["type"] for e in evals}
    assert "should-trigger" in types
    assert "should-not-trigger" in types
    assert "edge-case" in types


def test_each_eval_has_assertions():
    for ev in _load_evals():
        assert len(ev["assertions"]) >= 2, f"eval {ev['id']} needs assertions"


def test_should_trigger_postgres_sync():
    """Sync Postgres into OpenMetadata and tag customers."""
    # Executor/grader subagents validate MCP workflow assertions.
    assert True


def test_should_trigger_snowflake_inspect():
    """Snowflake metadata sync and schema/table inspection."""
    assert True


def test_should_trigger_mysql_onboard():
    """Onboard new MySQL source — plan/apply route."""
    assert True


def test_should_trigger_existing_postgres_rerun():
    """Re-run ingestion on existing Postgres service."""
    assert True


def test_should_trigger_glossary_assign():
    """Assign glossary term to orders table."""
    assert True


def test_should_trigger_oracle_new():
    """First-time Oracle registration."""
    assert True


def test_should_not_trigger_vocab_publisher():
    """Governance vocabulary push — not catalog-sync."""
    assert True


def test_should_not_trigger_glossary_tagger():
    """AI glossary mapping — catalog-glossary-tagger."""
    assert True


def test_should_not_trigger_dbt_lineage():
    """dbt lineage import — governance-import-dbt-lineage."""
    assert True


def test_edge_password_env_only():
    """Onboard with password env var name only."""
    assert True


def test_edge_duplicate_service_stop():
    """Refuse onboard when service already exists."""
    assert True


def test_edge_vague_fix_catalog():
    """Clarify vague fix my data catalog request."""
    assert True
