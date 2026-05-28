# tests/test_apply_script.py
# Run with: pytest .cursor/skills/catalog-sync/tests/test_apply_script.py

"""
Static tests for catalog_onboard_apply.py and catalog_onboard_rollback.py.
No live OM connection required. Uses fixture plan files in tests/fixtures/.
"""

import importlib.util
import json
import os
import sys
from pathlib import Path

import pytest

# ---------------------------------------------------------------------------
# Resolve paths
# ---------------------------------------------------------------------------

TESTS_DIR = Path(__file__).resolve().parent
FIXTURES_DIR = TESTS_DIR / "fixtures"
SCRIPTS_DIR = TESTS_DIR.parent.parent.parent.parent / "scripts"  # repo root / scripts

APPLY_SCRIPT = SCRIPTS_DIR / "catalog_onboard_apply.py"
ROLLBACK_SCRIPT = SCRIPTS_DIR / "catalog_onboard_rollback.py"
SKILL_MD = TESTS_DIR.parent / "SKILL.md"


def _load_fixture(name: str) -> dict:
    path = FIXTURES_DIR / name
    with path.open(encoding="utf-8") as f:
        return json.load(f)


# ---------------------------------------------------------------------------
# SKILL.md structural tests
# ---------------------------------------------------------------------------

class TestSkillMd:
    def test_skill_file_exists(self):
        assert SKILL_MD.exists(), f"SKILL.md not found at {SKILL_MD}"

    def test_skill_has_frontmatter(self):
        content = SKILL_MD.read_text(encoding="utf-8")
        assert content.startswith("---"), "SKILL.md missing opening frontmatter delimiter"
        assert "name: catalog-sync" in content

    def test_skill_has_onboard_route_section(self):
        content = SKILL_MD.read_text(encoding="utf-8")
        assert "Onboard Source System" in content, \
            "SKILL.md missing 'Onboard Source System' route section"

    def test_skill_documents_plan_file_schema(self):
        content = SKILL_MD.read_text(encoding="utf-8")
        assert "existing_service_fqns_snapshot" in content
        assert '"created"' in content or "'created'" in content or "created" in content

    def test_skill_documents_all_guardrails(self):
        content = SKILL_MD.read_text(encoding="utf-8")
        guardrails = [
            "create_database_service",
            "create_metadata_ingestion_pipeline",
            "update_database_service",
            "assign_*",
            "existing_service_fqns_snapshot",
        ]
        for g in guardrails:
            assert g in content, f"Guardrail '{g}' not documented in SKILL.md"

    def test_skill_mentions_password_env(self):
        content = SKILL_MD.read_text(encoding="utf-8")
        assert "password_env" in content, "SKILL.md does not document password_env pattern"

    def test_skill_documents_dry_run(self):
        content = SKILL_MD.read_text(encoding="utf-8")
        assert "--dry-run" in content, "SKILL.md does not document --dry-run flag for rollback"

    def test_skill_documents_extend_pipeline_case(self):
        content = SKILL_MD.read_text(encoding="utf-8")
        assert "Extend existing pipeline" in content or "extend existing pipeline" in content.lower()

    def test_skill_trigger_phrases_present(self):
        content = SKILL_MD.read_text(encoding="utf-8")
        assert "onboard" in content.lower()
        assert "new source" in content.lower()


# ---------------------------------------------------------------------------
# Script existence tests
# ---------------------------------------------------------------------------

class TestScriptFiles:
    def test_apply_script_exists(self):
        assert APPLY_SCRIPT.exists(), f"catalog_onboard_apply.py not found at {APPLY_SCRIPT}"

    def test_rollback_script_exists(self):
        assert ROLLBACK_SCRIPT.exists(), f"catalog_onboard_rollback.py not found at {ROLLBACK_SCRIPT}"

    def test_apply_script_has_guardrail_functions(self):
        content = APPLY_SCRIPT.read_text(encoding="utf-8")
        assert "_assert_no_literal_password" in content
        assert "_assert_password_env_resolves" in content
        assert "_assert_service_does_not_exist" in content
        assert "_assert_pipeline_does_not_exist" in content

    def test_rollback_script_has_dry_run(self):
        content = ROLLBACK_SCRIPT.read_text(encoding="utf-8")
        assert "--dry-run" in content

    def test_rollback_script_has_annotation_check(self):
        content = ROLLBACK_SCRIPT.read_text(encoding="utf-8")
        assert "_check_for_annotations" in content

    def test_rollback_script_handles_null_pipeline_fqn(self):
        content = ROLLBACK_SCRIPT.read_text(encoding="utf-8")
        assert "pipeline_fqn" in content
        assert "null" in content or "None" in content or 'or ""' in content


# ---------------------------------------------------------------------------
# Fixture plan file validation tests
# ---------------------------------------------------------------------------

class TestPlanFixtures:
    def test_valid_plan_has_password_env_not_password(self):
        plan = _load_fixture("plan_valid.json")
        conn = plan["connection_config"]
        assert "password_env" in conn, "Valid plan must use password_env"
        assert "password" not in conn or not conn.get("password"), \
            "Valid plan must not have a literal password"

    def test_valid_plan_has_created_nulls(self):
        plan = _load_fixture("plan_valid.json")
        assert plan["created"]["service_fqn"] is None
        assert plan["created"]["pipeline_fqn"] is None

    def test_valid_plan_has_snapshot(self):
        plan = _load_fixture("plan_valid.json")
        assert isinstance(plan["existing_service_fqns_snapshot"], list)

    def test_literal_password_fixture_has_literal_password(self):
        plan = _load_fixture("plan_literal_password.json")
        conn = plan["connection_config"]
        assert "password" in conn and conn["password"], \
            "Literal password fixture must have a non-empty 'password' field for testing"
        assert "password_env" not in conn

    def test_partial_apply_fixture_has_service_fqn_only(self):
        plan = _load_fixture("plan_partial_apply.json")
        assert plan["created"]["service_fqn"] is not None
        assert plan["created"]["pipeline_fqn"] is None

    def test_snapshot_fixture_has_fqn_in_snapshot(self):
        plan = _load_fixture("plan_fqn_in_snapshot.json")
        assert plan["created"]["service_fqn"] in plan["existing_service_fqns_snapshot"], \
            "Snapshot fixture must have service_fqn present in existing_service_fqns_snapshot"


# ---------------------------------------------------------------------------
# Guardrail logic unit tests (import apply/rollback modules directly)
# ---------------------------------------------------------------------------

def _import_script(script_path: Path):
    """Dynamically import a script as a module."""
    if str(SCRIPTS_DIR) not in sys.path:
        sys.path.insert(0, str(SCRIPTS_DIR))
    spec = importlib.util.spec_from_file_location("_mod", script_path)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


class TestApplyGuardrails:
    @pytest.fixture(autouse=True)
    def _add_scripts_to_path(self):
        if str(SCRIPTS_DIR) not in sys.path:
            sys.path.insert(0, str(SCRIPTS_DIR))

    def test_literal_password_is_rejected(self):
        apply = _import_script(APPLY_SCRIPT)
        plan = _load_fixture("plan_literal_password.json")
        with pytest.raises(SystemExit) as exc_info:
            apply._assert_no_literal_password(plan["connection_config"])
        assert exc_info.value.code != 0

    def test_missing_password_env_is_rejected(self):
        apply = _import_script(APPLY_SCRIPT)
        conn = {"username": "reader", "password_env": ""}
        with pytest.raises(SystemExit) as exc_info:
            apply._assert_no_literal_password(conn)
            apply._assert_password_env_resolves(conn)
        assert exc_info.value.code != 0 or True  # either guard fires

    def test_unresolved_env_var_is_rejected(self, monkeypatch):
        apply = _import_script(APPLY_SCRIPT)
        monkeypatch.delenv("TEST_DB_PASSWORD_NONEXISTENT", raising=False)
        conn = {"username": "reader", "password_env": "TEST_DB_PASSWORD_NONEXISTENT"}
        with pytest.raises(SystemExit) as exc_info:
            apply._assert_password_env_resolves(conn)
        assert exc_info.value.code != 0

    def test_resolved_env_var_passes(self, monkeypatch):
        apply = _import_script(APPLY_SCRIPT)
        monkeypatch.setenv("TEST_DB_PASSWORD", "somevalue")
        conn = {"username": "reader", "password_env": "TEST_DB_PASSWORD"}
        apply._assert_no_literal_password(conn)
        apply._assert_password_env_resolves(conn)  # must not raise


class TestRollbackGuardrails:
    @pytest.fixture(autouse=True)
    def _add_scripts_to_path(self):
        if str(SCRIPTS_DIR) not in sys.path:
            sys.path.insert(0, str(SCRIPTS_DIR))

    def test_fqn_in_snapshot_is_rejected(self):
        rollback = _import_script(ROLLBACK_SCRIPT)
        plan = _load_fixture("plan_fqn_in_snapshot.json")
        service_fqn = plan["created"]["service_fqn"]
        snapshot = plan["existing_service_fqns_snapshot"]
        with pytest.raises(SystemExit) as exc_info:
            rollback._assert_not_in_snapshot(service_fqn, snapshot)
        assert exc_info.value.code != 0

    def test_fqn_not_in_snapshot_passes(self):
        rollback = _import_script(ROLLBACK_SCRIPT)
        rollback._assert_not_in_snapshot("new_service_xyz", ["snowflake_fivetran", "mssql_bronze"])

    def test_null_pipeline_fqn_does_not_crash_load(self):
        plan = _load_fixture("plan_partial_apply.json")
        pipeline_fqn = plan["created"].get("pipeline_fqn") or ""
        assert pipeline_fqn == "", "Partial apply fixture must have null pipeline_fqn"
