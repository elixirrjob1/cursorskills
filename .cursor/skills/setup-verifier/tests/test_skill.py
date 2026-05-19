# tests/test_skill.py
# Run with: pytest tests/test_skill.py

import json
import os

SKILL_PATH = os.path.join(os.path.dirname(__file__), "..", "SKILL.md")
EVALS_PATH = os.path.join(os.path.dirname(__file__), "evals", "evals.json")


def load_skill():
    with open(SKILL_PATH) as f:
        return f.read()


def load_evals():
    with open(EVALS_PATH) as f:
        return json.load(f)["evals"]


def test_skill_file_exists():
    """SKILL.md must exist at expected path."""
    assert os.path.exists(SKILL_PATH), f"SKILL.md not found at {SKILL_PATH}"


def test_skill_has_frontmatter():
    """SKILL.md must have valid YAML frontmatter with name and description."""
    content = load_skill()
    assert content.startswith("---"), "SKILL.md missing opening frontmatter delimiter"
    assert "name: setup-verifier" in content, "name field missing from frontmatter"
    assert "description:" in content, "description field missing from frontmatter"


def test_skill_under_500_lines():
    """SKILL.md body must be under 500 lines."""
    with open(SKILL_PATH) as f:
        lines = f.readlines()
    assert len(lines) < 500, f"SKILL.md has {len(lines)} lines, exceeds 500-line limit"


def test_skill_covers_all_four_systems():
    """SKILL.md must document checks for Snowflake, OpenMetadata, dbt Cloud, and Fivetran."""
    content = load_skill()
    for system in ["Snowflake", "OpenMetadata", "dbt Cloud", "Fivetran"]:
        assert system in content, f"SKILL.md does not mention {system}"


def test_skill_defines_output_format():
    """SKILL.md must define an output table format."""
    content = load_skill()
    assert "| System" in content or "|System" in content, "Output table with System column not found"
    assert "Status" in content, "Status column not defined in output format"


def test_skill_includes_remediation():
    """SKILL.md must include specific fixes for each system failure."""
    content = load_skill()
    assert "If it fails" in content, "No remediation guidance found"
    assert "401" in content or "Unauthorized" in content, "No auth failure remediation found"


def test_skill_references_env_check():
    """SKILL.md must document .env variable validation."""
    content = load_skill()
    assert ".env" in content, "No .env check documented"
    assert "SNOWFLAKE_ACCOUNT" in content, "Required .env variable SNOWFLAKE_ACCOUNT not listed"


def test_skill_defines_completion_message():
    """SKILL.md must state what to output when all checks pass."""
    content = load_skill()
    assert "Setup complete" in content, "No completion message defined for full-pass scenario"


def test_evals_cover_all_types():
    """evals.json must have should-trigger, should-not-trigger, and edge-case entries."""
    evals = load_evals()
    types = {e["type"] for e in evals}
    assert "should-trigger" in types, "No should-trigger evals defined"
    assert "should-not-trigger" in types, "No should-not-trigger evals defined"
    assert "edge-case" in types, "No edge-case evals defined"


def test_should_trigger_check_my_setup(monkeypatch):
    """'check my setup' should trigger the setup-verifier skill."""
    # Assert: skill description contains trigger term 'check setup'
    content = load_skill()
    assert "check setup" in content.lower() or "check my setup" in content.lower() or \
           "check setup" in content, \
        "Trigger phrase 'check setup' not present in skill description"


def test_should_trigger_verify_connections(monkeypatch):
    """'verify my connections are working' should trigger the setup-verifier skill."""
    content = load_skill()
    assert "verify connections" in content.lower() or "verify" in content.lower(), \
        "Trigger phrase 'verify connections' not present in skill"


def test_should_not_trigger_dbt_logs():
    """'extract the last dbt Cloud run logs' should NOT trigger setup-verifier."""
    content = load_skill()
    assert "run logs" not in content.lower() or "extract" not in content.lower(), \
        "Skill may accidentally trigger on dbt log extraction requests"


def test_output_distinguishes_required_vs_optional():
    """Output format must distinguish required failures (❌) from optional missing vars (⚠️)."""
    content = load_skill()
    assert "⚠️" in content or "optional" in content.lower(), \
        "No distinction between required failures and optional missing variables"
