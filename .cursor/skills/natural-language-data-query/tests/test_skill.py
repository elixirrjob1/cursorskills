"""
Eval suite for natural-language-data-query skill.
Run with: pytest tests/test_skill.py -v

These are structural/behavioural assertions against the SKILL.md content.
They validate that the skill's written instructions would produce correct
agent behaviour for each test case — not live execution against Snowflake.
"""

import re
from pathlib import Path

SKILL_MD = Path(__file__).parent.parent / "SKILL.md"
SKILL_TEXT = SKILL_MD.read_text()


# ---------------------------------------------------------------------------
# Should-trigger: verify the description contains the expected trigger terms
# ---------------------------------------------------------------------------

def test_trigger_sales_query():
    """'What were total sales last quarter?' — 'sales' must be a trigger term."""
    assert "sales" in SKILL_TEXT.lower(), "Trigger term 'sales' not found in skill"


def test_trigger_customer_orders():
    """'How many customers placed orders' — 'customers' and 'orders' must appear as triggers."""
    assert "customers" in SKILL_TEXT.lower()
    assert "orders" in SKILL_TEXT.lower()


def test_trigger_report():
    """'Give me a report on...' — 'report' must be listed as a trigger."""
    assert "report" in SKILL_TEXT.lower()


def test_trigger_can_you_look_up():
    """'Can you look up inventory levels' — 'can you look up' must be a trigger phrase."""
    assert "can you look up" in SKILL_TEXT.lower()


def test_trigger_inventory():
    """'Can you look up inventory levels' — 'inventory' must be a trigger term."""
    assert "inventory" in SKILL_TEXT.lower()


# ---------------------------------------------------------------------------
# Should-not-trigger: verify skill does NOT claim ownership of adjacent tasks
# ---------------------------------------------------------------------------

def test_no_trigger_dbt_model_generation():
    """Skill should not claim to generate dbt models."""
    assert "generate a dbt model" not in SKILL_TEXT.lower(), (
        "Skill description claims dbt model generation — overlaps with dbt-model-from-stm"
    )


def test_no_trigger_write_python_scripts():
    """Skill should not claim to write Python scripts."""
    assert "write" not in SKILL_TEXT[:500].lower() or "write sql" in SKILL_TEXT.lower(), (
        "Skill description may claim general script writing"
    )


def test_no_trigger_glossary_creation():
    """Skill should not claim to create glossary terms as a primary purpose."""
    desc_section = SKILL_TEXT[:600]
    assert "create glossary" not in desc_section.lower(), (
        "Skill description claims glossary creation — overlaps with catalog skills"
    )


# ---------------------------------------------------------------------------
# Edge cases: verify skill handles ambiguous inputs with clarification steps
# ---------------------------------------------------------------------------

def test_edge_case_collects_business_question():
    """Vague queries: skill must collect the business question before proceeding."""
    assert "business question" in SKILL_TEXT.lower(), (
        "Skill must reference collecting the business question in Step 1"
    )
    assert "step 1" in SKILL_TEXT.lower()


def test_edge_case_broad_query_no_limit_bypass():
    """'Show me everything' — LIMIT 1000 must be the safety default."""
    assert "limit 1000" in SKILL_TEXT.lower(), (
        "Skill must enforce LIMIT 1000 for safety on broad queries"
    )


def test_edge_case_vague_question_has_semantic_search_fallback():
    """Vague questions: semantic_search fallback must exist for poor keyword results."""
    assert "semantic_search" in SKILL_TEXT, (
        "Skill must use semantic_search as fallback for vague/exploratory questions"
    )


# ---------------------------------------------------------------------------
# Structural integrity checks
# ---------------------------------------------------------------------------

def test_credential_section_exists():
    """Credential Management section must be present."""
    assert "credential management" in SKILL_TEXT.lower()


def test_error_handling_table_exists():
    """Error Handling section must be present."""
    assert "error handling" in SKILL_TEXT.lower()


def test_layer_preference_logic_exists():
    """Layer preference (Gold/Silver/Bronze) decision logic must be documented."""
    assert "gold" in SKILL_TEXT.lower()
    assert "layer" in SKILL_TEXT.lower()


def test_identifier_quoting_rule_exists():
    """Double-quoting rule for Snowflake identifiers must be documented."""
    assert "double" in SKILL_TEXT.lower() or 'double-quote' in SKILL_TEXT.lower()
    assert "quoted_ref" in SKILL_TEXT


def test_no_use_role_statements():
    """Skill must warn against USE ROLE/DATABASE/WAREHOUSE in generated SQL."""
    assert "use role" in SKILL_TEXT.lower(), (
        "Skill must explicitly forbid USE ROLE statements in generated SQL"
    )


def test_requirements_file_exists():
    """scripts/requirements.txt must exist alongside the skill."""
    req = Path(__file__).parent.parent / "scripts" / "requirements.txt"
    assert req.exists(), "scripts/requirements.txt not found"


def test_query_engine_script_exists():
    """scripts/query_engine.py must exist."""
    script = Path(__file__).parent.parent / "scripts" / "query_engine.py"
    assert script.exists(), "scripts/query_engine.py not found"


def test_no_hardcoded_credentials():
    """No literal credential values should appear in SKILL.md."""
    # Check for common patterns of hardcoded secrets
    hardcoded_patterns = [r'password\s*=\s*["\'][^"\'<>{}\[\]]+["\']',
                          r'token\s*=\s*["\'][^"\'<>{}\[\]]+["\']']
    for pattern in hardcoded_patterns:
        matches = re.findall(pattern, SKILL_TEXT, re.IGNORECASE)
        assert not matches, f"Possible hardcoded credential found: {matches}"
