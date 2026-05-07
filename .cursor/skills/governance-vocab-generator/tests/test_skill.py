# tests/test_skill.py
# Run with: pytest tests/test_skill.py

GOVERNANCE_PATH_PATTERN = "governance-vocabularies/"
CANONICAL_DIMS = ["Architecture", "Privacy", "Criticality", "Lifecycle", "Retention", "QualityTrust", "ComplianceLegal"]


def test_should_trigger_retail_banking():
    """Generate a data governance vocabulary for the retail banking domain."""
    # Assert: response contains a governance-vocabularies/ file path
    # Assert: uses exact canonical dimension names, not synonyms
    # Assert: no tables or JSON in output vocabulary
    assert True  # evaluated by executor subagent


def test_should_trigger_medallion_architecture():
    """Create governance taxonomy for our data platform — it uses Bronze/Silver/Gold medallion architecture."""
    # Assert: Architecture classification included with Bronze/Silver/Gold levels
    # Assert: output path follows governance-vocabularies/<slug>-governance-vocab.md
    assert True


def test_should_trigger_digital_health():
    """We need a classification framework for our digital health data catalog."""
    # Assert: Privacy classification included
    # Assert: returns file path to governance-vocabularies/
    # Assert: no lengthy prose as primary output
    assert True


def test_should_trigger_legal_docs():
    """Produce a governance vocabulary for a legal document management system."""
    # Assert: ComplianceLegal included
    # Assert: Architecture omitted or explicitly noted as N/A (no staged processing)
    # Assert: returns file path
    assert True


def test_should_not_trigger_tag_assignment():
    """Apply the Privacy and ComplianceLegal governance tags to all the tables in our OpenMetadata catalog."""
    # Assert: does NOT produce governance-vocabularies/ path as main deliverable
    # Assert: routes to catalog-glossary-tagger or catalog-sync
    # Assert: does NOT output classification dimensions as primary content
    assert True


def test_should_not_trigger_dbt_model():
    """Build a dbt SQL model that tracks data classification levels in our Snowflake warehouse."""
    # Assert: references dbt, SQL, or dbt-model-from-stm
    # Assert: does NOT generate governance vocabulary markdown
    assert True


def test_edge_case_iot_sensor_type():
    """Generate a governance vocabulary for IoT sensor data — also add a custom SensorType classification."""
    # Assert: SensorType omitted or explicitly fails 3-condition test
    # Assert: Architecture included (IoT has staged processing)
    # Assert: canonical-first evaluation applied
    assert True


def test_edge_case_hr_all_canonical():
    """Create a governance classification framework for HR data — include all 7 canonical dimensions."""
    # Assert: Privacy included
    # Assert: Architecture not blindly included — evaluated per-dimension rule
    # Assert: returns file path
    assert True
