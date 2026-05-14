# tests/test_skill.py
# Run with: pytest tests/test_skill.py

def test_should_trigger_postgresql_analysis():
    """Can you analyze our PostgreSQL database for ingestion readiness?"""
    # Assert: routes to PostgreSQL module, runs preflight, produces schema.json
    ...

def test_should_trigger_csv_flat_file():
    """I have a CSV file of customer orders, can you profile it and produce a schema contract?"""
    # Assert: routes to flat/generic, runs tabular_schema_json.py, produces schema.json
    ...

def test_should_trigger_rest_api():
    """Analyze this REST API for ingestion: https://api.example.com/orders"""
    # Assert: routes to API generic module, runs api_reader.py and api_analyzer.py
    ...

def test_should_trigger_mssql_pipeline():
    """Generate a schema.json for our MSSQL source system before we build the pipeline"""
    # Assert: identifies MSSQL, runs preflight, routes to mssql module
    ...

def test_should_trigger_oracle_data_quality():
    """Check data quality risks in our Oracle database"""
    # Assert: routes to Oracle module, reports NULL/timezone/late-arrival findings
    ...

def test_should_not_trigger_dbt_model():
    """Create a dbt model for the sales table"""
    # Assert: does not invoke source-system-analyser; redirects to dbt-model-from-stm
    ...

def test_should_not_trigger_catalog_sync():
    """Sync the metadata from our Snowflake tables to OpenMetadata"""
    # Assert: does not invoke this skill; redirects to catalog-sync
    ...

def test_should_not_trigger_python_script():
    """Write me a Python script to connect to PostgreSQL and run a query"""
    # Assert: answers from general knowledge, does not invoke this skill
    ...

def test_edge_case_vague_source():
    """Can you analyze my data source?"""
    # Assert: asks for source type and connection details before proceeding
    ...

def test_edge_case_unsupported_mysql():
    """I have a MySQL database I want to analyze"""
    # Assert: uses generic fallback or asks user to confirm before proceeding
    ...

def test_edge_case_excel_file():
    """Analyze this file: sales_data.xlsx"""
    # Assert: routes to flat/generic module, runs tabular_schema_json.py for Excel
    ...
