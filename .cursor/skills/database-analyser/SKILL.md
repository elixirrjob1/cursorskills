---
name: database-analyser
description: Analyzes database schemas and produces a fully enriched JSON file with all metadata. Use when analyzing database structures, generating schema documentation, or when users mention database analysis, schema introspection, or data mapping.
---

# Database Schema Analyzer

## Overview

This skill provides a simple tool for analyzing database schemas and producing a fully enriched JSON file with all metadata. It's a single-function tool that does everything: connects to the database, analyzes all tables, enriches with metadata, and saves to JSON.

**When to use:**
- User asks to analyze a database schema
- Need to generate schema documentation
- Planning data migrations or ETL pipelines
- Understanding database structure and relationships
- Detecting sensitive fields, partitions, or CDC configuration

## Obtaining the Database Connection String

**IMPORTANT**: The AI must obtain the database connection string before running the analysis. Follow this priority order:

1. **User-provided connection string**: If the user explicitly provides a connection string in their request, use it directly.

2. **Environment variables**: Check for common environment variable names:
   - `DATABASE_URL`
   - `DB_URL`
   - `POSTGRES_URL` (for PostgreSQL)
   - `DB_CONNECTION_STRING`
   
   Example: `os.environ.get('DATABASE_URL')`

3. **Configuration files**: Look for connection strings in common config files:
   - `.env` files (use `python-dotenv` if available)
   - `config.py`, `settings.py`, `config.json`
   - `docker-compose.yml` (if present)
   - Application-specific config files

4. **Ask the user**: **If no connection string is found from the above sources, the AI MUST ask the user to provide it:**
   - "I need a database connection string to analyze the schema. Please provide it in the format: `postgresql://user:password@host:port/database`"
   - Or ask for individual components: "Please provide the database connection details (host, port, database name, username, password)"

**Connection string format:**
- PostgreSQL: `postgresql://[user[:password]@][host][:port][/database]`
- Example: `postgresql://postgres:mypassword@localhost:5432/mydb`

**Security note**: Never hardcode credentials. Always use environment variables or ask the user for sensitive information. Never proceed with the analysis without a valid connection string.

## Prerequisites

**Required Python packages:**
- `sqlalchemy` - Database connectivity
- `psycopg2-binary` - PostgreSQL driver (for PostgreSQL databases)

**Note:** This script is self-contained and has no external dependencies beyond SQLAlchemy. All database functions are implemented directly in the script.

## Running the Script

Before running the script:

1. **Obtain the database connection string** (see "Obtaining the Database Connection String" section above). **If no connection string is found, ask the user to provide it before proceeding.**

2. Ensure a virtual environment is available:
   - Check if `.venv/bin/python` exists in the project root.
   - If it does NOT exist, create one and install dependencies:
     ```bash
     python3 -m venv .venv
     .venv/bin/pip install sqlalchemy psycopg2-binary
     ```
   - If it DOES exist, verify required packages are installed:
     ```bash
     .venv/bin/pip install --quiet sqlalchemy psycopg2-binary
     ```

3. Run the script using the venv Python:
   ```bash
   .venv/bin/python .cursor/skills/database-analyser/scripts/database_analyzer.py <database_url> <output_json_path> [schema]
   ```

Example:
```bash
# Using explicit connection string
.venv/bin/python .cursor/skills/database-analyser/scripts/database_analyzer.py "postgresql://user:pass@host/db" schema.json public

# Using environment variable
export DATABASE_URL="postgresql://user:pass@host/db"
.venv/bin/python .cursor/skills/database-analyser/scripts/database_analyzer.py "$DATABASE_URL" schema.json public
```

## Function Reference

### `analyze_database_to_json()`

**Main function** - Analyzes database and saves enriched schema to JSON.

**Parameters:**
- `database_url` (str): Database connection URL
- `output_path` (str): Path to save JSON file
- `schema` (str, optional): Schema name to filter tables
- `include_sample_data` (bool, optional): Include sample data rows (default: False)

**Returns:**
- Dict containing the full enriched schema (same as what's saved to JSON)

**What it does:**
1. Connects to database
2. Fetches all table metadata (columns, PKs, FKs)
3. Enriches with:
   - Row counts
   - Field classifications (pricing, quantity, categorical, temporal, contact)
   - Sensitive fields detection (PII, financial, credentials)
   - Incremental columns (for ETL watermarking)
   - Partition columns (for date partitioning)
   - CDC status (change data capture configuration)
4. Saves everything to JSON file

## JSON Output Format

The generated JSON file has this structure:

```json
{
  "metadata": {
    "generated_at": "2026-02-09T12:00:00",
    "database_url": "host/db",
    "schema_filter": "public",
    "total_tables": 10,
    "total_rows": 1000000
  },
  "connection": {
    "host": "localhost",
    "port": "5432",
    "database": "mydb",
    "driver": "postgresql",
    "timezone": "UTC"
  },
  "tables": [
    {
      "table": "users",
      "schema": "public",
      "columns": [
        {
          "name": "id",
          "type": "integer",
          "nullable": false,
          "is_incremental": true
        },
        {
          "name": "email",
          "type": "varchar(255)",
          "nullable": false,
          "is_incremental": false
        }
      ],
      "primary_keys": ["id"],
      "foreign_keys": [],
      "row_count": 50000,
      "field_classifications": {
        "email": "contact",
        "created_at": "temporal"
      },
      "sensitive_fields": {
        "email": "pii_contact"
      },
      "incremental_columns": ["id", "updated_at"],
      "partition_columns": ["created_at"],
      "cdc_enabled": false,
      "has_primary_key": true,
      "has_foreign_keys": false,
      "has_sensitive_fields": true
    }
  ]
}
```

## Enriched Metadata

### Field Classifications

Automatically classifies fields based on naming patterns:
- **pricing**: price, cost, amount, total, subtotal
- **quantity**: quantity, qty
- **categorical**: category, type, status
- **temporal**: created_at, updated_at, modified_at
- **contact**: email, phone

### Sensitive Fields

Detects PII and sensitive data:
- **government_id**: ssn, passport, tax_id, driver_license
- **financial**: credit_card, bank_account, salary
- **pii_contact**: email, phone, mobile
- **pii_personal**: date_of_birth, gender, ethnicity
- **pii_address**: address, street, postal_code
- **network_identity**: ip_address, mac_address
- **credential**: password, secret, token, api_key

### Incremental Columns

Identifies columns suitable for incremental/watermark loads:
- Auto-increment primary keys (serial, identity)
- Timestamp columns: updated_at, modified_at, created_at

### Partition Columns

Detects columns suitable for date/range partitioning:
- Date/timestamp columns with time-series naming patterns
- For Postgres: queries system catalogs for actual partition keys

### CDC Status

Checks if change data capture is enabled:
- For Postgres: checks REPLICA IDENTITY setting
- Returns true if CDC is configured (full or index replica identity)

## Examples

### Example 1: Analyze entire database

```python
from scripts.database_analyzer import analyze_database_to_json

schema = analyze_database_to_json(
    database_url="postgresql://user:pass@localhost/mydb",
    output_path="full_schema.json"
)
```

### Example 2: Analyze specific schema

```python
schema = analyze_database_to_json(
    database_url="postgresql://user:pass@localhost/mydb",
    output_path="public_schema.json",
    schema="public"
)
```

### Example 3: Include sample data

```python
schema = analyze_database_to_json(
    database_url="postgresql://user:pass@localhost/mydb",
    output_path="schema_with_samples.json",
    include_sample_data=True
)
```

### Example 4: Use in a script

```python
import json
from scripts.database_analyzer import analyze_database_to_json

# Analyze database
schema = analyze_database_to_json(
    "postgresql://user:pass@host/db",
    "schema.json"
)

# Use the returned dict
print(f"Found {schema['metadata']['total_tables']} tables")
for table in schema['tables']:
    if table['has_sensitive_fields']:
        print(f"Table {table['table']} has sensitive fields: {table['sensitive_fields']}")
```

## Error Handling

- If no tables found: Returns dict with `{"error": "No tables found"}`
- If table analysis fails: Logs warning and continues with next table
- If connection fails: Raises exception with error details
- All errors are logged using Python logging

## Best Practices

1. **Use schema filter** when working with multi-schema databases to reduce analysis time
2. **Set include_sample_data=False** (default) unless you need sample data - it's slower
3. **Check the returned dict** for errors before using the JSON file
4. **Review sensitive_fields** in the output to identify PII/data privacy concerns
5. **Use incremental_columns** for planning ETL watermark strategies
6. **Check partition_columns** for optimizing large table queries

## Script Reference

See `.cursor/skills/database-analyser/scripts/database_analyzer.py` for implementation details.

**Key function:**
- `analyze_database_to_json()` - Main function that does everything

**Helper functions:**
- `detect_sensitive_fields()` - Detect PII/sensitive columns
- `detect_partition_columns()` - Find partition candidates
- `detect_incremental_columns()` - Find incremental load columns
- `detect_cdc_enabled()` - Check CDC configuration
- `parse_connection_info()` - Extract connection details
- `classify_field()` - Classify field by naming pattern
