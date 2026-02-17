---
name: source-system-analyser
description: Analyzes source database schemas and assesses data quality in one pass, producing a combined schema.json with full metadata and data quality findings. Use when analyzing database structures, schema introspection, data mapping, data quality, data integrity, source system readiness, ingestion preparation, delete management, late-arriving data, or timezone.
---

# Source System Analyser

## Overview

This skill provides a single tool that analyzes a source database schema and assesses data quality in one pass. It produces a combined `schema.json` file containing:

- **Schema metadata**: Tables, columns, primary keys, foreign keys, row counts, field classifications, sensitive fields, incremental columns, partition columns, join candidates, CDC status, column timezone, statistics
- **Data quality findings**: 9 checks (controlled value candidates, missing constraints, format inconsistencies, delete management, late-arriving data, timezone, etc.) with severity and recommendations

**When to use:**
- User asks to analyze a database schema or generate schema documentation
- Planning data migrations or ETL pipelines
- User asks about data quality or data integrity assessment
- Preparing a source system for ingestion or migration
- Need to find columns that should use controlled value lists
- Want to identify missing NOT NULL, FK, or CHECK constraints
- Investigating referential integrity issues
- Looking for format inconsistencies, delete management strategy, late-arriving data patterns, or timezone handling

## Obtaining the Database Connection String

**IMPORTANT**: Obtain the database connection string before running. Follow this priority:

1. **User-provided connection string**: If the user explicitly provides a connection string, use it directly.

2. **Environment variables**: Check `DATABASE_URL`, `DB_URL`, `POSTGRES_URL`, `DB_CONNECTION_STRING`
   - `DATABASE_SCHEMA` or `SCHEMA` — schema name to filter tables (script loads .env automatically)

3. **Configuration files**: `.env`, `config.py`, `settings.py`, `config.json`, `docker-compose.yml`

4. **Ask the user**: If no connection string is found, ask: "I need a database connection string. Please provide it in the format: `postgresql://user:password@host:port/database`"

**Connection string formats:**

| Database | Format |
|----------|--------|
| PostgreSQL | `postgresql://user:password@host:port/database` |
| Microsoft SQL Server / Azure SQL | `mssql+pyodbc://user:password@host:port/database?driver=ODBC+Driver+17+for+SQL+Server` |
| Oracle | `oracle+cx_oracle://user:password@host:port/?service_name=XE` |

**Security note**: Never hardcode credentials. Use environment variables or ask the user.

## Prerequisites

**Required Python packages:**
- `sqlalchemy` — Database connectivity
- `psycopg2-binary` — PostgreSQL driver
- `pyodbc` — Microsoft SQL Server / Azure SQL (optional, for MSSQL)
- `cx_Oracle` or `oracledb` — Oracle (optional, for Oracle)

Same as other skills. If `.venv` exists with those packages, no extra setup needed. Install drivers only for the databases you use.

## Running the Script

1. **Obtain the database connection string** (see above). If none found, ask the user.

2. Ensure a virtual environment is available:
   - If `.venv/bin/python` does NOT exist: `python3 -m venv .venv` then `.venv/bin/pip install sqlalchemy psycopg2-binary`
   - For MSSQL add: `pip install pyodbc`
   - For Oracle add: `pip install cx_Oracle` or `pip install oracledb`
   - If it DOES exist: `.venv/bin/pip install --quiet sqlalchemy psycopg2-binary` (and drivers as needed)

3. Run the script:
   ```bash
   .venv/bin/python scripts/source_system_analyzer.py <database_url> <output_json_path> [schema] [--dialect postgresql|mssql|oracle]
   ```

### Arguments

| Argument | Required | Description |
|----------|----------|-------------|
| `database_url` | Yes | Database connection URL |
| `output_json_path` | Yes | Where to save the combined output (e.g. `schema.json`) |
| `schema` | No | Schema to analyze (default: from `DATABASE_SCHEMA` or `SCHEMA` env, or dialect default: `public`/`dbo`/current user) |
| `--dialect` | No | Override dialect: `postgresql`, `mssql`, or `oracle` (default: inferred from URL) |

### Examples

```bash
# PostgreSQL (basic)
.venv/bin/python scripts/source_system_analyzer.py "$DATABASE_URL" schema.json public

# PostgreSQL (schema from .env)
.venv/bin/python scripts/source_system_analyzer.py "$DATABASE_URL" schema.json

# Microsoft SQL Server / Azure SQL
.venv/bin/python scripts/source_system_analyzer.py "$MSSQL_URL" schema.json dbo --dialect mssql

# Oracle
.venv/bin/python scripts/source_system_analyzer.py "$ORACLE_URL" schema.json MYSCHEMA --dialect oracle
```

## Combined Output Format

The single `schema.json` uses a **table-centric** structure — all data quality findings are nested under the table they belong to, eliminating duplication:

```json
{
  "metadata": {
    "generated_at": "2026-02-12T10:00:00+00:00",
    "database_url": "host:5432/db",
    "schema_filter": "public",
    "total_tables": 10,
    "total_rows": 1000000,
    "total_findings": 43
  },
  "connection": {
    "host": "localhost",
    "port": "5432",
    "database": "mydb",
    "driver": "postgresql",
    "timezone": "UTC"
  },
  "data_quality_summary": {
    "critical": 0,
    "warning": 18,
    "info": 25,
    "by_check": {
      "controlled_value_candidate": 7,
      "nullable_but_never_null": 13,
      "delete_management": 10,
      "late_arriving_data": 3,
      "timezone": 10
    },
    "constraints_found": {
      "check_constraints": 0,
      "enum_columns": 0,
      "unique_constraints": 2
    }
  },
  "tables": [
    {
      "table": "products",
      "schema": "public",
      "columns": [...],
      "primary_keys": ["product_id"],
      "foreign_keys": [...],
      "row_count": 5,
      "field_classifications": {...},
      "sensitive_fields": {...},
      "incremental_columns": ["product_id", "created_at", "updated_at"],
      "partition_columns": ["created_at"],
      "join_candidates": [...],
      "cdc_enabled": false,
      "has_primary_key": true,
      "has_foreign_keys": true,
      "has_sensitive_fields": false,
      "data_quality": {
        "controlled_value_candidates": [
          {"column": "category", "distinct_values": ["Bakery","Dairy"], "cardinality": 4, "severity": "warning", "recommendation": "..."}
        ],
        "delete_management": {
          "delete_strategy": "soft_delete",
          "soft_delete_column": "active",
          "soft_delete_type": "active_flag",
          "cdc_enabled": false,
          "has_audit_trail": false,
          "severity": "info",
          "detail": "...",
          "recommendation": "..."
        },
        "timezone": {
          "server_timezone": "UTC",
          "distinct_timezones": ["UTC"],
          "tz_aware_count": 0,
          "tz_naive_count": 2,
          "severity": "info",
          "recommendation": "..."
        },
        "findings": [...]
      }
    }
  ]
}
```

**Key design:** Each table's `data_quality` object contains typed sections (`controlled_value_candidates`, `nullable_but_never_null`, `delete_management`, `late_arriving_data`, `timezone`, etc.) plus a flat `findings` list for programmatic access. The global `data_quality_summary` provides aggregate counts. No data is duplicated.

## Schema Enrichment

Each table includes:
- **Field classifications**: pricing, quantity, categorical, temporal, contact
- **Sensitive fields**: PII, financial, credentials, network identity
- **Incremental columns**: for ETL watermarking
- **Partition columns**: for date partitioning
- **Column timezone**: effective timezone for date/time columns
- **Column statistics**: cardinality, null counts, data range
- **Join candidates**: implicit FK detection via naming patterns
- **Data categories**: nominal, ordinal, discrete, continuous
- **CDC status**: change data capture configuration

## Data Quality Checks (9 total)

| # | Check | Severity | What It Finds |
|---|-------|----------|---------------|
| 1 | **Controlled Value Candidates** | warning | Text columns with few distinct values but no CHECK/ENUM/FK constraint |
| 2 | **Nullable But Never Null** | info | Nullable columns that have zero NULLs — candidates for NOT NULL |
| 3 | **Missing Primary Keys** | critical | Tables without a primary key |
| 4 | **Missing Foreign Keys** | warning/critical | FK-patterned columns without FK constraints; detects orphaned references |
| 5 | **Format Inconsistency** | warning | Text columns with mixed format patterns (emails, phones, dates) |
| 6 | **Range Violations** | warning | Negative prices or quantities |
| 7 | **Delete Management** | warning/info | Per-table soft-delete vs hard-delete strategy; CDC detection; audit trail detection |
| 8 | **Late-Arriving Data** | warning/info | Lag between business dates and insertion timestamps; recommended lookback windows |
| 9 | **Timezone** | warning/info | Per-table and cross-database timezone assessment; TZ-aware vs TZ-naive; mixed-TZ warnings |

**Note:** Data quality checks run for PostgreSQL, Microsoft SQL Server / Azure SQL, and Oracle. For other dialects, `data_quality` will be empty.

## Script Reference

See `scripts/source_system_analyzer.py` for implementation details. Dialect-specific logic lives in `scripts/databases/`.

**Key function:**
- `analyze_source_system()` — Main function that analyzes schema and runs data quality checks in one pass
