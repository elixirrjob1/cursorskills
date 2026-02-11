---
name: data-quality
description: Assesses data integrity and quality of a source database, identifying controlled value list candidates, missing constraints, orphaned references, format inconsistencies, range violations, delete management strategy, late-arriving data patterns, and timezone handling. Use when users mention data quality, data integrity, source system readiness, ingestion preparation, delete management, late-arriving data, or timezone.
---

# Data Quality Analyzer

## Overview

This skill provides a tool for assessing data integrity and quality in a source database. It produces a JSON report with actionable findings — flagging columns that should use controlled value lists, missing constraints, orphaned references, and more.

**When to use:**
- User asks about data quality or data integrity assessment
- Preparing a source system for ingestion or migration
- Need to find columns that should be constrained to controlled value lists
- Want to identify missing NOT NULL, FK, or CHECK constraints
- Investigating referential integrity issues
- Looking for format inconsistencies in text columns

## Obtaining the Database Connection String

Follow the same priority as the database-analyser skill:

1. User-provided connection string
2. Environment variables: `DATABASE_URL`, `DB_URL`, `POSTGRES_URL`, `DB_CONNECTION_STRING`
3. `.env` file (script loads it automatically)
4. Ask the user if none found

**Connection string format:** `postgresql://[user[:password]@][host][:port][/database]`

## Prerequisites

**Required Python packages:**
- `sqlalchemy` — Database connectivity
- `psycopg2-binary` — PostgreSQL driver

Same as database-analyser. If `.venv` exists with those packages, no extra setup needed.

## Running the Script

Before running the script:

1. **Obtain the database connection string** (see above). If none found, ask the user.

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
   .venv/bin/python .cursor/skills/data-quality/scripts/data_quality.py <database_url> <output_json_path> [schema] [schema_json_path]
   ```

### Arguments

| Argument | Required | Description |
|----------|----------|-------------|
| `database_url` | Yes | PostgreSQL connection URL |
| `output_json_path` | Yes | Where to save the quality report (e.g. `data_quality_report.json`) |
| `schema` | No | Schema to analyze (default: from `DATABASE_SCHEMA` or `SCHEMA` env var, or `public`) |
| `schema_json_path` | No | Path to existing `schema.json` from database-analyser (skips re-scanning) |

### Examples

```bash
# Basic usage
.venv/bin/python .cursor/skills/data-quality/scripts/data_quality.py "$DATABASE_URL" data_quality_report.json public

# Reuse existing schema.json (faster — skips schema scanning)
.venv/bin/python .cursor/skills/data-quality/scripts/data_quality.py "$DATABASE_URL" data_quality_report.json public schema.json
```

## Quality Checks

The script runs 9 checks:

### 1. Controlled Value List Candidates (`controlled_value_candidate`)
**Severity: warning**

Finds text columns with low cardinality (<=20 distinct values) that lack CHECK, ENUM, or FK constraints. These are columns where free-form input should be replaced with a controlled value list.

Skips: primary keys, columns with existing constraints, known free-form columns (names, descriptions, emails, addresses, etc.)

### 2. Nullable But Never Null (`nullable_but_never_null`)
**Severity: info**

Finds columns defined as nullable that have zero NULL values. Suggests adding NOT NULL constraints.

### 3. Missing Primary Keys (`missing_primary_key`)
**Severity: critical**

Finds tables without any primary key defined.

### 4. Missing Foreign Keys (`missing_foreign_key`)
**Severity: warning or critical**

Finds columns following FK naming patterns (e.g. `customer_id`, `store_key`) that don't have FK constraints. Also checks for orphaned references — values that don't exist in the target table.

Severity escalates to **critical** if orphaned values are found.

### 5. Format Inconsistency (`format_inconsistency`)
**Severity: warning**

Samples text columns and checks for mixed format patterns (email, phone, date, URL, numeric). Flags columns where a dominant pattern exists but isn't consistently followed.

### 6. Range Violations (`range_violation`)
**Severity: warning**

Checks pricing and quantity columns for negative values where they shouldn't exist.

### 7. Delete Management (`delete_management`)
**Severity: warning (hard delete) or info (soft delete / CDC)**

Assesses how each table handles deletions — critical for determining ingestion strategy:

- **soft_delete**: Table has a soft-delete column (`deleted_at`, `is_deleted`) or active flag (`active` boolean). Deleted rows are preserved and can be detected incrementally.
- **hard_delete_with_cdc**: No soft-delete column, but CDC is enabled (REPLICA IDENTITY FULL/INDEX). Hard deletes can be captured via change data capture.
- **hard_delete**: No soft-delete column and no CDC. Deleted rows are lost — requires periodic full loads to detect removals.

Also detects audit-trail tables (`*_history`, `*_audit`, `*_log`) and reports the value distribution for soft-delete columns.

### 8. Late-Arriving Data (`late_arriving_data`)
**Severity: info or warning**

For tables that have both a business-date column (`order_date`, `transaction_date`, etc.) and a system-insertion timestamp (`created_at`), computes the lag between them. This reveals how far back data can land after the business event — critical for setting incremental-load lookback windows.

Reports:
- Min, avg, P95, and max lag in hours
- Number of rows arriving >1 day and >7 days late
- Recommended lookback window in days
- Which column is safer to use as the incremental watermark

Severity is **warning** when max lag exceeds 24 hours (late arrivals could be missed by simple watermarking). Otherwise **info**.

### 9. Timezone (`timezone`)
**Severity: info or warning**

Assesses timezone handling across all date/time columns in the database. For each table, reports:

- Server timezone
- Per-column effective timezone (UTC for timestamptz, server TZ for TZ-naive types)
- Count of TZ-aware vs TZ-naive columns
- Mixed-timezone warnings within a table and across the database

Severity is **warning** when mixed timezones are detected within a table or across the database (risk of silent conversion errors). Otherwise **info**.

Uses `column_timezone` from schema.json if available, otherwise classifies directly from column types and the server timezone.

## JSON Output Format

```json
{
  "metadata": {
    "generated_at": "2026-02-11T12:00:00+00:00",
    "database_url": "host:5432/db",
    "schema_filter": "public",
    "total_tables_analyzed": 10,
    "total_findings": 12
  },
  "summary": {
    "critical": 0,
    "warning": 8,
    "info": 4,
    "by_check": {
      "controlled_value_candidate": 4,
      "nullable_but_never_null": 4,
      "missing_foreign_key": 0,
      "format_inconsistency": 0,
      "range_violation": 0
    }
  },
  "findings": [
    {
      "table": "products",
      "column": "category",
      "check": "controlled_value_candidate",
      "severity": "warning",
      "detail": "Text column with 4 distinct value(s) ('Bakery', 'Beverages', 'Dairy', 'Produce') but no CHECK, ENUM, or FK constraint",
      "recommendation": "Add a CHECK constraint, convert to an ENUM type, or create a lookup/reference table to prevent invalid values",
      "distinct_values": ["Bakery", "Beverages", "Dairy", "Produce"],
      "cardinality": 4
    }
  ],
  "controlled_value_candidates": [
    {
      "table": "products",
      "column": "category",
      "distinct_values": ["Bakery", "Beverages", "Dairy", "Produce"],
      "cardinality": 4,
      "has_constraint": false
    }
  ],
  "constraints_found": {
    "check_constraints": 0,
    "enum_columns": 0,
    "unique_constraints": 2
  }
}
```

## Integration with Database Analyser

If a `schema.json` from the database-analyser skill exists, pass it as the 4th argument to skip re-scanning the database schema. The data quality script will use the column metadata (cardinality, null counts, data ranges) from the existing analysis and only query the database for constraint introspection and value-level checks.

## Script Reference

See `.cursor/skills/data-quality/scripts/data_quality.py` for implementation details.

**Key function:**
- `analyze_data_quality()` — Main function that runs all checks

**Check functions:**
- `check_controlled_value_candidates()` — Low-cardinality text columns without constraints
- `check_nullable_but_never_null()` — Nullable columns with zero NULLs
- `check_missing_primary_keys()` — Tables without primary keys
- `check_missing_foreign_keys()` — FK-patterned columns without FK constraints + orphan detection
- `check_format_inconsistency()` — Mixed format patterns in text columns
- `check_range_violations()` — Negative pricing/quantity values
- `check_delete_management()` — Soft-delete vs hard-delete strategy per table
- `check_late_arriving_data()` — Lag between business dates and insertion timestamps
- `check_timezone()` — TZ-aware vs TZ-naive columns, server timezone, mixed-TZ detection

**Constraint introspection:**
- `fetch_check_constraints()` — CHECK constraints from information_schema
- `fetch_enum_columns()` — ENUM types and their allowed values
- `fetch_unique_constraints()` — UNIQUE constraints
