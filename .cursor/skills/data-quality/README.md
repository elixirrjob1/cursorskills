# Data Quality Analyzer

Assesses data integrity and quality of a source database, producing a JSON report with actionable findings and recommendations.

> *"Garbage in, garbage out. Having quality data coming in will help the overall ingestion."* — The core principle behind this tool.

## What It Does

Connects to a PostgreSQL database and runs 9 quality checks:

| Check | Severity | What It Finds |
|-------|----------|---------------|
| **Controlled Value Candidates** | warning | Text columns with few distinct values but no CHECK/ENUM/FK constraint |
| **Nullable But Never Null** | info | Nullable columns that have zero NULLs (candidates for NOT NULL) |
| **Missing Primary Keys** | critical | Tables without a primary key |
| **Missing Foreign Keys** | warning/critical | FK-patterned columns without FK constraints; orphaned references |
| **Format Inconsistency** | warning | Text columns with mixed format patterns (emails, phones, dates) |
| **Range Violations** | warning | Negative prices or quantities |
| **Delete Management** | warning/info | Per-table assessment of soft-delete vs hard-delete strategy for ingestion planning |
| **Late-Arriving Data** | warning/info | Measures lag between business dates and insertion timestamps; recommends lookback windows |
| **Timezone** | warning/info | Per-table and cross-database timezone assessment; TZ-aware vs TZ-naive columns; mixed-TZ detection |

## Quick Start

```bash
# Run against a database
.venv/bin/python .cursor/skills/data-quality/scripts/data_quality.py \
  "postgresql://user:pass@host:5432/db" \
  data_quality_report.json \
  public

# Reuse existing schema.json from database-analyser (faster)
.venv/bin/python .cursor/skills/data-quality/scripts/data_quality.py \
  "$DATABASE_URL" \
  data_quality_report.json \
  public \
  schema.json
```

## Requirements

- Python 3.7+
- `sqlalchemy`
- `psycopg2-binary`

```bash
# If .venv doesn't exist yet
python3 -m venv .venv
.venv/bin/pip install sqlalchemy psycopg2-binary
```

## Example Output

Running against a sample retail database:

```
INFO: Starting data quality analysis for schema: public
INFO: Loading schema metadata from schema.json
INFO: Fetching constraint metadata…
INFO: Check 1/9: Controlled value list candidates…
INFO: Check 2/9: Nullable but never-null columns…
INFO: Check 3/9: Missing primary keys…
INFO: Check 4/9: Missing foreign keys & orphaned references…
INFO: Check 5/9: Format inconsistencies…
INFO: Check 6/9: Range / domain violations…
INFO: Check 7/9: Delete management assessment…
INFO: Check 8/9: Late-arriving data assessment…
INFO: Check 9/9: Timezone assessment…
INFO: Done — 43 finding(s) across 10 table(s)
INFO:   Critical : 0
INFO:   Warning  : 18
INFO:   Info     : 25
```

### Key Findings

**Controlled value candidates** — Columns like `status`, `category`, and `role` that have a small set of valid values but aren't enforced by a database constraint:

```json
{
  "table": "products",
  "column": "category",
  "check": "controlled_value_candidate",
  "severity": "warning",
  "detail": "Text column with 4 distinct value(s) ('Bakery', 'Beverages', 'Dairy', 'Produce') but no CHECK, ENUM, or FK constraint",
  "recommendation": "Add a CHECK constraint, convert to an ENUM type, or create a lookup/reference table to prevent invalid values"
}
```

**Nullable but never null** — Columns that could be tightened:

```json
{
  "table": "customers",
  "column": "email",
  "check": "nullable_but_never_null",
  "severity": "info",
  "detail": "Column is nullable but has 0 NULLs across 3 row(s)",
  "recommendation": "Consider adding a NOT NULL constraint if the column should always have a value"
}
```

## Integration with Database Analyser

This tool works standalone or alongside the [Database Analyser](../database-analyser/README.md):

1. **Standalone** — Performs its own lightweight schema scan
2. **With schema.json** — Pass the output of the database analyser as the 4th argument to skip re-scanning. The quality analyzer uses existing column metadata (cardinality, null counts, data ranges) and only queries the database for constraint introspection.

## How It Decides What to Flag

### Controlled Value List Detection

The most impactful check. For every text column, it asks:

1. Is cardinality low? (<=20 distinct values)
2. Is there an existing CHECK constraint, ENUM type, FK, or UNIQUE constraint? If yes, skip.
3. Does the column name suggest free-form content? (names, descriptions, emails, addresses, etc.) If yes, skip.
4. **Flag it** with the distinct values and a recommendation to add a constraint.

### Free-Form Column Detection

These columns are skipped for controlled-value analysis since their content is inherently variable:
- Person names (`first_name`, `last_name`, `display_name`, etc.)
- Descriptions and comments (`description`, `note`, `body`, etc.)
- Contact info (`email`, `phone`, `address`, etc.)
- Identifiers (`sku`, `barcode`, `code`, `uuid`, etc.)
- Secrets (`password`, `token`, `api_key`, etc.)
