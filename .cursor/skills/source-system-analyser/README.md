# Source System Analyser Skill for Cursor

A Cursor skill that analyzes source database schemas and assesses data quality in one pass, producing a combined `schema.json` with full metadata and actionable findings.

## Features

- **Complete schema analysis**: Tables, columns, primary keys, foreign keys
- **Enriched metadata**: Row counts, field classifications, sensitive fields detection, incremental columns, partition columns, CDC status
- **Column-level statistics**: Cardinality, null counts, data range, data categories
- **Data quality assessment**: 9 checks with severity levels and recommendations
- **Single output**: Everything in one `schema.json` file

## Quick Start

```bash
# Run against a database
.venv/bin/python .cursor/skills/source-system-analyser/scripts/source_system_analyzer.py \
  "$DATABASE_URL" schema.json public
```

## Requirements

- Python 3.7+
- `sqlalchemy`
- `psycopg2-binary`

```bash
python3 -m venv .venv
.venv/bin/pip install sqlalchemy psycopg2-binary
```

## Output

The script produces a single `schema.json` containing:

- **metadata**: generated_at, database_url, schema_filter, total_tables, total_rows, total_findings
- **connection**: host, port, database, driver, timezone
- **tables**: full schema with columns, PKs, FKs, enrichments per table
- **data_quality**: summary, findings, controlled_value_candidates, delete_management, late_arriving_data, timezone_summary, constraints_found

## Data Quality Checks

| Check | Severity | What It Finds |
|-------|----------|---------------|
| Controlled Value Candidates | warning | Text columns with few distinct values but no CHECK/ENUM/FK constraint |
| Nullable But Never Null | info | Nullable columns that have zero NULLs |
| Missing Primary Keys | critical | Tables without a primary key |
| Missing Foreign Keys | warning/critical | FK-patterned columns without FK constraints; orphaned references |
| Format Inconsistency | warning | Mixed format patterns in text columns |
| Range Violations | warning | Negative prices or quantities |
| Delete Management | warning/info | Soft-delete vs hard-delete strategy per table |
| Late-Arriving Data | warning/info | Lag between business dates and insertion timestamps |
| Timezone | warning/info | Per-table and cross-database timezone assessment |

Data quality checks run only for PostgreSQL.

## Usage

The skill is automatically available when you:
- Ask to analyze a database schema
- Request schema documentation
- Ask about data quality or data integrity
- Need to prepare a source system for ingestion

### Manual Script Usage

```bash
.venv/bin/python .cursor/skills/source-system-analyser/scripts/source_system_analyzer.py \
  "postgresql://user:pass@host/db" schema.json public
```

## Environment Variables

Create a `.env` file (see `.env.example`):

```
DATABASE_URL=postgresql://user:pass@host:5432/database
SCHEMA=public
```

All scripts load `.env` automatically. You can also pass the connection string directly as a CLI argument.
