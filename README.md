# Cursor Skills Repository

A collection of reusable Cursor AI skills for database analysis, data quality assessment, and capacity planning. Built for teams preparing source systems for data ingestion.

## Quick Start

```bash
# 1. Clone and set up
git clone <repo-url> && cd skills
python3 -m venv .venv
.venv/bin/pip install sqlalchemy psycopg2-binary

# 2. Configure your database connection
cp .env.example .env
# Edit .env with your DATABASE_URL and SCHEMA

# 3. Run any skill
.venv/bin/python .cursor/skills/database-analyser/scripts/database_analyzer.py "$DATABASE_URL" schema.json public
.venv/bin/python .cursor/skills/data-quality/scripts/data_quality.py "$DATABASE_URL" data_quality_report.json public schema.json
.venv/bin/python .cursor/skills/volume-projection/scripts/collector.py "$DATABASE_URL" --setup && \
.venv/bin/python .cursor/skills/volume-projection/scripts/collector.py "$DATABASE_URL" --collect
.venv/bin/python .cursor/skills/volume-projection/scripts/predictor.py "$DATABASE_URL" capacity_report.json
```

Or just ask Cursor: *"Analyze my database"*, *"Run a data quality check"*, *"Project storage growth"* — skills are picked up automatically.

## Available Skills

### 1. Database Analyzer

Connects to a PostgreSQL database and produces a fully enriched `schema.json` with comprehensive column-level metadata.

| Feature | What It Provides |
|---------|-----------------|
| Schema analysis | Tables, columns, primary keys, foreign keys |
| Column statistics | Cardinality, null counts, data ranges (min/max) |
| Field classification | Pricing, quantity, categorical, temporal, contact |
| Sensitive fields | PII, financial, credentials, network identity |
| Data categories | Nominal, ordinal, discrete, continuous |
| Join candidates | Implicit FK detection via naming patterns |
| ETL metadata | Incremental columns, partition columns, CDC status |
| Timezone | Per-column effective timezone (UTC, server TZ, offset) |

**Usage:**

```bash
.venv/bin/python .cursor/skills/database-analyser/scripts/database_analyzer.py \
  <database_url> <output_json> [schema]

# Example
.venv/bin/python .cursor/skills/database-analyser/scripts/database_analyzer.py \
  "$DATABASE_URL" schema.json public
```

**Output:** `schema.json` — one JSON file with everything. Used as input by the other skills.

---

### 2. Data Quality Analyzer

Assesses data integrity and quality of a source database. Produces a `data_quality_report.json` with actionable findings, severity levels, and recommendations.

Runs **9 checks**:

| # | Check | Severity | What It Finds |
|---|-------|----------|---------------|
| 1 | **Controlled Value Candidates** | warning | Text columns with few distinct values but no CHECK/ENUM/FK constraint — should use a controlled value list |
| 2 | **Nullable But Never Null** | info | Nullable columns that have zero NULLs — candidates for NOT NULL |
| 3 | **Missing Primary Keys** | critical | Tables without a primary key |
| 4 | **Missing Foreign Keys** | warning/critical | FK-patterned columns without FK constraints; detects orphaned references |
| 5 | **Format Inconsistency** | warning | Text columns with mixed format patterns (emails, phones, dates) |
| 6 | **Range Violations** | warning | Negative prices or quantities |
| 7 | **Delete Management** | warning/info | Per-table soft-delete vs hard-delete strategy; CDC detection; audit trail detection |
| 8 | **Late-Arriving Data** | warning/info | Lag between business dates and insertion timestamps; recommended lookback windows |
| 9 | **Timezone** | warning/info | Per-table and cross-database timezone assessment; TZ-aware vs TZ-naive; mixed-TZ warnings |

**Usage:**

```bash
.venv/bin/python .cursor/skills/data-quality/scripts/data_quality.py \
  <database_url> <output_json> [schema] [schema_json]

# Standalone (scans schema itself)
.venv/bin/python .cursor/skills/data-quality/scripts/data_quality.py \
  "$DATABASE_URL" data_quality_report.json public

# With existing schema.json (faster — skips re-scanning)
.venv/bin/python .cursor/skills/data-quality/scripts/data_quality.py \
  "$DATABASE_URL" data_quality_report.json public schema.json
```

**Output:** `data_quality_report.json` — findings with severity, detail, recommendations, plus summary sections for controlled value candidates, delete management, late-arriving data, and timezone.

---

### 3. Volume Projection

Collects table sizes, churn metrics, and growth history from PostgreSQL, stores them in a `prediction` schema, and generates capacity forecasts for 6/12/24 month horizons.

Two components:

| Component | What It Does |
|-----------|-------------|
| **Collector** | Creates `prediction` schema, gathers table sizes, row churn (inserts/updates/deletes from `pg_stat`), and monthly growth history from `created_at` columns |
| **Predictor** | Reads from `prediction` schema, computes growth trends, write profiles, and produces a capacity report with projections |

**Usage:**

```bash
# One-time setup (creates prediction schema and tables)
.venv/bin/python .cursor/skills/volume-projection/scripts/collector.py \
  "$DATABASE_URL" --setup

# Collect metrics (run regularly, e.g. monthly)
.venv/bin/python .cursor/skills/volume-projection/scripts/collector.py \
  "$DATABASE_URL" --collect --schema public

# Generate capacity report
.venv/bin/python .cursor/skills/volume-projection/scripts/predictor.py \
  "$DATABASE_URL" capacity_report.json
```

**Output:** `capacity_report.json` — per-table metrics (size, growth rate, write profile), projected sizes at 6/12/24 months, fastest-growing tables, and database-level summary.

## Recommended Workflow

Run the skills in this order for a complete source system assessment:

```
1. Database Analyzer  →  schema.json           (understand the schema)
2. Data Quality       →  data_quality_report.json  (assess readiness)
3. Volume Projection  →  capacity_report.json   (plan capacity)
```

The Data Quality skill accepts `schema.json` as input to avoid re-scanning — so running the Database Analyzer first makes everything faster.

## Feature Coverage

These skills address the following source system preparation concerns:

| Concern | Skill | Status |
|---------|-------|--------|
| Column-level metadata (cardinality, ranges, nulls, joins, data categories) | Database Analyzer | Done |
| Data integrity / quality (controlled values, constraints, format) | Data Quality (#1–6) | Done |
| Delete management (soft-delete, hard-delete, CDC) | Data Quality (#7) | Done |
| Late-arriving data (lag analysis, lookback windows) | Data Quality (#8) | Done |
| Timezone (server TZ, per-column TZ, mixed-TZ detection) | Data Quality (#9) | Done |
| Volume / size projection (growth trends, capacity forecasts) | Volume Projection | Done |

## Installation

### Global (Available in Every Project)

Copy the skills to your home directory. Cursor automatically picks up skills from `~/.cursor/skills/`, so they'll be available in every project you open — no per-project setup needed.

```bash
# Copy all skills globally
mkdir -p ~/.cursor/skills
cp -r .cursor/skills/* ~/.cursor/skills/

# Verify they're in place
ls ~/.cursor/skills/
# database-analyser/  data-quality/  volume-projection/
```

After copying, open any project in Cursor and the skills are immediately available. Just ask: *"Analyze my database"*, *"Run a data quality check"*, etc.

To update global skills later, just re-copy from this repo:

```bash
cp -r .cursor/skills/* ~/.cursor/skills/
```

### Per-Project

If you only want skills available in a specific project, copy them into that project's `.cursor/skills/` folder:

```bash
# Copy all skills into a project
mkdir -p /path/to/your/project/.cursor/skills
cp -r .cursor/skills/* /path/to/your/project/.cursor/skills/

# Or copy just one skill
mkdir -p /path/to/your/project/.cursor/skills
cp -r .cursor/skills/data-quality /path/to/your/project/.cursor/skills/
```

### Global vs Per-Project

| | Global (`~/.cursor/skills/`) | Per-Project (`.cursor/skills/`) |
|---|---|---|
| **Scope** | Every project you open in Cursor | Only that one project |
| **Team sharing** | Only on your machine | Commit to git, whole team gets it |
| **Updates** | Re-copy from this repo | Pull from git |
| **Best for** | Personal use, trying skills out | Team projects, CI/CD |

You can use both at the same time. Per-project skills take precedence if there's a name conflict.

### Requirements

- Python 3.7+
- `sqlalchemy`
- `psycopg2-binary`

```bash
python3 -m venv .venv
.venv/bin/pip install sqlalchemy psycopg2-binary
```

### Environment Variables

Create a `.env` file (see `.env.example`):

```
DATABASE_URL=postgresql://user:pass@host:5432/database
SCHEMA=public
```

All scripts load `.env` automatically. You can also pass the connection string directly as a CLI argument.

## Project Structure

```
.cursor/
  └── skills/
      ├── database-analyser/
      │   ├── README.md
      │   ├── SKILL.md
      │   └── scripts/
      │       └── database_analyzer.py
      ├── data-quality/
      │   ├── README.md
      │   ├── SKILL.md
      │   └── scripts/
      │       └── data_quality.py
      └── volume-projection/
          ├── SKILL.md
          └── scripts/
              ├── collector.py
              └── predictor.py
```

## Contributing

To add a new skill:

1. Create a new folder under `.cursor/skills/`
2. Include:
   - `SKILL.md` — AI instructions (tells Cursor when and how to use the skill)
   - `README.md` — User-facing documentation
   - `scripts/` — Implementation
3. Follow the existing patterns: single self-contained Python script, same connection string handling, same `.env` loading

## License

[Add your license here]
