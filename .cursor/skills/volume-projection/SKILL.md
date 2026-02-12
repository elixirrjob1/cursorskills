---
name: volume-projection
description: Collects volume and capacity metrics from PostgreSQL databases, stores them in a prediction schema, and generates capacity forecasts. Use when planning storage, backup windows, capacity allocation, or when users mention volume projection, growth trends, or capacity planning.
---

# Volume Projection Skill

## Overview

This skill provides tools for capacity planning by collecting table sizes, churn metrics, and growth history from PostgreSQL databases, storing them in a `prediction` schema, and generating capacity forecasts.

**When to use:**
- User asks about volume projection, capacity planning, or growth trends
- Need to plan storage, backup windows, or resource allocation
- Want to understand table growth patterns over time
- Need capacity forecasts for 6/12/24 month horizons

## Architecture

The skill has two components:

1. **Collector** (`collector.py`) — Creates the `prediction` schema, collects table sizes, churn metrics, and growth history from `created_at` columns, and stores everything in the database.

2. **Predictor** (`predictor.py`) — Reads from the `prediction` schema, computes growth trends and write profiles, and generates a capacity report with projections.

## Obtaining the Database Connection String

Follow the same priority as the source-system-analyser skill:

1. User-provided connection string
2. Environment variables: `DATABASE_URL`, `DB_URL`, `POSTGRES_URL`, `DB_CONNECTION_STRING`
3. `.env` file (script loads it automatically)
4. Ask the user if none found

**Connection string format:** `postgresql://[user[:password]@][host][:port][/database]`

## Prerequisites

**Required Python packages:**
- `sqlalchemy` — Database connectivity
- `psycopg2-binary` — PostgreSQL driver

Same as source-system-analyser. If `.venv` exists with those packages, no extra setup needed.

## Running the Collector

### 1. Setup (one-time)

Creates the `prediction` schema and tables (`collection_runs`, `table_size_snapshots`, `growth_history`, `database_snapshots`):

```bash
.venv/bin/python .cursor/skills/volume-projection/scripts/collector.py <database_url> --setup
```

### 2. Collect (run periodically)

Runs a full collection: table sizes, churn from `pg_stat_user_tables`, growth history from `created_at` columns (2 years back), and database-level metrics:

```bash
.venv/bin/python .cursor/skills/volume-projection/scripts/collector.py <database_url> --collect
```

Optional `--schema` (default: `public`):

```bash
.venv/bin/python .cursor/skills/volume-projection/scripts/collector.py "$DATABASE_URL" --collect --schema public
```

**Recommendation:** Run `--collect` regularly (e.g., monthly) to build historical data for better projections.

## Running the Predictor

Reads from the `prediction` schema and writes a capacity report:

```bash
.venv/bin/python .cursor/skills/volume-projection/scripts/predictor.py <database_url> <output_path>
```

Example:

```bash
.venv/bin/python .cursor/skills/volume-projection/scripts/predictor.py "$DATABASE_URL" capacity_report.json
```

Default output path is `capacity_report.json` if omitted.

## Output Format

The predictor produces a JSON report with:

- **summary** — Current total size, projected 6/12/24 month sizes, fastest growing tables, largest tables
- **tables** — Per-table metrics: current row count, size, avg row size, bloat ratio, write profile; growth (avg monthly growth, trend direction); projections (estimated rows and size at each horizon)
- **database** — Database-level metrics (total size, config, temp file usage)

Write profiles: `append_only`, `update_heavy`, `delete_heavy`, `mixed`, `unknown`

Trend directions: `increasing`, `stable`, `decreasing`

## Prediction Schema Tables

| Table | Purpose |
|-------|---------|
| `prediction.collection_runs` | Tracks each collection run |
| `prediction.table_size_snapshots` | Point-in-time size and churn per table |
| `prediction.growth_history` | Monthly row growth from `created_at` columns |
| `prediction.database_snapshots` | Database-level metrics |

## Best Practices

1. Run `--setup` once before the first `--collect`
2. Run `--collect` regularly (e.g., monthly) to build growth history
3. Tables need a `created_at` (or `created_date`, `inserted_at`) column for growth history
4. Projections improve with more historical data points
5. Use the capacity report as input for planning; layer business context (expansion, seasonal patterns) separately

## Script Reference

- **collector.py** — `--setup` creates schema/tables; `--collect` gathers metrics
- **predictor.py** — Reads prediction schema, outputs `capacity_report.json`
