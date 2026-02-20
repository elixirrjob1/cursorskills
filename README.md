# Cursor Skills Repository

A collection of reusable Cursor AI skills for database analysis, data quality assessment, and capacity planning. Built for teams preparing source systems for data ingestion.

## Quick Start

```bash
# 1. Clone and install skills globally (recommended)
git clone <repo-url> && cd skills
mkdir -p ~/.cursor/skills
cp -r .cursor/skills/* ~/.cursor/skills/

# 2. Set up Python env in your project (the one with your database)
cd /path/to/your/project
python3 -m venv .venv
.venv/bin/pip install sqlalchemy psycopg2-binary

# 3. Configure your database connection
cp .env.example .env
# Edit .env with your DATABASE_URL and SCHEMA

# 4. Run any skill (from your project dir)
.venv/bin/python ~/.cursor/skills/source-system-analyser/scripts/source_system_analyzer.py "$DATABASE_URL" schema.json public
.venv/bin/python ~/.cursor/skills/volume-projection/scripts/collector.py "$DATABASE_URL" --setup && \
.venv/bin/python ~/.cursor/skills/volume-projection/scripts/collector.py "$DATABASE_URL" --collect
.venv/bin/python ~/.cursor/skills/volume-projection/scripts/predictor.py "$DATABASE_URL" capacity_report.json
```

Or just ask Cursor: *"Analyze my database"*, *"Run a data quality check"*, *"Project storage growth"* — skills are picked up automatically.

## Available Skills

### 1. Source System Analyser

Connects to a PostgreSQL database and produces a combined `schema.json` with full schema metadata and data quality findings in one pass.

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
| Data quality | 9 checks: controlled values, constraints, format, delete management, late-arriving data, timezone |

**Usage:**

```bash
.venv/bin/python ~/.cursor/skills/source-system-analyser/scripts/source_system_analyzer.py \
  <database_url> <output_json> [schema]

# Example
.venv/bin/python ~/.cursor/skills/source-system-analyser/scripts/source_system_analyzer.py \
  "$DATABASE_URL" schema.json public
```

**Output:** `schema.json` — one JSON file with schema metadata and `data_quality` section (findings, summary, recommendations). Used as input by Volume Projection.

---

### 2. Volume Projection

Collects table sizes, churn metrics, and growth history from PostgreSQL, stores them in a `prediction` schema, and generates capacity forecasts for 6/12/24 month horizons.

Two components:

| Component | What It Does |
|-----------|-------------|
| **Collector** | Creates `prediction` schema, gathers table sizes, row churn (inserts/updates/deletes from `pg_stat`), and monthly growth history from `created_at` columns |
| **Predictor** | Reads from `prediction` schema, computes growth trends, write profiles, and produces a capacity report with projections |

**Usage:**

```bash
# One-time setup (creates prediction schema and tables)
.venv/bin/python ~/.cursor/skills/volume-projection/scripts/collector.py \
  "$DATABASE_URL" --setup

# Collect metrics (run regularly, e.g. monthly)
.venv/bin/python ~/.cursor/skills/volume-projection/scripts/collector.py \
  "$DATABASE_URL" --collect --schema public

# Generate capacity report
.venv/bin/python ~/.cursor/skills/volume-projection/scripts/predictor.py \
  "$DATABASE_URL" capacity_report.json
```

**Output:** `capacity_report.json` — per-table metrics (size, growth rate, write profile), projected sizes at 6/12/24 months, fastest-growing tables, and database-level summary.

## Recommended Workflow

Run the skills in this order for a complete source system assessment:

```
1. Source System Analyser  →  schema.json         (schema + data quality in one pass)
2. Volume Projection       →  capacity_report.json (plan capacity)
```

## Feature Coverage

These skills address the following source system preparation concerns:

| Concern | Skill | Status |
|---------|-------|--------|
| Column-level metadata (cardinality, ranges, nulls, joins, data categories) | Source System Analyser | Done |
| Data integrity / quality (controlled values, constraints, format) | Source System Analyser | Done |
| Delete management (soft-delete, hard-delete, CDC) | Source System Analyser | Done |
| Late-arriving data (lag analysis, lookback windows) | Source System Analyser | Done |
| Timezone (server TZ, per-column TZ, mixed-TZ detection) | Source System Analyser | Done |
| Volume / size projection (growth trends, capacity forecasts) | Volume Projection | Done |

## Installation

**Always install skills globally.** Copy them to `~/.cursor/skills/` — Cursor picks them up automatically and they’ll be available in every project. Global install keeps skills outside the agent’s workspace, so the agent won’t try to modify them.

```bash
# Install globally (recommended)
mkdir -p ~/.cursor/skills
cp -r .cursor/skills/* ~/.cursor/skills/

# Verify
ls ~/.cursor/skills/
# source-system-analyser/  volume-projection/
```

Then open your project in Cursor and ask: *"Analyze my database"*, *"Run a data quality check"*, etc.

To update skills later:
```bash
cd /path/to/skills-repo && cp -r .cursor/skills/* ~/.cursor/skills/
```

### Cursor MCP (optional)

To use Azure tools (e.g. Key Vault, Storage, App Service) from Cursor, add the **Azure MCP Server** to your Cursor MCP config. If it isn’t already there, run from the repo root:

```bash
./scripts/setup_cursor_mcp.sh
```

This creates `~/.cursor/mcp.json` if missing, or adds the Azure MCP Server entry to `mcpServers` without overwriting existing servers. Override the config path with `CURSOR_MCP_JSON=/path/to/mcp.json`. Requires `jq`; if `jq` is not installed, the script prints the snippet to add manually.

**Manual add:** In `~/.cursor/mcp.json` (create it if needed), ensure `mcpServers` contains:

```json
"Azure MCP Server": {
  "command": "npx",
  "args": ["-y", "@azure/mcp@latest", "server", "start"]
}
```

Restart Cursor or reload MCP after changing the file.

### Per-Project (Not Recommended)

Per-project install (`.cursor/skills/` in your project) puts skills inside the agent’s workspace; the agent may try to edit them. Use global install instead. If you must use per-project, this repo includes rules and `.cursorignore` to reduce that risk when the repo itself is the opened project.

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

### Simulated API (Azure)

A read-only REST API is deployed as an Azure Function for testing and integration:

| Item | Value |
|------|--------|
| **Base URL** | `https://skillssimapifilip20260218.azurewebsites.net` |
| **Auth** | `Authorization: Bearer <token>` (required) |
| **Endpoints** | `GET /api/tables`, `GET /api/{table}?limit=100&offset=0` |

Response notes:
- `GET /api/tables` returns `tables` as table metadata objects (schema-aligned structural metadata), not only table names.
- `GET /api/{table}` returns `metadata` plus `data`.

The **API key (Bearer token)** is stored in **Azure Key Vault**; do not commit it. For local use, set `API_AUTH_TOKEN` in `.env` or obtain it from Key Vault (e.g. via `KEYVAULT_NAME` and your Key Vault loader). See `API_CONNECTION_INSTRUCTIONS.txt` for details.

## Project Structure

```
.cursor/
  └── skills/
      ├── source-system-analyser/
      │   ├── README.md
      │   ├── SKILL.md
      │   └── scripts/
      │       └── source_system_analyzer.py
      └── volume-projection/
          ├── SKILL.md
          └── scripts/
              ├── collector.py
              └── predictor.py
```

## Agent Usage

Skills in `.cursor/skills/` are **read-only**. The Cursor agent is instructed to use them but never modify them (via `.cursor/rules/skills-readonly.mdc` and `.cursorignore`). To customize or extend a skill, copy it elsewhere or contribute upstream.

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
