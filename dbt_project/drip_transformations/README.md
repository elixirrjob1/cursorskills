# drip_transformations — dbt Project

Silver → Gold transformation layer for the Drip Data Intelligence platform.
Models land in Snowflake schemas `DBT_DEV` / `DBT_DEV_ENRICHED` (dev) and
`DBT_PROD` / `DBT_PROD_ENRICHED` (prod).

---

## Getting started

### 1. Prerequisites

| Tool | Install |
|---|---|
| [uv](https://docs.astral.sh/uv/) | `curl -Ls https://astral.sh/uv/install.sh \| sh` |
| dbt (Snowflake) | `uv tool install dbt-snowflake` |
| Cursor | [cursor.com](https://cursor.com) |

### 2. Clone & configure

```bash
git clone https://github.com/responsum-team/dbtproject.git
cd dbtproject
cp .env.example .env          # fill in your credentials (see below)
```

Open `.env` and set at minimum:

```dotenv
SNOWFLAKE_ACCOUNT=<account-id>        # e.g. xy12345.us-east-1
SNOWFLAKE_USER=<your-username>
SNOWFLAKE_FIVETRAN_PASSWORD=<password>
```

Everything else has defaults that match the project.

### 3. Authenticate with dbt Cloud

Pick **one**:

| Method | Steps |
|---|---|
| **OAuth** (recommended for local dev) | Run `uvx dbt-mcp auth` in this folder once. Token is cached in `mcp.yml` (gitignored). |
| **Service PAT** (CI / shared machines) | Add `DBT_PAT=dbtu_…` to `.env`. The startup script exports it as `DBT_TOKEN`. |

### 4. Open in Cursor

Open the cloned folder in Cursor. The dbt MCP server starts automatically
(`start-dbt-mcp.sh` is launched via `.cursor/mcp.json`).

You can now ask Cursor things like:
- *"Run all models"*
- *"Show me the lineage for FactSales"*
- *"Trigger a full-refresh prod job"*
- *"What failed in the last run?"*

---

## Cursor — dbt MCP features

| Feature | Status | How to unlock |
|---|---|---|
| Admin API (list/trigger jobs & runs) | ✅ On by default | — |
| Discovery API (lineage, model health) | 🔒 Off | Add `DBT_PROD_ENV_ID=<id>` to `.env`, flip `DISABLE_DISCOVERY=false` in `start-dbt-mcp.sh` |
| SQL execution | 🔒 Off | Add `DBT_DEV_ENV_ID=<id>` to `.env`, flip `DISABLE_SQL=false` |
| Semantic Layer | 🔒 Off | Requires dbt Cloud Semantic Layer licence |

Environment IDs are the numeric IDs in your dbt Cloud environment URLs
(`/deploy/<account>/projects/<project>/environments/<id>/`).

---

## dbt CLI usage

```bash
# Install dependencies
dbt deps

# Run all models (dev)
dbt run

# Full refresh (rebuild from scratch)
dbt run --full-refresh

# Run a single model
dbt run --select DimDate

# Test
dbt test

# Generate & serve docs
dbt docs generate && dbt docs serve
```

Profiles are read from `profiles.yml` in this directory — no `~/.dbt/profiles.yml` needed.

---

## Project structure

```
.
├── .cursor/
│   └── mcp.json             # Cursor MCP server config (auto-start)
├── models/
│   ├── views/               # vw_* staging views (silver layer)
│   └── enriched/            # Dim* / Fact* tables (gold layer)
├── tests/                   # singular & generic tests
├── macros/                  # reusable Jinja macros
├── profiles.yml             # Snowflake connection (reads from .env)
├── dbt_project.yml          # project config, materialisations, quoting
├── start-dbt-mcp.sh         # MCP server entrypoint
├── .env.example             # credential template
└── .gitignore               # excludes .env, mcp.yml, target/, logs/
```

---

## Notes

- **PascalCase identifiers** — all objects and columns in Snowflake use
  quoted PascalCase (`"DimDate"`, `"DateHashPK"`). This is enforced by
  `quoting: identifier: true` in `dbt_project.yml` and double-quoted
  `AS "ColumnName"` aliases in every model.
- **mcp.yml** is gitignored — it contains live OAuth tokens. Re-run
  `uvx dbt-mcp auth` after a fresh clone if using the OAuth flow.
- **dbt Cloud** is connected to this repo via GitHub App and runs the
  `dbt run --full-refresh` job on every push to `main`.
