# Project Onboarding Guide

---

## 1. What is this project?

This is an AI-assisted data platform engagement. Cursor AI skills automate the entire data engineering pipeline — from source system governance and analysis, through ingestion and transformation, to a fully enriched data catalog.

All Cursor skills, rules, and MCP configurations are distributed via the **`responsum-team/plugins`** repo, which is pushed to every team member's Cursor environment through the Responsum team marketplace.

**Reference implementation:** A source ERP system is ingested via Fivetran into Snowflake, transformed by dbt into a dimensional model, and governed in OpenMetadata.

---

## 2. Prerequisites — access you need before starting

Request access to the following before your first day of active work:

| System | What you need |
|--------|---------------|
| **GitHub** | Read on `responsum-team/plugins` (skills, rules, MCPs — installed via team marketplace) and read/write on `responsum-team/dbtproject` (primary dbt repo — this is what dbt Cloud runs) |
| **Bitwarden** | Member of the Elixir org with access to the **Training Credentials** collection — item **Cursor onboarding – sample .env** |
| **Snowflake** | Account access and the appropriate role on the project database |
| **dbt Cloud** | Member access on the project in dbt Cloud |
| **Fivetran** | Viewer or Connector Creator on the project destination group |
| **OpenMetadata** | User account on the team OpenMetadata instance |
| **Cursor IDE** | [cursor.com](https://cursor.com) — any recent version |

Ask a team admin for the specific account URLs and role names for Snowflake, dbt Cloud, and OpenMetadata.

---

## 3. Local setup

### 3.1 Install the Cursor plugin

All skills, rules, and MCP configurations are distributed as a single plugin from `responsum-team/plugins` via the **Responsum Cursor team marketplace**. You do not clone a skills repo manually — Cursor installs it for you.

When you open Cursor and connect to the Responsum team, you will be prompted to install the latest plugin update. Accept the prompt. This pushes all skills, rules, and MCP server configs into your local Cursor environment automatically. The only thing you may need to update manually after install is MCP credentials (see §3.3).

> If you are not yet seeing the install prompt, ask a team admin to confirm you have been added to the Responsum team in Cursor.

**dbt repo** (primary dbt project — dbt Cloud runs directly from this):

```bash
git clone https://github.com/responsum-team/dbtproject.git
cd dbtproject
cp .env.example .env   # fill in Snowflake credentials
```

Open `dbtproject` in Cursor for all dbt work — the dbt MCP server is configured in its own `.cursor/mcp.json` and starts automatically.

### 3.2 Configure secrets via `.env`

There are two `.env` files — one per repo. Both have an `.env.example` to copy from.

**`dbtproject/`** — for dbt and Snowflake work:
```bash
cd dbtproject
cp .env.example .env
```

**`responsum-team/plugins`** (or the local skills context provided by your team) — for source analysis, catalog, and ingestion scripts:
```bash
cp .env.example .env
```

Copy the training values from Bitwarden into your local `.env` files. Variable names come from `.env.example`; values are in Bitwarden:

**Collection:** Training Credentials → **Item:** Cursor onboarding – sample .env

[Open in Bitwarden](https://vault.bitwarden.com/#/vault?collectionId=12a08d6f-8af4-443e-970a-ab7400a607b2&itemId=004c5dd0-d1a6-43ca-8e73-b45000c5a175&action=view)

> You must be logged into Bitwarden with Elixir org access. Ask a team admin if the link does not open or the item is missing.

**Key Vault:** Some scripts support loading from Azure Key Vault when `KEYVAULT_NAME` is set. That applies only if you have already created a vault, stored secrets in it, and been granted access — it is not the onboarding path. See [KEYVAULT_SETUP.md](KEYVAULT_SETUP.md).

#### `dbtproject/.env` — required variables

| Variable | What it is |
|----------|------------|
| `SNOWFLAKE_ACCOUNT` | Snowflake account identifier (format: `orgname-accountname`) |
| `SNOWFLAKE_USER` | Snowflake user for the Fivetran ingestion service account |
| `SNOWFLAKE_FIVETRAN_PASSWORD` | Password for the Fivetran Snowflake user |
| `SNOWFLAKE_DATABASE` | Snowflake database name for the project |
| `SNOWFLAKE_WAREHOUSE` | Snowflake warehouse used for ingestion queries |
| `SNOWFLAKE_DBT_USER` | Separate dbt transformation service account user |
| `SNOWFLAKE_DBT_ROLE` | Role for the dbt service account |
| `SNOWFLAKE_DBT_WAREHOUSE` | Warehouse for dbt transformation runs |
| `SNOWFLAKE_DBT_PASSWORD` | Password for the dbt service account |
| `DBT_HOST` | dbt Cloud tenant host (e.g. `<id>.us1.dbt.com`) |
| `DBT_ACCOUNT_ID` | Numeric dbt Cloud account ID (from the URL in dbt Cloud settings) |

#### `dbtproject/.env` — optional variables (enable advanced MCP features)

| Variable | What it is |
|----------|------------|
| `DBT_PAT` | dbt Cloud service PAT — alternative to OAuth, used for CI or shared machines |
| `DBT_USER_ID` | Your numeric dbt Cloud user ID (from your profile URL in dbt Cloud) |
| `DBT_PROD_ENV_ID` | Numeric ID of the production dbt Cloud environment — unlocks Discovery API |
| `DBT_DEV_ENV_ID` | Numeric ID of the dev dbt Cloud environment — unlocks SQL execution via MCP |
| `DISABLE_DISCOVERY` | Set to `false` to enable lineage/model health tools (requires `DBT_PROD_ENV_ID`) |
| `DISABLE_SQL` | Set to `false` to enable SQL execution via MCP (requires `DBT_DEV_ENV_ID`) |
| `DISABLE_SEMANTIC_LAYER` | Set to `false` to enable Semantic Layer tools (requires dbt Cloud licence) |

#### Skills/scripts `.env` — for source analysis, catalog, and ingestion tooling

| Variable | What it is |
|----------|------------|
| `KEYVAULT_NAME` | Azure Key Vault name — if set, secrets load from Key Vault instead of `.env` |
| `OM_BASE_URL` | OpenMetadata instance URL (e.g. `https://<host>:8585`, no trailing slash) |
| `OM_TOKEN` | Long-lived JWT from OpenMetadata (Settings → Access Tokens or Bots) |
| `FIVETRAN_API_KEY` / `FIVETRAN_API_SECRET` | Fivetran API credentials for the MCP and ingestion scripts |
| `AZURE_MSSQL_URL` | Connection string for the Azure SQL source database |
| `AZURE_MSSQL_SCHEMA` | Schema name in the Azure SQL source (typically `dbo`) |
| `DATABASE_URL` | PostgreSQL connection string (used by source analyser scripts) |
| `API_AUTH_TOKEN` | Bearer token for the project REST API (if applicable) |

### 3.3 Configure MCP credentials

MCP server configurations (Snowflake, OpenMetadata, Fivetran, dbt) are included in the plugin and installed automatically. After the plugin installs, you will need to supply your own credentials for each MCP connection — the plugin cannot carry these for you.

Update the relevant entries in `~/.cursor/mcp.json` (or the per-project `mcp.json` in `dbtproject/`) with your personal credentials for:
- Snowflake (account, username, password/key)
- OpenMetadata (`OM_BASE_URL` and `OM_TOKEN` JWT in `.env`)
- Fivetran (API key)

The dbt MCP uses OAuth or a service PAT — run `uvx dbt-mcp auth` inside the `dbtproject` folder once to authenticate.

After any credential change, restart Cursor or use **Cursor → Reload MCP**.

---

## 4. Tech stack at a glance

```
Source ERP database
        │
        │  Fivetran (ingestion connector)
        ▼
Snowflake — Bronze schema (raw landed data)
        │
        │  dbt (project in responsum-team/dbtproject, runs on dbt Cloud)
        ▼
Snowflake — Silver schema (staging views)
           Gold schema (dimensional model — Dim* / Fact*)
        │
        │  OpenMetadata ingestion + enrichment
        ▼
OpenMetadata catalog (glossary, classification tags, column descriptions, lineage)
        │
        │  Cursor Skills + MCP (AI automation layer)
        ▼
Analysts / Engineers — governed, queryable, documented data
```

**Medallion layers:**

| Layer | Contents |
|-------|----------|
| Bronze | Raw tables as landed by Fivetran |
| Silver | Staging views, cleaned and typed |
| Gold | Dimensional model (`Dim*`, `Fact*`) |

The specific Snowflake database and schema names are in `.env` / `.env.example` in the dbt repo.

---

## 5. Repo map

There are three places this documentation lives — same content, mirrored for convenience:

| Copy | Path | How it gets there |
|------|------|-------------------|
| **Canonical (edit here)** | `elixirrjob1/cursorskills/docs/` | Direct commit to main |
| **dbt project** | `responsum-team/dbtproject/docs/` | `./scripts/sync_onboarding_docs.sh` then `./scripts/sync_dbt_secondary_repo.sh` |
| **Plugins** | `responsum-team/plugins/docs/` | `./scripts/sync_onboarding_docs.sh` then `./scripts/sync_plugins_secondary_repo.sh` |

After changing any file under `docs/`, run `./scripts/sync_onboarding_docs.sh` before syncing to the secondary repos.

There are two repos you interact with for day-to-day work:

### `responsum-team/plugins` — installed via Cursor team marketplace

This is the distribution repo for all Cursor agent tooling. You do not clone it directly; Cursor installs it automatically. It contains:

```
plugins/
├── docs/            ← platform onboarding + runbooks (mirrored from cursorskills/docs/)
├── skills/          ← all Cursor skills (source-system-analyser, dbt-model-from-stm, etc.)
├── rules/           ← agent guardrails (.mdc rule files)
├── mcps/            ← MCP server configs (Snowflake, OpenMetadata, Fivetran, dbt)
└── agents/          ← any registered Cursor agents
```

Each skill inside `plugins/skills/` also has its own standalone repo under `responsum-team/skill-<name>` — these are kept in sync automatically via GitHub Actions whenever a skill is updated.

### `responsum-team/dbtproject` — clone this for dbt work

```
dbtproject/
├── docs/
│   ├── README.md        ← doc index (mirrored from cursorskills/docs/)
│   └── ONBOARDING.md    ← platform onboarding (Bitwarden, env, MCP, pipeline)
├── .cursor/
│   └── mcp.json         ← dbt MCP server config (auto-start in Cursor)
├── models/
│   ├── views/           ← silver staging views
│   └── enriched/        ← gold Dim* / Fact* tables
├── tests/               ← singular and generic dbt tests
├── macros/              ← reusable Jinja macros
├── profiles.yml         ← Snowflake connection (reads from .env)
├── dbt_project.yml      ← project config, materialisations, quoting
├── start-dbt-mcp.sh     ← MCP server entrypoint
└── .env.example         ← credential template
```

---

## 6. End-to-end pipeline

The full pipeline has 6 steps. For detailed descriptions and a visual diagram see [end-to-end-flow.md](end-to-end-flow.md).

**Quick summary:**

| Step | Skill(s) | Output |
|------|----------|--------|
| 1. Governance authoring | `schema-glossary-generator`, `governance-vocab-generator` | Glossary terms JSON + classification vocabulary `.md` |
| 2. Publish to catalog | `catalog-vocab-publisher` | Glossary + classifications live in OpenMetadata |
| 3. Source system analysis | `source-system-analyser` | `schema.json` — schema, descriptions, data quality, tags |
| 4. Ingestion and landing | `ingestion-from-analyzer`, `database-setup` | Fivetran recommendations + Snowflake destination provisioned → bronze data |
| 5. Catalog sync and enrichment | `catalog-sync`, `catalog-glossary-tagger` | Bronze tables in OpenMetadata, fully tagged |
| 6. Source-to-target mapping | `stm-from-data-model`, `stm-catalogue-enricher`, `dbt-model-from-stm` | STM docs + generated dbt models |

Each skill has its own `SKILL.md` — that file is what Cursor reads to decide when and how to invoke the skill.

---

## 7. Working with dbt

**The primary dbt repo is `responsum-team/dbtproject`.** Clone and open that repo directly for all dbt development — dbt Cloud is connected to it via GitHub App and runs on every push to `main`.

For local dbt CLI work (from inside the cloned `dbtproject` folder):

```bash
dbt deps
dbt run                      # run all models
dbt run --select <ModelName> # run one model
dbt test                     # run tests
dbt docs generate && dbt docs serve
```

For Cursor-assisted dbt work, open `dbtproject` in Cursor — the dbt MCP server starts automatically. Ask Cursor:
- *"What failed in the last dbt run?"*
- *"Show me the lineage for a specific model"*
- *"Run all enriched models"*

See the [dbtproject docs/ONBOARDING.md](https://github.com/responsum-team/dbtproject/blob/main/docs/ONBOARDING.md) for dbt Cloud auth (OAuth vs PAT) and MCP feature flags.

---

## 8. Your first task

Once you have completed setup, ask Cursor:

> *"Check my setup"*

The `setup-verifier` skill will test all four MCP connections and your `.env` in one go and tell you exactly what to fix if anything fails.

---

## 9. Key links and references

Ask a team admin to fill in the specific URLs for your environment:

| Resource | Location |
|----------|----------|
| dbt Cloud | Your team's dbt Cloud account |
| Sample `.env` (Bitwarden) | **Training Credentials** → **Cursor onboarding – sample .env** — [open link](https://vault.bitwarden.com/#/vault?collectionId=12a08d6f-8af4-443e-970a-ab7400a607b2&itemId=004c5dd0-d1a6-43ca-8e73-b45000c5a175&action=view) |
| Azure Key Vault | Optional — see [KEYVAULT_SETUP.md](KEYVAULT_SETUP.md) only if you maintain your own vault with secrets |
| Plugins repo (skills, rules, MCPs) | [responsum-team/plugins](https://github.com/responsum-team/plugins) |
| dbt project repo | [responsum-team/dbtproject](https://github.com/responsum-team/dbtproject) |
| End-to-end flow | [docs/end-to-end-flow.md](end-to-end-flow.md) (also in dbtproject and plugins `docs/`) |
| Key Vault setup | [docs/KEYVAULT_SETUP.md](KEYVAULT_SETUP.md) |
| Fivetran runbooks | [FIVETRAN_TROUBLESHOOTING.md](FIVETRAN_TROUBLESHOOTING.md), [FIVETRAN_ADVANCED_TUNING.md](FIVETRAN_ADVANCED_TUNING.md) |
| OpenMetadata + glossary | [docs/README.md](README.md) |
| Sync docs to dbt + plugins repos | `./scripts/sync_onboarding_docs.sh` from cursorskills root |
