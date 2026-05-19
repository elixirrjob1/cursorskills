# Project Onboarding Guide

**Goal:** Get a new team member productive on this project within 1–2 hours.

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
| **Azure Key Vault** | Reader + Secrets User role on the team Key Vault — this is where all credentials live |
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

Copy the example env file and populate it with the credentials from the team Key Vault:

```bash
cp .env.example .env
```

The `.env.example` file lists all required secret names. Obtain the actual values from the team Key Vault (ask a team admin for the vault name and access). Key categories of secrets:

| Category | What it covers |
|----------|---------------|
| Source database connection | Connection string and schema for the ERP source |
| Snowflake credentials | Account, user, role, warehouse, database |
| OpenMetadata connection | Base URL, email, password |
| API auth token | Bearer token for the project API (if applicable) |

To load secrets from Key Vault programmatically (if the loader script is set up):

```bash
python scripts/keyvault_loader.py
```

### 3.3 Configure MCP credentials

MCP server configurations (Snowflake, OpenMetadata, Fivetran, dbt) are included in the plugin and installed automatically. After the plugin installs, you will need to supply your own credentials for each MCP connection — the plugin cannot carry these for you.

Update the relevant entries in `~/.cursor/mcp.json` (or the per-project `mcp.json` in `dbtproject/`) with your personal credentials for:
- Snowflake (account, username, password/key)
- OpenMetadata (base URL, email, password)
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

There are two repos you interact with:

### `responsum-team/plugins` — installed via Cursor team marketplace

This is the distribution repo for all Cursor agent tooling. You do not clone it directly; Cursor installs it automatically. It contains:

```
plugins/
├── skills/          ← all Cursor skills (source-system-analyser, dbt-model-from-stm, etc.)
├── rules/           ← agent guardrails (.mdc rule files)
├── mcps/            ← MCP server configs (Snowflake, OpenMetadata, Fivetran, dbt)
└── agents/          ← any registered Cursor agents
```

Each skill inside `plugins/skills/` also has its own standalone repo under `responsum-team/skill-<name>` — these are kept in sync automatically via GitHub Actions whenever a skill is updated.

### `responsum-team/dbtproject` — clone this for dbt work

```
dbtproject/
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

See the [dbtproject README](https://github.com/responsum-team/dbtproject) for dbt Cloud auth (OAuth vs PAT) and MCP feature flags.

---

## 8. Your first tasks

Follow this sequence to verify your setup and get context quickly:

1. **Verify source access** — ask Cursor: *"Run the source system analyser against the source database"*. It will use the source DB connection secret and produce a `schema.json`. If it succeeds, your Key Vault and database access are working.

2. **Check dbt health** — open `responsum-team/dbtproject` in Cursor and ask: *"Extract the last dbt Cloud run logs"*. The `dbt-cloud-log-extractor` skill will report model pass/fail status.

3. **Browse the catalog** — ask Cursor: *"List all tables in OpenMetadata"*. This confirms OpenMetadata MCP access.

4. **Explore the STMs** — browse `stm/output/` in the dbt repo for the target tables. Each STM shows source → target column mapping plus governance metadata.

---

## 9. Key links and references

Ask a team admin to fill in the specific URLs for your environment:

| Resource | Location |
|----------|----------|
| dbt Cloud | Your team's dbt Cloud account |
| Azure Key Vault | Your team's Key Vault (Azure portal) |
| Plugins repo (skills, rules, MCPs) | [responsum-team/plugins](https://github.com/responsum-team/plugins) |
| dbt project repo | [responsum-team/dbtproject](https://github.com/responsum-team/dbtproject) |
| End-to-end flow | [end-to-end-flow.md](end-to-end-flow.md) |
| Key Vault setup | [KEYVAULT_SETUP.md](KEYVAULT_SETUP.md) |
| Fivetran runbooks | [FIVETRAN_TROUBLESHOOTING.md](FIVETRAN_TROUBLESHOOTING.md), [FIVETRAN_ADVANCED_TUNING.md](FIVETRAN_ADVANCED_TUNING.md) |
| OpenMetadata + glossary | [README.md](README.md) |
