---
name: catalog-sync
description: Configure any supported database service in a data catalog, run metadata sync, inspect imported databases/schemas/tables/columns, and assign glossary terms or classification tags. Currently supports OpenMetadata; designed to extend to Azure Purview, Databricks Unity Catalog, and others. Use when the user wants catalog sync, metadata import, data-catalog setup, OpenMetadata ingestion, or metadata tagging instead of doing those steps manually.
---

# OpenMetadata Sync

Use this skill when the task is to:
- configure or update a database service in OpenMetadata
- create or run OpenMetadata metadata ingestion
- inspect imported catalog entities
- assign glossary terms or classification tags to tables and columns

This skill assumes the repo-local OpenMetadata MCP server is the execution surface. It should orchestrate OpenMetadata through MCP tools, not by inventing ad hoc API calls in the chat response.

**MCP server:** `user-openmetadata` (Cursor may also show `openmetadata` in `mcp.json`). Always invoke tools as `user-openmetadata:<tool_name>` — for example `user-openmetadata:test_connection`, not bare `test_connection`.

## Prerequisites

Before using the workflow:
- ensure the OpenMetadata MCP server is registered in Cursor
- ensure `OM_BASE_URL` and `OM_TOKEN` are available in `.env` (legacy aliases: `OPENMETADATA_BASE_URL`, `OPENMETADATA_JWT_TOKEN`)
- ensure database connection secrets are stored in the repo `.env` under names the user provides (see Credentials below)

If the MCP server is not set up yet, use the repo scripts:

```bash
./scripts/install_openmetadata_mcp_deps.sh
./scripts/setup_openmetadata_mcp.sh
```

## Workflow

1. Validate connectivity with `user-openmetadata:test_connection`.
2. Discover whether the database service already exists with `user-openmetadata:list_database_services` or `user-openmetadata:get_database_service`.
3. Create or update the service with:
   - `user-openmetadata:create_database_service`
   - `user-openmetadata:update_database_service`
4. Create or update the metadata ingestion pipeline with:
   - `user-openmetadata:create_metadata_ingestion_pipeline`
   - `user-openmetadata:update_metadata_ingestion_pipeline`
5. Run sync with `user-openmetadata:run_ingestion_pipeline`, then inspect status with `user-openmetadata:get_ingestion_status`.
6. Inspect imported assets with:
   - `user-openmetadata:list_databases`
   - `user-openmetadata:list_schemas`
   - `user-openmetadata:list_tables`
   - `user-openmetadata:get_table`
   - `user-openmetadata:get_column`
7. Inspect approved governance metadata with:
   - `user-openmetadata:list_glossaries`
   - `user-openmetadata:list_glossary_terms`
   - `user-openmetadata:list_classifications`
   - `user-openmetadata:list_tags`
8. Apply metadata directly in OpenMetadata with:
   - `user-openmetadata:assign_glossary_term_to_table`
   - `user-openmetadata:assign_glossary_term_to_column`
   - `user-openmetadata:assign_tags_to_table`
   - `user-openmetadata:assign_tags_to_column`
9. Re-read the updated table or column with `user-openmetadata:get_table` or `user-openmetadata:get_column` to confirm the assignment landed.

## Service Configuration Rules

- The skill is generic across supported database services. Do not hardcode Snowflake-only logic.
- Pass `service_type` explicitly, for example `Snowflake`, `Postgres`, `Mysql`, `Mssql`, or `Oracle`.
- Pass the service-specific connection settings through `connection_config`.
- Do not invent connection fields or credentials. If required values are missing, stop and ask for them.

### Credentials (never ask for secret values in chat)

For any sensitive or connection field (`password`, `username`, etc.):

1. Ask the user for the **`.env` variable name** only (e.g. `MYSQL_RETAIL_PROD_PASSWORD`), not the value.
2. Confirm the variable exists in the repo `.env` (you may run `grep '^MYSQL_RETAIL' .env` to check the name exists — do not print values).
3. Pass references in `connection_config` using either form:
   - **`{field}_env`**: e.g. `"password_env": "MYSQL_RETAIL_PROD_PASSWORD"`
   - **`env:VAR`**: e.g. `"password": "env:MYSQL_RETAIL_PROD_PASSWORD"`

The OpenMetadata MCP server resolves these from `.env` at call time. The agent never sees or repeats secret values.

Non-secret fields (`hostPort`, `account`, `warehouse`, `database`) may be literals in `connection_config` or use the same `*_env` / `env:` pattern if the user prefers.

For known service types, use these minimum fields (literal or `*_env` / `env:` for each):

- `Snowflake`: `username`, `password`, `account`, `warehouse`
- `Postgres`: `username`, `password`, `hostPort`, `database`
- `Mysql`: `username`, `password`, `hostPort`, `database`
- `Mssql`: `username`, `password`, `hostPort`, `database`
- `Oracle`: `username`, `password`, `hostPort`, `serviceName`

Example `connection_config` for MySQL (password only via `.env`):

```json
{
  "username": "om_reader",
  "password_env": "MYSQL_RETAIL_PROD_PASSWORD",
  "hostPort": "mysql.example.com:3306",
  "database": "retail_erp"
}
```

## Onboard Source System (plan / apply / rollback)

Use this route when registering a **new** database source in OpenMetadata for the first time.
It is the only route in this skill that writes to OM in a reviewed, reversible sequence.

For re-running ingestion on an existing service, updating service config, or assigning tags,
use the standard Workflow section above — not this route.

### Trigger phrases

"Onboard a new source", "register a new database service", "add a new source to the catalog",
"set up catalog ingestion for a new source".

### Interview (plan step)

Ask the following questions in order. Stop if any required answer is missing or ambiguous.

1. **Service name** — lowercase, underscores, no spaces (e.g. `mysql_retail_erp`)
2. **Database type** — `Mysql` / `Postgres` / `Mssql` / `Oracle` / `Snowflake`
3. **Host and port** — e.g. `mysql.example.com:3306`
4. **Database name**
5. **Username** — literal value, not a secret
6. **Password** — ask for the `.env` variable **name** only (e.g. `MYSQL_RETAIL_PROD_PASSWORD`).
   Confirm the name exists with `grep '^MYSQL_RETAIL' .env` — do not print the value.
7. **Analyser JSON** — does a `schema_*.json` from source-system-analyser exist for this source?
   - Yes → path to the file; tables will be verified by name after ingestion.
   - No → count-only verification will be used.
8. **Schema filter** — schemas to include (leave blank for all)
9. **Confirm** — present a full summary (see plan file schema below) and ask the user to approve
   before writing anything.

### Plan file

Write to `.cursor/flat/onboard_plan_<service_name>.json`. Never commit this file (covered by `.gitignore`).

```json
{
  "version": 1,
  "created_at": "<ISO timestamp>",
  "service_name": "mysql_retail_erp",
  "service_type": "Mysql",
  "connection_config": {
    "username": "om_reader",
    "password_env": "MYSQL_RETAIL_PROD_PASSWORD",
    "hostPort": "mysql.example.com:3306",
    "database": "retail_erp"
  },
  "include_schemas": ["retail_erp"],
  "analyzer_json": ".cursor/flat/schema_mysql_retail_erp.json",
  "expected_tables": ["orders", "customers", "products"],
  "existing_service_fqns_snapshot": ["snowflake_fivetran", "mssql_bronze"],
  "created": {
    "service_fqn": null,
    "pipeline_fqn": null
  }
}
```

Rules for the plan file:
- `connection_config` must contain `password_env` (variable name). Never a literal `password` value.
- `existing_service_fqns_snapshot` must list every service FQN currently in OM at plan time
  (call `user-openmetadata:list_database_services` to get these).
- `expected_tables` is populated from `analyzer_json` if provided; otherwise left empty.
- `created` starts with both fields null. Apply fills them in as each entity is created.

### Apply step

Run: `python .cursor/skills/catalog-sync/scripts/catalog_onboard_apply.py .cursor/flat/onboard_plan_<service_name>.json`

The script enforces these guardrails — if any fails, it exits before creating anything:
- Password env var resolves to a non-empty value after loading `.env` / Key Vault.
- No literal `password` field in `connection_config`.
- Service with that name does not already exist in OM.
- No ingestion pipeline already exists for the service.

On success: `created.service_fqn` and `created.pipeline_fqn` are written back to the plan file.
Ingestion polls for up to 10 minutes. If it times out, apply exits with a manual-check message
— this is not a failure, ingestion may still be running.

#### Key Vault password resolution

If the password env var is not in `.env` directly but is stored in Azure Key Vault:
- `KEYVAULT_NAME` must be set in `.env`.
- The secret name in Key Vault uses hyphens: `MYSQL-RETAIL-PROD-PASSWORD`.
- The variable name must appear in the `ENV_VARS` list in the shared `scripts/keyvault_loader.py`
  (repo-root scripts — this allowlist is centrally maintained, not bundled per skill).
  If it does not, the apply script will exit with a clear diagnostic and instructions to add it.

### Rollback step

Run: `python .cursor/skills/catalog-sync/scripts/catalog_onboard_rollback.py .cursor/flat/onboard_plan_<service_name>.json`

Add `--dry-run` to preview what would be deleted without making any changes:
`python .cursor/skills/catalog-sync/scripts/catalog_onboard_rollback.py .cursor/flat/onboard_plan_<service_name>.json --dry-run`

The script enforces these guardrails — if any fails, it exits and deletes nothing:
- The service FQN to be deleted is not in `existing_service_fqns_snapshot`.
- No table under the service has tags, glossary terms, or non-empty descriptions.
  If annotations are found, the script lists them and stops. Remove them manually or accept the service.

Handles partial apply: if `pipeline_fqn` is null (pipeline creation failed), rollback still
deletes the service cleanly.

### Extend existing pipeline (dbt logs / add schema to Snowflake-Fivetran)

When dbt logs land as Snowflake tables via Fivetran, do not register a new OM connector.
Widen the existing Snowflake-Fivetran pipeline filter instead:

1. Read the current pipeline to get the existing `schemaFilterPattern.includes` list.
2. Append the new schema — do not replace the existing list.
3. Run: `python .cursor/skills/stm-to-catalog-enricher/scripts/patch_pipeline_filter.py \`
   `--pipeline-id <uuid> --include-schemas <existing_schemas>,<new_schema>`
   The script patches the filter, then automatically calls `/deploy` (pushes updated DAG
   to Airflow) and `/trigger` (fires an immediate run). Both steps are required:
   without `/deploy` Airflow runs the stale cached DAG and silently ignores the new schema.
4. Verify the new schema's tables appeared in OM.

Always read the current filter before patching. The patch replaces the list — merging must
happen in step 2, not in the script.

### Existing service on onboard route (hard stop)

When the user asks to **onboard** a service name that **already exists** in OpenMetadata:

1. Call `user-openmetadata:get_database_service` for that name **before** any plan or apply step.
2. If the service exists: **stop immediately**. State clearly that the service already exists and the onboard create path is blocked. Do **not** call `user-openmetadata:create_database_service`, write a plan file for create, or run `catalog_onboard_apply.py`.
3. Offer the **standard Workflow** instead: `user-openmetadata:update_database_service` / `user-openmetadata:update_metadata_ingestion_pipeline` (if config changed), then `user-openmetadata:run_ingestion_pipeline` → `user-openmetadata:get_ingestion_status`.
4. If the user insists the name exists but `get_database_service` returns 404: report the mismatch (name not found in OM) and ask whether they meant a different service name or an existing FQN from `user-openmetadata:list_database_services`.

### Guardrails (hard stops — no bypass)

- NEVER call `user-openmetadata:create_database_service` if a service with that name already exists.
- NEVER call `user-openmetadata:create_metadata_ingestion_pipeline` if a pipeline already exists for the service.
- NEVER call `user-openmetadata:update_database_service` or `user-openmetadata:update_metadata_ingestion_pipeline` from this route.
- NEVER call any `user-openmetadata:assign_*` tool from this route.
- NEVER delete a service or pipeline whose FQN is in `existing_service_fqns_snapshot`.
- NEVER rollback if any table under the service has a tag, glossary term, or non-empty description.
- NEVER patch a pipeline filter without first reading the current filter and merging, not replacing.

## Tagging Rules

- OpenMetadata is the source of truth once glossary terms and tags are assigned there.
- Use glossary assignment tools for business-term mapping.
- Use tag assignment tools for classifications and policy labels.
- Preserve existing unrelated tags on the entity.
- Prefer rerunnable operations. The MCP tools are intended to be idempotent.

## Analyzer Handoff

When the analyzer runs after this workflow:
- it should read existing OpenMetadata glossary and classification assignments
- it should not invent replacement business labels when authoritative catalog metadata already exists

The analyzer output can keep technical metadata such as column type and generated technical description, but business metadata should come from OpenMetadata.

## Vague or broad requests ("fix my data catalog")

Do **not** run ingestion, tagging, or onboard apply until intent is clear.

1. **Ask first** (one short message): what outcome do they need — refresh metadata, register a new source, fix connection/config, add descriptions/tags, or widen a pipeline schema filter?
2. **Offer scoped options** tied to this skill:
   - **Re-sync** existing service → standard Workflow (`run_ingestion_pipeline`, then inspect)
   - **New source** → Onboard route (interview → plan → approved apply)
   - **Governance on assets** → tagging steps in Workflow, or `catalog-glossary-tagger` for AI matching at scale
   - **Vocabulary definitions** → `catalog-vocab-publisher` (not this skill)
3. Only after the user picks (or their reply implies one path), call MCP tools or scripts.

## Guardrails

- Never ask for or print secret **values** in chat (only `.env` variable **names**).
- Never print secrets in the final response.
- Do not create duplicate services or pipelines when an existing one can be reused.
- Do not treat glossary and classification tags as the same thing.
- If ingestion fails, report the failing service, pipeline, and MCP tool step clearly (use `user-openmetadata:<tool>` names).

## Gotchas / Common Mistakes

- **`run_ingestion_pipeline` needs the pipeline FQN**, not the short pipeline name alone (e.g. `service_name.service_name_metadata`, not just `metadata`).
- **Onboard vs standard Workflow:** first-time registration uses plan + `catalog_onboard_apply.py`; re-sync on an existing connector uses Workflow only — never `create_database_service` for a name that already exists.
- **Extend pipeline filter:** `patch_pipeline_filter.py` replaces the includes list — merge current schemas in step 2 before calling the script; then `/deploy` and `/trigger` are both required or Airflow keeps a stale DAG.
- **Glossary vs tags:** business terms use `assign_glossary_term_*`; policy/classifications use `assign_tags_*` — do not swap them.
- **User says service exists but OM returns 404:** list services with `user-openmetadata:list_database_services` and reconcile the name before attempting create.
- **Ingestion timeout (600 s):** apply script may exit with a manual-check message while ingestion still runs — use `user-openmetadata:get_ingestion_status` before re-triggering.

## Coexistence & Routing

| Skill | Purpose |
|---|---|
| `catalog-sync` | Database service setup, metadata ingestion, inspect assets, assign tags/glossary via MCP. |
| `catalog-vocab-publisher` | Publish a governance vocabulary `.md` to OpenMetadata Classifications/Tags. Not database ingestion. |
| `catalog-glossary-tagger` | AI-driven glossary matching on **already-catalogued** tables/columns. Not full connector setup. |
| `governance-import-dbt-lineage` | Import dbt `manifest.json` lineage edges. Not metadata ingestion pipelines. |
| `stm-to-catalog-enricher` | Enrich Snowflake tables from STM markdown after dbt lands — uses catalog-sync primitives plus STM loop. |
| `source-system-analyser` | Produces `schema_*.json` used for post-onboard table verification — not OpenMetadata writes. |

**Trigger precision:** Fires on catalog sync, metadata import, OpenMetadata ingestion, onboard/register new database source, assign classification/glossary to catalog assets. Does **not** fire on publish vocabulary `.md`, AI glossary mapping at scale, or dbt lineage import.

## Model Compatibility

Validated on **Claude Sonnet** (default Cursor agent tier) for MCP-heavy runs: multi-step ingestion, tagging, and onboard apply. Routing-only cases (should-not-trigger adjacent skills, vague-request clarification) spot-checked on **Claude Haiku**.

**Note:** Haiku may skip `user-openmetadata:` prefixes or clarification-first steps — prefer Sonnet when the task chains more than three MCP calls or uses the onboard plan/apply path.

## Registry

| Field | Value |
|-------|-------|
| Owner | Platform / Data Engineering |
| Reviewer | Peer review required before merging `SKILL.md` changes to `main` |
| Version | Tracks repo `main`; onboard scripts at `scripts/catalog_onboard_*.py` |
| Lifecycle stage | **Test / Deploy** — active; iterate via skill-reviewer after changes |
| Last evaluated | `2026-05-29T073125Z` (PASS — skill-reviewer) |
| Dependencies | `user-openmetadata` MCP, `scripts/catalog_onboard_apply.py` + `scripts/catalog_onboard_rollback.py` (skill-local) + bundled `scripts/om_auth.py`, shared root `scripts/keyvault_loader.py` (ENV_VARS allowlist), `.env` / Key Vault for `OM_*` and connection secrets |
| Source | `.cursor/skills/catalog-sync/` in cursorskills repo |

**Versioning:** Consumers pin to the skill folder at a given git commit. Roll back by reverting `SKILL.md` and re-running `pytest .cursor/skills/catalog-sync/tests/`.
