---
name: catalog-sync
description: Configure any supported database service in a data catalog, run metadata sync, inspect imported databases/schemas/tables/columns, and assign glossary terms or classification tags. Currently supports OpenMetadata; designed to extend to Azure Purview, Databricks Unity Catalog, and others. Use when the user wants Cursor to orchestrate catalog sync and metadata tagging instead of doing those steps manually.
---

# OpenMetadata Sync

Use this skill when the task is to:
- configure or update a database service in OpenMetadata
- create or run OpenMetadata metadata ingestion
- inspect imported catalog entities
- assign glossary terms or classification tags to tables and columns

This skill assumes the repo-local OpenMetadata MCP server is the execution surface. It should orchestrate OpenMetadata through MCP tools, not by inventing ad hoc API calls in the chat response.

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

1. Validate connectivity with `test_connection`.
2. Discover whether the database service already exists with `list_database_services` or `get_database_service`.
3. Create or update the service with:
   - `create_database_service`
   - `update_database_service`
4. Create or update the metadata ingestion pipeline with:
   - `create_metadata_ingestion_pipeline`
   - `update_metadata_ingestion_pipeline`
5. Run sync with `run_ingestion_pipeline`, then inspect status with `get_ingestion_status`.
6. Inspect imported assets with:
   - `list_databases`
   - `list_schemas`
   - `list_tables`
   - `get_table`
   - `get_column`
7. Inspect approved governance metadata with:
   - `list_glossaries`
   - `list_glossary_terms`
   - `list_classifications`
   - `list_tags`
8. Apply metadata directly in OpenMetadata with:
   - `assign_glossary_term_to_table`
   - `assign_glossary_term_to_column`
   - `assign_tags_to_table`
   - `assign_tags_to_column`
9. Re-read the updated table or column to confirm the assignment landed.

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
  (call `list_database_services` to get these).
- `expected_tables` is populated from `analyzer_json` if provided; otherwise left empty.
- `created` starts with both fields null. Apply fills them in as each entity is created.

### Apply step

Run: `python scripts/catalog_onboard_apply.py .cursor/flat/onboard_plan_<service_name>.json`

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
- The variable name must appear in the `ENV_VARS` list in `scripts/keyvault_loader.py`.
  If it does not, the apply script will exit with a clear diagnostic and instructions to add it.

### Rollback step

Run: `python scripts/catalog_onboard_rollback.py .cursor/flat/onboard_plan_<service_name>.json`

Add `--dry-run` to preview what would be deleted without making any changes:
`python scripts/catalog_onboard_rollback.py .cursor/flat/onboard_plan_<service_name>.json --dry-run`

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

### Guardrails (hard stops — no bypass)

- NEVER call `create_database_service` if a service with that name already exists.
- NEVER call `create_metadata_ingestion_pipeline` if a pipeline already exists for the service.
- NEVER call `update_database_service` or `update_metadata_ingestion_pipeline` from this route.
- NEVER call any `assign_*` tool from this route.
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

## Guardrails

- Never ask for or print secret **values** in chat (only `.env` variable **names**).
- Never print secrets in the final response.
- Do not create duplicate services or pipelines when an existing one can be reused.
- Do not treat glossary and classification tags as the same thing.
- If ingestion fails, report the failing service, pipeline, and API step clearly.
