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
