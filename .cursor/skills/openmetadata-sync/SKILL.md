---
name: openmetadata-sync
description: Configure any supported database service in OpenMetadata, run metadata sync, inspect imported databases/schemas/tables/columns, and assign glossary terms or classification tags through the OpenMetadata MCP server. Use when the user wants Cursor to orchestrate catalog sync and metadata tagging in OpenMetadata instead of doing those steps manually.
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
- ensure `OPENMETADATA_BASE_URL`, `OPENMETADATA_EMAIL`, and `OPENMETADATA_PASSWORD` are available
- ensure the database connection details are available for the target service type

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

For known service types, use these minimum fields:
- `Snowflake`: `username`, `password`, `account`, `warehouse`
- `Postgres`: `username`, `password`, `hostPort`, `database`
- `Mysql`: `username`, `password`, `hostPort`, `database`
- `Mssql`: `username`, `password`, `hostPort`, `database`
- `Oracle`: `username`, `password`, `hostPort`, `serviceName`

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

- Never print secrets in the final response.
- Do not create duplicate services or pipelines when an existing one can be reused.
- Do not treat glossary and classification tags as the same thing.
- If ingestion fails, report the failing service, pipeline, and API step clearly.
