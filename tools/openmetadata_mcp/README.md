# OpenMetadata MCP Server

Repo-local stdio MCP server for OpenMetadata glossary operations, database sync orchestration, and direct table/column tagging, designed for Cursor integration the same way as the existing Fivetran MCP server.

## Required environment variables

```bash
export OPENMETADATA_BASE_URL="http://52.255.209.74:8585"
export OPENMETADATA_EMAIL="admin@open-metadata.org"
export OPENMETADATA_PASSWORD="admin"
```

Optional:

```bash
export OPENMETADATA_JWT_TOKEN="existing-jwt-token"
```

If `OPENMETADATA_JWT_TOKEN` is present, the server uses it directly. Otherwise it logs in with email/password and caches the JWT in-memory.

## Local run

```bash
./scripts/install_openmetadata_mcp_deps.sh
./tools/openmetadata_mcp/run.sh
```

## Available tools

- `list_database_services`
- `get_database_service`
- `create_database_service`
- `update_database_service`
- `list_ingestion_pipelines`
- `get_ingestion_pipeline`
- `create_metadata_ingestion_pipeline`
- `update_metadata_ingestion_pipeline`
- `run_ingestion_pipeline`
- `get_ingestion_status`
- `list_databases`
- `list_schemas`
- `list_tables`
- `get_table`
- `get_column`
- `test_connection`
- `list_glossaries`
- `get_glossary`
- `create_glossary`
- `update_glossary`
- `list_glossary_terms`
- `get_glossary_term`
- `create_glossary_term`
- `update_glossary_term`
- `list_classifications`
- `list_tags`
- `assign_glossary_term_to_table`
- `assign_glossary_term_to_column`
- `assign_tags_to_table`
- `assign_tags_to_column`

## Notes

- Database service configuration is generic. The MCP is not Snowflake-specific and accepts service-type-specific connection payloads.
- Metadata sync uses OpenMetadata ingestion pipelines rather than a custom importer.
- Glossary terms and classification tags are applied directly to imported table and column entities.

## Cursor registration

Use:

```bash
./scripts/install_openmetadata_mcp_deps.sh
./scripts/setup_openmetadata_mcp.sh
```

This adds an `openmetadata` entry to `~/.cursor/mcp.json` without replacing other MCP servers.
