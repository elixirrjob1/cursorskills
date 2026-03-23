# Fivetran MCP Server

Local copy of Fivetran's `examples/mcp/quickstart` server from `fivetran/api_framework`, adapted for this repo.

## Files

- `server.py`: stdio MCP server for common Fivetran operations
- `requirements.txt`: minimal Python dependencies

## Required environment variables

```bash
export FIVETRAN_API_KEY="your_key"
export FIVETRAN_API_SECRET="your_secret"
```

## Local run

```bash
./scripts/install_fivetran_mcp_deps.sh
./tools/fivetran_mcp/run.sh
```

## Available tools

- `test_connection`
- `list_groups`
- `create_group`
- `get_group_details`
- `list_destinations`
- `get_destination_details`
- `create_destination`
- `update_destination`
- `run_destination_setup_tests`
- `list_connectors`
- `get_connector_status`
- `pause_connector`
- `resume_connector`
- `trigger_sync`
- `run_connection_setup_tests`
- `resync_connector`
- `get_connection_details`
- `list_webhooks`
- `create_group_webhook`
- `update_webhook`
- `delete_webhook`
- `list_users`
- `get_user_details`
- `get_connector_metadata`
- `get_connection_schema_config`
- `get_table_columns_config`
- `reload_connection_schema_config`
- `update_schema_config`
- `update_table_config`
- `update_column_config`
- `update_connector`
- `create_connector`
- `delete_connection`
- `delete_destination`

## Typical refinement flow

After a connector exists, you can refine sync scope from Cursor with prompts like:

```text
Show me the schema config for connector CONNECTOR_ID.
```

```text
Disable table `audit_log` in schema `dbo` for connector CONNECTOR_ID.
```

```text
Show me the columns for `dbo.customers` in connector CONNECTOR_ID, then disable `password_hash` and hash `email`.
```

## Destination setup flow

You can now create the destination path from Cursor too:

```text
List my groups and destinations.
```

```text
Create a new Fivetran group named `Azure MSSQL Landing`.
```

```text
Create a destination in group GROUP_ID for service DESTINATION_TYPE using this config ..., then run setup tests and show me the result.
```

## Cursor registration

Use:

```bash
./scripts/install_fivetran_mcp_deps.sh
./scripts/setup_fivetran_mcp.sh
```

This adds a `fivetran-example` entry to `~/.cursor/mcp.json` without replacing other MCP servers.
