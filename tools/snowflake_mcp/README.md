# Snowflake MCP Server

Wraps [`snowflake-labs-mcp`](https://github.com/Snowflake-Labs/mcp) (installed on-demand via `uvx`) as a stdio MCP server for Cursor.

## Files

| File | Purpose |
|---|---|
| `run.sh` | Launcher — loads `.env`, validates required vars, starts the MCP process |
| `tools_config.yaml` | Enables object manager + query manager; controls which SQL statement types are allowed |
| `.env.example` | Template for the five required environment variables |

## Prerequisites

- [`uv`](https://docs.astral.sh/uv/getting-started/installation/) installed and on `PATH` (`uvx` ships with it)
  ```bash
  curl -LsSf https://astral.sh/uv/install.sh | sh
  ```
- A Snowflake account with a user + password + warehouse you can connect with

## Setup

1. **Copy the example env file** to the repo root and fill in your values:
   ```bash
   cp tools/snowflake_mcp/.env.example .env
   # then edit .env and set the five SNOWFLAKE_* variables
   ```

   | Variable | Description |
   |---|---|
   | `SNOWFLAKE_ACCOUNT` | Account identifier — `<orgname>-<accountname>` or legacy locator |
   | `SNOWFLAKE_USER` | Login username |
   | `SNOWFLAKE_FIVETRAN_PASSWORD` | Password (re-exported as `SNOWFLAKE_PASSWORD` for the binary) |
   | `SNOWFLAKE_DATABASE` | Default database context for the session |
   | `SNOWFLAKE_WAREHOUSE` | Compute warehouse to run queries on |

2. **Register the server in Cursor** — add the following block to `~/.cursor/mcp.json`:
   ```json
   {
     "mcpServers": {
       "snowflake": {
         "command": "/absolute/path/to/tools/snowflake_mcp/run.sh"
       }
     }
   }
   ```
   Replace the path with the actual absolute path to `run.sh` in your clone.

3. **Reload Cursor** — open the MCP panel (`Cursor Settings → MCP`) and confirm the `snowflake` server shows a green status.

## Manual test (without Cursor)

Run the launcher directly to confirm credentials and connectivity before registering it in Cursor:

```bash
bash tools/snowflake_mcp/run.sh
```

A healthy start prints nothing and waits for JSON-RPC input on stdin. `Ctrl-C` to exit.

You can also pipe a quick ping:

```bash
echo '{"jsonrpc":"2.0","id":1,"method":"tools/list","params":{}}' \
  | bash tools/snowflake_mcp/run.sh
```

## Available tools

| Tool | Description |
|---|---|
| `run_snowflake_query` | Execute any SQL statement (DML/DDL) |
| `list_objects` | List databases, schemas, tables, views, warehouses, roles, and more |
| `describe_object` | Describe columns and properties of any object |
| `create_object` / `create_or_alter_object` | Create or replace Snowflake objects |
| `drop_object` | Drop a Snowflake object |

## SQL permissions

Configured in `tools_config.yaml`. All statement types are enabled by default (`Unknown: False` blocks unrecognised patterns). Edit the file to restrict permissions for read-only setups:

```yaml
sql_statement_permissions:
  - Select: True
  - Describe: True
  - Show: True
  - Create: False   # disable DDL in read-only mode
  - Drop: False
  # ...
```

## Tip — binary columns

Snowflake `BINARY` columns (e.g. `Hashbytes`) cannot be serialised to JSON by the MCP transport. Either exclude them or wrap with `HEX_ENCODE()`:

```sql
SELECT HEX_ENCODE("Hashbytes") AS HashbytesHex, ... FROM my_table
```
