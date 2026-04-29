#!/usr/bin/env bash
# Start the Snowflake MCP server (snowflake-labs-mcp via uvx).
# Credentials are read from .env — never hardcoded here.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ENV_FILE="${SCRIPT_DIR}/../../.env"
CONFIG_FILE="${SCRIPT_DIR}/tools_config.yaml"
CONN_FILE="${HOME}/.snowflake/connections.toml"

# Load .env
if [[ -f "$ENV_FILE" ]]; then
  set -a; source "$ENV_FILE"; set +a
fi

: "${SNOWFLAKE_ACCOUNT:?SNOWFLAKE_ACCOUNT not set in .env}"
: "${SNOWFLAKE_USER:?SNOWFLAKE_USER not set in .env}"
: "${SNOWFLAKE_FIVETRAN_PASSWORD:?SNOWFLAKE_FIVETRAN_PASSWORD not set in .env}"
: "${SNOWFLAKE_DATABASE:?SNOWFLAKE_DATABASE not set in .env}"
: "${SNOWFLAKE_WAREHOUSE:?SNOWFLAKE_WAREHOUSE not set in .env}"

# Pass credentials via env vars (snowflake-labs-mcp picks these up directly).
export SNOWFLAKE_ACCOUNT
export SNOWFLAKE_USER
export SNOWFLAKE_PASSWORD="${SNOWFLAKE_FIVETRAN_PASSWORD}"
export SNOWFLAKE_DATABASE
export SNOWFLAKE_WAREHOUSE

exec uvx snowflake-labs-mcp \
  --service-config-file "$CONFIG_FILE"
