#!/usr/bin/env bash
# Start the dbt MCP server for this project.
# Run automatically by Cursor via .cursor/mcp.json — no manual invocation needed.
#
# Auth (pick one):
#   A) OAuth  — run `uvx dbt-mcp auth` once to populate mcp.yml (gitignored)
#   B) PAT    — set DBT_PAT in .env; the script exports it as DBT_TOKEN
#
# Full feature matrix:
#   Admin API  (jobs, runs, envs)  — works with auth only
#   Discovery / SQL / Semantic     — also requires DBT_PROD_ENV_ID in .env

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ENV_FILE="${SCRIPT_DIR}/.env"

if [[ -f "$ENV_FILE" ]]; then
  set -a
  # shellcheck disable=SC1090
  source <(grep -E '^[A-Z_][A-Z0-9_]*=' "$ENV_FILE")
  set +a
fi

export DBT_PROJECT_DIR="$SCRIPT_DIR"
export DBT_PROFILES_DIR="$SCRIPT_DIR"
export DBT_PATH="${DBT_PATH:-dbt}"

# dbt Cloud host — override in .env if your tenant is different
export DBT_HOST="${DBT_HOST:-rm291.us1.dbt.com}"

# PAT → token fallback (skips the OAuth mcp.yml flow)
if [[ -n "${DBT_PAT:-}" ]]; then
  export DBT_TOKEN="$DBT_PAT"
fi

# User ID — required by dbt MCP; set DBT_USER_ID in .env
if [[ -n "${DBT_USER_ID:-}" ]]; then
  export DBT_USER_ID
fi

# Advanced features — disabled until env IDs are configured
export DISABLE_ADMIN_API="${DISABLE_ADMIN_API:-false}"
export DISABLE_DISCOVERY="${DISABLE_DISCOVERY:-${DBT_PROD_ENV_ID:+false}}"
export DISABLE_DISCOVERY="${DISABLE_DISCOVERY:-true}"
export DISABLE_SEMANTIC_LAYER="${DISABLE_SEMANTIC_LAYER:-true}"
export DISABLE_SQL="${DISABLE_SQL:-true}"

exec uvx dbt-mcp
