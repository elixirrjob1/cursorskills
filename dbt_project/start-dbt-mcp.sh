#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ENV_FILE="${SCRIPT_DIR}/../.env"

if [ -f "$ENV_FILE" ]; then
  set -a
  source "$ENV_FILE"
  set +a
fi

export DBT_PROJECT_DIR="${SCRIPT_DIR}/drip_transformations"
export DBT_PATH="/home/fillip/.local/bin/dbt"
export DBT_PROFILES_DIR="${SCRIPT_DIR}/drip_transformations"

# dbt Cloud integration
# Auth: OAuth token cached in drip_transformations/mcp.yml (run `uvx dbt-mcp auth`
# to (re)authenticate). Alternatively export DBT_TOKEN in .env with a service token.
export DBT_HOST="${DBT_HOST:-rm291.us1.dbt.com}"

# Admin API: list projects, connections, environments, jobs, runs. Works with
# just auth + DBT_HOST — no environment IDs required.
export DISABLE_ADMIN_API="${DISABLE_ADMIN_API:-false}"

# Discovery / Semantic Layer / SQL tools need a configured prod (and dev, for SQL)
# environment in dbt Cloud. Set DBT_PROD_ENV_ID (and DBT_DEV_ENV_ID) in .env and
# flip these to false once the Cloud project + environments exist.
export DISABLE_DISCOVERY="${DISABLE_DISCOVERY:-true}"
export DISABLE_SEMANTIC_LAYER="${DISABLE_SEMANTIC_LAYER:-true}"
export DISABLE_SQL="${DISABLE_SQL:-true}"

exec uvx dbt-mcp
