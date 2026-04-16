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

# Cloud platform — disabled until a production environment is created in dbt Cloud
unset DBT_HOST DBT_TOKEN DBT_PROD_ENV_ID
export DISABLE_SEMANTIC_LAYER=true
export DISABLE_DISCOVERY=true
export DISABLE_SQL=true
export DISABLE_ADMIN_API=true

exec uvx dbt-mcp
