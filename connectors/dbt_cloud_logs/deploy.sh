#!/usr/bin/env bash
# deploy.sh — deploy the dbt_cloud_logs connector to Fivetran
#
# Resolves env vars from .env into a temp configuration file, deploys,
# then immediately removes the temp file so secrets never persist on disk.
#
# Usage:
#   ./deploy.sh [--force]     # --force updates an existing connection

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"
ENV_FILE="${REPO_ROOT}/.env"
VENV_PYTHON="${REPO_ROOT}/.venv/bin/python3"
VENV_FIVETRAN="${REPO_ROOT}/.venv/bin/fivetran"
TEMP_CONFIG="${SCRIPT_DIR}/configuration.resolved.json"

# ---- Load .env ----
if [[ -f "$ENV_FILE" ]]; then
  set -a
  # shellcheck disable=SC1090
  source "$ENV_FILE"
  set +a
else
  echo "ERROR: .env not found at $ENV_FILE" >&2
  exit 1
fi

# ---- Validate required env vars ----
REQUIRED_VARS=(FIVETRAN_API_KEY FIVETRAN_API_SECRET DBT_ACCOUNT_ID DBT_SERVICE_TOKEN)
for var in "${REQUIRED_VARS[@]}"; do
  if [[ -z "${!var:-}" ]]; then
    echo "ERROR: $var is not set in .env" >&2
    exit 1
  fi
done

# ---- Build base64 API key ----
API_B64=$(echo -n "${FIVETRAN_API_KEY}:${FIVETRAN_API_SECRET}" | base64 -w 0)

# ---- Write resolved config to temp file ----
"$VENV_PYTHON" - <<PYEOF
import json, os
cfg = {
    "base_url": "https://kd329.us1.dbt.com",
    "account_id": os.environ["DBT_ACCOUNT_ID"],
    "dbt_service_token": os.environ["DBT_SERVICE_TOKEN"],
}
json.dump(cfg, open("${TEMP_CONFIG}", "w"), indent=2)
PYEOF

# ---- Ensure temp file is removed even on error ----
trap 'rm -f "${TEMP_CONFIG}"' EXIT

# ---- Deploy ----
FORCE_FLAG=""
if [[ "${1:-}" == "--force" ]]; then
  FORCE_FLAG="--force"
fi

cd "$SCRIPT_DIR"
"$VENV_FIVETRAN" deploy \
  --api-key "$API_B64" \
  --destination "DRIP_DATA_INTELLIGENCE_Snowflake" \
  --connection "bronze_dbt_logs" \
  --configuration "${TEMP_CONFIG}" \
  --python-version 3.12 \
  $FORCE_FLAG
