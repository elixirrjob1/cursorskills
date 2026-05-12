#!/usr/bin/env bash
# Run source system analyser against Snowflake DBT_OBSERVABILITY schema.
# Prerequisites: DATABASE_URL (snowflake://...), pip install snowflake-sqlalchemy
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
cd "$ROOT"
if [ -z "${DATABASE_URL:-}" ]; then
  echo "ERROR: Set DATABASE_URL to a snowflake:// SQLAlchemy URL." >&2
  exit 1
fi
cp -f scripts/dbt_observability/db-analysis-config.json .
SCHEMA="${SNOWFLAKE_DBT_OBSERVABILITY_SCHEMA:-DBT_OBSERVABILITY}"
SCHEMA="$(echo "$SCHEMA" | tr '[:lower:]' '[:upper:]')"
OUT="${DBT_OBS_ANALYZER_OUT:-.cursor/flat/dbt_observability_schema.json}"
python3 .cursor/skills/source-system-analyser/scripts/source_system_analyzer.py \
  "${DATABASE_URL}" \
  "$OUT" \
  "$SCHEMA" \
  --dialect snowflake
