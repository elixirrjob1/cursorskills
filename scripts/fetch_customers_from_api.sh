#!/bin/bash
# Fetch /api/tables and /api/customers using the api-reader skill.
# Requires: API running (e.g. python scripts/run_api.py), API_AUTH_TOKEN in .env or Key Vault.
set -e
cd "$(dirname "$0")/.."
[ -f .env ] && export $(grep -E '^API_AUTH_TOKEN=' .env | xargs) 2>/dev/null || true
TOKEN="${API_AUTH_TOKEN:?Set API_AUTH_TOKEN in .env or export it}"
python3 .cursor/skills/api-reader/scripts/api_reader.py \
  http://localhost:8000 \
  --bearer "$TOKEN" \
  --path /api/tables \
  --path /api/customers \
  -o api_customers_info.json
echo "Written to api_customers_info.json"
cat api_customers_info.json | head -80
