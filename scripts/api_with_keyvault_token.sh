#!/usr/bin/env bash
# Call the API reader with bearer token from Azure Key Vault.
# Requires: KEYVAULT_NAME (e.g. from .env), optional API_AUTH_SECRET_NAME (default: API-AUTH-TOKEN).
# Usage: ./scripts/api_with_keyvault_token.sh [api_reader args...]
# Example: ./scripts/api_with_keyvault_token.sh --path /api/tables --output .cursor/flat/api_tables.json

set -e
REPO_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$REPO_ROOT"

: "${KEYVAULT_NAME:?Set KEYVAULT_NAME (e.g. in .env)}"
VAULT="$KEYVAULT_NAME"
SECRET="${API_AUTH_SECRET_NAME:-API-AUTH-TOKEN}"
TOKEN=$(az keyvault secret show --vault-name "$VAULT" --name "$SECRET" --query value -o tsv)

PYTHON="${PYTHON:-.venv/bin/python}"
SCRIPT=".cursor/skills/api-reader/scripts/api_reader.py"
BASE_URL="https://skillssimapifilip20260218.azurewebsites.net"

exec "$PYTHON" "$SCRIPT" "$BASE_URL" --bearer "$TOKEN" "$@"
