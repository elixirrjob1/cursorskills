#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'EOF'
Usage:
  deploy_keyvault.sh --vault NAME --resource-group RG --location LOC [options]

Options:
  --subscription SUB   Set Azure subscription before deployment
  --upload-env         Upload secrets from local .env using scripts/populate_keyvault.py
  --user USER          Per-user suffix for secrets when used with --upload-env
  -h, --help           Show this help
EOF
}

VAULT=""
RG=""
LOC=""
SUB=""
UPLOAD_ENV=0
USER_SUFFIX=""

while [[ $# -gt 0 ]]; do
  case "$1" in
    --vault) VAULT="${2:-}"; shift 2 ;;
    --resource-group) RG="${2:-}"; shift 2 ;;
    --location) LOC="${2:-}"; shift 2 ;;
    --subscription) SUB="${2:-}"; shift 2 ;;
    --upload-env) UPLOAD_ENV=1; shift ;;
    --user) USER_SUFFIX="${2:-}"; shift 2 ;;
    -h|--help) usage; exit 0 ;;
    *) echo "Unknown argument: $1" >&2; usage; exit 2 ;;
  esac
done

if [[ -z "$VAULT" || -z "$RG" || -z "$LOC" ]]; then
  usage
  exit 2
fi

if [[ -n "$SUB" ]]; then
  az account set --subscription "$SUB"
fi

echo "Ensuring resource group exists: $RG ($LOC)"
az group create --name "$RG" --location "$LOC" --output none

echo "Creating Key Vault: $VAULT"
az keyvault create \
  --name "$VAULT" \
  --resource-group "$RG" \
  --location "$LOC" \
  --enable-rbac-authorization true \
  --output none

USER_ID="$(az ad signed-in-user show --query id -o tsv)"
SUB_ID="$(az account show --query id -o tsv)"
KV_SCOPE="/subscriptions/${SUB_ID}/resourceGroups/${RG}/providers/Microsoft.KeyVault/vaults/${VAULT}"

echo "Assigning Key Vault Secrets Officer to current user: $USER_ID"
az role assignment create \
  --role "Key Vault Secrets Officer" \
  --assignee "$USER_ID" \
  --scope "$KV_SCOPE" \
  --output none || true

echo "Key Vault ready: $VAULT"

if [[ "$UPLOAD_ENV" -eq 1 ]]; then
  if [[ ! -f "scripts/populate_keyvault.py" ]]; then
    echo "scripts/populate_keyvault.py not found. Skipping upload." >&2
    exit 1
  fi
  if [[ -n "$USER_SUFFIX" ]]; then
    echo "Uploading .env secrets with user suffix: $USER_SUFFIX"
    python3 scripts/populate_keyvault.py --vault "$VAULT" --user "$USER_SUFFIX"
  else
    echo "Uploading .env secrets as shared secrets"
    python3 scripts/populate_keyvault.py --vault "$VAULT"
  fi
fi

echo "Done."
