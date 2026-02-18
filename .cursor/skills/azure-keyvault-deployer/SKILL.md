---
name: azure-keyvault-deployer
description: Deploys a new Azure Key Vault quickly via Azure CLI (create resource group if needed, create vault with RBAC, grant current user secrets access, and optionally upload .env secrets). Use when user asks to create/deploy/setup a new Azure Key Vault or bootstrap one for local scripts.
---

# Azure Key Vault Deployer

Use this skill when the user wants a fast, repeatable Azure Key Vault setup.

## Required User Inputs

Before running anything, collect these from the user:
- `vault name` (globally unique)
- `resource group`
- `location` (Azure region, e.g. `eastus`, `westeurope`)
- Optional: `subscription`
- Optional: whether to upload `.env` secrets
- Optional: per-user suffix for secrets

## What This Skill Does

1. Ensures target resource group exists.
2. Creates a new Key Vault with RBAC enabled.
3. Grants the signed-in user `Key Vault Secrets Officer`.
4. Optionally uploads secrets from `.env` (shared or per-user).

## Script

Run:

```bash
bash .cursor/skills/azure-keyvault-deployer/scripts/deploy_keyvault.sh \
  --vault <vault-name> \
  --resource-group <resource-group> \
  --location <azure-region> \
  [--subscription <sub-id-or-name>] \
  [--upload-env] \
  [--user <user-suffix>]
```

Examples:

```bash
# Create vault and access only
bash .cursor/skills/azure-keyvault-deployer/scripts/deploy_keyvault.sh \
  --vault <vault-name> \
  --resource-group <resource-group> \
  --location <location>

# Create vault and upload .env as shared secrets
bash .cursor/skills/azure-keyvault-deployer/scripts/deploy_keyvault.sh \
  --vault <vault-name> \
  --resource-group <resource-group> \
  --location <location> \
  --upload-env

# Create vault and upload .env as per-user secrets (e.g., DATABASE-URL-FILIP)
bash .cursor/skills/azure-keyvault-deployer/scripts/deploy_keyvault.sh \
  --vault <vault-name> \
  --resource-group <resource-group> \
  --location <location> \
  --upload-env \
  --user <user-suffix>
```

## Notes

- Requires `az login` first.
- Vault names are globally unique.
- If `--upload-env` is used, this script calls `scripts/populate_keyvault.py`.
