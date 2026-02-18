# Azure Key Vault Setup

This project can load environment variables from Azure Key Vault instead of a local `.env` file. Use Key Vault when you want secrets in a central location or when multiple users share the same vault with per-user overrides.

## 1. Create Key Vault in fmaric Resource Group

Ensure you have an Azure subscription and the Azure CLI installed (`az`). Log in with `az login` if needed.

**Select the correct subscription** (if you have multiple):

```bash
az account set --subscription "YOUR_SUBSCRIPTION_NAME_OR_ID"
az account show  # verify
```

The `fmaric-resource-group` resource group already exists. **Create the Key Vault** (vault name must be globally unique):

```bash
az keyvault create --name skills-fmaric-kv --resource-group fmaric-resource-group --enable-rbac-authorization true
```

If `skills-fmaric-kv` is taken, use a different name (e.g. `skills-fmaric-kv-<random>`).

**Grant yourself access** to create and read secrets:

```bash
# Get your user object ID
USER_ID=$(az ad signed-in-user show --query id -o tsv)

# Assign Key Vault Secrets Officer (create/read) for this vault
az role assignment create --role "Key Vault Secrets Officer" \
  --assignee "$USER_ID" \
  --scope "/subscriptions/$(az account show --query id -o tsv)/resourceGroups/fmaric-resource-group/providers/Microsoft.KeyVault/vaults/skills-fmaric-kv"
```

Replace `skills-fmaric-kv` with your actual vault name.

## 2. Populate Key Vault from .env

From the project root, with a valid `.env` containing your secrets:

```bash
# Upload all vars from .env to Key Vault
python scripts/populate_keyvault.py --vault skills-fmaric-kv

# Or use KEYVAULT_NAME from .env
export KEYVAULT_NAME=skills-fmaric-kv
python scripts/populate_keyvault.py
```

**Note:** Key Vault secret names use hyphens (e.g. `DATABASE-URL`) because underscores are not allowed. The loader maps these back to env var names (`DATABASE_URL`).

**Per-user secrets** (optional): Create secrets for a specific user so they get their own overrides:

```bash
python scripts/populate_keyvault.py --vault skills-fmaric-kv --user FILIP
```

This creates `DATABASE-URL-FILIP`, `MSSQL-URL-FILIP`, etc. When `AZURE_USER_NAME=filip` is set locally, the loader will prefer those over the shared `DATABASE-URL`, `MSSQL-URL`, etc.

## 3. Local Config

After migration, your `.env` only needs:

```
KEYVAULT_NAME=skills-fmaric-kv
AZURE_USER_NAME=filip   # optional; omit for shared secrets only
```

All other vars (DATABASE_URL, MSSQL_URL, etc.) come from Key Vault.

## 4. Authentication

Scripts use `DefaultAzureCredential`, which tries:

1. Environment variables (e.g. `AZURE_CLIENT_ID`, `AZURE_CLIENT_SECRET` for service principal)
2. Managed identity (when running in Azure)
3. Azure CLI (`az login`)

For local development, run `az login` before running scripts.

## 5. Running Scripts with Key Vault

Project scripts (`mssql_to_azure.py`, `schema_json_to_mssql.py`, etc.) load from Key Vault automatically when `KEYVAULT_NAME` is set.

**For skills** (source-system-analyser, volume-projection), use the wrapper so they receive env from Key Vault:

```bash
python scripts/run_with_keyvault.py .cursor/skills/source-system-analyser/scripts/source_system_analyzer.py @DATABASE_URL schema.json public
```

The `@DATABASE_URL` substitutes the value from Key Vault (or .env) into the command.

## 6. Fallback

If `KEYVAULT_NAME` is not set or Key Vault is unavailable (e.g. no network, not logged in), scripts fall back to loading from `.env`. Local development continues to work without Key Vault.
