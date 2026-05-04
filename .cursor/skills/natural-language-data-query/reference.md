# Reference — OpenMetadata MCP & Snowflake Patterns

---

## OpenMetadata MCP Server

OpenMetadata has a **native MCP server** built into the platform. It is the
primary connection mechanism for the natural-language-data-query skill.

### Endpoint

```
{OM_URL}/mcp
```

### Available MCP tools

| Tool | Purpose |
|---|---|
| `search_metadata` | Keyword-based search for tables, topics, dashboards, etc. |
| `semantic_search` | Vector/meaning-based discovery for vague queries |
| `get_entity_details` | Full metadata for a known entity (columns, tags, joins, lineage) |
| `get_entity_lineage` | Upstream / downstream dependency graph |
| `create_glossary_term` | Add a new glossary term |
| `patch_entity` | Update an entity's metadata |

### Cursor workspace configuration

Credentials are stored in the workspace `.env` file (gitignored) and loaded
automatically by the bridge script. See `.env` for the actual values.

```
OPENMETADATA_API_URL=<your-om-url>
OPENMETADATA_USERNAME=<your-email>
OPENMETADATA_PASSWORD=<your-password>
# or use a long-lived token instead:
OPENMETADATA_TOKEN=<bot-jwt-token>
```

For production, set `SECRETS_PROVIDER=azure-keyvault` and `SECRETS_URL=https://<vault>.vault.azure.net`
in `.env` — the bridge will read all credentials from Key Vault automatically.

---

## Snowflake Connection Patterns

### Account identifier format

`query_engine.py` accepts either form and normalises automatically:

```
account-locator                         # bare locator
account-locator.snowflakecomputing.com  # full hostname (stripped automatically)
xy12345.us-east-1.aws                   # region-qualified locator
```

### Credentials in `.env`

```
SNOWFLAKE_BEARER_TOKEN=<oauth-pat-token>
SNOWFLAKE_SQL_API_HOST=https://<account>.snowflakecomputing.com
SNOWFLAKE_WAREHOUSE=<warehouse>
SNOWFLAKE_DATABASE=<database>
SNOWFLAKE_ROLE=<role>
```

### Key Snowflake date functions

| Function | Example | Notes |
|---|---|---|
| `CURRENT_DATE()` | `WHERE order_date = CURRENT_DATE()` | Today |
| `DATEADD(part, n, date)` | `DATEADD('month', -1, CURRENT_DATE())` | Relative offset |
| `DATE_TRUNC(part, date)` | `DATE_TRUNC('month', order_date)` | Period start |
| `LAST_DAY(date)` | `LAST_DAY(DATEADD('month', -1, CURRENT_DATE()))` | Period end |
| `DATEDIFF(part, d1, d2)` | `DATEDIFF('day', start_date, end_date)` | Interval |

### "Last month" SQL pattern

```sql
WHERE order_date
  BETWEEN DATE_TRUNC('month', DATEADD('month', -1, CURRENT_DATE()))
      AND LAST_DAY(DATEADD('month', -1, CURRENT_DATE()))
```

### "Last quarter" SQL pattern

```sql
WHERE order_date
  BETWEEN DATE_TRUNC('quarter', DATEADD('quarter', -1, CURRENT_DATE()))
      AND DATEADD('day', -1, DATE_TRUNC('quarter', CURRENT_DATE()))
```

---

## Secrets Manager Secret Keys

All three providers use the same key names. Store as individual secrets (Azure KV, HashiCorp Vault)
or as a single JSON blob (AWS Secrets Manager).

| Key name | Maps to env var |
|---|---|
| `openmetadata-api-url` | `OPENMETADATA_API_URL` |
| `openmetadata-username` | `OPENMETADATA_USERNAME` |
| `openmetadata-password` | `OPENMETADATA_PASSWORD` |
| `openmetadata-token` | `OPENMETADATA_TOKEN` |
| `snowflake-bearer-token` | `SNOWFLAKE_BEARER_TOKEN` |
| `snowflake-sql-api-host` | `SNOWFLAKE_SQL_API_HOST` |
| `snowflake-warehouse` | `SNOWFLAKE_WAREHOUSE` |
| `snowflake-database` | `SNOWFLAKE_DATABASE` |
| `snowflake-role` | `SNOWFLAKE_ROLE` |
| `snowflake-account` | `SNOWFLAKE_ACCOUNT` *(connector fallback)* |
| `snowflake-username` | `SNOWFLAKE_USERNAME` *(connector fallback)* |
| `snowflake-password` | `SNOWFLAKE_PASSWORD` *(connector fallback)* |

---

## Provider Setup Guides

### Azure Key Vault

```bash
az keyvault create --name <vault-name> --resource-group <rg> --location <region>

az role assignment create \
  --role "Key Vault Secrets User" \
  --assignee $(az ad signed-in-user show --query id -o tsv) \
  --scope $(az keyvault show --name <vault-name> --query id -o tsv)

az keyvault secret set --vault-name <vault-name> \
  --name openmetadata-api-url --value "<your-om-url>"
az keyvault secret set --vault-name <vault-name> \
  --name snowflake-bearer-token --value "<your-pat>"
# repeat for each key above

az login  # local auth
```

In `.env` (or `mcp.json` env block) set:
```
SECRETS_PROVIDER=azure-keyvault
SECRETS_URL=https://<vault-name>.vault.azure.net
```

In production (Azure VM / AKS / App Service): enable a **Managed Identity** — no static credentials needed.

---

### AWS Secrets Manager

Store all credentials as a **single JSON secret**:

```json
{
  "openmetadata-api-url": "<your-om-url>",
  "openmetadata-username": "<email>",
  "openmetadata-password": "<password>",
  "snowflake-bearer-token": "<pat>",
  "snowflake-sql-api-host": "https://<account>.snowflakecomputing.com",
  "snowflake-warehouse": "<warehouse>",
  "snowflake-database": "<database>"
}
```

```bash
aws secretsmanager create-secret \
  --name businesschat/creds \
  --region us-east-1 \
  --secret-string file://creds.json
```

In `.env` set:
```
SECRETS_PROVIDER=aws-secrets-manager
SECRETS_REGION=us-east-1
SECRETS_NAME=businesschat/creds
```

In production (ECS / Lambda / EC2): attach an **IAM role** with `secretsmanager:GetSecretValue`.

---

### HashiCorp Vault (KV-v2)

```bash
vault secrets enable -path=secret kv-v2

vault kv put secret/businesschat/creds \
  openmetadata-api-url="<your-om-url>" \
  openmetadata-username="<email>" \
  openmetadata-password="<password>" \
  snowflake-bearer-token="<pat>" \
  snowflake-sql-api-host="https://<account>.snowflakecomputing.com" \
  snowflake-warehouse="<warehouse>" \
  snowflake-database="<database>"
```

In `.env` set:
```
SECRETS_PROVIDER=hashicorp-vault
SECRETS_URL=https://<vault-address>
SECRETS_PATH=secret/data/businesschat/creds
VAULT_TOKEN=<token>  # use AppRole or Kubernetes auth in production
```
