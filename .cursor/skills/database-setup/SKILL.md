---
name: database-setup
description: Provisions database/warehouse objects (users, roles, schemas, warehouses) for an ingestion destination using the repo's PAT-based setup scripts, with strict required env or Key Vault-backed values and no business-value defaults. Currently supports Snowflake for Fivetran; designed to extend to Databricks, Redshift, Fabric, and other warehouse destinations. Use when the user wants Cursor to set up a destination warehouse and output the exact values needed for downstream ingestion configuration.
---

# Snowflake Setup

Use this skill when the user wants the Snowflake side of a Fivetran destination prepared in one flow.

This skill does not assume defaults for the Snowflake setup values. If any required value is missing, stop and ask the user to set it in `.env` or in Azure Key Vault through the repo's env loader. Never invent names or credentials.

## Required values

Before running setup, ensure all of these are available from env or Key Vault-backed env loading:

- `SNOWFLAKE_PAT`
- `SNOWFLAKE_SQL_API_HOST` or `SNOWFLAKE_ACCOUNT`
- `SNOWFLAKE_SQL_API_EXECUTION_WAREHOUSE`
- `SNOWFLAKE_FIVETRAN_PASSWORD`
- `SNOWFLAKE_FIVETRAN_USER`
- `SNOWFLAKE_FIVETRAN_ROLE`
- `SNOWFLAKE_FIVETRAN_WAREHOUSE`
- `SNOWFLAKE_DRIP_DATABASE`
- `SNOWFLAKE_BRONZE_SCHEMA`

Optional for downstream handoff:

- `SNOWFLAKE_HOST`

## Workflow

1. Run the skill wrapper in check mode first:

```bash
python3 .cursor/skills/database-setup/scripts/run_snowflake_setup.py --check-only
```

2. If any required value is missing, ask the user to add it to `.env` or the configured Key Vault. Do not proceed until the missing values are present.

3. Run the Snowflake setup:

```bash
python3 .cursor/skills/database-setup/scripts/run_snowflake_setup.py
```

4. Report the handoff values for downstream Fivetran destination setup:

- host
- user
- password source name only
- role
- warehouse
- database
- schema

## Guardrails

- PAT is for provisioning only. Do not present it as the Fivetran login credential.
- Never print secret values.
- If setup fails, report the exact missing variable or failing Snowflake API step.
- Run from the repo root so the wrapper can find `scripts/snowflake_setup/run_snowflake_sql_pat.py`.
