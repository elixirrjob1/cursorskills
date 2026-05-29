# Eval Suite: catalog-sync

_Re-run this suite after every skill update to catch regressions._

## Should-trigger

| # | Input | Expected behavior |
|---|-------|-------------------|
| 1 | Sync my Postgres database into OpenMetadata and assign the PII classification tag to the customers table. | Loads catalog-sync; uses OpenMetadata MCP tools (`test_connection`, service create/update, `run_ingestion_pipeline`, `assign_tags_to_table`); does not invent raw REST calls in chat. |
| 2 | Run catalog metadata sync for Snowflake and show me imported schemas and tables. | Follows Workflow steps 1–6: connectivity check, service discovery, ingestion run/status, `list_schemas` / `list_tables` inspection. |
| 3 | Onboard a new MySQL source to the catalog for the first time. | Routes to Onboard Source System: interview questions, plan file at `.cursor/flat/onboard_plan_<service>.json`, apply via `catalog_onboard_apply.py`; does not call `create_database_service` directly from chat on this route. |
| 4 | Configure OpenMetadata ingestion for an existing Postgres service and re-run the pipeline. | Uses standard Workflow (not onboard): `update_metadata_ingestion_pipeline` / `run_ingestion_pipeline`, not duplicate service creation. |
| 5 | Assign glossary term Customer to the orders table in OpenMetadata. | Uses `assign_glossary_term_to_table` (or column variant if specified); re-reads entity to confirm; distinguishes glossary vs classification tags. |
| 6 | Set up catalog ingestion for a new Oracle database we have never registered in OpenMetadata. | Triggers onboard route phrases; asks for service name, type, connection fields; password via `.env` variable name only. |

## Should-not-trigger

| # | Input | Expected behavior |
|---|-------|-------------------|
| 7 | Push our governance vocabulary Markdown file to OpenMetadata classifications and tags. | Should use catalog-vocab-publisher, not catalog-sync (no database service / ingestion orchestration). |
| 8 | Use AI to map business glossary terms onto already-catalogued Snowflake tables. | Should use catalog-glossary-tagger, not full catalog-sync ingestion setup. |
| 9 | Import dbt manifest lineage into OpenMetadata. | Should use governance-import-dbt-lineage skill, not catalog-sync. |

## Edge cases

| # | Input | Expected behavior |
|---|-------|-------------------|
| 10 | Onboard mysql_retail_erp — the password is already in .env as MYSQL_RETAIL_PROD_PASSWORD. | Asks only to confirm the env var name exists (grep name, not value); plan uses `password_env`; never requests or prints the secret value. |
| 11 | Onboard mysql_retail_erp but that service already exists in OpenMetadata. | Calls `user-openmetadata:get_database_service` first; if exists, hard stop (no plan/apply/create); if 404, reports mismatch and lists services; never runs apply. |
| 12 | Fix my data catalog. | Asks what to fix **before** any MCP call; lists sync / onboard / tag / pipeline options; does not run `run_ingestion_pipeline` in the first reply. |
