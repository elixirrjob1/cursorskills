# Eval Suite: source-system-analyser

_Re-run this suite after every skill update to catch regressions._

## Should-trigger

| # | Input | Expected behavior |
|---|-------|-------------------|
| 1 | "Analyze our PostgreSQL database — I already configured exclusions, here's the connection: postgresql://..." AND db-analysis-config.json exists in working dir | Tests "If it exists, reuse it and do not ask": agent must NOT ask preflight questions, must run the analyzer directly with the existing config |
| 2 | "Analyze our MSSQL database, no exclusions needed" AND no db-analysis-config.json exists | Tests "If it does not exist, ask": agent asks all 3 preflight questions (schemas, tables, row limit); when user answers no to all three, agent does NOT create db-analysis-config.json, then runs analyzer |
| 3 | "The schema.json was just generated but I notice some columns have empty descriptions" | Tests Description Enrichment Continuation: agent runs build_description_enrichment_checklist.py, works column-by-column (columns first, then table description), queries up to 3 sample rows per unresolved column, then merges with apply_description_enrichment.py |
| 4 | "Can you profile this CSV file for ingestion? File: orders_2025.csv" | Tests flat file routing: agent routes to flat/generic module, runs tabular_schema_json.py inspect first, then to-json, produces schema.json |
| 5 | "We need a capacity growth forecast for our PostgreSQL database over the next 12 months" | Tests volume projection routing: agent routes to volume-projection module, runs collector.py --setup then --collect, then predictor.py, produces capacity_report.json |

## Should-not-trigger

| # | Input | Expected behavior |
|---|-------|-------------------|
| 1 | "Create a dbt model for the orders table" | Should be handled by dbt-model-from-stm — not a source analysis task |
| 2 | "Sync our Snowflake schema metadata to OpenMetadata" | Should be handled by catalog-sync — write operation to catalog, not source analysis |
| 3 | "Query my database and show me the top 10 customers by revenue" | Should be handled by natural-language-data-query — data retrieval question, not ingestion readiness analysis |

## Edge cases

| # | Input | Expected behavior |
|---|-------|-------------------|
| 1 | "I have a schema_columns.xlsx file that describes my source schema" | Tests routing disambiguation: .xlsx is flat file route, NOT the database analyzer — agent must route to flat/generic, run tabular_schema_json.py inspect on the xlsx, not source_system_analyzer.py |
| 2 | "My schema.json has a lot of null concept_ids and low confidence scores" | Tests classification review workflow: agent must follow the classification-review-workflow (bucket nulls/false positives, fix one family at a time, rerun) — NOT just blindly rerun the full analyzer |
| 3 | "Analyze my data source" (no source type or connection given) | Tests fallback rule: source type is unclear — agent must ask for source type and connection details before running any script |
