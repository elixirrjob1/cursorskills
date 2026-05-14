---
name: source-system-analyser
description: Analyze source systems for ingestion readiness and data quality across databases, APIs, and flat files. Use when generating schema metadata, identifying data quality risks, mapping source structures, evaluating delete/late-arrival/timezone behavior, or producing a normalized schema.json contract for downstream ingestion. Triggers: analyze, profile, assess, audit, inspect source, ingestion readiness check, schema contract, data quality risks, source system, database analysis, API analysis, flat file, CSV schema, volume projection, capacity forecast.
version: 1.0.0
lifecycle: production
owner: data-engineering
dependencies:
  - scripts/source_system_analyzer.py
  - scripts/apis/api_reader.py + api_analyzer.py
  - scripts/flat/tabular_schema_json.py
  - scripts/volume_projection/collector.py + predictor.py
last_reviewed: 2026-05-06
rollback: pin to commit hash; revert via PR
---

# Source System Analyser

## Quick Routing

1. Determine source type (database, API, or flat file).
2. For database sources, run the database preflight before analysis.
3. Route to the matching module.
4. Produce normalized output.

Source routes:
- Database sources: `references/databases/postgresql/README.md`, `references/databases/mssql/README.md`, `references/databases/oracle/README.md`
- API sources: `references/apis/test-api/README.md` (current provider), fallback `references/apis/generic/README.md`
- Flat file sources (CSV/Excel): `references/flat/generic/README.md`
- Volume and capacity forecasting: `references/volume-projection/README.md`
- Unknown or mixed source types: start with `references/routing.md`

## Shared Requirements

Load these before executing any source workflow:
- Prerequisites: `references/shared/prerequisites.md`
- Output contract: `references/shared/output-schema.md`
- Classification review workflow: `references/shared/classification-review-workflow.md`
- Troubleshooting: `references/shared/troubleshooting.md`

## Database Preflight

Before starting database analysis:

1. Check whether `db-analysis-config.json` already exists in the working directory.
2. If it exists, reuse it and do not ask database exclusion or row-limit questions again.
3. If it does not exist, ask the user:
   - whether they want to exclude any schemas
   - whether they want to exclude any tables
   - whether they want to set a maximum row limit
4. If the user answers yes to any of those, create `db-analysis-config.json` with this shape:

```json
{
  "exclude_schemas": [],
  "exclude_tables": [],
  "max_row_limit": null
}
```

5. If the user answers no to all three questions, do not create the JSON file.

This preflight applies only to database sources. API and flat-file workflows should not ask these questions.

## API Preflight

Before starting API analysis, run this preflight to produce the scoping config used by `references/apis/generic/endpoint-scoping.md`.

1. Check whether `api-scope-config.json` already exists in the working directory.
2. If it exists, reuse it silently — do not ask the user questions again.
3. If it does not exist, ask the user:
   - Base URL of the API (e.g. `https://api.example.com`)
   - Whether an OpenAPI/Swagger spec is available: provide a URL or local file path, or `none`
   - Auth type: `bearer`, `api_key`, or `none`
   - If bearer or api_key: the **variable name** (env var name or Key Vault secret name) — never ask for the value itself
   - Any path prefixes to exclude from scoping (e.g. `/admin`, `/internal`)
4. Create `api-scope-config.json` in the working directory:

```json
{
  "base_url": "https://...",
  "openapi_spec_url": null,
  "auth_type": "bearer",
  "auth_env_var": "MY_API_TOKEN",
  "exclude_path_prefixes": [],
  "methods_to_scope": ["GET"]
}
```

5. Route to `references/apis/generic/endpoint-scoping.md` for the full scoping procedure.

This preflight applies only to API sources. Database and flat-file workflows should not ask these questions.

## Description Enrichment Continuation

After any analysis writes `schema.json` — whether from a database, API, or flat file source — check whether any table has an empty `table_description` or any column has an empty `column_description`.

If any descriptions are missing:

1. Build a checklist file:

```bash
python3 scripts/build_description_enrichment_checklist.py schema.json
```

2. Use the checklist as the ordered worklist for missing descriptions.
3. Work table by table in checklist order.
4. For each table:
   - complete missing `column_description` items first
   - query up to 3 sample rows per unresolved column when needed  _(3 rows: enough context to infer column meaning without pulling significant data volume)_
   - write generated column descriptions into each checklist item's `proposed_description`
   - only after that table's column descriptions are complete, generate the table's `table_description` from the completed column descriptions for that table
5. Do not do a separate table-query step unless the column-level context is still insufficient.
6. Merge the checklist back into the main analyzer JSON:

```bash
python3 scripts/apply_description_enrichment.py schema.json schema_description_checklist.json
```

Do not treat the analysis as complete while the final analyzer JSON still has blank table or column descriptions.

## Backward Compatibility

Keep existing database analyzer entrypoint unchanged:

```bash
.venv/bin/python scripts/source_system_analyzer.py <database_url> <output_json_path> [schema] [--dialect postgresql|mssql|oracle]
# or: .venv/bin/python scripts/source_system_analyzer.py --database-url-secret AZURE-MSSQL-URL <output_json_path> [schema]
```

The merged API and tabular flows are now available directly inside this skill:
- API script: `scripts/apis/api_reader.py`
- Test API wrapper: `scripts/apis/test_api/test_api_reader.py`
- API analyzer: `scripts/apis/api_analyzer.py`
- Tabular schema script: `scripts/flat/tabular_schema_json.py`
- Volume projection collector: `scripts/volume_projection/collector.py`
- Volume projection predictor: `scripts/volume_projection/predictor.py`

## Common Mistakes

- **Re-asking preflight questions on rerun**: Always check for `db-analysis-config.json` first. If it exists, reuse it silently — do not ask the user about exclusions again.
- **Leaving blank descriptions in schema.json**: The analysis is not complete until all `table_description` and `column_description` fields are filled. Always run the description enrichment continuation after the analyzer.
- **Passing database URL with credentials as a CLI argument**: `<database_url>` embeds passwords visible in `ps aux` and shell history. Prefer `--database-url-secret` (reads from Azure Key Vault) in shared or production environments.
- **Running the full analyzer to fix null classifications**: Use the classification review workflow (one family at a time) — not a full rerun — when improving concept assignments.
- **Using source_system_analyzer.py for flat files**: CSV/Excel inputs use `tabular_schema_json.py`, not the database analyzer.

## Fallback Rules

- If source type is unclear, classify in this order: URL scheme, protocol (`http/https`), file extension, then available metadata.
- If still ambiguous, ask the user for source type before running scripts.
- If a provider-specific parser is missing, use the generic workflow and map output to the shared schema contract.
- Do not hardcode credentials; use environment variables or user-provided secure values.
