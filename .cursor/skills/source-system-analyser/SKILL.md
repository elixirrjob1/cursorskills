---
name: source-system-analyser
description: Analyze source systems for ingestion readiness and data quality across databases, APIs, and flat files. Use when generating schema metadata, identifying data quality risks, mapping source structures, evaluating delete/late-arrival/timezone behavior, or producing a normalized schema.json contract for downstream ingestion.
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

## Description Enrichment Continuation

After database analysis writes `schema.json`, check whether any table has an empty `table_description` or any column has an empty `column_description`.

If any descriptions are missing:

1. Build a checklist file:

```bash
python3 scripts/build_description_enrichment_checklist.py schema.json
```

2. Use the checklist as the ordered worklist for missing descriptions.
3. Work table by table in checklist order.
4. For each table:
   - complete missing `column_description` items first
   - query up to 3 sample rows per unresolved column when needed
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
- Preferred database wrapper for analysis + checklist creation: `scripts/run_source_analysis_with_description_checklist.py`

## Fallback Rules

- If source type is unclear, classify in this order: URL scheme, protocol (`http/https`), file extension, then available metadata.
- If still ambiguous, ask the user for source type before running scripts.
- If a provider-specific parser is missing, use the generic workflow and map output to the shared schema contract.
- Do not hardcode credentials; use environment variables or user-provided secure values.
