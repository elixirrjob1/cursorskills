---
name: source-system-analyser
description: Analyze source systems for ingestion readiness and data quality across databases, APIs, and flat files. Use when generating schema metadata, identifying data quality risks, mapping source structures, evaluating delete/late-arrival/timezone behavior, or producing a normalized schema.json contract for downstream ingestion.
---

# Source System Analyser

## Quick Routing

1. Determine source type (database, API, or flat file).
2. Route to the matching module.
3. Produce normalized output.

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
- Troubleshooting: `references/shared/troubleshooting.md`

## Backward Compatibility

Keep existing database analyzer entrypoint unchanged:

```bash
.venv/bin/python scripts/source_system_analyzer.py <database_url> <output_json_path> [schema] [--dialect postgresql|mssql|oracle]
```

The merged API and tabular flows are now available directly inside this skill:
- API script: `scripts/apis/api_reader.py`
- Test API wrapper: `scripts/apis/test_api/test_api_reader.py`
- API analyzer: `scripts/apis/api_analyzer.py`
- Tabular schema script: `scripts/flat/tabular_schema_json.py`
- Volume projection collector: `scripts/volume_projection/collector.py`
- Volume projection predictor: `scripts/volume_projection/predictor.py`

## Fallback Rules

- If source type is unclear, classify in this order: URL scheme, protocol (`http/https`), file extension, then available metadata.
- If still ambiguous, ask the user for source type before running scripts.
- If a provider-specific parser is missing, use the generic workflow and map output to the shared schema contract.
- Do not hardcode credentials; use environment variables or user-provided secure values.
