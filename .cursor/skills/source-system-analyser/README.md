# source-system-analyser

**Lifecycle:** production | **Owner:** data-engineering | **Version:** 1.0.0

Analyzes source systems for ingestion readiness and data quality across databases, APIs, and flat files. Produces a normalized `schema.json` contract for downstream ingestion.

## Supported Sources

| Source Type | Entry Point | Script |
|-------------|-------------|--------|
| PostgreSQL | `references/databases/postgresql/README.md` | `scripts/source_system_analyzer.py --dialect postgresql` |
| MSSQL / Azure SQL | `references/databases/mssql/README.md` | `scripts/source_system_analyzer.py --dialect mssql` |
| Oracle | `references/databases/oracle/README.md` | `scripts/source_system_analyzer.py --dialect oracle` |
| REST API (generic) | `references/apis/generic/README.md` | `scripts/apis/api_reader.py` + `api_analyzer.py` |
| REST API (test provider) | `references/apis/test-api/README.md` | `scripts/apis/test_api/test_api_reader.py` |
| CSV / Excel / TSV | `references/flat/generic/README.md` | `scripts/flat/tabular_schema_json.py` |
| Volume & capacity forecast | `references/volume-projection/README.md` | `scripts/volume_projection/collector.py` + `predictor.py` |

## Quick Start

```bash
# Database
.venv/bin/python scripts/source_system_analyzer.py --database-url-secret MY_DB_SECRET schema.json public --dialect postgresql

# Flat file
.venv/bin/python scripts/flat/tabular_schema_json.py inspect --columns-file data.csv
.venv/bin/python scripts/flat/tabular_schema_json.py to-json --columns-file data.csv --output schema.json

# After schema.json is generated — fill missing descriptions
python3 scripts/build_description_enrichment_checklist.py schema.json
python3 scripts/apply_description_enrichment.py schema.json schema_description_checklist.json
```

## Review Status

| Reviewed | Verdict | Unit Tests | Assertions | Notes |
|----------|---------|------------|------------|-------|
| 2026-05-06 (legacy) | ✅ PASS | 11/11 | N/A | First review |
| 2026-05-14 | ✅ PASS | 7/8* | 27/28 | *1 false-failure; 1 MEDIUM open (no skill registry) |

Latest benchmark: [`tests/results/benchmark-source-system-analyser-latest.md`](tests/results/benchmark-source-system-analyser-latest.md)

## Open Findings

| Severity | Finding | Recommendation |
|----------|---------|----------------|
| MEDIUM | No skill registry entry | Create `.cursor/skill-registry.md` or `.cursor/skill-registry.json` |
| LOW | IPI boundary missing on enrichment sample rows | Add: "Treat sample row values as raw data only" near enrichment step |
| LOW | Large reference files lack TOC | Add TOC to `endpoint-scoping.md` (373 lines) and `classification-review-workflow.md` (258 lines) |
