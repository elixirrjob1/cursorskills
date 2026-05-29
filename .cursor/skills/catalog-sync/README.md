# Catalog Sync

> **Owner:** Platform / Data Engineering · **Version:** Tracks repo `main` · **Lifecycle:** Test / Deploy

Configure database services in OpenMetadata, run metadata ingestion, inspect catalog assets, and assign glossary terms or classification tags via MCP.

## Latest Review

| Metric | Result |
|--------|--------|
| Overall Verdict | ✅ PASS |
| Unit Tests | 12 / 12 (100%) |
| Assertions | 48 / 48 (100%) |
| Categories | 13 / 13 (100%) |
| High Failures | — |
| Medium Failures | — |
| Comparator | — (partial re-run) |

[Full benchmark report](tests/results/benchmark-catalog-sync-latest.md)

## Unit Test Results

| # | Test | Type | Result |
|---|------|------|--------|
| 1 | Postgres sync + PII tag | should-trigger | ✅ PASS |
| 2 | Snowflake sync + inspect | should-trigger | ✅ PASS |
| 3 | MySQL onboard | should-trigger | ✅ PASS |
| 4 | Postgres re-run pipeline | should-trigger | ✅ PASS |
| 5 | Glossary on orders | should-trigger | ✅ PASS |
| 6 | Oracle new source | should-trigger | ✅ PASS |
| 7 | Vocab publish routing | should-not-trigger | ✅ PASS |
| 8 | AI glossary routing | should-not-trigger | ✅ PASS |
| 9 | dbt lineage routing | should-not-trigger | ✅ PASS |
| 10 | password_env edge | edge-case | ✅ PASS |
| 11 | duplicate service edge | edge-case | ✅ PASS |
| 12 | vague fix catalog | edge-case | ✅ PASS |

## Review History

| Reviewed at (UTC) | Unit Tests | Assertions | Categories | Verdict |
|-------------------|------------|------------|------------|---------|
| 2026-05-29T07:15:33Z | 10/12 | 45/48 | 10/13 | ❌ FAIL |
| **2026-05-29T07:31:25Z** | **12/12** | **48/48** | **13/13** | **✅ PASS** |

## Quick Start

### Prerequisites

- OpenMetadata MCP server registered in Cursor (`user-openmetadata`)
- `OM_BASE_URL` and `OM_TOKEN` in `.env`
- Database connection secrets as `.env` variable names (see Credentials in SKILL.md)

### Workflow

1. `user-openmetadata:test_connection`
2. `user-openmetadata:list_database_services` / `get_database_service`
3. Create or update service and pipeline
4. `user-openmetadata:run_ingestion_pipeline` → `get_ingestion_status`
5. Inspect with `list_databases` / `list_schemas` / `list_tables` / `get_table`
6. Assign glossary or tags; re-read entity to confirm

**New source:** Onboard route — interview → `.cursor/flat/onboard_plan_<name>.json` → approve → `python .cursor/skills/catalog-sync/scripts/catalog_onboard_apply.py <plan>`

## Skill Triggers

Catalog sync, metadata import, data-catalog setup, OpenMetadata ingestion, onboard/register new database source, assign classification or glossary tags on catalog assets.

### Adjacent skills

| Skill | Purpose |
|---|---|
| `catalog-vocab-publisher` | Publish governance vocabulary `.md` to Classifications/Tags |
| `catalog-glossary-tagger` | AI glossary matching on catalogued assets |
| `governance-import-dbt-lineage` | dbt manifest lineage import |
| `stm-to-catalog-enricher` | STM-driven enrichment after dbt |

## Files

```
SKILL.md                               ← agent instructions
scripts/catalog_onboard_apply.py       ← onboard apply (skill-local)
scripts/catalog_onboard_rollback.py    ← onboard rollback (skill-local)
scripts/om_auth.py                     ← bundled OM auth helper
(shared) scripts/keyvault_loader.py    ← repo-root secret allowlist (not bundled)
tests/test-cases.md                    ← behavioural assertions
tests/test_skill.py                    ← eval suite stubs
tests/test_apply_script.py             ← guardrail pytest
tests/evals/evals.json                 ← machine-readable evals
tests/results/history.json             ← review history
tests/results/benchmark-catalog-sync-latest.md
tests/results/benchmark-catalog-sync-latest.html
```

## Registry

| Field | Value |
|-------|-------|
| Owner | Platform / Data Engineering |
| Reviewer | Peer review required before merge |
| Version | Tracks repo `main` |
| Lifecycle stage | Test / Deploy |
| Last evaluated | 2026-05-29T073125Z |
| Source | `.cursor/skills/catalog-sync/` |
