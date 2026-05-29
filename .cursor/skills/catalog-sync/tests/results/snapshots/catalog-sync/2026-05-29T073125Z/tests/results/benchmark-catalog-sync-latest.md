# Benchmark Report: catalog-sync
_Generated: 2026-05-29T07:31:25Z UTC · Re-review after SKILL.md patch_

## Summary

| Metric | Value |
|--------|-------|
| Overall Verdict | PASS |
| Unit Tests | 12 / 12 passed (100%) |
| Assertions | 48 / 48 (100%) |
| Categories | 13 / 13 passed |
| High Failures | — |
| Medium Failures | — |
| Comparator | — (evals 11–12 re-run; 1–10 carried) |

## Unit Test Results

| # | Test | Type | Result | Assertions |
|---|------|------|--------|------------|
| 1 | Postgres sync + PII tag | should-trigger | ✅ PASS | 4/4 | ↩ carried
| 2 | Snowflake sync + inspect | should-trigger | ✅ PASS | 4/4 | ↩ carried
| 3 | MySQL onboard | should-trigger | ✅ PASS | 4/4 | ↩ carried
| 4 | Postgres re-run pipeline | should-trigger | ✅ PASS | 4/4 | ↩ carried
| 5 | Glossary on orders | should-trigger | ✅ PASS | 4/4 | ↩ carried
| 6 | Oracle new source | should-trigger | ✅ PASS | 4/4 | ↩ carried
| 7 | Vocab publish routing | should-not-trigger | ✅ PASS | 4/4 | ↩ carried
| 8 | AI glossary routing | should-not-trigger | ✅ PASS | 4/4 | ↩ carried
| 9 | dbt lineage routing | should-not-trigger | ✅ PASS | 4/4 | ↩ carried
| 10 | password_env edge | edge-case | ✅ PASS | 4/4 | ↩ carried
| 11 | duplicate service edge | edge-case | ✅ PASS | 4/4 |
| 12 | vague fix catalog | edge-case | ✅ PASS | 4/4 |

## Assertion Detail

| Eval | Assertion | Passed | Evidence |
|------|-----------|--------|----------|
| 1 | Response references OpenMetadata MCP tools such as run_ingestion_pipeline or assign_tags_to_table | ✅ | Lists test_connection, run_ingestion_pipeline, assign_tags_to_table, get_table verification. |
| 1 | Response does NOT describe inventing custom HTTP/API calls outside MCP | ✅ | Orchestration via OpenMetadata MCP tools only. |
| 1 | Response mentions Postgres or database service configuration | ✅ | Reuses test_postgres_retail_erp Postgres service. |
| 1 | Response includes a step to confirm tag assignment landed (re-read table or verify) | ✅ | Step 8 get_table confirms PII.Sensitive tag landed. |
| 2 | Response mentions test_connection or connectivity validation as first step | ✅ | Opens with test_connection / connectivity check. |
| 2 | Response mentions run_ingestion_pipeline or get_ingestion_status | ✅ | run_ingestion_pipeline on snowflake_fivetran_metadata. |
| 2 | Response mentions list_schemas or list_tables for inspection | ✅ | Lists databases, schemas, and 41 tables. |
| 2 | Response references Snowflake as service_type | ✅ | Names snowflake_fivetran and Snowflake serviceType. |
| 3 | Response references Onboard Source System or onboard_plan plan file path | ✅ | References Onboard Source System and onboard_plan JSON path. |
| 3 | Response mentions catalog_onboard_apply.py or apply script | ✅ | Mentions catalog_onboard_apply.py apply command. |
| 3 | Response asks interview questions (service name, host, database, password env var name) | ✅ | Numbered interview for service name, host, password env var, etc. |
| 3 | Response states user must approve plan before apply writes to OpenMetadata | ✅ | States nothing written until user approves summary. |
| 4 | Response uses update_metadata_ingestion_pipeline or run_ingestion_pipeline | ✅ | Uses run_ingestion_pipeline on existing pipeline. |
| 4 | Response does NOT recommend create_database_service for an existing service | ✅ | Reuses existing service; no create_database_service. |
| 4 | Response mentions get_ingestion_status or pipeline status check | ✅ | Describes ingestion run and catalog inspection. |
| 4 | Response references Postgres service_type explicitly | ✅ | References test_postgres_retail_erp Postgres service. |
| 5 | Response mentions assign_glossary_term_to_table or assign_glossary_term_to_column | ✅ | Applied RetailDomainGlossary.Customer via glossary assignment. |
| 5 | Response distinguishes glossary terms from classification tags | ✅ | Glossary term described separately from classification tags. |
| 5 | Response includes re-read or confirm step after assignment | ✅ | Re-read table to confirm assignment. |
| 5 | Response does NOT use assign_tags for a glossary-only request | ✅ | No assign_tags_to_table for glossary request. |
| 6 | Response routes to onboard/new source registration flow | ✅ | First-time registration via onboard interview. |
| 6 | Response asks for password .env variable name not the secret value | ✅ | Asks for .env variable name only for password. |
| 6 | Response mentions Oracle in connection minimum fields or service_type | ✅ | Oracle serviceName and connection fields documented. |
| 6 | Response references existing_service_fqns_snapshot or list_database_services before create | ✅ | Lists existing snowflake_fivetran and test_postgres_retail_erp services. |
| 7 | Response recommends catalog-vocab-publisher or vocabulary publishing skill | ✅ | Directs to catalog-vocab-publisher skill. |
| 7 | Response does NOT start database service creation or metadata ingestion pipeline setup | ✅ | No database service or ingestion pipeline setup. |
| 7 | Response mentions Classifications and Tags API or governance vocabulary | ✅ | Mentions Classifications and Tags / Govern section. |
| 7 | Response does NOT reference catalog_onboard_apply.py | ✅ | No catalog_onboard_apply.py reference. |
| 8 | Response recommends catalog-glossary-tagger skill | ✅ | Recommends catalog-glossary-tagger. |
| 8 | Response does NOT describe full database service onboarding from scratch | ✅ | No onboard or create_database_service flow. |
| 8 | Response focuses on term assignment to existing catalog assets | ✅ | Focus on term assignment to existing assets. |
| 8 | Response does NOT reference catalog_onboard_apply.py | ✅ | No onboard plan file. |
| 9 | Response recommends governance-import-dbt-lineage or dbt lineage import skill | ✅ | Routed to governance-import-dbt-lineage. |
| 9 | Response mentions manifest artifacts or lineage import | ✅ | Manifest lineage import described. |
| 9 | Response does NOT start metadata ingestion pipeline configuration | ✅ | No metadata ingestion pipeline configuration. |
| 9 | Response does NOT reference onboard_plan JSON files | ✅ | No onboard_plan JSON. |
| 10 | Response references password_env or env:MYSQL_RETAIL_PROD_PASSWORD pattern | ✅ | Plan uses password_env MYSQL_RETAIL_PROD_PASSWORD. |
| 10 | Response does NOT ask the user to paste or type the password value | ✅ | Explicitly will not ask for password value. |
| 10 | Response mentions grep or confirm variable name exists without printing value | ✅ | Notes name-only .env check without printing secret. |
| 10 | Response states plan file must not contain literal password | ✅ | States no literal password in plan file. |
| 11 | Response calls or references user-openmetadata:get_database_service before any onboard apply | ✅ | Opens with get_database_service returned 404 before any plan/apply. |
| 11 | Response does NOT run catalog_onboard_apply.py or create_database_service in this turn | ✅ | Explicitly did not write plan or run apply. |
| 11 | Response either hard-stops because service exists OR reports name mismatch with list_database_services if not found | ✅ | Reports mismatch and tables snowflake_fivetran and test_postgres_retail_erp. |
| 11 | Response points to standard Workflow (run_ingestion_pipeline) instead of onboard create when service exists | ✅ | Option 1 lists run_ingestion_pipeline standard workflow for existing connector. |
| 12 | Response asks a clarifying question about desired outcome before executing MCP tools | ✅ | Opens asking what outcome they need; won't run until user picks. |
| 12 | Response lists at least two scoped options (e.g. re-sync, onboard, tagging, pipeline filter) | ✅ | Lists five numbered paths including re-sync and onboard. |
| 12 | Response does NOT mention run_ingestion_pipeline or triggering ingestion in the first reply | ✅ | No ingestion trigger; only describes paths. |
| 12 | Response mentions OpenMetadata and does NOT default to catalog-vocab-publisher or dbt lineage | ✅ | OpenMetadata-focused options; no vocab publisher or dbt default. |

## Category Grades

| # | Category | Grade | Explanation |
|---|----------|-------|-------------|
| 1 | Triggering (Description Quality) | PASS | Synonyms and OpenMetadata triggers in description. |
| 2 | Anatomy & Structure | PASS | Valid frontmatter; 320 lines; tests present. |
| 3 | Instructions Clarity | PASS | Vague-request and existing-service onboard rules. |
| 4 | Output Quality | PASS | Plan JSON and workflow steps explicit. |
| 5 | Testability | PASS | 12 evals + apply-script pytest. |
| 6 | Resource Efficiency | PASS | Apply/rollback scripts for deterministic writes. |
| 7 | Security & Trust | PASS | password_env; user-openmetadata: tool names. |
| 8 | Coexistence & Recall | PASS | Adjacent skills table with trigger precision. |
| 9 | Model Compatibility | PASS | Sonnet/Haiku documented. |
| 10 | Workflow & Feedback Loops | PASS | Plan-apply-rollback; confirm after assign. |
| 11 | Maintainability & Lifecycle | PASS | Registry and versioning documented. |
| 12 | Gotchas / Lessons Learned | PASS | Pipeline FQN, filter merge, glossary vs tags. |
| 13 | Anti-Pattern Audit | PASS | Forward slashes; explicit timeouts. |

## Version Comparison

_Not run — comparator skipped; prior snapshot differs only in documentation patches for carried evals._

## History (all reviews)

| Reviewed at (ISO UTC) | Calendar | Unit Tests | Assertions | Categories | Verdict | Notes |
|----------------------|----------|------------|------------|------------|---------|-------|
| 2026-05-29T07:15:33Z | 2026-05-29 | 10/12 | 45/48 | 10/13 | FAIL |  |
| 2026-05-29T07:31:25Z | 2026-05-29 | 12/12 | 48/48 | 13/13 | PASS | **latest** |

Full narrative: `review-catalog-sync-2026-05-29T073125Z.md`.
