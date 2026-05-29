# Benchmark Report: catalog-sync
_Generated: 2026-05-29T07:15:33Z UTC · First full review (no prior snapshot)_

## Summary

| Metric | Value |
|--------|-------|
| Overall Verdict | FAIL |
| Unit Tests | 10 / 12 passed (83%) |
| Assertions | 45 / 48 (94%) |
| Categories | 10 / 13 passed |
| High Failures | — |
| Medium Failures | 6 (registry, lifecycle, MCP naming, gotchas, models) |
| Comparator | — (first review; no prior snapshot) |

## Unit Test Results

| # | Test | Type | Result | Assertions |
|---|------|------|--------|------------|
| 1 | Postgres sync + PII tag | should-trigger | ✅ PASS | 4/4 |
| 2 | Snowflake sync + inspect | should-trigger | ✅ PASS | 4/4 |
| 3 | MySQL onboard | should-trigger | ✅ PASS | 4/4 |
| 4 | Postgres re-run pipeline | should-trigger | ✅ PASS | 4/4 |
| 5 | Glossary on orders | should-trigger | ✅ PASS | 4/4 |
| 6 | Oracle new source | should-trigger | ✅ PASS | 4/4 |
| 7 | Vocab publish routing | should-not-trigger | ✅ PASS | 4/4 |
| 8 | AI glossary routing | should-not-trigger | ✅ PASS | 4/4 |
| 9 | dbt lineage routing | should-not-trigger | ✅ PASS | 4/4 |
| 10 | password_env edge | edge-case | ✅ PASS | 4/4 |
| 11 | duplicate service edge | edge-case | ❌ FAIL | 3/4 |
| 12 | vague fix catalog | edge-case | ❌ FAIL | 2/4 |

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
| 11 | Response states service already exists and refuses create_database_service | ❌ | Response says mysql_retail_erp is NOT in OpenMetadata (404), not that it already exists. |
| 11 | Response references onboard guardrail or apply script refusal when service exists | ✅ | Documents apply script and onboard guardrails when name is taken. |
| 11 | Response does NOT proceed with create_database_service or apply script | ✅ | Does not run create or apply. |
| 11 | Response suggests using standard workflow to update/re-run instead of onboard create | ✅ | Points to standard workflow with run_ingestion_pipeline. |
| 12 | Response asks clarifying question about desired action OR lists sync/onboard/tag options | ❌ | Immediately triggered ingestion and pushed descriptions without first asking what to fix. |
| 12 | Response mentions OpenMetadata as the supported catalog platform | ✅ | References OpenMetadata URL and services. |
| 12 | Response does NOT immediately run ingestion without clarification | ❌ | Ran run_ingestion_pipeline on both pipelines before clarifying user intent. |
| 12 | Response does NOT route to unrelated skills like dbt or vocab publisher as default | ✅ | Does not default to vocab publisher or dbt lineage skills. |

Grader JSON: `tests/results/runs/2026-05-29T071533Z/grading/grading-<n>.json`

## Category Grades

| # | Category | Grade | Explanation |
|---|----------|-------|-------------|
| 1 | Triggering (Description Quality) | PASS | Description states catalog sync, tagging, OpenMetadata triggers. |
| 2 | Anatomy & Structure | PASS | 252-line SKILL.md; valid YAML; tests/ present. |
| 3 | Instructions Clarity | PASS | Onboard vs standard workflow clearly separated. |
| 4 | Output Quality | PASS | Plan JSON schema and tagging rules templated. |
| 5 | Testability | PASS | Eval suite + apply-script pytest fixtures. |
| 6 | Resource Efficiency | PASS | Deterministic apply/rollback scripts invoked explicitly. |
| 7 | Security & Trust | PASS | No secrets in chat; script guardrails audited. |
| 8 | Coexistence & Recall | PASS | Peer skills document boundaries; routing evals pass. |
| 9 | Model Compatibility | FAIL | No model tier documentation in SKILL.md. |
| 10 | Workflow & Feedback Loops | PASS | Plan-apply-rollback and post-assign verification. |
| 11 | Maintainability & Lifecycle | FAIL | Missing Registry, lifecycle, versioning in SKILL.md. |
| 12 | Gotchas / Lessons Learned | FAIL | No Gotchas section despite rich guardrails. |
| 13 | Anti-Pattern Audit | PASS | Unix paths; documented timeouts; explicit installs. |

## Version Comparison (if comparator was run)

_Not run — first review; no prior snapshot._

## History (all reviews)

| Reviewed at (ISO UTC) | Calendar | Unit Tests | Assertions | Categories | Verdict | Notes |
|----------------------|----------|------------|------------|------------|---------|-------|
| 2026-05-29T07:15:33Z | 2026-05-29 | 10/12 | 45/48 | 10/13 | FAIL | First review |

Full narrative: `review-catalog-sync-2026-05-29T071533Z.md`.
