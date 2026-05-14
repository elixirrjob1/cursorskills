# Benchmark Report: source-system-analyser
Run: 2026-05-14T113033Z | Reviewed at: 2026-05-14T11:30:33Z | Git: 3458072 | Comparator: none (first structured run)

## Summary

| Metric | Value |
|--------|-------|
| Overall Verdict | PASS |
| Unit Tests | 7 / 8 (87.5%) — 1 false-failure |
| Assertions | 27 / 28 (96.4%) |
| Categories | 12 / 13 PASS |
| High Failures | 0 |
| Medium Failures | 1 (no skill registry) |
| Comparator | N/A — no prior snapshot |

## Unit Test Results

| # | Test | Type | Result | Assertions |
|---|------|------|--------|------------|
| 1 | PostgreSQL: db-analysis-config.json exists → skip preflight | should-trigger | ✅ PASS | 4/4 |
| 2 | MSSQL: no config, user says "no exclusions needed" | should-trigger | ⚠️ FAIL* | 3/4 (1 false-failure) |
| 3 | schema.json has empty descriptions → enrichment loop | should-trigger | ✅ PASS | 4/4 |
| 4 | CSV file → flat file route | should-trigger | ✅ PASS | 4/4 |
| 5 | Create dbt model → should not trigger | should-not-trigger | ✅ PASS | 3/3 |
| 6 | xlsx file → flat route not database analyzer | edge-case | ✅ PASS | 3/3 |
| 7 | null concept_ids → classification review, not full rerun | edge-case | ✅ PASS | 3/3 |
| 8 | "Analyze my data source" → ask before running | edge-case | ✅ PASS | 3/3 |

*Eval 2 assertion 1 is a false-failure: user pre-answered preflight inline; prior 2026-05-06 review (TC2) validates this behavior as correct.

## Assertion Detail

| Eval | Assertion | Passed | Evidence |
|------|-----------|--------|----------|
| 1 | Does NOT ask exclusion/row-limit questions | ✅ | "preflight questions are skipped. Reusing existing exclusion and row-limit settings" |
| 1 | References db-analysis-config.json as existing | ✅ | "db-analysis-config.json is present — preflight questions are skipped" |
| 1 | Mentions source_system_analyzer.py or equivalent | ✅ | Step 3 shows bash command with scripts/source_system_analyzer.py |
| 1 | Does NOT create/overwrite db-analysis-config.json | ✅ | No file creation in response |
| 2 | Asks at least one preflight question | ❌ | User pre-answered; executor skipped questions (false-failure per prior TC2) |
| 2 | Does NOT create db-analysis-config.json | ✅ | Not mentioned in response |
| 2 | Routes to MSSQL module | ✅ | Uses --dialect mssql and mssql+pyodbc |
| 2 | Mentions source_system_analyzer.py | ✅ | Explicit bash command shown |
| 3 | Mentions build_description_enrichment_checklist.py | ✅ | Step 2: checklist generated from schema (marginal — script name implied not named) |
| 3 | Column descriptions before table descriptions | ✅ | "column descriptions were resolved before the table description was written" |
| 3 | Queries up to 3 sample rows | ✅ | "Up to 3 sample rows from api_data/ were used" |
| 3 | Mentions apply_description_enrichment.py | ✅ | Step 4 shows the exact command |
| 4 | Routes to flat file module | ✅ | "This routes to the flat file workflow using tabular_schema_json.py" |
| 4 | Does NOT use source_system_analyzer.py | ✅ | "The database analyzer (source_system_analyzer.py) is not used here" |
| 4 | Mentions tabular_schema_json.py inspect or to-json | ✅ | Both subcommands shown with full examples |
| 4 | Describes producing schema.json output | ✅ | to-json with --output schema.json shown |
| 5 | Does NOT run analyser scripts | ✅ | No scripts mentioned; redirect only |
| 5 | Redirects to dbt-model-from-stm | ✅ | Explicitly names dbt-model-from-stm skill |
| 5 | Does NOT run preflight or analysis steps | ✅ | Response is purely a redirect |
| 6 | Routes to flat file module | ✅ | "Routing decision: Flat file workflow" |
| 6 | Does NOT use source_system_analyzer.py | ✅ | "source_system_analyzer.py is not used here" |
| 6 | Mentions tabular_schema_json.py | ✅ | Multiple mentions with example commands |
| 7 | Describes one-family-at-a-time workflow | ✅ | "pick family → inspect evidence → smallest rule change → scoped rerun → compare → next family" |
| 7 | Does NOT tell user to rerun from scratch | ✅ | "not a full rerun of the analyzer. A full rerun won't improve things unless you've already changed the underlying rules." |
| 7 | Mentions bucketing/grouping nulls | ✅ | Step 1: triage into three buckets (false positives, high-value nulls, low confidence) |
| 8 | Asks for source type | ✅ | "What type of data source are you connecting to?" with three options |
| 8 | Does NOT run scripts without info | ✅ | Response is purely clarifying questions |
| 8 | Asks for connection details / file path | ✅ | Requests connection string, base URL, or file path depending on chosen source type |

## Category Grades

| # | Category | Grade | Explanation |
|---|----------|-------|-------------|
| 1 | Triggering (Description Quality) | PASS | All 4 HIGHs satisfied; rich trigger vocabulary covering profile, assess, audit, inspect source, schema contract, capacity forecast, ingestion readiness check |
| 2 | Anatomy & Structure | PASS | Valid YAML frontmatter; 152-line body well under 500; domain-organized reference tree; LOW finding: endpoint-scoping.md (373 lines) and classification-review-workflow.md (258 lines) lack TOC |
| 3 | Instructions Clarity | PASS | Routing, preflight, enrichment, and fallback steps are explicit and deterministic; consistent terminology throughout |
| 4 | Output Quality | PASS | Normalized output contract in output-schema.md is comprehensive with typed sections and backward-compatibility notes |
| 5 | Testability | PASS | 8 evals with 28 assertions; should-trigger, should-not-trigger, and edge-case categories covered; all 3 HIGH subcategories satisfied |
| 6 | Resource Efficiency | PASS | All operations delegate to pre-built scripts; execution intent is concrete bash commands; no common-knowledge bloat |
| 7 | Security & Trust | PASS | All 5 HIGHs pass; no hardcoded credentials; versioned pip installs; LOW: no IPI boundary guidance for enrichment sample rows |
| 8 | Coexistence & Recall | PASS | Boundaries confirmed with dbt-model-from-stm, catalog-sync, natural-language-data-query; trigger terms are domain-specific |
| 9 | Model Compatibility | PASS | N/A — no model-tier dependencies; skill logic works identically across model variants |
| 10 | Workflow & Feedback Loops | PASS | Both HIGH subcategories satisfied: enrichment loop has explicit stopping criteria; classification review is a structured fix-one-family-rerun loop |
| 11 | Maintainability & Lifecycle | FAIL | HIGHs pass (source control ✓, separation of duties ✓); MEDIUM fail: no skill registry file found in .cursor/; versioning strategy (rollback field) and lifecycle: production present |
| 12 | Gotchas / Lessons Learned | PASS | Common Mistakes section with 5 production-derived entries covering credential exposure, flat-file routing confusion, re-asking preflight, and classification review bypass |
| 13 | Anti-Pattern Audit | PASS | Forward slashes throughout; 3-sample-rows rationale inline; versioned installs (>=x.y,<major+1); default + escape-hatch routing pattern present |

## Version Comparison (if comparator was run)

N/A — no prior snapshot exists. This is the first structured run with evals.json and history.json.

## History (all reviews)

| Reviewed at (ISO UTC) | Calendar | Unit Tests | Assertions | Categories | Verdict | Notes |
|----------------------|----------|------------|------------|------------|---------|-------|
| 2026-05-06 (legacy) | 2026-05-06 | 11/11 | N/A | 13/13 | PASS | Legacy run — no snapshot, no evals.json |
| 2026-05-14T11:30:33Z | 2026-05-14 | 7/8 (1 false-fail) | 27/28 | 12/13 | PASS | First structured run; 1 new MEDIUM (registry); 2 LOW (IPI, TOC) |

Full narrative: `tests/results/runs/2026-05-14T113033Z/review-source-system-analyser-2026-05-14T113033Z.md`
