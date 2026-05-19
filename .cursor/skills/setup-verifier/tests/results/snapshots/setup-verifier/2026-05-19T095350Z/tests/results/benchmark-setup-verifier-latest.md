# Benchmark Report: setup-verifier
_Generated: 2026-05-19T09:53:50Z UTC · Run 3 — incremental re-check of evals 1–3_

## Summary

| Metric | Value |
|--------|-------|
| Overall Verdict | FAIL |
| Unit Tests | 9 / 9 passed (100%) |
| Assertions | 26 / 26 (100%) |
| Categories | 12 / 13 passed |
| High Failures | 1 |
| Medium Failures | — |
| Comparator | new_wins (3/3 — Evals 1–3 now produce structured preview) |

## Unit Test Results

| # | Test | Type | Result | Assertions |
|---|------|------|--------|------------|
| 1 | check my setup | Should-trigger | ✅ PASS | 3/3 |
| 2 | verify my connections are working | Should-trigger | ✅ PASS | 3/3 |
| 3 | validate my environment before I start work | Should-trigger | ✅ PASS | 3/3 |
| 4 | is my MCP access set up correctly? | Should-trigger | ✅ PASS | 3/3 ↩ carried |
| 5 | extract the last dbt Cloud run logs | Should-not-trigger | ✅ PASS | 3/3 ↩ carried |
| 6 | run the source system analyser | Should-not-trigger | ✅ PASS | 3/3 ↩ carried |
| 7 | how do I install Cursor? | Should-not-trigger | ✅ PASS | 3/3 ↩ carried |
| 8 | just check my Snowflake connection | Edge case | ✅ PASS | 3/3 ↩ carried |
| 9 | my setup is done | Edge case | ✅ PASS | 2/2 ↩ carried |

## Assertion Detail

| Eval | Assertion | Passed | Evidence |
|------|-----------|--------|----------|
| 1 | Response references Snowflake, OpenMetadata, dbt, and Fivetran checks | ✅ Yes | All 4 in preview table rows |
| 1 | Response includes a table or structured list showing all four systems | ✅ Yes | 5-row table: Snowflake, OpenMetadata, dbt Cloud, Fivetran, .env variables |
| 1 | Response references .env variable validation | ✅ Yes | .env variables row in table; "check both dbtproject/.env and skills .env" |
| 2 | Response includes a results table with at least 4 rows | ✅ Yes | 5-row table (all systems + .env keys) |
| 2 | Response contains status indicators (✅, ❌, or —) or names each system | ✅ Yes | All rows have '—' status; Snowflake example fix shown |
| 2 | Response does NOT describe how to install or configure a new system | ✅ Yes | Only verification steps shown |
| 3 | Response includes at least one specific fix or remediation step | ✅ Yes | Per-system fix column: 401 → fix credentials, 401 → regenerate OM_TOKEN, 401 → uvx dbt-mcp auth, 401/403 → fix FIVETRAN keys |
| 3 | Response covers all four systems: Snowflake, OpenMetadata, dbt, Fivetran | ✅ Yes | All 4 + .env row in table |
| 3 | Response describes expected output structure including 'Setup complete' or a failure fix | ✅ Yes | "If all pass, I'll point you to §8 of ONBOARDING.md" + per-system failure fixes |
| 4 | Response mentions testing or querying specific MCP servers | ✅ Yes | ↩ carried from 2026-05-19T094549Z |
| 4 | Response outputs results in a tabular or structured format | ✅ Yes | ↩ carried from 2026-05-19T094549Z |
| 4 | Response does NOT just describe what MCP is without running checks | ✅ Yes | ↩ carried from 2026-05-19T094549Z |
| 5 | Response does NOT call list_database_services or list_connectors | ✅ Yes | ↩ carried from 2026-05-19T094549Z |
| 5 | Response focuses on dbt run history, model status, or log extraction | ✅ Yes | ↩ carried from 2026-05-19T094549Z |
| 5 | Response does NOT output a 4-system connection check table | ✅ Yes | ↩ carried from 2026-05-19T094549Z |
| 6 | Response does NOT run a 4-system connection check | ✅ Yes | ↩ carried from 2026-05-19T093403Z |
| 6 | Response mentions running or invoking the source system analyser | ✅ Yes | ↩ carried from 2026-05-19T093403Z |
| 6 | Response does NOT output the setup-verifier pass/fail table format | ✅ Yes | ↩ carried from 2026-05-19T093403Z |
| 7 | Response does NOT call any MCP tools | ✅ Yes | ↩ carried from 2026-05-19T093403Z |
| 7 | Response does NOT output a setup verification table | ✅ Yes | ↩ carried from 2026-05-19T093403Z |
| 7 | Response provides installation guidance or points to cursor.com | ✅ Yes | ↩ carried from 2026-05-19T093403Z |
| 8 | Response checks Snowflake at minimum | ✅ Yes | ↩ carried from 2026-05-19T094549Z |
| 8 | Response does NOT skip the other MCP systems without explanation | ✅ Yes | ↩ carried from 2026-05-19T094549Z |
| 8 | Response produces a structured result for at least the Snowflake connection | ✅ Yes | ↩ carried from 2026-05-19T094549Z |
| 9 | Response does NOT immediately run all four MCP checks | ✅ Yes | ↩ carried from 2026-05-19T094549Z |
| 9 | Response asks clarifying question or acknowledges without triggering | ✅ Yes | ↩ carried from 2026-05-19T094549Z |

## Category Grades

| # | Category | Grade | Explanation |
|---|----------|-------|-------------|
| 1 | Triggering | PASS | ↩ carried |
| 2 | Anatomy & Structure | PASS | ↩ carried |
| 3 | Instructions Clarity | PASS | ↩ carried |
| 4 | Output Quality | PASS | ↩ carried |
| 5 | Testability | PASS | ↩ carried — 9/9 unit tests pass |
| 6 | Resource Efficiency | PASS | ↩ carried |
| 7 | Security & Trust | PASS | ↩ carried |
| 8 | Coexistence & Recall | PASS | ↩ carried |
| 9 | Model Compatibility | PASS | ↩ carried |
| 10 | Workflow & Feedback Loops | PASS | ↩ carried |
| 11 | Maintainability & Lifecycle | FAIL | HIGH: no peer review on record — process gate only |
| 12 | Gotchas / Lessons Learned | PASS | ↩ carried |
| 13 | Anti-Pattern Audit | PASS | ↩ carried |

## Version Comparison

| Eval | Verdict | Reasoning |
|------|---------|-----------|
| 1 | new_wins | New produces structured 5-row preview table + concrete example fixes; prior only explained mode constraint |
| 2 | new_wins | Same — preview table with '—' status shown; prior had no table |
| 3 | new_wins | Per-system fix column in preview table; prior was generic mode-switch only |

Overall: **new_wins** (3/3)

## History (all reviews)

| Reviewed at (ISO UTC) | Calendar | Unit Tests | Assertions | Categories | Verdict | Notes |
|----------------------|----------|------------|------------|------------|---------|-------|
| 2026-05-19T09:34:03Z | 2026-05-19 | 5/9 (56%) | 21/26 (81%) | 9/13 | FAIL | First review |
| 2026-05-19T09:45:49Z | 2026-05-19 | 6/9 (67%) | 19/26 (73%) | 12/13 | FAIL | 3 categories fixed |
| **2026-05-19T09:53:50Z** | **2026-05-19** | **9/9 (100%)** | **26/26 (100%)** | **12/13** | **FAIL** | **9/9 unit tests pass — process gate only** |

Full narrative: `review-setup-verifier-2026-05-19T095350Z.md`.
