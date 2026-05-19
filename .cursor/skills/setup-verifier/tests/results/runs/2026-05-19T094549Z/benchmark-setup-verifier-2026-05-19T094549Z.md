# Benchmark Report: setup-verifier
_Generated: 2026-05-19T09:45:49Z UTC · Incremental re-check after Run 1 fixes_

## Summary

| Metric | Value |
|--------|-------|
| Overall Verdict | FAIL |
| Unit Tests | 6 / 9 passed (67%) |
| Assertions | 19 / 26 (73%) |
| Categories | 12 / 13 passed |
| High Failures | 1 |
| Medium Failures | — |
| Comparator | new_wins (Eval 9 — ambiguous trigger fixed) |

## Unit Test Results

| # | Test | Type | Result | Assertions |
|---|------|------|--------|------------|
| 1 | check my setup | Should-trigger | ❌ FAIL | 1/3 |
| 2 | verify my connections are working | Should-trigger | ❌ FAIL | 1/3 |
| 3 | validate my environment before I start work | Should-trigger | ❌ FAIL | 0/3 |
| 4 | is my MCP access set up correctly? | Should-trigger | ✅ PASS | 3/3 |
| 5 | extract the last dbt Cloud run logs | Should-not-trigger | ✅ PASS | 3/3 |
| 6 | run the source system analyser | Should-not-trigger | ✅ PASS | 3/3 ↩ carried |
| 7 | how do I install Cursor? | Should-not-trigger | ✅ PASS | 3/3 ↩ carried |
| 8 | just check my Snowflake connection | Edge case | ✅ PASS | 3/3 |
| 9 | my setup is done | Edge case | ✅ PASS | 2/2 **FIXED** |

## Assertion Detail

| Eval | Assertion | Passed | Evidence |
|------|-----------|--------|----------|
| 1 | Response references Snowflake, OpenMetadata, dbt, and Fivetran checks | ✅ Yes | All 4 listed by name |
| 1 | Response includes a table with pass/fail (✅ or ❌) per system | ❌ No | No table — Ask mode blocked execution |
| 1 | Response references .env variable validation | ❌ No | .env not mentioned in this response |
| 2 | Response includes a results table with at least 4 rows | ❌ No | No table rendered — Ask mode |
| 2 | Response contains ✅ or ❌ symbols indicating per-system status | ❌ No | No symbols — steps listed descriptively |
| 2 | Response does NOT describe how to install or configure a new system | ✅ Yes | Focus on running checks only |
| 3 | Response includes at least one specific fix or remediation step | ❌ No | Generic "specific fixes for any failures" without showing any |
| 3 | Response covers all four systems: Snowflake, OpenMetadata, dbt, Fivetran | ❌ No | Said "all four MCP checks" without naming each |
| 3 | Response ends with 'Setup complete' or a list of failures | ❌ No | Ended with mode-switch instruction |
| 4 | Response mentions testing or querying specific MCP servers | ✅ Yes | Listed all 4 by qualified name (ServerName:tool format) |
| 4 | Response outputs results in a tabular or structured format | ✅ Yes | Step / Check / Tool table with example fail row |
| 4 | Response does NOT just describe what MCP is without running checks | ✅ Yes | Went straight to tool invocations |
| 5 | Response does NOT call list_database_services or list_connectors | ✅ Yes | Routed to dbt-cloud-log-extractor |
| 5 | Response focuses on dbt run history, model status, or log extraction | ✅ Yes | "query dbt Cloud MCP for most recent run...model-level pass/fail..." |
| 5 | Response does NOT output a 4-system connection check table | ✅ Yes | No setup-verifier table shown |
| 6 | Response does NOT run a 4-system connection check | ✅ Yes | ↩ carried from 2026-05-19T093403Z |
| 6 | Response mentions running or invoking the source system analyser | ✅ Yes | ↩ carried from 2026-05-19T093403Z |
| 6 | Response does NOT output the setup-verifier pass/fail table format | ✅ Yes | ↩ carried from 2026-05-19T093403Z |
| 7 | Response does NOT call any MCP tools | ✅ Yes | ↩ carried from 2026-05-19T093403Z |
| 7 | Response does NOT output a setup verification table | ✅ Yes | ↩ carried from 2026-05-19T093403Z |
| 7 | Response provides installation guidance or points to cursor.com | ✅ Yes | ↩ carried from 2026-05-19T093403Z |
| 8 | Response checks Snowflake at minimum | ✅ Yes | Described Snowflake MCP call; offered result table |
| 8 | Response does NOT skip the other MCP systems without explanation | ✅ Yes | "(and optionally the other MCPs)" — all systems covered |
| 8 | Response produces a structured result for at least the Snowflake connection | ✅ Yes | Pass/fail table offered on mode switch |
| 9 | Response does NOT immediately run all four MCP checks | ✅ Yes | Asked for confirmation ("Would you like me to run a verification check?") |
| 9 | Response asks clarifying question or acknowledges without triggering | ✅ Yes | "Got it — setup is done, great! Would you like me to run a verification check?" |

## Category Grades

| # | Category | Grade | Explanation |
|---|----------|-------|-------------|
| 1 | Triggering (Description Quality) | PASS | ↩ carried — WHAT+WHEN, 5 trigger terms, name/description within limits |
| 2 | Anatomy & Structure | PASS | ↩ carried — valid frontmatter, 124 lines, no deep chains |
| 3 | Instructions Clarity | PASS | ↩ carried — low-freedom MCP calls, consistent terminology, If-fails branches |
| 4 | Output Quality | PASS | ↩ carried — exact table template with columns, icons, messages |
| 5 | Testability | PASS | ↩ carried — all HIGH subcategories pass; eval suite in place |
| 6 | Resource Efficiency | PASS | ↩ carried — no padding, MCP calls explicit, execution intent clear |
| 7 | Security & Trust | PASS | **Fixed** — all 4 MCP calls now qualified (ServerName:tool_name); 2/2 MEDIUMs pass |
| 8 | Coexistence & Recall | PASS | ↩ carried — Evals 5-7 confirm clean routing |
| 9 | Model Compatibility | PASS | **Fixed** — Registry documents "Validated on Claude 3.5 Sonnet / Cursor Agent mode" |
| 10 | Workflow & Feedback Loops | PASS | ↩ carried — N/A (read-only verification skill) |
| 11 | Maintainability & Lifecycle | FAIL | HIGH: no actual second reviewer yet; Registry/versioning/lifecycle now all PASS (3/3 MEDIUMs) |
| 12 | Gotchas / Lessons Learned | PASS | **Fixed** — Gotchas section documents Ask mode limitation and ambiguous-trigger rule |
| 13 | Anti-Pattern Audit | PASS | ↩ carried — forward slashes, no magic numbers, no scripts |

## Version Comparison (comparator run)

| Eval | Prompt | Verdict | Reasoning |
|------|--------|---------|-----------|
| 9 | my setup is done | **new_wins** | New skill asks "Would you like me to run a check?" — old skill presumed intent and said "I'd love to verify it now" |

Overall comparator: **new_wins** (1 new_wins / 0 old_wins / 0 ties across 1 compared eval).

## History (all reviews)

| Reviewed at (ISO UTC) | Calendar | Unit Tests | Assertions | Categories | Verdict | Notes |
|----------------------|----------|------------|------------|------------|---------|-------|
| 2026-05-19T09:34:03Z | 2026-05-19 | 5/9 (56%) | 21/26 (81%) | 9/13 | FAIL | First review |
| **2026-05-19T09:45:49Z** | **2026-05-19** | **6/9 (67%)** | **19/26 (73%)** | **12/13** | **FAIL** | **Incremental re-check; 3 categories fixed** |

Full narrative: `review-setup-verifier-2026-05-19T094549Z.md`.
