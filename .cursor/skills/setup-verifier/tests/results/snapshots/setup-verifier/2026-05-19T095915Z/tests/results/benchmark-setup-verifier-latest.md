# Benchmark Report: setup-verifier
_Generated: 2026-05-19T09:59:15Z UTC · Run 4 — incremental (secondary repo created)_

## Summary

| Metric | Value |
|--------|-------|
| Overall Verdict | FAIL |
| Unit Tests | 9 / 9 passed (100%) |
| Assertions | 26 / 26 (100%) |
| Categories | 12 / 13 passed |
| High Failures | 1 |
| Medium Failures | — |
| Comparator | — (no skill-definition change vs prior snapshot) |

## Unit Test Results

| # | Test | Type | Result | Assertions |
|---|------|------|--------|------------|
| 1 | check my setup | Should-trigger | ✅ PASS | 3/3 ↩ carried |
| 2 | verify my connections are working | Should-trigger | ✅ PASS | 3/3 ↩ carried |
| 3 | validate my environment before I start work | Should-trigger | ✅ PASS | 3/3 ↩ carried |
| 4 | is my MCP access set up correctly? | Should-trigger | ✅ PASS | 3/3 ↩ carried |
| 5 | extract the last dbt Cloud run logs | Should-not-trigger | ✅ PASS | 3/3 ↩ carried |
| 6 | run the source system analyser | Should-not-trigger | ✅ PASS | 3/3 ↩ carried |
| 7 | how do I install Cursor? | Should-not-trigger | ✅ PASS | 3/3 ↩ carried |
| 8 | just check my Snowflake connection | Edge case | ✅ PASS | 3/3 ↩ carried |
| 9 | my setup is done | Edge case | ✅ PASS | 2/2 ↩ carried |

## Assertion Detail

All 26 assertions carried from run 3 (2026-05-19T095350Z). See `runs/2026-05-19T095350Z/benchmark-setup-verifier-2026-05-19T095350Z.md` for full assertion detail. No skill-definition change; no new assertions to grade.

## Category Grades

| # | Category | Grade | Explanation |
|---|----------|-------|-------------|
| 1 | Triggering | PASS | ↩ carried |
| 2 | Anatomy & Structure | PASS | ↩ carried |
| 3 | Instructions Clarity | PASS | ↩ carried |
| 4 | Output Quality | PASS | ↩ carried |
| 5 | Testability | PASS | ↩ carried |
| 6 | Resource Efficiency | PASS | ↩ carried |
| 7 | Security & Trust | PASS | ↩ carried |
| 8 | Coexistence & Recall | PASS | ↩ carried |
| 9 | Model Compatibility | PASS | ↩ carried |
| 10 | Workflow & Feedback Loops | PASS | ↩ carried |
| 11 | Maintainability & Lifecycle | FAIL | HIGH: secondary repo now exists but no PR peer review on record yet |
| 12 | Gotchas / Lessons Learned | PASS | ↩ carried |
| 13 | Anti-Pattern Audit | PASS | ↩ carried |

## Version Comparison

_Not run — no skill-definition change vs prior snapshot (2026-05-19T095350Z)._

## History (all reviews)

| Reviewed at (ISO UTC) | Calendar | Unit Tests | Assertions | Categories | Verdict | Notes |
|----------------------|----------|------------|------------|------------|---------|-------|
| 2026-05-19T09:34:03Z | 2026-05-19 | 5/9 (56%) | 21/26 (81%) | 9/13 | FAIL | First review |
| 2026-05-19T09:45:49Z | 2026-05-19 | 6/9 (67%) | 19/26 (73%) | 12/13 | FAIL | 3 categories fixed |
| 2026-05-19T09:53:50Z | 2026-05-19 | 9/9 (100%) | 26/26 (100%) | 12/13 | FAIL | 9/9 tests pass |
| **2026-05-19T09:59:15Z** | **2026-05-19** | **9/9 (100%)** | **26/26 (100%)** | **12/13** | **FAIL** | **Secondary repo created; process gate only** |

Full narrative: `review-setup-verifier-2026-05-19T095915Z.md`.
