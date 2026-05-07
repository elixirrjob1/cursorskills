# Benchmark Report: skill-reviewer
_Generated: 2026-05-07 · live executor harness (third history entry)_

## Summary

| Metric | Value |
|--------|-------|
| Overall Verdict | FAIL |
| Unit Tests | 10 / 10 passed (100%) — live `generalPurpose` subagents, one prompt each |
| Assertions | 30 / 30 (100%) — re-graded from subagent transcripts |
| Categories | 9 / 13 passed |
| High Failures | 1 |
| Medium Failures | 5 (coexistence, model compatibility, registry/version/lifecycle x3, gotchas — rolled up) |
| Comparator | — (not run; no snapshot / `SKILL.md` unchanged vs prior review) |

## Unit Test Results

| # | Test | Type | Result | Assertions |
|---|------|------|--------|------------|
| 1 | meetingintroskill full rubric | should-trigger | ✅ PASS | 3/3 |
| 2 | incremental re-check | should-trigger | ✅ PASS | 3/3 |
| 3 | HIGH findings only | should-trigger | ✅ PASS | 3/3 |
| 4 | save tests/results + history | should-trigger | ✅ PASS | 3/3 |
| 5 | blind comparison docs | should-trigger | ✅ PASS | 3/3 |
| 6 | STM → dbt | should-not-trigger | ✅ PASS | 3/3 |
| 7 | Key Vault bootstrap | should-not-trigger | ✅ PASS | 3/3 |
| 8 | Paris trivia | should-not-trigger | ✅ PASS | 3/3 |
| 9 | "Run the skill reviewer" | edge-case | ✅ PASS | 3/3 |
| 10 | grader contract | edge-case | ✅ PASS | 3/3 |

## Assertion Detail

Each eval’s machine-graded result is in `runs/2026-05-07/grading-<eval_id>.json` (`passed` / `evidence` per assertion, `overall_result`).

| Eval | Summary | Overall |
|------|---------|---------|
| 1 | meetingintroskill full rubric | PASS |
| 2 | incremental prior review | PASS |
| 3 | HIGH-only audit | PASS |
| 4 | save artifacts / history | PASS |
| 5 | Step 3c / comparator | PASS |
| 6 | STM→dbt routing | PASS |
| 7 | Key Vault routing | PASS |
| 8 | trivia | PASS |
| 9 | missing target | PASS |
| 10 | grader contract path | PASS |

## Category Grades

| # | Category | Grade |
|---|----------|-------|
| 1 | Triggering | PASS |
| 2 | Anatomy & Structure | PASS |
| 3 | Instructions Clarity | PASS |
| 4 | Output Quality | PASS |
| 5 | Testability | PASS |
| 6 | Resource Efficiency | PASS |
| 7 | Security & Trust | PASS |
| 8 | Coexistence & Recall | FAIL |
| 9 | Model Compatibility | FAIL |
| 10 | Workflow & Feedback Loops | PASS |
| 11 | Maintainability & Lifecycle | FAIL |
| 12 | Gotchas / Lessons Learned | FAIL |
| 13 | Anti-Pattern Audit | PASS |

## Version Comparison (if comparator was run)

_Not run — no Step 3c snapshot in this session._

## History (all reviews)

| Date | Unit Tests | Assertions | Categories | Verdict | Comparison |
|------|------------|------------|------------|---------|------------|
| 2026-05-07 | 10/10 | 30/30 | 9/13 | FAIL | — (first; simulated executor text) |
| 2026-05-07 | 10/10 | 30/30 | 9/13 | FAIL | — (incremental; evals SKIP) |
| 2026-05-07 | 10/10 | 30/30 | 9/13 | FAIL | — (live subagents) |

Full narrative: `review-skill-reviewer-2026-05-07.md`.
