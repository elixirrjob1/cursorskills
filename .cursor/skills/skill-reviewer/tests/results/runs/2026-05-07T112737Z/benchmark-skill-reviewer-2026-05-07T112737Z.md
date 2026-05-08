# Benchmark Report: skill-reviewer
_Generated: 2026-05-07T11:27:37Z · post-fix; first 13/13 PASS_

## Summary

| Metric | Value |
|--------|-------|
| Overall Verdict | **PASS** |
| Unit Tests | 10 / 10 passed (100%) |
| Assertions | 30 / 30 (100%) |
| Categories | **13 / 13 passed** |
| High Failures | — |
| Medium Failures | — |
| Comparator | — (evals not re-run this round) |

## Unit Test Results

| # | Test | Type | Result | Assertions | Evidence |
|---|------|------|--------|------------|----------|
| 1 | Review `.cursor/skills/meetingintroskill/` for production readiness — full rubric. | should-trigger | ✅ PASS | 3/3 |  |
| 2 | We have a prior review in tests/results/review-meetingintroskill-2026-05-07.md — only re-check what failed last time. | should-trigger | ✅ PASS | 3/3 |  |
| 3 | Grade this SKILL.md against Anthropic skill best practices and list HIGH findings only. | should-trigger | ✅ PASS | 3/3 |  |
| 4 | After you review the skill, save the report to tests/results/ and append history.json. | should-trigger | ✅ PASS | 3/3 |  |
| 5 | Does our reviewer workflow document blind comparison between skill versions? | should-trigger | ✅ PASS | 3/3 |  |
| 6 | Convert this STM markdown into dbt SQL models for Snowflake. | should-not-trigger | ✅ PASS | 3/3 |  |
| 7 | Bootstrap a new Azure Key Vault and upload secrets from `.env`. | should-not-trigger | ✅ PASS | 3/3 |  |
| 8 | What is the capital of France? | should-not-trigger | ✅ PASS | 3/3 |  |
| 9 | Run the skill reviewer. | edge-case | ✅ PASS | 3/3 |  |
| 10 | Use the grader agent — what file defines its contract? | edge-case | ✅ PASS | 3/3 |  |

## Assertion Detail

| Eval | Assertion | Passed | Evidence |
|------|-----------|--------|----------|


## Category Grades

| # | Category | Grade | Explanation |
|---|----------|-------|-------------|
| 1 | Triggering (Description Quality) | ✅ PASS | Description lists what + when; triggers rich ↩ carried |
| 2 | Anatomy & Structure | ✅ PASS | Frontmatter, references, body length ↩ carried |
| 3 | Instructions Clarity | ✅ PASS | Steps 1–7, Step 3c, 7d; `carried_from` rule in Step 3b ↩ carried |
| 4 | Output Quality | ✅ PASS | Output format template strict ↩ carried |
| 5 | Testability | ✅ PASS | Isolation, assertions, eval suite ↩ carried |
| 6 | Resource Efficiency | ✅ PASS | Templates extracted; lean workflow ↩ carried |
| 7 | Security & Trust | ✅ PASS | Markdown-first; W007/W011 pointers ↩ carried |
| 8 | Coexistence & Recall | ✅ PASS | `## Deployment / Coexistence Notes` section present ↩ carried |
| 9 | Model Compatibility | ✅ PASS | `## Model Compatibility` section present ↩ carried |
| 10 | Workflow & Feedback Loops | ✅ PASS | Incremental loop; `carried_from` prevents duplication ↩ carried |
| 11 | Maintainability & Lifecycle | ✅ PASS | `## Registry` with separation of duties ↩ carried |
| 12 | Gotchas / Lessons Learned | ✅ PASS | `## Gotchas` covers `carried_from`, snapshot recursion ↩ carried |
| 13 | Anti-Pattern Audit | ✅ PASS | Forward-slash paths; no bundled scripts ↩ carried |

## Version Comparison

_`RUN_STEP_3C=true` — SKILL.md changed vs prior snapshot. Comparator not executed._

## History (all reviews)

| Reviewed at (ISO UTC) | Calendar | Unit Tests | Assertions | Categories | Verdict | Notes |
|----------------------|----------|------------|------------|------------|---------|-------|
| 2026-05-07T10:00:00Z | 2026-05-07 | 10/10 | 30/30 | 9/13 | FAIL | First review. Legacy date-only run dir. |
| 2026-05-07T11:00:00Z | 2026-05-07 | 10/10 | 30/30 | 9/13 | FAIL | Incremental re-check; all evals SKIP (carried PASS); no comparator snapshot. |
| 2026-05-07T12:00:00Z | 2026-05-07 | 10/10 | 30/30 | 9/13 | FAIL | Live harness: 10 parallel executor subagents; 30/30 assertions. |
| 2026-05-07T11:21:20Z | 2026-05-07 | 10/10 | 30/30 | 9/13 | FAIL | Incremental; all evals SKIP; first 7d snapshot. |
| 2026-05-07T11:27:37Z | 2026-05-07 | 10/10 | 30/30 | 13/13 | PASS | Post-fix: Registry, Gotchas, Model Compat, Coexistence added. 13/13 PASS. |
| 2026-05-07T11:40:58Z | 2026-05-07 | 10/10 | 30/30 | 13/13 | PASS | carried_from pointers in timing.json; no grading JSON duplicated for SKIP evals. RUN_STEP_3C=true; comparator skipped (all SKIP). |

Full narrative: `runs/2026-05-07T112737Z/review-skill-reviewer-2026-05-07T112737Z.md`.
