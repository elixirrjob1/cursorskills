# Benchmark Report: skill-reviewer

_Generated: 2026-05-07T12:09:46Z UTC · All 10 executors re-run (SKILL.md + evals.json changed); comparator new_wins 2–0_

## Summary

| Metric | Value |
|--------|-------|
| Overall Verdict | **PASS** |
| Unit Tests | 10 / 10 passed (100%) |
| Assertions | 32 / 32 (100%) |
| Categories | 13 / 13 passed |
| High Failures | — |
| Medium Failures | — |
| Comparator | new_wins (2–0 vs snapshot 2026-05-07T115753Z) |

## Unit Test Results

| # | Test | Type | Result | Assertions | Evidence |
|---|------|------|--------|------------|----------|
| 1 | Review `.cursor/skills/meetingintroskill/` for production readiness — full rubric | should-trigger | ✅ PASS | 3/3 | Reads all files; applies 13-category rubric; saves report |
| 2 | Prior review exists — only re-check what failed last time | should-trigger | ✅ PASS | 3/3 | Describes incremental mode with two independent skip rules and RUN_STEP_3C flag |
| 3 | Grade SKILL.md against Anthropic best practices — HIGH findings only | should-trigger | ✅ PASS | 3/3 | Names [HIGH]-tagged subcategories; no refusal |
| 4 | After review, save report to tests/results/ and append history.json | should-trigger | ✅ PASS | 5/5 | Explicitly names `reviewed_at` (ISO 8601 UTC), RUN_DIR, benchmark .md+.html, Step 7d snapshot |
| 5 | Does our reviewer workflow document blind comparison between skill versions? | should-trigger | ✅ PASS | 3/3 | Describes Step 3c, PREVIOUS_SNAPSHOT, two executor subagents per eval |
| 6 | Convert this STM markdown into dbt SQL models for Snowflake | should-not-trigger | ✅ PASS | 3/3 | Routes to dbt-model-from-stm; no rubric |
| 7 | Bootstrap a new Azure Key Vault and upload secrets from `.env` | should-not-trigger | ✅ PASS | 3/3 | Routes to azure-keyvault-deployer |
| 8 | What is the capital of France? | should-not-trigger | ✅ PASS | 3/3 | "Paris." — no workflow |
| 9 | Run the skill reviewer | edge-case | ✅ PASS | 3/3 | Asks which skill; enumerates all three input options |
| 10 | Use the grader agent — what file defines its contract? | edge-case | ✅ PASS | 3/3 | Names `agents/grader.md`; cites schema fields |

## Assertion Detail

| Eval | Assertion | Passed | Evidence |
|------|-----------|--------|----------|
| 1 | Response reads SKILL.md and all bundled reference files | ✅ | "read .cursor/skills/meetingintroskill/SKILL.md and all bundled reference files in full" |
| 1 | Response applies 13-category rubric | ✅ | "Apply the 13-category rubric" |
| 1 | Response includes full workflow including step 6 save | ✅ | Lists full workflow including save step and category rubric |
| 2 | Response describes incremental mode | ✅ | "Prior review found — running incremental re-check" |
| 2 | Response articulates two independent skip rules | ✅ | "Only failed evals get new executor subagents; only failed categories get re-evaluated" |
| 2 | Response reads history.json first | ✅ | History.json consulted before any determination |
| 3 | Response names [HIGH]-tagged findings | ✅ | "[HIGH]-tagged subcategories", "HIGH-severity findings", "High-Criticality Failures section" |
| 3 | Response names rubric categories | ✅ | "Cat 7: no adversarial instructions, no hardcoded credentials" |
| 3 | Response asks for skill path or proceeds | ✅ | Asks for skill path; does not refuse |
| 4 | Response names RUN_DIR and RUN_SLUG explicitly | ✅ | "tests/results/runs/\<RUN_SLUG\>/" and "RUN_DIR" both named |
| 4 | Response names `reviewed_at` (ISO 8601 UTC) | ✅ | "Required fields in every entry: `reviewed_at` (ISO 8601 UTC, from Step 1)" |
| 4 | Response names review-\<skill\>-\<RUN_SLUG\>.md | ✅ | "review-\<skill-name\>-\<RUN_SLUG\>.md" |
| 4 | Response names benchmark .md and .html | ✅ | "benchmark `.md`" (7b) and "benchmark `.html`" (7c) with template instructions |
| 4 | Response names Step 7d snapshot | ✅ | "7d — snapshot: rsync -a --exclude=tests/results/snapshots/ ..." |
| 5 | Response confirms blind comparison documented in Step 3c | ✅ | "blind version comparison is documented in Step 3c", "comparator subagent" |
| 5 | Response describes PREVIOUS_SNAPSHOT and two executor subagents | ✅ | "PREVIOUS_SNAPSHOT", "current skill tree", "PREVIOUS_SNAPSHOT tree" — two subagents per eval |
| 5 | Response confirms trigger conditions and skip conditions | ✅ | Feature fully documented with trigger and skip conditions |
| 6 | Response routes to dbt-model-from-stm | ✅ | Routes immediately to dbt-model-from-stm — no audit performed |
| 6 | Response does NOT apply rubric | ✅ | "dbt-model-from-stm skill, not a skill quality audit" |
| 6 | No category table in response | ✅ | No category table present |
| 7 | Response routes to azure-keyvault-deployer | ✅ | Routes to azure-keyvault-deployer — no rubric |
| 7 | Response names the correct skill | ✅ | "azure-keyvault-deployer skill" named explicitly |
| 7 | Response does NOT perform a review | ✅ | No review or category grade table |
| 8 | Response answers the question directly | ✅ | "Paris." — two words, no workflow |
| 8 | Response does NOT trigger the skill | ✅ | No review workflow initiated |
| 8 | Response is concise | ✅ | One-line response |
| 9 | Response asks which skill to review | ✅ | "Which skill would you like me to review?" |
| 9 | Response asks for path; no review generated | ✅ | Clarification requested; no review output |
| 9 | Response enumerates all three input options | ✅ | Enumerates path, pasted content, skill name |
| 10 | Response names `agents/grader.md` | ✅ | "agents/grader.md inside the skill-reviewer folder" |
| 10 | Response cites grading schema fields | ✅ | "`passed: true`", "`evidence`", "`overall_result`" all cited |
| 10 | Response names grading-\<eval_id\>.json | ✅ | "grading-\<eval_id\>.json", "assertions" array, "eval_id" |

## Category Grades

| # | Category | Grade | Explanation |
|---|----------|-------|-------------|
| 1 | Triggering (Description Quality) | PASS | ↩ carried — trigger terms precise; no false-positive overlap |
| 2 | Anatomy & Structure | PASS | ↩ carried — valid YAML frontmatter; references/ and agents/ present |
| 3 | Instructions Clarity | PASS | Two independent skip rules (category / executor) clearly documented; RUN_STEP_3C behaviour unambiguous |
| 4 | Output Quality | PASS | Step 7a ⚠️ callout makes `reviewed_at` unmissable; executor now cites all required history.json fields |
| 5 | Testability | PASS | evals.json eval 4 expanded to 5 assertions, matching test-cases.md spec; all assertions verified |
| 6 | Resource Efficiency | PASS | ↩ carried — no common-knowledge padding; parallel subagents explicit |
| 7 | Security & Trust | PASS | ↩ carried — no credential handling; no destructive ops |
| 8 | Coexistence & Recall | PASS | ↩ carried — Registry and no-overlap notes present |
| 9 | Model Compatibility | PASS | ↩ carried — Model Compatibility section present |
| 10 | Workflow & Feedback Loops | PASS | Incremental mode, carry PASS, re-run FAIL, comparator loop all documented |
| 11 | Maintainability & Lifecycle | PASS | ↩ carried — Registry with owner/version/last-eval present |
| 12 | Gotchas / Lessons Learned | PASS | ↩ carried — Gotchas covers comparator, rsync, Unicode, executor-skip rule |
| 13 | Anti-Pattern Audit | PASS | ↩ carried — no backslashes, no magic constants, no install commands |

## Version Comparison

| Eval | Verdict | Reasoning |
|------|---------|-----------|
| 2 | new_wins | New version articulates two distinct skip rules and RUN_STEP_3C flag; old version described one undifferentiated rule |
| 4 | new_wins | New version explicitly names `reviewed_at` (ISO 8601 UTC) in history.json description; old version omits it |

**Overall: new_wins (2–0)** vs snapshot `snapshots/skill-reviewer/2026-05-07T115753Z/`

## History

| reviewed_at | date | Unit Tests | Assertions | Categories | Verdict | Notes |
|-------------|------|------------|------------|------------|---------|-------|
| 2026-05-07T10:00:00Z | 2026-05-07 | 10/10 | 30/30 | 9/13 | FAIL | First review — separation of duties HIGH failure |
| 2026-05-07T11:21:20Z | 2026-05-07 | 10/10 | 30/30 | 9/13 | FAIL | Incremental; all SKIP; first snapshot |
| 2026-05-07T11:27:37Z | 2026-05-07 | 10/10 | 30/30 | 13/13 | PASS | Registry, Gotchas, Model Compat added |
| 2026-05-07T11:40:58Z | 2026-05-07 | 10/10 | 30/30 | 13/13 | PASS | carried_from pointers; comparator starvation bug documented |
| 2026-05-07T11:57:53Z | 2026-05-07 | 9/10 | — | 12/13 | FAIL | Eval 4 FAIL — reviewed_at missing; Cat 4 FAIL |
| 2026-05-07T12:09:46Z | 2026-05-07 | 10/10 | 32/32 | 13/13 | PASS | ⚠️ callout + 5-assertion eval 4 fix; comparator new_wins 2–0 |
