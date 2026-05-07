# Benchmark Report: skill-reviewer
_Generated: 2026-05-07 · live executor harness; benchmark `.html` aligned to Step 7c (full Assertion Detail + Explanation column)._

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

| Eval | Assertion | Passed | Evidence |
|------|-----------|--------|----------|
| 1 | Response mentions reading SKILL.md or the skill folder | ✅ | Opens with reading `SKILL.md`, `test-cases.md`, `evals.json`, and the meetingintroskill folder/review files. |
| 1 | Response references 13 categories or the full rubric | ✅ | Delivers **full 13-category** assessment and **Category Grades** table. |
| 1 | Response describes PASS/FAIL or structured review output (Category Grades or similar) | ✅ | States Overall Verdict FAIL and per-category PASS/FAIL in the Category Grades table. |
| 2 | Response mentions incremental, prior review, or carry forward | ✅ | States **prior review** baseline and incremental workflow. |
| 2 | Response mentions re-running only failed tests or failed categories | ✅ | **PASS rows were carried**; only **failed** items re-checked. |
| 2 | Response does NOT claim there is no prior review without checking | ✅ | Does not deny prior review—explicitly loads `review-meetingintroskill-2026-05-07.md`. |
| 3 | Response mentions HIGH, [HIGH], or severity | ✅ | Frames output as **[HIGH]**-only and lists **High-criticality failures**. |
| 3 | Response references Category 7, Security, or rubric | ✅ | Discusses **Category 7 (Security & Trust)** and rubric context. |
| 3 | Response does NOT refuse as out of scope | ✅ | Completes a substantive audit; does not refuse the request. |
| 4 | Response mentions tests/results or results folder | ✅ | **`tests/results/`** and artifact table with full paths. |
| 4 | Response mentions history.json or benchmark report | ✅ | **`history.json`** and **benchmark** `.md`/`.html` called out. |
| 4 | Response mentions review- prefix filename or dated report pattern | ✅ | **`review-<skill>-YYYY-MM-DD.md`** naming pattern documented. |
| 5 | Response mentions Step 3c OR comparator OR blind comparison | ✅ | **Step 3c**, **comparator**, and **blind comparison** explained. |
| 5 | Response mentions snapshot OR old version vs new version OR two skill versions | ✅ | **Snapshot**, **current vs snapshot**, and **two skill versions** described. |
| 5 | Response does NOT say the skill has no such feature | ✅ | Confirms the workflow documents this feature rather than denying it. |
| 6 | Response does NOT begin a full 13-category skill audit as the main deliverable | ✅ | Lead section routes to **dbt-model-from-stm**; no 13-category audit as main body. |
| 6 | Response references dbt, STM, SQL, or another skill instead | ✅ | **STM**, **dbt**, **Snowflake**, and repo paths referenced. |
| 6 | Response does NOT output only Category 1-13 tables with no dbt content | ✅ | Main content is dbt/STM guidance, not Category 1–13 tables alone. |
| 7 | Response does NOT perform a skill-quality rubric review as the sole answer | ✅ | Explicitly states skill-reviewer is **not** for Key Vault; no rubric-as-sole-deliverable. |
| 7 | Response mentions Azure, Key Vault, vault, or deployment | ✅ | **Azure**, **Key Vault**, `azure-keyvault-deployer`, and populate script referenced. |
| 7 | Response does not claim skill-reviewer alone is sufficient for vault bootstrap without naming deployer or infrastructure skill | ✅ | Points to **azure-keyvault-deployer** skill rather than claiming skill-reviewer suffices alone. |
| 8 | Response does NOT perform a skill audit workflow | ✅ | Single factual sentence; no skill-audit workflow. |
| 8 | Response answers Paris OR treats as general knowledge | ✅ | **Paris** as capital of France. |
| 8 | Response is short or direct — not a 13-category report | ✅ | One-line answer; not a 13-category report. |
| 9 | Response contains a question mark OR asks which skill or path | ✅ | **Please specify the target** and bullet options for path/skill/paste — asks which skill to review. |
| 9 | Response does NOT output a complete fabricated review for a specific unnamed skill as if the target were known | ✅ | No fabricated full review for a specific unnamed skill—only asks for target. |
| 9 | Response mentions needing SKILL.md, path, or pasted content | ✅ | Mentions **path**, **skill folder**, **Pasted content** / **SKILL.md**. |
| 10 | Response mentions agents/grader.md OR grader.md | ✅ | Names **`agents/grader.md`** as the contract file. |
| 10 | Response mentions passed AND evidence OR overall_result in relation to grading | ✅ | Describes **`passed`**, **`evidence`**, and **`overall_result`** in the JSON shape. |
| 10 | Response mentions grading JSON OR eval_id OR assertion | ✅ | Mentions **`assertions[]`**, **`eval_id`**, and grading JSON structure. |

_Grader artifacts (machine-readable): `tests/results/runs/2026-05-07/grading-1.json` … `grading-10.json`._

## Category Grades

| # | Category | Grade | Explanation |
|---|----------|-------|-------------|
| 1 | Triggering (Description Quality) | ✅ PASS | Description lists what + when; triggers rich ↩ carried |
| 2 | Anatomy & Structure | ✅ PASS | Frontmatter, references, body length ↩ carried |
| 3 | Instructions Clarity | ✅ PASS | Steps 1–7, Step 3c signposted ↩ carried |
| 4 | Output Quality | ✅ PASS | Output format template strict ↩ carried |
| 5 | Testability | ✅ PASS | Isolation, assertions, eval suite ↩ carried |
| 6 | Resource Efficiency | ✅ PASS | Templates extracted; lean workflow ↩ carried |
| 7 | Security & Trust | ✅ PASS | Markdown-first; W007/W011 pointers ↩ carried |
| 8 | Coexistence & Recall | ❌ FAIL | [MEDIUM] Rubric text exists (lines 366–368) but no **operational** guidance for teams (e.g. validating alongside adjacent skills, practical recall/skill-count constraints in deployment notes). |
| 9 | Model Compatibility | ❌ FAIL | No documented validation target (model tier / product) for the workflow-heavy reviewer path. |
| 10 | Workflow & Feedback Loops | ✅ PASS | Incremental loop, grader reconciliation ↩ carried |
| 11 | Maintainability & Lifecycle | ❌ FAIL | [HIGH] No documented separation of author vs reviewer. [MEDIUM] No registry block (owner, dependencies, last-eval), version pin/rollback, or explicit lifecycle stage in skill body. |
| 12 | Gotchas / Lessons Learned | ❌ FAIL | No "Gotchas" / "Common mistakes" section (e.g. Step 3c snapshot prerequisite, when to skip comparator, incremental carry-forward caveats). |
| 13 | Anti-Pattern Audit | ✅ PASS | Forward-slash paths; no bundled scripts requiring error handling ↩ carried |

## Version Comparison (if comparator was run)

_Not run — no Step 3c snapshot in this session._

## History (all reviews)

| Date | Unit Tests | Assertions | Categories | Verdict | Comparison |
|------|------------|------------|------------|---------|------------|
| 2026-05-07 | 10/10 | 30/30 | 9/13 | FAIL | — (first; simulated executor text) |
| 2026-05-07 | 10/10 | 30/30 | 9/13 | FAIL | — (incremental; evals SKIP) |
| 2026-05-07 | 10/10 | 30/30 | 9/13 | FAIL | — (live subagents) |

Full narrative: `review-skill-reviewer-2026-05-07.md`.
