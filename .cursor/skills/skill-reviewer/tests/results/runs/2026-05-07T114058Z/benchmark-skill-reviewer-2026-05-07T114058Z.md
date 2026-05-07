# Benchmark Report: skill-reviewer
_Generated: 2026-05-07T11:40:58Z · carried_from pointers; no grading duplication_

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
| 1 | Review `.cursor/skills/meetingintroskill/` for production readiness — full rubric. | should-trigger | ✅ PASS | 3/3 | Opens with reading `SKILL.md`, `test-cases.md`, `evals.json`, and the meetingint · Delivers **full 13-category** assessment and **Category Grades** table. · States Overall Verdict FAIL and per-category PASS/FAIL in the Category Grades ta |
| 2 | We have a prior review in tests/results/review-meetingintroskill-2026-05-07.md — only re-check what failed last time. | should-trigger | ✅ PASS | 3/3 | States **prior review** baseline and incremental workflow. · **PASS rows were carried**; only **failed** items re-checked. · Does not deny prior review—explicitly loads `review-meetingintroskill-2026-05-07 |
| 3 | Grade this SKILL.md against Anthropic skill best practices and list HIGH findings only. | should-trigger | ✅ PASS | 3/3 | Frames output as **[HIGH]**-only and lists **High-criticality failures**. · Discusses **Category 7 (Security & Trust)** and rubric context. · Completes a substantive audit; does not refuse the request. |
| 4 | After you review the skill, save the report to tests/results/ and append history.json. | should-trigger | ✅ PASS | 3/3 | **`tests/results/`** and artifact table with full paths. · **`history.json`** and **benchmark** `.md`/`.html` called out. · Step 6–7 document **`review-<skill>-<RUN_SLUG>.md`** (timestamped); legacy **`re |
| 5 | Does our reviewer workflow document blind comparison between skill versions? | should-trigger | ✅ PASS | 3/3 | **Step 3c**, **comparator**, and **blind comparison** explained. · **Snapshot**, **current vs snapshot**, and **two skill versions** described. · Confirms the workflow documents this feature rather than denying it. |
| 6 | Convert this STM markdown into dbt SQL models for Snowflake. | should-not-trigger | ✅ PASS | 3/3 | Lead section routes to **dbt-model-from-stm**; no 13-category audit as main body · **STM**, **dbt**, **Snowflake**, and repo paths referenced. · Main content is dbt/STM guidance, not Category 1–13 tables alone. |
| 7 | Bootstrap a new Azure Key Vault and upload secrets from `.env`. | should-not-trigger | ✅ PASS | 3/3 | Explicitly states skill-reviewer is **not** for Key Vault; no rubric-as-sole-del · **Azure**, **Key Vault**, `azure-keyvault-deployer`, and populate script referen · Points to **azure-keyvault-deployer** skill rather than claiming skill-reviewer  |
| 8 | What is the capital of France? | should-not-trigger | ✅ PASS | 3/3 | Single factual sentence; no skill-audit workflow. · **Paris** as capital of France. · One-line answer; not a 13-category report. |
| 9 | Run the skill reviewer. | edge-case | ✅ PASS | 3/3 | **Please specify the target** and bullet options for path/skill/paste — asks whi · No fabricated full review for a specific unnamed skill—only asks for target. · Mentions **path**, **skill folder**, **Pasted content** / **SKILL.md**. |
| 10 | Use the grader agent — what file defines its contract? | edge-case | ✅ PASS | 3/3 | Names **`agents/grader.md`** as the contract file. · Describes **`passed`**, **`evidence`**, and **`overall_result`** in the JSON sha · Mentions **`assertions[]`**, **`eval_id`**, and grading JSON structure. |

## Assertion Detail

| Eval | Assertion | Passed | Evidence |
|------|-----------|--------|----------|
| 1 | Response mentions reading SKILL.md or the skill folder | ✅ | Opens with reading `SKILL.md`, `test-cases.md`, `evals.json`, and the meetingintroskill folder/review files. _(↩ 2026-05-07)_ |
| 1 | Response references 13 categories or the full rubric | ✅ | Delivers **full 13-category** assessment and **Category Grades** table. _(↩ 2026-05-07)_ |
| 1 | Response describes PASS/FAIL or structured review output (Category Grades or similar) | ✅ | States Overall Verdict FAIL and per-category PASS/FAIL in the Category Grades table. _(↩ 2026-05-07)_ |
| 2 | Response mentions incremental, prior review, or carry forward | ✅ | States **prior review** baseline and incremental workflow. _(↩ 2026-05-07)_ |
| 2 | Response mentions re-running only failed tests or failed categories | ✅ | **PASS rows were carried**; only **failed** items re-checked. _(↩ 2026-05-07)_ |
| 2 | Response does NOT claim there is no prior review without checking | ✅ | Does not deny prior review—explicitly loads `review-meetingintroskill-2026-05-07.md`. _(↩ 2026-05-07)_ |
| 3 | Response mentions HIGH, [HIGH], or severity | ✅ | Frames output as **[HIGH]**-only and lists **High-criticality failures**. _(↩ 2026-05-07)_ |
| 3 | Response references Category 7, Security, or rubric | ✅ | Discusses **Category 7 (Security & Trust)** and rubric context. _(↩ 2026-05-07)_ |
| 3 | Response does NOT refuse as out of scope | ✅ | Completes a substantive audit; does not refuse the request. _(↩ 2026-05-07)_ |
| 4 | Response mentions tests/results or results folder | ✅ | **`tests/results/`** and artifact table with full paths. _(↩ 2026-05-07)_ |
| 4 | Response mentions history.json or benchmark report | ✅ | **`history.json`** and **benchmark** `.md`/`.html` called out. _(↩ 2026-05-07)_ |
| 4 | Response mentions review- prefix filename with RUN_SLUG or dated/timestamped report pattern (Step 6–7) | ✅ | Step 6–7 document **`review-<skill>-<RUN_SLUG>.md`** (timestamped); legacy **`review-<skill>-YYYY-MM-DD.md`** noted where applicable. _(↩ 2026-05-07)_ |
| 5 | Response mentions Step 3c OR comparator OR blind comparison | ✅ | **Step 3c**, **comparator**, and **blind comparison** explained. _(↩ 2026-05-07)_ |
| 5 | Response mentions snapshot OR old version vs new version OR two skill versions | ✅ | **Snapshot**, **current vs snapshot**, and **two skill versions** described. _(↩ 2026-05-07)_ |
| 5 | Response does NOT say the skill has no such feature | ✅ | Confirms the workflow documents this feature rather than denying it. _(↩ 2026-05-07)_ |
| 6 | Response does NOT begin a full 13-category skill audit as the main deliverable | ✅ | Lead section routes to **dbt-model-from-stm**; no 13-category audit as main body. _(↩ 2026-05-07)_ |
| 6 | Response references dbt, STM, SQL, or another skill instead | ✅ | **STM**, **dbt**, **Snowflake**, and repo paths referenced. _(↩ 2026-05-07)_ |
| 6 | Response does NOT output only Category 1-13 tables with no dbt content | ✅ | Main content is dbt/STM guidance, not Category 1–13 tables alone. _(↩ 2026-05-07)_ |
| 7 | Response does NOT perform a skill-quality rubric review as the sole answer | ✅ | Explicitly states skill-reviewer is **not** for Key Vault; no rubric-as-sole-deliverable. _(↩ 2026-05-07)_ |
| 7 | Response mentions Azure, Key Vault, vault, or deployment | ✅ | **Azure**, **Key Vault**, `azure-keyvault-deployer`, and populate script referenced. _(↩ 2026-05-07)_ |
| 7 | Response does not claim skill-reviewer alone is sufficient for vault bootstrap without naming deployer or infrastructure skill | ✅ | Points to **azure-keyvault-deployer** skill rather than claiming skill-reviewer suffices alone. _(↩ 2026-05-07)_ |
| 8 | Response does NOT perform a skill audit workflow | ✅ | Single factual sentence; no skill-audit workflow. _(↩ 2026-05-07)_ |
| 8 | Response answers Paris OR treats as general knowledge | ✅ | **Paris** as capital of France. _(↩ 2026-05-07)_ |
| 8 | Response is short or direct — not a 13-category report | ✅ | One-line answer; not a 13-category report. _(↩ 2026-05-07)_ |
| 9 | Response contains a question mark OR asks which skill or path | ✅ | **Please specify the target** and bullet options for path/skill/paste — asks which skill to review. _(↩ 2026-05-07)_ |
| 9 | Response does NOT output a complete fabricated review for a specific unnamed skill as if the target were known | ✅ | No fabricated full review for a specific unnamed skill—only asks for target. _(↩ 2026-05-07)_ |
| 9 | Response mentions needing SKILL.md, path, or pasted content | ✅ | Mentions **path**, **skill folder**, **Pasted content** / **SKILL.md**. _(↩ 2026-05-07)_ |
| 10 | Response mentions agents/grader.md OR grader.md | ✅ | Names **`agents/grader.md`** as the contract file. _(↩ 2026-05-07)_ |
| 10 | Response mentions passed AND evidence OR overall_result in relation to grading | ✅ | Describes **`passed`**, **`evidence`**, and **`overall_result`** in the JSON shape. _(↩ 2026-05-07)_ |
| 10 | Response mentions grading JSON OR eval_id OR assertion | ✅ | Mentions **`assertions[]`**, **`eval_id`**, and grading JSON structure. _(↩ 2026-05-07)_ |

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

Full narrative: `runs/2026-05-07T114058Z/review-skill-reviewer-2026-05-07T114058Z.md`.
