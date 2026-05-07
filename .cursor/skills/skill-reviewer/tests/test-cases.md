# Eval Suite: skill-reviewer

_Re-run this suite after every skill update to catch regressions._

## Should-trigger

| # | Input | Expected behavior |
|---|-------|-------------------|
| 1 | "Review `.cursor/skills/meetingintroskill/` for production readiness — full rubric." | Agent follows workflow: identifies skill path, reads SKILL.md + bundled files, will produce 13-category review with PASS/FAIL output format (Unit Tests, Category Grades, High/Medium/Low failures). |
| 2 | "We have a prior review in `tests/results/review-meetingintroskill-2026-05-07.md` — only re-check what failed last time." | Agent uses incremental mode language: carry forward prior PASS tests/categories, re-run failures only. |
| 3 | "Grade this SKILL.md against Anthropic skill best practices and list HIGH findings only." | Agent scopes to rubric category 7 HIGH and overall structure; produces severity-tagged findings. |
| 4 | "After you review the skill, save the report to `tests/results/` and append `history.json`." | Agent cites Step 6–7: review filename pattern `review-<skill>-YYYY-MM-DD.md`, history append, benchmark .md/.html. |
| 5 | "Does our reviewer workflow document blind comparison between skill versions?" | Agent cites Step 3c, comparator, snapshot path — not invented behavior. |

## Should-not-trigger

| # | Input | Expected behavior |
|---|-------|-------------------|
| 1 | "Convert this STM markdown into dbt SQL models for Snowflake." | Handled by `dbt-model-from-stm` or general SQL work — not a skill quality audit. |
| 2 | "Bootstrap a new Azure Key Vault and upload secrets from `.env`." | Handled by `azure-keyvault-deployer` — not skill reviewing. |
| 3 | "What is the capital of France?" | General knowledge — no skill review. |

## Edge cases

| # | Input | Expected behavior |
|---|-------|-------------------|
| 1 | "Run the skill reviewer." | No target skill given — agent must ask for path, pasted SKILL.md, or skill name before running steps. |
| 2 | "Use the grader agent — what file defines its contract?" | Agent points to `agents/grader.md` and the `grading-<eval_id>.json` schema with `text`, `passed`, `evidence`. |
