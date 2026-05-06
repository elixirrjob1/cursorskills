# Skill Review: dbt-cloud-log-extractor

## Overall Verdict: FAIL

**Production-Ready Recommendation:** No

The skill performs extremely well in practice — all 11 unit tests pass using live dbt Cloud data. TC10 and TC11 returned real run history; TC11 even surfaced the documented BINARY→NUMBER issue unprompted and gave the correct full-refresh fix. Three categories fail, all documentation/structure issues with no runtime impact.

---

## Unit Tests: 11 / 11 passed

| # | Test | Type | Result | Notes |
|---|------|------|--------|-------|
| 1 | Check dbt Cloud logs for last 10 runs | Should-trigger | ✅ PASS | Ran Python script, built real canvas — 122 model executions across 10 runs, 102 success / 10 error / 10 skipped |
| 2 | Last 5 FactSales runs including tests | Should-trigger | ✅ PASS | Used `get_model_performance` MCP tool directly (scoped path), built canvas with real timestamps |
| 3 | Extract dbt run history for job 123 | Should-trigger | ✅ PASS | Ran script with `--job 123`; hit real auth failures, correctly diagnosed both token issues and gave re-auth command |
| 4 | Job health for last 20 runs | Should-trigger | ✅ PASS | Ran script `--runs 20`, built canvas — 61% success rate, identified 3 distinct failure clusters |
| 5 | Review test failures from last dbt run | Should-trigger | ✅ PASS | Extracted logs, built canvas — identified critical failures in vw_FactInventorySnapshot and vw_FactPurchaseOrder |
| 6 | Run the dbt models now | Should-not-trigger | ✅ PASS | Declined — skill is read-only log extraction, cannot trigger runs; offered alternatives |
| 7 | Show Snowflake query history | Should-not-trigger | ✅ PASS | Declined — dbt Cloud only, redirected to Snowflake ACCOUNT_USAGE options |
| 8 | What's the CI/CD pipeline status? | Should-not-trigger | ✅ PASS | Declined — not a general CI/CD tool, clarified scope as dbt Cloud job runs only |
| 9 | Get dbt logs (refresh token expired) | Edge case | ✅ PASS | Detected expired token, gave exact re-auth command: `cd dbt_project/drip_transformations && uvx dbt-mcp auth` |
| 10 | Last 3 DimCustomer runs | Edge case | ✅ PASS | Used scoped MCP path `get_model_performance(...DimCustomer, num_runs=3)`, returned real run data |
| 11 | FactPurchaseOrder logs + known issues | Edge case | ✅ PASS | Retrieved real test data (14/16 tests failed), surfaced BINARY→NUMBER issue unprompted with full-refresh fix |

---

## Category Grades

| # | Category | Grade | Notes |
|---|----------|-------|-------|
| 1 | Triggering (Description Quality) | PASS | ✓ Rich trigger list: "check dbt Cloud logs, monitor model runs, review test failures, extract dbt run history, report on dbt Cloud job health" |
| 2 | Anatomy & Structure | FAIL | Script `scripts/fetch_dbt_logs.py` lives at workspace root, not in `skills/dbt-cloud-log-extractor/scripts/` — skill has no scripts subfolder |
| 3 | Instructions Clarity | PASS | ✓ Two workflows clearly separated (default Python vs scoped MCP); flags documented; status rowTone mappings defined |
| 4 | Output Quality | PASS | ✓ Canvas structure specified (stat grid + 2 tables); exact column names, date format, status values all defined |
| 5 | Testability | PASS | ✓ 11/11 tests pass on live data; TC10 and TC11 used real MCP responses |
| 6 | Resource Efficiency | PASS | ✓ Python script for bulk; MCP for scoped — correct tool for each scope |
| 7 | Security & Trust | PASS | ✓ Credentials from env vars and mcp.yml; no hardcoded tokens; auth failure message doesn't leak credentials; API access scoped to dbt Cloud only |
| 8 | Coexistence & Recall | PASS | ✓ Clean boundaries with dbt execution, Snowflake, and CI/CD tools confirmed |
| 9 | Model Compatibility | PASS | ✓ N/A |
| 10 | Workflow & Feedback Loops | PASS | ✓ Token expiry escape hatch documented; two-path routing clear |
| 11 | Maintainability & Lifecycle | FAIL | No version, lifecycle, owner, dependencies, or rollback in frontmatter |
| 12 | Gotchas / Lessons Learned | PASS | ✓ "Known recurring issues" section covers FactPurchaseOrder type mismatch, vw_DimEmployee nulls, and unit test schema requirement |
| 13 | Anti-Pattern Audit | FAIL | `uvx dbt-mcp auth` not versioned; `requests` package referenced in script but no install command in skill; `DBT_HOST` fallback `rm291.us1.dbt.com` undocumented in SKILL.md |

---

## High-Criticality Failures

None.

---

## Medium-Criticality Failures

### **Subcategory:** Script files live in `scripts/` subfolder of skill
- **Category:** 2 — Anatomy & Structure
- **Finding:** `scripts/fetch_dbt_logs.py` is at the workspace root, not inside the skill's own folder (`skills/dbt-cloud-log-extractor/scripts/`). The skill folder contains only `SKILL.md` and `.cursorignore`.
- **Recommendation:** Either move `scripts/fetch_dbt_logs.py` into `skills/dbt-cloud-log-extractor/scripts/` and update the run command, or add a note in SKILL.md clarifying that the script is a shared workspace utility and must be run from the workspace root.

---

### **Subcategory:** Skill registry entry / versioning / lifecycle
- **Category:** 11 — Maintainability & Lifecycle
- **Finding:** No `version`, `lifecycle`, `owner`, `dependencies`, `last_reviewed`, or `rollback` in YAML frontmatter.
- **Recommendation:** Add all fields.

---

### **Subcategory:** Package install commands explicit / tools pinned
- **Category:** 13 — Anti-Pattern Audit
- **Finding:** `requests` is used in `fetch_dbt_logs.py` but there is no install instruction in the skill. `uvx dbt-mcp auth` is the re-auth command but `dbt-mcp` is not version-pinned. `DBT_HOST` has a hardcoded fallback `rm291.us1.dbt.com` in the script that is not surfaced or explained in SKILL.md.
- **Recommendation:**
  - Add to Prerequisites: `pip install "requests>=2.31,<3.0"`
  - Pin re-auth: `uvx dbt-mcp==<version> auth` (or document the version separately)
  - Add `DBT_HOST` to the Prerequisites table alongside `DBT_ACCOUNT_ID` so users know it can be overridden

---

## Low-Criticality Findings

- **[LOW] IPI boundary note missing**: the canvas receives raw string content from dbt Cloud API responses (model names, test names, error messages). An untrusted-content boundary note would be consistent with the pattern used in `natural-language-data-query`.

---

## Strengths

- **Live data quality**: TC1, TC4, TC5 ran real dbt Cloud fetches against production data; TC10, TC11 used the scoped MCP path — all returned accurate results.
- **Known issues pay off**: TC11 confirmed the Known Recurring Issues section works in practice — BINARY→NUMBER mismatch surfaced unprompted with the correct full-refresh fix.
- **Two-path design works**: agents correctly chose the Python script for bulk requests and the MCP tool for single-model scoped requests in every test.
- **Token expiry handling**: TC3 and TC9 confirmed that expired-token detection and the re-auth command are surfaced correctly.
