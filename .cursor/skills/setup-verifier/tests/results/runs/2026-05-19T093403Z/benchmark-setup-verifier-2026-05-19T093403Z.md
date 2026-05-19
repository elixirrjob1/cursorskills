# Benchmark Report: setup-verifier
_Generated: 2026-05-19T09:34:03Z UTC · First review run_

## Summary

| Metric | Value |
|--------|-------|
| Overall Verdict | FAIL |
| Unit Tests | 5 / 9 passed (56%) |
| Assertions | 21 / 26 (81%) |
| Categories | 9 / 13 passed |
| High Failures | 1 |
| Medium Failures | 6 |
| Comparator | — (first run, no prior snapshot) |

## Unit Test Results

| # | Test | Type | Result | Assertions |
|---|------|------|--------|------------|
| 1 | check my setup | Should-trigger | ❌ FAIL | 2/3 |
| 2 | verify my connections are working | Should-trigger | ❌ FAIL | 2/3 |
| 3 | validate my environment before I start work | Should-trigger | ❌ FAIL | 1/3 |
| 4 | is my MCP access set up correctly? | Should-trigger | ✅ PASS | 3/3 |
| 5 | extract the last dbt Cloud run logs | Should-not-trigger | ✅ PASS | 3/3 |
| 6 | run the source system analyser against my database | Should-not-trigger | ✅ PASS | 3/3 |
| 7 | how do I install Cursor? | Should-not-trigger | ✅ PASS | 3/3 |
| 8 | just check my Snowflake connection | Edge case | ✅ PASS | 3/3 |
| 9 | my setup is done | Edge case | ❌ FAIL | 1/2 |

## Assertion Detail

| Eval | Assertion | Passed | Evidence |
|------|-----------|--------|----------|
| 1 | Response references Snowflake, OpenMetadata, dbt, and Fivetran checks | ✅ Yes | Listed all 4: "Snowflake query, OpenMetadata service list, dbt Cloud job list, Fivetran connector list" |
| 1 | Response includes a table with pass/fail (✅ or ❌) per system | ❌ No | No table rendered — Ask mode blocked live MCP calls; agent described intent only |
| 1 | Response references .env variable validation | ✅ Yes | "plus the .env variable audit" |
| 2 | Response includes a results table with at least 4 rows | ✅ Yes | Table with 5 rows shown (all systems + .env) in pending state |
| 2 | Response contains ✅ or ❌ symbols indicating per-system status | ❌ No | Status column shows "—" (pending), not ✅/❌ — no live execution |
| 2 | Response does NOT describe how to install or configure a new system | ✅ Yes | No installation guidance; focused on pending check results |
| 3 | Response includes at least one specific fix or remediation step | ❌ No | Agent referenced fixes generically; no specific fix text surfaced |
| 3 | Response covers all four systems: Snowflake, OpenMetadata, dbt, Fivetran | ✅ Yes | All four in the pending table |
| 3 | Response ends with 'Setup complete' or a list of failures | ❌ No | Ended with mode-switch request |
| 4 | Response mentions testing or querying specific MCP servers | ✅ Yes | Listed each by tool name in a step table |
| 4 | Response outputs results in a tabular or structured format | ✅ Yes | Step | Check | Tool called table |
| 4 | Response does NOT just describe what MCP is without running checks | ✅ Yes | No MCP explanation; went straight to tool list |
| 5 | Response does NOT call list_database_services or list_connectors | ✅ Yes | Routed to dbt-cloud-log-extractor only |
| 5 | Response focuses on dbt run history, model status, or log extraction | ✅ Yes | "query the dbt Cloud MCP server for the most recent job run(s)..." |
| 5 | Response does NOT output a 4-system connection check table | ✅ Yes | No setup-verifier table shown |
| 6 | Response does NOT run a 4-system connection check | ✅ Yes | Routed to source-system-analyser only |
| 6 | Response mentions running or invoking the source system analyser | ✅ Yes | "I use SKILL 2 - source-system-analyser" |
| 6 | Response does NOT output the setup-verifier pass/fail table format | ✅ Yes | No System | Status | Detail table |
| 7 | Response does NOT call any MCP tools | ✅ Yes | Plain installation guidance only |
| 7 | Response does NOT output a setup verification table | ✅ Yes | Noted skill is post-install; no check table |
| 7 | Response provides installation guidance or points to cursor.com | ✅ Yes | "Download the installer from cursor.com" with OS-specific steps |
| 8 | Response checks Snowflake at minimum | ✅ Yes | Showed Snowflake SELECT + example result row |
| 8 | Response does NOT skip other MCP systems without explanation | ✅ Yes | Full table with all 4 systems shown |
| 8 | Response produces a structured result for at least Snowflake | ✅ Yes | "| Snowflake | ✅ | Connected as FILLIP / TRANSFORMER |" |
| 9 | Response does NOT immediately run all four MCP checks | ✅ Yes | No live checks fired (Ask mode); requested mode switch first |
| 9 | Response asks clarifying question or acknowledges without triggering | ❌ No | "I'd love to verify it now" — offered verification without a clarifying question |

## Category Grades

| # | Category | Grade | Explanation |
|---|----------|-------|-------------|
| 1 | Triggering (Description Quality) | PASS | WHAT+WHEN present; 5 trigger terms; 14-char name; 350-char description; covers natural synonyms |
| 2 | Anatomy & Structure | PASS | Valid frontmatter; 101 lines; no deep reference chains; conventional layout |
| 3 | Instructions Clarity | PASS | Low-freedom MCP calls correct for verification; consistent terminology; no time-sensitive content; clear If-fails branches |
| 4 | Output Quality | PASS | Exact table template with columns, icons, ✅/❌ examples, and completion/failure messages |
| 5 | Testability | PASS | All HIGH subcategories pass; eval suite created this run; isolation works in Agent mode |
| 6 | Resource Efficiency | PASS | No common-knowledge padding; MCP calls over scripts; "Call X with Y" execution intent explicit |
| 7 | Security & Trust | FAIL | MCP calls use bare names not ServerName:tool format (MEDIUM fail; 1/2 MEDIUMs = 50% < 70%) |
| 8 | Coexistence & Recall | PASS | Clean routing confirmed; 3/3 should-not-trigger evals passed; no trigger overlap |
| 9 | Model Compatibility | FAIL | No model tier tested or documented (1 MEDIUM fails = 0% < 70%) |
| 10 | Workflow & Feedback Loops | PASS | N/A — read-only skill, no destructive or batch operations |
| 11 | Maintainability & Lifecycle | FAIL | HIGH: no separation of duties; no Registry, versioning, or lifecycle stage (0/3 MEDIUMs pass) |
| 12 | Gotchas / Lessons Learned | FAIL | No Gotchas section; not marked v0.1 draft (1 MEDIUM fails) |
| 13 | Anti-Pattern Audit | PASS | Forward slashes; no magic numbers; no scripts; default+escape used |

## Version Comparison (if comparator was run)

_Not run — first review, no prior snapshot available._

## History (all reviews)

| Reviewed at (ISO UTC) | Calendar | Unit Tests | Assertions | Categories | Verdict | Notes |
|----------------------|----------|------------|------------|------------|---------|-------|
| 2026-05-19T09:34:03Z | 2026-05-19 | 5/9 (56%) | 21/26 (81%) | 9/13 | FAIL | First review |

Full narrative: `review-setup-verifier-2026-05-19T093403Z.md`.
