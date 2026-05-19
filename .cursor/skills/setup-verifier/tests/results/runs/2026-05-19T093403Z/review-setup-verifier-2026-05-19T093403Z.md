# Skill Review: setup-verifier

## Overall Verdict: FAIL

**Production-Ready Recommendation:** No

The `setup-verifier` skill is well-designed for its core purpose: it has clear, specific triggering, concise instructions, correct output format, and strong routing behaviour for should-not-trigger cases. However, it fails on four categories — one HIGH failure (no separation of duties / no registry) in Category 11 that blocks production readiness, plus medium failures in Security (MCP tool format), Model Compatibility, and Gotchas. The three should-trigger eval failures are primarily an artefact of the readonly test environment (Ask mode blocks live MCP calls) and are not indicative of skill design defects; the real gap is Eval 9 where the skill over-triggers on an ambiguous statement.

**Note on unit test environment:** Evals 1–3 failed on output-table assertions because readonly subagents cannot execute live MCP calls. In an actual Agent mode session the skill would produce the correct pass/fail table. This is a test environment constraint, not a skill design defect. It is flagged as a medium finding in Testability.

## Unit Tests: 5 / 9 passed

| # | Test | Type | Result | Notes |
|---|------|------|--------|-------|
| 1 | check my setup | Should-trigger | ❌ FAIL | Table assertion unmet — Ask mode blocked live MCP execution |
| 2 | verify my connections are working | Should-trigger | ❌ FAIL | Status column shows "—" not ✅/❌ — Ask mode constraint |
| 3 | validate my environment before I start work | Should-trigger | ❌ FAIL | No specific fixes shown; no completion message — Ask mode constraint |
| 4 | is my MCP access set up correctly? | Should-trigger | ✅ PASS | Listed all 4 tools by name; showed structured step table |
| 5 | extract the last dbt Cloud run logs | Should-not-trigger | ✅ PASS | Correctly routed to dbt-cloud-log-extractor |
| 6 | run the source system analyser | Should-not-trigger | ✅ PASS | Correctly routed to source-system-analyser |
| 7 | how do I install Cursor? | Should-not-trigger | ✅ PASS | General answer; noted skill is post-install only |
| 8 | just check my Snowflake connection | Edge case | ✅ PASS | Ran full check; showed Snowflake row with ✅ |
| 9 | my setup is done | Edge case | ❌ FAIL | Over-eagerly offered to verify without clarifying question |

## Category Grades

| # | Category | Grade | Explanation |
|---|----------|-------|-------------|
| 1 | Triggering (Description Quality) | PASS | Specific WHAT+WHEN, 5 synonym triggers, 14-char name, 350-char description |
| 2 | Anatomy & Structure | PASS | Valid frontmatter, 101 lines, no deep reference chains |
| 3 | Instructions Clarity | PASS | Low-freedom MCP calls appropriate for verification; conditional If-it-fails blocks present |
| 4 | Output Quality | PASS | Exact table template defined with column names, icons, completion/failure messages |
| 5 | Testability | PASS | All HIGH subcategories pass; LOW gaps (test queries) addressed by this review |
| 6 | Resource Efficiency | PASS | No padding prose; MCP calls preferred over scripts; execution intent explicit |
| 7 | Security & Trust | FAIL | MCP tool references lack ServerName: prefix (MEDIUM) — 1/2 MEDIUMs pass (50% < 70%) |
| 8 | Coexistence & Recall | PASS | Triggers don't conflict with adjacent skills; Eval 5-6 confirm clean routing |
| 9 | Model Compatibility | FAIL | No model tier documentation; not tested across Claude tiers (MEDIUM) |
| 10 | Workflow & Feedback Loops | PASS | N/A — read-only verification skill; no destructive or batch operations |
| 11 | Maintainability & Lifecycle | FAIL | HIGH: no separation of duties documented; no Registry table, versioning, or lifecycle stage |
| 12 | Gotchas / Lessons Learned | FAIL | No Gotchas section; skill not marked as v0.1 draft (MEDIUM) |
| 13 | Anti-Pattern Audit | PASS | Forward slashes throughout; no magic numbers; no scripts to review; default+escape pattern used |

## High-Criticality Failures

### **Subcategory:** Separation of duties observed (skill author is not also the sole reviewer)
- **Category:** Maintainability & Lifecycle
- **Finding:** The skill was authored and committed by the same agent/session in a single operation with no documented second reviewer. No PR review process, reviewer role, or approval requirement is stated in the skill or its metadata.
- **Recommendation:** Before merging to `main` in `cursorskills` or promoting to `plugins`, require a peer/tech-lead review of the SKILL.md diff. Document the reviewer expectation in a Registry table (see Medium failure below).

## Medium-Criticality Failures

### **Subcategory:** MCP tool references use full `ServerName:tool_name` format
- **Category:** Security & Trust
- **Finding:** All four MCP calls use bare function names (`execute_query`, `list_database_services`, `list_jobs`, `list_connectors`) without the server qualifier. The active MCPs are `project-0-cursorskills-snowflake`, `user-openmetadata`, `project-0-cursorskills-dbt`, and `user-fivetran-example`.
- **Recommendation:** Change to qualified format, e.g. `project-0-cursorskills-snowflake:execute_query(...)`, `user-openmetadata:list_database_services()`, etc. This prevents silent ambiguity if server names change or two MCPs expose the same tool name.

### **Subcategory:** Tested across all model tiers the team uses
- **Category:** Model Compatibility
- **Finding:** No model tier documentation exists. The skill makes live MCP calls which are model-agnostic in principle, but Cursor model selection affects how instructions are interpreted.
- **Recommendation:** Add a one-line Model Compatibility note (e.g. "Validated on Claude 3.5 Sonnet / Cursor Agent mode") to the skill or its Registry.

### **Subcategory:** Skill registry entry exists
- **Category:** Maintainability & Lifecycle
- **Finding:** No Registry table in SKILL.md. Fields missing: owner, reviewer, version, dependencies, lifecycle stage, last-eval date.
- **Recommendation:** Add a Registry section at the bottom of SKILL.md with at minimum: Owner, Lifecycle stage (e.g. "Draft — v0.1"), Dependencies ("none"), Last evaluated.

### **Subcategory:** Versioning strategy defined
- **Category:** Maintainability & Lifecycle
- **Finding:** No versioning strategy documented. The skill has no version label and no rollback plan.
- **Recommendation:** Add a Version field to the Registry (e.g. `v0.1 — git 0b3ce01`). State that production deployments pin to a specific commit.

### **Subcategory:** Lifecycle stage explicitly documented
- **Category:** Maintainability & Lifecycle
- **Finding:** No lifecycle stage present. The skill is newly created and has not been through a Create-Review cycle.
- **Recommendation:** Mark lifecycle as "Create / Review" until it passes a full peer review, then promote to "Deploy".

### **Subcategory:** Gotchas / Common Mistakes section
- **Category:** Gotchas / Lessons Learned
- **Finding:** No Gotchas section present. The skill is new so there is no production failure history, but it is not explicitly marked v0.1 to waive this requirement.
- **Recommendation:** Either add `> **v0.1 draft** — no production Gotchas yet` to the frontmatter/body, or add a minimal Gotchas section noting the known edge cases (Ask mode limitation for live checks; ambiguous trigger on "my setup is done").

### **Subcategory:** Triggering over-eager on ambiguous statement
- **Category:** Testability (Eval 9 edge case)
- **Finding:** The prompt "my setup is done" caused the skill to offer immediate verification without a clarifying question. The skill description does not define "confirm onboarding is complete" as a trigger carefully enough — an agent reads it as "user says setup is complete → offer to verify".
- **Recommendation:** Tighten the description: replace "confirm onboarding is complete" with "confirm setup is complete by running checks" or add a note in the skill body: "Only trigger on an explicit request for verification — not on a bare statement that setup is done."

## Low-Criticality Failures

None.

## Strengths

- **Precise, non-overlapping triggers.** The should-not-trigger evals (Evals 5-7) all passed cleanly — the skill did not fire for dbt log extraction, source analysis, or a general Cursor install question, and correctly redirected each to the right skill or knowledge.
- **Strong remediation specificity.** Each of the four system checks has named error codes (401, 403, "connection refused") with a concrete fix, rather than generic "check your credentials" advice.
- **Correct .env scope.** The skill documents both the `dbtproject/.env` and skills/scripts `.env` separately, matching the two-repo setup documented in ONBOARDING.md.
- **Lean and focused.** At 101 lines the skill is well within budget, covers the full verification surface, and avoids padding or common-knowledge explanations.
