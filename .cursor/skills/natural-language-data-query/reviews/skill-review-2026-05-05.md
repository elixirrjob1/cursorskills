# Skill Review: natural-language-data-query

**Reviewed:** 2026-05-05  
**Reviewer:** Cursor Agent (skill-reviewer v1.0)  
**Skill path:** `.cursor/skills/natural-language-data-query/`

---

## Overall Verdict: FAIL

**Production-Ready Recommendation:** No

The skill is technically sophisticated and operationally well-designed — clear 5-step workflow, solid error handling, good security posture on credentials, and pre-built scripts for the fragile Snowflake operations. However it fails on testability (no test cases mean triggering accuracy can't be validated), coexistence (not tested alongside the active skill set), and several maintenance/lifecycle criteria. Nine of thirteen categories fail, almost all due to missing governance artefacts rather than broken logic.

---

## Category Grades

| # | Category | Grade |
|---|----------|-------|
| 1 | Triggering (Description Quality) | FAIL |
| 2 | Anatomy & Structure | FAIL |
| 3 | Instructions Clarity | PASS |
| 4 | Output Quality | FAIL |
| 5 | Testability | FAIL |
| 6 | Resource Efficiency | PASS |
| 7 | Security & Trust | FAIL |
| 8 | Coexistence & Recall | FAIL |
| 9 | Model Compatibility | FAIL |
| 10 | Workflow & Feedback Loops | FAIL |
| 11 | Maintainability & Lifecycle | FAIL |
| 12 | Gotchas / Lessons Learned | PASS |
| 13 | Anti-Pattern Audit | PASS |

---

## High-Criticality Failures

### **Subcategory:** Triggering accuracy can be evaluated from supplied test cases
- **Category:** Testability
- **Finding:** No test cases exist anywhere in the skill folder. There is no way to verify the description triggers correctly, doesn't fire on adjacent prompts (e.g. "generate a SQL migration"), or handles edge cases like vague questions.
- **Recommendation:** Add a `## Test Cases` section or `references/test-cases.md` with at least 3–5 queries covering: should-trigger (e.g. "what were total sales last quarter"), should-not-trigger (e.g. "write me a Python script"), and edge cases (e.g. "can you look something up for me").

### **Subcategory:** Tested alongside the active skill set, not just in isolation
- **Category:** Coexistence & Recall
- **Finding:** No documentation that the skill has been tested alongside other skills in the repo (e.g. `source-system-analyser`, `catalog-sync`). Broad triggers like "show me data about" and "report" risk firing on prompts that other skills should handle.
- **Recommendation:** Document which adjacent skills were checked for trigger overlap and confirm the skill doesn't degrade them. Add a brief coexistence note to the skill.

---

## Medium-Criticality Failures

### **Subcategory:** Description written in third person
- **Category:** Triggering (Description Quality)
- **Finding:** Description opens with "Answer business users'…" — imperative form. Should be "Answers business users'…" to conform to third-person convention.
- **Recommendation:** Change "Answer" → "Answers" at the start of the description.

### **Subcategory:** Reference files over 100 lines include a table of contents
- **Category:** Anatomy & Structure
- **Finding:** `reference.md` is 204 lines covering OpenMetadata MCP tools, Snowflake patterns, secrets key names, and three provider setup guides — no table of contents.
- **Recommendation:** Add a ToC at the top of `reference.md` linking to each H2 section.

### **Subcategory:** Input/output example pairs included
- **Category:** Output Quality
- **Finding:** The workflow shows one example MCP call but no end-to-end example of what the final response to the business user looks like — no sample table, no sample direct-answer sentence, no example assumption statement.
- **Recommendation:** Add a short example at the end of Step 5 showing a complete sample response for a simple question like "How many orders last month?".

### **Subcategory:** MCP tool references use full `ServerName:tool_name` format
- **Category:** Security & Trust
- **Finding:** MCP tools are called as bare names throughout: `search_metadata`, `semantic_search`, `get_entity_details`, `get_entity_lineage`. Without the server prefix, the agent may call the wrong tool if multiple MCP servers expose similarly-named tools.
- **Recommendation:** Replace all bare tool names with the fully qualified format: `openmetadata:search_metadata`, `openmetadata:semantic_search`, `openmetadata:get_entity_details`, `openmetadata:get_entity_lineage`.

### **Subcategory:** Tested across all model tiers
- **Category:** Model Compatibility
- **Finding:** No mention of which models the skill has been validated on. The skill makes complex multi-step MCP + script calls that may behave differently across model tiers.
- **Recommendation:** Add a one-line note documenting validated models (e.g. "Validated on: Sonnet 3.5. Not tested on Haiku.").

### **Subcategory:** Copyable checklist provided for multi-step workflows
- **Category:** Workflow & Feedback Loops
- **Finding:** The 5-step workflow is clearly numbered but has no copyable checkbox checklist to track progress across a session.
- **Recommendation:** Add a checklist block at the top of the Workflow section (e.g. `- [ ] Step 1: Credentials + question` through `- [ ] Step 5: Present results`).

### **Subcategory:** Skill registry entry / versioning / lifecycle stage
- **Category:** Maintainability & Lifecycle
- **Finding:** No registry entry (owner, version, dependencies, last-eval date), no versioning strategy, and no lifecycle stage documented anywhere in the skill folder.
- **Recommendation:** Add a `## Skill Metadata` block to SKILL.md or a `registry.md` file capturing: owner, current version, dependencies (`openmetadata MCP`, `query_engine.py`, `snowflake-connector-python`), and lifecycle stage.

---

## Low-Criticality Failures

### **Subcategory:** At least 3-5 representative test queries provided
- **Category:** Testability
- **Finding:** No test queries anywhere in the skill. Same fix as the High finding above.

### **Subcategory:** Coexistence behavior can be evaluated
- **Category:** Testability
- **Finding:** No documentation of which adjacent skills were reviewed for trigger overlap. Same fix as the High finding above.

### **Subcategory:** No voodoo constants
- **Category:** Anti-Pattern Audit
- **Finding:** `size: 8` (Step 2a keyword search) and `size=5` (Step 2b semantic search) have no documented rationale. `LIMIT 1000` is partially justified but the number itself is undocumented.
- **Recommendation:** Add inline comments explaining each magic number (e.g. "`size: 8` — enough candidates to cover multiple schemas without flooding context").

---

## Strengths

- **Credential security is exemplary** — three-tier resolution order (secrets manager → `.env` → ask user), explicit guidance to never log or display tokens, and `.env` gitignore guidance throughout.
- **Pre-built scripts for all fragile Snowflake operations** — `query_engine.py` handles both identifier resolution and SQL execution, keeping generated code out of the skill entirely.
- **Error handling is comprehensive and production-tested** — the error table covers 10 specific failure modes with concrete remediation steps per error, clearly drawn from real-world use.
- **Layer preference logic is well-designed** — Gold → Silver → Bronze decision tree driven by metadata tags (not naming conventions) is the right architecture for multi-client environments.
