# Skill Review: catalog-sync

## Overall Verdict: FAIL

**Production-Ready Recommendation:** No

The skill is strong on OpenMetadata orchestration, credential hygiene, and the reviewed onboard plan/apply/rollback path — and executor subagents followed MCP workflows reliably for most triggers. It does not yet meet production bar because maintainability metadata (registry, lifecycle, model notes) and a Gotchas section are missing from `SKILL.md`, MCP tools are not named in `ServerName:tool_name` form, and two edge-case evals failed (duplicate-service scenario and vague “fix my catalog”).

## Unit Tests: 10 / 12 passed

| # | Test | Type | Result | Notes |
|---|------|------|--------|-------|
| 1 | Postgres sync + PII tag | Should-trigger | ✅ PASS | MCP tools + tag verification |
| 2 | Snowflake sync + inspect | Should-trigger | ✅ PASS | Ingestion + list schemas/tables |
| 3 | MySQL first-time onboard | Should-trigger | ✅ PASS | Interview + plan/apply route |
| 4 | Existing Postgres re-run | Should-trigger | ✅ PASS | Reused service, run pipeline |
| 5 | Glossary on orders | Should-trigger | ✅ PASS | Glossary assign + confirm |
| 6 | New Oracle registration | Should-trigger | ✅ PASS | Onboard interview + password_env |
| 7 | Vocabulary publish | Should-not-trigger | ✅ PASS | Routed to catalog-vocab-publisher |
| 8 | AI glossary mapping | Should-not-trigger | ✅ PASS | Routed to catalog-glossary-tagger |
| 9 | dbt lineage import | Should-not-trigger | ✅ PASS | Routed to governance-import-dbt-lineage |
| 10 | Password env only | Edge case | ✅ PASS | password_env, no secret in chat |
| 11 | Duplicate service onboard | Edge case | ❌ FAIL | Agent reported 404 (not exists), not hard-stop wording |
| 12 | Vague fix catalog | Edge case | ❌ FAIL | Ran ingestion before clarifying intent |

## Category Grades

| # | Category | Grade | Notes |
|---|----------|-------|-------|
| 1 | Triggering (Description Quality) | PASS | Clear what/when; name `catalog-sync` valid |
| 2 | Anatomy & Structure | PASS | 252 lines; valid frontmatter; apply scripts at repo `scripts/` |
| 3 | Instructions Clarity | PASS | Onboard vs workflow routes well signposted |
| 4 | Output Quality | PASS | Plan JSON schema and workflow steps explicit |
| 5 | Testability | PASS | Eval suite + fixture pytest for apply guardrails |
| 6 | Resource Efficiency | PASS | Uses `catalog_onboard_apply.py` / rollback scripts |
| 7 | Security & Trust | PASS | password_env pattern; script guardrails; no secrets in chat |
| 8 | Coexistence & Recall | PASS | Adjacent skills documented in peer skills |
| 9 | Model Compatibility | FAIL | No validated-model or tier notes in SKILL.md |
| 10 | Workflow & Feedback Loops | PASS | Plan-apply-rollback; re-read after assign |
| 11 | Maintainability & Lifecycle | FAIL | No Registry / lifecycle / versioning block in skill |
| 12 | Gotchas / Lessons Learned | FAIL | Guardrails present but no Gotchas / Common Mistakes section |
| 13 | Anti-Pattern Audit | PASS | Forward slashes; explicit timeouts; script `_die` |

## High-Criticality Failures

None.

## Medium-Criticality Failures

### **Subcategory:** MCP tool references use full `ServerName:tool_name` format
- **Category:** Security & Trust
- **Finding:** Workflow lists bare tool names (`test_connection`, `run_ingestion_pipeline`) without server prefix (e.g. `user-openmetadata:test_connection`).
- **Recommendation:** Prefix each MCP tool in SKILL.md with the registered server name from Cursor MCP config.

### **Subcategory:** Skill registry entry exists
- **Category:** Maintainability & Lifecycle
- **Finding:** `SKILL.md` has no Registry table (owner, version, last-eval, dependencies).
- **Recommendation:** Add a Registry section matching other production skills (e.g. skill-reviewer pattern).

### **Subcategory:** Lifecycle stage explicitly documented
- **Category:** Maintainability & Lifecycle
- **Finding:** No Plan / Deploy / Monitor lifecycle statement.
- **Recommendation:** Document stage (e.g. **Test / Deploy**) and last review `run_slug`.

### **Subcategory:** Versioning strategy defined
- **Category:** Maintainability & Lifecycle
- **Finding:** No pin/rollback guidance for skill consumers.
- **Recommendation:** State that skill tracks repo `main` and onboard scripts are versioned with the monorepo.

### **Subcategory:** Tested across model tiers / documented models
- **Category:** Model Compatibility
- **Finding:** No Model Compatibility section.
- **Recommendation:** Note validated tiers (e.g. Sonnet for MCP-heavy runs, Haiku for routing-only).

### **Subcategory:** Has a "Gotchas" or "Common Mistakes" section
- **Category:** Gotchas / Lessons Learned
- **Finding:** Guardrails are thorough but no operational gotchas (e.g. pipeline FQN vs short name, filter merge on patch).
- **Recommendation:** Add Gotchas citing eval-1 note on `run_ingestion_pipeline` FQN and extend-pipeline merge rule.

## Low-Criticality Failures

### **Subcategory:** Documented which models the skill is validated on
- **Category:** Model Compatibility
- **Finding:** Same as medium model-compat gap.
- **Recommendation:** Fold into Model Compatibility section.

### **Subcategory:** Coexistence table inside catalog-sync SKILL.md
- **Category:** Coexistence & Recall
- **Finding:** Routing to vocab-publisher / glossary-tagger appears only in peer skills, not in catalog-sync.
- **Recommendation:** Add short “Adjacent skills” table (as catalog-vocab-publisher does).

## Strengths

- Credential handling is exemplary: `password_env` / `env:` patterns, grep-without-printing-values, and apply-script refusal of literal passwords.
- Onboard route implements plan → apply → rollback with `existing_service_fqns_snapshot` and annotation checks — strong plan-validate-execute pattern.
- Standard workflow is MCP-first with explicit tool sequence and confirmation after tagging.
- Repo-level pytest fixtures (`tests/test_apply_script.py`) statically enforce guardrail functions in apply/rollback scripts.
