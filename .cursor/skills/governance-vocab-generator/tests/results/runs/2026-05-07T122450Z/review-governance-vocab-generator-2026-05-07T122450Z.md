# Skill Review: governance-vocab-generator

**Reviewed at:** 2026-05-07T12:24:50Z | **Run:** 2026-05-07T122450Z

## Overall Verdict: FAIL

**Production-Ready Recommendation:** No

The governance-vocab-generator is a clean, well-scoped skill with clear output contracts, canonical dimension rules, and strong instruction clarity. However it fails on four categories: Testability (HIGH — the "return only the file path" output spec makes output quality unverifiable from response text), Model Compatibility, Maintainability & Lifecycle (no registry, versioning, or lifecycle stage documented), and Gotchas. These are all straightforward to fix and do not reflect design flaws in the core skill logic.

## Unit Tests: 3 / 8 passed

| # | Test | Type | Result | Notes |
|---|------|------|--------|-------|
| 1 | Generate retail banking governance vocabulary | Should-trigger | ❌ FAIL | Path returned correctly; canonical dimension names unverifiable from path-only response |
| 2 | Governance taxonomy for Bronze/Silver/Gold medallion platform | Should-trigger | ❌ FAIL | Architecture and B/S/G level selection unverifiable from path-only response |
| 3 | Classification framework for digital health catalog | Should-trigger | ❌ FAIL | Privacy selection unverifiable from path-only response |
| 4 | Governance vocabulary for legal document management | Should-trigger | ❌ FAIL | ComplianceLegal/Architecture decisions unverifiable from path-only response |
| 5 | Apply tags to OpenMetadata catalog tables | Should-not-trigger | ✅ PASS | Correctly routes to catalog-glossary-tagger |
| 6 | Build dbt SQL model for classification tracking | Should-not-trigger | ✅ PASS | Correctly routes to dbt-model-from-stm |
| 7 | IoT governance vocab with custom SensorType | Edge case | ❌ FAIL | SensorType 3-condition reasoning correct; Architecture inclusion for IoT not confirmed |
| 8 | HR governance vocab — include all 7 canonical dims | Edge case | ✅ PASS | Correctly evaluates each dimension; Architecture justified; file path returned |

## Category Grades

| # | Category | Grade | Notes |
|---|----------|-------|-------|
| 1 | Triggering (Description Quality) | ✅ PASS | What + when both present; specific trigger terms; name valid; description ~440 chars < 1024 |
| 2 | Anatomy & Structure | ✅ PASS | Valid YAML frontmatter; single file 150 lines; no reference hops |
| 3 | Instructions Clarity | ✅ PASS | Degrees of freedom appropriate for strict file-generation; canonical/non-canonical rules clearly signposted |
| 4 | Output Quality | ✅ PASS | Strict format explicitly defined; justified for data catalog consumption |
| 5 | Testability | ❌ FAIL | [HIGH] Output quality cannot be evaluated — "return only file path" spec makes content assertions unverifiable; no test infrastructure existed before this review |
| 6 | Resource Efficiency | ✅ PASS | No filler content; execution intent explicit ("Write a .md file to…; Return only the file path") |
| 7 | Security & Trust | ✅ PASS | No credentials, no scripts, no network access, no adversarial instructions |
| 8 | Coexistence & Recall | ✅ PASS | Trigger terms (generate/produce vocabulary) distinct from adjacent catalog-assignment skills |
| 9 | Model Compatibility | ❌ FAIL | [MEDIUM] No model tier documented; no compatibility notes |
| 10 | Workflow & Feedback Loops | ✅ PASS | N/A — text-file generation only; no fragile or destructive operations |
| 11 | Maintainability & Lifecycle | ❌ FAIL | [MEDIUM×3] No registry table, no versioning strategy, no lifecycle stage |
| 12 | Gotchas / Lessons Learned | ❌ FAIL | [MEDIUM] No Gotchas section |
| 13 | Anti-Pattern Audit | ✅ PASS | Forward slashes throughout; no magic constants; no scripts; no installs |

## High-Criticality Failures

### **Subcategory:** Output quality can be evaluated against assertions or rubric
- **Category:** Testability (5)
- **Finding:** The skill's "Return only the file path when done" instruction (output contract section, line 17) means every executor response is a single path string. Test assertions that check whether correct canonical dimension names were used, whether Architecture was correctly included/excluded, or whether non-canonical rules were applied cannot be verified from the response alone. 5 of 8 evals failed entirely because of this — not because the skill behaved incorrectly, but because the response doesn't expose what the skill did.
- **Recommendation:** Add a brief dimension summary line before the file path — e.g. `Dimensions included: Privacy, Criticality, ComplianceLegal, Retention` — that can be checked by automated assertions. Alternatively, document a `--verbose` invocation mode that shows the classification list, keeping the default output path-only for automation.

### **Subcategory:** Instruction-following can be evaluated
- **Category:** Testability (5)
- **Finding:** Same root cause — returning only the file path means the classifier's decision-making (3-condition test, Architecture inclusion/exclusion, level naming) is invisible and cannot be graded by the harness. Eval 7 partially passed only because the executor voluntarily added a SensorType note; this is not guaranteed.
- **Recommendation:** Same as above — expose dimension selection in the response so the grader can check rule adherence.

## Medium-Criticality Failures

### **Subcategory:** Tested across all model tiers the team uses
- **Category:** Model Compatibility (9)
- **Finding:** No model compatibility section or note in SKILL.md. Not documented which model tier this was validated on.
- **Recommendation:** Add a one-paragraph `## Model Compatibility` section (e.g. "Validated on Claude 3.5 Sonnet; text-only, no tool calls required, expected to work on all tiers").

### **Subcategory:** Skill registry entry exists
- **Category:** Maintainability & Lifecycle (11)
- **Finding:** No registry table in SKILL.md (owner, version, last-eval date, dependencies).
- **Recommendation:** Add a `## Registry` table matching the pattern in other skills in this repo.

### **Subcategory:** Versioning strategy defined
- **Category:** Maintainability & Lifecycle (11)
- **Finding:** No versioning or rollback strategy documented.
- **Recommendation:** Add a one-line note (e.g. "Pinned to `main`; changes via PR with peer review").

### **Subcategory:** Lifecycle stage explicitly documented
- **Category:** Maintainability & Lifecycle (11)
- **Finding:** No lifecycle stage (Plan / Create-Review / Test / Deploy / Monitor / Iterate-or-Deprecate) stated.
- **Recommendation:** Add lifecycle stage to the registry table.

### **Subcategory:** Has a "Gotchas" or "Common Mistakes" section
- **Category:** Gotchas / Lessons Learned (12)
- **Finding:** No Gotchas section. Given the skill's 3-condition non-canonical test and Architecture inclusion rule, there are likely failure modes worth documenting.
- **Recommendation:** Add a `## Gotchas` section covering: (a) common mistake of including Architecture for non-pipeline domains, (b) synonyms that must not be used in place of exact canonical names, (c) the path-only output making content validation require reading the file.

## Low-Criticality Failures

None.

## Strengths

- **Canonical dimension table** is concise and precise — exact names listed, no ambiguity about synonyms.
- **3-condition non-canonical test** is rigorous and prevents vocabulary bloat; well-documented with all three conditions clearly stated.
- **Architecture guidance** is specific and actionable — when to include, when Medallion labels are appropriate, what counts as "staged processing."
- **Output contract** is strict and unambiguous — exact markdown structure, separator rules, and return-path-only instruction leave no room for inconsistent outputs.
