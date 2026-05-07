# Benchmark Report: governance-vocab-generator

_Generated: 2026-05-07T12:24:50Z UTC · First review run — full eval suite_

## Summary

| Metric | Value |
|--------|-------|
| Overall Verdict | **FAIL** |
| Unit Tests | 3 / 8 passed (38%) |
| Assertions | 16 / 24 (67%) |
| Categories | 9 / 13 passed |
| High Failures | 1 (Testability — return-only-path output contract) |
| Medium Failures | 4 (Model Compat, Maintainability ×3, Gotchas) |
| Comparator | — (first review, no prior snapshot) |

## Unit Test Results

| # | Test | Type | Result | Assertions | Evidence |
|---|------|------|--------|------------|----------|
| 1 | Generate retail banking governance vocabulary | should-trigger | ❌ FAIL | 1/3 | Path returned; canonical dimension names unverifiable from path-only response |
| 2 | Governance taxonomy for Bronze/Silver/Gold medallion platform | should-trigger | ❌ FAIL | 1/3 | Architecture and B/S/G level selection unverifiable from path-only response |
| 3 | Classification framework for digital health catalog | should-trigger | ❌ FAIL | 2/3 | Privacy selection unverifiable from path-only response |
| 4 | Governance vocabulary for legal document management | should-trigger | ❌ FAIL | 1/3 | ComplianceLegal/Architecture decisions unverifiable from path-only response |
| 5 | Apply tags to OpenMetadata catalog tables | should-not-trigger | ✅ PASS | 3/3 | Correctly routes to catalog-glossary-tagger |
| 6 | Build dbt SQL model for classification tracking | should-not-trigger | ✅ PASS | 3/3 | Correctly routes to dbt-model-from-stm |
| 7 | IoT governance vocab with custom SensorType | edge-case | ❌ FAIL | 2/3 | Architecture inclusion for IoT not confirmed from path-only response |
| 8 | HR governance vocab — include all 7 canonical dims | edge-case | ✅ PASS | 3/3 | Privacy justified; Architecture included; path returned |

## Assertion Detail

| Eval | Assertion | Passed | Evidence |
|------|-----------|--------|----------|
| 1 | Response contains a file path matching governance-vocabularies/*.md | ✅ | Path returned correctly |
| 1 | Response uses exact canonical dimension names | ❌ | Path-only response — dimension names not visible |
| 1 | Response does NOT contain tables or JSON in the output | ✅ | Path-only; no tables |
| 2 | Response includes Architecture classification or mentions it as selected | ❌ | Path-only — Architecture selection invisible |
| 2 | Response references Bronze, Silver, or Gold as level names | ❌ | Path-only — level names invisible |
| 2 | Response output path follows governance-vocabularies/<slug>-governance-vocab.md | ✅ | Path format correct |
| 3 | Response includes Privacy classification or explicitly mentions Privacy as selected | ❌ | Path-only — Privacy selection invisible |
| 3 | Response returns a file path to governance-vocabularies/ | ✅ | Path returned |
| 3 | Response does NOT contain lengthy prose as the primary output | ✅ | Path-only; no prose |
| 4 | Response includes ComplianceLegal classification or notes it as applicable | ❌ | Path-only — ComplianceLegal invisible |
| 4 | Response does NOT include Architecture, OR explicitly notes Architecture is omitted | ❌ | Path-only — Architecture decision invisible |
| 4 | Response returns a file path | ✅ | Path returned |
| 5 | Response does NOT produce a governance-vocabularies/ file path as main deliverable | ✅ | Correctly redirected |
| 5 | Response references catalog-glossary-tagger or catalog-sync | ✅ | Routes to catalog-glossary-tagger |
| 5 | Response does NOT output classification dimensions | ✅ | No vocabulary output |
| 6 | Response references dbt, SQL, Snowflake, or dbt-model-from-stm | ✅ | Routes to dbt-model-from-stm |
| 6 | Response does NOT generate a governance vocabulary markdown file | ✅ | No vocabulary output |
| 6 | Response does NOT output ## Classification headed sections | ✅ | No classification sections |
| 7 | Response does NOT include SensorType, OR explicitly rejects it | ✅ | SensorType 3-condition reasoning correct |
| 7 | Response includes Architecture or acknowledges it is relevant for IoT | ❌ | Path-only — Architecture decision for IoT invisible |
| 7 | Response applies canonical-first evaluation before considering SensorType | ✅ | Canonical-first reasoning confirmed |
| 8 | Response includes Privacy classification | ✅ | Privacy included (HR has personal data) |
| 8 | Response does NOT blindly include all 7 dims without evaluation | ✅ | Correctly evaluates each dimension |
| 8 | Response returns a file path | ✅ | Path returned |

## Category Grades

| # | Category | Grade | Explanation |
|---|----------|-------|-------------|
| 1 | Triggering (Description Quality) | PASS | What + when both present; specific trigger terms; name valid; description ~440 chars |
| 2 | Anatomy & Structure | PASS | Valid YAML frontmatter; single file 150 lines; no reference hops |
| 3 | Instructions Clarity | PASS | Degrees of freedom appropriate; canonical/non-canonical rules clearly signposted |
| 4 | Output Quality | PASS | Strict format explicitly defined; justified for data catalog consumption |
| 5 | Testability | FAIL | [HIGH] "Return only file path" output contract makes content assertions unverifiable; 5/8 evals fail because response doesn't expose dimension selection |
| 6 | Resource Efficiency | PASS | No filler content; execution intent explicit |
| 7 | Security & Trust | PASS | No credentials, no scripts, no network access |
| 8 | Coexistence & Recall | PASS | Trigger terms distinct from adjacent catalog-assignment skills |
| 9 | Model Compatibility | FAIL | [MEDIUM] No model tier documented; no compatibility notes |
| 10 | Workflow & Feedback Loops | PASS | N/A — text-file generation; no fragile or destructive operations |
| 11 | Maintainability & Lifecycle | FAIL | [MEDIUM×3] No registry table, no versioning strategy, no lifecycle stage |
| 12 | Gotchas / Lessons Learned | FAIL | [MEDIUM] No Gotchas section; Architecture inclusion and canonical-name synonyms are likely failure modes |
| 13 | Anti-Pattern Audit | PASS | Forward slashes throughout; no magic constants; no scripts; no install commands |

## High-Criticality Findings

**Cat 5 — Output quality cannot be evaluated against assertions**
The "Return only the file path when done" instruction makes content assertions unverifiable. 5 of 8 evals failed because the dimension selection (Privacy, Architecture, canonical names, level labels) is invisible in a path-only response. Recommendation: add a brief dimension summary line before the path, or document a `--verbose` mode.

## History

| reviewed_at | date | Unit Tests | Assertions | Categories | Verdict | Notes |
|-------------|------|------------|------------|------------|---------|-------|
| 2026-05-07T12:24:50Z | 2026-05-07 | 3/8 | 16/24 | 9/13 | FAIL | First review — testability HIGH failure; cats 9/11/12 FAIL |
