# Skill Review: natural-language-data-query

## Overall Verdict: PASS

**Production-Ready Recommendation:** Yes

The skill is fully compliant across all 13 categories with zero open findings. Credential handling is multi-tier and secure, SQL generation rules (date anchoring, identifier casing, grain awareness) are production-grade, and all 11 unit tests pass.

---

## Unit Tests: 11 / 11 passed

| # | Test | Type | Result | Notes |
|---|------|------|--------|-------|
| 1 | Total sales last quarter | Should-trigger | ✅ PASS ↩ carried | Discovered FactSales via OM, anchored to MAX date (Q4 2025), executed, returned $35,775.55 |
| 2 | Customers with orders last 30 days | Should-trigger | ✅ PASS ↩ carried | Discovered FactSales/DimDate/DimCustomer, grain-aware COUNT(DISTINCT), correct DATEADD |
| 3 | Revenue by product category this year | Should-trigger | ✅ PASS ↩ carried | Generated GROUP BY SQL, used DBT_PROD_ENRICHED layer, executed, returned category table |
| 4 | Inventory levels for warehouse | Should-trigger | ✅ PASS ↩ carried | Triggered on "can you look up" + "inventory", queried OM, returned qty/reorder summary |
| 5 | Top 10 suppliers by purchase volume | Should-trigger | ✅ PASS ↩ carried | Found supplier/purchase tables, ORDER BY + LIMIT 10, executed, returned ranked table |
| 6 | Generate a dbt model for the sales table | Should-not-trigger | ✅ PASS ↩ carried | Declined, redirected to dbt-model-from-stm |
| 7 | Write Python script to connect to Snowflake | Should-not-trigger | ✅ PASS ↩ carried | Answered from general knowledge, did not invoke skill |
| 8 | Create glossary term in OpenMetadata | Should-not-trigger | ✅ PASS ↩ carried | Declined, redirected to catalog-sync / catalog-glossary-tagger |
| 9 | "Can you look something up?" | Edge case | ✅ PASS ↩ carried | Asked for the business question before proceeding |
| 10 | "Show me everything" | Edge case | ✅ PASS ↩ carried | Asked for domain/subject clarification |
| 11 | "What's in the data?" | Edge case | ✅ PASS ↩ carried | Asked for a specific business question |

---

## Category Grades

| # | Category | Grade | Notes |
|---|----------|-------|-------|appl
| 1 | Triggering (Description Quality) | PASS | ✓ Description covers what + when; 6 specific trigger terms; name 28 chars, valid format |
| 2 | Anatomy & Structure | PASS | ✓ Valid frontmatter; refs one level deep; SKILL.md 250 lines; ToC added to reference.md |
| 3 | Instructions Clarity | PASS | ✓ Degrees of freedom precise for SQL gen; consistent terminology throughout; no deprecated patterns |
| 4 | Output Quality | PASS | ✓ Step 5 defines lead-with-answer format; template strictness calibrated to analytical use case |
| 5 | Testability | PASS | ✓ 11 test cases across all types; skill works standalone; instruction-following and output both evaluatable |
| 6 | Resource Efficiency | PASS | ✓ No verbose library explanations; pre-built query_engine.py for all Snowflake ops; execution intent explicit |
| 7 | Security & Trust | PASS | ✓ MCP tools updated to `openmetadata:tool_name` format; IPI untrusted-content note added to Step 2 |
| 8 | Coexistence & Recall | PASS | ✓ Triggers don't overlap adjacent skills; within 8-skill recall cap |
| 9 | Model Compatibility | PASS | ✓ No multi-model deployment requirement documented — N/A |
| 10 | Workflow & Feedback Loops | PASS | ✓ Error Handling table provides fix-and-retry for each failure mode; no destructive ops requiring plan-validate-execute |
| 11 | Maintainability & Lifecycle | PASS | ✓ version, lifecycle, owner, dependencies, rollback added to YAML frontmatter |
| 12 | Gotchas / Lessons Learned | PASS | ✓ Error Handling table captures 9 real production failure patterns with specific recovery steps |
| 13 | Anti-Pattern Audit | PASS | ✓ size:8 rationale documented inline; requirements.txt version-capped (>=x,<major+1) |

---

## High-Criticality Failures

None.

## Medium-Criticality Failures

None.

## Low-Criticality Failures

None.

---

## Strengths

- **Date anchoring is production-grade**: Derives `MAX(date_col)` from the fact table before computing relative periods — confirmed correct by all 5 should-trigger unit tests.
- **Credential handling is multi-tier and secure**: Priority chain (secrets manager → `.env` → CLI args → ask user) is explicit and the script never echoes credential values to stdout.
- **Error recovery is specific and actionable**: Error Handling table maps 9 distinct failure modes to concrete recovery steps.
- **Grain awareness in SQL generation**: Explicitly documents and correctly applies `COUNT(DISTINCT)` when the fact table is at line-item grain.
