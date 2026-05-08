# Skill Review: catalog-vocab-publisher

## Overall Verdict: PASS

**Production-Ready Recommendation:** Yes with caveats

This is an incremental re-review of `catalog-vocab-publisher` (RUN_SLUG `2026-05-08T085526Z`). The previous run (`2026-05-08T083926Z`) achieved 13/13 category PASS but 10/13 unit tests due to three eval assertions that were misaligned with SKILL.md's Step 1 gating behavior. This run updates assertions for evals 2, 3, and 7 to correctly reflect gating. Two of the three previously-failing evals now PASS (evals 2 and 7); eval 3 remains FAIL because the executor response omits the idempotency acknowledgment that the updated assertion requires. All 13 rubric categories continue to PASS (12 carried from prior run; Category 5 Testability re-evaluated and confirmed PASS). The skill is production-ready with the caveat that the single remaining unit test failure (eval 3 idempotency assertion) should be addressed either by tightening the SKILL.md workflow instructions to explicitly prompt idempotency confirmation, or by adjusting the assertion to match achievable behavior.

## Unit Tests: 12 / 13 passed

| # | Test | Type | Result | Notes |
|---|------|------|--------|-------|
| 1 | Publish my governance vocabulary to OpenMetadata | Should-trigger | ✅ PASS | |
| 2 | Push my retail-governance-vocab.md to the data catalog | Should-trigger | ✅ PASS | Previously FAIL — new assertions aligned with Step 1 gating |
| 3 | Sync our governance tags — re-run the vocab publisher | Should-trigger | ❌ FAIL | Assertion "Response states or implies the script is idempotent / safe to re-run" not satisfied — executor asks for file path and env vars but omits idempotency mention |
| 4 | I got 401 Unauthorized when publishing the vocabulary | Should-trigger | ✅ PASS | |
| 5 | The publish script returned 403 Forbidden | Should-trigger | ✅ PASS | |
| 6 | Upload our governance framework classifications to OpenMetadata | Should-trigger | ✅ PASS | |
| 7 | Our tag descriptions have changed — push the updates | Should-trigger | ✅ PASS | Previously FAIL — new assertions aligned with Step 1 gating + UPDATED/SKIPPED logic |
| 8 | Push vocabulary to OpenMetadata, publish classifications | Should-trigger | ✅ PASS | |
| 9 | Generate a governance vocabulary for the retail domain | Should-not-trigger | ✅ PASS | |
| 10 | Run a metadata ingestion from Snowflake into OpenMetadata | Should-not-trigger | ✅ PASS | |
| 11 | Publish my vocab | Edge case | ✅ PASS | |
| 12 | I want to publish but haven't set up the service account yet | Edge case | ✅ PASS | |
| 13 | I get SSL errors when trying to publish | Edge case | ✅ PASS | |

## Category Grades

| # | Category | Grade | Notes |
|---|----------|-------|-------|
| 1 | Triggering (Description Quality) | PASS | ✓ carried from 2026-05-08T083926Z |
| 2 | Anatomy & Structure | PASS | ✓ carried from 2026-05-08T083926Z |
| 3 | Instructions Clarity | PASS | ✓ carried from 2026-05-08T083926Z |
| 4 | Output Quality | PASS | ✓ carried from 2026-05-08T083926Z |
| 5 | Testability | PASS | Re-evaluated: test suite covers all documented branches; HIGH subcategories (isolation, instruction-following, output quality evaluability) all pass; LOW subcategories pass |
| 6 | Resource Efficiency | PASS | ✓ carried from 2026-05-08T083926Z |
| 7 | Security & Trust | PASS | ✓ carried from 2026-05-08T083926Z |
| 8 | Coexistence & Recall | PASS | ✓ carried from 2026-05-08T083926Z |
| 9 | Model Compatibility | PASS | ✓ carried from 2026-05-08T083926Z |
| 10 | Workflow & Feedback Loops | PASS | ✓ carried from 2026-05-08T083926Z |
| 11 | Maintainability & Lifecycle | PASS | ✓ carried from 2026-05-08T083926Z |
| 12 | Gotchas / Lessons Learned | PASS | ✓ carried from 2026-05-08T083926Z |
| 13 | Anti-Pattern Audit | PASS | ✓ carried from 2026-05-08T083926Z |

## High-Criticality Failures

None.

## Medium-Criticality Failures

None.

## Low-Criticality Failures

None.

## Unit Test Failure Detail

### Eval 3: "Sync our governance tags — re-run the vocab publisher"

- **Failing assertion:** "Response states or implies the script is idempotent / safe to re-run"
- **Evidence:** The executor correctly applies Step 1 gating (asks for file path, confirms env vars) but does not mention that the publisher is idempotent or safe to re-run. SKILL.md Workflow section states "The script is **idempotent** — safe to re-run at any time" but this information is not surfaced when the user explicitly requests a re-run.
- **Recommendation:** Either (a) add an explicit instruction in SKILL.md Workflow Step 1 to acknowledge idempotency when the user signals a re-run intent, or (b) relax the assertion to remove the idempotency requirement and focus solely on the Step 1 gating behavior (file path + env var confirmation).

## Comparator Summary (Step 3c)

SKILL.md is identical between current tree and prior snapshot (`2026-05-08T083926Z`). Only test files changed. All 13 comparisons returned **tie** — no regression in skill behavior.

| Metric | Value |
|--------|-------|
| New wins | 0 |
| Old wins | 0 | 
| Ties | 13 |
| Overall | tie |

## Strengths

- **Strong trigger precision.** The description uses specific action verbs (publish, push, sync, upload) and the Coexistence & Routing table clearly delineates adjacent skills — misrouting evals 9 and 10 pass cleanly.
- **Thorough error-handling coverage.** Dedicated workflow steps for 401 and 403 errors with exact navigation paths (Settings → Bots → vocab-publisher-bot) make troubleshooting deterministic.
- **Comprehensive Gotchas section.** Five documented gotchas (bot policy, HTTP/HTTPS, PowerShell, token expiry, .env git-ignore) directly address the most common failure modes.
- **Security posture.** Credential security section, .env exclusion, and OM_TOKEN-only auth (no username/password flags) demonstrate solid security hygiene throughout.
