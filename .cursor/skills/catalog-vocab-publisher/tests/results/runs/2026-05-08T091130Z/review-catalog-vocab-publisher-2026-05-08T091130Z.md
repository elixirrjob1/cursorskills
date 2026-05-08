# Skill Review: catalog-vocab-publisher

_Reviewed at: 2026-05-08T09:11:30Z UTC · Run slug: 2026-05-08T091130Z · Incremental re-check (RUN\_STEP\_3C=true — SKILL.md Step 1 updated)_

## Overall Verdict: PASS

**Production-Ready Recommendation:** Yes

This skill is production-ready. It has a tight, well-documented scope (publish a governance vocabulary `.md` file to OpenMetadata Classifications and Tags), clear step-by-step workflow with explicit gating, comprehensive error-handling steps for 401/403/SSL failures, a Gotchas section, and a full security credential policy. The incremental change in this review — adding an explicit idempotency acknowledgment to Workflow Step 1 for re-run signals — directly resolves the single remaining unit-test failure from run `2026-05-08T085526Z` (eval 3, assertion 1). All 13 unit tests now pass and all 13 rubric categories are PASS.

## Unit Tests: 13 / 13 passed

| # | Test | Type | Result | Notes |
|---|------|------|--------|-------|
| 1 | Publish governance vocabulary to OpenMetadata | Should-trigger | ✅ PASS | Step 1 gating: confirms file path + env vars; no --username/--password |
| 2 | Push retail-governance-vocab.md to data catalog | Should-trigger | ✅ PASS | File path acknowledged from prompt; env-var gate before command |
| 3 | Sync governance tags — re-run the vocab publisher | Should-trigger | ✅ PASS | New Step 1: proactively acknowledges idempotency on re-run signal; asks for file path |
| 4 | 401 Unauthorized when publishing vocabulary | Should-trigger | ✅ PASS | Step 4: token expiry/revocation → regenerate under Settings → Bots → vocab-publisher-bot |
| 5 | 403 Forbidden from publish script | Should-trigger | ✅ PASS | Step 5: missing write permissions → VocabPublisherPolicy; Settings → Bots → Roles |
| 6 | Upload governance framework classifications | Should-trigger | ✅ PASS | Step 1 gating: asks for file path; shows publish_vocab.py; does not generate vocab |
| 7 | Tag descriptions changed — push updates | Should-trigger | ✅ PASS | Step 1 gating: asks for file path; UPDATED/SKIPPED workflow referenced; no deletion |
| 8 | Push vocabulary / publish classifications (exact phrase) | Should-trigger | ✅ PASS | Exact trigger phrase match; proceeds with publish workflow; no generate clarification |
| 9 | Generate governance vocabulary for retail domain | Should-not-trigger | ✅ PASS | Correctly routes to governance-vocab-generator; does not invoke publish script |
| 10 | Run metadata ingestion from Snowflake into OpenMetadata | Should-not-trigger | ✅ PASS | Correctly routes to catalog-sync; does not reference vocab file or publish script |
| 11 | "Publish my vocab" (no file path) | Edge case | ✅ PASS | Stops to ask for .md file path; checks OM env vars; does not run script without path |
| 12 | Publish but service account not set up | Edge case | ✅ PASS | Surfaces Prerequisites section; references vocab-publisher-bot; stops at Step 1 |
| 13 | SSL errors when publishing | Edge case | ✅ PASS | Cites HTTP vs HTTPS Gotcha; advises OM_BASE_URL scheme correction |

## Category Grades

| # | Category | Grade | Notes |
|---|----------|-------|-------|
| 1 | Triggering (Description Quality) | PASS | Description covers what/when/how with specific trigger terms; valid name; under 1024 chars ↩ carried |
| 2 | Anatomy & Structure | PASS | Valid YAML frontmatter; SKILL.md ~157 lines; logical folder structure (scripts/, reference.md) ↩ carried |
| 3 | Instructions Clarity | PASS | Step 1 re-evaluated: new idempotency conditional adds a clearly signposted decision point; degrees of freedom well-calibrated for this low-fragility terminal task |
| 4 | Output Quality | PASS | Script output template provided with concrete CREATED/UPDATED/SKIPPED format; summary table example included ↩ carried |
| 5 | Testability | PASS | Re-evaluated: 13 evals across 3 types; all HIGH subcategories pass; new Step 1 instruction is directly testable via eval 3 (idempotency assertion) |
| 6 | Resource Efficiency | PASS | Pre-built Python script used; no common-knowledge explanations; execution intent explicit ↩ carried |
| 7 | Security & Trust | PASS | No hardcoded credentials; .env git-ignored; script uses env vars only; OM_TOKEN never logged; audit trail through script output ↩ carried |
| 8 | Coexistence & Recall | PASS | Coexistence & Routing table explicitly differentiates from 4 adjacent skills; trigger precision documented ↩ carried |
| 9 | Model Compatibility | PASS | `validated_on: Claude Sonnet 4.6` in frontmatter; model noted in Registry ↩ carried |
| 10 | Workflow & Feedback Loops | PASS | Validate-fix-repeat covered via Steps 3–5 (output review, 401/403 handling); plan-gate-execute via Step 1 ↩ carried |
| 11 | Maintainability & Lifecycle | PASS | Registry section present with owner, reviewer sep-of-duties, versioning policy, rollback plan, lifecycle stage, last-reviewed date ↩ carried |
| 12 | Gotchas / Lessons Learned | PASS | Five documented gotchas: bot role vs policy, HTTP vs HTTPS, PowerShell env loading, token expiry, .env git-ignored ↩ carried |
| 13 | Anti-Pattern Audit | PASS | All forward slashes; requirements.txt pins requests==2.33.1; no magic constants; script handles errors; no multi-option paralysis ↩ carried |

## High-Criticality Failures

None.

## Medium-Criticality Failures

None.

## Low-Criticality Failures

None.

## Comparator Summary (Step 3c)

SKILL.md changed vs prior snapshot (`2026-05-08T085526Z`): one line diff in Workflow Step 1 (idempotency acknowledgment added for re-run signals). All 13 evals re-run.

| Eval | Prompt | Verdict | Reasoning |
|------|--------|---------|-----------|
| 1 | Publish my governance vocabulary... | tie | Step 1 change not triggered; behavior identical |
| 2 | Push my retail-governance-vocab.md... | tie | No re-run signal in prompt; behavior identical |
| 3 | Sync governance tags — re-run the vocab publisher | **new_wins** | New SKILL.md proactively acknowledges idempotency; old SKILL.md only asked for file path — silent on re-run safety |
| 4 | I got 401 Unauthorized... | tie | 401 handling (Step 4) unchanged |
| 5 | 403 Forbidden... | tie | 403 handling (Step 5) unchanged |
| 6 | Upload our governance framework... | tie | No re-run signal; behavior identical |
| 7 | Our tag descriptions have changed... | tie | No re-run signal; behavior identical |
| 8 | Push vocabulary / publish classifications | tie | No re-run signal; behavior identical |
| 9 | Generate a governance vocabulary... | tie | Should-not-trigger; routing unaffected |
| 10 | Run metadata ingestion from Snowflake... | tie | Should-not-trigger; routing unaffected |
| 11 | Publish my vocab | tie | Edge case; no re-run signal |
| 12 | Publish but no service account set up | tie | Edge case; no re-run signal |
| 13 | SSL errors when publishing | tie | Edge case; no re-run signal |

**Overall comparator result: new_wins** (1 new_wins, 0 old_wins, 12 ties / 13 total)

## Strengths

- **Laser-focused scope** — the skill does exactly one thing (publish vocab to OM), with no scope creep, clear coexistence routing, and explicit non-trigger documentation.
- **Complete error-handling coverage** — Steps 4 and 5 provide concrete remediation paths for the two most common API failures (401, 403), and the Gotchas section covers SSL, token expiry, and PowerShell quirks.
- **Idempotency-first design** — the publish script's create/update/skip logic is documented at multiple levels (Workflow Step 3 output example, "What the script does", Gotchas, and now Step 1 gating for re-run signals), giving users full confidence to re-run safely.
- **Strong credential security posture** — explicit policy against logging OM_TOKEN, .env git-ignored, CI/CD injection pattern documented, combined with a dedicated Credential Security section.
