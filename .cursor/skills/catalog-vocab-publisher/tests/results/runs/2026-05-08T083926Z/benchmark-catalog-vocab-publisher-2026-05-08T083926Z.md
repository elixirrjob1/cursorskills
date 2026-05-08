# Benchmark Report: catalog-vocab-publisher
_Generated: 2026-05-08T08:39:26Z UTC · Incremental re-check vs 2026-05-08T081053Z — all 4 reported fixes verified; Cat 11 re-evaluated; RUN_STEP_3C=true (SKILL.md changed)_

## Summary

| Metric | Value |
|--------|-------|
| Overall Verdict | PASS |
| Unit Tests | 10 / 13 passed (77%) |
| Assertions | 35 / 39 (90%) |
| Categories | 13 / 13 passed |
| High Failures | — |
| Medium Failures | — |
| Comparator | tie (13/13 evals — SKILL.md governance metadata changes only; no behavioral diff) |

## Unit Test Results

| # | Test | Type | Result | Assertions |
|---|------|------|--------|------------|
| 1 | Basic publish trigger | should-trigger | ✅ PASS | 3/3 |
| 2 | Named file push with JWT auth | should-trigger | ❌ FAIL | 2/3 |
| 3 | Idempotent re-run | should-trigger | ❌ FAIL | 2/3 |
| 4 | 401 error handling | should-trigger | ✅ PASS | 3/3 |
| 5 | 403 error handling | should-trigger | ✅ PASS | 3/3 |
| 6 | Upload framework classifications | should-trigger | ✅ PASS | 3/3 |
| 7 | Push updated tag descriptions | should-trigger | ❌ FAIL | 1/3 |
| 8 | Exact trigger phrase | should-trigger | ✅ PASS | 3/3 |
| 9 | Should-not-trigger: vocab generation | should-not-trigger | ✅ PASS | 3/3 |
| 10 | Should-not-trigger: Snowflake ingestion | should-not-trigger | ✅ PASS | 3/3 |
| 11 | Edge: no file path given | edge-case | ✅ PASS | 3/3 |
| 12 | Edge: service account not set up | edge-case | ✅ PASS | 3/3 |
| 13 | Edge: SSL errors | edge-case | ✅ PASS | 3/3 |

## Assertion Detail

| Eval | Assertion | Passed | Evidence |
|------|-----------|--------|----------|
| 1 | Response asks for or confirms the .md vocabulary file path | ✅ Yes | Agent asks for the vocabulary file path per SKILL.md Step 1. |
| 1 | Response references OM_BASE_URL and/or OM_TOKEN environment variables | ✅ Yes | SKILL.md Step 1 still requires verifying OM_BASE_URL and OM_TOKEN; agent confirms both. |
| 1 | Response does NOT use --username / --password flags | ✅ Yes | Script uses env vars only; no --username or --password. Auth model consistent across SKILL.md, reference.md, and script. |
| 2 | Response includes a publish_vocab.py command with --file argument | ❌ No | SKILL.md Step 1 gates on confirming env vars before Step 2; file given but agent stops to verify env vars first. |
| 2 | Response references OM_TOKEN (JWT / service account token) | ✅ Yes | Agent references OM_TOKEN for Bearer JWT auth per SKILL.md Step 1 and updated reference.md. |
| 2 | Response does NOT include --username or --password flags in the run command | ✅ Yes | No credential flags in publish_vocab.py or SKILL.md. OM_TOKEN env var is the sole auth mechanism. |
| 3 | Response states or implies the script is idempotent / safe to re-run | ✅ Yes | SKILL.md 'What the script does': 'The script is idempotent — safe to re-run at any time.' Agent surfaces this. |
| 3 | Response includes the publish_vocab.py command | ❌ No | No file path given; SKILL.md Step 1 requires confirming path before Step 2; agent stops at Step 1. |
| 3 | Response does not warn that re-running will cause destructive changes | ✅ Yes | SKILL.md documents idempotent behavior; agent correctly omits destructive-change warning. |
| 4 | Response references the bot token expiry / revocation as cause of 401 | ✅ Yes | SKILL.md Step 4: 'the bot token has expired or been revoked.' Agent cites directly. |
| 4 | Response mentions regenerating the token under Settings → Bots or vocab-publisher-bot | ✅ Yes | SKILL.md Step 4: 'regenerate it under Settings → Bots → vocab-publisher-bot.' Agent follows exact path. |
| 4 | Response mentions updating OM_TOKEN after regeneration | ✅ Yes | SKILL.md Step 4: 'update OM_TOKEN'. Agent instructs user to replace old value. |
| 5 | Response explains 403 is a permissions error (not auth) | ✅ Yes | SKILL.md Step 5: 'the bot is missing write permissions.' Agent distinguishes 403 from 401. |
| 5 | Response mentions VocabPublisherPolicy or equivalent custom policy | ✅ Yes | SKILL.md Step 5 and Gotchas name VocabPublisherPolicy (Create, EditAll, ViewAll). Agent surfaces this. |
| 5 | Response directs user to Settings → Bots → vocab-publisher-bot → Roles | ✅ Yes | SKILL.md Step 5 navigation path cited exactly. |
| 6 | Response asks for or confirms the vocabulary .md file path | ✅ Yes | SKILL.md Step 1 requires confirming file path. Agent asks for it. |
| 6 | Response includes or references the publish_vocab.py script invocation | ✅ Yes | Agent references publish script; SKILL.md Step 2 shows exact command. |
| 6 | Response does NOT attempt to generate a vocabulary (that is a different skill) | ✅ Yes | Agent focuses on publishing workflow. New Coexistence section reinforces separation. |
| 7 | Response explains that changed descriptions will be updated and unchanged ones skipped | ❌ No | No file path; agent enters Step 1 confirmation phase — UPDATED/SKIPPED explanation not shown in immediate response. |
| 7 | Response includes publish_vocab.py command | ❌ No | No file path provided; agent gates on Step 1 per SKILL.md. Same step-gating behavior as prior review. |
| 7 | Response does NOT say all tags will be deleted and recreated | ✅ Yes | SKILL.md documents create/update/skip logic; agent correctly omits deletion warning. |
| 8 | Response engages with the publish workflow (confirms file path and/or env vars) | ✅ Yes | Exact trigger phrase. Agent enters publish workflow, confirms file path and checks env vars per Step 1. |
| 8 | Response does NOT deflect to a different skill | ✅ Yes | Agent remains in publish workflow. No deflection to catalog-sync or governance-vocab-generator. |
| 8 | Response does NOT ask clarifying questions about whether to generate vs publish | ✅ Yes | No generate-vs-publish ambiguity. Coexistence section reinforces distinction. |
| 9 | Response does NOT attempt to run publish_vocab.py | ✅ Yes | Generation task. Coexistence section explicitly states skill does not fire on 'generate'. Correct routing. |
| 9 | Response indicates this is a vocabulary generation task (not publishing) | ✅ Yes | Agent produces vocabulary content — generation behavior confirmed. |
| 9 | Response does NOT ask for OM credentials or base URL | ✅ Yes | No OM_BASE_URL, OM_TOKEN, or OpenMetadata credentials mentioned for generation task. |
| 10 | Response does NOT run or reference publish_vocab.py | ✅ Yes | Ingestion task. Coexistence section lists catalog-sync as correct skill. Agent routes correctly. |
| 10 | Response treats this as a catalog ingestion / sync task | ✅ Yes | Agent engages with catalog ingestion behavior. |
| 10 | Response does NOT ask for a vocabulary .md file | ✅ Yes | No vocabulary file requested. Agent works with Snowflake service and ingestion configuration. |
| 11 | Response asks for the vocabulary .md file path | ✅ Yes | No path given; SKILL.md Step 1 requires confirming path. Agent correctly asks. |
| 11 | Response does NOT attempt to run the script without knowing the file path | ✅ Yes | Agent stops at Step 1; does not show command without --file argument. |
| 11 | Response checks or asks about OM_BASE_URL and OM_TOKEN | ✅ Yes | SKILL.md Step 1: 'verify OM_BASE_URL and OM_TOKEN are set.' Agent asks. |
| 12 | Response lists prerequisite setup steps (service account and/or OM_TOKEN) | ✅ Yes | SKILL.md Prerequisites lists vocab-publisher-bot, VocabPublisherPolicy, OM_TOKEN. Agent covers all. |
| 12 | Response references vocab-publisher-bot or equivalent service account | ✅ Yes | SKILL.md Prerequisites names 'vocab-publisher-bot'. Agent surfaces this. |
| 12 | Response does NOT proceed to run the script before prerequisites are confirmed | ✅ Yes | Agent stops at setup guidance; indicates script will run once prerequisites are in place. |
| 13 | Response links SSL/TLS errors to the HTTP vs HTTPS mismatch | ✅ Yes | SKILL.md Gotchas covers TLS handshake error from scheme mismatch. Agent cites directly. |
| 13 | Response advises checking or correcting the OM_BASE_URL scheme (http:// vs https://) | ✅ Yes | Agent advises correcting OM_BASE_URL scheme. SKILL.md Gotchas covers this. |
| 13 | Response does NOT suggest reinstalling requests or Python as primary fix | ✅ Yes | Primary fix is OM_BASE_URL correction. No reinstall suggestions. |

## Category Grades

| # | Category | Grade | Explanation |
|---|----------|-------|-------------|
| 1 | Triggering (Description Quality) | PASS | Name valid (23 chars), what/when/triggers all documented, 5 explicit trigger phrases in description ↩ carried |
| 2 | Anatomy & Structure | PASS | Valid YAML frontmatter; ~157 lines; references shallow (SKILL.md → reference.md, 1 hop) ↩ carried |
| 3 | Instructions Clarity | PASS | Numbered workflow steps, explicit 401/403 branches, reference.md auth now fully aligned with SKILL.md and script ↩ carried |
| 4 | Output Quality | PASS | Example output block defines exact CREATED/UPDATED/SKIPPED format; summary table templated ↩ carried |
| 5 | Testability | PASS | 13-eval test suite with evals.json, test-cases.md, test_skill.py; all HIGH subcategories satisfied ↩ carried |
| 6 | Resource Efficiency | PASS | Pre-built script invoked deterministically; no common-knowledge over-explanation; explicit 'Run X' instructions ↩ carried |
| 7 | Security & Trust | PASS | OM_TOKEN Bearer JWT auth throughout; reference.md fully aligned; no hardcoded credentials; no logging of token values ↩ carried |
| 8 | Coexistence & Recall | PASS | Coexistence & Routing section added with a 4-skill routing table; trigger precision documented; LOW finding resolved |
| 9 | Model Compatibility | PASS | `validated_on: Claude Sonnet 4.6` in both frontmatter and Registry ↩ carried |
| 10 | Workflow & Feedback Loops | PASS | Confirm → run → review loop; 401/403 fix-and-retry paths present ↩ carried |
| 11 | Maintainability & Lifecycle | PASS | Reviewer field with peer-review requirement resolves HIGH; Version bump policy + Rollback plan resolve MEDIUM versioning |
| 12 | Gotchas / Lessons Learned | PASS | 5 real-world gotchas documenting bot policy, HTTP/HTTPS, PowerShell, token expiry, .env git-ignore ↩ carried |
| 13 | Anti-Pattern Audit | PASS | requirements.txt now present (requests==2.33.1) — all 4 MEDIUM subcategories pass; no voodoo constants |

## Version Comparison (if comparator was run)

| Eval | Prompt | Verdict | Reasoning |
|------|--------|---------|-----------|
| 1 | Publish my governance vocabulary to OpenMetadata | tie | SKILL.md workflow unchanged; Registry/Coexistence additions are governance metadata only |
| 2 | Push my retail-governance-vocab.md to the data catalog | tie | Same step-1 gating behavior; FAIL in both old and new |
| 3 | Sync our governance tags — re-run the vocab publisher | tie | Same step-1 gating; FAIL in both old and new |
| 4 | I got 401 Unauthorized when publishing the vocabulary | tie | 401 error branch unchanged |
| 5 | The publish script returned 403 Forbidden | tie | 403 error branch unchanged |
| 6 | Upload our governance framework classifications to OpenMetadata | tie | Upload workflow unchanged |
| 7 | Our tag descriptions have changed — push the updates | tie | Same step-1 gating; FAIL in both old and new |
| 8 | Push vocabulary to OpenMetadata, publish classifications | tie | Trigger phrase and workflow unchanged |
| 9 | Generate a governance vocabulary for the retail domain | tie | Routing already correct in old; Coexistence section reinforces but doesn't materially change outcome |
| 10 | Run a metadata ingestion from Snowflake into OpenMetadata | tie | Routing already correct in old; catalog-sync guidance unchanged |
| 11 | Publish my vocab | tie | Step-1 gating unchanged |
| 12 | I want to publish but haven't set up the service account yet | tie | Prerequisites section unchanged |
| 13 | I get SSL errors when trying to publish | tie | Gotchas section unchanged |

_Comparator summary: new_wins=0, old_wins=0, ties=13, overall=tie. SKILL.md changes are governance metadata (Registry rows + Coexistence section); no behavioral diff vs prior snapshot._

## History (all reviews)

| Reviewed at (ISO UTC) | Calendar | Unit Tests | Assertions | Categories | Verdict | Notes |
|----------------------|----------|------------|------------|------------|---------|-------|
| 2026-05-07T14:02:43Z | 2026-05-07 | 10/13 (77%) | 32/39 (82%) | 11/13 (85%) | FAIL | Initial incremental re-check; Cat 7 HIGH resolved; Cat 11 HIGH + MEDIUM still failing |
| 2026-05-08T08:10:53Z | 2026-05-08 | 10/13 (77%) | 35/39 (90%) | 12/13 (92%) | FAIL | Cat 7 fully resolved; Cat 13 + Cat 8 promoted to PASS; Cat 11 HIGH still failing |
| 2026-05-08T08:39:26Z | 2026-05-08 | 10/13 (77%) | 35/39 (90%) | 13/13 (100%) | PASS | All fixes applied; Cat 11 HIGH + MEDIUM resolved; 13/13 categories PASS |

Full narrative: `review-catalog-vocab-publisher-2026-05-08T083926Z.md`.
