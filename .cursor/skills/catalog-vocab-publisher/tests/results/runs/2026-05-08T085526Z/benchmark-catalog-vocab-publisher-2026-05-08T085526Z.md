# Benchmark Report: catalog-vocab-publisher
_Generated: 2026-05-08T08:55:26Z UTC · Incremental re-review; RUN_STEP_3C=true (test files changed); 12 categories carried from 2026-05-08T083926Z; Category 5 re-evaluated_

## Summary

| Metric | Value |
|--------|-------|
| Overall Verdict | PASS |
| Unit Tests | 12 / 13 passed (92%) |
| Assertions | 38 / 39 (97%) |
| Categories | 13 / 13 passed |
| High Failures | — |
| Medium Failures | — |
| Comparator | tie (SKILL.md unchanged vs prior snapshot; all 13 comparisons tie) |

## Unit Test Results

| # | Test | Type | Result | Assertions |
|---|------|------|--------|------------|
| 1 | Publish my governance vocabulary to OpenMetadata | should-trigger | ✅ PASS | 3/3 |
| 2 | Push my retail-governance-vocab.md to the data catalog | should-trigger | ✅ PASS | 3/3 |
| 3 | Sync our governance tags — re-run the vocab publisher | should-trigger | ❌ FAIL | 2/3 |
| 4 | I got 401 Unauthorized when publishing the vocabulary | should-trigger | ✅ PASS | 3/3 |
| 5 | The publish script returned 403 Forbidden | should-trigger | ✅ PASS | 3/3 |
| 6 | Upload our governance framework classifications to OpenMetadata | should-trigger | ✅ PASS | 3/3 |
| 7 | Our tag descriptions have changed — push the updates | should-trigger | ✅ PASS | 3/3 |
| 8 | Push vocabulary to OpenMetadata, publish classifications | should-trigger | ✅ PASS | 3/3 |
| 9 | Generate a governance vocabulary for the retail domain | should-not-trigger | ✅ PASS | 3/3 |
| 10 | Run a metadata ingestion from Snowflake into OpenMetadata | should-not-trigger | ✅ PASS | 3/3 |
| 11 | Publish my vocab | edge-case | ✅ PASS | 3/3 |
| 12 | I want to publish but haven't set up the service account yet | edge-case | ✅ PASS | 3/3 |
| 13 | I get SSL errors when trying to publish | edge-case | ✅ PASS | 3/3 |

## Assertion Detail

| Eval | Assertion | Passed | Evidence |
|------|-----------|--------|----------|
| 1 | Response asks for or confirms the .md vocabulary file path | ✅ Yes | Response lists 7 vocabulary files and explicitly asks 'Which vocabulary file do you want to publish?' before proceeding. |
| 1 | Response references OM_BASE_URL and/or OM_TOKEN environment variables | ✅ Yes | Response states 'Both OM_BASE_URL and OM_TOKEN are missing' and provides export commands for both variables. |
| 1 | Response does NOT use --username / --password flags | ✅ Yes | No --username or --password flags appear anywhere in the response; authentication is handled entirely via OM_TOKEN. |
| 2 | Response acknowledges the retail-governance-vocab.md file path from the prompt | ✅ Yes | Response states 'I found your vocab file at governance-vocabularies/retail-governance-vocab.md' and references the file path again later. |
| 2 | Response asks about or references OM_TOKEN / OM_BASE_URL before showing the run command (Step 1 env-var confirmation) | ✅ Yes | Response references both OM_TOKEN (present) and OM_BASE_URL (missing), and asks 'What is your OpenMetadata base URL?' before proceeding — no run command is shown. |
| 2 | Response does NOT include --username or --password flags | ✅ Yes | The response contains no command invocation at all, and no --username or --password flags appear anywhere in the text. |
| 3 | Response states or implies the script is idempotent / safe to re-run | ❌ No | The response does not mention idempotency or that re-running is safe anywhere. It only asks clarifying questions about the file and environment variables. |
| 3 | Response asks for the .md vocabulary file path before running (Step 1 gating — no file path in prompt) | ✅ Yes | The response explicitly asks 'Which one (or ones) do you want to sync?' after listing found vocabulary files, gating execution on the user's file selection. |
| 3 | Response does not warn that re-running will cause destructive changes | ✅ Yes | The response contains no warning about destructive changes; it only asks about the file choice and missing environment variables. |
| 4 | Response references the bot token expiry / revocation as cause of 401 | ✅ Yes | Response states 'the JWT token for the vocab-publisher-bot service account has either expired or been revoked'. |
| 4 | Response mentions regenerating the token under Settings → Bots or vocab-publisher-bot | ✅ Yes | Response states 'go to Settings → Bots → vocab-publisher-bot' and 'Regenerate the token there'. |
| 4 | Response mentions updating OM_TOKEN after regeneration | ✅ Yes | Response states 'Update your OM_TOKEN environment variable with the new token' with example export command. |
| 5 | Response explains 403 is a permissions error (not auth) | ✅ Yes | Response states '403 Forbidden means the vocab-publisher-bot service account is missing the write permissions it needs' — framed as permissions. |
| 5 | Response mentions VocabPublisherPolicy or equivalent custom policy | ✅ Yes | Response explicitly references 'VocabPublisherPolicy' including required permissions (Create, EditAll, ViewAll) on Classification and Tag resources. |
| 5 | Response directs user to Settings → Bots → vocab-publisher-bot → Roles | ✅ Yes | Step 2 says 'go to Settings → Bots → vocab-publisher-bot → Roles and confirm VocabPublisherRole is listed'. |
| 6 | Response asks for or confirms the vocabulary .md file path | ✅ Yes | Response explicitly asks 'Which vocabulary file do you want to publish?' and lists 7 candidate .md files. |
| 6 | Response includes or references the publish_vocab.py script invocation | ✅ Yes | Response states 'I'll run the publish script and show you the summary of what gets created, updated, or skipped'. |
| 6 | Response does NOT attempt to generate a vocabulary (that is a different skill) | ✅ Yes | Response contains no vocabulary generation; it only lists existing files and asks which one to publish. |
| 7 | Response asks for the vocabulary .md file path before running (Step 1 gating — no file path in prompt) | ✅ Yes | Response explicitly asks 'Which vocabulary file do you want to push?' and lists candidate files before proceeding. |
| 7 | Response references the publish workflow or indicates changed descriptions will be updated and unchanged ones skipped | ✅ Yes | Response states 'the [UPDATED] tags with changed descriptions will be patched in OpenMetadata automatically — everything already in sync will be skipped'. |
| 7 | Response does NOT say all tags will be deleted and recreated | ✅ Yes | No mention of deletion or recreation anywhere in the response. It describes a patch/update flow with skipping unchanged items. |
| 8 | Response engages with the publish workflow (confirms file path and/or env vars) | ✅ Yes | Response lists available vocabulary files, asks which to publish, and calls out that OM_BASE_URL and OM_TOKEN env vars are not set. |
| 8 | Response does NOT deflect to a different skill | ✅ Yes | The response stays entirely within the catalog-vocab-publisher workflow. No mention of other skills. |
| 8 | Response does NOT ask clarifying questions about whether to generate vs publish | ✅ Yes | The response assumes publishing is the goal and only asks which file to target and for env vars. |
| 9 | Response does NOT attempt to run publish_vocab.py | ✅ Yes | The agent routed to the governance-vocab-generator skill and did not attempt to run publish_vocab.py at any point. |
| 9 | Response indicates this is a vocabulary generation task (not publishing) | ✅ Yes | The agent explicitly identified this as a vocabulary generation task and routed to the governance-vocab-generator skill. |
| 9 | Response does NOT ask for OM credentials or base URL | ✅ Yes | The agent asked for no credentials or base URL; it simply read the governance-vocab-generator skill file. |
| 10 | Response does NOT run or reference publish_vocab.py | ✅ Yes | The response makes no mention of publish_vocab.py anywhere. It only references Airflow, OpenMetadata connectivity, and pipeline triggers. |
| 10 | Response treats this as a catalog ingestion / sync task | ✅ Yes | The response describes triggering a metadata ingestion pipeline via Airflow to sync Snowflake tables/columns into OpenMetadata. |
| 10 | Response does NOT ask for a vocabulary .md file | ✅ Yes | The response does not request or reference any vocabulary Markdown file. |
| 11 | Response asks for the vocabulary .md file path | ✅ Yes | Response lists 7 found vocabulary files and explicitly asks 'Which vocabulary file do you want to publish?' |
| 11 | Response does NOT attempt to run the script without knowing the file path | ✅ Yes | Response stops and requests both the file selection and env vars before proceeding; no script execution is attempted. |
| 11 | Response checks or asks about OM_BASE_URL and OM_TOKEN | ✅ Yes | Response explicitly states 'The environment variables OM_BASE_URL and OM_TOKEN are not set' and asks the user to provide both values. |
| 12 | Response lists prerequisite setup steps (service account and/or OM_TOKEN) | ✅ Yes | Response provides 5 numbered steps covering bot creation, policy creation, role assignment, and setting OM_BASE_URL/OM_TOKEN. |
| 12 | Response references vocab-publisher-bot or equivalent service account | ✅ Yes | Step 1 explicitly instructs: 'create a new bot named vocab-publisher-bot'. |
| 12 | Response does NOT proceed to run the script before prerequisites are confirmed | ✅ Yes | Step 5 ends with 'Once you confirm the file and the env vars are set, you're ready to run the publish script' — gating execution. |
| 13 | Response links SSL/TLS errors to the HTTP vs HTTPS mismatch | ✅ Yes | Response states 'SSL errors when publishing almost always mean the OM_BASE_URL is set to https:// but the OpenMetadata server is running plain HTTP'. |
| 13 | Response advises checking or correcting the OM_BASE_URL scheme (http:// vs https://) | ✅ Yes | Response explicitly says 'Check your OM_BASE_URL value and switch the scheme from https:// to http://' with before/after example. |
| 13 | Response does NOT suggest reinstalling requests or Python as primary fix | ✅ Yes | No mention of reinstalling requests, pip, or Python anywhere in the response. |

## Category Grades

| # | Category | Grade | Explanation |
|---|----------|-------|-------------|
| 1 | Triggering (Description Quality) | PASS | ✓ carried · Description is specific, ≤1024 chars, includes exact trigger phrases and no generic language ↩ carried |
| 2 | Anatomy & Structure | PASS | ✓ carried · Valid YAML frontmatter; references shallow; SKILL.md body within 500 lines ↩ carried |
| 3 | Instructions Clarity | PASS | ✓ carried · Degrees of freedom calibrated; consistent terminology; no time-sensitive content ↩ carried |
| 4 | Output Quality | PASS | ✓ carried · Template strictness appropriate for API-driven workflow; output format defined ↩ carried |
| 5 | Testability | PASS | Re-evaluated · All HIGH subcategories pass (isolation, instruction-following, output quality evaluable); test suite covers 8 should-trigger, 3 should-not-trigger, 3 edge cases; coexistence documented in Routing section |
| 6 | Resource Efficiency | PASS | ✓ carried · Pre-built Python script used; no generated code for deterministic ops; execution intent explicit ↩ carried |
| 7 | Security & Trust | PASS | ✓ carried · No hardcoded credentials; OM_TOKEN env-var-only auth; .env git-ignored; .env.example has placeholders ↩ carried |
| 8 | Coexistence & Recall | PASS | ✓ carried · Routing table clearly delineates adjacent skills; trigger terms non-overlapping ↩ carried |
| 9 | Model Compatibility | PASS | ✓ carried · Validated on Claude Sonnet 4.6; documented in YAML frontmatter ↩ carried |
| 10 | Workflow & Feedback Loops | PASS | ✓ carried · Validate-fix-repeat loop present (steps 4-5 for 401/403); plan-validate-execute pattern used ↩ carried |
| 11 | Maintainability & Lifecycle | PASS | ✓ carried · Registry table present; peer-review requirement documented; versioning strategy and rollback plan defined ↩ carried |
| 12 | Gotchas / Lessons Learned | PASS | ✓ carried · Five documented gotchas covering real failure modes (bot policy, HTTP/HTTPS, PowerShell, token expiry, .env) ↩ carried |
| 13 | Anti-Pattern Audit | PASS | ✓ carried · Forward slashes throughout; pinned requests==2.33.1; explicit error handling; no voodoo constants ↩ carried |

## Version Comparison (if comparator was run)

SKILL.md is identical between current tree and prior snapshot (`2026-05-08T083926Z`). Only test files (test-cases.md, evals.json, test_skill.py) changed. All 13 comparisons returned **tie**.

| Eval | Prompt | Verdict | Reasoning |
|------|--------|---------|-----------|
| 1 | Publish my governance vocabulary to OpenMetadata | tie | Both outputs identical: ask for file and gate on env vars. |
| 2 | Push my retail-governance-vocab.md to the data catalog | tie | Both outputs identical: acknowledge file path, confirm OM_TOKEN present, ask for OM_BASE_URL. |
| 3 | Sync our governance tags — re-run the vocab publisher | tie | Both outputs identical: ask for file and env vars; neither mentions idempotency (shared gap). |
| 4 | I got 401 Unauthorized when publishing the vocabulary | tie | Both outputs identical: diagnose expired/revoked JWT; direct to Settings → Bots. |
| 5 | The publish script returned 403 Forbidden | tie | Both outputs identical: attribute to missing permissions; direct to VocabPublisherPolicy. |
| 6 | Upload our governance framework classifications to OpenMetadata | tie | Both outputs identical: gate on file selection and env vars. |
| 7 | Our tag descriptions have changed — push the updates | tie | Both outputs identical: gate on file, reference UPDATED/SKIPPED logic. |
| 8 | Push vocabulary to OpenMetadata, publish classifications | tie | Both outputs identical: engage publish workflow; mention idempotency. |
| 9 | Generate a governance vocabulary for the retail domain | tie | Both outputs identical: route to governance-vocab-generator. |
| 10 | Run a metadata ingestion from Snowflake into OpenMetadata | tie | Both outputs identical: treat as ingestion task, no vocab publisher triggered. |
| 11 | Publish my vocab | tie | Both outputs identical: ask for file and confirm env vars. |
| 12 | I want to publish but haven't set up the service account yet | tie | Both outputs identical: surface prerequisite setup steps. |
| 13 | I get SSL errors when trying to publish | tie | Both outputs identical: cite HTTP vs HTTPS gotcha. |

## History (all reviews)

| Reviewed at (ISO UTC) | Calendar | Unit Tests | Assertions | Categories | Verdict | Notes |
|-----------------------|----------|------------|------------|------------|---------|-------|
| 2026-05-07T14:02:43Z | 2026-05-07 | 10/13 | 32/39 | 11/13 | FAIL | Initial review |
| 2026-05-08T08:10:53Z | 2026-05-08 | 10/13 | 35/39 | 12/13 | FAIL | Incremental re-review after Cat 7 + Cat 11 fixes |
| 2026-05-08T08:39:26Z | 2026-05-08 | 10/13 | 35/39 | 13/13 | PASS | All categories pass; 3 unit test assertions misaligned |
| 2026-05-08T08:55:26Z | 2026-05-08 | 12/13 | 38/39 | 13/13 | PASS | Evals 2+7 fixed by new assertions; eval 3 still fails (idempotency not mentioned) |

Full narrative: `review-catalog-vocab-publisher-2026-05-08T085526Z.md`.
