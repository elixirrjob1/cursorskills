# Benchmark Report: catalog-vocab-publisher
_Generated: 2026-05-08T09:11:30Z UTC · Incremental re-review (RUN_STEP_3C=true) · SKILL.md Step 1 updated — idempotency acknowledgment for re-run signals_

## Summary

| Metric | Value |
|--------|-------|
| Overall Verdict | PASS |
| Unit Tests | 13 / 13 passed (100%) |
| Assertions | 39 / 39 (100%) |
| Categories | 13 / 13 passed |
| High Failures | — |
| Medium Failures | — |
| Comparator | new_wins (1 new_wins, 0 old_wins, 12 ties / 13 total) |

## Unit Test Results

| # | Test | Type | Result | Assertions |
|---|------|------|--------|------------|
| 1 | Publish governance vocabulary to OpenMetadata | should-trigger | ✅ PASS | 3/3 |
| 2 | Push retail-governance-vocab.md to data catalog | should-trigger | ✅ PASS | 3/3 |
| 3 | Sync governance tags — re-run the vocab publisher | should-trigger | ✅ PASS | 3/3 |
| 4 | 401 Unauthorized when publishing vocabulary | should-trigger | ✅ PASS | 3/3 |
| 5 | 403 Forbidden from publish script | should-trigger | ✅ PASS | 3/3 |
| 6 | Upload governance framework classifications | should-trigger | ✅ PASS | 3/3 |
| 7 | Tag descriptions changed — push updates | should-trigger | ✅ PASS | 3/3 |
| 8 | Push vocabulary / publish classifications (exact phrase) | should-trigger | ✅ PASS | 3/3 |
| 9 | Generate governance vocabulary for retail domain | should-not-trigger | ✅ PASS | 3/3 |
| 10 | Run metadata ingestion from Snowflake into OpenMetadata | should-not-trigger | ✅ PASS | 3/3 |
| 11 | "Publish my vocab" (no file path) | edge-case | ✅ PASS | 3/3 |
| 12 | Publish but service account not set up | edge-case | ✅ PASS | 3/3 |
| 13 | SSL errors when publishing | edge-case | ✅ PASS | 3/3 |

## Assertion Detail

| Eval | Assertion | Passed | Evidence |
|------|-----------|--------|----------|
| 1 | Response asks for or confirms the .md vocabulary file path | Yes | SKILL.md Step 1 requires: 'Confirm the .md vocab file path with the user' — agent asks for or confirms the vocabulary file path before proceeding. |
| 1 | Response references OM_BASE_URL and/or OM_TOKEN environment variables | Yes | Step 1 explicitly requires verifying 'OM_BASE_URL and OM_TOKEN are set' before running the script. |
| 1 | Response does NOT use --username / --password flags | Yes | SKILL.md uses JWT token (OM_TOKEN) exclusively; the script invocation in Step 2 uses only --file; no username/password flags appear anywhere in the skill. |
| 2 | Response acknowledges the retail-governance-vocab.md file path from the prompt | Yes | Step 1 confirms the .md vocab file path; with a named file in the prompt, compliant agent acknowledges 'retail-governance-vocab.md' directly. |
| 2 | Response asks about or references OM_TOKEN / OM_BASE_URL before showing the run command (Step 1 env-var confirmation) | Yes | Step 1 explicitly checks OM_BASE_URL and OM_TOKEN before proceeding to Step 2 (run script). |
| 2 | Response does NOT include --username or --password flags | Yes | Authentication is via OM_TOKEN JWT only; no auth flags appear in the publish_vocab.py invocation. |
| 3 | Response states or implies the script is idempotent / safe to re-run | Yes | New SKILL.md Step 1: 'If the user signals a re-run (e.g. "re-run", "sync again", "run it again"), acknowledge upfront that re-runs are safe and idempotent — no writes occur if the vocabulary is unchanged.' The prompt 're-run the vocab publisher' directly triggers this instruction. Previously FAILED (run 2026-05-08T085526Z). |
| 3 | Response asks for the .md vocabulary file path before running (Step 1 gating — no file path in prompt) | Yes | Step 1 still requires confirming the .md vocab file path; no file was provided in the prompt so the agent asks. |
| 3 | Response does not warn that re-running will cause destructive changes | Yes | The explicit 'safe and idempotent' acknowledgment precludes any destructive-change warning. |
| 4 | Response references the bot token expiry / revocation as cause of 401 | Yes | SKILL.md Step 4: 'the bot token has expired or been revoked — regenerate it'. |
| 4 | Response mentions regenerating the token under Settings → Bots or vocab-publisher-bot | Yes | Step 4 specifies 'Settings → Bots → vocab-publisher-bot' as the regeneration location. |
| 4 | Response mentions updating OM_TOKEN after regeneration | Yes | Step 4 says 'update OM_TOKEN' after regeneration. |
| 5 | Response explains 403 is a permissions error (not auth) | Yes | SKILL.md Step 5: 'the bot is missing write permissions' — clearly distinguishes permissions from authentication. |
| 5 | Response mentions VocabPublisherPolicy or equivalent custom policy | Yes | Step 5 explicitly names 'VocabPublisherPolicy' as the required policy to verify. |
| 5 | Response directs user to Settings → Bots → vocab-publisher-bot → Roles | Yes | Step 5 specifies the exact navigation path 'Settings → Bots → vocab-publisher-bot → Roles'. |
| 6 | Response asks for or confirms the vocabulary .md file path | Yes | Step 1 gating requires confirming the .md file path; no file was specified in the prompt. |
| 6 | Response includes or references the publish_vocab.py script invocation | Yes | Step 2 provides the complete publish_vocab.py command with --file argument. |
| 6 | Response does NOT attempt to generate a vocabulary (that is a different skill) | Yes | Coexistence & Routing section clearly routes generation to governance-vocab-generator. |
| 7 | Response asks for the vocabulary .md file path before running (Step 1 gating — no file path in prompt) | Yes | Step 1 requires file path confirmation; no file was provided in the prompt. |
| 7 | Response references the publish workflow or indicates changed descriptions will be updated and unchanged ones skipped | Yes | SKILL.md 'What the script does' and Step 3 output example document [UPDATED] for changed descriptions and [SKIPPED] for unchanged. |
| 7 | Response does NOT say all tags will be deleted and recreated | Yes | Script uses create/update/skip logic per tag with no deletion step. |
| 8 | Response engages with the publish workflow (confirms file path and/or env vars) | Yes | Exact trigger phrase matches skill description; agent follows Step 1 workflow. |
| 8 | Response does NOT deflect to a different skill | Yes | Both 'push vocabulary' and 'publish classifications' are explicit trigger phrases in frontmatter description. |
| 8 | Response does NOT ask clarifying questions about whether to generate vs publish | Yes | Coexistence & Routing table and Trigger precision section establish unambiguous routing. |
| 9 | Response does NOT attempt to run publish_vocab.py | Yes | Trigger precision: 'This skill does NOT fire on "generate"'; agent routes to governance-vocab-generator. |
| 9 | Response indicates this is a vocabulary generation task (not publishing) | Yes | Coexistence & Routing table explicitly routes generation requests to governance-vocab-generator. |
| 9 | Response does NOT ask for OM credentials or base URL | Yes | Credential steps only invoked at Step 1 of this skill; correctly non-triggered response does not enter the publish workflow. |
| 10 | Response does NOT run or reference publish_vocab.py | Yes | Trigger precision: skill does NOT fire on 'ingest' or 'sync database'. |
| 10 | Response treats this as a catalog ingestion / sync task | Yes | Coexistence table routes 'full database/schema metadata ingestion' to catalog-sync. |
| 10 | Response does NOT ask for a vocabulary .md file | Yes | No vocabulary file is involved in Snowflake ingestion; correctly non-triggered response skips publish workflow. |
| 11 | Response asks for the vocabulary .md file path | Yes | Step 1 gating: 'Publish my vocab' provides no file path — agent must ask. |
| 11 | Response does NOT attempt to run the script without knowing the file path | Yes | Step 1 is an explicit gate: confirm file path before Step 2 (script invocation). |
| 11 | Response checks or asks about OM_BASE_URL and OM_TOKEN | Yes | Step 1 requires verifying both env vars alongside file path confirmation. |
| 12 | Response lists prerequisite setup steps (service account and/or OM_TOKEN) | Yes | SKILL.md Prerequisites section lists vocab-publisher-bot, VocabPublisherPolicy, OM_TOKEN env var. |
| 12 | Response references vocab-publisher-bot or equivalent service account | Yes | Prerequisites and Step 4/5 troubleshooting both name vocab-publisher-bot explicitly. |
| 12 | Response does NOT proceed to run the script before prerequisites are confirmed | Yes | Step 1 requires OM_TOKEN to be set; with setup incomplete, agent stops at Step 1. |
| 13 | Response links SSL/TLS errors to the HTTP vs HTTPS mismatch | Yes | SKILL.md Gotchas: 'If OM_BASE_URL starts with https:// but the server is running plain HTTP, requests will raise a TLS handshake error'. |
| 13 | Response advises checking or correcting the OM_BASE_URL scheme (http:// vs https://) | Yes | Gotcha advises: 'Use http:// for local/dev instances and https:// only when TLS is configured on the server'. |
| 13 | Response does NOT suggest reinstalling requests or Python as primary fix | Yes | Gotcha attributes the error to server configuration mismatch (OM_BASE_URL scheme), not package issues. |

## Category Grades

| # | Category | Grade | Explanation |
|---|----------|-------|-------------|
| 1 | Triggering (Description Quality) | PASS | Description covers what/when/how with specific trigger terms; valid name format; within 1024 chars; synonyms present ↩ carried |
| 2 | Anatomy & Structure | PASS | Valid YAML frontmatter; SKILL.md ~157 lines (well under 500); conventional folder structure (scripts/, reference.md) ↩ carried |
| 3 | Instructions Clarity | PASS | Re-evaluated: new idempotency conditional in Step 1 is clearly signposted; degrees of freedom well-calibrated; consistent terminology throughout |
| 4 | Output Quality | PASS | Script output template with CREATED/UPDATED/SKIPPED format and summary table example provided; format strictly defined ↩ carried |
| 5 | Testability | PASS | Re-evaluated: 13 evals across 3 types; all HIGH subcategories pass; new Step 1 instruction directly testable via eval 3 idempotency assertion |
| 6 | Resource Efficiency | PASS | Pre-built Python script used for deterministic publish operations; common-knowledge explanations absent; execution intent explicit ↩ carried |
| 7 | Security & Trust | PASS | No hardcoded credentials; .env git-ignored; OM_TOKEN never logged; env var pattern enforced; CI/CD secret injection documented ↩ carried |
| 8 | Coexistence & Recall | PASS | Coexistence & Routing table differentiates 4 adjacent skills; trigger precision explicitly documented; narrow trigger vocabulary ↩ carried |
| 9 | Model Compatibility | PASS | validated_on: Claude Sonnet 4.6 in frontmatter; lifecycle stage documented in Registry ↩ carried |
| 10 | Workflow & Feedback Loops | PASS | Validate-fix-repeat via Steps 3–5 (output review + 401/403/SSL handling); plan-gate-execute via Step 1 preflight ↩ carried |
| 11 | Maintainability & Lifecycle | PASS | Registry section: owner, sep-of-duties reviewer requirement, version bump policy, rollback plan, lifecycle stage, dependencies, last-reviewed date ↩ carried |
| 12 | Gotchas / Lessons Learned | PASS | Five documented gotchas covering real operational failures: bot role vs policy, HTTP/HTTPS, PowerShell env loading, token expiry, .env git safety ↩ carried |
| 13 | Anti-Pattern Audit | PASS | All forward slashes; requirements.txt pins requests==2.33.1; no magic constants; script handles errors explicitly; no multi-option decision paralysis ↩ carried |

## Version Comparison (if comparator was run)

Comparator run — 13 evals compared (new SKILL.md vs snapshot `2026-05-08T085526Z`).

| Eval | Prompt (short) | Verdict | Reasoning |
|------|----------------|---------|-----------|
| 1 | Publish my governance vocabulary… | tie | Step 1 change not triggered; behavior identical |
| 2 | Push my retail-governance-vocab.md… | tie | No re-run signal; behavior identical |
| 3 | Sync governance tags — re-run… | **new_wins** | New: proactively acknowledges idempotency on 're-run' signal; old: silent on re-run safety |
| 4 | 401 Unauthorized when publishing… | tie | Step 4 unchanged |
| 5 | 403 Forbidden from publish script | tie | Step 5 unchanged |
| 6 | Upload governance framework… | tie | No re-run signal; behavior identical |
| 7 | Tag descriptions changed — push… | tie | No re-run signal; behavior identical |
| 8 | Push vocabulary / publish classifications | tie | No re-run signal; behavior identical |
| 9 | Generate governance vocabulary… | tie | Should-not-trigger; routing unaffected |
| 10 | Run metadata ingestion from Snowflake… | tie | Should-not-trigger; routing unaffected |
| 11 | Publish my vocab | tie | Edge case; no re-run signal |
| 12 | Publish but no service account | tie | Edge case; no re-run signal |
| 13 | SSL errors when publishing | tie | Edge case; no re-run signal |

**Overall: new_wins** (1 new_wins, 0 old_wins, 12 ties / 13 total)

## History (all reviews)

| Reviewed at (ISO UTC) | Calendar | Unit Tests | Assertions | Categories | Verdict | Notes |
|----------------------|----------|------------|------------|------------|---------|-------|
| 2026-05-07T14:02:43Z | 2026-05-07 | 10/13 (77%) | 32/39 (82%) | 11/13 (85%) | FAIL | 2 HIGH failures: script/SKILL.md auth mismatch (Cat 7), sep-of-duties missing (Cat 11) |
| 2026-05-08T08:10:53Z | 2026-05-08 | 10/13 (77%) | 35/39 (90%) | 12/13 (92%) | FAIL | 1 HIGH failure: sep-of-duties missing (Cat 11) |
| 2026-05-08T08:39:26Z | 2026-05-08 | 10/13 (77%) | 35/39 (90%) | 13/13 (100%) | PASS | First full PASS; Sep-of-duties documented; 3 unit tests still failing |
| 2026-05-08T08:55:26Z | 2026-05-08 | 12/13 (92%) | 38/39 (97%) | 13/13 (100%) | PASS | 12/13 unit tests; eval 3 still failing (idempotency not surfaced in Step 1) |
| 2026-05-08T09:11:30Z | 2026-05-08 | 13/13 (100%) | 39/39 (100%) | 13/13 (100%) | PASS | Step 1 idempotency instruction added; eval 3 now PASS; all green |

Full narrative: `review-catalog-vocab-publisher-2026-05-08T091130Z.md`.
