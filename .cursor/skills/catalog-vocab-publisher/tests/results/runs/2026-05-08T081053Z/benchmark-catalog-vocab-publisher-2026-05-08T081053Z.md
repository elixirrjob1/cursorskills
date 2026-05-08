# Benchmark Report: catalog-vocab-publisher
_Generated: 2026-05-08T08:10:53Z UTC · Incremental re-check; RUN_STEP_3C=true (scripts/publish_vocab.py changed); Cat 7 auth mismatch resolved_

## Summary

| Metric | Value |
|--------|-------|
| Overall Verdict | FAIL |
| Unit Tests | 10 / 13 passed (77%) |
| Assertions | 35 / 39 (90%) |
| Categories | 12 / 13 passed |
| High Failures | 1 |
| Medium Failures | 3 (Cat 11 versioning, Cat 13 requirements.txt, Cat 3 reference.md stale auth — Cat 3 still PASS) |
| Comparator | tie (0 new wins / 0 old wins / 13 ties — SKILL.md unchanged, only script changed) |

## Unit Test Results

| # | Test | Type | Result | Assertions |
|---|------|------|--------|------------|
| 1 | Basic publish trigger | should-trigger | ✅ PASS | 3/3 |
| 2 | Named file push with JWT auth | should-trigger | ❌ FAIL | 2/3 |
| 3 | Idempotent re-run | should-trigger | ❌ FAIL | 2/3 |
| 4 | 401 error handling | should-trigger | ✅ PASS | 3/3 |
| 5 | 403 error handling | should-trigger | ✅ PASS | 3/3 |
| 6 | Upload framework | should-trigger | ✅ PASS | 3/3 |
| 7 | Push updated descriptions | should-trigger | ❌ FAIL | 1/3 |
| 8 | Exact trigger phrase | should-trigger | ✅ PASS | 3/3 |
| 9 | Should-not-trigger: vocab generation | should-not-trigger | ✅ PASS | 3/3 |
| 10 | Should-not-trigger: Snowflake ingestion | should-not-trigger | ✅ PASS | 3/3 |
| 11 | Edge: no file path | edge-case | ✅ PASS | 3/3 |
| 12 | Edge: service account not set up | edge-case | ✅ PASS | 3/3 |
| 13 | Edge: SSL errors | edge-case | ✅ PASS | 3/3 |

## Assertion Detail

| Eval | Assertion | Passed | Evidence |
|------|-----------|--------|----------|
| 1 | Response asks for or confirms the .md vocabulary file path | ✅ Yes | Agent asks 'Which vocabulary file do you want to publish?' per SKILL.md Step 1 |
| 1 | Response references OM_BASE_URL and/or OM_TOKEN environment variables | ✅ Yes | SKILL.md Step 1 requires verifying both env vars; agent confirms both |
| 1 | Response does NOT use --username / --password flags | ✅ Yes | Script now uses env vars only; no credential flags in SKILL.md or script |
| 2 | Response includes a publish_vocab.py command with --file argument | ❌ No | SKILL.md Step 1 gates command on file + env-var confirmation; file given but env vars unconfirmed → agent stays in Step 1 without showing command |
| 2 | Response references OM_TOKEN (JWT / service account token) | ✅ Yes | SKILL.md Step 1: 'verify OM_TOKEN'; agent references it. Script now consistent (Bearer JWT). |
| 2 | Response does NOT include --username or --password flags in the run command | ✅ Yes | Script updated to env vars only; no credential CLI flags anywhere |
| 3 | Response states or implies the script is idempotent / safe to re-run | ✅ Yes | SKILL.md 'What the script does': 'idempotent — safe to re-run at any time'; agent surfaces this |
| 3 | Response includes the publish_vocab.py command | ❌ No | No file path given in prompt; SKILL.md Step 1 requires confirming it before proceeding to Step 2 command |
| 3 | Response does not warn that re-running will cause destructive changes | ✅ Yes | Idempotency clearly stated; no destructive warning issued |
| 4 | Response references the bot token expiry / revocation as cause of 401 | ✅ Yes | SKILL.md Step 4: 'bot token has expired or been revoked'; agent cites this exactly |
| 4 | Response mentions regenerating the token under Settings → Bots or vocab-publisher-bot | ✅ Yes | SKILL.md Step 4: 'Settings → Bots → vocab-publisher-bot'; agent follows this path |
| 4 | Response mentions updating OM_TOKEN after regeneration | ✅ Yes | SKILL.md Step 4: 'update OM_TOKEN'; agent instructs update after regeneration |
| 5 | Response explains 403 is a permissions error (not auth) | ✅ Yes | SKILL.md Step 5: 'missing write permissions'; agent distinguishes 403 from 401 |
| 5 | Response mentions VocabPublisherPolicy or equivalent custom policy | ✅ Yes | SKILL.md Step 5 and Gotchas name VocabPublisherPolicy explicitly; agent surfaces it |
| 5 | Response directs user to Settings → Bots → vocab-publisher-bot → Roles | ✅ Yes | SKILL.md Step 5: exact UI path cited; agent follows it |
| 6 | Response asks for or confirms the vocabulary .md file path | ✅ Yes | SKILL.md Step 1: 'Confirm the .md vocab file path'; agent asks for it |
| 6 | Response includes or references the publish_vocab.py script invocation | ✅ Yes | Agent references publish script; SKILL.md Step 2 shows exact command |
| 6 | Response does NOT attempt to generate a vocabulary (that is a different skill) | ✅ Yes | Agent stays in publishing workflow; no generation attempted |
| 7 | Response explains that changed descriptions will be updated and unchanged ones skipped | ❌ No | Agent enters Step 1 confirmation phase; UPDATED/SKIPPED explanation not surfaced without file context |
| 7 | Response includes publish_vocab.py command | ❌ No | No file path in prompt; SKILL.md Step 1 gating prevents Step 2 command from appearing |
| 7 | Response does NOT say all tags will be deleted and recreated | ✅ Yes | SKILL.md describes create/update/skip logic; no deletion language used |
| 8 | Response engages with the publish workflow (confirms file path and/or env vars) | ✅ Yes | Exact trigger phrase; agent immediately enters publish workflow per Step 1 |
| 8 | Response does NOT deflect to a different skill | ✅ Yes | No deflection; agent stays in publish context |
| 8 | Response does NOT ask clarifying questions about whether to generate vs publish | ✅ Yes | No generate-vs-publish confusion; correct intent parsed |
| 9 | Response does NOT attempt to run publish_vocab.py | ✅ Yes | 'Generate' prompt routes to vocab generator; no publish_vocab.py invoked |
| 9 | Response indicates this is a vocabulary generation task (not publishing) | ✅ Yes | Agent produces vocabulary content — generation behavior confirmed |
| 9 | Response does NOT ask for OM credentials or base URL | ✅ Yes | No OM_BASE_URL or OM_TOKEN mentioned for generation task |
| 10 | Response does NOT run or reference publish_vocab.py | ✅ Yes | Ingestion task; no publish_vocab.py referenced |
| 10 | Response treats this as a catalog ingestion / sync task | ✅ Yes | Agent engages with catalog ingestion workflow |
| 10 | Response does NOT ask for a vocabulary .md file | ✅ Yes | No vocabulary file requested; agent works with ingestion pipeline |
| 11 | Response asks for the vocabulary .md file path | ✅ Yes | No file in prompt; SKILL.md Step 1 triggers file path request |
| 11 | Response does NOT attempt to run the script without knowing the file path | ✅ Yes | Agent stops at Step 1; no premature script execution |
| 11 | Response checks or asks about OM_BASE_URL and OM_TOKEN | ✅ Yes | SKILL.md Step 1; agent confirms env vars needed |
| 12 | Response lists prerequisite setup steps (service account and/or OM_TOKEN) | ✅ Yes | SKILL.md Prerequisites; agent produces full setup guide |
| 12 | Response references vocab-publisher-bot or equivalent service account | ✅ Yes | 'vocab-publisher-bot' named explicitly per SKILL.md Prerequisites |
| 12 | Response does NOT proceed to run the script before prerequisites are confirmed | ✅ Yes | Agent waits for prerequisite confirmation before offering to run |
| 13 | Response links SSL/TLS errors to the HTTP vs HTTPS mismatch | ✅ Yes | SKILL.md Gotchas: TLS handshake error from scheme mismatch; agent cites directly |
| 13 | Response advises checking or correcting the OM_BASE_URL scheme (http:// vs https://) | ✅ Yes | Agent advises correcting OM_BASE_URL scheme per SKILL.md Gotchas |
| 13 | Response does NOT suggest reinstalling requests or Python as primary fix | ✅ Yes | Primary fix is scheme correction; no reinstall suggestions |

## Category Grades

| # | Category | Grade | Explanation |
|---|----------|-------|-------------|
| 1 | Triggering (Description Quality) | PASS ↩ carried | Name 23 chars, valid; what/when/triggers all present; 5 explicit trigger phrases cover natural synonyms |
| 2 | Anatomy & Structure | PASS ↩ carried | Valid YAML frontmatter; ~139 lines (well under 500); reference.md is one hop from SKILL.md |
| 3 | Instructions Clarity | PASS ↩ carried | Step-by-step workflow; explicit 401/403 branches; consistent terminology throughout; reference.md auth section is stale (MEDIUM note — does not fail category) |
| 4 | Output Quality | PASS ↩ carried | Example output block defines exact CREATED/UPDATED/SKIPPED format; template strictness appropriate for API-calling skill |
| 5 | Testability | PASS ↩ carried | 13-case eval suite present; all HIGH subcategories pass; test-cases.md and test_skill.py maintained |
| 6 | Resource Efficiency | PASS ↩ carried | Pre-built script invoked deterministically; no over-explanation; common-knowledge explanations stripped |
| 7 | Security & Trust | PASS | Re-evaluated: script now uses OM_TOKEN env var exclusively — auth mismatch resolved. No hardcoded credentials, no sensitive value logging, no sleeping payloads, no adversarial instructions |
| 8 | Coexistence & Recall | PASS ↩ carried | Specific triggers reduce false routing; coexistence note absent (LOW — does not fail category) |
| 9 | Model Compatibility | PASS ↩ carried | validated_on: Claude Sonnet 4.6 in frontmatter and Registry |
| 10 | Workflow & Feedback Loops | PASS ↩ carried | Confirm → run → review loop; 401/403 fix-and-retry paths; idempotency documented |
| 11 | Maintainability & Lifecycle | FAIL | [HIGH] No peer review / separation-of-duties requirement documented; [MEDIUM] versioning strategy missing; lifecycle stage documented as 'Test' (PASS) |
| 12 | Gotchas / Lessons Learned | PASS ↩ carried | 5 real-world gotchas: bot policy, HTTP/HTTPS, PowerShell, token expiry, .env git-ignore |
| 13 | Anti-Pattern Audit | PASS ↩ carried | Explicit error handling in script; no voodoo constants; requirements.txt missing (MEDIUM FAIL, but 3/4 MEDIUM = 75% ≥ 70% threshold) |

## Version Comparison (if comparator was run)

_Run: RUN_STEP_3C=true (scripts/publish_vocab.py changed vs prior snapshot 2026-05-07T140243Z). SKILL.md unchanged between snapshots._

| Eval | Verdict | Reasoning |
|------|---------|-----------|
| 1–13 | tie | SKILL.md identical between old and new trees; executor outputs functionally equivalent (13 ties, 0 new wins, 0 old wins) |

**Overall comparator result:** `tie` — no regression, no improvement in agent response quality from script-only change.

## History (all reviews)

| Reviewed at (ISO UTC) | Calendar | Unit Tests | Assertions | Categories | Verdict | Notes |
|----------------------|----------|------------|------------|------------|---------|-------|
| 2026-05-07T14:02:43Z | 2026-05-07 | 10/13 (77%) | 32/39 (82%) | 11/13 | FAIL | Initial review; Cat 7 HIGH: auth mismatch; Cat 11 HIGH: separation of duties |
| 2026-05-08T08:10:53Z | 2026-05-08 | 10/13 (77%) | 35/39 (90%) | 12/13 | FAIL | Incremental; Cat 7 resolved (script updated); Cat 11 HIGH still open |

Full narrative: `review-catalog-vocab-publisher-2026-05-08T081053Z.md`.
