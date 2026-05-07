# Benchmark Report: catalog-vocab-publisher
_Generated: 2026-05-07T14:02:43Z UTC · First review — full run, no prior snapshot_

## Summary

| Metric | Value |
|--------|-------|
| Overall Verdict | FAIL |
| Unit Tests | 10 / 13 passed (77%) |
| Assertions | 32 / 39 (82%) |
| Categories | 11 / 13 passed |
| High Failures | 2 |
| Medium Failures | 2 |
| Comparator | — (first review, no prior snapshot) |

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
| 1 | Response asks for or confirms the .md vocabulary file path | ✅ | "Which vocabulary file do you want to publish?" with available file list |
| 1 | Response references OM_BASE_URL and/or OM_TOKEN environment variables | ✅ | Both OM_BASE_URL and OM_TOKEN explicitly mentioned |
| 1 | Response does NOT use --username / --password flags | ✅ | No such flags in response |
| 2 | Response includes a publish_vocab.py command with --file argument | ❌ | Agent correctly in Step 1; command not shown — asked for file confirmation first |
| 2 | Response references OM_TOKEN (JWT / service account token) | ✅ | OM_TOKEN referenced as JWT token |
| 2 | Response does NOT include --username or --password flags in the run command | ✅ | No such flags |
| 3 | Response states or implies the script is idempotent / safe to re-run | ✅ | "CREATED / UPDATED / SKIPPED output" implies no destructive overwrites |
| 3 | Response includes the publish_vocab.py command | ❌ | Agent in Step 1 confirmation phase; command not shown |
| 3 | Response does not warn that re-running will cause destructive changes | ✅ | No destructive warning |
| 4 | Response references the bot token expiry / revocation as cause of 401 | ✅ | "bot token has expired or been revoked" stated explicitly |
| 4 | Response mentions regenerating the token under Settings → Bots or vocab-publisher-bot | ✅ | "Settings → Bots → vocab-publisher-bot" cited |
| 4 | Response mentions updating OM_TOKEN after regeneration | ✅ | "replace the old value with the new token" in OM_TOKEN |
| 5 | Response explains 403 is a permissions error (not auth) | ✅ | "authenticated successfully (token is valid) but lacks write permissions" |
| 5 | Response mentions VocabPublisherPolicy or equivalent custom policy | ✅ | VocabPublisherPolicy with Create/EditAll/ViewAll named |
| 5 | Response directs user to Settings → Bots → vocab-publisher-bot → Roles | ✅ | Exact UI path stated |
| 6 | Response asks for or confirms the vocabulary .md file path | ✅ | File path requested before proceeding |
| 6 | Response includes or references the publish_vocab.py script invocation | ✅ | "I'll run the publish script and review the output with you" |
| 6 | Response does NOT attempt to generate a vocabulary (that is a different skill) | ✅ | No generation attempted |
| 7 | Response explains that changed descriptions will be updated and unchanged ones skipped | ❌ | UPDATED vs SKIPPED behavior not explained in response |
| 7 | Response includes publish_vocab.py command | ❌ | Agent in Step 1 confirmation phase; no command shown |
| 7 | Response does NOT say all tags will be deleted and recreated | ✅ | No deletion/recreation mentioned |
| 8 | Response engages with the publish workflow (confirms file path and/or env vars) | ✅ | File path and env vars confirmed |
| 8 | Response does NOT deflect to a different skill | ✅ | Remained in publish workflow |
| 8 | Response does NOT ask clarifying questions about whether to generate vs publish | ✅ | No generate-vs-publish confusion |
| 9 | Response does NOT attempt to run publish_vocab.py | ✅ | No publish actions |
| 9 | Response indicates this is a vocabulary generation task (not publishing) | ✅ | Generated retail domain vocabulary content |
| 9 | Response does NOT ask for OM credentials or base URL | ✅ | No OM credentials requested |
| 10 | Response does NOT run or reference publish_vocab.py | ✅ | Ran catalog ingestion pipeline instead |
| 10 | Response treats this as a catalog ingestion / sync task | ✅ | Triggered Snowflake ingestion pipeline via MCP |
| 10 | Response does NOT ask for a vocabulary .md file | ✅ | Worked with Snowflake service and pipeline |
| 11 | Response asks for the vocabulary .md file path | ✅ | "Can you share the path to the .md vocab file?" |
| 11 | Response does NOT attempt to run the script without knowing the file path | ✅ | Stopped at Step 1 |
| 11 | Response checks or asks about OM_BASE_URL and OM_TOKEN | ✅ | "Do you have those configured already?" |
| 12 | Response lists prerequisite setup steps (service account and/or OM_TOKEN) | ✅ | 4-step guide: create policy → role → bot → env vars |
| 12 | Response references vocab-publisher-bot or equivalent service account | ✅ | vocab-publisher-bot explicitly named |
| 12 | Response does NOT proceed to run the script before prerequisites are confirmed | ✅ | "Once those are in place, come back…" |
| 13 | Response links SSL/TLS errors to the HTTP vs HTTPS mismatch | ✅ | "OM_BASE_URL is likely set to https:// but server is running plain HTTP" |
| 13 | Response advises checking or correcting the OM_BASE_URL scheme (http:// vs https://) | ✅ | Exact before/after correction shown |
| 13 | Response does NOT suggest reinstalling requests or Python as primary fix | ✅ | Only OM_BASE_URL scheme fix recommended |

## Category Grades

| # | Category | Grade | Explanation |
|---|----------|-------|-------------|
| 1 | Triggering (Description Quality) | PASS | Valid name, what/when documented, 5 explicit trigger phrases |
| 2 | Anatomy & Structure | PASS | ~139 lines, valid frontmatter with extended fields, shallow references |
| 3 | Instructions Clarity | PASS | Step-by-step workflow, 401/403 branches explicit, consistent terms |
| 4 | Output Quality | PASS | Exact CREATED/UPDATED/SKIPPED example output shown |
| 5 | Testability | PASS | Tests created this run; all HIGH subcategories satisfied |
| 6 | Resource Efficiency | PASS | Pre-built script, deterministic invocation, no over-explanation |
| 7 | Security & Trust | FAIL | [HIGH] publish_vocab.py uses --username/--password; SKILL.md says OM_TOKEN; run command incomplete |
| 8 | Coexistence & Recall | PASS | Specific triggers; no coexistence issue at HIGH/MEDIUM level |
| 9 | Model Compatibility | PASS | validated_on Claude Sonnet 4.6 in frontmatter and Registry |
| 10 | Workflow & Feedback Loops | PASS | Confirm→run→review loop; 401/403 fix paths present |
| 11 | Maintainability & Lifecycle | FAIL | [HIGH] No peer review/separation-of-duties; [MEDIUM] versioning strategy missing |
| 12 | Gotchas / Lessons Learned | PASS | 5 gotchas covering real operational failures |
| 13 | Anti-Pattern Audit | PASS | No magic constants; explicit error handling; requirements.txt missing (MEDIUM, 3/4 pass) |

## Version Comparison (if comparator was run)

_Not run — first review, no prior snapshot available._

## History (all reviews)

| Reviewed at (ISO UTC) | Calendar | Unit Tests | Assertions | Categories | Verdict | Notes |
|----------------------|----------|------------|------------|------------|---------|-------|
| 2026-05-07T14:02:43Z | 2026-05-07 | 10/13 | 32/39 | 11/13 | FAIL | First review — script/SKILL.md auth mismatch; separation of duties missing |

Full narrative: `review-catalog-vocab-publisher-2026-05-07T140243Z.md`.
