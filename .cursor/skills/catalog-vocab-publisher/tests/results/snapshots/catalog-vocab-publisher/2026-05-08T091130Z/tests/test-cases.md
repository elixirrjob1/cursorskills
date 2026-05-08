# Eval Suite: catalog-vocab-publisher

_Re-run this suite after every skill update to catch regressions._

## Should-trigger

| # | Input | Expected behavior |
|---|-------|-------------------|
| 1 | "Publish my governance vocabulary to OpenMetadata" | Skill triggers; confirms .md file path and that OM_BASE_URL + OM_TOKEN are set; runs publish_vocab.py with --file argument |
| 2 | "Push my retail-governance-vocab.md to the data catalog" | Skill triggers; acknowledges the file path from the prompt; confirms or asks about OM_BASE_URL + OM_TOKEN before showing the run command (Step 1 gating); uses OM_TOKEN JWT (not username/password) |
| 3 | "Sync our governance tags — re-run the vocab publisher" | Skill triggers; Step 1 gating applies — asks for the .md file path since none was provided; notes the re-run is idempotent (safe, zero writes if vocab unchanged) |
| 4 | "I got 401 Unauthorized when publishing the vocabulary" | Skill triggers; instructs user to regenerate bot token under Settings → Bots → vocab-publisher-bot and update OM_TOKEN |
| 5 | "The publish script returned 403 Forbidden" | Skill triggers; instructs user to verify VocabPublisherPolicy (Create, EditAll, ViewAll) is attached to the bot under Settings → Bots → vocab-publisher-bot → Roles |
| 6 | "Upload our governance framework classifications to OpenMetadata" | Skill triggers; asks for .md file path; runs full publish workflow |
| 7 | "Our tag descriptions have changed — push the updates" | Skill triggers; Step 1 gating applies — asks for the .md file path since none was provided; references the publish workflow where changed descriptions are UPDATED and unchanged ones are SKIPPED |
| 8 | "Push vocabulary to OpenMetadata, publish classifications" | Skill triggers (exact trigger phrase match); proceeds with publish workflow |

## Should-not-trigger

| # | Input | Expected behavior |
|---|-------|-------------------|
| 1 | "Generate a governance vocabulary for the retail domain" | Should NOT trigger; governance-vocab-generator skill should handle this instead |
| 2 | "Run a metadata ingestion from Snowflake into OpenMetadata" | Should NOT trigger; catalog-sync skill should handle this instead |
| 3 | "Show me what classifications exist in my OpenMetadata catalog" | Should NOT trigger; direct OpenMetadata MCP query or catalog-sync skill handles read-only inspection |

## Edge cases

| # | Input | Expected behavior |
|---|-------|-------------------|
| 1 | "Publish my vocab" with no file path provided | Skill triggers but stops to ask for the .md file path before running (Step 1 requires confirming file path with user) |
| 2 | "I want to publish but haven't set up the service account yet" | Skill triggers; surfaces Prerequisites section (vocab-publisher-bot account, VocabPublisherPolicy, OM_TOKEN env var) and stops until user confirms setup |
| 3 | "I get SSL errors when trying to publish" | Skill triggers; cites HTTP vs HTTPS Gotcha — checks whether OM_BASE_URL uses http:// vs https:// and advises correcting the scheme to match server config |
