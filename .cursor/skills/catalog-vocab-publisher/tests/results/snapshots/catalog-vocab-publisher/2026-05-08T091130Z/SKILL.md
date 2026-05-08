---
name: catalog-vocab-publisher
description: >-
  Publish a generated governance vocabulary Markdown file to OpenMetadata's Classifications
  and Tags API (Govern section). Use when asked to publish, push, sync, or upload a
  governance vocabulary to OpenMetadata. Handles deduplication — skips classifications
  and tags that already exist with unchanged descriptions, updates those that differ.
  Triggers: push vocabulary to OpenMetadata, publish classifications, sync governance
  tags, upload vocab to OpenMetadata, publish governance framework.
version: 1.0.0
lifecycle: Test
validated_on: Claude Sonnet 4.6
---

# OpenMetadata Vocabulary Publisher

Reads a governance vocabulary `.md` file (produced by `governance-vocab-generator`) and
pushes it to OpenMetadata as Classifications and Tags under the Govern section. Skips
anything already in sync; updates descriptions that have changed.

---

## Registry

| Field | Value |
|---|---|
| Owner | Data Platform team |
| Reviewer | Peer or tech-lead — skill author must **not** be the sole approver of any PR that modifies this skill. A second reviewer must inspect the diff and approve via the normal PR process before merge. |
| Version | 1.0.0 |
| Version bump policy | Patch (x.x.N) for bug fixes and doc updates; Minor (x.N.0) for new workflow steps or script changes; Major (N.0.0) for breaking auth or API changes. Tag the commit `catalog-vocab-publisher-vX.Y.Z`. |
| Rollback plan | Revert to the previous tagged commit on `main`. The secondary skill repo (`responsum-team/skill-catalog-vocab-publisher`) mirrors via the standard rsync-push sync — revert there too using the approved copy-and-commit process. |
| Lifecycle | Test |
| Validated on | Claude Sonnet 4.6 |
| Dependencies | Python 3.8+, `requests==2.33.1` (pinned in `requirements.txt`), OpenMetadata instance, `vocab-publisher-bot` service account |
| Last reviewed | 2026-05-08 |

---

## Prerequisites

- Python 3.8+
- `requests` library — install from the pinned lockfile: `pip install -r requirements.txt`
- OpenMetadata instance reachable over HTTP (dev) or HTTPS (production — required)
- `vocab-publisher-bot` service account with `VocabPublisherPolicy` (Classification + Tag: Create, EditAll, ViewAll)
- `OM_BASE_URL` and `OM_TOKEN` set in environment (see Configuration below)

## Configuration — environment variables

| Variable | Description |
|---|---|
| `OM_BASE_URL` | OpenMetadata base URL — no trailing slash (e.g. `https://<host>:8585`) |
| `OM_TOKEN` | Long-lived JWT token for the `vocab-publisher-bot` service account |

Copy `.env.example` at the repo root to `.env` and fill in your values. Load into PowerShell:

```powershell
$line = Get-Content .env | Select-String "^OM_BASE_URL="
$env:OM_BASE_URL = ($line.Line -split '=', 2)[1].Trim()
$line = Get-Content .env | Select-String "^OM_TOKEN="
$env:OM_TOKEN = ($line.Line -split '=', 2)[1].Trim()
```

## Workflow

1. Confirm the `.md` vocab file path with the user and verify `OM_BASE_URL` and `OM_TOKEN` are set. If the user signals a re-run (e.g. "re-run", "sync again", "run it again"), acknowledge upfront that re-runs are safe and idempotent — no writes occur if the vocabulary is unchanged.
2. Run the publish script:

```bash
python .cursor/skills/catalog-vocab-publisher/scripts/publish_vocab.py \
  --file governance-vocabularies/<domain-slug>-governance-vocab.md
```

3. Review the printed output — one line per classification and tag:

```
Parsing: governance-vocabularies/retail-governance-vocab.md
Found 8 classification(s)

  [SKIPPED] Classification: Architecture (no change)
    [CREATED] Tag: Retention.Financial-Statutory
    [UPDATED] Tag: QualityTrust.Reconciled (description changed)
    [SKIPPED] Tag: Privacy.AnonymousAggregate (no change)

==================================================
Summary
==================================================
  Classifications :  0 created   0 updated   8 skipped
  Tags            : 19 created  12 updated   0 skipped
```

4. If `401 Unauthorized` is returned, the bot token has expired or been revoked — regenerate it under **Settings → Bots → vocab-publisher-bot** and update `OM_TOKEN`.
5. If `403 Forbidden` is returned, the bot is missing write permissions — verify `VocabPublisherPolicy` is attached under **Settings → Bots → vocab-publisher-bot → Roles**.

## What the script does

- Parses the `.md` file into Classifications and Tags using the vocabulary format.
- For each **Classification**: GETs by name first. If absent → creates. If present and `mutuallyExclusive` changed → updates. If identical → skips.
- For each **Tag** within a classification: GETs by FQN (`Classification.TagName`). Same create / update / skip logic.
- Prints a final summary table.
- The script is **idempotent** — safe to re-run at any time. Running twice against unchanged vocabulary produces zero writes.

## Input file format

The script expects the exact output format of `governance-vocab-generator`:

```
## ClassificationName          ← becomes a Classification
*Mutually exclusive*            ← mutuallyExclusive: true
*Multi-select*                  ← mutuallyExclusive: false
### TagName                     ← becomes a Tag under the current Classification
Plain text description line     ← tag description
> Domain context blockquote     ← appended to description after a newline
---                             ← end of classification block
```

## API reference

See [reference.md](reference.md) for payload shapes, FQN format, and auth details.

---

## Gotchas

- **Bot role is not enough on its own — a custom policy is required.** The built-in `Data Steward` role only includes Edit operations, not Create. The bot needs `VocabPublisherPolicy` (Classification + Tag: Create, EditAll, ViewAll) attached via a dedicated `VocabPublisherRole`. Without it, the script will succeed on reads and fail with `403` on the first tag write.

- **HTTP vs HTTPS.** If `OM_BASE_URL` starts with `https://` but the server is running plain HTTP, `requests` will raise a TLS handshake error ("underlying connection was closed"). Use `http://` for local/dev instances and `https://` only when TLS is configured on the server.

- **PowerShell env var loading.** Multi-line `ForEach-Object` blocks pasted into an interactive PowerShell session enter continuation (`>>`) mode and may require Ctrl+C to exit without executing. Use the single-line `Select-String` form shown in the Configuration section above.

- **Token expiry.** Bot tokens with no expiry date are convenient for development but should be replaced with a 90-day rotating token before production. Set a calendar reminder to regenerate and update `OM_TOKEN` in your secret store before expiry.

- **`.env` is git-ignored.** The `.env` file must never be committed. Only `.env.example` (with placeholders) is tracked. Run `git status` and confirm `.env` does not appear before any commit.

---

## Credential Security

- Use environment variable references (`OM_TOKEN`, `OM_BASE_URL`) — never literal values in commands or code
- Never log, display, or echo the token value
- `.env` is listed in `.gitignore` — never commit it
- In CI/CD, inject `OM_TOKEN` as a pipeline secret variable (GitHub Actions Secrets, Azure DevOps Secret Variables, or Azure Key Vault)

---

## Coexistence & Routing

This skill has a narrow, terminal job: **publish** a vocab file that already exists on disk. Do not confuse it with adjacent skills:

| Skill | Purpose |
|---|---|
| `governance-vocab-generator` | **Creates** the governance vocabulary `.md` file from a domain description. Use this first. |
| `catalog-vocab-publisher` | **Publishes** an existing `.md` vocab file to OpenMetadata Classifications and Tags. Use this after generation. |
| `catalog-sync` | Configures and runs full database/schema metadata ingestion into OpenMetadata. Unrelated to vocabulary publishing. |
| `catalog-glossary-tagger` | Assigns glossary terms to already-catalogued table/column assets. Runs after catalog-sync, not after vocab publishing. |

**Trigger precision:** This skill fires on "publish", "push", "sync", "upload" vocab/classification/tag language. It does **not** fire on "generate", "create", "ingest", "sync database", or "tag columns".
