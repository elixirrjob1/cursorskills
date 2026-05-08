# catalog-vocab-publisher

> **Cursor Agent Skill** · Owner: Data Platform team · Version: 1.0.0 · Lifecycle: Test

Reads a governance vocabulary `.md` file (produced by [`governance-vocab-generator`](../governance-vocab-generator)) and pushes it to OpenMetadata as **Classifications and Tags** under the Govern section. Skips anything already in sync; updates descriptions that have changed.

---

## Latest Review — 2026-05-08T09:11:30Z

| Metric | Result |
|--------|--------|
| **Overall Verdict** | ✅ **PASS** |
| Unit Tests | **13 / 13 (100%)** |
| Assertions | **39 / 39 (100%)** |
| Categories | **13 / 13 (100%)** |
| High Failures | — |
| Medium Failures | — |
| Comparator | `new_wins` (1 improvement vs prior snapshot) |

Full benchmark: [`tests/results/benchmark-catalog-vocab-publisher-latest.md`](tests/results/benchmark-catalog-vocab-publisher-latest.md)

### Unit Test Results

| # | Test | Type | Result |
|---|------|------|--------|
| 1 | Publish governance vocabulary to OpenMetadata | should-trigger | ✅ PASS |
| 2 | Push retail-governance-vocab.md to data catalog | should-trigger | ✅ PASS |
| 3 | Sync governance tags — re-run the vocab publisher | should-trigger | ✅ PASS |
| 4 | 401 Unauthorized when publishing vocabulary | should-trigger | ✅ PASS |
| 5 | 403 Forbidden from publish script | should-trigger | ✅ PASS |
| 6 | Upload governance framework classifications | should-trigger | ✅ PASS |
| 7 | Tag descriptions changed — push updates | should-trigger | ✅ PASS |
| 8 | Push vocabulary / publish classifications (exact phrase) | should-trigger | ✅ PASS |
| 9 | Generate governance vocabulary for retail domain | should-not-trigger | ✅ PASS |
| 10 | Run metadata ingestion from Snowflake into OpenMetadata | should-not-trigger | ✅ PASS |
| 11 | "Publish my vocab" (no file path) | edge-case | ✅ PASS |
| 12 | Publish but service account not set up | edge-case | ✅ PASS |
| 13 | SSL errors when publishing | edge-case | ✅ PASS |

### Review History

| Reviewed at (UTC) | Unit Tests | Assertions | Categories | Verdict |
|-------------------|------------|------------|------------|---------|
| 2026-05-07T14:02:43Z | 10/13 (77%) | 32/39 (82%) | 11/13 (85%) | ❌ FAIL |
| 2026-05-08T08:10:53Z | 10/13 (77%) | 35/39 (90%) | 12/13 (92%) | ❌ FAIL |
| 2026-05-08T08:39:26Z | 10/13 (77%) | 35/39 (90%) | 13/13 (100%) | ✅ PASS |
| 2026-05-08T08:55:26Z | 12/13 (92%) | 38/39 (97%) | 13/13 (100%) | ✅ PASS |
| **2026-05-08T09:11:30Z** | **13/13 (100%)** | **39/39 (100%)** | **13/13 (100%)** | ✅ **PASS** |

---

## Quick Start

### Prerequisites

- Python 3.8+
- `pip install -r requirements.txt` (pins `requests==2.33.1`)
- OpenMetadata instance reachable over HTTP (dev) or HTTPS (production)
- `vocab-publisher-bot` service account with `VocabPublisherPolicy` (Classification + Tag: Create, EditAll, ViewAll)
- Environment variables set:

| Variable | Description |
|----------|-------------|
| `OM_BASE_URL` | OpenMetadata base URL — no trailing slash (e.g. `https://<host>:8585`) |
| `OM_TOKEN` | Long-lived JWT token for the `vocab-publisher-bot` service account |

### Run

```bash
python scripts/publish_vocab.py \
  --file governance-vocabularies/<domain-slug>-governance-vocab.md
```

### Example output

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

The script is **idempotent** — safe to re-run at any time. Zero writes if the vocabulary is unchanged.

---

## Skill Triggers

This skill fires on: `publish`, `push`, `sync`, `upload` + vocab / classification / tag language.

It does **not** fire on: `generate`, `create`, `ingest`, `sync database`, or `tag columns`.

### Adjacent skills

| Skill | Purpose |
|-------|---------|
| `governance-vocab-generator` | **Creates** the `.md` vocabulary from a domain description — use this first |
| `catalog-vocab-publisher` | **Publishes** an existing `.md` vocab to OpenMetadata — use this after generation |
| `catalog-sync` | Full database/schema metadata ingestion — unrelated to vocab publishing |
| `catalog-glossary-tagger` | Assigns glossary terms to catalogued assets — runs after catalog-sync |

---

## Files

```
SKILL.md                          ← full skill instructions for the Cursor agent
reference.md                      ← OpenMetadata API reference (auth, payloads, FQN format)
requirements.txt                  ← pinned Python dependencies
scripts/publish_vocab.py          ← publish script
tests/test-cases.md               ← behavioural assertions (should-trigger / should-not-trigger / edge)
tests/test_skill.py               ← pytest-compatible unit test stubs
tests/evals/evals.json            ← machine-readable eval suite
tests/results/history.json        ← all review runs
tests/results/benchmark-*-latest.md   ← latest benchmark report (markdown)
tests/results/benchmark-*-latest.html ← latest benchmark report (HTML)
```

---

## Security

- Auth uses `OM_TOKEN` (Bot JWT) from environment — no username/password ever passed
- `.env` is git-ignored; never commit it
- In CI/CD inject `OM_TOKEN` as a pipeline secret (GitHub Actions, Azure DevOps, or Azure Key Vault)
- Never log or echo the token value

---

## Registry

| Field | Value |
|-------|-------|
| Owner | Data Platform team |
| Reviewer | Peer or tech-lead — skill author must not be sole PR approver |
| Version | 1.0.0 |
| Validated on | Claude Sonnet 4.6 |
| Source | `elixirrjob1/cursorskills` · `.cursor/skills/catalog-vocab-publisher/` |
| Mirror | `responsum-team/skill-catalog-vocab-publisher` (this repo) |
