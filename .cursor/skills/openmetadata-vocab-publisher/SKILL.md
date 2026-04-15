---
name: openmetadata-vocab-publisher
description: >-
  Publish a generated governance vocabulary Markdown file to OpenMetadata's Classifications
  and Tags API (Govern section). Use when asked to publish, push, sync, or upload a
  governance vocabulary to OpenMetadata. Handles deduplication — skips classifications
  and tags that already exist with unchanged descriptions, updates those that differ.
  Triggers: push vocabulary to OpenMetadata, publish classifications, sync governance
  tags, upload vocab to OpenMetadata, publish governance framework.
---

# OpenMetadata Vocabulary Publisher

Reads a governance vocabulary `.md` file (often produced by `governance-vocab-generator`) and
pushes it to OpenMetadata as Classifications and Tags under the Govern section. Skips
anything already in sync; updates descriptions that have changed.

## Prerequisites

- Python 3.8+
- `requests` library (`pip install requests`)
- OpenMetadata username (email) and password with Classification and Tag write access

## Workflow

1. Confirm the `.md` vocab file path, OpenMetadata base URL (no trailing slash), username, and password with the user. Do not echo or log the password.
2. From the repository root, run:

```bash
python .cursor/skills/openmetadata-vocab-publisher/scripts/publish_vocab.py \
  --file governance-vocabularies/<domain-slug>-governance-vocab.md \
  --base-url https://<your-openmetadata-host> \
  --username you@example.com \
  --password yourpassword
```

3. Review the printed summary — created / updated / skipped counts per classification and tag.
4. If errors occur, verify the base URL is reachable, credentials are correct, and the user has permission to create or update classifications and tags.

## What the script does

- Parses the `.md` file into Classifications and Tags using the vocabulary format.
- For each **Classification**: GETs by name first. If absent → creates. If present and `mutuallyExclusive` changed → updates. If identical → skips.
- For each **Tag** within a classification: GETs by FQN (`Classification.TagName`). Same create / update / skip logic.
- Prints a final summary table.

## Input file format

The script expects the standard governance vocabulary structure (compatible with `governance-vocab-generator` output):

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

See [reference.md](reference.md) for payload shapes, FQN format, and authentication.
