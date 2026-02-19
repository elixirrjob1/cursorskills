# API Generic Module

Use this module when source data is exposed through HTTP APIs.

## Load Next

- Auth patterns: `references/apis/generic/auth.md`
- Schema mapping: `references/apis/generic/schema-mapping.md`
- Quality rules: `references/apis/generic/quality-rules.md`
- Discovered reference names: `references/apis/generic/discovered-references.md`

## Script

Use merged local API reader script:

```bash
.venv/bin/python scripts/apis/api_reader.py <base_url> --path /api/tables --output api_data.json
```

Have the agent detect source type and use this script directly for API sources.

When a reference name is confirmed, persist the name (only) in `references/apis/generic/discovered-references.md`.

## Execution Pattern

1. Discover endpoints and sample payloads.
2. Normalize payload entities into table-like structures.
3. Emit shared `schema.json` contract.
