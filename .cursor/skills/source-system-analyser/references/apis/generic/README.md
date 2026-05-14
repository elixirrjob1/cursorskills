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

## Scoping Mode

For full endpoint discovery and response-key inspection — including OpenAPI spec parsing, live probing, entity mapping, and output file generation — follow:

`references/apis/generic/endpoint-scoping.md`

This mode is triggered by the API Preflight in `SKILL.md` whenever the user asks to scope, discover endpoints, analyze, or ingest an API source.

## Execution Pattern

1. Run API Preflight (SKILL.md) to produce `api-scope-config.json`.
2. Follow `references/apis/generic/endpoint-scoping.md` for full scoping procedure.
3. Discover endpoints and sample payloads.
4. Normalize payload entities into table-like structures.
5. Emit `endpoint_catalog.json` and shared `schema.json` contract to `.cursor/flat/`.
