# Test API Module

Use this module for the current test API environment.

## Load Next

- Auth details: `references/apis/test-api/auth.md`
- Endpoints and examples: `references/apis/test-api/endpoints.md`
- Mapping/quality guidance: `references/apis/test-api/schema-mapping.md`
- Discovered reference names: `references/apis/test-api/discovered-references.md`

## Preferred Script

```bash
.venv/bin/python scripts/apis/test_api/test_api_reader.py --output api_test_data.json
```

This wrapper targets the test API base URL by default and calls `scripts/apis/api_reader.py` under the hood.

When a reference name is confirmed during execution, persist the name (only) in `references/apis/test-api/discovered-references.md`.

## Analyzer Step (required for schema output)

After downloading discovery and per-table payload files, run:

```bash
.venv/bin/python scripts/apis/api_analyzer.py --discovery api_discovery.json --data-dir ./api_data --base-url "$API_BASE_URL" --output schema_api.json
```

Expected inputs for analyzer:
- discovery file must include `tables` list
- `data-dir` must contain one file per table (filename = table name) with `data` array payload

## Direct Script (advanced)

```bash
.venv/bin/python scripts/apis/api_reader.py "$API_BASE_URL" --path /api/tables --path /api/customers --output api_test_data.json
```
