# Flat Generic Module

Use this module for CSV, TSV, JSON, NDJSON, and Excel schema files.

## Load Next

- Format detection: `references/flat/generic/format-detection.md`
- Schema mapping: `references/flat/generic/schema-mapping.md`
- Quality rules: `references/flat/generic/quality-rules.md`

## Scripts

Use merged tabular schema converter:

```bash
.venv/bin/python scripts/flat/tabular_schema_json.py to-json --columns-file schema_columns.csv --output schema_from_tabular.json --default-schema public
```

Inspect first:

```bash
.venv/bin/python scripts/flat/tabular_schema_json.py inspect --columns-file schema_columns.xlsx --sample-size 5 --output tabular_inspect.json
```

Have the agent detect source type and use this script directly for CSV/Excel flat sources.

## Execution Pattern

1. Identify format and relevant sheets/files.
2. Infer column schema from headers and sampled rows.
3. Normalize into shared `schema.json` contract.
