---
name: tabular-schema-json
description: Converts spreadsheet/tabular schema inputs (.csv/.xlsx) into schema.json-compatible output for analysis, and exports schema.json into CSV templates (tables + columns). Use when user has schema metadata in Excel/CSV and needs JSON for AI analysis.
---

# Tabular Schema JSON

Use this skill when schema metadata is in Excel/CSV and needs to be analyzed like normal JSON.

## Required Inputs

Collect from user:
- Path to tabular file(s): columns file (required), tables file (optional)
- Output JSON path
- Optional default schema name

## Supported Inputs

- `.csv`
- `.xlsx` (requires `openpyxl`)

## Script

### 1) Inspect first (recommended)

Run this first to list headers and sample rows, then decide mapping/conversion:

```bash
python3 .cursor/skills/tabular-schema-json/scripts/tabular_schema_json.py inspect \
  --columns-file schema_columns.csv \
  --tables-file schema_tables.csv \
  --sample-size 5 \
  --output tabular_inspect.json
```

### 2) Convert tabular files to schema JSON

```bash
python3 .cursor/skills/tabular-schema-json/scripts/tabular_schema_json.py to-json \
  --columns-file schema_columns.csv \
  --tables-file schema_tables.csv \
  --output schema_from_tabular.json \
  --default-schema public
```

### 3) Export existing schema.json to CSV templates

```bash
python3 .cursor/skills/tabular-schema-json/scripts/tabular_schema_json.py from-json \
  --schema-json schema.json \
  --columns-out schema_columns_template.csv \
  --tables-out schema_tables_template.csv
```

## Columns File Format (minimum)

Required headers:
- `table`
- `column`
- `type`

Common optional headers:
- `nullable`
- `is_incremental`
- `primary_key`
- `foreign_key` (format: `referenced_table.referenced_column`)
- `cardinality`
- `null_count`
- `min`
- `max`
- `data_category`
- `schema`
- Alias headers are also accepted, including:
- `table_name`, `column_name`, `data_type`, `schema_name`, `pk`, `fk`, `min_value`, `max_value`

## Tables File Format (optional)

Optional headers:
- `table`
- `schema`
- `row_count`
- `cdc_enabled`

## Notes

- If `tables` file is omitted, table-level fields are auto-filled with defaults.
- Output contains the same top-level structure used by `schema.json` (`metadata`, `connection`, `data_quality_summary`, `tables`).
