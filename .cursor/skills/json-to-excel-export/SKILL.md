---
name: json-to-excel-export
description: Convert analyzer schema JSON files into a styled Excel workbook for review. Use when the user asks to export `.json` schema output (including units, descriptions, join candidates, and sample data) to `.xlsx`.
---

# JSON To Excel Export

## Overview

Use this skill to convert schema analyzer JSON into clean Excel tabs for filtering and QA. It preserves table metadata, column metadata, join candidates, unit context, and sample data without raw payload blobs.

## Run

```bash
.venv/bin/python .cursor/skills/json-to-excel-export/scripts/json_to_excel.py \
  <input_json> \
  <output_xlsx>
```

Example:

```bash
.venv/bin/python .cursor/skills/json-to-excel-export/scripts/json_to_excel.py \
  .cursor/flat/schema_postgres_keyvault_public_public_postgresql.json \
  .cursor/flat/schema_postgres_keyvault_public_public_postgresql.xlsx
```

### OpenMetadata Glossary Terms

The `DataGovernanceTerms` sheet is **fetched automatically** from OpenMetadata when the env vars `OPENMETADATA_BASE_URL` and auth credentials (`OPENMETADATA_EMAIL` / `OPENMETADATA_PASSWORD` or `OPENMETADATA_JWT_TOKEN`) are set. No extra flags or MCP calls are needed.

To **skip** the automatic fetch:

```bash
.venv/bin/python .cursor/skills/json-to-excel-export/scripts/json_to_excel.py \
  <input_json> <output_xlsx> --no-openmetadata
```

To provide a **pre-fetched** glossary file instead:

```bash
.venv/bin/python .cursor/skills/json-to-excel-export/scripts/json_to_excel.py \
  <input_json> <output_xlsx> --glossary-json <glossary_json>
```

If OpenMetadata is unreachable or credentials are missing, the script continues without the sheet.

Reverse (Excel back to JSON):

```bash
.venv/bin/python .cursor/skills/json-to-excel-export/scripts/excel_to_json.py \
  <input_xlsx> \
  <output_json>
```

Restore original payload only (ignore visible sheet edits):

```bash
.venv/bin/python .cursor/skills/json-to-excel-export/scripts/excel_to_json.py \
  <input_xlsx> \
  <output_json> \
  --no-apply-edits
```

## Output Shape

Workbook tabs:
- `Summary`
- `SourceSystem`
- `DataQualityFindings`
- `DataGovernanceTerms` (when `--glossary-json` is provided)
- one worksheet per source table
- hidden round-trip tabs (`__rt_*`)

Formatting:
- Styled header row
- Frozen header pane
- Auto-filter enabled
- Auto-sized columns
- Nested JSON fields are flattened into readable columns/text (no raw payload column)
- Any nested JSON/list field on any sheet is emitted as its own table block on that same sheet.
- Hidden round-trip tabs (`__rt_*`) store the full original JSON payload for lossless reconstruction.

## Notes

- Requires `openpyxl` in the active environment.
- If `output_xlsx` is omitted, the script writes next to input with `.xlsx` extension.
- Reverse conversion applies visible sheet edits by default across the current workbook layout (`Summary`, `SourceSystem`, per-table worksheets, `DataQualityFindings`) and remains backward-compatible with legacy tabs.
- Non-exported fields remain intact because reconstruction starts from hidden full payload.
- Use `--no-apply-edits` for exact original payload restore.
