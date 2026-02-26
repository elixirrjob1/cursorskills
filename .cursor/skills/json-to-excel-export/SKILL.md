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
- `Tables`
- `Columns`
- `JoinCandidates`
- `ForeignKeys`
- `SampleData`
- `Units`

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
- Reverse conversion applies visible sheet edits by default across all exported tabs (`Summary`, `SourceContextManual`, `DataQualityFindings`, `latearivingdata`, `Tables`, `Columns`, `JoinCandidates`, `ForeignKeys`, `SampleData`, `Units`).
- Non-exported fields remain intact because reconstruction starts from hidden full payload.
- Use `--no-apply-edits` for exact original payload restore.
