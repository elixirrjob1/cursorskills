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

The `DataGovernanceTerms` sheet is **fetched automatically** from OpenMetadata when `OM_BASE_URL` and `OM_TOKEN` are set (legacy aliases: `OPENMETADATA_BASE_URL`, `OPENMETADATA_JWT_TOKEN`). No extra flags or MCP calls are needed.

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

- **Install:** `pip install "openpyxl>=3.1,<4.0" "requests>=2.31,<3.0"` (`requests` is only required when OpenMetadata auto-fetch is used; omit it if always passing `--no-openmetadata` or `--glossary-json`).
- If `output_xlsx` is omitted, the script writes next to input with `.xlsx` extension.
- Reverse conversion applies visible sheet edits by default across the current workbook layout (`Summary`, `SourceSystem`, per-table worksheets, `DataQualityFindings`) and remains backward-compatible with legacy tabs.
- Non-exported fields remain intact because reconstruction starts from hidden full payload.
- Use `--no-apply-edits` for exact original payload restore.

## Gotchas

**Omitting `--no-openmetadata` when OM is slow.**
If `OM_BASE_URL` is set but the instance is unreachable or slow, the script will hang on the glossary fetch (10 s connect, 30 s read timeout per request). Pass `--no-openmetadata` to skip the fetch entirely when OM availability is uncertain.

**Editing `__rt_*` or `__dv_*` tabs is silently ignored.**
Hidden round-trip tabs (`__rt_meta`, `__rt_payload`) and the classification-validation helper sheet (`__dv_classifications`) are internal. Any values written to those tabs are never read back during reverse conversion — only the visible sheets are applied. Edit the visible tabs only.

**`requests` must be installed for auto-fetch.**
The `requests` library is imported lazily inside `_om_fetch_glossary_payload` (which reads `OM_TOKEN` via `om_auth.py`). If it is not installed and `--no-openmetadata` / `--glossary-json` are not set, the script will raise an `ImportError` at runtime. Install it or use one of the two skip flags.

**Legacy workbooks without `__rt_*` tabs.**
`excel_to_json.py` falls back to applying visible sheet edits only when no `__rt_meta` sheet is found. The reconstructed JSON will lack any fields that were not visible in the workbook — this is expected and documented behaviour.

## Registry

| Field | Value |
|-------|-------|
| Owner | Platform / Data Engineering team |
| Reviewer | Peer review required before merging changes to `main` |
| Version | Pinned to `main`; changes via PR with peer review |
| Lifecycle stage | **Deploy / Monitor** — actively used; iterate on failures via PR |
| Last evaluated | 2026-05-07 (`run_slug: 2026-05-07T124559Z`) |
| Dependencies | `scripts/json_to_excel.py`, `scripts/excel_to_json.py`, `openpyxl>=3.1`, `requests>=2.31` (optional) |

## Model Compatibility

This skill executes pre-built Python scripts — no LLM inference is involved in the conversion itself. The agent's role is limited to resolving file paths and running the correct command with the correct flags. Validated on Claude 3.5 Sonnet; expected to work on all model tiers since the task is command construction only.
