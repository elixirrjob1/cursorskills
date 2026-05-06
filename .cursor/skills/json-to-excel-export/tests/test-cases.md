# Eval Suite: json-to-excel-export

_Re-run this suite after every skill update to catch regressions._

## Should-trigger

| # | Input | Expected behavior |
|---|-------|-------------------|
| 1 | "Export my schema JSON to Excel" with a schema.json file present | Tests standard conversion: agent runs json_to_excel.py with input and output path, produces workbook with Summary, SourceSystem, DataQualityFindings, and per-table tabs |
| 2 | "Convert schema.json to Excel but skip the OpenMetadata glossary sheet" | Tests --no-openmetadata flag: agent runs json_to_excel.py with --no-openmetadata, does not attempt to fetch glossary, produces workbook without DataGovernanceTerms tab |
| 3 | "I have a pre-fetched glossary file, use that instead of fetching from OpenMetadata" with glossary.json provided | Tests --glossary-json flag: agent runs json_to_excel.py with --glossary-json <path>, uses provided file instead of live fetch |
| 4 | "Convert the Excel workbook back to JSON, applying my edits" with an .xlsx file | Tests reverse conversion: agent runs excel_to_json.py with input xlsx and output json, applies visible sheet edits by default |
| 5 | "Restore the original JSON from this Excel file, ignoring any edits I made" | Tests --no-apply-edits: agent runs excel_to_json.py with --no-apply-edits, restores from hidden __rt_* round-trip tabs, ignores visible cell changes |

## Should-not-trigger

| # | Input | Expected behavior |
|---|-------|-------------------|
| 1 | "Export a report of top 10 customers from Snowflake to Excel" | Should be handled by natural-language-data-query (data retrieval) — not a schema JSON export |
| 2 | "Generate a dbt model and export it to Excel" | Should be handled by dbt-model-from-stm — not a schema JSON conversion task |
| 3 | "Convert my Python script to an Excel file" | General task — agent answers from general knowledge, does not invoke this skill |

## Edge cases

| # | Input | Expected behavior |
|---|-------|-------------------|
| 1 | "Export schema.json to Excel" with no output path specified | Tests default output behavior: agent runs json_to_excel.py without output_xlsx argument — script writes .xlsx next to input file with same base name |
| 2 | "Export schema.json to Excel" but OPENMETADATA_BASE_URL is not set / OpenMetadata unreachable | Tests graceful degradation: agent runs conversion, OpenMetadata fetch is skipped silently, workbook produced without DataGovernanceTerms tab — no error raised |
| 3 | "Convert my Excel back to JSON" with a legacy-format workbook (no __rt_* tabs) | Tests backward compatibility: agent runs excel_to_json.py, applies edits from visible tabs only, works without hidden round-trip tabs |
