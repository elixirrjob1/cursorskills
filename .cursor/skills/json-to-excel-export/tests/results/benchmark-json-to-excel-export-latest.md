# Benchmark Report: json-to-excel-export

_Generated: 2026-05-07T12:56:12Z UTC · Incremental re-check — cats 11/12/13 re-evaluated, all 11 executors re-run (SKILL.md + script changed), comparator new_wins 3–0_

## Summary

| Metric | Value |
|--------|-------|
| Overall Verdict | **PASS** |
| Unit Tests | 11 / 11 passed (100%) |
| Assertions | 33 / 33 (100%) |
| Categories | 13 / 13 passed |
| High Failures | — |
| Medium Failures | — |
| Comparator | new_wins (3–0 vs snapshot 2026-05-07T124559Z) |

## Unit Test Results

| # | Test | Type | Result | Assertions | Evidence |
|---|------|------|--------|------------|----------|
| 1 | Export my schema JSON to Excel (schema.json present) | should-trigger | ✅ PASS | 3/3 | Ran json_to_excel.py; listed Summary, per-table sheets, `__rt_*` tabs |
| 2 | Convert schema.json to Excel but skip the OpenMetadata glossary sheet | should-trigger | ✅ PASS | 3/3 | Used `--no-openmetadata`; confirmed DataGovernanceTerms skipped |
| 3 | I have a pre-fetched glossary.json file, use that instead of fetching from OpenMetadata | should-trigger | ✅ PASS | 3/3 | Used `--glossary-json .cursor/flat/om_glossary_terms.json`; no live OM call |
| 4 | Convert the Excel workbook back to JSON, applying my edits | should-trigger | ✅ PASS | 3/3 | Ran excel_to_json.py; no `--no-apply-edits`; output JSON confirmed |
| 5 | Restore the original JSON from this Excel file, ignoring any edits I made | should-trigger | ✅ PASS | 3/3 | Used `--no-apply-edits`; mentioned `__rt_*` round-trip tabs |
| 6 | Export a report of top 10 customers from Snowflake to Excel | should-not-trigger | ✅ PASS | 3/3 | Correctly declined; redirected to natural-language-data-query |
| 7 | Generate a dbt model and export it to Excel | should-not-trigger | ✅ PASS | 3/3 | Correctly separated dbt-model-from-stm from schema JSON export |
| 8 | Convert my Python script to an Excel file | should-not-trigger | ✅ PASS | 3/3 | Correctly declined; explained Python script is not schema metadata |
| 9 | Export schema.json to Excel (no output path specified) | edge-case | ✅ PASS | 3/3 | Showed command without output arg; confirmed `schema.xlsx` written next to input |
| 10 | Export schema.json to Excel but OPENMETADATA_BASE_URL is not set | edge-case | ✅ PASS | 3/3 | "No error, no crash — it degrades gracefully"; DataGovernanceTerms skipped silently |
| 11 | Convert my Excel back to JSON with a legacy-format workbook (no __rt_* tabs) | edge-case | ✅ PASS | 3/3 | Confirmed backward-compatible fallback to visible-sheet mode |

## Assertion Detail

| Eval | Assertion | Passed | Evidence |
|------|-----------|--------|----------|
| 1 | Response runs or describes running json_to_excel.py | ✅ | Agent ran the script; 76 rows across 16 sheets written to schema.xlsx |
| 1 | Response mentions Summary or per-table worksheet output | ✅ | Explicitly lists Summary, SourceSystem, DataQualityFindings, per-table worksheets |
| 1 | Response does NOT refuse or ask for a different skill | ✅ | Executed conversion without redirecting |
| 2 | Response includes --no-openmetadata flag | ✅ | Command included `--no-openmetadata` |
| 2 | Response does NOT attempt to fetch from OpenMetadata | ✅ | No OM call made; flag passed explicitly |
| 2 | Response mentions DataGovernanceTerms sheet being skipped or absent | ✅ | "The DataGovernanceTerms sheet was skipped as requested" |
| 3 | Response includes --glossary-json flag | ✅ | Used `--glossary-json .cursor/flat/om_glossary_terms.json` |
| 3 | Response references the provided glossary file path | ✅ | Mentions path as the pre-fetched source |
| 3 | Response does NOT attempt a live OpenMetadata fetch when glossary is pre-fetched | ✅ | "using … for the DataGovernanceTerms sheet instead of making a live call to OpenMetadata" |
| 4 | Response runs or describes running excel_to_json.py | ✅ | Agent ran excel_to_json.py on schema.xlsx |
| 4 | Response does NOT include --no-apply-edits | ✅ | Default mode; edits applied; no flag added |
| 4 | Response mentions output JSON file | ✅ | "JSON has been written to schema.json at the workspace root" |
| 5 | Response includes --no-apply-edits flag | ✅ | Ran with `--no-apply-edits`; restored from hidden `__rt_*` tabs |
| 5 | Response mentions __rt_ round-trip tabs or original payload restore | ✅ | "restored … from the hidden __rt_* round-trip tabs" |
| 5 | Response uses excel_to_json.py script | ✅ | excel_to_json.py invoked |
| 6 | Response does NOT run json_to_excel.py | ✅ | Correctly declined; no script invoked |
| 6 | Response declines or redirects | ✅ | "The json-to-excel-export skill isn't the right tool" |
| 6 | Response does NOT produce an Excel workbook from a Snowflake query | ✅ | No workbook produced; redirected instead |
| 7 | Response references dbt-model-from-stm or general dbt work | ✅ | "dbt-model-from-stm — generates dbt SQL view + incremental model files" |
| 7 | Response does NOT run json_to_excel.py as the main action | ✅ | Did not invoke json_to_excel.py; asked for clarification |
| 7 | Response declines or redirects | ✅ | Explained skills are unrelated; asked which task user wanted |
| 8 | Response does NOT run json_to_excel.py | ✅ | Correctly declined |
| 8 | Response does not treat Python script as a schema JSON input | ✅ | "A Python script is source code, not schema metadata" |
| 8 | Response declines or answers from general knowledge | ✅ | Offered alternatives (document functions, export script output) |
| 9 | Response runs or describes running json_to_excel.py with only the input argument | ✅ | Command: `json_to_excel.py schema.json` (no output arg) |
| 9 | Response mentions output file lands next to input OR uses same base name with .xlsx extension | ✅ | "schema.xlsx will appear next to schema.json" |
| 9 | Response does NOT error on missing output path | ✅ | Graceful behavior confirmed; no error mentioned |
| 10 | Response notes OpenMetadata fetch will be skipped or produce no DataGovernanceTerms sheet | ✅ | "skip the DataGovernanceTerms sheet — the export continues normally" |
| 10 | Response does NOT error out — graceful degradation described | ✅ | "No error, no crash — it degrades gracefully" |
| 10 | Response still proceeds with the conversion | ✅ | Full run command provided; all other tabs confirmed intact |
| 11 | Response runs excel_to_json.py | ✅ | Full excel_to_json.py command provided |
| 11 | Response notes backward compatibility with legacy workbooks OR applies visible tab edits | ✅ | "detects the absence of __rt_meta and automatically switches to visible-sheet mode" |
| 11 | Response does NOT refuse or error on missing __rt_* tabs | ✅ | "expected behaviour and fully supported" |

## Category Grades

| # | Category | Grade | Explanation |
|---|----------|-------|-------------|
| 1 | Triggering (Description Quality) | PASS | ↩ carried — name, description, and trigger terms all correct |
| 2 | Anatomy & Structure | PASS | ↩ carried — valid YAML frontmatter; well-organised sections; scripts in conventional folder |
| 3 | Instructions Clarity | PASS | ↩ carried — clear step-by-step commands with explicit flags and fallbacks |
| 4 | Output Quality | PASS | ↩ carried — output shape, tab names, and formatting rules explicitly documented |
| 5 | Testability | PASS | ↩ carried — 11 test cases covering all documented flows |
| 6 | Resource Efficiency | PASS | ↩ carried — pre-built scripts used; execution intent explicit |
| 7 | Security & Trust | PASS | ↩ carried — no hardcoded credentials; env-var auth pattern |
| 8 | Coexistence & Recall | PASS | ↩ carried — specific description; does not overlap with adjacent skills |
| 9 | Model Compatibility | PASS | Model Compatibility section added; validated on Claude 3.5 Sonnet |
| 10 | Workflow & Feedback Loops | PASS | ↩ carried — deterministic script execution; graceful degradation documented |
| 11 | Maintainability & Lifecycle | PASS | New Registry table: owner, reviewer, versioning, lifecycle stage, last-eval date, dependencies |
| 12 | Gotchas / Lessons Learned | PASS | Four concrete gotchas: OM timeout, `__rt_*` silent-ignore, `requests` ImportError, legacy fallback |
| 13 | Anti-Pattern Audit | PASS | All 8 magic constants documented; versioned install command explicit |

## Version Comparison

| Eval | Verdict | Reasoning |
|------|---------|-----------|
| 9 | new_wins | New version adds OM auto-fetch gotcha and workbook-tab list; old version correct but minimal |
| 10 | new_wins | New version cites correct sheet names, full script path, and `--no-openmetadata` timeout tip |
| 11 | new_wins | New version confirms backward-compatible fallback as a supported feature; old version left resolution incomplete |

**Overall: new_wins (3–0)** vs snapshot `snapshots/json-to-excel-export/2026-05-07T124559Z/`

## History

| reviewed_at | date | Unit Tests | Assertions | Categories | Verdict | Notes |
|-------------|------|------------|------------|------------|---------|-------|
| 2026-05-07T12:45:59Z | 2026-05-07 | 11/11 | 33/33 | 10/13 | FAIL | Cats 11/12/13 FAIL — no Registry, no Gotchas, undocumented magic constants |
| 2026-05-07T12:56:12Z | 2026-05-07 | 11/11 | 33/33 | 13/13 | PASS | All 3 previously-failing cats now PASS. Comparator new_wins 3–0. |
