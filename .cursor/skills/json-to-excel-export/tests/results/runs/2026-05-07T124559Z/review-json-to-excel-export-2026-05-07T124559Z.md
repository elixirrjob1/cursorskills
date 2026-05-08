# Skill Review: json-to-excel-export

**Reviewed at:** 2026-05-07T12:45:59Z | **Run:** 2026-05-07T124559Z

## Overall Verdict: FAIL

**Production-Ready Recommendation:** No

Incremental re-check against prior review 2026-05-06. All 11 unit tests carried (PASS from prior run). Categories 1–10 carried (PASS). Re-evaluated categories 11, 12, 13 — all three remain FAIL with identical findings. No fixes were applied to SKILL.md since the last review (last SKILL.md commit: 2026-04-10). The skill is operationally solid with zero high-criticality failures; remediation required only for documentation/metadata categories.

## Unit Tests: 11 / 11 passed

| # | Test | Type | Result | Notes |
|---|------|------|--------|-------|
| 1 | Export schema JSON to Excel | Should-trigger | ✅ PASS ↩ carried | Carried from 2026-05-06 |
| 2 | Skip OpenMetadata sheet | Should-trigger | ✅ PASS ↩ carried | Carried from 2026-05-06 |
| 3 | Use pre-fetched glossary.json | Should-trigger | ✅ PASS ↩ carried | Carried from 2026-05-06 |
| 4 | Convert Excel back to JSON with edits | Should-trigger | ✅ PASS ↩ carried | Carried from 2026-05-06 |
| 5 | Restore original JSON ignoring edits | Should-trigger | ✅ PASS ↩ carried | Carried from 2026-05-06 |
| 6 | Export Snowflake query to Excel | Should-not-trigger | ✅ PASS ↩ carried | Carried from 2026-05-06 |
| 7 | Generate dbt model and export to Excel | Should-not-trigger | ✅ PASS ↩ carried | Carried from 2026-05-06 |
| 8 | Convert Python script to Excel | Should-not-trigger | ✅ PASS ↩ carried | Carried from 2026-05-06 |
| 9 | Export without output path | Edge case | ✅ PASS ↩ carried | Carried from 2026-05-06 |
| 10 | OpenMetadata not set → graceful degradation | Edge case | ✅ PASS ↩ carried | Carried from 2026-05-06 |
| 11 | Legacy workbook without __rt_* tabs | Edge case | ✅ PASS ↩ carried | Carried from 2026-05-06 |

## Category Grades

| # | Category | Grade | Notes |
|---|----------|-------|-------|
| 1 | Triggering (Description Quality) | ✅ PASS ↩ carried | Specific triggers; name valid; description clear |
| 2 | Anatomy & Structure | ✅ PASS ↩ carried | SKILL.md 91 lines; scripts/ conventional; agents/openai.yaml LOW-only flag |
| 3 | Instructions Clarity | ✅ PASS ↩ carried | All flags documented with exact if/then cases |
| 4 | Output Quality | ✅ PASS ↩ carried | Output Shape section lists all tabs and formatting rules |
| 5 | Testability | ✅ PASS ↩ carried | 11 tests; TC9 and TC10 executed successfully in real environment |
| 6 | Resource Efficiency | ✅ PASS ↩ carried | Pre-built scripts for both directions; no verbose explanations |
| 7 | Security & Trust | ✅ PASS ↩ carried | OM credentials from env only; no credential logging; network limited to OM REST |
| 8 | Coexistence & Recall | ✅ PASS ↩ carried | Clean boundaries confirmed with natural-language-data-query and dbt-model-from-stm |
| 9 | Model Compatibility | ✅ PASS ↩ carried | N/A — single-path script execution, no multi-model requirement |
| 10 | Workflow & Feedback Loops | ✅ PASS ↩ carried | N/A — single-step conversion, not fragile or destructive |
| 11 | Maintainability & Lifecycle | ❌ FAIL | No registry table, versioning strategy, or lifecycle stage — unchanged since 2026-05-06 |
| 12 | Gotchas / Lessons Learned | ❌ FAIL | No Gotchas section — unchanged since 2026-05-06 |
| 13 | Anti-Pattern Audit | ❌ FAIL | Undocumented magic constants (`limit:1000` ×2, `8` for blank row padding, `30000` for chunk size, `31` for sheet name truncation); `requests` not mentioned; `openpyxl` unversioned |

## High-Criticality Failures

None.

## Medium-Criticality Failures

### **Subcategory:** Skill registry entry / versioning / lifecycle
- **Category:** 11 — Maintainability & Lifecycle
- **Finding:** SKILL.md frontmatter contains only `name` and `description`. No `version`, `owner`, `lifecycle_stage`, `dependencies`, `last_reviewed`, or `rollback` fields. Unchanged since last review.
- **Recommendation:** Add a `## Registry` table (see skill-reviewer pattern in this repo).

### **Subcategory:** Has a Gotchas / Common Mistakes section
- **Category:** 12 — Gotchas / Lessons Learned
- **Finding:** No Gotchas section. Unchanged since last review.
- **Recommendation:** Add `## Gotchas` covering: (1) forgetting `--no-openmetadata` causes a timeout when OM is slow; (2) editing cells in `__rt_*` or `__dv_*` tabs is silently ignored — edits must be in visible tabs; (3) `requests` must be installed for OM auto-fetch; (4) multiple new undocumented magic constants in script (see Cat 13).

### **Subcategory:** Package install commands explicit
- **Category:** 13 — Anti-Pattern Audit
- **Finding:** Notes says "Requires `openpyxl` in the active environment" — no version. `requests` is used for OpenMetadata fetch (`import requests as _req` in `json_to_excel.py` lines 40, 71) but not mentioned in SKILL.md at all.
- **Recommendation:** Add to Notes: `pip install "openpyxl>=3.1,<4.0" "requests>=2.31,<3.0"` (requests only needed for OM auto-fetch).

### **Subcategory:** No voodoo constants
- **Category:** 13 — Anti-Pattern Audit
- **Finding:** Multiple undocumented magic numbers in `json_to_excel.py`. Since last review, additional constants identified: `8` used repeatedly (lines 375, 388, 433, 449, 457, 479) for blank row padding in manual-input sections — no comment; `30000` for payload chunk size (line 1096) — no rationale; `31` for Excel sheet name truncation (line 718) — actually a spec-defined limit but undocumented.
- **Recommendation:** Add inline comments: `# 8: minimum fillable rows for manual input sections`; `# 30000: safely below Excel's 32767-char cell limit`; `# 31: Excel sheet name character limit (per OOXML spec)`; `# 1000: safely above any real-world glossary size; OM paginates beyond this`.

## Low-Criticality Failures

### **Subcategory:** Bundled resources in conventional folders
- **Category:** 2 — Anatomy & Structure
- **Finding:** `agents/openai.yaml` is a non-standard folder. ↩ carried from 2026-05-06.
- **Recommendation (non-blocking):** Move to `references/` or document its purpose in SKILL.md.

## Strengths

- **Both directions work in production**: TC9 and TC10 ran against real workspace (76 rows, 16 sheets; OM graceful degradation confirmed). ↩ carried.
- **Flag documentation is complete**: `--no-openmetadata`, `--glossary-json`, `--no-apply-edits` all documented with exact syntax and purpose.
- **Lossless round-trip by design**: Hidden `__rt_*` tabs store full original payload; `--no-apply-edits` restores exactly.
- **OpenMetadata auto-fetch is fully optional**: Three independent paths (auto-fetch, `--glossary-json`, `--no-openmetadata`) documented and tested.
