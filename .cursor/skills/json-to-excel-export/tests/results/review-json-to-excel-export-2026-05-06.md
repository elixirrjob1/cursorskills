# Skill Review: json-to-excel-export

## Overall Verdict: FAIL

**Production-Ready Recommendation:** No

The skill is operationally solid — all 11 unit tests pass, both conversion directions work correctly in the real environment (TC9 produced a 76-row/16-sheet workbook; TC10 confirmed graceful degradation when OpenMetadata is unavailable), and all flags are correctly applied. Three categories fail, all documentation/metadata issues with no behavioural impact.

---

## Unit Tests: 11 / 11 passed

| # | Test | Type | Result | Notes |
|---|------|------|--------|-------|
| 1 | Export schema.json to Excel | Should-trigger | ✅ PASS | Found schema JSON files in workspace, asked to confirm which one before running |
| 2 | Convert to Excel, skip OpenMetadata sheet | Should-trigger | ✅ PASS | Asked for file path, showed correct command with --no-openmetadata |
| 3 | Use pre-fetched glossary.json | Should-trigger | ✅ PASS | Found schema.json, identified glossary.json missing, asked for path before running |
| 4 | Convert Excel back to JSON applying edits | Should-trigger | ✅ PASS | Found available .xlsx files, offered correct options for reverse conversion |
| 5 | Restore original JSON ignoring edits | Should-trigger | ✅ PASS | Showed excel_to_json.py with --no-apply-edits and correct explanation of __rt_* tabs |
| 6 | Export Snowflake query results to Excel | Should-not-trigger | ✅ PASS | Declined — skill doesn't connect to Snowflake or execute queries |
| 7 | Generate dbt model and export to Excel | Should-not-trigger | ✅ PASS | Declined, redirected to dbt-model-from-stm |
| 8 | Convert Python script to Excel | Should-not-trigger | ✅ PASS | Declined — not a general-purpose file converter |
| 9 | Export without specifying output path | Edge case | ✅ PASS | Ran script — wrote schema.xlsx (76 rows, 16 sheets) next to input file |
| 10 | OpenMetadata not set → graceful degradation | Edge case | ✅ PASS | Ran script — DataGovernanceTerms skipped silently; all other tabs present |
| 11 | Legacy workbook without __rt_* tabs | Edge case | ✅ PASS | Handled correctly — applies visible edits, backward-compatible |

---

## Category Grades

| # | Category | Grade | Notes |
|---|----------|-------|-------|
| 1 | Triggering (Description Quality) | PASS | ✓ Specific triggers: "export .json schema output to .xlsx"; name 20 chars, valid |
| 2 | Anatomy & Structure | PASS | ✓ SKILL.md 91 lines; scripts/ conventional; agents/openai.yaml is non-standard folder (LOW, non-blocking) |
| 3 | Instructions Clarity | PASS | ✓ All flags documented with clear if/then cases (--no-openmetadata, --glossary-json, --no-apply-edits) |
| 4 | Output Quality | PASS | ✓ Output Shape section lists all tabs and formatting rules; strictness calibrated |
| 5 | Testability | PASS | ✓ 11 tests written; TC9 and TC10 actually executed successfully in real environment |
| 6 | Resource Efficiency | PASS | ✓ Pre-built scripts for both conversion directions; no verbose explanations |
| 7 | Security & Trust | PASS | ✓ OM credentials from env only; no hardcoded values; no credential logging; network limited to OM REST API |
| 8 | Coexistence & Recall | PASS | ✓ TC6–TC8 confirm clean boundaries with natural-language-data-query and dbt-model-from-stm |
| 9 | Model Compatibility | PASS | ✓ N/A — no multi-model requirement |
| 10 | Workflow & Feedback Loops | PASS | ✓ N/A — single-step conversion, not a fragile or destructive operation |
| 11 | Maintainability & Lifecycle | FAIL | No version, lifecycle, owner, or rollback in frontmatter — 0/3 MEDIUM pass |
| 12 | Gotchas / Lessons Learned | FAIL | No gotchas section — 0/1 MEDIUM pass |
| 13 | Anti-Pattern Audit | FAIL | openpyxl noted but not versioned; requests not mentioned; limit:1000 in script undocumented — 2/4 MEDIUM pass |

---

## High-Criticality Failures

None.

---

## Medium-Criticality Failures

### **Subcategory:** Skill registry entry / versioning / lifecycle
- **Category:** 11 — Maintainability & Lifecycle
- **Finding:** No `version`, `lifecycle`, `owner`, `dependencies`, `last_reviewed`, or `rollback` in YAML frontmatter.
- **Recommendation:** Add all fields to frontmatter.

---

### **Subcategory:** Has a Gotchas / Common Mistakes section
- **Category:** 12 — Gotchas / Lessons Learned
- **Finding:** No gotchas section in SKILL.md.
- **Recommendation:** Add `## Common Mistakes` with at minimum: (1) forgetting `--no-openmetadata` causes a timeout if OM is slow to respond; (2) editing cells in `__rt_*` tabs is ignored — edits must be in the visible tabs; (3) `requests` must be installed for OpenMetadata auto-fetch to work.

---

### **Subcategory:** Package install commands explicit
- **Category:** 13 — Anti-Pattern Audit
- **Finding:** Notes says "Requires `openpyxl` in the active environment" with no version. `requests` is used for OpenMetadata fetch but not mentioned at all. No install command provided.
- **Recommendation:** Add to Notes: `pip install "openpyxl>=3.1,<4.0" "requests>=2.31,<3.0"` (requests only needed when OpenMetadata auto-fetch is used).

### **Subcategory:** No voodoo constants
- **Category:** 13 — Anti-Pattern Audit
- **Finding:** `limit: 1000` used in `_om_fetch_glossary_payload` for glossary fetch with no rationale.
- **Recommendation:** Add inline comment: `# 1000: safely above any real-world glossary size; OM paginates beyond this if needed`

---

## Low-Criticality Failures

### **Subcategory:** Bundled resources in conventional folders
- **Category:** 2 — Anatomy & Structure
- **Finding:** `agents/openai.yaml` is not in a standard `scripts/`, `references/`, or `assets/` folder. The `agents/` folder is non-standard.
- **Recommendation (non-blocking):** Move to `references/` or document its purpose in SKILL.md if it is intentionally part of the skill's interface.

---

## Strengths

- **Both directions work in production**: TC9 and TC10 actually ran against the real workspace — 76 rows, 16 sheets produced; OpenMetadata graceful degradation confirmed.
- **Flag documentation is complete**: All three flags (`--no-openmetadata`, `--glossary-json`, `--no-apply-edits`) are documented with purpose and exact command syntax.
- **Reverse conversion is lossless by design**: Hidden `__rt_*` tabs store full original payload; `--no-apply-edits` restores exactly. Legacy workbooks without these tabs still work.
- **Boundary with adjacent skills is clean**: TC6–TC8 confirm no leakage into data query or dbt model generation.
