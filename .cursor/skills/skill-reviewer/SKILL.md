---
name: skill-reviewer
description: "Reviews and evaluates Claude skills (SKILL.md files and skill folders) against Anthropic best practices to determine production-readiness. Use whenever a user asks to review, evaluate, audit, grade, score, lint, or check the quality of a skill. Also use when users mention skill optimization, skill best-practices compliance, token-efficiency review, skill design review, or production-readiness assessment for a SKILL.md file or skill directory. Produces a structured pass/fail report with severity-tagged findings (High/Medium/Low) and concrete remediation steps."
---

# Skill Reviewer

Reviews a Claude skill against a 13-category rubric derived from Anthropic's Skills best practices and enterprise governance documentation. Outputs a structured pass/fail report with severity-tagged remediation recommendations.

## Workflow

### Step 1: Identify the skill

Ask the user for the skill if not already provided. Acceptable inputs:
- A path to a SKILL.md file or skill folder
- Pasted skill content
- A skill name accessible via the filesystem

Read all bundled reference files and scripts — they are within scope. Do not begin until the full skill content is available.

**Check for a prior review (incremental mode):**

Look for an existing review file in `tests/results/`. If one or more files are present, read the most recent one (highest date in filename). Extract:
- Which unit tests previously **PASSED** → skip re-running those subagents in Step 3; carry forward their PASS result
- Which categories previously **PASSED** → skip re-evaluating those in Step 4; carry forward their PASS result

Only re-run tests and re-evaluate categories that previously **FAILED** or are **new** (no prior result). Announce at the start: _"Prior review found — running incremental re-check of N failed tests and M failed categories."_

If no prior review exists, run the full review.

### Step 2: Research the skill deeply, then generate unit test cases

Before writing any test cases, read and understand the full skill:

1. Read SKILL.md in full — note every trigger condition, routing rule, workflow step, fallback, and output format.
2. Read every bundled reference file and script listed in SKILL.md. For large scripts (>200 lines), read enough to understand inputs, outputs, and failure modes.
3. Only after reading all content, derive test cases from what the skill **actually does** — not from its name or description alone. Tests must exercise specific behaviors documented in the skill (e.g., if the skill has a preflight config check, test that; if it has a routing decision tree, test each branch).

Then create a `tests/` folder inside the skill folder and write:

If `tests/test-cases.md` already exists, reuse it. Only regenerate if the skill content has changed significantly since the last review (use judgment based on SKILL.md modification vs. review date).

The `tests/` folder contains two files:

#### `tests/test-cases.md` — behavioural assertions
Declares what the skill should and should not do. Three sections:

- **Should-trigger (3–8 cases):** prompts that must cause the agent to load and apply this skill. Scale to the number of distinct documented workflows, routing branches, or config paths — one test per meaningful branch. Each case must exercise a **specific documented behavior** — not just re-state the skill name. For each: `input` (user prompt) and `expected` (behaviour assertion citing the specific step or rule being tested).
- **Should-not-trigger (2–4 cases):** adjacent prompts that must NOT trigger this skill. Cover the most likely confusion skills. For each: `input` and `expected` (which skill or behaviour should handle it instead).
- **Edge cases (2–4 cases):** ambiguous or boundary inputs that exercise **documented fallback or clarification rules**. Cover only edge cases that are explicitly documented in the skill. For each: `input` and `expected` (clarify, partial apply, or decline — citing the specific rule).

Use this template:

```markdown
# Eval Suite: <skill-name>

_Re-run this suite after every skill update to catch regressions._

## Should-trigger

| # | Input | Expected behavior |
|---|-------|-------------------|
| 1 | ... | ... |

## Should-not-trigger

| # | Input | Expected behavior |
|---|-------|-------------------|
| 1 | ... | ... |

## Edge cases

| # | Input | Expected behavior |
|---|-------|-------------------|
| 1 | ... | ... |
```

#### `tests/test_skill.py` — executable unit tests
A Python test script that can be run independently to validate the skill. Generate this file alongside `test-cases.md`. Each test case from the markdown becomes a `pytest`-compatible function. Use assertions on expected keywords, structure, or behaviour in the output where testable programmatically.

```python
# tests/test_skill.py
# Run with: pytest tests/test_skill.py

def test_should_trigger_<description>():
    """<input prompt>"""
    # Assert: <expected behaviour>
    ...

def test_should_not_trigger_<description>():
    ...

def test_edge_case_<description>():
    ...
```

Write both files to `tests/` inside the skill folder being reviewed (e.g. `.cursor/skills/my-skill/tests/`). If the path is unavailable, output both files inline before proceeding.

Also write `tests/evals/evals.json` — the machine-readable version of the same test cases, with assertions filled in. Assertions are evaluated programmatically by the grader (Step 3b), so they must be concrete and checkable against response text — not vague goals.

```json
{
  "skill_name": "<skill-name>",
  "generated_date": "YYYY-MM-DD",
  "evals": [
    {
      "id": 1,
      "type": "should-trigger | should-not-trigger | edge-case",
      "prompt": "...",
      "expected_behavior": "...",
      "assertions": [
        "Response contains X strategy labels",
        "Response does NOT contain a clarifying question",
        "Response includes a recommendation section"
      ]
    }
  ]
}
```

Write 2–4 assertions per eval. Good assertions are objectively verifiable from the response text — presence/absence of phrases, structure, or explicit statements. Do not leave `assertions` empty. If `tests/evals/evals.json` already exists and test-cases.md has not changed significantly, reuse it as-is.

### Step 3: Run the unit tests

Skip any test that carried a **PASS** from the prior review (Step 1 incremental check). For the remaining tests, launch a subagent with:
- The full skill content (SKILL.md + any reference files) as context
- The test `input` as the user prompt
- No instruction to self-judge — the subagent just responds naturally

Run all subagents in parallel. Once responses are collected, evaluate each one yourself by comparing the actual response against the `expected` behavior defined in `test-cases.md`:

- **PASS**: the actual response matches the expected behavior (e.g. expected "generates SQL and queries metadata" → response contains SQL and mentions metadata discovery)
- **FAIL**: the actual response contradicts the expected behavior (e.g. expected "asks clarifying question" → response instead attempts to run a query)

Do not ask the subagent to grade itself. You evaluate the responses against the expected column in `test-cases.md`.

**Record run results:** Save all run outcomes to `tests/results/runs/YYYY-MM-DD/timing.json` (create the directory if needed):

```json
{
  "review_date": "YYYY-MM-DD",
  "runs": [
    {
      "eval_id": 1,
      "test_type": "should-trigger | should-not-trigger | edge-case",
      "prompt_short": "<first 80 chars of prompt>",
      "result": "PASS | FAIL | SKIP"
    }
  ]
}
```

For skipped tests (carried from prior review), set `result: "SKIP"`.

Summarise as `X / Y tests passed` before proceeding to the rubric.

### Step 3b: Grade assertions

Spawn grader subagents in parallel — one per eval that was actually run (skip evals with `result: "SKIP"`). Pass each grader:
- The eval's `assertions` array from `evals.json`
- The full actual response from the subagent

See `agents/grader.md` for grader instructions. Each grader saves its output to `tests/results/runs/YYYY-MM-DD/grading-<eval_id>.json`:

```json
{
  "eval_id": 1,
  "prompt_short": "...",
  "overall_result": "PASS | FAIL",
  "assertions": [
    {
      "text": "Response contains 5 strategy labels",
      "passed": true,
      "evidence": "Response includes Relationship, Purpose, Curiosity, Proof light, Time & flow headings"
    },
    {
      "text": "Response does NOT contain a clarifying question",
      "passed": false,
      "evidence": "Response opens with 'Before I write the openers, I need to lock in three things'"
    }
  ]
}
```

An eval's `overall_result` is PASS only if all assertions pass. If grading contradicts your earlier PASS/FAIL verdict, trust the grader — update the unit test summary accordingly.

### Step 3c: Blind comparison (incremental runs only)

**Skip this step on first reviews.** Only run when:
- A prior review exists (Step 1 found one), AND
- The skill has been edited since that review (SKILL.md mtime is newer than the last review date)

**Setup:** Snapshot the old skill before editing: `cp -r <skill-path> /tmp/skill-snapshot-<skill-name>`. If no snapshot exists for this session, skip Step 3c.

**Run:** For each re-run eval (not SKIP), spawn two executor subagents in parallel — one with the current skill, one with the snapshot — using the same prompt, without labelling which is which. Then spawn one comparator subagent per eval (see `agents/comparator.md`), passing `output_a` and `output_b` unlabelled. Each comparator saves `tests/results/runs/YYYY-MM-DD/comparison-<eval_id>.json` with fields: `eval_id`, `prompt_short`, `verdict` (`a_wins | b_wins | tie`), `reasoning`.

**Aggregate:** After all comparators complete, map `a_wins`/`b_wins` to `new_wins`/`old_wins` (you know which was A vs B — track it). Save `tests/results/runs/YYYY-MM-DD/comparison-summary.json` with fields: `review_date`, `overall` (`new_wins | old_wins | tie | mixed`), `new_wins`, `old_wins`, `ties`, `total_compared`. `overall` is `new_wins` if new > old, `old_wins` if reversed, `tie` if equal, `mixed` if both > 0 but split. Include the summary in the benchmark report (Step 7b/7c) when present.

### Step 4: Review each subcategory

Skip any category that carried a **PASS** from the prior review (Step 1 incremental check) — carry it forward as-is. For categories being re-evaluated, assign **PASS**, **FAIL** (with a one-to-two-sentence evidence-based reason), or **N/A** (criterion does not apply). Cite the specific line, file, or absence of content that justifies the verdict.

### Step 5: Roll up to category grades

A category **PASS** requires:
- All [HIGH] subcategories pass, AND
- At least 70% of applicable [MEDIUM] subcategories pass

### Step 6: Generate the report

Use the output format defined at the bottom of this skill. Include the unit test summary (`X / Y tests passed`) at the top of the report.

After generating the report, save it as a file inside the skill's `tests/results/` folder. Name the file using the skill name and current date: `review-<skill-name>-YYYY-MM-DD.md` (e.g. `review-json-to-excel-export-2026-05-06.md`). Create the `results/` folder if it does not exist. If the path is unavailable, output the report inline only.

In incremental runs, append `↩ carried` to the existing Notes value in the Unit Tests and Category Grades tables — do not replace the original note text. Example: `Discovered FactSales via OM, returned $35K ↩ carried`. Only re-evaluated rows get new note text.

### Step 7: Update history and generate benchmark reports

After saving the review markdown file, do three more things in order.

#### 7a: Append to `tests/results/history.json`

Read the existing file (create it with `{"history": []}` if absent), then append one new entry and write it back. Never truncate prior entries.

```json
{
  "history": [
    {
      "date": "YYYY-MM-DD",
      "skill_version": "<git short-hash or 'unversioned'>",
      "unit_tests": { "passed": 0, "total": 0, "pass_rate": 0.00 },
      "assertions": { "passed": 0, "total": 0, "pass_rate": 0.00 },
      "categories": { "passed": 0, "total": 13, "pass_rate": 0.00 },
      "verdict": "PASS | FAIL",
      "high_failures": ["<subcategory text>"],
      "review_file": "review-<skill-name>-YYYY-MM-DD.md",
      "timing_file": "runs/YYYY-MM-DD/timing.json"
    }
  ]
}
```

For `skill_version`, run `git -C <skill-folder> rev-parse --short HEAD 2>/dev/null || echo unversioned`.

#### 7b: Generate `benchmark-<skill-name>-YYYY-MM-DD.md`

Save to `tests/results/`.

**Reuse the canonical markdown template (preferred):** Read `references/benchmark-report-template.md` from **this skill’s folder** (`skill-reviewer`) — e.g. `.cursor/skills/skill-reviewer/references/benchmark-report-template.md`. Copy to the target’s `tests/results/benchmark-<skill-name>-YYYY-MM-DD.md`, remove the `<!-- … -->` comment block at the top, and **replace every placeholder** (`___NAME___`).

**Summary table (required):** Must contain exactly these **seven** metrics in this order — same as `references/benchmark-report-template.html`:

| Metric | Value (examples) |
|--------|------------------|
| Overall Verdict | `PASS` / `FAIL` |
| Unit Tests | `X / Y passed (Z%)` |
| Assertions | `A / B (C%)` or `—` if not tracked |
| Categories | `X / 13 passed` |
| High Failures | `N` or `—` |
| Medium Failures | `N` or short rollup text |
| Comparator | e.g. `— (not run)` or `new_wins` summary |

| Placeholder | Replace with |
|-------------|----------------|
| `___SKILL_NAME___` | Target skill display name |
| `___META_LINE___` | One line, e.g. `_Generated: YYYY-MM-DD · <run note>_` (leading `_` for italics if desired) |
| `___SUMMARY_VERDICT___` | `PASS` or `FAIL` |
| `___SUMMARY_UNIT_TESTS___` | e.g. `10 / 10 passed (100%)` |
| `___SUMMARY_ASSERTIONS___` | e.g. `30 / 30 (100%)` |
| `___SUMMARY_CATEGORIES___` | e.g. `9 / 13 passed` |
| `___SUMMARY_HIGH_FAILURES___` | e.g. `1` or `—` |
| `___SUMMARY_MEDIUM_FAILURES___` | e.g. `5 (rolled up)` or `—` |
| `___SUMMARY_COMPARATOR___` | e.g. `— (not run; no snapshot)` |
| `___UNIT_TEST_ROWS___` | Markdown table body rows: `\| # \| label \| type \| ✅ PASS / ❌ FAIL \| a/b \|` |
| `___ASSERTION_DETAIL_ROWS___` | **Required:** one row per assertion for **every eval** in `evals.json` (reuse prior grading JSON rows for incremental SKIP evals). **Forbidden:** omitting this table or replacing the section with only text like “Per-eval JSON: `runs/…/grading-<n>.json`” / “see JSON files” without listing every assertion inline. A footnote listing artifact paths is allowed *after* the full table, not instead of it. |
| `___CATEGORY_ROWS___` | Rows `\| n \| Category \| PASS / FAIL \| Explanation \|` — **Explanation** = one short clause (why PASS or key finding for FAIL), aligned with the narrative `review-*.md`. |
| `___VERSION_COMPARISON_BLOCK___` | Comparator table or italic `_Not run — …_` |
| `___HISTORY_ROWS___` | One data row per `history.json` entry |
| `___REVIEW_FILENAME___` | `review-<skill-name>-YYYY-MM-DD.md` for this run |

**Assertion Detail (required):** The **Assertion Detail** section must contain a **complete Markdown table** (columns: Eval, Assertion, Passed, Evidence) with **one row per assertion** for **every eval** in `tests/evals/evals.json`. Populate from this run’s `runs/YYYY-MM-DD/grading-<eval_id>.json` when Step 3 re-ran the eval; on **incremental** runs, for evals **not** re-run (SKIP), copy assertion rows from the **prior** run’s grading files or prior benchmark so **no eval drops out**.

**Forbidden:** Using only a pointer to JSON files as the Assertion Detail body (e.g. “Per-eval JSON: `runs/2026-05-07/grading-<n>.json` for *n* = 1…10” with **no** assertion rows). You may add a sentence **after** the full table citing machine-readable grader paths, but the table is mandatory.

**Category Grades (required):** Include column **Explanation** (brief rationale per category, consistent with the review narrative).

**Fallback** if the template file cannot be read: write the same sections manually; **Summary** must still list all seven metrics in the order above; **Assertion Detail** and **Category Grades** rules still apply.

#### 7c: Generate `benchmark-<skill-name>-YYYY-MM-DD.html`

Save alongside the `.md` file. Must be a **fully self-contained** HTML file — no external CDN links, all CSS in the template’s `<style>` block (do not strip or rename classes).

**Reuse the canonical template (preferred):** Read `references/benchmark-report-template.html` from **this skill’s folder** (`skill-reviewer`), not from the skill under review. In a typical repo layout that is `.cursor/skills/skill-reviewer/references/benchmark-report-template.html`. Copy the entire file to the target’s `tests/results/benchmark-<skill-name>-YYYY-MM-DD.html`, then **replace every placeholder** (each is unique `___NAME___`):

| Placeholder | Replace with |
|-------------|----------------|
| `___SKILL_NAME___` | Target skill display name (same as markdown benchmark) |
| `___META_LINE___` | One line, e.g. `Generated: YYYY-MM-DD · <short run note>` |
| `___SUMMARY_TD_VERDICT___` | Second column for **Overall Verdict** — full `<td class="pass">PASS</td>` or `<td class="fail">FAIL</td>` |
| `___SUMMARY_TD_UNIT_TESTS___` | e.g. `<td>10 / 10 passed (100%)</td>` |
| `___SUMMARY_TD_ASSERTIONS___` | e.g. `<td>30 / 30 (100%)</td>` |
| `___SUMMARY_TD_CATEGORIES___` | e.g. `<td>9 / 13 passed</td>` |
| `___SUMMARY_TD_HIGH_FAILURES___` | e.g. `<td>1</td>` or `<td>—</td>` |
| `___SUMMARY_TD_MEDIUM_FAILURES___` | e.g. `<td>5 (rolled up)</td>` or `<td>—</td>` |
| `___SUMMARY_TD_COMPARATOR___` | e.g. `<td class="skip">— (not run)</td>` or outcome text |
| `___UNIT_TEST_ROWS___` | Table rows: `#`, short test label, type, result cell (`<td class="pass">` / `fail` / `skip`), assertions fraction |
| `___ASSERTION_DETAIL_ROWS___` | **Required:** one `<tr>` per assertion: `<td>eval_id</td><td>…assertion text…</td><td class="pass">Yes</td>` / `fail` + evidence — columns **Eval | Assertion | Passed | Evidence**. **Forbidden:** only a `<p class="note">` pointing at JSON files with no rows. You may repeat the table from the `.md` benchmark (HTML-escape cell text). |
| `___CATEGORY_ROWS___` | Rows: `#`, category name, grade cell (`pass` / `fail`), `<td>…explanation…</td>` |
| `___VERSION_COMPARISON_BLOCK___` | Comparator table + rows, or `<p class="note">…</p>` if skipped |
| `___HISTORY_ROWS___` | One row per `history.json` entry; use **bar-track** / **bar-fill** for test/assertion/category rates (`style="width:NN%"` on bar-fill) |
| `___REVIEW_FILENAME___` | `review-<skill-name>-YYYY-MM-DD.md` for this run |

Remove the instructional HTML comment block from the template in the saved output. **Do not** leave any `___…___` placeholder in the final file.

**Assertion Detail (HTML):** Must mirror the markdown benchmark: a full **`<table>`** with thead **Eval | Assertion | Passed | Evidence** and one `<tr>` per assertion for **every eval** in the suite (same incremental carry-forward rule as Step 7b).

**Category Grades (HTML):** Fourth column **Explanation** for every category.

**Fallback** if the template file cannot be read: build equivalent HTML from scratch using the same sections and styles: white background, `system-ui`, 14px; tables `border-collapse: collapse`, `1px solid #ccc`, alternating `#f9f9f9`; `.pass` / `.fail` / `.skip` as in the template; history pass rates as inline green bar + percentage; **Assertion Detail** and **Category Explanation** rules still apply.

---

## Rubric

Each subcategory is tagged [HIGH], [MEDIUM], or [LOW].

### Category 1: Triggering (Description Quality)
- [HIGH] Description includes both *what* the skill does AND *when* to trigger it
- [HIGH] Specific triggers and key terms present (not generic phrasing like "helps with documents")
- [HIGH] Name format: max 64 chars, lowercase letters/numbers/hyphens only, no reserved words ("anthropic", "claude")
- [HIGH] Description max 1024 chars; non-empty; no XML tags
- [MEDIUM] Trigger terms cover natural synonyms users would say (e.g. for a data skill: "report", "metrics", "KPIs", not just the technical action name)
- [LOW] Name uses gerund form (`processing-pdfs`) or acceptable noun-phrase alternative

### Category 2: Anatomy & Structure
- [HIGH] Valid YAML frontmatter present with required `name` and `description` fields
- [MEDIUM] References kept reasonably shallow from SKILL.md — flag only if chains exceed two hops (SKILL.md → ref → ref → ref) or if a required file is not reachable from SKILL.md at all
- [MEDIUM] SKILL.md body under 500 lines. If exceeded, flag specific extraction candidates: subagent prompts, large CTE/code patterns, provider setup guides, troubleshooting tables
- [LOW] Reference files over 100 lines include a table of contents
- [MEDIUM] Domain-specific organization where multiple domains exist (e.g., `references/finance.md`, `references/sales.md`)
- [LOW] Bundled resources placed in conventional folders (`scripts/`, `references/`, `assets/`)

### Category 3: Instructions Clarity
- [HIGH] Degrees of freedom calibrated to task fragility (low/specific for fragile or destructive ops; high/flexible for open-ended tasks)
- [MEDIUM] Consistent terminology throughout (one term per concept)
- [MEDIUM] No time-sensitive content in main flow (deprecated patterns moved to a collapsed "Old patterns" section)
- [LOW] Conditional decision points clearly signposted ("If creating → … / If editing → …")

### Category 4: Output Quality
- [MEDIUM] Template strictness calibrated to use case (strict for API/data formats, flexible for analysis)
- [LOW] Output format explicitly defined or templated

### Category 5: Testability
- [LOW] Triggering accuracy can be evaluated (test cases exist for should-trigger / should-not-trigger / edge cases)
- [HIGH] Isolation behavior can be evaluated (skill works on its own given its stated prerequisites)
- [HIGH] Instruction-following can be evaluated
- [HIGH] Output quality can be evaluated against assertions or rubric
- [LOW] At least 3-5 representative test queries provided or referenced
- [LOW] Coexistence behavior documented (doesn't degrade other skills)

### Category 6: Resource Efficiency
- [MEDIUM] Common-knowledge explanations stripped (does not explain what well-known libraries, file formats, or platforms are)
- [MEDIUM] Pre-built scripts preferred over generated code for deterministic, repeated operations
- [MEDIUM] Execution intent explicit ("Run X" vs "See X for the algorithm")

### Category 7: Security & Trust
- [HIGH] No adversarial instructions (no directives to ignore safety, hide actions, or alter behavior conditionally)
- [HIGH] No hardcoded credentials in any file. Also check: config templates that use literal token placeholders (e.g. `token: my_dbt_token`) encourage embedding secrets — flag as **W007** and recommend environment variable references instead
- [HIGH] Bundled scripts reviewed and behavior matches stated purpose. Also check: scripts that access credential files (`.env`, `profiles.yml`, `mcp.yml`) without instructing the agent not to display or log sensitive values — flag as **Data Exfiltration risk**
- [HIGH] Network access patterns audited (`fetch`, `curl`, `requests`, hardcoded URLs all justified). Also check:
  - Skill fetches content from external URLs/APIs and uses it without an untrusted-content boundary — flag as **W011** and recommend a "Handling External Content" section (see `references/auditing-skills.md` for template)
  - External tools installed at runtime without version pinning (`pip install X`, `uvx tool`, `curl | bash`) — flag as **W012 / RCE** and recommend version pinning or a link to official install docs
- [HIGH] No sleeping payloads (no date- or input-conditional behavior that could mask malicious activity)
- [LOW] Untrusted input boundary: if the skill ingests external or user-supplied content (files, API responses, logs, SQL) and uses it to generate commands or code, there must be explicit guidance to treat that content as untrusted and extract only expected structured fields — flag absence as **IPI (Indirect Prompt Injection)**. Non-blocking — catalog content integrity is the responsibility of the data steward, not the skill.
- [MEDIUM] File system scope contained (no path traversal `../`, no broad globs outside the skill directory)
- [MEDIUM] MCP tool references use full `ServerName:tool_name` format

### Category 8: Coexistence & Recall
- [LOW] Description does not steal triggers from existing skills (check overlap with adjacent skill descriptions)
- [LOW] Tested alongside the active skill set, not just in isolation
- [MEDIUM] Within recall and platform caps (API allows max 8 skills per request; recall degrades beyond ~10-15 active)

### Category 9: Model Compatibility
- [MEDIUM] Tested across all model tiers the team uses. Treat as [HIGH] if the skill serves multiple model tiers in production
- [LOW] Documented which models the skill is validated on

### Category 10: Workflow & Feedback Loops
- [HIGH] Validate-fix-repeat loop included for fragile or quality-critical operations
- [HIGH] Plan-validate-execute pattern used for batch or destructive operations
### Category 11: Maintainability & Lifecycle
- [HIGH] Stored in source control (Git-tracked, PR-reviewable)
- [HIGH] Separation of duties observed (skill author is not also the sole reviewer)
- [MEDIUM] Skill registry entry exists (purpose, owner, version, dependencies, last-eval date)
- [MEDIUM] Versioning strategy defined (production pinned to specific version; rollback plan documented)
- [MEDIUM] Lifecycle stage explicitly documented (Plan / Create-Review / Test / Deploy / Monitor / Iterate-or-Deprecate)

### Category 12: Gotchas / Lessons Learned
- [MEDIUM] Has a "Gotchas" or "Common Mistakes" section capturing real failures from production use. Treat as [HIGH] for mature/widely-deployed skills; [LOW] for clearly-marked v0.1 drafts

### Category 13: Anti-Pattern Audit
- [HIGH] Forward slashes used in all file paths (no Windows-style backslashes)
- [MEDIUM] No voodoo constants (every magic number documented with rationale)
- [MEDIUM] Scripts handle errors explicitly rather than punting to Claude
- [MEDIUM] Default + escape hatch pattern used (not 5+ options presented without a recommended default)
- [MEDIUM] Package install commands explicit; no assumed installations

---

## Output Format

```markdown
# Skill Review: <skill-name>

## Overall Verdict: <PASS | FAIL>

**Production-Ready Recommendation:** <Yes | No | Yes with caveats>

<One-paragraph summary of overall quality and fitness for production.>

## Unit Tests: X / Y passed

| # | Test | Type | Result | Notes |
|---|------|------|--------|-------|
| 1 | <test name> | Should-trigger / Should-not-trigger / Edge case | ✅ PASS / ❌ FAIL | One-line reason if FAIL |
| 2 | ... | | | |

## Category Grades

| # | Category | Grade | Notes |
|---|----------|-------|-------|
| 1 | Triggering (Description Quality) | PASS / FAIL | One-line reason if FAIL, e.g. "description not third-person"; "✓" if PASS |
| 2 | Anatomy & Structure | PASS / FAIL | |
| 3 | Instructions Clarity | PASS / FAIL | |
| 4 | Output Quality | PASS / FAIL | |
| 5 | Testability | PASS / FAIL | |
| 6 | Resource Efficiency | PASS / FAIL | |
| 7 | Security & Trust | PASS / FAIL | |
| 8 | Coexistence & Recall | PASS / FAIL | |
| 9 | Model Compatibility | PASS / FAIL | |
| 10 | Workflow & Feedback Loops | PASS / FAIL | |
| 11 | Maintainability & Lifecycle | PASS / FAIL | |
| 12 | Gotchas / Lessons Learned | PASS / FAIL | |
| 13 | Anti-Pattern Audit | PASS / FAIL | |

## High-Criticality Failures

<For each [HIGH] subcategory that failed:>

### **Subcategory:** <subcategory text>
- **Category:** <category name>
- **Finding:** <evidence-based explanation citing specific line or file>
- **Recommendation:** <concrete fix>

<If none: "None.">

## Medium-Criticality Failures

<Same format as above for [MEDIUM] failures.>

## Low-Criticality Failures

<Same format as above for [LOW] failures.>

## Strengths

<2-4 bullets calling out things the skill does well.>
```

---

## Scoring Rules

**Category PASS requires:**
- All [HIGH] subcategories PASS, AND
- At least 70% of applicable [MEDIUM] subcategories PASS
- [LOW] subcategories do not affect the grade

**Overall PASS requires:** all 13 categories PASS.

**Production-Ready Recommendation:**
- **Yes** — overall PASS, zero [HIGH] failures, at least 80% of [MEDIUM] subcategories passing
- **Yes with caveats** — overall PASS but some [MEDIUM] failures remain
- **No** — any [HIGH] failure, or overall FAIL

## Notes for the Reviewer

- Be concrete. Every FAIL needs a specific reason citing the skill content.
- Do not invent failures. If a subcategory cannot be evaluated from available content, mark N/A.
- For markdown-only skills (no scripts), Category 7 security subcategories and Category 13 script subcategories are often N/A — that is normal.
- Be charitable on terminology and style; be strict on security, triggering, and testability.
- Flag coexistence risks explicitly even if they don't cause individual subcategories to fail.

---

For Category 7 remediation wording, read `references/security-templates.md`.
