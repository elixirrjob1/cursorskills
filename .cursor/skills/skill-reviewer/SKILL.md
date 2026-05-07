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

**Timestamp this run (required):** As soon as the skill folder path is known, record:
- **`reviewed_at`** — UTC wall time in ISO 8601, e.g. `2026-05-07T14:30:52Z` (use `date -u +"%Y-%m-%dT%H:%M:%SZ"`).
- **`RUN_SLUG`** — compact, filesystem-safe token **derived from the same instant**: `YYYY-mm-ddTHHMMSSZ` with colons removed from the time portion, e.g. `2026-05-07T143052Z`. Same instant as `reviewed_at`; used in folder and filenames.
- **`RUN_DIR`** — `tests/results/runs/<RUN_SLUG>/` (under the skill being reviewed). **Every** artifact for this review goes here: `timing.json`, `grading-*.json`, `comparison-*.json`, `comparison-summary.json`, `review-<skill>-<RUN_SLUG>.md`, `benchmark-<skill>-<RUN_SLUG>.md`, `benchmark-<skill>-<RUN_SLUG>.html`. The `tests/results/` root holds only `history.json`, the `runs/` tree, and the `snapshots/` tree.

**Previous snapshot for Step 3c:** Read `tests/results/history.json` if it exists. Take the **last** entry in the `history` array (previous completed review). Let **`snapshot_dir`** be that entry’s **`snapshot_dir`** field. If **`snapshot_dir`** is missing, `null`, or empty, **`PREVIOUS_SNAPSHOT`** is unavailable — skip the diff check and Step 3c. Otherwise resolve **`PREVIOUS_SNAPSHOT`** = `<skill_dir>/tests/results/<snapshot_dir>`. If that directory does not exist on disk (e.g. clone without snapshots), treat as unavailable.

**Check for a prior review (incremental mode):**

Prefer **`tests/results/history.json`**: if present, use the latest entry by **`reviewed_at`** (ISO string compare) or the last array element if all same day. From that entry (and the review file it cites), extract:
- Which unit tests previously **PASSED** → skip re-running those subagents in Step 3; carry forward their PASS result
- Which categories previously **PASSED** → skip re-evaluating those in Step 4; carry forward their PASS result

If `history.json` is missing or empty, fall back to an existing **`review-*.md`** in `tests/results/` — use the file whose name sorts last (prefer filenames containing a **`RUN_SLUG`** over date-only names if both exist).

Only re-run tests and re-evaluate categories that previously **FAILED** or are **new** (no prior result). Announce at the start: _"Prior review found — running incremental re-check of N failed tests and M failed categories."_

If no prior review exists, run the full review.

**Step 3c eligibility (diff check):** After `PREVIOUS_SNAPSHOT` is resolved, if it is non-empty, compare **skill-definition** subtrees only (ignore all of `tests/results/` when deciding — new grading JSON and benchmarks always differ and must not force comparators):

For each of: `SKILL.md`, `agents/` (if present), `references/` (if present), `scripts/` (if present), `tests/evals/`, `tests/test-cases.md`, `tests/test_skill.py` — run `diff -qr "$PREVIOUS_SNAPSHOT/<path>" "$SKILL_DIR/<path>"` (skip missing paths if one side lacks a folder). If **any** diff exits non-zero, set **`RUN_STEP_3C=true`**. If all absent-or-equal, **skip Step 3c** (state in benchmark: no skill-definition change vs prior snapshot). If `PREVIOUS_SNAPSHOT` was empty, **skip Step 3c**.

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

**Two independent skip rules apply — apply both:**

1. **Category skip (rubric only):** If a rubric category carried PASS from the prior review (Step 1), skip re-evaluating it in Step 4. This does **not** affect whether executors run.
2. **Executor skip (unit tests):** If `RUN_STEP_3C` is **false** (no skill-definition change vs prior snapshot), skip re-running executor subagents for evals that previously PASSed; set `result: "SKIP"` and `carried_from` in `timing.json`. If `RUN_STEP_3C` is **true** (SKILL.md or other skill-definition files changed), **run all executor subagents** — even for previously-PASSing evals — so the comparator (Step 3c) has fresh outputs to compare against the prior snapshot. In this case set `result: "PASS" | "FAIL"` (never SKIP) and omit `carried_from`.

For each executor subagent being run, launch with:
- The full skill content (SKILL.md + any reference files) as context
- The test `input` as the user prompt
- No instruction to self-judge — the subagent just responds naturally

Run all subagents in parallel. Once responses are collected, evaluate each one yourself by comparing the actual response against the `expected` behavior defined in `test-cases.md`:

- **PASS**: the actual response matches the expected behavior (e.g. expected "generates SQL and queries metadata" → response contains SQL and mentions metadata discovery)
- **FAIL**: the actual response contradicts the expected behavior (e.g. expected "asks clarifying question" → response instead attempts to run a query)

Do not ask the subagent to grade itself. You evaluate the responses against the expected column in `test-cases.md`.

**Record run results:** Save all run outcomes to **`tests/results/runs/<RUN_SLUG>/timing.json`** (see Step 1 — create `RUN_DIR` if needed):

```json
{
  "review_date": "YYYY-MM-DD",
  "reviewed_at": "YYYY-MM-DDTHH:MM:SSZ",
  "run_slug": "<RUN_SLUG>",
  "runs": [
    {
      "eval_id": 1,
      "test_type": "should-trigger | should-not-trigger | edge-case",
      "prompt_short": "<first 80 chars of prompt>",
      "result": "PASS | FAIL | SKIP",
      "carried_from": null
    }
  ]
}
```

For skipped tests (carried from prior review), set `result: "SKIP"` and `carried_from: "runs/<prior_RUN_SLUG>/grading-<eval_id>.json"` (relative to `tests/results/`). **Do not copy** the prior grading JSON into the new `RUN_DIR` — the pointer is the canonical reference. If no prior grading file exists (e.g. the first review), set `carried_from: null`.

Summarise as `X / Y tests passed` before proceeding to the rubric.

### Step 3b: Grade assertions

Spawn grader subagents in parallel — one per eval with `result: "PASS" | "FAIL"` (i.e. **actually executed** this run). Do **not** spawn a grader for `result: "SKIP"` evals — their grading record lives at the path in `carried_from`; do not copy or re-create it. Pass each grader:
- The eval's `assertions` array from `evals.json`
- The full actual response from the subagent

See `agents/grader.md` for grader instructions. Each grader saves its output to **`tests/results/runs/<RUN_SLUG>/grading-<eval_id>.json`** (only for evals executed this run). When generating benchmarks (Step 7b/7c), for SKIP evals read the grading JSON from `carried_from`; add a `↩ carried from <prior_slug>` note in the Evidence cell to show the data is from a prior run:

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

### Step 3c: Blind comparison (when prior snapshot differs)

**Purpose:** Compare executor outputs from the **current** skill tree vs the **frozen tree** saved at the end of the **previous** review (see Step 7d).

**Skip Step 3c when:**
- **`RUN_STEP_3C`** is false (no `PREVIOUS_SNAPSHOT`, or `diff` showed no changes vs that snapshot — see Step 1), OR
- This is the **first** review (no prior `snapshot_dir` in history).

**Do not** rely on `/tmp` or a manual pre-edit copy. The **old** version is always **`PREVIOUS_SNAPSHOT`** from the last history entry.

**Run:** For each eval that was **not** `SKIP` in `timing.json` for this run, spawn **two** executor subagents in parallel with the **same** user prompt:
- **New:** Load `SKILL.md` and bundled files from the **current** skill folder on disk.
- **Old:** Load `SKILL.md` and bundled files from **`PREVIOUS_SNAPSHOT`** (same relative paths). If a file existed only in one tree, state that in the executor briefing.

Do **not** label which output is old vs new when calling the comparator. Then spawn one **comparator** subagent per eval (see `agents/comparator.md`), passing `output_a` and `output_b` unlabelled. Track internally which output came from **current** vs **snapshot**. Each comparator saves to **`tests/results/runs/<RUN_SLUG>/comparison-<eval_id>.json`** with fields: `eval_id`, `prompt_short`, `verdict` (`a_wins | b_wins | tie`), `reasoning`.

**Aggregate:** After all comparators complete, map `a_wins`/`b_wins` to `new_wins`/`old_wins`. Save **`tests/results/runs/<RUN_SLUG>/comparison-summary.json`** with fields: `reviewed_at`, `review_date`, `overall` (`new_wins | old_wins | tie | mixed`), `new_wins`, `old_wins`, `ties`, `total_compared`. `overall` is `new_wins` if new > old, `old_wins` if reversed, `tie` if equal, `mixed` if split. Include the summary in the benchmark report (Step 7b/7c) when present.

### Step 4: Review each subcategory

Skip any category that carried a **PASS** from the prior review (Step 1 incremental check) — carry it forward as-is. For categories being re-evaluated, assign **PASS**, **FAIL** (with a one-to-two-sentence evidence-based reason), or **N/A** (criterion does not apply). Cite the specific line, file, or absence of content that justifies the verdict.

### Step 5: Roll up to category grades

A category **PASS** requires:
- All [HIGH] subcategories pass, AND
- At least 70% of applicable [MEDIUM] subcategories pass

### Step 6: Generate the report

Use the output format defined at the bottom of this skill. Include the unit test summary (`X / Y tests passed`) at the top of the report.

After generating the report, save it under **`RUN_DIR`** (Step 1): **`tests/results/runs/<RUN_SLUG>/review-<skill-name>-<RUN_SLUG>.md`** (e.g. `tests/results/runs/2026-05-07T143052Z/review-json-to-excel-export-2026-05-07T143052Z.md`). All run artifacts (review, benchmarks, timing, grading) live together in that one folder — `tests/results/` root contains only `history.json`, `runs/`, and `snapshots/`. Create `RUN_DIR` if needed. Legacy date-only names (`review-<skill>-YYYY-MM-DD.md`) may still exist in older run dirs — new runs always use `RUN_SLUG`. If the path is unavailable, output the report inline only.

In incremental runs, append `↩ carried` to the existing Notes value in the Unit Tests and Category Grades tables — do not replace the original note text. Example: `Discovered FactSales via OM, returned $35K ↩ carried`. Only re-evaluated rows get new note text.

### Step 7: Update history and generate benchmark reports

After saving the review markdown file, do **four** more things in order (**7a–7d**).

#### 7a: Append to `tests/results/history.json`

Read the existing file (create it with `{"history": []}` if absent), then append one new entry and write it back. Never truncate prior entries.

```json
{
  "history": [
    {
      "reviewed_at": "YYYY-MM-DDTHH:MM:SSZ",
      "date": "YYYY-MM-DD",
      "run_slug": "YYYY-mm-ddTHHMMSSZ",
      "skill_version": "<git short-hash or 'unversioned'>",
      "unit_tests": { "passed": 0, "total": 0, "pass_rate": 0.00 },
      "assertions": { "passed": 0, "total": 0, "pass_rate": 0.00 },
      "categories": { "passed": 0, "total": 13, "pass_rate": 0.00 },
      "verdict": "PASS | FAIL",
      "high_failures": ["<subcategory text>"],
      "review_file": "runs/<RUN_SLUG>/review-<skill-name>-<RUN_SLUG>.md",
      "timing_file": "runs/<RUN_SLUG>/timing.json",
      "benchmark_md": "runs/<RUN_SLUG>/benchmark-<skill-name>-<RUN_SLUG>.md",
      "benchmark_html": "runs/<RUN_SLUG>/benchmark-<skill-name>-<RUN_SLUG>.html",
      "snapshot_dir": "snapshots/<skill-folder-name>/<RUN_SLUG>",
      "comparison_file": "runs/<RUN_SLUG>/comparison-summary.json or null"
    }
  ]
}
```

> ⚠️ **Required in every history.json entry:** `reviewed_at` (ISO 8601 UTC, from Step 1), `run_slug`, `review_file`, `benchmark_md`, `benchmark_html`, `snapshot_dir`, `verdict`. When describing or citing `history.json` to a user, always name **`reviewed_at`** explicitly — it is the primary timestamp key for the run.

- **`reviewed_at`** — ISO 8601 UTC; must match the instant recorded in Step 1.
- **`date`** — calendar portion only (for dashboards); same local-calendar day as `reviewed_at` in UTC unless you document otherwise.
- **`run_slug`** — identical to **`RUN_SLUG`** (Step 1).
- **`snapshot_dir`** — relative to `tests/results/`; the directory is created in **7d** (after benchmarks), not before.

For `skill_version`, run `git -C <skill-folder> rev-parse --short HEAD 2>/dev/null || echo unversioned`.

#### 7b: Generate `benchmark-<skill-name>-<RUN_SLUG>.md`

Save to **`RUN_DIR`** — i.e. `tests/results/runs/<RUN_SLUG>/benchmark-<skill-name>-<RUN_SLUG>.md` (same folder as the review `.md`, timing, and grading JSON).

**Reuse the canonical markdown template (preferred):** Read `references/benchmark-report-template.md` from **this skill’s folder** (`skill-reviewer`) — e.g. `.cursor/skills/skill-reviewer/references/benchmark-report-template.md`. Copy to `RUN_DIR/benchmark-<skill-name>-<RUN_SLUG>.md`, remove the `<!-- … -->` comment block at the top, and **replace every placeholder** (`___NAME___`).

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
| `___META_LINE___` | One line including **ISO `reviewed_at`**, e.g. `_Generated: 2026-05-07T14:30:52Z UTC · <run note>_` |
| `___SUMMARY_VERDICT___` | `PASS` or `FAIL` |
| `___SUMMARY_UNIT_TESTS___` | e.g. `10 / 10 passed (100%)` |
| `___SUMMARY_ASSERTIONS___` | e.g. `30 / 30 (100%)` |
| `___SUMMARY_CATEGORIES___` | e.g. `9 / 13 passed` |
| `___SUMMARY_HIGH_FAILURES___` | e.g. `1` or `—` |
| `___SUMMARY_MEDIUM_FAILURES___` | e.g. `5 (rolled up)` or `—` |
| `___SUMMARY_COMPARATOR___` | e.g. `— (no diff vs prior snapshot)` or `new_wins` summary from `comparison-summary.json` |
| `___UNIT_TEST_ROWS___` | Markdown table body rows: `\| # \| label \| type \| ✅ PASS / ❌ FAIL \| a/b \|` |
| `___ASSERTION_DETAIL_ROWS___` | **Required:** one row per assertion for **every eval** in `evals.json` (reuse prior grading JSON rows for incremental SKIP evals). **Forbidden:** omitting this table or replacing the section with only text like “Per-eval JSON: `runs/…/grading-<n>.json`” / “see JSON files” without listing every assertion inline. A footnote listing artifact paths is allowed *after* the full table, not instead of it. |
| `___CATEGORY_ROWS___` | Rows `\| n \| Category \| PASS / FAIL \| Explanation \|` — **Explanation** = one short clause (why PASS or key finding for FAIL), aligned with the narrative `review-*.md`. |
| `___VERSION_COMPARISON_BLOCK___` | Comparator table or italic `_Not run — …_` |
| `___HISTORY_ROWS___` | One row per `history.json` entry: `\| reviewed_at \| date \| 10/10 \| 30/30 \| 9/13 \| FAIL \| note \|` |
| `___REVIEW_FILENAME___` | `review-<skill-name>-<RUN_SLUG>.md` for this run |

**Assertion Detail (required):** The **Assertion Detail** section must contain a **complete Markdown table** (columns: Eval, Assertion, Passed, Evidence) with **one row per assertion** for **every eval** in `tests/evals/evals.json`. Populate from this run’s **`tests/results/runs/<RUN_SLUG>/grading-<eval_id>.json`** when Step 3 re-ran the eval; on **incremental** runs, for evals **not** re-run (SKIP), copy assertion rows from the **prior** run’s grading files or prior benchmark so **no eval drops out**.

**Forbidden:** Using only a pointer to JSON files as the Assertion Detail body (e.g. “Per-eval JSON: `runs/<RUN_SLUG>/grading-<n>.json` for *n* = 1…10” with **no** assertion rows). You may add a sentence **after** the full table citing machine-readable grader paths, but the table is mandatory.

**Category Grades (required):** Include column **Explanation** (brief rationale per category, consistent with the review narrative).

**Fallback** if the template file cannot be read: write the same sections manually; **Summary** must still list all seven metrics in the order above; **Assertion Detail** and **Category Grades** rules still apply.

#### 7c: Generate `benchmark-<skill-name>-<RUN_SLUG>.html`

Save to **`RUN_DIR`** alongside the `.md` file — i.e. `tests/results/runs/<RUN_SLUG>/benchmark-<skill-name>-<RUN_SLUG>.html`. Must be a **fully self-contained** HTML file — no external CDN links, all CSS in the template’s `<style>` block (do not strip or rename classes).

**Reuse the canonical template (preferred):** Read `references/benchmark-report-template.html` from **this skill’s folder** (`skill-reviewer`), not from the skill under review. In a typical repo layout that is `.cursor/skills/skill-reviewer/references/benchmark-report-template.html`. Copy the entire file to `RUN_DIR/benchmark-<skill-name>-<RUN_SLUG>.html`, then **replace every placeholder** (each is unique `___NAME___`):

| Placeholder | Replace with |
|-------------|----------------|
| `___SKILL_NAME___` | Target skill display name (same as markdown benchmark) |
| `___META_LINE___` | One line, e.g. `Generated: 2026-05-07T14:30:52Z UTC · <short run note>` |
| `___REVIEW_FILENAME___` | `review-<skill-name>-<RUN_SLUG>.md` for this run |
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
| `___HISTORY_ROWS___` | One row per `history.json` entry (include **`reviewed_at`**); use **bar-track** / **bar-fill** for rates where applicable |

Remove the instructional HTML comment block from the template in the saved output. **Do not** leave any `___…___` placeholder in the final file.

**Assertion Detail (HTML):** Must mirror the markdown benchmark: a full **`<table>`** with thead **Eval | Assertion | Passed | Evidence** and one `<tr>` per assertion for **every eval** in the suite (same incremental carry-forward rule as Step 7b).

**Category Grades (HTML):** Fourth column **Explanation** for every category.

**Fallback** if the template file cannot be read: build equivalent HTML from scratch using the same sections and styles: white background, `system-ui`, 14px; tables `border-collapse: collapse`, `1px solid #ccc`, alternating `#f9f9f9`; `.pass` / `.fail` / `.skip` as in the template; history pass rates as inline green bar + percentage; **Assertion Detail** and **Category Explanation** rules still apply.

#### 7d: Snapshot the skill tree (every run)

After **7b** and **7c**, materialize **`tests/results/snapshots/<skill-folder-name>/<RUN_SLUG>/`** (`<skill-folder-name>` = basename of the reviewed skill directory, e.g. `skill-reviewer`):

```bash
mkdir -p "$SKILL_DIR/tests/results/snapshots/<skill-folder-name>/$RUN_SLUG"
rsync -a \
  --exclude='tests/results/snapshots/' \
  --exclude='tests/results/runs/' \
  "$SKILL_DIR/" "$SKILL_DIR/tests/results/snapshots/<skill-folder-name>/$RUN_SLUG/"
```

This records **`SKILL.md`**, **`agents/`**, **`references/`**, **`tests/evals/`**, **`tests/test-cases.md`**, **`tests/test_skill.py`**, and **`tests/results/history.json`** — but **not** `tests/results/runs/` (all per-run artifacts stay there) and **not** `tests/results/snapshots/` (avoids recursion). The **next** review diffs the live skill-definition paths against this snapshot to decide Step 3c. The **next** review diffs the live tree vs the **last** history entry’s `snapshot_dir` to decide Step 3c.

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

---

## Registry

| Field | Value |
|-------|-------|
| Owner | Platform / AI Engineering team |
| Reviewer | Distinct from author — peer or tech-lead review required before promoting changes to `main` |
| Version | Pinned to `main`; feature changes branch from `main`, reviewed via PR, squash-merged |
| Lifecycle stage | **Deploy / Monitor** — actively used; iterate on failing categories via PR; deprecate when replaced by a successor rubric |
| Last evaluated | 2026-05-07 (`run_slug: 2026-05-07T112120Z`) |
| Dependencies | `agents/grader.md`, `agents/comparator.md`, `references/benchmark-report-template.md/.html`, `references/auditing-skills.md`, `references/security-templates.md` |

**Separation of duties:** The author of a `SKILL.md` change must **not** be the sole approver of the PR that merges it. A second reviewer (peer or tech-lead) must inspect the diff and approve via the normal PR review process before merge.

---

## Model Compatibility

Validated on **Claude 3.5 Sonnet** (the default model tier for Cursor `generalPurpose` subagents). The rubric evaluation logic (Steps 1–7) is text-only and model-agnostic; it has been spot-checked on **Claude 3 Haiku** for the should-not-trigger routing cases.

**Note:** Step 3 executor subagents and Step 3b grader subagents are the most token-intensive path. For teams running on a lighter tier (e.g. Haiku-only), test the executor outputs on at least three should-trigger evals before adopting this workflow in production.

---

## Deployment / Coexistence Notes

- This skill's description contains specific trigger terms (`review`, `evaluate`, `audit`, `grade`, `lint`) that could overlap with broader code-review or QA skills. Validate alongside any co-deployed code-reviewer or linter skill to confirm routing is correct.
- The Cursor platform carries all active skills in the system prompt; recall quality degrades beyond ~10–15 loaded skills. If the active skill count approaches that range, test this skill's trigger precision explicitly — run the should-trigger / should-not-trigger eval suite with the full active skill list loaded.
- The API cap is 8 skills per request on the Skills API. Exceeding that drops skills silently — ensure skill-reviewer is ranked appropriately if slot limits apply.

---

## Gotchas

**Step 3c has no comparator on the first review.**  
`PREVIOUS_SNAPSHOT` comes from the last `history.json` entry's `snapshot_dir`. On the very first review, and on any run where the prior row had `snapshot_dir: null`, Step 3c is skipped entirely — this is correct, not a bug.

**Snapshot recursion guard.**  
Step 7d uses `--exclude='tests/results/snapshots/'` and `--exclude='tests/results/runs/'` in the `rsync` command. If those flags are omitted, the snapshot will recursively embed prior snapshots and grow unbounded.

**Incremental carry-forward only applies to prior PASSes.**  
A category or eval that was previously **FAIL** is never carried — it is always re-evaluated. Carrying a FAIL forward would silently drop required re-work.

**Grader is the source of truth on assertion conflicts.**  
If your initial PASS/FAIL verdict for a unit test disagrees with the grader's `overall_result`, trust the grader (Step 3b) and update the unit test summary accordingly.

**`RUN_SLUG` colons are stripped.**  
`reviewed_at` uses standard ISO 8601 (`2026-05-07T11:21:20Z`); `RUN_SLUG` removes the colons from the time portion (`2026-05-07T112120Z`). Both refer to the same instant. Use `RUN_SLUG` for folder names and filenames; use `reviewed_at` for JSON fields and report headers.

**Legacy `runs/YYYY-MM-DD/` directories.**  
Older runs landed under a calendar-only path. New runs use `runs/<RUN_SLUG>/`. Both shapes are valid on disk; the `timing_file` field in `history.json` is the canonical pointer.
