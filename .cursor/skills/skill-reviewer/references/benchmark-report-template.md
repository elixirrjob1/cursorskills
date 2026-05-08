<!--
  skill-reviewer Step 7b: Copy to tests/results/benchmark-<skill-name>-<RUN_SLUG>.md
  in the *target* skill folder. Remove this comment block. Replace every ___PLACEHOLDER___.
  Summary metrics are fixed (same order as benchmark-report-template.html).

  Assertion Detail: use the table below; ___ASSERTION_DETAIL_ROWS___ must list every graded assertion (Eval | Assertion | Passed | Evidence) for every eval in evals.json — on incremental runs, carry forward rows for SKIP evals from prior grading files.
  Forbidden: a section that only says to open grading-*.json files without this table body.

  Category Grades: include an Explanation column (brief rationale, same spirit as the narrative review).
-->

# Benchmark Report: ___SKILL_NAME___
___META_LINE___

## Summary

| Metric | Value |
|--------|-------|
| Overall Verdict | ___SUMMARY_VERDICT___ |
| Unit Tests | ___SUMMARY_UNIT_TESTS___ |
| Assertions | ___SUMMARY_ASSERTIONS___ |
| Categories | ___SUMMARY_CATEGORIES___ |
| High Failures | ___SUMMARY_HIGH_FAILURES___ |
| Medium Failures | ___SUMMARY_MEDIUM_FAILURES___ |
| Comparator | ___SUMMARY_COMPARATOR___ |

## Unit Test Results

| # | Test | Type | Result | Assertions |
|---|------|------|--------|------------|
___UNIT_TEST_ROWS___

## Assertion Detail

| Eval | Assertion | Passed | Evidence |
|------|-----------|--------|----------|
___ASSERTION_DETAIL_ROWS___

## Category Grades

| # | Category | Grade | Explanation |
|---|----------|-------|-------------|
___CATEGORY_ROWS___

## Version Comparison (if comparator was run)

___VERSION_COMPARISON_BLOCK___

## History (all reviews)

| Reviewed at (ISO UTC) | Calendar | Unit Tests | Assertions | Categories | Verdict | Notes |
|----------------------|----------|------------|------------|------------|---------|-------|
___HISTORY_ROWS___

Full narrative: `___REVIEW_FILENAME___`.
