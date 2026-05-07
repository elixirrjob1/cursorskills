<!--
  skill-reviewer Step 7b: Copy to tests/results/benchmark-<skill-name>-YYYY-MM-DD.md
  in the *target* skill folder. Remove this comment block. Replace every ___PLACEHOLDER___.
  Summary metrics are fixed (same order as benchmark-report-template.html).
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

___ASSERTION_DETAIL_BLOCK___

## Category Grades

| # | Category | Grade |
|---|----------|-------|
___CATEGORY_ROWS___

## Version Comparison (if comparator was run)

___VERSION_COMPARISON_BLOCK___

## History (all reviews)

| Date | Unit Tests | Assertions | Categories | Verdict | Comparison |
|------|------------|------------|------------|---------|------------|
___HISTORY_ROWS___

Full narrative: `___REVIEW_FILENAME___`.
