# vw_DimEmployee — Unit Test Results

Latest local dbt run executed against the `vw_DimEmployee` model on branch `main`.

## Run metadata

| Field | Value |
| --- | --- |
| dbt version | `2.0.0-preview.173` (Fusion) |
| Adapter | Snowflake |
| Target | `dev` |
| Run date (local) | 2026-04-30 |
| Total test elapsed | ~23.0s (full suite of 24 tests) |
| Selector | `dbt test --select "test_type:unit"` |

## Per-test results

| # | Status | Time (s) | Test | Message |
| - | ------ | -------- | ---- | ------- |
| 1 | pass | 21.20 | `test_dim_employee__latest_sync_wins` | Pass |
| 2 | pass | 14.94 | `test_dim_employee__fullname_handles_nulls` | Pass |

**Summary: 2 / 2 passed.**

## Notes on what the tests assert

- `test_dim_employee__latest_sync_wins` — two source rows share an
  `EMPLOYEE_ID`; the row with the latest `_FIVETRAN_SYNCED` survives the
  `QUALIFY ROW_NUMBER() OVER (PARTITION BY EMPLOYEE_ID ORDER BY _FIVETRAN_SYNCED DESC)` filter.
- `test_dim_employee__fullname_handles_nulls` — `vw_DimEmployee.sql` uses
  `TRIM(FIRST_NAME) || ' ' || TRIM(LAST_NAME)` *without* `COALESCE`, so any
  NULL on either side propagates through string concatenation and yields
  `FullName = NULL`. Intentionally different from the customer dimension,
  which wraps each side in `COALESCE(..., '')`.
