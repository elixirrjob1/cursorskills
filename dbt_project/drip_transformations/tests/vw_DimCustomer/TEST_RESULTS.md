# vw_DimCustomer — Unit Test Results

Latest local dbt run executed against the `vw_DimCustomer` model on branch `main`.

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
| 1 | pass | 20.77 | `test_dim_customer__scd2_dedup_and_current_flag` | Pass |
| 2 | pass | 15.42 | `test_dim_customer__deleted_flag_when_inactive` | Pass |
| 3 | pass | 12.02 | `test_dim_customer__fullname_handles_nulls` | Pass |

**Summary: 3 / 3 passed.**

## Notes on what the tests assert

- `test_dim_customer__scd2_dedup_and_current_flag` — proves the
  LEAD-before-dedup behavior of `vw_DimCustomer.sql`. The middle no-op row is
  dropped by `LAG(Hashbytes)` row reduction, but the surviving v1 row's
  `EffectiveEndDateTime` still resolves to *(dropped row's start − 1µs)*.
  `CurrentFlagYN` is `'Y'` only on the row whose `LeadEffectiveStartDateTimeUTC`
  is NULL.
- `test_dim_customer__deleted_flag_when_inactive` — single Fivetran-inactive
  row with no lead. Asserts `CurrentFlagYN = 'Y'` (no lead means final row,
  regardless of active flag) and `DeletedFlagYN = 'Y'` (final row AND
  `_FIVETRAN_ACTIVE = false`); `EffectiveEndDateTime` falls back to
  `_FIVETRAN_END`.
- `test_dim_customer__fullname_handles_nulls` — `vw_DimCustomer.sql` uses
  `TRIM(COALESCE(FIRST_NAME, '')) || ' ' || TRIM(COALESCE(LAST_NAME, ''))`,
  so NULL parts collapse to empty strings (different from `vw_DimEmployee`).
