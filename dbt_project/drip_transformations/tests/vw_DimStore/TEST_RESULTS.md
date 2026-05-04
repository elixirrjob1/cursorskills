# vw_DimStore — Unit Test Results

Latest local dbt run executed against the `vw_DimStore` model on branch `main`.

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
| 1 | pass | 13.78 | `test_dim_store__latest_sync_wins` | Pass |
| 2 | pass | 14.13 | `test_dim_store__null_columns_are_hardcoded` | Pass |

**Summary: 2 / 2 passed.**

## Notes on what the tests assert

- `test_dim_store__latest_sync_wins` — two source rows share `STORE_ID=10`.
  The row with `_FIVETRAN_SYNCED = '2024-06-01'` survives the `QUALIFY
  ROW_NUMBER()` filter. Pass-through columns (`StoreName`, `StreetAddress`,
  `City`, `StateProvince`, `PostalCode`) and `EtlBatchId = 0` verified.
- `test_dim_store__null_columns_are_hardcoded` — single source row; all 13
  columns not available in the source are asserted as `NULL`.
