# vw_DimSupplier — Unit Test Results

Latest local dbt run executed against the `vw_DimSupplier` model on branch `main`.

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
| 1 | pass | 17.73 | `test_dim_supplier__latest_sync_wins` | Pass |
| 2 | pass | 14.56 | `test_dim_supplier__null_columns_are_hardcoded` | Pass |

**Summary: 2 / 2 passed.**

## Notes on what the tests assert

- `test_dim_supplier__latest_sync_wins` — two source rows share
  `SUPPLIER_ID=301`. The row with `_FIVETRAN_SYNCED = '2024-06-01'` survives.
  Updated `SupplierName`, `ContactName`, `ContactEmail`, `ContactPhone` and
  `EtlBatchId = 0` verified.
- `test_dim_supplier__null_columns_are_hardcoded` — single source row; all 12
  columns not available in the source are asserted as `NULL`.
