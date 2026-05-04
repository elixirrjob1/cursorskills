# vw_DimProduct — Unit Test Results

Latest local dbt run executed against the `vw_DimProduct` model on branch `main`.

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
| 1 | pass | 17.93 | `test_dim_product__scd2_dedup_and_current_flag` | Pass |
| 2 | pass | 13.63 | `test_dim_product__soft_deleted_vs_hard_deleted` | Pass |
| 3 | pass | 15.83 | `test_dim_product__is_discontinued_reflects_active_flag` | Pass |

**Summary: 3 / 3 passed.**

## Notes on what the tests assert

- `test_dim_product__scd2_dedup_and_current_flag` — proves the LEAD-before-dedup
  behavior. The middle no-op row is dropped by `LAG(Hashbytes)` row reduction,
  but the surviving v1 row's `EffectiveEndDateTime` still resolves to
  *(dropped row's start − 1µs)*. `CurrentFlagYN = 'Y'` only on the final row.
- `test_dim_product__soft_deleted_vs_hard_deleted` — two single-row products
  exercise the three-way flag matrix: source `ACTIVE` vs Fivetran
  `_FIVETRAN_ACTIVE` determines `SoftDeletedFlagYN` and `DeletedFlagYN`.
- `test_dim_product__is_discontinued_reflects_active_flag` — simple projection:
  `ACTIVE=true → IsDiscontinued=false`, `ACTIVE=false → IsDiscontinued=true`.
