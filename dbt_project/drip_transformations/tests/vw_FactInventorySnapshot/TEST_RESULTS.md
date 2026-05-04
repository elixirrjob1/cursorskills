# vw_FactInventorySnapshot — Unit Test Results

Latest local dbt run executed against the `vw_FactInventorySnapshot` model on branch `main`.

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
| 1 | pass | 12.41 | `test_fact_inventory__join_and_pass_through` | Pass |
| 2 | pass | 12.76 | `test_fact_inventory__missing_product_yields_null_cost` | Pass |
| 3 | pass | 14.60 | `test_fact_inventory__dedup_latest_sync` | Pass |
| 4 | pass | 19.46 | `test_fact_inventory__dedup_tied_sync_identical_rows` | Pass |

**Summary: 4 / 4 passed.**

## Notes on what the tests assert

- `test_fact_inventory__join_and_pass_through` — one inventory row with a
  matching product. Verifies `UnitCost` comes from PRODUCTS `COST_PRICE`,
  pass-through of `QuantityOnHand`, `ReorderPoint`, `InventoryValue`, and that
  all stub columns (`QuantityReserved`, etc.) are `NULL`.
- `test_fact_inventory__missing_product_yields_null_cost` — `PRODUCT_ID=999`
  has no match in PRODUCTS. The `LEFT JOIN` yields `UnitCost = NULL` while
  `InventoryValue` (from INVENTORY) is still populated.
- `test_fact_inventory__dedup_latest_sync` — two INVENTORY rows with
  `INVENTORY_ID=3001`. Only the row with `_FIVETRAN_SYNCED = '2024-02-01'`
  survives. `QuantityOnHand` updated from 100 → 150.
- `test_fact_inventory__dedup_tied_sync_identical_rows` — duplicate INVENTORY
  and PRODUCTS rows with tied `_FIVETRAN_SYNCED` and identical payload produce
  one stable logical output row.
