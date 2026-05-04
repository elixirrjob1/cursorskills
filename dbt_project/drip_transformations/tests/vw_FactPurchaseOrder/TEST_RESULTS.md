# vw_FactPurchaseOrder — Unit Test Results

Latest local dbt run executed against the `vw_FactPurchaseOrder` model on branch `main`.

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
| 1 | pass | 11.70 | `test_fact_po__join_and_order_amount` | Pass |
| 2 | pass | 13.14 | `test_fact_po__null_expected_date_yields_null_hash` | Pass |
| 3 | pass | 18.27 | `test_fact_po__dedup_latest_sync` | Pass |
| 4 | pass | 20.25 | `test_fact_po__orphan_item_dropped_by_inner_join` | Pass |

**Summary: 4 / 4 passed.**

## Notes on what the tests assert

- `test_fact_po__join_and_order_amount` — one PO item (qty 10, unit cost 25.50)
  joined to its header. `OrderAmount = 255.0000`. Pass-through of
  `PurchaseOrderNumber`, `OrderStatus`, `QuantityOrdered`. All stub columns
  (`QuantityShipped`, etc.) verified as `NULL`.
- `test_fact_po__null_expected_date_yields_null_hash` — PO header with
  `EXPECTED_DATE = NULL`. The `IFF(EXPECTED_DATE IS NULL, NULL, HASH(...))`
  guard yields `DateExpectedHashFK = NULL`.
- `test_fact_po__dedup_latest_sync` — two PO_ITEM rows with `PO_ITEM_ID=3`.
  Only the latest sync survives; `QuantityOrdered` updated from 20 → 25,
  `OrderAmount = 125.0000`.
- `test_fact_po__orphan_item_dropped_by_inner_join` — PO item with no matching
  header returns zero rows, protecting the model's intended INNER JOIN behavior.
