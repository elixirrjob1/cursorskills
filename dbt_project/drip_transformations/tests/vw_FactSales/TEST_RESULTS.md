# vw_FactSales — Unit Test Results

Latest local dbt run executed against the `vw_FactSales` model on branch `main`.

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
| 1 | pass | 18.66 | `test_fact_sales__join_and_calculated_amounts` | Pass |
| 2 | pass | 17.11 | `test_fact_sales__dedup_latest_sync` | Pass |
| 3 | pass | 10.86 | `test_fact_sales__null_customer_and_employee` | Pass |
| 4 | pass | 14.13 | `test_fact_sales__missing_product_yields_null_cost` | Pass |

**Summary: 4 / 4 passed.**

## Notes on what the tests assert

- `test_fact_sales__join_and_calculated_amounts` — full three-way join with
  matching data. `GrossAmount = 3 × 49.99 = 149.9700`,
  `CostAmount = 3 × 20.00 = 60.0000`. Pass-through of `TransactionNumber`,
  `TransactionLineNumber`, `Quantity`, `UnitPrice`, `UnitCost`.
- `test_fact_sales__dedup_latest_sync` — duplicate line, order, and product rows
  verify that the latest `_FIVETRAN_SYNCED` row survives in each CTE before
  joins and amount calculations.
- `test_fact_sales__null_customer_and_employee` — anonymous walk-in sale with
  `CUSTOMER_ID = NULL` and `EMPLOYEE_ID = NULL`. Both `CustomerHashFK` and
  `EmployeeHashFK` resolve to `NULL` via the `IFF` guard.
- `test_fact_sales__missing_product_yields_null_cost` — `PRODUCT_ID=999` has no
  match in PRODUCTS. `UnitCost = NULL` and `CostAmount = NULL` via `LEFT JOIN`
  propagation. `GrossAmount` is still computed (uses `UNIT_PRICE` from the line
  item, not the product lookup).
