# vw_FactSales

Sales fact view with a three-way join: SALES_ORDER_ITEMS, SALES_ORDERS, and PRODUCTS.

- **dbt model name**: `vw_FactSales`
- **Materialization**: `view`
- **Model location**: `models/views/vw_FactSales.sql`
- **Sources**:
  - `source('bronze_erp__dbo', 'SALES_ORDER_ITEMS')` — fact grain (line items)
  - `source('bronze_erp__dbo', 'SALES_ORDERS')` — header lookup
  - `source('bronze_erp__dbo', 'PRODUCTS')` — lookup for `COST_PRICE`
- **Dialect**: Snowflake

## What the view does

Three CTEs, each deduped with `QUALIFY ROW_NUMBER()`, joined via `LEFT JOIN`:

1. **`cteSALES_ORDER_ITEMS`** — deduplicates per `(SALES_ORDER_ID, SALES_ORDER_ITEM_ID)`,
   keeping the latest `_FIVETRAN_SYNCED`.
2. **`cteSALES_ORDERS`** — deduplicates per `SALES_ORDER_ID`, keeping the
   latest `_FIVETRAN_SYNCED`.
3. **`ctePRODUCTS`** — deduplicates per `PRODUCT_ID`, keeping the latest
   `_FIVETRAN_SYNCED`. Only `COST_PRICE` is used.
4. **Final SELECT** — two `LEFT JOIN`s. Projects:
   - `TransactionNumber` (cast `SALES_ORDER_ID`), `TransactionLineNumber`
   - `Quantity`, `UnitPrice`, `UnitCost` (= `COST_PRICE`)
   - **`GrossAmount = QUANTITY * UNIT_PRICE`** cast to `DECIMAL(19,4)`
   - **`CostAmount = QUANTITY * COST_PRICE`** cast to `DECIMAL(19,4)`
   - `CustomerHashFK` / `EmployeeHashFK` = `NULL` when source IDs are `NULL`
     (via `IFF` guard)
   - Hash keys, `Hashbytes`, `EtlBatchId = 0`, `LoadTimestamp`

## Tests in `vw_FactSales.yml`

Four unit tests, all keyed to `model: vw_FactSales`:

| # | Test | Exercises |
|---|---|---|
| 1 | `test_fact_sales__join_and_calculated_amounts` | Full three-way join; `GrossAmount = 3 × 49.99 = 149.9700`; `CostAmount = 3 × 20.00 = 60.0000` |
| 2 | `test_fact_sales__dedup_latest_sync` | Duplicate line, order, and product rows; latest `_FIVETRAN_SYNCED` wins in all three CTEs before joins and calculations |
| 3 | `test_fact_sales__null_customer_and_employee` | `CUSTOMER_ID = NULL`, `EMPLOYEE_ID = NULL` → both hash FKs are `NULL` via `IFF` guard |
| 4 | `test_fact_sales__missing_product_yields_null_cost` | `PRODUCT_ID` not in PRODUCTS → `UnitCost = NULL`, `CostAmount = NULL` via `LEFT JOIN` |

## Running

```bash
# Full build (parents + model + tests)
dbt build --select vw_FactSales

# Unit tests only
dbt test --select "vw_FactSales,test_type:unit"

# One specific test
dbt test --select test_fact_sales__join_and_calculated_amounts
```

## Known caveats

- **LEFT JOIN on both SALES_ORDERS and PRODUCTS**: orphan line items (no
  matching header or product) are kept but with `NULL` values for the joined
  columns.
- **Hash columns omitted from `expect`**: `SalesHashPK`, `DateHashFK`,
  `ProductHashFK`, `StoreHashFK`, `CustomerHashFK`, `EmployeeHashFK`,
  `Hashbytes`.
- **`CostAmount` is `NULL` when `COST_PRICE` is `NULL`**: Snowflake's
  `QUANTITY * NULL = NULL` propagation means a missing product results in
  `NULL` for both `UnitCost` and `CostAmount`.
