# vw_FactInventorySnapshot

Inventory snapshot fact view joining INVENTORY and PRODUCTS with latest-sync dedup.

- **dbt model name**: `vw_FactInventorySnapshot`
- **Materialization**: `view`
- **Model location**: `models/views/vw_FactInventorySnapshot.sql`
- **Sources**:
  - `source('bronze_erp__dbo', 'INVENTORY')` — fact grain
  - `source('bronze_erp__dbo', 'PRODUCTS')` — lookup for `COST_PRICE`
- **Dialect**: Snowflake

## What the view does

Two CTEs, each deduped with `QUALIFY ROW_NUMBER()`, joined via `LEFT JOIN`:

1. **`cteINVENTORY`** — deduplicates INVENTORY rows per `INVENTORY_ID`,
   keeping the latest `_FIVETRAN_SYNCED`.
2. **`ctePRODUCTS`** — deduplicates PRODUCTS rows per `PRODUCT_ID`,
   keeping the latest `_FIVETRAN_SYNCED`. Only `COST_PRICE` is used.
3. **Final SELECT** — `LEFT JOIN` on `PRODUCT_ID`. Projects:
   - `QuantityOnHand`, `ReorderPoint` (= `REORDER_LEVEL`),
     `InventoryValue` (= `STOCK_VALUE`) from INVENTORY
   - `UnitCost` (= `COST_PRICE`) from PRODUCTS — `NULL` when no match
   - Hash keys, `Hashbytes`, `EtlBatchId = 0`, `LoadTimestamp`
   - Many columns hardcoded as `NULL` (`QuantityReserved`, `QuantityAvailable`,
     `QuantityOnOrder`, `QuantityInTransit`, `SafetyStockLevel`, `DaysOfSupply`)

## Tests in `vw_FactInventorySnapshot.yml`

Three unit tests, all keyed to `model: vw_FactInventorySnapshot`:

| # | Test | Exercises |
|---|---|---|
| 1 | `test_fact_inventory__join_and_pass_through` | Duplicate PRODUCTS rows prove latest product cost wins; matching product → `UnitCost` populated; pass-through of `QuantityOnHand`, `ReorderPoint`, `InventoryValue`; NULL stubs verified |
| 2 | `test_fact_inventory__missing_product_yields_null_cost` | `PRODUCT_ID` not in PRODUCTS → `UnitCost = NULL` via `LEFT JOIN` |
| 3 | `test_fact_inventory__dedup_latest_sync` | Two INVENTORY rows with same `INVENTORY_ID` and duplicate PRODUCTS rows; latest `_FIVETRAN_SYNCED` survives on both sides |

## Running

```bash
# Full build (parents + model + tests)
dbt build --select vw_FactInventorySnapshot

# Unit tests only
dbt test --select "vw_FactInventorySnapshot,test_type:unit"

# One specific test
dbt test --select test_fact_inventory__join_and_pass_through
```

## Known caveats

- **Hash columns omitted from `expect`**: `InventorySnapshotHashPK`,
  `DateHashFK`, `ProductHashFK`, `WarehouseHashFK`, `Hashbytes`.
- **LEFT JOIN semantics**: if a `PRODUCT_ID` has no match in PRODUCTS,
  `UnitCost` is `NULL` but `InventoryValue` (`STOCK_VALUE`) is still populated
  from the INVENTORY source directly.
