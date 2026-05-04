# vw_FactPurchaseOrder

Purchase order fact view joining PURCHASE_ORDER_ITEMS and PURCHASE_ORDERS with latest-sync dedup.

- **dbt model name**: `vw_FactPurchaseOrder`
- **Materialization**: `view`
- **Model location**: `models/views/vw_FactPurchaseOrder.sql`
- **Sources**:
  - `source('bronze_erp__dbo', 'PURCHASE_ORDER_ITEMS')` — fact grain (line items)
  - `source('bronze_erp__dbo', 'PURCHASE_ORDERS')` — header lookup
- **Dialect**: Snowflake

## What the view does

Two CTEs, each deduped with `QUALIFY ROW_NUMBER()`, joined via `INNER JOIN`:

1. **`ctePURCHASE_ORDER_ITEMS`** — deduplicates per `PO_ITEM_ID`, keeping
   the latest `_FIVETRAN_SYNCED`.
2. **`ctePURCHASE_ORDERS`** — deduplicates per `PO_ID`, keeping the latest
   `_FIVETRAN_SYNCED`.
3. **Final SELECT** — `INNER JOIN` on `PO_ID`. Projects:
   - `PurchaseOrderNumber` (cast `PO_ID`), `PurchaseOrderLineNumber` (`PO_ITEM_ID`)
   - `OrderStatus` (= `STATUS`), `QuantityOrdered` (= `QUANTITY`), `UnitCost`
   - **`OrderAmount = QUANTITY * UNIT_COST`** cast to `DECIMAL(19,4)`
   - `DateExpectedHashFK` = `NULL` when `EXPECTED_DATE IS NULL` (via `IFF` guard)
   - Hash keys, `Hashbytes`, `EtlBatchId = 0`, `LoadTimestamp`
   - Many columns hardcoded as `NULL` (`QuantityShipped`, `QuantityReceived`,
     `ShippedAmount`, `DaysToShip`, etc.)

## Tests in `vw_FactPurchaseOrder.yml`

Three unit tests, all keyed to `model: vw_FactPurchaseOrder`:

| # | Test | Exercises |
|---|---|---|
| 1 | `test_fact_po__join_and_order_amount` | Duplicate PO headers prove latest header wins; matching PO header → `OrderAmount = 10 × 25.50 = 255.0000`; pass-through columns; NULL stubs |
| 2 | `test_fact_po__null_expected_date_yields_null_hash` | `EXPECTED_DATE = NULL` → `DateExpectedHashFK = NULL` via `IFF` guard |
| 3 | `test_fact_po__dedup_latest_sync` | Duplicate PO_ITEM and PO header rows; latest `_FIVETRAN_SYNCED` survives; `QuantityOrdered` updates |

## Running

```bash
# Full build (parents + model + tests)
dbt build --select vw_FactPurchaseOrder

# Unit tests only
dbt test --select "vw_FactPurchaseOrder,test_type:unit"

# One specific test
dbt test --select test_fact_po__join_and_order_amount
```

## Known caveats

- **INNER JOIN**: unlike the fact sales/inventory views which use `LEFT JOIN`,
  this view uses `INNER JOIN` — PO items without a matching PO header are
  dropped entirely (no orphan handling).
- **Hash columns omitted from `expect`**: `PurchaseOrderHashPK`,
  `ProductHashFK`, `SupplierHashFK`, `WarehouseHashFK`, `DateOrderedHashFK`,
  `Hashbytes`.
