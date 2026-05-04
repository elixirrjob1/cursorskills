# vw_DimSupplier

Supplier dimension view using latest-sync dedup on Fivetran-synced ERP data.

- **dbt model name**: `vw_DimSupplier`
- **Materialization**: `view`
- **Model location**: `models/views/vw_DimSupplier.sql`
- **Source**: `source('bronze_erp__dbo', 'SUPPLIERS')`
- **Dialect**: Snowflake

## What the view does

A single CTE with a `QUALIFY ROW_NUMBER()` dedup pattern:

1. **`cteSUPPLIERS`** — selects from SUPPLIERS and keeps only the row with the
   latest `_FIVETRAN_SYNCED` per `SUPPLIER_ID`.
2. **Final SELECT** — projects dimension columns: `SupplierName`, `ContactName`,
   `ContactEmail`, `ContactPhone`, hash keys (`SupplierHashPK`,
   `SupplierHashBK`), `Hashbytes`, and `LoadTimestamp`. Many columns not
   available in the source (`SupplierDBAName`, `StreetAddress`, `City`,
   `StateProvince`, `PostalCode`, `Country`, payment terms, `LeadTimeDays`,
   `MinimumOrderAmount`, `IsActive`, `IsPreferred`) are hardcoded as `NULL`.

## Tests in `vw_DimSupplier.yml`

Two unit tests, all keyed to `model: vw_DimSupplier`:

| # | Test | Exercises |
|---|---|---|
| 1 | `test_dim_supplier__latest_sync_wins` | Two rows with same `SUPPLIER_ID`; only the latest `_FIVETRAN_SYNCED` survives; pass-through columns verified |
| 2 | `test_dim_supplier__null_columns_are_hardcoded` | All columns not available in source (`StreetAddress`, `City`, `PaymentTerms*`, etc.) are consistently `NULL` |

## Running

```bash
# Full build (parents + model + tests)
dbt build --select vw_DimSupplier

# Unit tests only
dbt test --select "vw_DimSupplier,test_type:unit"

# One specific test
dbt test --select test_dim_supplier__latest_sync_wins
```

## Known caveats

- **Hash columns omitted from `expect`**: `SupplierHashPK`, `SupplierHashBK`,
  `Hashbytes` are `HASH`/`SHA2_BINARY` outputs.
- **`SupplierHashPK` = `SupplierHashBK`**: both use `HASH(SUPPLIER_ID)` — this
  is by design in the current SQL (no separate business key column exists).
