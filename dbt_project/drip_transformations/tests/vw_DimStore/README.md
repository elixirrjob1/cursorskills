# vw_DimStore

Store dimension view using latest-sync dedup on Fivetran-synced ERP data.

- **dbt model name**: `vw_DimStore`
- **Materialization**: `view`
- **Model location**: `models/views/vw_DimStore.sql`
- **Source**: `source('bronze_erp__dbo', 'STORES')`
- **Dialect**: Snowflake

## What the view does

A single CTE with a `QUALIFY ROW_NUMBER()` dedup pattern:

1. **`cteSTORES`** — selects from STORES and keeps only the row with the latest
   `_FIVETRAN_SYNCED` per `STORE_ID`.
2. **Final SELECT** — projects dimension columns: `StoreName`, `StreetAddress`,
   `City`, `StateProvince`, `PostalCode`, hash keys (`StoreHashPK`,
   `StoreHashBK`), `Hashbytes`, and `LoadTimestamp`. Many columns not available
   in the source (`StoreType`, `Country`, `Latitude`, `Longitude`, district/region
   codes, `StoreManager`, `OpenDate`, `CloseDate`, `SquareFootage`, `IsActive`)
   are hardcoded as `NULL`.

## Tests in `vw_DimStore.yml`

Two unit tests, all keyed to `model: vw_DimStore`:

| # | Test | Exercises |
|---|---|---|
| 1 | `test_dim_store__latest_sync_wins` | Two rows with same `STORE_ID`; only the latest `_FIVETRAN_SYNCED` survives; pass-through columns verified |
| 2 | `test_dim_store__null_columns_are_hardcoded` | All columns not available in source (`StoreType`, `Country`, `Latitude`, etc.) are consistently `NULL` |

## Running

```bash
# Full build (parents + model + tests)
dbt build --select vw_DimStore

# Unit tests only
dbt test --select "vw_DimStore,test_type:unit"

# One specific test
dbt test --select test_dim_store__latest_sync_wins
```

## Known caveats

- **Hash columns omitted from `expect`**: `StoreHashPK`, `StoreHashBK`,
  `Hashbytes` are `HASH`/`SHA2_BINARY` outputs.
- **No SCD2 history**: unlike `vw_DimCustomer` or `vw_DimProduct`, this view
  uses simple latest-sync dedup (no history tracking, no `CurrentFlagYN`).
