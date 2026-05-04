# vw_DimProduct

SCD2-style product dimension view built on top of Fivetran-synced ERP data.

- **dbt model name**: `vw_DimProduct`
- **Materialization**: `view`
- **Model location**: `models/views/vw_DimProduct.sql`
- **Source**: `source('bronze_erp__dbo', 'PRODUCTS')`
- **Dialect**: Snowflake

## What the view does

The view flattens Fivetran history-mode rows into clean SCD2 records:

1. **`cte_prep`** — reads the source, renames Fivetran metadata columns, and
   tags each row with `SourceSystemCode = 'ERP'`.
2. **`cte_prep_hash`** — computes a `SHA2_BINARY` hash over the hashable
   attributes (`ACTIVE`, `CATEGORY`, `COST_PRICE`, `NAME`,
   `PRODUCT_DESCRIPTION`, `UNIT_PRICE`) with a NULL sentinel `'#@#@#@#@#'`.
3. **`cte_prep_hash_lag`** — adds `LAG(Hashbytes)` per `PRODUCT_ID` *and*
   computes `LEAD(EffectiveStartDateTimeUTC) - 1 microsecond` *before* row
   reduction. The LEAD ordering means a surviving v1 row's end timestamp is
   computed against the next *raw* row — including duplicates the next CTE will
   drop.
4. **`cte_row_reduce`** — drops rows where the hash is unchanged from the
   previous row (dedup no-op updates).
5. **`fin`** — projects the final dimension columns including:
   - `CurrentFlagYN` (`'Y'` whenever the row has no lead)
   - `SoftDeletedFlagYN` (`'Y'` when no lead AND `NOT ACTIVE` AND `IsFivetranActive`)
   - `DeletedFlagYN` (`'Y'` when no lead AND `NOT IsFivetranActive`)
   - `IsDiscontinued` = `IFF(ACTIVE, FALSE, TRUE)`
   - `UnitCost` / `UnitListPrice` cast to `NUMBER(19,4)`

## Tests in `vw_DimProduct.yml`

Three unit tests, all keyed to `model: vw_DimProduct`:

| # | Test | Exercises |
|---|---|---|
| 1 | `test_dim_product__scd2_dedup_and_current_flag` | `LAG(Hashbytes)` dedup drops no-op middle row; LEAD computed *before* dedup; `CurrentFlagYN = 'Y'` only on last row; `UnitCost`/`UnitListPrice` precision |
| 2 | `test_dim_product__soft_deleted_vs_hard_deleted` | Single-row products: `ACTIVE=false, _FIVETRAN_ACTIVE=true` → `SoftDeletedFlagYN='Y'`; `ACTIVE=true, _FIVETRAN_ACTIVE=false` → `DeletedFlagYN='Y'` |
| 3 | `test_dim_product__is_discontinued_reflects_active_flag` | `IFF(ACTIVE, FALSE, TRUE)` correctly maps `ACTIVE=true` → `IsDiscontinued=false` and vice versa |

## Running

```bash
# Full build (parents + model + tests)
dbt build --select vw_DimProduct

# Unit tests only
dbt test --select "vw_DimProduct,test_type:unit"

# One specific test
dbt test --select test_dim_product__scd2_dedup_and_current_flag
```

## Known caveats

- **Hash columns omitted from `expect`**: `Hashbytes`, `ProductHashPK`,
  `ProductHashBK` are `SHA2_BINARY`/`HASH` outputs. Mocking exact binary
  values is painful and brittle.
- **`TIMESTAMP_TZ` literals**: formatted as `'YYYY-MM-DD HH:MM:SS +00:00'`.
- **Three-way delete flag matrix**: `SoftDeletedFlagYN` and `DeletedFlagYN`
  depend on both the source `ACTIVE` column and the Fivetran `_FIVETRAN_ACTIVE`
  flag — see test #2 for the full truth table.
