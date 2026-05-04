# vw_DimCustomer

SCD2-style customer dimension view built on top of Fivetran-synced ERP data.

- **dbt model name**: `vw_DimCustomer`
- **Materialization**: `view`
- **Model location**: `models/views/vw_DimCustomer.sql`
- **Source**: `source('bronze_erp__dbo', 'CUSTOMERS')`
  - OpenMetadata FQN: `snowflake_fivetran.DRIP_DATA_INTELLIGENCE.BRONZE_ERP__DBO.CUSTOMERS`
- **Dialect**: Snowflake

## What the view does

The view flattens Fivetran history-mode rows into clean SCD2 records:

1. **`cte_prep`** — reads the source, renames Fivetran metadata columns, and
   tags each row with `SourceSystemCode = 'ERP'`.
2. **`cte_prep_hash`** — computes a `SHA2_BINARY` hash over the hashable
   attributes (`CREATED_AT`, `EMAIL`, `FIRST_NAME`, `LAST_NAME`, `PHONE`) with
   a NULL sentinel `'#@#@#@#@#'` so NULLs don't collide.
3. **`cte_prep_hash_lag`** — adds `LAG(Hashbytes)` per `CUSTOMER_ID` *and*
   computes `LEAD(EffectiveStartDateTime) - 1 microsecond` (per Customer)
   *before* row reduction. The LEAD ordering means a surviving v1 row's end
   timestamp is computed against the next *raw* row — including duplicates
   that the next CTE will drop.
4. **`cte_row_reduce`** — drops rows where the hash is unchanged from the
   previous row (dedup no-op updates). LEAD has already been computed at this
   point, so it survives the filter.
5. **`fin`** — projects the final dimension columns including `CurrentFlagYN`
   (`'Y'` whenever the row has no lead, regardless of `_FIVETRAN_ACTIVE`) and
   `DeletedFlagYN` (`'Y'` when the row has no lead AND is Fivetran-inactive).

> **Source-of-truth note.** The model SQL lives in `models/views/vw_DimCustomer.sql`.
> Two behaviors are known divergences from the original spec:
>
> - LEAD is computed before dedup (step 3 above), so the surviving v1 row
>   end-dates against the dropped duplicate's start, not the next surviving
>   version. The unit tests assert this behavior directly.
> - `CurrentFlagYN` no longer factors `_FIVETRAN_ACTIVE`; the
>   `_FIVETRAN_ACTIVE` signal is moved into the new `DeletedFlagYN` column.

## Tests in `vw_DimCustomer.yml`

Six unit tests, all keyed to `model: vw_DimCustomer`. Hand-crafted assertions
match the behavior of the current `vw_DimCustomer.sql`:

- LEAD over `EffectiveStartDateTimeUTC` runs in `cte_prep_hash_lag` *before*
  the row-reduce step, so end-dating on a surviving row references the
  dropped duplicate's start instead of the next surviving row.
- `CurrentFlagYN = 'Y'` whenever `LeadEffectiveStartDateTimeUTC IS NULL`,
  regardless of `_FIVETRAN_ACTIVE` (the active flag now drives `DeletedFlagYN`).
- `IsActive` is hard-coded `TRUE` in the projection.
- All `_FIVETRAN_*` inputs are passed with explicit `+00:00` so timestamp
  comparisons are session-timezone-independent.

### Hand-crafted (full assertions)

| # | Test | Exercises |
|---|---|---|
| 1 | `test_dim_customer__scd2_dedup_and_current_flag` | `LAG(Hashbytes)` dedup drops no-op middle row; LEAD is computed *before* dedup, so the surviving v1 row's `EffectiveEndDateTime` aligns with the dropped row's start - 1µs; `CurrentFlagYN = 'Y'` only on the row whose LEAD is NULL |
| 2 | `test_dim_customer__deleted_flag_when_inactive` | Single inactive Fivetran row → `CurrentFlagYN = 'Y'` (no lead) and `DeletedFlagYN = 'Y'` (no lead AND `_FIVETRAN_ACTIVE = false`); `EffectiveEndDateTime` falls back to `_FIVETRAN_END` |
| 3 | `test_dim_customer__fullname_handles_nulls` | `TRIM(COALESCE(...))` behavior for NULL first/last/both names, including preserved concatenation spaces |

### Generated (schema-coverage, with TODO expected values)

| # | Test | Fixture style |
|---|---|---|
| 4 | `test_dim_customer__basic` | Type/name-aware realistic values from OM metadata |
| 5 | `test_dim_customer__with_nulls` | Same as basic, one row with all nullable columns NULL |
| 6 | `test_dim_customer__boundary` | Length-limit strings (`VARCHAR(256)`/`VARCHAR(900)` pushed to the edge), min/max numerics |

Tests 4–6 are generated scaffolds and are disabled with
`config.enabled: false`. Their derived `expect` columns (hashes, SCD2
timestamps, `CurrentFlagYN`, `FullName`, casts) are placeholders marked
`# TODO: fill expected value (derived expression)`. Enable one generated test
at a time, use the `actual differs from expected` diff output from dbt to fill
the TODO values, then re-enable it permanently.

## Running

```bash
# Full build (parents + model + tests)
dbt build --select vw_DimCustomer

# Unit tests only
dbt test --select "vw_DimCustomer,test_type:unit"

# One specific test
dbt test --select test_dim_customer__deleted_flag_when_inactive
```

## Known caveats

- **Hash columns omitted from `expect`**: `Hashbytes`, `CustomerHashPK`,
  `CustomerHashBK` are `SHA2_BINARY`/`HASH` outputs. Mocking exact binary
  values is painful and brittle. If you need to assert them, compute the
  expected value once in Snowflake and paste it in.
- **`TIMESTAMP_TZ` literals**: formatted as `'YYYY-MM-DD HH:MM:SS +00:00'`.
  If your session timezone causes drift in the 1-microsecond `LEAD` math,
  switch that input to `format: sql` and use a typed literal.
- **Session-dependent behavior**: `DATEADD(MICROSECOND, -1, ...)` crosses into
  sub-second territory — make sure the test warehouse has no timezone offset
  applied silently.

## Lineage artifacts used to build the generated tests

The intermediate `lineage.json` / `enriched.json` / `ref_map.json` are not
committed here. If you need to regenerate, use the
`view-to-dbt-unit-test` skill with:

```
OM host:           http://52.255.209.74:8585
Service prefix:    snowflake_fivetran
Resolved FQN:      snowflake_fivetran.DRIP_DATA_INTELLIGENCE.BRONZE_ERP__DBO.CUSTOMERS
Model name:        vw_DimCustomer
Test name base:    test_dim_customer
Scenarios:         basic,with_nulls,boundary
Rows per input:    2
```
