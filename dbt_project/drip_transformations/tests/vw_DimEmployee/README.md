# vw_DimEmployee

Employee dimension view built on top of Fivetran-synced ERP data.
Simpler than `vw_DimCustomer` — no SCD2, just latest-sync dedup + projection.

- **dbt model name**: `vw_DimEmployee`
- **Materialization**: `view`
- **Model location**: `models/views/vw_DimEmployee.sql`
- **Source**: `source('bronze_erp__dbo', 'EMPLOYEES')`
  - OpenMetadata FQN: `snowflake_fivetran.DRIP_DATA_INTELLIGENCE.BRONZE_ERP__DBO.EMPLOYEES`
- **Dialect**: Snowflake

## What the view does

1. **`cteEMPLOYEES`** — reads the source and keeps only the latest row per
   `EMPLOYEE_ID` via
   `QUALIFY ROW_NUMBER() OVER (PARTITION BY EMPLOYEE_ID ORDER BY _FIVETRAN_SYNCED DESC) = 1`.
2. **Final `SELECT`** — projects the dim columns:
   - `EmployeeHashPK` / `EmployeeHashBK` — `SHA2(COALESCE(CAST(EMPLOYEE_ID AS VARCHAR),'#@#@#@#@#'),256)` cast to `BINARY(32)` (both identical).
   - `HomeStoreHashFK` — `NULL` when `STORE_ID IS NULL`, else `SHA2(..)`.
   - `FullName` — `TRIM(FIRST_NAME) || ' ' || TRIM(LAST_NAME)` (no `COALESCE`,
     so any NULL name part propagates and yields `FullName = NULL`).
   - `Hashbytes` — `SHA2(CONCAT_WS('|', FIRST_NAME, LAST_NAME, EMAIL, "ROLE", HIRE_DATE, STORE_ID), 256)`.
   - Several columns (`Department`, `TerminationDate`, `ManagerEmployeeHashFK`, `IsActive`, `EtlBatchId`) are hard-coded NULL / `0` — "not available in source".

> **Source-of-truth note.** The model SQL lives in `models/views/vw_DimEmployee.sql`.
> The `FullName` expression intentionally lacks `COALESCE` (unlike
> `vw_DimCustomer`); any NULL on either side propagates to a NULL `FullName`.

## Tests in `vw_DimEmployee.yml`

Five unit tests, all keyed to `model: vw_DimEmployee`. The first two are
hand-crafted logic tests with real assertions. The last three were produced by
the `view-to-dbt-unit-test` skill with OM-backed, type-/name-aware fixtures and
are disabled until TODO expected values are filled.

| # | Test | Fixture style |
|---|---|---|
| 1 | `test_dim_employee__latest_sync_wins` | Hand-crafted: two rows share `EMPLOYEE_ID`; latest `_FIVETRAN_SYNCED` survives |
| 2 | `test_dim_employee__fullname_handles_nulls` | Hand-crafted: `vw_DimEmployee` uses `TRIM(FIRST_NAME) \|\| ' ' \|\| TRIM(LAST_NAME)` *without* `COALESCE`, so any NULL name yields `FullName = NULL` (different from the customer dimension) |
| 3 | `test_dim_employee__basic` | Generated scaffold, disabled: 2 realistic employees (`EMPLOYEE_ID`/`STORE_ID` as NUMBER(38,0), `EMAIL` as text(900), `HIRE_DATE` as DATE, `_FIVETRAN_SYNCED` as TIMESTAMP_TZ) |
| 4 | `test_dim_employee__with_nulls` | Generated scaffold, disabled: same as basic, one row with every nullable column set to NULL |
| 5 | `test_dim_employee__boundary` | Generated scaffold, disabled: length-limit strings and boundary numerics |

### Auto-filled pass-throughs (ready as-is)

`FirstName`, `LastName`, `EmailAddress`, `JobTitle`, `HireDate`, `LoadTimestamp`

### Derived columns you need to fill in

| Column | How to derive |
|---|---|
| `EmployeeHashPK` / `EmployeeHashBK` | Compute `SHA2(COALESCE(CAST(EMPLOYEE_ID AS VARCHAR),'#@#@#@#@#'),256)` in Snowflake and paste the `BINARY(32)` value, **or** drop these keys from `expect` — the `dict` format skips unlisted columns |
| `FullName` | `TRIM(FIRST_NAME) \|\| ' ' \|\| TRIM(LAST_NAME)` (no `COALESCE`) — e.g. basic row 1: `'Grace Frank'`; if either name is NULL, `FullName = NULL` |
| `Department`, `TerminationDate`, `ManagerEmployeeHashFK`, `IsActive` | All `CAST(NULL AS …)` literals → expected value is `null` |
| `EtlBatchId` | `CAST(0 AS INT)` → expected value is `0` |
| `HomeStoreHashFK` | `null` when `STORE_ID IS NULL`; otherwise an opaque `SHA2(..)` value — same guidance as `EmployeeHashPK` |
| `Hashbytes` | Recommend omitting from `expect` — precomputing `SHA2_BINARY` hashes for every scenario is brittle |

### Generated scaffold status

Generated tests 3–5 are disabled with `config.enabled: false`. Enable one at a
time, use dbt's `actual differs from expected` output to fill TODO values, then
keep it enabled permanently.

## Running

```bash
# Full build (parents + model + tests)
dbt build --select vw_DimEmployee

# Unit tests only
dbt test --select "vw_DimEmployee,test_type:unit"

# One specific test
dbt test --select test_dim_employee__latest_sync_wins
```

## Known caveats

- **Hash columns omitted from `expect`**: `EmployeeHashPK`, `EmployeeHashBK`,
  `HomeStoreHashFK`, `Hashbytes` are `SHA2`/`SHA2_BINARY` outputs. Mocking
  exact binary values is painful and brittle.
- **Quoted identifier `"ROLE"`**: the source column is the reserved word
  `ROLE`, so the view double-quotes it. The generated fixtures use key
  `ROLE` in YAML — dbt handles the quoting when materializing the mock.
- **`TIMESTAMP_TZ` literals**: only `_FIVETRAN_SYNCED` in this view. Format
  is `'YYYY-MM-DD HH:MM:SS'`. If casting fails, switch the input to
  `format: sql` and supply a typed literal.

## Lineage artifacts used to build the generated tests

The intermediate `lineage.json` / `enriched.json` / `ref_map.json` are not
committed here. To regenerate, use the `view-to-dbt-unit-test` skill with:

```
OM host:           http://52.255.209.74:8585
Auth:              email=admin@open-metadata.org, password=admin (basic-auth
                   login → JWT exchange, supported since skill commit 6036beb)
Service prefix:    snowflake_fivetran
Resolved FQN:      snowflake_fivetran.DRIP_DATA_INTELLIGENCE.BRONZE_ERP__DBO.EMPLOYEES
Model name:        vw_DimEmployee
Test name base:    test_dim_employee
Scenarios:         basic,with_nulls,boundary
Rows per input:    2
```

Current skill versions parse dbt Jinja directly and filter CTE aliases from
source lineage, so `cteEMPLOYEES` should no longer appear as a `given:` input.
