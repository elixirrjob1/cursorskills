---
name: dbt-model-from-stm
description: Generate dbt SQL models and schema YAML from source-to-target mapping (STM) markdown files. Use when the user wants to convert STMs into dbt models, create dbt transformations from mappings, or generate enriched models for a dbt project targeting Snowflake.
---

# dbt Model From STM

## When to use

- User has STM markdown files (from `stm/output/`) and wants dbt model code
- User asks to generate dbt models, SQL transformations, or enriched layers from mappings

## Inputs

- **STM files:** one or more STM markdown files from `stm/output/`
- **dbt project:** local dbt project at `dbt_project/drip_transformations/`

If the user specifies STM files, use those. Otherwise process all `*-stm.md` files in `stm/output/`.

## Architecture — two models per target table

Every target table (dimension or fact) produces **two** dbt models:

| Model file | Folder | Materialization | Purpose |
|---|---|---|---|
| `vw_{Entity}.sql` | `models/views/` | `view` | Holds all transformation logic; references `{{ source() }}` directly |
| `{Entity}.sql` | `models/enriched/` | `incremental` (Type 1) or `table` (Type 2) | `SELECT * FROM {{ ref('vw_{Entity}') }}` |

**Type 1 vs Type 2 materialization:**
- Type 1 (SCD Type 1 or non-SCD): enriched model is `incremental` with `unique_key=<HashPK>` and a `WHERE LoadTimestamp > MAX(LoadTimestamp)` filter.
- Type 2: enriched model is `table` (full refresh). Incremental materialization is **unsafe** for Type 2 because a new version of an existing row rewrites the end-date (`EffectiveEndDateTime`, `CurrentFlagYN`) of the previous version, which `unique_key` would silently overwrite. Full refresh from the view is the only correct pattern when the view computes `LEAD(EffectiveStartDateTime)` over the partition.

**View naming convention:** `vw_<Entity>` where `<Entity>` is the STM's `Target Table` **verbatim PascalCase**. Example: `vw_DimEmployee`, `vw_FactSales`.

**Incremental model naming:** `<Entity>` verbatim PascalCase (e.g. `DimEmployee.sql`, `FactSales.sql`). NOT snake_case.

**Why two models?** The view is "what we want to load" and the incremental table is "what got loaded." This enables model-to-model data testing and comparison downstream.

**No staging layer.** Enriched view models reference source tables directly via `{{ source() }}`. There is no intermediate `stg_*` hop.

## Three-phase workflow

### Phase 1 — Scaffolding (Python script)

Generates the mechanical parts that don't need reasoning:

```bash
python3 .cursor/skills/dbt-model-from-stm/scripts/generate_dbt_models.py \
  --stm-dir stm/output \
  --dbt-project dbt_project/drip_transformations
```

This produces:
- `models/staging/_sources.yml` — source definitions for `{{ source() }}` references
- `models/views/_schema.yml` — model entries for the `vw_` view models, with column descriptions and tests
- `models/enriched/_schema.yml` — model entries for the base incremental models, with column descriptions and tests

It does NOT generate enriched model SQL — that is Phase 2.

### Phase 2 — Enriched models (parallel subagents)

Launch **one subagent per STM** to generate **two** enriched model SQL files. Each subagent reads the STM, picks up the Snowflake SQL expressions from the Transformation column, and writes the view + incremental pair.

Launch all subagents in parallel using the Task tool with `subagent_type="generalPurpose"`. Each subagent gets this prompt (fill in the specifics per STM):

```
You are generating dbt models for Snowflake from a source-to-target mapping.

Read the STM file: {stm_filepath}
Read the sources file: {dbt_project}/models/staging/_sources.yml
Read the skill file: .cursor/skills/dbt-model-from-stm/SKILL.md

### Step 0 — Detect SCD type

Before writing SQL, determine whether this is an SCD Type 2 STM. It IS Type 2 if ANY of:
- Section 4 (Target Schema Definition) lists `SCD Type = Type 2`.
- Section 7 (Field-Level Mapping Matrix) uses the Ajay format: one or more `### Data Condition N` blocks followed by a `### Final` block.
- Section 7 target columns include all of `EffectiveStartDateTime`, `EffectiveEndDateTime`, `CurrentFlagYN`.
- Section 8 lists rule `BR12 — Type 2 Metadata`.

If Type 2 → follow the **SCD Type 2 Handling (Ajay Kalyan multi-CTE pattern)** section of SKILL.md. Use the 5-CTE pipeline (`cte_prep` → `cte_prep_hash` → `cte_prep_hash_lag` → `cte_row_reduce` → `fin`), `HASH()` for keys (→ NUMBER(38,0)), `SHA2_BINARY()` for Hashbytes (→ BINARY), Fivetran history-mode columns for Type 2 windowing, and `materialized='table'` for the enriched model.

Otherwise (Type 1 / non-SCD) → follow the Type 1 pattern described below in this prompt (single `cte<TABLE>` with QUALIFY, SHA2 VARCHAR(64) keys, `materialized='incremental'` on `LoadTimestamp`).

You must produce TWO files:

### File 1 — View model
Write to: {dbt_project}/models/views/{view_model_name}.sql

The view_model_name follows the pattern: vw_{Entity}
where {Entity} = the STM's `Target Table` verbatim (PascalCase).
Examples: vw_DimEmployee, vw_FactSales, vw_DimProduct

This model holds all transformation logic.

Rules for the view (Type 1 — use the Type 2 section of SKILL.md if Step 0 detected Type 2):
- Start with: {{ config(materialized='view') }}
- Use WITH ... AS CTEs referencing source tables via {{ source('<source_name>', '<TABLE>') }}.
  Look up the correct source name from _sources.yml.

**CTE naming convention:**
- Name each source CTE as `cte<TABLE_NAME>` (e.g. `cteEMPLOYEES`, `cteSALES_ORDER_ITEMS`).
- Reference columns from the CTE WITHOUT a table alias prefix (just `EMPLOYEE_ID`, not `e.EMPLOYEE_ID`).
- In the FROM clause, use the CTE name directly without aliasing (e.g. `FROM cteEMPLOYEES`, not `FROM cteEMPLOYEES e`).
- For fact tables with multiple CTEs that need JOINs, use the CTE names directly in JOIN clauses (e.g. `FROM cteSALES_ORDER_ITEMS JOIN cteSALES_ORDERS ON ...`). If column names conflict between CTEs, qualify with the CTE name.

**Source CTE — latest record deduplication (Snowflake QUALIFY):**
- Use Snowflake's QUALIFY clause to filter on ROW_NUMBER() in a single step — do NOT create a separate CTE just for deduplication.
- The QUALIFY clause goes AFTER the FROM/JOIN/WHERE and operates on window functions directly.
- Pattern:
    WITH cte<TABLE> AS (
        SELECT
            <columns>
        FROM {{ source('...', '...') }}
        QUALIFY ROW_NUMBER() OVER (
            PARTITION BY <business_key_col>
            ORDER BY <modified_timestamp> DESC
        ) = 1
    )
- Use the best available ordering column from the source (_FIVETRAN_SYNCED, UPDATED_AT, LAST_UPDATE_DATE, etc.).
- Do NOT add a separate LatestRank column + WHERE LatestRank = 1 filter. QUALIFY replaces that pattern entirely.
- If no ordering column exists, omit QUALIFY and add a `-- NOTE: no deduplication column available` comment.

**Hash key generation (SHA2 + COALESCE sentinel, VARCHAR(64) output):**
- All HashPK, HashBK, HashFK columns use plain `SHA2` — returns 64-char hex VARCHAR:
    SHA2(COALESCE(CAST(col AS VARCHAR), '#@#@#@#@#'), 256)
- For composite keys, concatenate with '|' delimiters:
    SHA2(COALESCE(CAST(col1 AS VARCHAR), '#@#@#@#@#') || '|' || COALESCE(CAST(col2 AS VARCHAR), '#@#@#@#@#'), 256)
- Nullable FKs: wrap in IFF(col IS NULL, NULL, SHA2(...))
- Do NOT use MD5. Always use SHA2 with digest size 256.
- Do NOT wrap in `CAST(... AS BINARY(32))` or `SHA2_BINARY`. The team convention is human-readable hex stored as `VARCHAR(64)`. Casting to `BINARY` depends on the session `BINARY_INPUT_FORMAT` and can silently truncate.

**Hashbytes change detection column:**
- Add a `hashbytes` column as the last column before any audit/metadata fields.
- It must be SHA2(256) of sourced attribute columns concatenated with '|' and COALESCE sentinel.
- Include ONLY columns that have a real source mapping (Source Table + Source Column in the STM).
- EXCLUDE unmapped columns (hardcoded defaults, NULLs with no source) — they never change so add no detection value.
- EXCLUDE HashPK, HashBK, HashFK, and audit/metadata fields.

**Column mapping rules:**
- The "Transformation / Business Rule" column contains Snowflake SQL expressions.
  Use them directly — columns are referenced WITHOUT a table/CTE prefix since there's only one CTE per dimension.
  For example, if Transformation = "CAST(QUANTITY * UNIT_PRICE AS DECIMAL(19,4))",
  write: CAST(QUANTITY * UNIT_PRICE AS DECIMAL(19,4)).
  HOWEVER: if the STM transformation uses MD5(), replace it with the SHA2 pattern above.
- **Output aliases must match the STM's `Target Column` EXACTLY — PascalCase, verbatim.**
  Examples: `AS EmployeeHashPK`, `AS FirstName`, `AS HomeStoreHashFK`, `AS LoadTimestamp`, `AS Hashbytes`.
  DO NOT snake_case the alias (no `employee_hash_pk`, no `first_name`). Copy the STM column name as-is.
- For fact tables with multiple source CTEs joined together, qualify column names with the CTE name
  ONLY when the same column name exists in multiple CTEs. Otherwise keep columns unqualified.
- If Transformation is empty and Source Table/Column are filled, it's a direct passthrough —
  just reference the source column directly (e.g. `FIRST_NAME AS FirstName`).
- Do NOT truncate fields with LEFT() or force data type lengths. Let columns pass through
  at source precision. TRIM() is acceptable but do not wrap in LEFT(..., N).
- For columns with no source mapping (empty Source Table + Source Column),
  use the default value from the STM if one exists (e.g. 'Unknown', 0, TRUE),
  or CAST(NULL AS <DataType>) if no default. Either way, add an inline comment:
  `-- not available in source`
  Example: `'Unknown' AS CustomerType, -- not available in source`
  Example: `CAST(NULL AS DATE) AS TerminationDate, -- not available in source`
- Place {{ config(...) }} BEFORE the WITH keyword.

**Audit/metadata columns:**
- `LoadTimestamp` (STM name) must be sourced from `_FIVETRAN_SYNCED` (the Fivetran sync timestamp on every source table), NOT from `CURRENT_TIMESTAMP()`.
  Using `CURRENT_TIMESTAMP()` would update every row on every run and break incremental logic (no rows would ever be filtered out).
  Write: `_FIVETRAN_SYNCED AS LoadTimestamp`
- Include `_FIVETRAN_SYNCED` in the source CTE's SELECT list so it can be projected through to LoadTimestamp.
- `EtlBatchId` remains `CAST(0 AS INT) AS EtlBatchId -- not available in source` or similar until a batch ID is wired in.

### File 2 — Enriched model
Write to: {dbt_project}/models/enriched/{Entity}.sql

The filename `{Entity}` is the STM's `Target Table` verbatim PascalCase (e.g. `DimEmployee.sql`, `FactSales.sql`). NOT snake_case.

**If Step 0 detected Type 1 / non-SCD — write an incremental model:**

{{ config(
    materialized='incremental',
    unique_key='<PK_COLUMN_VERBATIM_FROM_STM>'
) }}

SELECT * FROM {{ ref('vw_<Entity>') }}

{{% if is_incremental() %}}
WHERE LoadTimestamp > (SELECT MAX(LoadTimestamp) FROM {{ this }})
{{% endif %}}

**If Step 0 detected Type 2 — write a full-refresh table model (no incremental, no unique_key):**

{{ config(
    materialized='table'
) }}

-- SCD Type 2: full refresh from the view, which rebuilds the complete version
-- history on every run. Incremental is unsafe for Type 2 because closing out
-- old versions requires re-evaluating LEAD(EffectiveStartDateTime) across the
-- whole partition.
SELECT * FROM {{ ref('vw_<Entity>') }}

Rules (both variants):
- The view name reference must match the PascalCase naming: `vw_<Entity>` (e.g. `vw_DimEmployee`, `vw_FactSales`).
- For Type 1 only: `unique_key` is the STM Primary Key column name verbatim (e.g. `'EmployeeHashPK'`, `'SalesHashPK'`) — NOT snake_cased.
- For Type 1 only: the incremental filter references `LoadTimestamp` (PascalCase, as named in the view).
- For Type 2: do NOT set `unique_key` — the same HashPK legitimately repeats (once per version). The composite grain is `(<Entity>HashPK, EffectiveStartDateTime)`; document it in the schema.yml description.

Replace <pk_column_snake_case> with the snake_case primary key from the STM
(the column where Field Type = "Primary Key").

### Validation
After writing both files, call the dbt MCP tool `parse` (server: project-0-cursorskills-dbt)
to validate. If parse fails, fix the SQL and retry.

Return the file paths of both generated models.
```

### Phase 3 — Validate

After all subagents complete, run:
1. `parse` via dbt MCP — confirm all models valid
2. `list` via dbt MCP — confirm all models registered
3. `get_lineage_dev` on a sample model — confirm dependency graph is correct (source -> views/_v -> enriched/incremental)

## SCD Type 2 Handling (Ajay Kalyan multi-CTE pattern)

### How to detect Type 2 from the STM

An STM is Type 2 when **any** of these are true:
- Section 4 (Target Schema Definition) says `SCD Type = Type 2`.
- Section 7 (Field-Level Mapping Matrix) uses the Ajay format: multiple `### Data Condition N` blocks followed by a `### Final` block.
- Section 7 has target columns `EffectiveStartDateTime`, `EffectiveEndDateTime`, `CurrentFlagYN` (with `Field Type = Type 2 Metadata`).
- Section 8 lists rule `BR12 — Type 2 Metadata` requiring the three Type 2 columns.

If any of the above match, generate the Type 2 view pattern below instead of the Type 1 pattern.

### View SQL structure (Type 2)

The view does NOT use the Type 1 `cte<TABLE>` + `QUALIFY ROW_NUMBER()` pattern. Instead, it uses a 5-CTE pipeline that unifies source versions, hashes for change detection, reduces rows to real change boundaries, and projects the final Type 2 row:

```sql
{{ config(materialized='view') }}

WITH cte_prep AS (
    -- 1. Alias every source column and attach Type 2 windowing metadata.
    --    For Fivetran history-mode sources, pull _FIVETRAN_START / _FIVETRAN_END / _FIVETRAN_ACTIVE
    --    as EffectiveStartDateTimeUTC / EffectiveEndDateTimeRaw / IsFivetranActive.
    --    Hard-code DataCondition, SourceSystemCode, FileName here.
    SELECT
        <business_cols>,
        _FIVETRAN_SYNCED   AS InsertedDateTimeUTC,
        _FIVETRAN_START    AS EffectiveStartDateTimeUTC,
        _FIVETRAN_END      AS EffectiveEndDateTimeRaw,
        _FIVETRAN_ACTIVE   AS IsFivetranActive,
        'ERP'              AS SourceSystemCode,
        ''                 AS FileName,
        'Data Condition 1' AS DataCondition
    FROM {{ source('<source>', '<TABLE>') }}
),

cte_prep_hash AS (
    -- 2. Compute Hashbytes over all BUSINESS attributes (sorted alphabetically),
    --    pipe-delimited, NULL-sentinel '#@#@#@#@#'.
    --    Exclude keys/metadata/audit columns from the hash input.
    SELECT
        *,
        SHA2_BINARY(
               IFNULL(CAST(<attr_a> AS VARCHAR), '#@#@#@#@#') || '|'
            || IFNULL(CAST(<attr_b> AS VARCHAR), '#@#@#@#@#') || '|'
            || ...
            , 256
        ) AS Hashbytes
    FROM cte_prep
),

cte_prep_hash_lag AS (
    -- 3. For every business key, compute LAG(Hashbytes) to see the previous version's hash,
    --    and LEAD(EffectiveStartDateTimeUTC) - 1 microsecond to derive the real
    --    EffectiveEndDateTime of this version from the next version's start.
    SELECT
        *,
        LAG(Hashbytes) OVER (
            PARTITION BY <BUSINESS_KEY>
            ORDER BY EffectiveStartDateTimeUTC
        ) AS LagHash,
        DATEADD(
            MICROSECOND, -1,
            LEAD(EffectiveStartDateTimeUTC) OVER (
                PARTITION BY <BUSINESS_KEY>
                ORDER BY EffectiveStartDateTimeUTC
            )
        ) AS LeadEffectiveStartDateTimeUTC,
        InsertedDateTimeUTC AS StageInsertedDateTimeUTC
    FROM cte_prep_hash
),

cte_row_reduce AS (
    -- 4. Keep only rows that represent a real change boundary:
    --    either the first version (LagHash IS NULL) or a row whose attributes
    --    differ from the previous version (Hashbytes <> LagHash).
    --    This collapses Fivetran "no-op updates" (same business attributes, new _FIVETRAN_START).
    SELECT *
    FROM cte_prep_hash_lag
    WHERE Hashbytes <> LagHash OR LagHash IS NULL
),

fin AS (
    -- 5. Project final Type 2 row with Ajay's standard column ordering:
    --    keys -> business attributes -> Type 2 metadata -> audit -> source -> Hashbytes -> DataCondition.
    SELECT
        HASH(IFNULL(CAST(<BUSINESS_KEY> AS VARCHAR), '#@#@#@#@#'), SourceSystemCode) AS <Entity>HashPK,
        HASH(IFNULL(CAST(<NATURAL_KEY> AS VARCHAR), '#@#@#@#@#'), SourceSystemCode) AS <Entity>HashBK,
        <business attribute SELECTs, mirroring the STM Final section>,
        CAST(EffectiveStartDateTimeUTC AS TIMESTAMP_TZ) AS EffectiveStartDateTime,
        CAST(COALESCE(LeadEffectiveStartDateTimeUTC, EffectiveEndDateTimeRaw) AS TIMESTAMP_TZ)
            AS EffectiveEndDateTime,
        CASE
            WHEN LeadEffectiveStartDateTimeUTC IS NULL AND IsFivetranActive THEN 'Y'
            ELSE 'N'
        END AS CurrentFlagYN,
        CAST(EffectiveStartDateTimeUTC AS TIMESTAMP_TZ) AS CreatedDateTime,
        CAST(InsertedDateTimeUTC       AS TIMESTAMP_TZ) AS ModifiedDateTime,
        SourceSystemCode,
        CAST(<BUSINESS_KEY> AS VARCHAR(40))  AS Source<Entity>PK,
        CAST(<NATURAL_KEY>  AS VARCHAR(100)) AS Source<Entity>BK,
        FileName,
        CAST(StageInsertedDateTimeUTC AS TIMESTAMP_TZ) AS StageInsertedDateTimeUTC,
        Hashbytes,
        DataCondition
    FROM cte_row_reduce
)

SELECT * FROM fin
```

### Type 2 key & hash conventions (differ from Type 1)

Type 2 models follow **Ajay's** convention, which differs from the Type 1 convention earlier in this skill:

| Column class | Type 1 | Type 2 |
|---|---|---|
| `*HashPK`, `*HashBK`, `*HashFK` | `SHA2(..., 256)` → `VARCHAR(64)` | `HASH(<col>, 'ERP')` → `NUMBER(38,0)` (Snowflake native 64-bit) |
| `Hashbytes` (change detection) | `SHA2(..., 256)` → `VARCHAR(64)` | `SHA2_BINARY(..., 256)` → `BINARY` |

`HASH()` and `SHA2_BINARY()` are both Snowflake-native and do not depend on the session `BINARY_INPUT_FORMAT` (the warning against `CAST(... AS BINARY)` does **not** apply — these are dedicated functions, not casts). Always match the STM's `Data Type` column:
- STM says `NUMBER(38,0)` for `*Hash*` → use `HASH(...)`.
- STM says `BINARY` for `Hashbytes` → use `SHA2_BINARY(...)`.

Do NOT mix conventions within a single view.

### Fivetran history-mode awareness

When the STM says the source is in **Fivetran history mode** (Section 3 "Notes" mentions `_FIVETRAN_START` / `_FIVETRAN_END` / `_FIVETRAN_ACTIVE`), the Type 2 windowing logic comes FROM THE SOURCE — do NOT generate synthetic version windows. The view's job is simply to:
1. Pass `_FIVETRAN_START` through as `EffectiveStartDateTime`.
2. Close each version via `LEAD(_FIVETRAN_START) - 1 microsecond`, falling back to `_FIVETRAN_END` for the current/open version.
3. Derive `CurrentFlagYN` from `_FIVETRAN_ACTIVE` + absence of a LEAD row.
4. Collapse no-op updates via the `Hashbytes <> LagHash` filter.

If the STM explicitly lists "Data Condition 2" for hard deletes or similar, union an additional source-reading CTE into `cte_prep` before hashing. Most Fivetran history-mode sources do NOT need Data Condition 2 because `_FIVETRAN_END ≠ '9999-12-31 23:59:59.999'` already marks closed/deleted rows.

### Type 2 enriched model (full refresh)

The enriched model is `table` (not `incremental`):

```sql
{{ config(
    materialized='table'
) }}

-- SCD Type 2: full refresh from the view, which rebuilds the complete version
-- history on every run. Incremental is unsafe for Type 2 because closing out
-- old versions requires re-evaluating LEAD(EffectiveStartDateTime) across the
-- whole partition.
SELECT * FROM {{ ref('vw_<Entity>') }}
```

### Type 2 schema.yml conventions

- The PK has a `not_null` test but **NOT** a `unique` test — the same `<Entity>HashPK` legitimately appears once per version.
- The model's documented grain is composite: `(<Entity>HashPK, EffectiveStartDateTime)`.
- `CurrentFlagYN` gets `accepted_values: ['Y', 'N']`.
- `EffectiveStartDateTime`, `EffectiveEndDateTime`, `CurrentFlagYN`, `SourceSystemCode` all get `not_null`.
- Remove legacy Type 1 columns (`EtlBatchId`, `LoadTimestamp`, `IsCurrent`) from the Type 2 schema.yml entry — they are replaced by the Type 2 metadata + audit columns listed in the STM's Final section.
- Do not add `dbt_utils.unique_combination_of_columns` tests unless `dbt_utils` is declared in `packages.yml`. Document the composite grain in the model description instead.

### Unmapped columns in Type 2 views

Same rule as Type 1: every target column that has no source (empty Source Table + Source Column in the STM Final section) must emit `CAST(NULL AS <type>)` or the STM default, with an inline `-- not available in source` comment on the SAME line:

```sql
CAST(NULL AS VARCHAR(10)) AS BrandCode,   -- not available in source
CAST(NULL AS VARCHAR(50)) AS SubcategoryName, -- not available in source
```

This applies to Type 2 exactly as it does to Type 1. A Type 2 view must not silently cast unmapped columns to NULL without the comment — reviewers need to see at a glance which attributes are gaps vs real source-backed data.

## Conventions

- **Two models per table:** `models/views/vw_{Entity}` (view with logic) + `models/enriched/{Entity}` (incremental from view)
- **No staging layer:** view models use `{{ source() }}` directly
- **No precision truncation:** do not use `LEFT()` to force field lengths — let source precision flow through
- **Unmapped columns (applies to Type 1 AND Type 2):** every column with empty Source Table + Source Column in the STM's mapping must emit `CAST(NULL AS <type>)` (or the STM default) with an inline `-- not available in source` comment on the same line. This rule is non-negotiable — reviewers rely on it to see at a glance which target attributes are genuine data gaps vs real source-backed data.
- **Model/file naming — STM-verbatim (PascalCase):** file names and dbt model names use the STM `Target Table` verbatim — e.g. `DimEmployee.sql`, `vw_DimEmployee.sql`, `FactSales.sql`, `vw_FactSales.sql`. NEVER snake_case. The only lowercase piece is the literal `vw_` prefix on view models.
- **CTE naming:** `cte<TABLE_NAME>` (e.g. `cteEMPLOYEES`, `cteSALES_ORDER_ITEMS`)
- **Column naming — STM-verbatim (PascalCase):** target-column aliases and schema.yml entries MUST match the STM's `Target Column` exactly (e.g. `EmployeeHashPK`, `FirstName`, `HomeStoreHashFK`, `LoadTimestamp`, `Hashbytes`). Do NOT snake_case them.
- **Column references:** unqualified (no table/CTE prefix) unless needed to disambiguate in multi-CTE joins
- **Deduplication:** Snowflake `QUALIFY ROW_NUMBER() OVER (PARTITION BY ... ORDER BY ... DESC) = 1` inside the source CTE — no separate LatestRank column
- **load_timestamp:** sourced from `_FIVETRAN_SYNCED`, NEVER from `CURRENT_TIMESTAMP()` (breaks incremental logic)

## Key Generation & Hashing Convention

These rules apply to every view model generated by subagents.

### Hash Key Pattern (Snowflake)

All hash-based keys use plain **`SHA2`** with digest size 256 and COALESCE sentinel values for NULL safety. `SHA2(..., 256)` returns `VARCHAR(64)` hex, which is the team's standard storage type for hash columns:

```sql
SHA2(
    COALESCE(CAST(source_col AS VARCHAR), '#@#@#@#@#')
    || '|' || COALESCE(CAST(source_system_code AS VARCHAR), '#@#@#@#@#')
, 256)
```

> **Do not cast to `BINARY(32)`** (neither `CAST(SHA2(...) AS BINARY(32))` nor `SHA2_BINARY(...)`). Snowflake's `BINARY_INPUT_FORMAT` session parameter can be `HEX` or `BASE64` depending on the account, so `VARCHAR→BINARY` casts are not portable. Keeping hash columns as `VARCHAR(64)` is human-readable, portable, and dodges the issue entirely.

When a hash key is derived from multiple columns, concatenate them with `'|'` delimiters, each wrapped in `COALESCE(..., '#@#@#@#@#')`.

### Key Types

| Key suffix | Purpose | Derivation |
|---|---|---|
| `HashPK` | Surrogate primary key | `SHA2(..., 256)` of source business key column(s) + source system code, stored as `VARCHAR(64)` |
| `HashBK` | Business key | `SHA2(..., 256)` of source natural key + source system code, stored as `VARCHAR(64)` |
| `HashFK` | Foreign key | Same derivation as the referenced dimension's HashPK. Wrap in `IFF(col IS NULL, NULL, ...)` when nullable. `VARCHAR(64)`. |
| `Hashbytes` | Change detection | `SHA2` (VARCHAR hex) of **sourced descriptive/attribute columns** concatenated. Used for SCD and incremental change comparison. Stays VARCHAR — no BINARY cast. |

### Hashbytes Change Detection Column

Every view must include a `hashbytes` column as the **last column before audit/metadata fields**. It concatenates all non-key, non-audit attribute columns:

```sql
SHA2(
    COALESCE(CAST(attr_col_1 AS VARCHAR), '#@#@#@#@#')
    || '|' || COALESCE(CAST(attr_col_2 AS VARCHAR), '#@#@#@#@#')
    || '|' || COALESCE(CAST(attr_col_N AS VARCHAR), '#@#@#@#@#')
, 256) AS Hashbytes
```

The alias is `Hashbytes` (PascalCase) to match the STM's Target Column name.

Include only columns that are **sourced from the source system** — i.e. they have a real Source Table and Source Column in the STM. **Exclude** unmapped columns (those with no source, hardcoded defaults like 'Unknown', 0, NULL placeholders). Since unmapped columns never change, including them in hashbytes adds no change detection value and just produces noise.

Also exclude HashPK, HashBK, HashFK, and audit/metadata fields (`EtlBatchId`, `LoadTimestamp`). This enables downstream SCD Type 1/2 processing by comparing source Hashbytes vs target Hashbytes.

### Latest Record Deduplication — Snowflake QUALIFY

Use Snowflake's `QUALIFY` clause to deduplicate in a single step. Do NOT create a separate CTE with a LatestRank column followed by `WHERE LatestRank = 1` — `QUALIFY` applies the window-function filter directly.

```sql
WITH cte<TABLE_NAME> AS (
    SELECT
        <columns including _FIVETRAN_SYNCED>
    FROM {{ source('...', '...') }}
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY <business_key_column(s)>
        ORDER BY <last_modified_column> DESC
    ) = 1
)

SELECT
    <all hash keys aliased PascalCase: EmployeeHashPK, EmployeeHashBK, ...>,
    <all attribute columns aliased PascalCase: FirstName, LastName, ...>,
    <Hashbytes>,
    <audit columns: EtlBatchId, _FIVETRAN_SYNCED AS LoadTimestamp>
FROM cte<TABLE_NAME>
```

Use the best available ordering column from the source (e.g., `_FIVETRAN_SYNCED`, `UPDATED_AT`, `LAST_UPDATE_DATE`). If no obvious ordering column exists, omit the `QUALIFY` and add a `-- NOTE: no deduplication column available` comment.

### LoadTimestamp sourced from _FIVETRAN_SYNCED

Every view must emit `LoadTimestamp` from the source table's `_FIVETRAN_SYNCED` column, not from `CURRENT_TIMESTAMP()`. This is critical:

- `CURRENT_TIMESTAMP()` would assign a fresh timestamp to every row on every run, which makes `WHERE LoadTimestamp > MAX(LoadTimestamp)` match every row — incremental logic breaks.
- `_FIVETRAN_SYNCED` reflects when Fivetran actually synced that row from the source, so only new/changed rows advance `LoadTimestamp`.

The source CTE must select `_FIVETRAN_SYNCED` so it can be projected through:

```sql
WITH cte<TABLE> AS (
    SELECT
        <business cols>,
        _FIVETRAN_SYNCED
    FROM {{ source('...', '...') }}
    QUALIFY ROW_NUMBER() OVER (PARTITION BY <pk> ORDER BY _FIVETRAN_SYNCED DESC) = 1
)
SELECT
    ...,
    _FIVETRAN_SYNCED AS LoadTimestamp
FROM cte<TABLE>
```

### Column alias case convention — STM verbatim

Every output column alias in the view and every `name:` entry in the schema YAML must match the STM's `Target Column` exactly. The STM uses PascalCase (`EmployeeHashPK`, `FirstName`, `HomeStoreHashFK`, `IsActive`, `LoadTimestamp`, `Hashbytes`). Keep it that way. NEVER snake_case target column names. Only dbt model/file names are snake_case.

Examples:

- STM `EmployeeHashPK` → `AS EmployeeHashPK` in SQL, `- name: EmployeeHashPK` in YAML, `unique_key='EmployeeHashPK'` in the incremental config.
- STM `HomeStoreHashFK` → `AS HomeStoreHashFK`.
- STM `LoadTimestamp` → `AS LoadTimestamp`; the incremental `WHERE` clause uses `LoadTimestamp > (SELECT MAX(LoadTimestamp) FROM {{ this }})`.
