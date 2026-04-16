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
| `vw_interim_{entity}_{source_system}.sql` | `models/views/` | `view` | Holds all transformation logic; references `{{ source() }}` directly |
| `{model_name}.sql` | `models/enriched/` | `incremental` | `SELECT * FROM {{ ref('vw_interim_{entity}_{source_system}') }}` with incremental filter |

**View naming convention:** `vw_interim_<entity>_<source_system>` where entity is the snake_case target table name and source_system is derived from the source schema (e.g. `erp` from `BRONZE_ERP__DBO`). Example: `vw_interim_dim_employee_erp`.

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
- `models/views/_schema.yml` — model entries for the `_v` view models, with column descriptions and tests
- `models/enriched/_schema.yml` — model entries for the base incremental models, with column descriptions and tests

It does NOT generate enriched model SQL — that is Phase 2.

### Phase 2 — Enriched models (parallel subagents)

Launch **one subagent per STM** to generate **two** enriched model SQL files. Each subagent reads the STM, picks up the Snowflake SQL expressions from the Transformation column, and writes the view + incremental pair.

Launch all subagents in parallel using the Task tool with `subagent_type="generalPurpose"`. Each subagent gets this prompt (fill in the specifics per STM):

```
You are generating dbt models for Snowflake from a source-to-target mapping.

Read the STM file: {stm_filepath}
Read the sources file: {dbt_project}/models/staging/_sources.yml

You must produce TWO files:

### File 1 — View model
Write to: {dbt_project}/models/views/{view_model_name}.sql

The view_model_name follows the pattern: vw_interim_{entity}_{source_system}
where entity = snake_case target table name, source_system = erp (from BRONZE_ERP__DBO).
Example: vw_interim_dim_employee_erp

This model holds all transformation logic.

Rules for the view:
- Start with: {{ config(materialized='view') }}
- Use WITH ... AS CTEs referencing source tables via {{ source('<source_name>', '<TABLE>') }}.
  Look up the correct source name from _sources.yml.

**CTE naming convention:**
- Name each source CTE as `cte<TABLE_NAME>` (e.g. `cteEMPLOYEES`, `cteSALES_ORDER_ITEMS`).
- Reference columns from the CTE WITHOUT a table alias prefix (just `EMPLOYEE_ID`, not `e.EMPLOYEE_ID`).
- In the FROM clause, use the CTE name directly without aliasing (e.g. `FROM cteEMPLOYEES`, not `FROM cteEMPLOYEES e`).
- For fact tables with multiple CTEs that need JOINs, use the CTE names directly in JOIN clauses (e.g. `FROM cteSALES_ORDER_ITEMS JOIN cteSALES_ORDERS ON ...`). If column names conflict between CTEs, qualify with the CTE name.

**Source CTE — latest record deduplication:**
- The source CTE must include a ROW_NUMBER() window but NOT filter it:
    ROW_NUMBER() OVER (PARTITION BY <business_key_col> ORDER BY <modified_timestamp> DESC) AS LatestRank
- Use the best available ordering column from the source (_FIVETRAN_SYNCED, UPDATED_AT, etc.).
- Do NOT filter inside the CTE. Instead, the WHERE LatestRank = 1 filter goes at the very bottom of the final SELECT statement, after all column transformations.
- Structure must be:
    WITH cte<TABLE> AS (SELECT *, ROW_NUMBER() ... AS LatestRank FROM source)
    SELECT <all transformed columns> FROM cte<TABLE> WHERE LatestRank = 1
- If no ordering column exists, omit deduplication and add a comment.

**Hash key generation (SHA2_256 + COALESCE sentinel):**
- All HashPK, HashBK, HashFK columns use this pattern:
    CAST(SHA2(COALESCE(CAST(col AS VARCHAR), '#@#@#@#@#'), 256) AS BINARY(32))
- For composite keys, concatenate with '|' delimiters:
    CAST(SHA2(COALESCE(CAST(col1 AS VARCHAR), '#@#@#@#@#') || '|' || COALESCE(CAST(col2 AS VARCHAR), '#@#@#@#@#'), 256) AS BINARY(32))
- Nullable FKs: wrap in IFF(col IS NULL, NULL, CAST(SHA2(...) AS BINARY(32)))
- Do NOT use MD5. Always use SHA2 with 256.

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
- For fact tables with multiple source CTEs joined together, qualify column names with the CTE name
  ONLY when the same column name exists in multiple CTEs. Otherwise keep columns unqualified.
- If Transformation is empty and Source Table/Column are filled, it's a direct passthrough —
  just reference the source column directly (e.g. FIRST_NAME).
- Do NOT truncate fields with LEFT() or force data type lengths. Let columns pass through
  at source precision. TRIM() is acceptable but do not wrap in LEFT(..., N).
- For columns with no source mapping (empty Source Table + Source Column),
  use the default value from the STM if one exists (e.g. 'Unknown', 0, TRUE),
  or CAST(NULL AS <DataType>) if no default. Either way, add an inline comment:
  `-- not available in source`
  Example: `'Unknown' AS customer_type, -- not available in source`
  Example: `CAST(NULL AS DATE) AS termination_date, -- not available in source`
- Place {{ config(...) }} BEFORE the WITH keyword.

### File 2 — Incremental model
Write to: {dbt_project}/models/enriched/{model_name}.sql

This model reads from the view and persists the table.

Content:
{{ config(
    materialized='incremental',
    unique_key='<pk_column_snake_case>'
) }}

SELECT * FROM {{ ref('{view_model_name}') }}

{{% if is_incremental() %}}
WHERE load_timestamp > (SELECT MAX(load_timestamp) FROM {{ this }})
{{% endif %}}

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

## Conventions

- **Two models per table:** `models/views/vw_interim_{entity}_{source}` (view with logic) + `models/enriched/{entity}` (incremental from view)
- **No staging layer:** view models use `{{ source() }}` directly
- **No precision truncation:** do not use `LEFT()` to force field lengths — let source precision flow through
- **Unmapped columns:** `CAST(NULL AS <type>)` or STM default, with `-- not available in source` comment
- **View naming:** `vw_interim_<entity>_<source_system>` (e.g. `vw_interim_dim_employee_erp`)
- **CTE naming:** `cte<TABLE_NAME>` (e.g. `cteEMPLOYEES`, `cteSALES_ORDER_ITEMS`)
- **Column references:** unqualified (no table/CTE prefix) unless needed to disambiguate in multi-CTE joins
- **Dedup rank column:** `LatestRank` (PascalCase, matching reference convention)

## Key Generation & Hashing Convention

These rules apply to every view model generated by subagents.

### Hash Key Pattern (Snowflake)

All hash-based keys use SHA2_256 with COALESCE sentinel values for NULL safety, cast to BINARY for a deterministic fixed-width result:

```sql
CAST(SHA2(
    COALESCE(CAST(source_col AS VARCHAR), '#@#@#@#@#')
    || '|' || COALESCE(CAST(source_system_code AS VARCHAR), '#@#@#@#@#')
, 256) AS BINARY(32))
```

When a hash key is derived from multiple columns, concatenate them with `'|'` delimiters, each wrapped in `COALESCE(..., '#@#@#@#@#')`.

### Key Types

| Key suffix | Purpose | Derivation |
|---|---|---|
| `HashPK` | Surrogate primary key | SHA2 hash of source business key column(s) + source system code |
| `HashBK` | Business key | SHA2 hash of source natural key + source system code |
| `HashFK` | Foreign key | Same derivation as the referenced dimension's HashPK. Wrap in `IFF(col IS NULL, NULL, ...)` when nullable. |
| `hashbytes` | Change detection | SHA2 hash of **all descriptive/attribute columns** concatenated. Used for SCD and incremental change comparison. |

### Hashbytes Change Detection Column

Every view must include a `hashbytes` column as the **last column before audit/metadata fields**. It concatenates all non-key, non-audit attribute columns:

```sql
SHA2(
    COALESCE(CAST(attr_col_1 AS VARCHAR), '#@#@#@#@#')
    || '|' || COALESCE(CAST(attr_col_2 AS VARCHAR), '#@#@#@#@#')
    || '|' || COALESCE(CAST(attr_col_N AS VARCHAR), '#@#@#@#@#')
, 256) AS hashbytes
```

Include only columns that are **sourced from the source system** — i.e. they have a real Source Table and Source Column in the STM. **Exclude** unmapped columns (those with no source, hardcoded defaults like 'Unknown', 0, NULL placeholders). Since unmapped columns never change, including them in hashbytes adds no change detection value and just produces noise.

Also exclude HashPK, HashBK, HashFK, and audit/metadata fields (etl_batch_id, load_timestamp). This enables downstream SCD Type 1/2 processing by comparing source hashbytes vs target hashbytes.

### Latest Record Deduplication

The source CTE adds the ROW_NUMBER() but does NOT filter. The filter goes at the bottom of the final SELECT:

```sql
WITH cte<TABLE_NAME> AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY <business_key_column(s)>
            ORDER BY <last_modified_column> DESC
        ) AS LatestRank
    FROM {{ source('...', '...') }}
)

SELECT
    <all hash keys>,
    <all attribute columns>,
    <hashbytes>,
    <audit columns>
FROM cte<TABLE_NAME>
WHERE LatestRank = 1
```

The `WHERE LatestRank = 1` must be the last line before the closing semicolon — after all column transformations, not inside the CTE. Columns are referenced WITHOUT a prefix (e.g. `FIRST_NAME`, not `cteEMPLOYEES.FIRST_NAME`).

Use the most appropriate ordering column from the source (e.g., `_FIVETRAN_SYNCED`, `UPDATED_AT`, `LAST_UPDATE_DATE`, or whatever timestamp column exists). If no obvious ordering column exists, omit the deduplication and add a `-- NOTE: no deduplication column available` comment.
