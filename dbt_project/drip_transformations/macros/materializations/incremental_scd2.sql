-- =============================================================================
-- Custom materialization: incremental_scd2
-- =============================================================================
-- Implements a delete+reinsert incremental strategy for SCD Type 2 dimensions
-- where window functions (LEAD, LAG) prevent a simple row-append approach.
--
-- Why this exists:
--   Storing LEAD()-derived EffectiveEndDateTime means re-appending new rows
--   leaves the previous row's end-date stale. This materialization solves that
--   by deleting all versions of a changed primary key and reinserting the full,
--   correctly-computed set from the model's SELECT.
--
-- Change detection is hash-based (resolves [G1]):
--   SHA2 of business attribute columns is compared between source and the
--   current target row. No Fivetran or ingestion-tool dependency.
--
-- Three execution states (industry-standard SCD2 loading pattern):
--
--   STATE 1 — Target does not exist (or --full-refresh with safe_mode = false):
--     DROP existing table (if any) + CREATE TABLE AS SELECT.
--     Standard dbt first-run behaviour.
--
--   STATE 2 — Target exists and is EMPTY (initial population):
--     Column validation  → schema contract check (always runs).
--     Skip PK check      → nothing to compare against.
--     INSERT all rows    → full model output, no DELETE needed.
--     This handles loading into a pre-created dimension table for the first time.
--
--   STATE 3 — Target exists and has DATA (incremental update):
--     Column validation  → schema contract check (always runs).
--     PK existence check → hard error on unknown PKs (strict_pk_check = true).
--     Hash comparison    → only changed PKs processed.
--     DELETE stale       → remove old versions for changed PKs.
--     INSERT fresh       → reinsert all versions with correct window functions.
--
-- Safe mode (meta.safe_mode = true):
--   When the target table already exists, ALWAYS takes State 2 or State 3 —
--   never drops the table, even when --full-refresh is passed.
--   Use this when loading into a pre-existing dimension table that must be
--   preserved. Column validation always runs first.
--
-- Config — pass inside meta{} to avoid dbt deprecation warnings:
--
--   Required:
--     meta.source_ns        — dbt source name,           e.g. 'bronze_erp'
--     meta.source_tbl       — table in that source,       e.g. 'customers'
--     meta.source_bk_col    — PK column in source,        e.g. 'customer_id'
--     meta.target_bk_col    — PK column in target,        e.g. 'SourceCustomerBK'
--     meta.hash_cols        — source columns to SHA2 for change detection
--
--   Optional:
--     meta.is_current_col   — current-row flag column in target (default: 'CurrentFlagYN')
--     meta.is_current_value — value indicating current row    (default: 'Y')
--     meta.hashbytes_col    — hash storage column in target   (default: 'Hashbytes')
--     meta.safe_mode        — never drop existing table       (default: false)
--     meta.strict_pk_check  — hard error on unknown PKs in State 3 (default: true)
--
-- Column naming:
--   Column names in meta{} must match the model's SELECT aliases exactly.
--   Names are derived from scd2_conventions.yml via the generator — NOT from
--   any existing table in the database. The conventions file is the source of truth.
--
-- TODO: GENERIC roadmap (remaining items)
--   [G2] Change adapter='snowflake' to adapter='default' and use
--        adapter.dispatch() for any Snowflake-specific SQL fragments.
--   [G3] Replace TIMESTAMP_TZ sentinel with an adapter-dispatched null date.
-- =============================================================================

{% materialization incremental_scd2, adapter='snowflake' %}

  -- ── Relation setup ──────────────────────────────────────────────────────────
  {%- set target_relation   = this.incorporate(type='table') -%}
  {%- set existing_relation = load_relation(this) -%}
  {%- set tmp_relation      = make_temp_relation(target_relation) -%}

  -- ── Config resolution ───────────────────────────────────────────────────────
  {%- set m                = config.get('meta', {}) -%}
  {%- set safe_mode        = m.get('safe_mode',        false) -%}
  {%- set strict_pk_check  = m.get('strict_pk_check',  true) -%}
  {%- set source_ns        = m.get('source_ns')        or config.require('source_ns') -%}
  {%- set source_tbl       = m.get('source_tbl')       or config.require('source_tbl') -%}
  {%- set source_table     = source(source_ns, source_tbl) -%}
  {%- set source_bk_col    = m.get('source_bk_col')    or config.require('source_bk_col') -%}
  {%- set target_bk_col    = m.get('target_bk_col')    or config.require('target_bk_col') -%}
  {%- set hash_cols        = m.get('hash_cols')        or config.require('hash_cols') -%}
  {%- set is_current_col   = m.get('is_current_col',   'CurrentFlagYN') -%}
  {%- set is_current_value = m.get('is_current_value', 'Y') -%}
  {%- set hashbytes_col    = m.get('hashbytes_col',    'Hashbytes') -%}

  {{ run_hooks(pre_hooks) }}

  -- ── State determination ──────────────────────────────────────────────────────
  -- safe_mode = true → never drop; route to State 2 or 3 based on row count.
  -- safe_mode = false → State 1 when table missing or --full-refresh requested.
  {%- set use_existing = (existing_relation is not none) and
                         (safe_mode or not should_full_refresh()) -%}

  -- ── STATE 1: Target does not exist / full-refresh without safe_mode ─────────
  {% if not use_existing %}

    {%- if existing_relation is not none -%}
      {{ adapter.drop_relation(existing_relation) }}
    {%- endif -%}

    {% call statement('main') %}
      {{ create_table_as(False, target_relation, sql) }}
    {% endcall %}

  -- ── STATE 2 / 3: Target exists — determine row count to pick path ───────────
  {% else %}

    -- Determine whether the target is empty (State 2) or has data (State 3).
    {%- if execute -%}
      {%- set row_count_query -%}
        SELECT COUNT(*) FROM {{ existing_relation }}
      {%- endset -%}
      {%- set row_count_result = run_query(row_count_query) -%}
      {%- set target_row_count = row_count_result.columns[0].values()[0] | int -%}
    {%- else -%}
      {%- set target_row_count = 0 -%}
    {%- endif -%}

    -- ── Shared Step: Build zero-row temp to probe generated column structure ───
    -- Executes model SQL once with WHERE 1=0 — no data returned, schema captured.
    -- Used by column validation before any write touches the target.
    {% call statement('build_tmp_probe') %}
      CREATE TEMPORARY TABLE {{ tmp_relation }} AS
      SELECT model_rows.*
      FROM (
        {{ sql }}
      ) model_rows
      WHERE 1 = 0
    {% endcall %}

    -- ── Shared Step: Column validation (always runs in State 2 and State 3) ────
    --
    --   Rule 1 — Key columns (target_bk_col, hashbytes_col, is_current_col)
    --            must exist in both generated output and target. Hard error if not.
    --   Rule 2 — Every column the model generates must exist in the target.
    --            Hard error if not — prevents silent data loss.
    --   Rule 3 — Extra columns in the target not produced by the model are allowed.
    --            They retain their existing values / column defaults.
    --
    --   Uses LEFT JOINs (not EXCEPT) to avoid Snowflake's scalar subquery
    --   limitation with set-operation CTEs.
    {% call statement('validate_columns', fetch_result=True) %}
      WITH tmp_cols AS (
        SELECT UPPER(COLUMN_NAME) AS col_name
        FROM INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_SCHEMA = UPPER('{{ tmp_relation.schema }}')
          AND TABLE_NAME   = UPPER('{{ tmp_relation.identifier }}')
      ),
      tgt_cols AS (
        SELECT UPPER(COLUMN_NAME) AS col_name
        FROM INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_SCHEMA = UPPER('{{ existing_relation.schema }}')
          AND TABLE_NAME   = UPPER('{{ existing_relation.identifier }}')
      ),
      key_cols AS (
        SELECT UPPER('{{ target_bk_col }}') AS col_name UNION ALL
        SELECT UPPER('{{ hashbytes_col }}')             UNION ALL
        SELECT UPPER('{{ is_current_col }}')
      ),
      missing_gen AS (
        SELECT t.col_name
        FROM tmp_cols t
        LEFT JOIN tgt_cols g ON t.col_name = g.col_name
        WHERE g.col_name IS NULL
      ),
      missing_keys AS (
        SELECT k.col_name
        FROM key_cols k
        LEFT JOIN tmp_cols t ON k.col_name = t.col_name
        LEFT JOIN tgt_cols g ON k.col_name = g.col_name
        WHERE t.col_name IS NULL OR g.col_name IS NULL
      )
      SELECT
        (SELECT COUNT(*)
           FROM missing_gen)                                      AS missing_in_target_count,
        (SELECT LISTAGG(col_name, ', ')
                WITHIN GROUP (ORDER BY col_name)
           FROM missing_gen)                                      AS missing_in_target_cols,
        (SELECT COUNT(*)
           FROM missing_keys)                                     AS missing_key_count,
        (SELECT LISTAGG(col_name, ', ')
                WITHIN GROUP (ORDER BY col_name)
           FROM missing_keys)                                     AS missing_key_cols
    {% endcall %}

    {%- set validation           = load_result('validate_columns') -%}
    {%- set missing_in_target    = validation['data'][0][0] | int -%}
    {%- set missing_in_target_cols = validation['data'][0][1] -%}
    {%- set missing_key_count    = validation['data'][0][2] | int -%}
    {%- set missing_key_cols     = validation['data'][0][3] -%}

    {%- if missing_key_count > 0 -%}
      {{ exceptions.raise_compiler_error(
        "incremental_scd2 [column validation]: Key column(s) missing from "
        ~ "target table or generated model: [" ~ missing_key_cols ~ "]. "
        ~ "Check meta.target_bk_col, meta.hashbytes_col, meta.is_current_col "
        ~ "match the column names produced by the model SELECT and present in the target table."
      ) }}
    {%- endif -%}

    {%- if missing_in_target > 0 -%}
      {{ exceptions.raise_compiler_error(
        "incremental_scd2 [column validation]: Generated column(s) not found "
        ~ "in target table: [" ~ missing_in_target_cols ~ "]. "
        ~ "The model produces columns that do not exist in the target. "
        ~ "Update the target table schema or remove the column from the model."
      ) }}
    {%- endif -%}

    -- Drop the probe temp — it will be rebuilt with the right data below.
    {{ drop_relation_if_exists(tmp_relation) }}

    -- ── STATE 2: Target is empty — initial population ────────────────────────
    -- No DELETE needed. No PK check (nothing in target to compare against).
    -- Build temp with ALL model rows and INSERT everything.
    {% if target_row_count == 0 %}

      {% call statement('build_tmp') %}
        CREATE TEMPORARY TABLE {{ tmp_relation }} AS
        {{ sql }}
      {% endcall %}

      {% call statement('main') %}
        {{ scd2_insert_fresh(target_relation, tmp_relation) }}
      {% endcall %}

      {{ drop_relation_if_exists(tmp_relation) }}

    -- ── STATE 3: Target has data — incremental update ────────────────────────
    -- PK existence check → hash comparison → delete stale → insert fresh.
    {% else %}

      -- Step 3a: PK existence check — every PK the model would process must
      --          already exist in the target. Protects shared tables in dev.
      --          Skip by setting meta.strict_pk_check = false.
      {%- if strict_pk_check -%}

        {% call statement('validate_pks', fetch_result=True) %}
          WITH all_model_pks AS (
            SELECT DISTINCT CAST({{ target_bk_col }} AS VARCHAR) AS bk
            FROM (
              {{ sql }}
            ) model_rows
          ),
          target_pks AS (
            SELECT DISTINCT CAST({{ target_bk_col }} AS VARCHAR) AS bk
            FROM {{ existing_relation }}
          ),
          unexpected_pks AS (
            SELECT a.bk
            FROM all_model_pks a
            LEFT JOIN target_pks t ON a.bk = t.bk
            WHERE t.bk IS NULL
          )
          SELECT
            COUNT(*)                              AS unexpected_pk_count,
            LISTAGG(bk, ', ')
              WITHIN GROUP (ORDER BY bk)         AS unexpected_pk_values
          FROM unexpected_pks
        {% endcall %}

        {%- set pk_check         = load_result('validate_pks') -%}
        {%- set unexpected_count = pk_check['data'][0][0] | int -%}
        {%- set unexpected_values = pk_check['data'][0][1] -%}

        {%- if unexpected_count > 0 -%}
          {{ exceptions.raise_compiler_error(
            "incremental_scd2 [PK check]: "
            ~ unexpected_count | string ~ " business key(s) found in model output "
            ~ "that do not exist in the target table: [" ~ unexpected_values ~ "]. "
            ~ "No data has been changed. "
            ~ "Verify the source data or set meta.strict_pk_check = false "
            ~ "to allow new PKs (e.g. when onboarding new customers in production)."
          ) }}
        {%- endif -%}

      {%- endif -%}

      -- Step 3b: Build temp filtered to changed PKs only.
      --          Hash comparison: source SHA2 vs stored Hashbytes in current row.
      {% call statement('build_tmp') %}
        CREATE TEMPORARY TABLE {{ tmp_relation }} AS
        SELECT model_rows.*
        FROM (
          {{ sql }}
        ) model_rows
        WHERE {{ target_bk_col }} IN (
          {{ scd2_get_changed_keys(
               source_table,
               source_bk_col,
               existing_relation,
               target_bk_col,
               hash_cols,
               is_current_col,
               hashbytes_col
             ) }}
        )
      {% endcall %}

      -- Step 3c: Delete all target rows for changed PKs.
      {% call statement('delete_stale') %}
        {{ scd2_delete_stale(target_relation, target_bk_col, tmp_relation) }}
      {% endcall %}

      -- Step 3d: Insert fresh rows — explicit column list preserves model order.
      {% call statement('main') %}
        {{ scd2_insert_fresh(target_relation, tmp_relation) }}
      {% endcall %}

      {{ drop_relation_if_exists(tmp_relation) }}

    {% endif %}

  {% endif %}

  {{ run_hooks(post_hooks) }}

  {{ return({'relations': [target_relation]}) }}

{% endmaterialization %}
