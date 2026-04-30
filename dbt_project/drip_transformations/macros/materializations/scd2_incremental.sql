-- =============================================================================
-- Custom materialization: scd2_incremental
-- =============================================================================
-- Implements a delete+reinsert incremental strategy for SCD Type 2 dimensions
-- where LEAD() window functions prevent a simple row-append approach.
--
-- Strategy:
--   1. Find source PKs whose _FIVETRAN_SYNCED timestamp exceeds the watermark
--      stored in the target table (MAX of watermark_col).
--   2. Delete all existing target rows for those PKs (removes stale versions
--      whose EffectiveEndDateTime was recomputed by the view's LEAD()).
--   3. Reinsert all versions for those PKs from the model's SELECT (the view
--      re-evaluates LEAD() correctly over the full source partition).
--   4. On the first run (empty table) or --full-refresh, rebuild everything.
--
-- Config — pass custom keys inside meta{} to avoid deprecation warnings:
--
--   Required:
--     meta.source_ns      — dbt source name, e.g. 'bronze_erp__dbo'
--     meta.source_tbl     — table name within that source, e.g. 'CUSTOMERS'
--     meta.source_bk_col  — business-key column in the source, e.g. 'CUSTOMER_ID'
--     meta.target_bk_col  — matching column in the target, e.g. '"SourceCustomerPK"'
--
--   Optional:
--     meta.watermark_col  — target watermark column (default: '"StageInsertedDateTimeUTC"')
--     meta.synced_col     — source sync column (default: '_FIVETRAN_SYNCED')
--
-- Usage example:
--   {{ config(
--       materialized='scd2_incremental',
--       meta={
--           'source_ns':     'bronze_erp__dbo',
--           'source_tbl':    'CUSTOMERS',
--           'source_bk_col': 'CUSTOMER_ID',
--           'target_bk_col': '"SourceCustomerPK"',
--       }
--   ) }}
--   SELECT * FROM {{ ref('vw_DimCustomer') }}
-- =============================================================================

{% materialization scd2_incremental, adapter='snowflake' %}

  {%- set target_relation  = this.incorporate(type='table') -%}
  {%- set existing_relation = load_relation(this) -%}

  {%- set m              = config.get('meta', {}) -%}
  {%- set source_ns      = m.get('source_ns')     or config.require('source_ns') -%}
  {%- set source_tbl     = m.get('source_tbl')    or config.require('source_tbl') -%}
  {%- set source_table   = source(source_ns, source_tbl) -%}
  {%- set source_bk_col  = m.get('source_bk_col') or config.require('source_bk_col') -%}
  {%- set target_bk_col  = m.get('target_bk_col') or config.require('target_bk_col') -%}
  {%- set watermark_col  = m.get('watermark_col', '"StageInsertedDateTimeUTC"') -%}
  {%- set synced_col     = m.get('synced_col', '_FIVETRAN_SYNCED') -%}

  {{ run_hooks(pre_hooks) }}

  {% if existing_relation is none or should_full_refresh() %}

    {# ── Full build: drop existing and recreate from the full model SQL ── #}
    {% if existing_relation is not none %}
      {{ adapter.drop_relation(existing_relation) }}
    {% endif %}

    {% call statement('main') %}
      {{ create_table_as(False, target_relation, sql) }}
    {% endcall %}

  {% else %}

    {# ── Incremental: watermark → delete stale → reinsert updated versions ── #}

    {#
      Compute the watermark once so both DELETE and INSERT use the same cut-off,
      avoiding any skew between the two statements.
    #}
    {%- set watermark_query -%}
      SELECT COALESCE(MAX({{ watermark_col }}), '1900-01-01'::TIMESTAMP_TZ)
      FROM {{ target_relation }}
    {%- endset -%}
    {%- set watermark_result = run_query(watermark_query) -%}
    {%- set watermark_value  = watermark_result.columns[0].values()[0] -%}

    {# Step 1: delete all existing versions for changed source PKs #}
    {% call statement('delete_changed') %}
      DELETE FROM {{ target_relation }}
      WHERE {{ target_bk_col }} IN (
        SELECT DISTINCT CAST({{ source_bk_col }} AS VARCHAR(40))
        FROM {{ source_table }}
        WHERE {{ synced_col }} > '{{ watermark_value }}'::TIMESTAMP_TZ
      )
    {% endcall %}

    {# Step 2: reinsert all versions from the model SELECT, filtered to changed PKs #}
    {% call statement('main') %}
      INSERT INTO {{ target_relation }}
      SELECT model_data.*
      FROM ({{ sql }}) model_data
      WHERE {{ target_bk_col }} IN (
        SELECT DISTINCT CAST({{ source_bk_col }} AS VARCHAR(40))
        FROM {{ source_table }}
        WHERE {{ synced_col }} > '{{ watermark_value }}'::TIMESTAMP_TZ
      )
    {% endcall %}

  {% endif %}

  {{ run_hooks(post_hooks) }}

  {{ return({'relations': [target_relation]}) }}

{% endmaterialization %}
