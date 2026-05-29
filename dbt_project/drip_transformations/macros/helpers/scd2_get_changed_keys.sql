-- =============================================================================
-- scd2_get_changed_keys
-- =============================================================================
-- Returns a SQL subquery that yields the distinct set of business key values
-- whose source records have been created or changed since the last run.
--
-- Change detection is hash-based: compares SHA2 of business attribute columns
-- in the source against the stored hashbytes in the current target rows.
-- This removes the Fivetran _fivetran_synced dependency (resolves [G1]).
--
-- A LEFT JOIN approach handles both cases:
--   - New record  : tgt.target_bk_col IS NULL  (no matching row in target yet)
--   - Changed record: source hash != stored target hashbytes
--
-- Parameters:
--   source_table    — the dbt source() relation for the raw/staging table
--   source_bk_col   — business key column name in the source table
--   target_relation — the existing target table relation
--   target_bk_col   — business key column name in the target table
--   hash_cols       — list of source column names to include in the hash
--   is_current_col  — column flagging the active row in target (default: 'is_current')
--   hashbytes_col   — column storing the row hash in target (default: 'hashbytes')
--
-- Returns: SQL string (subquery, no trailing semicolon)
--
-- TODO: [G2] SHA2 / CONCAT_WS are Snowflake syntax.
--       Replace with adapter.dispatch('scd2_hash_expr') for other adapters.
-- =============================================================================

{% macro scd2_get_changed_keys(source_table, source_bk_col, target_relation, target_bk_col, hash_cols, is_current_col, hashbytes_col) %}

  SELECT DISTINCT CAST(src.{{ source_bk_col }} AS VARCHAR)
  FROM {{ source_table }} src
  LEFT JOIN {{ target_relation }} tgt
    ON  CAST(src.{{ source_bk_col }} AS VARCHAR) = CAST(tgt.{{ target_bk_col }} AS VARCHAR)
    AND tgt.{{ is_current_col }} = TRUE
  WHERE tgt.{{ target_bk_col }} IS NULL
     OR SHA2(CONCAT_WS('|',
          {%- for col in hash_cols %}
          src.{{ col }}{% if not loop.last %},{% endif %}
          {%- endfor %}
        )) != tgt.{{ hashbytes_col }}

{% endmacro %}
