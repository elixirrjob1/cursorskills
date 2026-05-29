-- =============================================================================
-- scd2_delete_stale
-- =============================================================================
-- Generates the DELETE statement that removes all target rows whose primary
-- key appears in the temp table.
--
-- Driving the DELETE from the temp table (not from the source) guarantees
-- that the set of deleted PKs exactly matches the set that will be reinserted,
-- even if the source changes between the build-temp and delete steps.
--
-- Parameters:
--   target_relation — the permanent target table relation
--   target_bk_col   — business key column name in the target
--   tmp_relation    — the temporary staging table relation
--
-- Returns: SQL string (DELETE statement, no trailing semicolon)
-- =============================================================================

{% macro scd2_delete_stale(target_relation, target_bk_col, tmp_relation) %}

  DELETE FROM {{ target_relation }}
  WHERE {{ target_bk_col }} IN (
    SELECT DISTINCT {{ target_bk_col }}
    FROM {{ tmp_relation }}
  )

{% endmacro %}
