-- =============================================================================
-- scd2_insert_fresh
-- =============================================================================
-- Generates the INSERT statement that copies all rows from the temp table
-- into the target using an explicit column list.
--
-- WHY explicit columns (not SELECT *):
--   The column list is derived from the temp table's actual columns via
--   INFORMATION_SCHEMA. This ensures:
--     1. Column order follows what the model (and by extension scd2_conventions.yml)
--        defines — not what the target table's physical order happens to be.
--     2. Extra columns in the target (not produced by the model) are safely
--        skipped — they retain their existing values or defaults.
--     3. The INSERT is unambiguous regardless of target table DDL changes.
--
-- No WHERE filter is needed here because the temp table was already built
-- with only the changed PKs' data. Every row in temp belongs in target.
--
-- Parameters:
--   target_relation — the permanent target table relation
--   tmp_relation    — the temporary staging table relation
--
-- Returns: SQL string (INSERT statement, no trailing semicolon)
-- =============================================================================

{% macro scd2_insert_fresh(target_relation, tmp_relation) %}

  {%- if execute -%}

    {%- set col_query -%}
      SELECT COLUMN_NAME
      FROM INFORMATION_SCHEMA.COLUMNS
      WHERE TABLE_SCHEMA = UPPER('{{ tmp_relation.schema }}')
        AND TABLE_NAME   = UPPER('{{ tmp_relation.identifier }}')
      ORDER BY ORDINAL_POSITION
    {%- endset -%}

    {%- set col_result = run_query(col_query) -%}
    {%- set col_names  = col_result.columns[0].values() -%}
    {%- set col_list   = col_names | join(', ') -%}

    INSERT INTO {{ target_relation }} ({{ col_list }})
    SELECT {{ col_list }} FROM {{ tmp_relation }}

  {%- else -%}

    INSERT INTO {{ target_relation }}
    SELECT * FROM {{ tmp_relation }}

  {%- endif -%}

{% endmacro %}
