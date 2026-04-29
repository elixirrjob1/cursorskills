{#
    Generic dbt test: source_hashdiff_matches_target

    Fails when SOURCE and TARGET are not aligned by PK + hashdiff:
    - PK exists only in source
    - PK exists only in target
    - PK exists in both but hashdiff differs

    Operational usage
    -----------------
    1) Run test (dbt prints PASS/FAIL/WARN/ERROR/SKIP summary):
       dbt test --select "test_name:source_hashdiff_matches_target" --target dev --store-failures

    2) If failures occur, dbt prints failure table names in DBT_DEV_dbt_test__audit.
       Query one example row per failed table to investigate quickly.

    Cursor prompt template
    ----------------------
    "Run source_hashdiff_matches_target locally in dev, then return:
     - per-table status/counts
     - one example mismatch row for each failed table
       (PK, source hash, target hash, failure_reason, source_row, target_row)."
#}

{% test source_hashdiff_matches_target(
    model,
    column_name,
    target_model,
    source_hashdiff_column='Hashbytes',
    target_hashdiff_column='Hashbytes',
    target_pk_column=none
) %}

    {{ config(store_failures = true) }}

    {%- set tgt_pk = target_pk_column if target_pk_column is not none else column_name -%}
    {%- set src_pk_q = adapter.quote(column_name) -%}
    {%- set tgt_pk_q = adapter.quote(tgt_pk) -%}
    {%- set src_hash_q = adapter.quote(source_hashdiff_column) -%}
    {%- set tgt_hash_q = adapter.quote(target_hashdiff_column) -%}

    with source_pairs as (
        select
            src.{{ src_pk_q }} as pk_value,
            src.{{ src_hash_q }} as hash_value
        from {{ model }} as src
    ),
    target_pairs as (
        select
            tgt.{{ tgt_pk_q }} as pk_value,
            tgt.{{ tgt_hash_q }} as hash_value
        from {{ target_model }} as tgt
    ),
    source_minus_target as (
        select pk_value, hash_value
        from source_pairs
        except
        select pk_value, hash_value
        from target_pairs
    ),
    target_minus_source as (
        select pk_value, hash_value
        from target_pairs
        except
        select pk_value, hash_value
        from source_pairs
    ),
    mismatches as (
        select pk_value from source_minus_target
        union
        select pk_value from target_minus_source
    )
    select
        '{{ model.identifier }}' as source_table,
        '{{ target_model.identifier }}' as target_table,
        '{{ column_name }}' as source_pk_column,
        '{{ tgt_pk }}' as target_pk_column,
        mm.pk_value,
        src.{{ src_hash_q }} as source_hashdiff,
        tgt.{{ tgt_hash_q }} as target_hashdiff,
        case
            when src.{{ src_pk_q }} is null then 'missing_in_source'
            when tgt.{{ tgt_pk_q }} is null then 'missing_in_target'
            else 'hashdiff_mismatch'
        end as failure_reason,
        OBJECT_CONSTRUCT_KEEP_NULL(src.*) as source_row,
        OBJECT_CONSTRUCT_KEEP_NULL(tgt.*) as target_row
    from mismatches as mm
    left join {{ model }} as src
        on src.{{ src_pk_q }} = mm.pk_value
    left join {{ target_model }} as tgt
        on tgt.{{ tgt_pk_q }} = mm.pk_value

{% endtest %}
