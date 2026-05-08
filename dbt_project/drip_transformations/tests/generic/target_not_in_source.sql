{#
    Generic dbt test: target_not_in_source

    Verifies that every value of the supplied PK column in the TARGET model
    is also present in the SOURCE model. Fails (returns rows) for any target
    PK value that does not exist in the source (orphaned/stale rows).

    Arguments
    ---------
    model         : the source model (passed implicitly by dbt when the test
                    is attached to a model/source/view in schema.yml).
    column_name   : the source PK column (passed implicitly by dbt when the
                    test is attached to a column in schema.yml).
    target_model  : the target model to compare against. Pass it as a ref(),
                    e.g. target_model: "{{ ref('DimSupplier') }}".
    target_column : optional. Name of the PK column in the target model.
                    Defaults to column_name when source and target use the
                    same PK column name (the convention in this project).

    Example usage (in schema.yml, alongside the source view):

        - name: vw_DimSupplier
          columns:
            - name: SupplierHashPK
              tests:
                - target_not_in_source:
                    arguments:
                      target_model: "{{ ref('DimSupplier') }}"

    Notes
    -----
    - Designed to run on the SOURCE side: attach to the *_HashPK column of
      the vw_* view so dbt fills in `model` and `column_name` automatically.
      Only `target_model` is required as an explicit argument.
    - The query returns the unmatched target PK values; dbt will mark the
      test as failed whenever this set is non-empty (rows in target with no
      corresponding source record).
#}

{% test target_not_in_source(model, column_name, target_model, target_column=none) %}

    {%- set tgt_col = target_column if target_column is not none else column_name -%}
    {%- set src_col_q = adapter.quote(column_name) -%}
    {%- set tgt_col_q = adapter.quote(tgt_col) -%}

    select
        tgt.{{ tgt_col_q }} as missing_pk
    from {{ target_model }} as tgt
    left join {{ model }} as src
        on tgt.{{ tgt_col_q }} = src.{{ src_col_q }}
    where src.{{ src_col_q }} is null

{% endtest %}
