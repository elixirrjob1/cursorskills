{#
    Generic dbt test: source_not_in_target

    Verifies that every value of the supplied PK column in the SOURCE model
    is also present in the TARGET model. Fails (returns rows) for any source
    PK value that does not exist in the target.

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
                - source_not_in_target:
                    target_model: "{{ ref('DimSupplier') }}"

    Notes
    -----
    - Designed to run on the SOURCE side: attach to the *_HashPK column of
      the vw_* view so dbt fills in `model` and `column_name` automatically.
      Only `target_model` is required as an explicit argument.
    - The query returns the unmatched source PK values; dbt will mark the
      test as failed whenever this set is non-empty.
#}

{% test source_not_in_target(model, column_name, target_model, target_column=none) %}

    {%- set tgt_col = target_column if target_column is not none else column_name -%}

    select
        src.{{ column_name }} as missing_pk
    from {{ model }} as src
    left join {{ target_model }} as tgt
        on src.{{ column_name }} = tgt.{{ tgt_col }}
    where tgt.{{ tgt_col }} is null

{% endtest %}
