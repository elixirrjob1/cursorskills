{{ config(
    materialized='table'
) }}

-- SCD Type 2: full refresh from the view, which rebuilds the complete version
-- history on every run. Incremental is unsafe for Type 2 because closing out
-- old versions requires re-evaluating LEAD(EffectiveStartDateTime) across the
-- whole partition.
SELECT * FROM {{ ref('vw_DimCustomer') }}
