{{ config(
    materialized='incremental',
    unique_key='ProductHashPK'
) }}

SELECT * FROM {{ ref('vw_DimProduct') }}

{% if is_incremental() %}
WHERE LoadTimestamp > (SELECT MAX(LoadTimestamp) FROM {{ this }})
{% endif %}
