{{ config(
    materialized='incremental',
    unique_key='DateHashPK'
) }}

SELECT * FROM {{ ref('vw_DimDate') }}

{% if is_incremental() %}
WHERE LoadTimestamp > (SELECT MAX(LoadTimestamp) FROM {{ this }})
{% endif %}
