{{ config(
    materialized='incremental',
    unique_key='StoreHashPK'
) }}

SELECT * FROM {{ ref('vw_DimStore') }}

{% if is_incremental() %}
WHERE LoadTimestamp > (SELECT MAX(LoadTimestamp) FROM {{ this }})
{% endif %}
