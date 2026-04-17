{{ config(
    materialized='incremental',
    unique_key='CustomerHashPK'
) }}

SELECT * FROM {{ ref('vw_DimCustomer') }}

{% if is_incremental() %}
WHERE LoadTimestamp > (SELECT MAX(LoadTimestamp) FROM {{ this }})
{% endif %}
