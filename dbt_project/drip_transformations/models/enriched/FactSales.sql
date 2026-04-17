{{ config(
    materialized='incremental',
    unique_key='SalesHashPK'
) }}

SELECT * FROM {{ ref('vw_FactSales') }}

{% if is_incremental() %}
WHERE LoadTimestamp > (SELECT MAX(LoadTimestamp) FROM {{ this }})
{% endif %}
