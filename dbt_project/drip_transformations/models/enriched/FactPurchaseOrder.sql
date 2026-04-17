{{ config(
    materialized='incremental',
    unique_key='PurchaseOrderHashPK'
) }}

SELECT * FROM {{ ref('vw_FactPurchaseOrder') }}

{% if is_incremental() %}
WHERE LoadTimestamp > (SELECT MAX(LoadTimestamp) FROM {{ this }})
{% endif %}
