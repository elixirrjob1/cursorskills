{{ config(
    materialized='incremental',
    unique_key='purchase_order_hash_pk'
) }}

SELECT * FROM {{ ref('vw_interim_fact_purchase_order_erp') }}

{% if is_incremental() %}
WHERE load_timestamp > (SELECT MAX(load_timestamp) FROM {{ this }})
{% endif %}
