{{ config(
    materialized='incremental',
    unique_key='product_hash_pk'
) }}

SELECT * FROM {{ ref('vw_interim_dim_product_erp') }}

{% if is_incremental() %}
WHERE load_timestamp > (SELECT MAX(load_timestamp) FROM {{ this }})
{% endif %}
