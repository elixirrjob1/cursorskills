{{ config(
    materialized='incremental',
    unique_key='customer_hash_pk'
) }}

SELECT * FROM {{ ref('vw_interim_dim_customer_erp') }}

{% if is_incremental() %}
WHERE load_timestamp > (SELECT MAX(load_timestamp) FROM {{ this }})
{% endif %}
