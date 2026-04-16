{{ config(
    materialized='incremental',
    unique_key='warehouse_hash_pk'
) }}

SELECT * FROM {{ ref('vw_interim_dim_warehouse_erp') }}

{% if is_incremental() %}
WHERE load_timestamp > (SELECT MAX(load_timestamp) FROM {{ this }})
{% endif %}
