{{ config(
    materialized='incremental',
    unique_key='sales_hash_pk'
) }}

SELECT * FROM {{ ref('vw_interim_fact_sales_erp') }}

{% if is_incremental() %}
WHERE load_timestamp > (SELECT MAX(load_timestamp) FROM {{ this }})
{% endif %}
