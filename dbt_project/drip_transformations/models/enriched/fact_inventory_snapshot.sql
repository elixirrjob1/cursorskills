{{ config(
    materialized='incremental',
    unique_key='inventory_snapshot_hash_pk'
) }}

SELECT * FROM {{ ref('vw_interim_fact_inventory_snapshot_erp') }}

{% if is_incremental() %}
WHERE load_timestamp > (SELECT MAX(load_timestamp) FROM {{ this }})
{% endif %}
