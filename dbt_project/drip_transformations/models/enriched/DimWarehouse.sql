{{ config(
    materialized='incremental',
    unique_key='"WarehouseHashPK"'
) }}

SELECT * FROM {{ ref('vw_DimWarehouse') }}

{% if is_incremental() %}
WHERE "LoadTimestamp" > (SELECT MAX("LoadTimestamp") FROM {{ this }})
{% endif %}
