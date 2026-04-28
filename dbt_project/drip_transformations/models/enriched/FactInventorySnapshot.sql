{{ config(
    materialized='incremental',
    unique_key='"InventorySnapshotHashPK"'
) }}

SELECT * FROM {{ ref('vw_FactInventorySnapshot') }}

{% if is_incremental() %}
WHERE "LoadTimestamp" > (SELECT MAX("LoadTimestamp") FROM {{ this }})
{% endif %}
