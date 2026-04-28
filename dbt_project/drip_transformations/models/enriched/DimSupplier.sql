{{ config(
    materialized='incremental',
    unique_key='"SupplierHashPK"'
) }}

SELECT * FROM {{ ref('vw_DimSupplier') }}

{% if is_incremental() %}
WHERE "LoadTimestamp" > (SELECT MAX("LoadTimestamp") FROM {{ this }})
{% endif %}
