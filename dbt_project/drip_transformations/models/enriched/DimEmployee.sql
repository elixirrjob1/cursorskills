{{ config(
    materialized='incremental',
    unique_key='"EmployeeHashPK"'
) }}

SELECT * FROM {{ ref('vw_DimEmployee') }}

{% if is_incremental() %}
WHERE "LoadTimestamp" > (SELECT MAX("LoadTimestamp") FROM {{ this }})
{% endif %}
