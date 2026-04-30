{{ config(
    materialized='incremental',
    unique_key='"ProductHashPK"',
    incremental_strategy='delete+insert',
    pre_hook=[
        "DELETE FROM {{ this }}
         WHERE \"SourceProductPK\" IN (
             SELECT DISTINCT CAST(PRODUCT_ID AS VARCHAR(40))
             FROM {{ source('bronze_erp__dbo', 'PRODUCTS') }}
             WHERE _FIVETRAN_SYNCED > (
                 SELECT COALESCE(MAX(\"StageInsertedDateTimeUTC\"), '1900-01-01'::TIMESTAMP_TZ)
                 FROM {{ this }}
             )
         )"
    ]
) }}

-- SCD Type 2 incremental: delete + reinsert pattern.
-- See DimCustomer.sql for full explanation of the strategy.
-- Watermark: StageInsertedDateTimeUTC (= _FIVETRAN_SYNCED on PRODUCTS source).

SELECT v.*
FROM {{ ref('vw_DimProduct') }} v
WHERE v."SourceProductPK" IN (
    SELECT DISTINCT CAST(PRODUCT_ID AS VARCHAR(40))
    FROM {{ source('bronze_erp__dbo', 'PRODUCTS') }}
    WHERE _FIVETRAN_SYNCED > (
        {% if is_incremental() %}
            (SELECT COALESCE(MAX("StageInsertedDateTimeUTC"), '1900-01-01'::TIMESTAMP_TZ) FROM {{ this }})
        {% else %}
            '1900-01-01'::TIMESTAMP_TZ
        {% endif %}
    )
)
