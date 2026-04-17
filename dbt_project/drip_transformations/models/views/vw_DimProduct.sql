{{ config(materialized='view') }}

WITH ctePRODUCTS AS (
    SELECT
        PRODUCT_ID,
        SKU,
        NAME,
        PRODUCT_DESCRIPTION,
        CATEGORY,
        COST_PRICE,
        UNIT_PRICE,
        ACTIVE,
        _FIVETRAN_SYNCED
    FROM {{ source('bronze_erp__dbo', 'PRODUCTS') }}
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY PRODUCT_ID
        ORDER BY _FIVETRAN_SYNCED DESC
    ) = 1
)

SELECT
    CAST(SHA2(COALESCE(CAST(PRODUCT_ID AS VARCHAR), '#@#@#@#@#'), 256) AS BINARY(32)) AS ProductHashPK,
    CAST(SHA2(COALESCE(CAST(SKU AS VARCHAR), '#@#@#@#@#'), 256) AS BINARY(32)) AS ProductHashBK,
    TRIM(NAME) AS ProductName,
    TRIM(PRODUCT_DESCRIPTION) AS ProductDescription,
    CAST(NULL AS VARCHAR(10)) AS CategoryCode, -- not available in source
    TRIM(CATEGORY) AS CategoryName,
    CAST(NULL AS VARCHAR(10)) AS SubcategoryCode, -- not available in source
    CAST(NULL AS VARCHAR(50)) AS SubcategoryName, -- not available in source
    CAST(NULL AS VARCHAR(10)) AS BrandCode, -- not available in source
    CAST(NULL AS VARCHAR(50)) AS BrandName, -- not available in source
    CAST(NULL AS VARCHAR(20)) AS UnitOfMeasure, -- not available in source
    CAST(NULL AS VARCHAR(20)) AS PackSize, -- not available in source
    COST_PRICE AS UnitCost,
    UNIT_PRICE AS UnitListPrice,
    ACTIVE AS IsActive,
    IFF(ACTIVE, FALSE, TRUE) AS IsDiscontinued,
    CAST(NULL AS DATE) AS EffectiveDate, -- not available in source
    CAST(NULL AS DATE) AS ExpirationDate, -- not available in source
    CAST(NULL AS BOOLEAN) AS IsCurrent, -- not available in source
    SHA2(
        COALESCE(CAST(NAME AS VARCHAR), '#@#@#@#@#')
        || '|' || COALESCE(CAST(PRODUCT_DESCRIPTION AS VARCHAR), '#@#@#@#@#')
        || '|' || COALESCE(CAST(CATEGORY AS VARCHAR), '#@#@#@#@#')
        || '|' || COALESCE(CAST(COST_PRICE AS VARCHAR), '#@#@#@#@#')
        || '|' || COALESCE(CAST(UNIT_PRICE AS VARCHAR), '#@#@#@#@#')
        || '|' || COALESCE(CAST(ACTIVE AS VARCHAR), '#@#@#@#@#')
    , 256) AS Hashbytes,
    CAST(0 AS INT) AS EtlBatchId, -- not available in source
    _FIVETRAN_SYNCED AS LoadTimestamp
FROM ctePRODUCTS
