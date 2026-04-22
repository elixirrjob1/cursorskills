{{ config(materialized='view') }}

WITH cteSTORES AS (
    SELECT
        STORE_ID,
        CODE,
        NAME,
        ADDRESS,
        CITY,
        STATE,
        POSTAL_CODE,
        _FIVETRAN_SYNCED
    FROM {{ source('bronze_erp__dbo', 'STORES') }}
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY STORE_ID
        ORDER BY _FIVETRAN_SYNCED DESC
    ) = 1
)

SELECT
    CAST(SHA2_BINARY(COALESCE(CAST(STORE_ID AS VARCHAR), '#@#@#@#@#'), 256) AS BINARY(32)) AS StoreHashPK,
    CAST(SHA2_BINARY(COALESCE(CAST(CODE AS VARCHAR), '#@#@#@#@#'), 256) AS BINARY(32)) AS StoreHashBK,
    NAME AS StoreName,
    CAST(NULL AS VARCHAR(20)) AS StoreType, -- not available in source
    ADDRESS AS StreetAddress,
    CITY AS City,
    STATE AS StateProvince,
    POSTAL_CODE AS PostalCode,
    CAST(NULL AS VARCHAR(50)) AS Country, -- not available in source
    CAST(NULL AS DECIMAL(9,6)) AS Latitude, -- not available in source
    CAST(NULL AS DECIMAL(9,6)) AS Longitude, -- not available in source
    CAST(NULL AS VARCHAR(10)) AS DistrictCode, -- not available in source
    CAST(NULL AS VARCHAR(50)) AS DistrictName, -- not available in source
    CAST(NULL AS VARCHAR(10)) AS RegionCode, -- not available in source
    CAST(NULL AS VARCHAR(50)) AS RegionName, -- not available in source
    CAST(NULL AS VARCHAR(100)) AS StoreManager, -- not available in source
    CAST(NULL AS DATE) AS OpenDate, -- not available in source
    CAST(NULL AS DATE) AS CloseDate, -- not available in source
    CAST(NULL AS INT) AS SquareFootage, -- not available in source
    CAST(NULL AS BOOLEAN) AS IsActive, -- not available in source
    CAST(SHA2_BINARY(
        COALESCE(CAST(NAME AS VARCHAR), '#@#@#@#@#')
        || '|' || COALESCE(CAST(ADDRESS AS VARCHAR), '#@#@#@#@#')
        || '|' || COALESCE(CAST(CITY AS VARCHAR), '#@#@#@#@#')
        || '|' || COALESCE(CAST(STATE AS VARCHAR), '#@#@#@#@#')
        || '|' || COALESCE(CAST(POSTAL_CODE AS VARCHAR), '#@#@#@#@#')
    , 256) AS BINARY(32)) AS Hashbytes,
    CAST(0 AS INT) AS EtlBatchId, -- not available in source
    _FIVETRAN_SYNCED AS LoadTimestamp
FROM cteSTORES
