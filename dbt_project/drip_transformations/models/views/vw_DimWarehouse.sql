{{ config(materialized='view') }}

WITH cteWAREHOUSE_SEED AS (
    SELECT
        warehouse_code
    FROM (VALUES
        ('UNKNOWN')
    ) AS seed(warehouse_code)
)

SELECT
    HASH(COALESCE(CAST(warehouse_code AS VARCHAR), '#@#@#@#@#')) AS "WarehouseHashPK",
    HASH(COALESCE(CAST(warehouse_code AS VARCHAR), '#@#@#@#@#')) AS "WarehouseHashBK",
    CAST(NULL AS VARCHAR(100)) AS "WarehouseName", -- not available in source
    CAST(NULL AS VARCHAR(20)) AS "WarehouseType", -- not available in source
    CAST(NULL AS VARCHAR(200)) AS "StreetAddress", -- not available in source
    CAST(NULL AS VARCHAR(50)) AS "City", -- not available in source
    CAST(NULL AS VARCHAR(50)) AS "StateProvince", -- not available in source
    CAST(NULL AS VARCHAR(20)) AS "PostalCode", -- not available in source
    CAST(NULL AS VARCHAR(50)) AS "Country", -- not available in source
    CAST(NULL AS VARCHAR(10)) AS "DistrictCode", -- not available in source
    CAST(NULL AS VARCHAR(50)) AS "DistrictName", -- not available in source
    CAST(NULL AS VARCHAR(10)) AS "RegionCode", -- not available in source
    CAST(NULL AS VARCHAR(50)) AS "RegionName", -- not available in source
    CAST(NULL AS INT) AS "TotalCapacityUnits", -- not available in source
    CAST(NULL AS BOOLEAN) AS "IsActive", -- not available in source
    CAST(NULL AS BINARY(32)) AS "Hashbytes", -- no source-mapped attributes; nothing to detect changes on
    CAST(0 AS INT) AS "EtlBatchId", -- not available in source
    '1900-01-01'::TIMESTAMP_NTZ AS "LoadTimestamp"
FROM cteWAREHOUSE_SEED
