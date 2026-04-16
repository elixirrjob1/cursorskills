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
        _FIVETRAN_SYNCED,
        ROW_NUMBER() OVER (PARTITION BY STORE_ID ORDER BY _FIVETRAN_SYNCED DESC) AS LatestRank
    FROM {{ source('bronze_erp__dbo', 'STORES') }}
)

SELECT
    CAST(SHA2(COALESCE(CAST(STORE_ID AS VARCHAR), '#@#@#@#@#'), 256) AS BINARY(32))    AS store_hash_pk,
    CAST(SHA2(COALESCE(CAST(CODE AS VARCHAR), '#@#@#@#@#'), 256) AS BINARY(32))        AS store_hash_bk,
    NAME                                                                                AS store_name,
    CAST(NULL AS VARCHAR(20))                                                           AS store_type,          -- not available in source
    ADDRESS                                                                             AS street_address,
    CITY                                                                                AS city,
    STATE                                                                               AS state_province,
    POSTAL_CODE                                                                         AS postal_code,
    CAST(NULL AS VARCHAR(50))                                                           AS country,             -- not available in source
    CAST(NULL AS DECIMAL(9,6))                                                          AS latitude,            -- not available in source
    CAST(NULL AS DECIMAL(9,6))                                                          AS longitude,           -- not available in source
    CAST(NULL AS VARCHAR(10))                                                           AS district_code,       -- not available in source
    CAST(NULL AS VARCHAR(50))                                                           AS district_name,       -- not available in source
    CAST(NULL AS VARCHAR(10))                                                           AS region_code,         -- not available in source
    CAST(NULL AS VARCHAR(50))                                                           AS region_name,         -- not available in source
    CAST(NULL AS VARCHAR(100))                                                          AS store_manager,       -- not available in source
    CAST(NULL AS DATE)                                                                  AS open_date,           -- not available in source
    CAST(NULL AS DATE)                                                                  AS close_date,          -- not available in source
    CAST(NULL AS INT)                                                                   AS square_footage,      -- not available in source
    CAST(NULL AS BOOLEAN)                                                               AS is_active,           -- not available in source
    CAST(0 AS INT)                                                                      AS etl_batch_id,        -- not available in source
    CURRENT_TIMESTAMP()                                                                 AS load_timestamp
FROM cteSTORES
WHERE LatestRank = 1
