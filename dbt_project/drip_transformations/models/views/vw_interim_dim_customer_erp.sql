{{ config(materialized='view') }}

WITH cteCUSTOMERS AS (
    SELECT
        CUSTOMER_ID,
        FIRST_NAME,
        LAST_NAME,
        EMAIL,
        PHONE,
        CREATED_AT,
        ROW_NUMBER() OVER (
            PARTITION BY CUSTOMER_ID
            ORDER BY _FIVETRAN_SYNCED DESC
        ) AS LatestRank
    FROM {{ source('bronze_erp__dbo', 'CUSTOMERS') }}
)

SELECT
    CAST(SHA2(COALESCE(CAST(CUSTOMER_ID AS VARCHAR), '#@#@#@#@#'), 256) AS BINARY(32))
        AS customer_hash_pk,

    CAST(SHA2(COALESCE(CAST(CUSTOMER_ID AS VARCHAR), '#@#@#@#@#'), 256) AS BINARY(32))
        AS customer_hash_bk,

    FIRST_NAME                                  AS first_name,
    LAST_NAME                                   AS last_name,
    TRIM(FIRST_NAME) || ' ' || TRIM(LAST_NAME)  AS full_name,
    EMAIL                                       AS email_address,
    PHONE                                       AS phone_number,

    CAST(CREATED_AT AS DATE)                    AS acquisition_date,

    CAST(NULL AS VARCHAR(200))                  AS street_address,       -- not available in source
    CAST(NULL AS VARCHAR(50))                   AS city,                 -- not available in source
    CAST(NULL AS VARCHAR(50))                   AS state_province,       -- not available in source
    CAST(NULL AS VARCHAR(20))                   AS postal_code,          -- not available in source
    CAST(NULL AS VARCHAR(50))                   AS country,              -- not available in source
    CAST('Unknown' AS VARCHAR(20))              AS customer_type,        -- not available in source
    CAST('Unknown' AS VARCHAR(50))              AS acquisition_channel,  -- not available in source
    CAST('Unknown' AS VARCHAR(20))              AS loyalty_tier,         -- not available in source
    CAST(0 AS INT)                              AS loyalty_points,       -- not available in source
    CAST(NULL AS DATE)                          AS loyalty_join_date,    -- not available in source
    CAST(TRUE AS BOOLEAN)                       AS is_active,            -- not available in source

    CURRENT_DATE                                AS effective_date,       -- not available in source
    CAST(NULL AS DATE)                          AS expiration_date,      -- not available in source
    CAST(TRUE AS BOOLEAN)                       AS is_current,           -- not available in source
    CAST(0 AS INT)                              AS etl_batch_id,         -- not available in source
    CURRENT_TIMESTAMP                           AS load_timestamp,       -- not available in source

    CAST(SHA2(CONCAT_WS('|',
        COALESCE(CAST(FIRST_NAME AS VARCHAR), '#@#@#@#@#'),
        COALESCE(CAST(LAST_NAME AS VARCHAR), '#@#@#@#@#'),
        COALESCE(CAST(EMAIL AS VARCHAR), '#@#@#@#@#'),
        COALESCE(CAST(PHONE AS VARCHAR), '#@#@#@#@#'),
        COALESCE(CAST(CREATED_AT AS VARCHAR), '#@#@#@#@#')
    ), 256) AS BINARY(32))                      AS hash_diff

FROM cteCUSTOMERS
WHERE LatestRank = 1
