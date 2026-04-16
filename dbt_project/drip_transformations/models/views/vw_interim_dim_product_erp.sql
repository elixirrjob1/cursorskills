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
        ROW_NUMBER() OVER (
            PARTITION BY PRODUCT_ID
            ORDER BY _FIVETRAN_SYNCED DESC
        ) AS LatestRank
    FROM {{ source('bronze_erp__dbo', 'PRODUCTS') }}
)

SELECT
    CAST(SHA2(COALESCE(CAST(PRODUCT_ID AS VARCHAR), '#@#@#@#@#'), 256) AS BINARY(32))
        AS product_hash_pk,
    CAST(SHA2(COALESCE(CAST(SKU AS VARCHAR), '#@#@#@#@#'), 256) AS BINARY(32))
        AS product_hash_bk,
    TRIM(NAME)                          AS product_name,
    TRIM(PRODUCT_DESCRIPTION)           AS product_description,
    CAST(NULL AS VARCHAR(10))           AS category_code,        -- not available in source
    TRIM(CATEGORY)                      AS category_name,
    CAST(NULL AS VARCHAR(10))           AS subcategory_code,     -- not available in source
    CAST(NULL AS VARCHAR(50))           AS subcategory_name,     -- not available in source
    CAST(NULL AS VARCHAR(10))           AS brand_code,           -- not available in source
    CAST(NULL AS VARCHAR(50))           AS brand_name,           -- not available in source
    CAST(NULL AS VARCHAR(20))           AS unit_of_measure,      -- not available in source
    CAST(NULL AS VARCHAR(20))           AS pack_size,            -- not available in source
    COST_PRICE                          AS unit_cost,
    UNIT_PRICE                          AS unit_list_price,
    ACTIVE                              AS is_active,
    IFF(ACTIVE, FALSE, TRUE)            AS is_discontinued,
    CAST(NULL AS DATE)                  AS effective_date,       -- not available in source
    CAST(NULL AS DATE)                  AS expiration_date,      -- not available in source
    CAST(NULL AS BOOLEAN)               AS is_current,           -- not available in source
    CAST(NULL AS INT)                   AS etl_batch_id,         -- not available in source
    CURRENT_TIMESTAMP()                 AS load_timestamp
FROM ctePRODUCTS
WHERE LatestRank = 1
