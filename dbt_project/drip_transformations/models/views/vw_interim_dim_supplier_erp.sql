{{ config(materialized='view') }}

WITH cteSUPPLIERS AS (
    SELECT
        SUPPLIER_ID,
        NAME,
        CONTACT_NAME,
        EMAIL,
        PHONE,
        _FIVETRAN_SYNCED,
        ROW_NUMBER() OVER (PARTITION BY SUPPLIER_ID ORDER BY _FIVETRAN_SYNCED DESC) AS LatestRank
    FROM {{ source('bronze_erp__dbo', 'SUPPLIERS') }}
)

SELECT
    CAST(SHA2(COALESCE(CAST(SUPPLIER_ID AS VARCHAR), '#@#@#@#@#'), 256) AS BINARY(32))
        AS supplier_hash_pk,

    CAST(SHA2(COALESCE(CAST(SUPPLIER_ID AS VARCHAR), '#@#@#@#@#'), 256) AS BINARY(32))
        AS supplier_hash_bk,

    CAST(SHA2(CONCAT_WS('|',
        COALESCE(CAST(NAME AS VARCHAR), '#@#@#@#@#'),
        COALESCE(CAST(CONTACT_NAME AS VARCHAR), '#@#@#@#@#'),
        COALESCE(CAST(EMAIL AS VARCHAR), '#@#@#@#@#'),
        COALESCE(CAST(PHONE AS VARCHAR), '#@#@#@#@#')
    ), 256) AS BINARY(32))
        AS hash_diff,

    NAME                                AS supplier_name,
    CAST(NULL AS VARCHAR(100))          AS supplier_dba_name,            -- not available in source
    CONTACT_NAME                        AS contact_name,
    EMAIL                               AS contact_email,
    PHONE                               AS contact_phone,
    CAST('Unknown' AS VARCHAR(200))     AS street_address,              -- not available in source
    CAST('Unknown' AS VARCHAR(50))      AS city,                        -- not available in source
    CAST('Unknown' AS VARCHAR(50))      AS state_province,              -- not available in source
    CAST('Unknown' AS VARCHAR(20))      AS postal_code,                 -- not available in source
    CAST('Unknown' AS VARCHAR(50))      AS country,                     -- not available in source
    CAST('Unknown' AS VARCHAR(10))      AS payment_terms_code,          -- not available in source
    CAST('Unknown' AS VARCHAR(50))      AS payment_terms_description,   -- not available in source
    CAST(0 AS INT)                      AS payment_terms_days,          -- not available in source
    CAST(0 AS INT)                      AS lead_time_days,              -- not available in source
    CAST(NULL AS DECIMAL(19,4))         AS minimum_order_amount,        -- not available in source
    CAST(TRUE AS BOOLEAN)               AS is_active,                   -- not available in source
    CAST(FALSE AS BOOLEAN)              AS is_preferred,                -- not available in source
    CAST(0 AS INT)                      AS etl_batch_id,                -- not available in source
    CURRENT_TIMESTAMP()                 AS load_timestamp               -- not available in source

FROM cteSUPPLIERS
WHERE LatestRank = 1
