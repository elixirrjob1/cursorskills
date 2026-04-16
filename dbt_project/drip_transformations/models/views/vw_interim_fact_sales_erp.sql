{{ config(materialized='view') }}

WITH cteSALES_ORDER_ITEMS AS (
    SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY SALES_ORDER_ID, SALES_ORDER_ITEM_ID ORDER BY _FIVETRAN_SYNCED DESC) AS LatestRank
    FROM {{ source('bronze_erp__dbo', 'SALES_ORDER_ITEMS') }}
),

cteSALES_ORDERS AS (
    SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY SALES_ORDER_ID ORDER BY _FIVETRAN_SYNCED DESC) AS LatestRank
    FROM {{ source('bronze_erp__dbo', 'SALES_ORDERS') }}
),

ctePRODUCTS AS (
    SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY PRODUCT_ID ORDER BY _FIVETRAN_SYNCED DESC) AS LatestRank
    FROM {{ source('bronze_erp__dbo', 'PRODUCTS') }}
)

SELECT
    CAST(SHA2(COALESCE(CAST(cteSALES_ORDER_ITEMS.SALES_ORDER_ID AS VARCHAR), '#@#@#@#@#') || '|' || COALESCE(CAST(SALES_ORDER_ITEM_ID AS VARCHAR), '#@#@#@#@#'), 256) AS BINARY(32))
                                                                            AS sales_hash_pk,

    CAST(SHA2(COALESCE(CAST(CAST(ORDER_DATE AS DATE) AS VARCHAR), '#@#@#@#@#'), 256) AS BINARY(32))
                                                                            AS date_hash_fk,

    CAST(SHA2(COALESCE(CAST(cteSALES_ORDER_ITEMS.PRODUCT_ID AS VARCHAR), '#@#@#@#@#'), 256) AS BINARY(32))
                                                                            AS product_hash_fk,

    CAST(SHA2(COALESCE(CAST(STORE_ID AS VARCHAR), '#@#@#@#@#'), 256) AS BINARY(32))
                                                                            AS store_hash_fk,

    IFF(CUSTOMER_ID IS NULL, NULL, CAST(SHA2(COALESCE(CAST(CUSTOMER_ID AS VARCHAR), '#@#@#@#@#'), 256) AS BINARY(32)))
                                                                            AS customer_hash_fk,

    IFF(EMPLOYEE_ID IS NULL, NULL, CAST(SHA2(COALESCE(CAST(EMPLOYEE_ID AS VARCHAR), '#@#@#@#@#'), 256) AS BINARY(32)))
                                                                            AS employee_hash_fk,

    CAST(cteSALES_ORDERS.SALES_ORDER_ID AS VARCHAR(20))                     AS transaction_number,
    SALES_ORDER_ITEM_ID                                                     AS transaction_line_number,
    CAST(NULL AS VARCHAR(10))                                               AS transaction_type,         -- not available in source
    QUANTITY                                                                AS quantity,
    UNIT_PRICE                                                              AS unit_price,
    COST_PRICE                                                              AS unit_cost,
    CAST(QUANTITY * UNIT_PRICE AS DECIMAL(19,4))                            AS gross_amount,
    CAST(NULL AS DECIMAL(19,4))                                             AS discount_amount,          -- not available in source
    CAST(NULL AS DECIMAL(19,4))                                             AS tax_amount,               -- not available in source
    CAST(NULL AS DECIMAL(19,4))                                             AS net_amount,               -- not available in source
    CAST(QUANTITY * COST_PRICE AS DECIMAL(19,4))                            AS cost_amount,
    CAST(NULL AS DECIMAL(19,4))                                             AS profit_amount,            -- not available in source
    CAST(NULL AS BOOLEAN)                                                   AS is_promotion,             -- not available in source
    CAST(NULL AS VARCHAR(20))                                               AS promotion_code,           -- not available in source
    CAST(NULL AS VARCHAR(20))                                               AS payment_method,           -- not available in source

    CAST(SHA2(
        COALESCE(CAST(CAST(ORDER_DATE AS DATE) AS VARCHAR), '#@#@#@#@#') || '|' ||
        COALESCE(CAST(cteSALES_ORDER_ITEMS.PRODUCT_ID AS VARCHAR), '#@#@#@#@#') || '|' ||
        COALESCE(CAST(STORE_ID AS VARCHAR), '#@#@#@#@#') || '|' ||
        COALESCE(CAST(CUSTOMER_ID AS VARCHAR), '#@#@#@#@#') || '|' ||
        COALESCE(CAST(EMPLOYEE_ID AS VARCHAR), '#@#@#@#@#') || '|' ||
        COALESCE(CAST(cteSALES_ORDERS.SALES_ORDER_ID AS VARCHAR), '#@#@#@#@#') || '|' ||
        COALESCE(CAST(SALES_ORDER_ITEM_ID AS VARCHAR), '#@#@#@#@#') || '|' ||
        COALESCE(CAST(QUANTITY AS VARCHAR), '#@#@#@#@#') || '|' ||
        COALESCE(CAST(UNIT_PRICE AS VARCHAR), '#@#@#@#@#') || '|' ||
        COALESCE(CAST(COST_PRICE AS VARCHAR), '#@#@#@#@#') || '|' ||
        COALESCE(CAST(QUANTITY * UNIT_PRICE AS VARCHAR), '#@#@#@#@#') || '|' ||
        COALESCE(CAST(QUANTITY * COST_PRICE AS VARCHAR), '#@#@#@#@#')
    , 256) AS BINARY(32))                                                   AS hashbytes,

    CAST(NULL AS INT)                                                       AS etl_batch_id,
    CURRENT_TIMESTAMP()                                                     AS load_timestamp

FROM cteSALES_ORDER_ITEMS
INNER JOIN cteSALES_ORDERS
    ON cteSALES_ORDER_ITEMS.SALES_ORDER_ID = cteSALES_ORDERS.SALES_ORDER_ID
INNER JOIN ctePRODUCTS
    ON cteSALES_ORDER_ITEMS.PRODUCT_ID = ctePRODUCTS.PRODUCT_ID
WHERE cteSALES_ORDER_ITEMS.LatestRank = 1
  AND cteSALES_ORDERS.LatestRank = 1
  AND ctePRODUCTS.LatestRank = 1
