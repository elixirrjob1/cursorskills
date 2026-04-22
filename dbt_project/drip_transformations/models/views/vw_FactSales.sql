{{ config(materialized='view') }}

WITH cteSALES_ORDER_ITEMS AS (
    SELECT
        SALES_ORDER_ID,
        SALES_ORDER_ITEM_ID,
        PRODUCT_ID,
        QUANTITY,
        UNIT_PRICE,
        _FIVETRAN_SYNCED
    FROM {{ source('bronze_erp__dbo', 'SALES_ORDER_ITEMS') }}
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY SALES_ORDER_ID, SALES_ORDER_ITEM_ID
        ORDER BY _FIVETRAN_SYNCED DESC
    ) = 1
),

cteSALES_ORDERS AS (
    SELECT
        SALES_ORDER_ID,
        ORDER_DATE,
        STORE_ID,
        CUSTOMER_ID,
        EMPLOYEE_ID,
        _FIVETRAN_SYNCED
    FROM {{ source('bronze_erp__dbo', 'SALES_ORDERS') }}
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY SALES_ORDER_ID
        ORDER BY _FIVETRAN_SYNCED DESC
    ) = 1
),

ctePRODUCTS AS (
    SELECT
        PRODUCT_ID,
        COST_PRICE,
        _FIVETRAN_SYNCED
    FROM {{ source('bronze_erp__dbo', 'PRODUCTS') }}
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY PRODUCT_ID
        ORDER BY _FIVETRAN_SYNCED DESC
    ) = 1
)

SELECT
    HASH(COALESCE(CAST(cteSALES_ORDER_ITEMS.SALES_ORDER_ID AS VARCHAR), '#@#@#@#@#')
        || '|' || COALESCE(CAST(SALES_ORDER_ITEM_ID AS VARCHAR), '#@#@#@#@#')) AS SalesHashPK,
    HASH(COALESCE(CAST(CAST(ORDER_DATE AS DATE) AS VARCHAR), '#@#@#@#@#')) AS DateHashFK,
    HASH(COALESCE(CAST(cteSALES_ORDER_ITEMS.PRODUCT_ID AS VARCHAR), '#@#@#@#@#')) AS ProductHashFK,
    HASH(COALESCE(CAST(STORE_ID AS VARCHAR), '#@#@#@#@#')) AS StoreHashFK,
    IFF(CUSTOMER_ID IS NULL, NULL, HASH(COALESCE(CAST(CUSTOMER_ID AS VARCHAR), '#@#@#@#@#'))) AS CustomerHashFK,
    IFF(EMPLOYEE_ID IS NULL, NULL, HASH(COALESCE(CAST(EMPLOYEE_ID AS VARCHAR), '#@#@#@#@#'))) AS EmployeeHashFK,
    CAST(cteSALES_ORDER_ITEMS.SALES_ORDER_ID AS VARCHAR(20)) AS TransactionNumber,
    SALES_ORDER_ITEM_ID AS TransactionLineNumber,
    CAST(NULL AS VARCHAR(10)) AS TransactionType, -- not available in source
    QUANTITY AS Quantity,
    UNIT_PRICE AS UnitPrice,
    COST_PRICE AS UnitCost,
    CAST(QUANTITY * UNIT_PRICE AS DECIMAL(19,4)) AS GrossAmount,
    CAST(NULL AS DECIMAL(19,4)) AS DiscountAmount, -- not available in source
    CAST(NULL AS DECIMAL(19,4)) AS TaxAmount, -- not available in source
    CAST(NULL AS DECIMAL(19,4)) AS NetAmount, -- not available in source
    CAST(QUANTITY * COST_PRICE AS DECIMAL(19,4)) AS CostAmount,
    CAST(NULL AS DECIMAL(19,4)) AS ProfitAmount, -- not available in source
    CAST(NULL AS BOOLEAN) AS IsPromotion, -- not available in source
    CAST(NULL AS VARCHAR(20)) AS PromotionCode, -- not available in source
    CAST(NULL AS VARCHAR(20)) AS PaymentMethod, -- not available in source
    CAST(SHA2_BINARY(
        COALESCE(CAST(cteSALES_ORDER_ITEMS.SALES_ORDER_ID AS VARCHAR), '#@#@#@#@#')
        || '|' || COALESCE(CAST(SALES_ORDER_ITEM_ID AS VARCHAR), '#@#@#@#@#')
        || '|' || COALESCE(CAST(QUANTITY AS VARCHAR), '#@#@#@#@#')
        || '|' || COALESCE(CAST(UNIT_PRICE AS VARCHAR), '#@#@#@#@#')
        || '|' || COALESCE(CAST(COST_PRICE AS VARCHAR), '#@#@#@#@#')
        || '|' || COALESCE(CAST(CAST(QUANTITY * UNIT_PRICE AS DECIMAL(19,4)) AS VARCHAR), '#@#@#@#@#')
        || '|' || COALESCE(CAST(CAST(QUANTITY * COST_PRICE AS DECIMAL(19,4)) AS VARCHAR), '#@#@#@#@#')
    , 256) AS BINARY(32)) AS Hashbytes,
    CAST(0 AS INT) AS EtlBatchId, -- not available in source
    cteSALES_ORDER_ITEMS._FIVETRAN_SYNCED AS LoadTimestamp
FROM cteSALES_ORDER_ITEMS
LEFT JOIN cteSALES_ORDERS
    ON cteSALES_ORDER_ITEMS.SALES_ORDER_ID = cteSALES_ORDERS.SALES_ORDER_ID
LEFT JOIN ctePRODUCTS
    ON cteSALES_ORDER_ITEMS.PRODUCT_ID = ctePRODUCTS.PRODUCT_ID
