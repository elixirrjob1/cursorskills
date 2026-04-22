{{ config(materialized='view') }}

WITH ctePURCHASE_ORDER_ITEMS AS (
    SELECT
        PO_ITEM_ID,
        PO_ID,
        PRODUCT_ID,
        QUANTITY,
        UNIT_COST,
        _FIVETRAN_SYNCED
    FROM {{ source('bronze_erp__dbo', 'PURCHASE_ORDER_ITEMS') }}
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY PO_ITEM_ID
        ORDER BY _FIVETRAN_SYNCED DESC
    ) = 1
),

ctePURCHASE_ORDERS AS (
    SELECT
        PO_ID,
        SUPPLIER_ID,
        STORE_ID,
        ORDER_DATE,
        EXPECTED_DATE,
        STATUS,
        _FIVETRAN_SYNCED
    FROM {{ source('bronze_erp__dbo', 'PURCHASE_ORDERS') }}
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY PO_ID
        ORDER BY _FIVETRAN_SYNCED DESC
    ) = 1
)

SELECT
    HASH(COALESCE(CAST(ctePURCHASE_ORDER_ITEMS.PO_ID AS VARCHAR), '#@#@#@#@#') || '|' || COALESCE(CAST(PO_ITEM_ID AS VARCHAR), '#@#@#@#@#')) AS PurchaseOrderHashPK,
    HASH(COALESCE(CAST(PRODUCT_ID AS VARCHAR), '#@#@#@#@#')) AS ProductHashFK,
    HASH(COALESCE(CAST(SUPPLIER_ID AS VARCHAR), '#@#@#@#@#')) AS SupplierHashFK,
    HASH(COALESCE(CAST(STORE_ID AS VARCHAR), '#@#@#@#@#')) AS WarehouseHashFK,
    HASH(COALESCE(CAST(ORDER_DATE AS VARCHAR), '#@#@#@#@#')) AS DateOrderedHashFK,
    IFF(EXPECTED_DATE IS NULL, NULL, HASH(COALESCE(CAST(EXPECTED_DATE AS VARCHAR), '#@#@#@#@#'))) AS DateExpectedHashFK,
    CAST(NULL AS NUMBER(19,0)) AS DateShippedHashFK, -- not available in source
    CAST(NULL AS NUMBER(19,0)) AS DateReceivedHashFK, -- not available in source
    CAST(NULL AS NUMBER(19,0)) AS DateInvoicedHashFK, -- not available in source
    CAST(ctePURCHASE_ORDERS.PO_ID AS VARCHAR(20)) AS PurchaseOrderNumber,
    PO_ITEM_ID AS PurchaseOrderLineNumber,
    STATUS AS OrderStatus,
    QUANTITY AS QuantityOrdered,
    CAST(NULL AS INT) AS QuantityShipped, -- not available in source
    CAST(NULL AS INT) AS QuantityReceived, -- not available in source
    CAST(NULL AS INT) AS QuantityAccepted, -- not available in source
    CAST(NULL AS INT) AS QuantityRejected, -- not available in source
    UNIT_COST AS UnitCost,
    CAST(QUANTITY * UNIT_COST AS DECIMAL(19,4)) AS OrderAmount,
    CAST(NULL AS DECIMAL(19,4)) AS ShippedAmount, -- not available in source
    CAST(NULL AS DECIMAL(19,4)) AS ReceivedAmount, -- not available in source
    CAST(NULL AS DECIMAL(19,4)) AS InvoicedAmount, -- not available in source
    CAST(NULL AS INT) AS DaysToShip, -- not available in source
    CAST(NULL AS INT) AS DaysInTransit, -- not available in source
    CAST(NULL AS INT) AS DaysToReceive, -- not available in source
    CAST(NULL AS INT) AS DaysToInvoice, -- not available in source
    CAST(SHA2_BINARY(
        COALESCE(CAST(ctePURCHASE_ORDERS.PO_ID AS VARCHAR), '#@#@#@#@#')
        || '|' || COALESCE(CAST(PO_ITEM_ID AS VARCHAR), '#@#@#@#@#')
        || '|' || COALESCE(CAST(STATUS AS VARCHAR), '#@#@#@#@#')
        || '|' || COALESCE(CAST(QUANTITY AS VARCHAR), '#@#@#@#@#')
        || '|' || COALESCE(CAST(UNIT_COST AS VARCHAR), '#@#@#@#@#')
        || '|' || COALESCE(CAST(QUANTITY * UNIT_COST AS VARCHAR), '#@#@#@#@#')
    , 256) AS BINARY(32)) AS Hashbytes,
    CAST(0 AS INT) AS EtlBatchId, -- not available in source
    ctePURCHASE_ORDER_ITEMS._FIVETRAN_SYNCED AS LoadTimestamp
FROM ctePURCHASE_ORDER_ITEMS
JOIN ctePURCHASE_ORDERS
    ON ctePURCHASE_ORDER_ITEMS.PO_ID = ctePURCHASE_ORDERS.PO_ID
