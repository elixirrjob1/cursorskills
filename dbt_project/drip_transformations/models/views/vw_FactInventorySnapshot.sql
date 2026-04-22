{{ config(materialized='view') }}

WITH cteINVENTORY AS (
    SELECT
        INVENTORY_ID,
        PRODUCT_ID,
        STORE_ID,
        QUANTITY_ON_HAND,
        REORDER_LEVEL,
        STOCK_VALUE,
        UPDATED_AT,
        _FIVETRAN_SYNCED
    FROM {{ source('bronze_erp__dbo', 'INVENTORY') }}
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY INVENTORY_ID
        ORDER BY _FIVETRAN_SYNCED DESC
    ) = 1
),

ctePRODUCTS AS (
    SELECT
        PRODUCT_ID,
        COST_PRICE
    FROM {{ source('bronze_erp__dbo', 'PRODUCTS') }}
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY PRODUCT_ID
        ORDER BY _FIVETRAN_SYNCED DESC
    ) = 1
)

SELECT
    HASH(COALESCE(CAST(cteINVENTORY.INVENTORY_ID AS VARCHAR), '#@#@#@#@#')) AS InventorySnapshotHashPK,
    HASH(COALESCE(CAST(cteINVENTORY.UPDATED_AT AS VARCHAR), '#@#@#@#@#')) AS DateHashFK,
    HASH(COALESCE(CAST(cteINVENTORY.PRODUCT_ID AS VARCHAR), '#@#@#@#@#')) AS ProductHashFK,
    HASH(COALESCE(CAST(cteINVENTORY.STORE_ID AS VARCHAR), '#@#@#@#@#')) AS WarehouseHashFK,
    cteINVENTORY.QUANTITY_ON_HAND AS QuantityOnHand,
    CAST(NULL AS INT) AS QuantityReserved, -- not available in source
    CAST(NULL AS INT) AS QuantityAvailable, -- not available in source
    CAST(NULL AS INT) AS QuantityOnOrder, -- not available in source
    CAST(NULL AS INT) AS QuantityInTransit, -- not available in source
    cteINVENTORY.REORDER_LEVEL AS ReorderPoint,
    CAST(NULL AS INT) AS SafetyStockLevel, -- not available in source
    CAST(NULL AS INT) AS DaysOfSupply, -- not available in source
    ctePRODUCTS.COST_PRICE AS UnitCost,
    cteINVENTORY.STOCK_VALUE AS InventoryValue,
    CAST(SHA2_BINARY(
        COALESCE(CAST(cteINVENTORY.QUANTITY_ON_HAND AS VARCHAR), '#@#@#@#@#')
        || '|' || COALESCE(CAST(cteINVENTORY.REORDER_LEVEL AS VARCHAR), '#@#@#@#@#')
        || '|' || COALESCE(CAST(ctePRODUCTS.COST_PRICE AS VARCHAR), '#@#@#@#@#')
        || '|' || COALESCE(CAST(cteINVENTORY.STOCK_VALUE AS VARCHAR), '#@#@#@#@#')
    , 256) AS BINARY(32)) AS Hashbytes,
    CAST(0 AS INT) AS EtlBatchId, -- not available in source
    cteINVENTORY._FIVETRAN_SYNCED AS LoadTimestamp
FROM cteINVENTORY
LEFT JOIN ctePRODUCTS
    ON cteINVENTORY.PRODUCT_ID = ctePRODUCTS.PRODUCT_ID
