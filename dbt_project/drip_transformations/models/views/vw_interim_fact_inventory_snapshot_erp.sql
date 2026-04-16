{{ config(materialized='view') }}

WITH cteINVENTORY AS (
    SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY INVENTORY_ID ORDER BY UPDATED_AT DESC) AS LatestRank
    FROM {{ source('bronze_erp__dbo', 'INVENTORY') }}
),

ctePRODUCTS AS (
    SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY PRODUCT_ID ORDER BY _FIVETRAN_SYNCED DESC) AS LatestRank
    FROM {{ source('bronze_erp__dbo', 'PRODUCTS') }}
)

SELECT
    CAST(SHA2(COALESCE(CAST(cteINVENTORY.INVENTORY_ID AS VARCHAR), '#@#@#@#@#'), 256) AS BINARY(32))
                                                                          AS inventory_snapshot_hash_pk,

    CAST(SHA2(COALESCE(CAST(cteINVENTORY.UPDATED_AT AS VARCHAR), '#@#@#@#@#'), 256) AS BINARY(32))
                                                                          AS date_hash_fk,

    CAST(SHA2(COALESCE(CAST(cteINVENTORY.PRODUCT_ID AS VARCHAR), '#@#@#@#@#'), 256) AS BINARY(32))
                                                                          AS product_hash_fk,

    CAST(SHA2(COALESCE(CAST(cteINVENTORY.STORE_ID AS VARCHAR), '#@#@#@#@#'), 256) AS BINARY(32))
                                                                          AS warehouse_hash_fk,

    cteINVENTORY.QUANTITY_ON_HAND                                         AS quantity_on_hand,
    0                                                                     AS quantity_reserved,          -- not available in source
    0                                                                     AS quantity_available,         -- not available in source
    0                                                                     AS quantity_on_order,          -- not available in source
    0                                                                     AS quantity_in_transit,        -- not available in source
    cteINVENTORY.REORDER_LEVEL                                            AS reorder_point,
    0                                                                     AS safety_stock_level,         -- not available in source
    CAST(NULL AS INT)                                                     AS days_of_supply,             -- not available in source

    ctePRODUCTS.COST_PRICE                                                AS unit_cost,
    cteINVENTORY.STOCK_VALUE                                              AS inventory_value,

    CAST(SHA2(
        COALESCE(CAST(cteINVENTORY.INVENTORY_ID AS VARCHAR), '#@#@#@#@#') || '|' ||
        COALESCE(CAST(cteINVENTORY.UPDATED_AT AS VARCHAR), '#@#@#@#@#') || '|' ||
        COALESCE(CAST(cteINVENTORY.PRODUCT_ID AS VARCHAR), '#@#@#@#@#') || '|' ||
        COALESCE(CAST(cteINVENTORY.STORE_ID AS VARCHAR), '#@#@#@#@#') || '|' ||
        COALESCE(CAST(cteINVENTORY.QUANTITY_ON_HAND AS VARCHAR), '#@#@#@#@#') || '|' ||
        COALESCE(CAST(cteINVENTORY.REORDER_LEVEL AS VARCHAR), '#@#@#@#@#') || '|' ||
        COALESCE(CAST(ctePRODUCTS.COST_PRICE AS VARCHAR), '#@#@#@#@#') || '|' ||
        COALESCE(CAST(cteINVENTORY.STOCK_VALUE AS VARCHAR), '#@#@#@#@#')
    , 256) AS BINARY(32))                                                 AS hashbytes,

    CAST(NULL AS INT)                                                     AS etl_batch_id,
    CURRENT_TIMESTAMP()                                                   AS load_timestamp

FROM cteINVENTORY
INNER JOIN ctePRODUCTS
    ON cteINVENTORY.PRODUCT_ID = ctePRODUCTS.PRODUCT_ID
WHERE cteINVENTORY.LatestRank = 1
  AND ctePRODUCTS.LatestRank = 1
