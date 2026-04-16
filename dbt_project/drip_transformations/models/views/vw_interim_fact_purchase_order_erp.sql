{{ config(materialized='view') }}

WITH ctePURCHASE_ORDER_ITEMS AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY PO_ID, PO_ITEM_ID
            ORDER BY _FIVETRAN_SYNCED DESC
        ) AS LatestRank
    FROM {{ source('bronze_erp__dbo', 'PURCHASE_ORDER_ITEMS') }}
),

ctePURCHASE_ORDERS AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY PO_ID
            ORDER BY _FIVETRAN_SYNCED DESC
        ) AS LatestRank
    FROM {{ source('bronze_erp__dbo', 'PURCHASE_ORDERS') }}
)

SELECT
    CAST(SHA2(
        COALESCE(CAST(ctePURCHASE_ORDER_ITEMS.PO_ID AS VARCHAR), '#@#@#@#@#')
        || '|' ||
        COALESCE(CAST(ctePURCHASE_ORDER_ITEMS.PO_ITEM_ID AS VARCHAR), '#@#@#@#@#')
    , 256) AS BINARY(32))                                                       AS purchase_order_hash_pk,

    CAST(SHA2(COALESCE(CAST(ctePURCHASE_ORDER_ITEMS.PRODUCT_ID AS VARCHAR), '#@#@#@#@#'), 256) AS BINARY(32))
                                                                                AS product_hash_fk,
    CAST(SHA2(COALESCE(CAST(ctePURCHASE_ORDERS.SUPPLIER_ID AS VARCHAR), '#@#@#@#@#'), 256) AS BINARY(32))
                                                                                AS supplier_hash_fk,
    CAST(SHA2(COALESCE(CAST(ctePURCHASE_ORDERS.STORE_ID AS VARCHAR), '#@#@#@#@#'), 256) AS BINARY(32))
                                                                                AS warehouse_hash_fk,
    CAST(SHA2(COALESCE(CAST(ctePURCHASE_ORDERS.ORDER_DATE AS VARCHAR), '#@#@#@#@#'), 256) AS BINARY(32))
                                                                                AS date_ordered_hash_fk,
    IFF(ctePURCHASE_ORDERS.EXPECTED_DATE IS NULL, NULL,
        CAST(SHA2(COALESCE(CAST(ctePURCHASE_ORDERS.EXPECTED_DATE AS VARCHAR), '#@#@#@#@#'), 256) AS BINARY(32)))
                                                                                AS date_expected_hash_fk,
    CAST(NULL AS BINARY(32))                                                    AS date_shipped_hash_fk,    -- not available in source
    CAST(NULL AS BINARY(32))                                                    AS date_received_hash_fk,   -- not available in source
    CAST(NULL AS BINARY(32))                                                    AS date_invoiced_hash_fk,   -- not available in source

    CAST(ctePURCHASE_ORDERS.PO_ID AS VARCHAR(20))                              AS purchase_order_number,
    ctePURCHASE_ORDER_ITEMS.PO_ITEM_ID                                         AS purchase_order_line_number,
    ctePURCHASE_ORDERS.STATUS                                                   AS order_status,
    ctePURCHASE_ORDER_ITEMS.QUANTITY                                            AS quantity_ordered,
    CAST(NULL AS INT)                                                           AS quantity_shipped,         -- not available in source
    CAST(NULL AS INT)                                                           AS quantity_received,        -- not available in source
    CAST(NULL AS INT)                                                           AS quantity_accepted,        -- not available in source
    CAST(NULL AS INT)                                                           AS quantity_rejected,        -- not available in source
    ctePURCHASE_ORDER_ITEMS.UNIT_COST                                           AS unit_cost,
    ctePURCHASE_ORDER_ITEMS.QUANTITY * ctePURCHASE_ORDER_ITEMS.UNIT_COST        AS order_amount,
    CAST(NULL AS DECIMAL(19,4))                                                 AS shipped_amount,           -- not available in source
    CAST(NULL AS DECIMAL(19,4))                                                 AS received_amount,          -- not available in source
    CAST(NULL AS DECIMAL(19,4))                                                 AS invoiced_amount,          -- not available in source
    CAST(NULL AS INT)                                                           AS days_to_ship,             -- not available in source
    CAST(NULL AS INT)                                                           AS days_in_transit,          -- not available in source
    CAST(NULL AS INT)                                                           AS days_to_receive,          -- not available in source
    CAST(NULL AS INT)                                                           AS days_to_invoice,          -- not available in source

    CAST(NULL AS INT)                                                           AS etl_batch_id,             -- not available in source
    CURRENT_TIMESTAMP()                                                         AS load_timestamp

FROM ctePURCHASE_ORDER_ITEMS
INNER JOIN ctePURCHASE_ORDERS
    ON ctePURCHASE_ORDER_ITEMS.PO_ID = ctePURCHASE_ORDERS.PO_ID
WHERE ctePURCHASE_ORDER_ITEMS.LatestRank = 1
  AND ctePURCHASE_ORDERS.LatestRank = 1
