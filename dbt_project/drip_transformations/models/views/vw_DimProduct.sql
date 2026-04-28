{{ config(materialized='view') }}

WITH cte_prep AS (
    SELECT
        PRODUCT_ID,
        SKU,
        NAME,
        PRODUCT_DESCRIPTION,
        CATEGORY,
        COST_PRICE,
        UNIT_PRICE,
        ACTIVE,
        _FIVETRAN_SYNCED   AS InsertedDateTimeUTC,
        _FIVETRAN_START    AS EffectiveStartDateTimeUTC,
        _FIVETRAN_END      AS EffectiveEndDateTimeRaw,
        _FIVETRAN_ACTIVE   AS IsFivetranActive,
        'ERP'              AS SourceSystemCode,
        ''                 AS FileName,
        'Data Condition 1' AS DataCondition
    FROM {{ source('bronze_erp__dbo', 'PRODUCTS') }}
),

cte_prep_hash AS (
    SELECT
        *,
        CAST(SHA2_BINARY(
               IFNULL(CAST(ACTIVE              AS VARCHAR), '#@#@#@#@#') || '|'
            || IFNULL(CAST(CATEGORY            AS VARCHAR), '#@#@#@#@#') || '|'
            || IFNULL(CAST(COST_PRICE          AS VARCHAR), '#@#@#@#@#') || '|'
            || IFNULL(CAST(NAME                AS VARCHAR), '#@#@#@#@#') || '|'
            || IFNULL(CAST(PRODUCT_DESCRIPTION AS VARCHAR), '#@#@#@#@#') || '|'
            || IFNULL(CAST(UNIT_PRICE          AS VARCHAR), '#@#@#@#@#')
            , 256
        ) AS BINARY(32)) AS Hashbytes
    FROM cte_prep
),

cte_prep_hash_lag AS (
    SELECT
        *,
        LAG(Hashbytes) OVER (
            PARTITION BY PRODUCT_ID
            ORDER BY EffectiveStartDateTimeUTC
        ) AS LagHash,
        DATEADD(
            MICROSECOND, -1,
            LEAD(EffectiveStartDateTimeUTC) OVER (
                PARTITION BY PRODUCT_ID
                ORDER BY EffectiveStartDateTimeUTC
            )
        ) AS LeadEffectiveStartDateTimeUTC,
        InsertedDateTimeUTC AS StageInsertedDateTimeUTC
    FROM cte_prep_hash
),

cte_row_reduce AS (
    SELECT *
    FROM cte_prep_hash_lag
    WHERE Hashbytes <> LagHash OR LagHash IS NULL
),

fin AS (
    SELECT
        HASH(COALESCE(CAST(PRODUCT_ID AS VARCHAR), '#@#@#@#@#')
            || '|' || COALESCE(CAST(SourceSystemCode AS VARCHAR), '#@#@#@#@#')) AS "ProductHashPK",
        HASH(COALESCE(CAST(SKU AS VARCHAR), '#@#@#@#@#')
            || '|' || COALESCE(CAST(SourceSystemCode AS VARCHAR), '#@#@#@#@#')) AS "ProductHashBK",
        CAST(NULL AS VARCHAR(10)) AS "BrandCode", -- not available in source
        CAST(NULL AS VARCHAR(50)) AS "BrandName", -- not available in source
        CAST(NULL AS VARCHAR(10)) AS "CategoryCode", -- not available in source
        TRIM(CATEGORY) AS "CategoryName",
        ACTIVE AS "IsActive",
        IFF(ACTIVE, FALSE, TRUE) AS "IsDiscontinued",
        CAST(NULL AS VARCHAR(20)) AS "PackSize", -- not available in source
        TRIM(PRODUCT_DESCRIPTION) AS "ProductDescription",
        TRIM(NAME) AS "ProductName",
        CAST(NULL AS VARCHAR(10)) AS "SubcategoryCode", -- not available in source
        CAST(NULL AS VARCHAR(50)) AS "SubcategoryName", -- not available in source
        CAST(COST_PRICE AS NUMBER(19,4)) AS "UnitCost",
        CAST(UNIT_PRICE AS NUMBER(19,4)) AS "UnitListPrice",
        CAST(NULL AS VARCHAR(20)) AS "UnitOfMeasure", -- not available in source
        CAST(EffectiveStartDateTimeUTC AS TIMESTAMP_TZ) AS "EffectiveStartDateTime",
        CAST(COALESCE(LeadEffectiveStartDateTimeUTC, EffectiveEndDateTimeRaw) AS TIMESTAMP_TZ) AS "EffectiveEndDateTime",
        CASE
            WHEN LeadEffectiveStartDateTimeUTC IS NULL AND IsFivetranActive THEN 'Y'
            ELSE 'N'
        END AS "CurrentFlagYN",
        CAST(EffectiveStartDateTimeUTC AS TIMESTAMP_TZ) AS "CreatedDateTime",
        CAST(InsertedDateTimeUTC       AS TIMESTAMP_TZ) AS "ModifiedDateTime",
        SourceSystemCode AS "SourceSystemCode",
        CAST(PRODUCT_ID AS VARCHAR(40))  AS "SourceProductPK",
        CAST(SKU        AS VARCHAR(100)) AS "SourceProductBK",
        FileName AS "FileName",
        CAST(StageInsertedDateTimeUTC AS TIMESTAMP_TZ) AS "StageInsertedDateTimeUTC",
        Hashbytes AS "Hashbytes",
        DataCondition AS "DataCondition"
    FROM cte_row_reduce
)

SELECT * FROM fin
