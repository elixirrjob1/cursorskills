{{ config(materialized='view') }}

WITH cte_prep AS (

    SELECT
        CUSTOMER_ID,
        FIRST_NAME,
        LAST_NAME,
        EMAIL,
        PHONE,
        CREATED_AT,
        UPDATED_AT,
        _FIVETRAN_SYNCED           AS InsertedDateTimeUTC,
        _FIVETRAN_START            AS EffectiveStartDateTimeUTC,
        _FIVETRAN_END              AS EffectiveEndDateTimeRaw,
        _FIVETRAN_ACTIVE           AS IsFivetranActive,
        'ERP'                      AS SourceSystemCode,
        ''                         AS FileName,
        'Data Condition 1'         AS DataCondition
    FROM {{ source('bronze_erp__dbo', 'CUSTOMERS') }}

),

cte_prep_hash AS (

    SELECT
        *,
        CAST(SHA2_BINARY(
               IFNULL(CAST(CREATED_AT AS VARCHAR), '#@#@#@#@#') || '|'
            || IFNULL(CAST(EMAIL      AS VARCHAR), '#@#@#@#@#') || '|'
            || IFNULL(CAST(FIRST_NAME AS VARCHAR), '#@#@#@#@#') || '|'
            || IFNULL(CAST(LAST_NAME  AS VARCHAR), '#@#@#@#@#') || '|'
            || IFNULL(CAST(PHONE      AS VARCHAR), '#@#@#@#@#')
            , 256
        ) AS BINARY(32)) AS Hashbytes
    FROM cte_prep

),

-- LAG only (needed for dedup filter) — LEAD intentionally excluded here
cte_prep_hash_lag AS (

    SELECT
        *,
        LAG(Hashbytes) OVER (
            PARTITION BY CUSTOMER_ID
            ORDER BY EffectiveStartDateTimeUTC
        ) AS LagHash,
        InsertedDateTimeUTC AS StageInsertedDateTimeUTC
    FROM cte_prep_hash

),

-- Dedup: keep first version per PK and any version where business data changed
cte_row_reduce AS (

    SELECT *
    FROM cte_prep_hash_lag
    WHERE Hashbytes <> LagHash OR LagHash IS NULL

),

-- LEAD computed after dedup so CurrentFlagYN reflects the reduced set correctly
cte_with_lead AS (

    SELECT
        *,
        DATEADD(
            MICROSECOND,
            -1,
            LEAD(EffectiveStartDateTimeUTC) OVER (
                PARTITION BY CUSTOMER_ID
                ORDER BY EffectiveStartDateTimeUTC
            )
        ) AS LeadEffectiveStartDateTimeUTC
    FROM cte_row_reduce

),

fin AS (

    SELECT
        HASH(COALESCE(CAST(CUSTOMER_ID AS VARCHAR), '#@#@#@#@#')
            || '|' || COALESCE(CAST(SourceSystemCode AS VARCHAR), '#@#@#@#@#')) AS "CustomerHashPK",

        HASH(COALESCE(CAST(CUSTOMER_ID AS VARCHAR), '#@#@#@#@#')
            || '|' || COALESCE(CAST(SourceSystemCode AS VARCHAR), '#@#@#@#@#')) AS "CustomerHashBK",

        CAST(NULL AS VARCHAR(50))          AS "AcquisitionChannel", -- not available in source
        CAST(CREATED_AT AS DATE)           AS "AcquisitionDate",
        CAST(NULL AS VARCHAR(50))          AS "City",               -- not available in source
        CAST(NULL AS VARCHAR(50))          AS "Country",            -- not available in source
        CAST(NULL AS VARCHAR(20))          AS "CustomerType",       -- not available in source
        EMAIL                              AS "EmailAddress",
        FIRST_NAME                         AS "FirstName",
        TRIM(COALESCE(FIRST_NAME, '')) || ' ' || TRIM(COALESCE(LAST_NAME, '')) AS "FullName",
        TRUE                               AS "IsActive",           -- not available in source
        LAST_NAME                          AS "LastName",
        CAST(NULL AS DATE)                 AS "LoyaltyJoinDate",    -- not available in source
        CAST(NULL AS INT)                  AS "LoyaltyPoints",      -- not available in source
        CAST(NULL AS VARCHAR(20))          AS "LoyaltyTier",        -- not available in source
        PHONE                              AS "PhoneNumber",
        CAST(NULL AS VARCHAR(20))          AS "PostalCode",         -- not available in source
        CAST(NULL AS VARCHAR(50))          AS "StateProvince",      -- not available in source
        CAST(NULL AS VARCHAR(200))         AS "StreetAddress",      -- not available in source

        CAST(EffectiveStartDateTimeUTC AS TIMESTAMP_TZ) AS "EffectiveStartDateTime",
        CAST(
            COALESCE(LeadEffectiveStartDateTimeUTC, EffectiveEndDateTimeRaw)
            AS TIMESTAMP_TZ
        ) AS "EffectiveEndDateTime",
        CASE
            WHEN LeadEffectiveStartDateTimeUTC IS NULL THEN 'Y'
            ELSE 'N'
        END AS "CurrentFlagYN",
        CASE
            WHEN LeadEffectiveStartDateTimeUTC IS NULL AND NOT IsFivetranActive THEN 'Y'
            ELSE 'N'
        END AS "DeletedFlagYN",

        CAST(EffectiveStartDateTimeUTC AS TIMESTAMP_TZ) AS "CreatedDateTime",
        CAST(InsertedDateTimeUTC       AS TIMESTAMP_TZ) AS "ModifiedDateTime",
        SourceSystemCode AS "SourceSystemCode",

        CAST(CUSTOMER_ID AS VARCHAR(40))   AS "SourceCustomerPK",
        CAST(CUSTOMER_ID AS VARCHAR(40))   AS "SourceCustomerBK",

        FileName AS "FileName",
        CAST(StageInsertedDateTimeUTC AS TIMESTAMP_TZ) AS "StageInsertedDateTimeUTC",
        Hashbytes AS "Hashbytes",
        DataCondition AS "DataCondition"
    FROM cte_with_lead

)

SELECT * FROM fin
