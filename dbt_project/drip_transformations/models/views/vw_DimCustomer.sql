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
        SHA2_BINARY(
               IFNULL(CAST(CREATED_AT AS VARCHAR), '#@#@#@#@#') || '|'
            || IFNULL(CAST(EMAIL      AS VARCHAR), '#@#@#@#@#') || '|'
            || IFNULL(CAST(FIRST_NAME AS VARCHAR), '#@#@#@#@#') || '|'
            || IFNULL(CAST(LAST_NAME  AS VARCHAR), '#@#@#@#@#') || '|'
            || IFNULL(CAST(PHONE      AS VARCHAR), '#@#@#@#@#')
            , 256
        ) AS Hashbytes
    FROM cte_prep

),

cte_prep_hash_lag AS (

    SELECT
        *,
        LAG(Hashbytes) OVER (
            PARTITION BY CUSTOMER_ID
            ORDER BY EffectiveStartDateTimeUTC
        ) AS LagHash,
        DATEADD(
            MICROSECOND,
            -1,
            LEAD(EffectiveStartDateTimeUTC) OVER (
                PARTITION BY CUSTOMER_ID
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
        HASH(
            IFNULL(CAST(CUSTOMER_ID AS VARCHAR), '#@#@#@#@#'),
            SourceSystemCode
        ) AS CustomerHashPK,

        HASH(
            IFNULL(CAST(CUSTOMER_ID AS VARCHAR), '#@#@#@#@#'),
            SourceSystemCode
        ) AS CustomerHashBK,

        CAST(NULL AS VARCHAR(50))          AS AcquisitionChannel, -- not available in source
        CAST(CREATED_AT AS DATE)           AS AcquisitionDate,
        CAST(NULL AS VARCHAR(50))          AS City,               -- not available in source
        CAST(NULL AS VARCHAR(50))          AS Country,            -- not available in source
        CAST(NULL AS VARCHAR(20))          AS CustomerType,       -- not available in source
        EMAIL                              AS EmailAddress,
        FIRST_NAME                         AS FirstName,
        TRIM(COALESCE(FIRST_NAME, '')) || ' ' || TRIM(COALESCE(LAST_NAME, '')) AS FullName,
        TRUE                               AS IsActive,           -- not available in source
        LAST_NAME                          AS LastName,
        CAST(NULL AS DATE)                 AS LoyaltyJoinDate,    -- not available in source
        CAST(NULL AS INT)                  AS LoyaltyPoints,      -- not available in source
        CAST(NULL AS VARCHAR(20))          AS LoyaltyTier,        -- not available in source
        PHONE                              AS PhoneNumber,
        CAST(NULL AS VARCHAR(20))          AS PostalCode,         -- not available in source
        CAST(NULL AS VARCHAR(50))          AS StateProvince,      -- not available in source
        CAST(NULL AS VARCHAR(200))         AS StreetAddress,      -- not available in source

        CAST(EffectiveStartDateTimeUTC AS TIMESTAMP_TZ) AS EffectiveStartDateTime,
        CAST(
            COALESCE(LeadEffectiveStartDateTimeUTC, EffectiveEndDateTimeRaw)
            AS TIMESTAMP_TZ
        ) AS EffectiveEndDateTime,
        CASE
            WHEN LeadEffectiveStartDateTimeUTC IS NULL AND IsFivetranActive THEN 'Y'
            ELSE 'N'
        END AS CurrentFlagYN,

        CAST(EffectiveStartDateTimeUTC AS TIMESTAMP_TZ) AS CreatedDateTime,
        CAST(InsertedDateTimeUTC       AS TIMESTAMP_TZ) AS ModifiedDateTime,
        SourceSystemCode,

        CAST(CUSTOMER_ID AS VARCHAR(40))   AS SourceCustomerPK,
        CAST(CUSTOMER_ID AS VARCHAR(40))   AS SourceCustomerBK,

        FileName,
        CAST(StageInsertedDateTimeUTC AS TIMESTAMP_TZ) AS StageInsertedDateTimeUTC,
        Hashbytes,
        DataCondition
    FROM cte_row_reduce

)

SELECT
    CustomerHashPK,
    CustomerHashBK,
    AcquisitionChannel,
    AcquisitionDate,
    City,
    Country,
    CustomerType,
    EmailAddress,
    FirstName,
    FullName,
    IsActive,
    LastName,
    LoyaltyJoinDate,
    LoyaltyPoints,
    LoyaltyTier,
    PhoneNumber,
    PostalCode,
    StateProvince,
    StreetAddress,
    EffectiveStartDateTime,
    EffectiveEndDateTime,
    CurrentFlagYN,
    CreatedDateTime,
    ModifiedDateTime,
    SourceSystemCode,
    SourceCustomerPK,
    SourceCustomerBK,
    FileName,
    StageInsertedDateTimeUTC,
    Hashbytes,
    DataCondition
FROM fin
