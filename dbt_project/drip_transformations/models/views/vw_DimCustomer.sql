{{ config(materialized='view') }}

WITH cteCUSTOMERS AS (
    SELECT
        CUSTOMER_ID,
        FIRST_NAME,
        LAST_NAME,
        EMAIL,
        PHONE,
        CREATED_AT,
        _FIVETRAN_SYNCED
    FROM {{ source('bronze_erp__dbo', 'CUSTOMERS') }}
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY CUSTOMER_ID
        ORDER BY _FIVETRAN_SYNCED DESC
    ) = 1
)

SELECT
    CAST(SHA2(COALESCE(CAST(CUSTOMER_ID AS VARCHAR), '#@#@#@#@#'), 256) AS BINARY(32)) AS CustomerHashPK,
    CAST(SHA2(COALESCE(CAST(CUSTOMER_ID AS VARCHAR), '#@#@#@#@#'), 256) AS BINARY(32)) AS CustomerHashBK,
    FIRST_NAME AS FirstName,
    LAST_NAME AS LastName,
    TRIM(FIRST_NAME) || ' ' || TRIM(LAST_NAME) AS FullName,
    EMAIL AS EmailAddress,
    PHONE AS PhoneNumber,
    CAST(NULL AS VARCHAR(200)) AS StreetAddress, -- not available in source
    CAST(NULL AS VARCHAR(50)) AS City, -- not available in source
    CAST(NULL AS VARCHAR(50)) AS StateProvince, -- not available in source
    CAST(NULL AS VARCHAR(20)) AS PostalCode, -- not available in source
    CAST(NULL AS VARCHAR(50)) AS Country, -- not available in source
    CAST(NULL AS VARCHAR(20)) AS CustomerType, -- not available in source
    CAST(NULL AS VARCHAR(50)) AS AcquisitionChannel, -- not available in source
    CAST(CREATED_AT AS DATE) AS AcquisitionDate,
    CAST(NULL AS VARCHAR(20)) AS LoyaltyTier, -- not available in source
    CAST(NULL AS INT) AS LoyaltyPoints, -- not available in source
    CAST(NULL AS DATE) AS LoyaltyJoinDate, -- not available in source
    CAST(NULL AS BOOLEAN) AS IsActive, -- not available in source
    CAST(NULL AS DATE) AS EffectiveDate, -- not available in source
    CAST(NULL AS DATE) AS ExpirationDate, -- not available in source
    CAST(NULL AS BOOLEAN) AS IsCurrent, -- not available in source
    SHA2(
        COALESCE(CAST(FIRST_NAME AS VARCHAR), '#@#@#@#@#')
        || '|' || COALESCE(CAST(LAST_NAME AS VARCHAR), '#@#@#@#@#')
        || '|' || COALESCE(CAST(EMAIL AS VARCHAR), '#@#@#@#@#')
        || '|' || COALESCE(CAST(PHONE AS VARCHAR), '#@#@#@#@#')
        || '|' || COALESCE(CAST(CREATED_AT AS VARCHAR), '#@#@#@#@#')
    , 256) AS Hashbytes,
    CAST(0 AS INT) AS EtlBatchId, -- not available in source
    _FIVETRAN_SYNCED AS LoadTimestamp
FROM cteCUSTOMERS
