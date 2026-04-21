{{ config(materialized='view') }}

WITH cteSUPPLIERS AS (
    SELECT
        SUPPLIER_ID,
        NAME,
        CONTACT_NAME,
        EMAIL,
        PHONE,
        _FIVETRAN_SYNCED
    FROM {{ source('bronze_erp__dbo', 'SUPPLIERS') }}
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY SUPPLIER_ID
        ORDER BY _FIVETRAN_SYNCED DESC
    ) = 1
)

SELECT
    SHA2(COALESCE(CAST(SUPPLIER_ID AS VARCHAR), '#@#@#@#@#'), 256) AS SupplierHashPK,
    SHA2(COALESCE(CAST(SUPPLIER_ID AS VARCHAR), '#@#@#@#@#'), 256) AS SupplierHashBK,
    NAME AS SupplierName,
    CAST(NULL AS VARCHAR(100)) AS SupplierDBAName, -- not available in source
    CONTACT_NAME AS ContactName,
    EMAIL AS ContactEmail,
    PHONE AS ContactPhone,
    CAST(NULL AS VARCHAR(200)) AS StreetAddress, -- not available in source
    CAST(NULL AS VARCHAR(50)) AS City, -- not available in source
    CAST(NULL AS VARCHAR(50)) AS StateProvince, -- not available in source
    CAST(NULL AS VARCHAR(20)) AS PostalCode, -- not available in source
    CAST(NULL AS VARCHAR(50)) AS Country, -- not available in source
    CAST(NULL AS VARCHAR(10)) AS PaymentTermsCode, -- not available in source
    CAST(NULL AS VARCHAR(50)) AS PaymentTermsDescription, -- not available in source
    CAST(NULL AS INT) AS PaymentTermsDays, -- not available in source
    CAST(NULL AS INT) AS LeadTimeDays, -- not available in source
    CAST(NULL AS DECIMAL(19,4)) AS MinimumOrderAmount, -- not available in source
    CAST(NULL AS BOOLEAN) AS IsActive, -- not available in source
    CAST(NULL AS BOOLEAN) AS IsPreferred, -- not available in source
    SHA2(
        COALESCE(CAST(NAME AS VARCHAR), '#@#@#@#@#')
        || '|' || COALESCE(CAST(CONTACT_NAME AS VARCHAR), '#@#@#@#@#')
        || '|' || COALESCE(CAST(EMAIL AS VARCHAR), '#@#@#@#@#')
        || '|' || COALESCE(CAST(PHONE AS VARCHAR), '#@#@#@#@#')
    , 256) AS Hashbytes,
    CAST(0 AS INT) AS EtlBatchId, -- not available in source
    _FIVETRAN_SYNCED AS LoadTimestamp
FROM cteSUPPLIERS
