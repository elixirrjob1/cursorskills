{{ config(materialized='view') }}

WITH cteEMPLOYEES AS (
    SELECT
        EMPLOYEE_ID,
        FIRST_NAME,
        LAST_NAME,
        EMAIL,
        "ROLE",
        HIRE_DATE,
        STORE_ID,
        _FIVETRAN_SYNCED
    FROM {{ source('bronze_erp__dbo', 'EMPLOYEES') }}
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY EMPLOYEE_ID
        ORDER BY _FIVETRAN_SYNCED DESC
    ) = 1
)

SELECT
    SHA2_BINARY(COALESCE(CAST(EMPLOYEE_ID AS VARCHAR), '#@#@#@#@#'), 256) AS EmployeeHashPK,
    SHA2_BINARY(COALESCE(CAST(EMPLOYEE_ID AS VARCHAR), '#@#@#@#@#'), 256) AS EmployeeHashBK,
    FIRST_NAME AS FirstName,
    LAST_NAME AS LastName,
    TRIM(FIRST_NAME) || ' ' || TRIM(LAST_NAME) AS FullName,
    EMAIL AS EmailAddress,
    "ROLE" AS JobTitle,
    CAST(NULL AS VARCHAR(50)) AS Department, -- not available in source
    HIRE_DATE AS HireDate,
    CAST(NULL AS DATE) AS TerminationDate, -- not available in source
    CAST(NULL AS BINARY(32)) AS ManagerEmployeeHashFK, -- not available in source
    IFF(STORE_ID IS NULL, NULL, SHA2_BINARY(COALESCE(CAST(STORE_ID AS VARCHAR), '#@#@#@#@#'), 256)) AS HomeStoreHashFK,
    CAST(NULL AS BOOLEAN) AS IsActive, -- not available in source
    SHA2_BINARY(
        COALESCE(CAST(FIRST_NAME AS VARCHAR), '#@#@#@#@#')
        || '|' || COALESCE(CAST(LAST_NAME AS VARCHAR), '#@#@#@#@#')
        || '|' || COALESCE(CAST(EMAIL AS VARCHAR), '#@#@#@#@#')
        || '|' || COALESCE(CAST("ROLE" AS VARCHAR), '#@#@#@#@#')
        || '|' || COALESCE(CAST(HIRE_DATE AS VARCHAR), '#@#@#@#@#')
        || '|' || COALESCE(CAST(STORE_ID AS VARCHAR), '#@#@#@#@#')
    , 256) AS Hashbytes,
    CAST(0 AS INT) AS EtlBatchId, -- not available in source
    _FIVETRAN_SYNCED AS LoadTimestamp
FROM cteEMPLOYEES
