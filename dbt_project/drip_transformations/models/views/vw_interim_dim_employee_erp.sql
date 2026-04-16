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
        _FIVETRAN_SYNCED,
        ROW_NUMBER() OVER (
            PARTITION BY EMPLOYEE_ID
            ORDER BY _FIVETRAN_SYNCED DESC
        ) AS LatestRank
    FROM {{ source('bronze_erp__dbo', 'EMPLOYEES') }}
)

SELECT
    CAST(SHA2(COALESCE(CAST(EMPLOYEE_ID AS VARCHAR), '#@#@#@#@#'), 256) AS BINARY(32))
        AS employee_hash_pk,

    CAST(SHA2(COALESCE(CAST(EMPLOYEE_ID AS VARCHAR), '#@#@#@#@#'), 256) AS BINARY(32))
        AS employee_hash_bk,

    FIRST_NAME AS first_name,
    LAST_NAME AS last_name,
    TRIM(FIRST_NAME) || ' ' || TRIM(LAST_NAME) AS full_name,
    EMAIL AS email_address,
    "ROLE" AS job_title,
    HIRE_DATE AS hire_date,

    CAST('Unknown' AS VARCHAR(50)) AS department,                -- not available in source
    CAST(NULL AS DATE) AS termination_date,                      -- not available in source

    CAST(NULL AS BINARY(32)) AS manager_employee_hash_fk,        -- not available in source
    IFF(STORE_ID IS NULL, NULL,
        CAST(SHA2(COALESCE(CAST(STORE_ID AS VARCHAR), '#@#@#@#@#'), 256) AS BINARY(32)))
        AS home_store_hash_fk,

    CAST(TRUE AS BOOLEAN) AS is_active,                          -- not available in source

    CAST(SHA2(
        COALESCE(CAST(FIRST_NAME AS VARCHAR), '#@#@#@#@#') || '|' ||
        COALESCE(CAST(LAST_NAME AS VARCHAR), '#@#@#@#@#') || '|' ||
        COALESCE(CAST(TRIM(FIRST_NAME) || ' ' || TRIM(LAST_NAME) AS VARCHAR), '#@#@#@#@#') || '|' ||
        COALESCE(CAST(EMAIL AS VARCHAR), '#@#@#@#@#') || '|' ||
        COALESCE(CAST("ROLE" AS VARCHAR), '#@#@#@#@#') || '|' ||
        COALESCE(CAST(HIRE_DATE AS VARCHAR), '#@#@#@#@#') || '|' ||
        COALESCE(CAST(STORE_ID AS VARCHAR), '#@#@#@#@#')
    , 256) AS BINARY(32)) AS hashbytes,

    CAST(0 AS INT) AS etl_batch_id,                              -- not available in source
    CURRENT_TIMESTAMP() AS load_timestamp

FROM cteEMPLOYEES
WHERE LatestRank = 1
