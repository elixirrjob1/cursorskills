{{ config(materialized='view') }}

SELECT
    CAST(NULL AS BINARY(32))    AS warehouse_hash_pk,     -- not available in source
    CAST(NULL AS BINARY(32))    AS warehouse_hash_bk,     -- not available in source
    CAST(NULL AS VARCHAR(100))  AS warehouse_name,        -- not available in source
    CAST(NULL AS VARCHAR(20))   AS warehouse_type,        -- not available in source
    CAST(NULL AS VARCHAR(200))  AS street_address,        -- not available in source
    CAST(NULL AS VARCHAR(50))   AS city,                  -- not available in source
    CAST(NULL AS VARCHAR(50))   AS state_province,        -- not available in source
    CAST(NULL AS VARCHAR(20))   AS postal_code,           -- not available in source
    CAST(NULL AS VARCHAR(50))   AS country,               -- not available in source
    CAST(NULL AS VARCHAR(10))   AS district_code,         -- not available in source
    CAST(NULL AS VARCHAR(50))   AS district_name,         -- not available in source
    CAST(NULL AS VARCHAR(10))   AS region_code,           -- not available in source
    CAST(NULL AS VARCHAR(50))   AS region_name,           -- not available in source
    CAST(NULL AS INT)           AS total_capacity_units,  -- not available in source
    CAST(NULL AS BOOLEAN)       AS is_active,             -- not available in source
    CAST(NULL AS INT)           AS etl_batch_id,          -- not available in source
    CAST(NULL AS TIMESTAMP)     AS load_timestamp         -- not available in source
WHERE 1 = 0
