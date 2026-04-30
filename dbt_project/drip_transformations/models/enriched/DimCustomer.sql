{{ config(
    materialized='incremental',
    unique_key='"CustomerHashPK"',
    incremental_strategy='delete+insert',
    pre_hook=[
        "DELETE FROM {{ this }}
         WHERE \"SourceCustomerPK\" IN (
             SELECT DISTINCT CAST(CUSTOMER_ID AS VARCHAR(40))
             FROM {{ source('bronze_erp__dbo', 'CUSTOMERS') }}
             WHERE _FIVETRAN_SYNCED > (
                 SELECT COALESCE(MAX(\"StageInsertedDateTimeUTC\"), '1900-01-01'::TIMESTAMP_TZ)
                 FROM {{ this }}
             )
         )"
    ]
) }}

-- SCD Type 2 incremental: delete + reinsert pattern.
--
-- Why not a simple append: LEAD(EffectiveStartDateTimeUTC) in vw_DimCustomer
-- recomputes end dates across the full customer partition. When a new Fivetran
-- row arrives, the previous version's EffectiveEndDateTime changes, so we must
-- delete all versions for any changed CUSTOMER_ID and reinsert from the view.
--
-- Watermark: StageInsertedDateTimeUTC (= _FIVETRAN_SYNCED).
-- The pre_hook deletes all existing rows for customers whose _FIVETRAN_SYNCED
-- timestamp is newer than the max already in this table. The SELECT below then
-- reinserts the full version history for those customers from the view.
-- On the initial (empty table) run, the watermark resolves to 1900-01-01,
-- so all customers are processed — equivalent to a full refresh.

SELECT v.*
FROM {{ ref('vw_DimCustomer') }} v
WHERE v."SourceCustomerPK" IN (
    SELECT DISTINCT CAST(CUSTOMER_ID AS VARCHAR(40))
    FROM {{ source('bronze_erp__dbo', 'CUSTOMERS') }}
    WHERE _FIVETRAN_SYNCED > (
        {% if is_incremental() %}
            (SELECT COALESCE(MAX("StageInsertedDateTimeUTC"), '1900-01-01'::TIMESTAMP_TZ) FROM {{ this }})
        {% else %}
            '1900-01-01'::TIMESTAMP_TZ
        {% endif %}
    )
)
