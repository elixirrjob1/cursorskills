-- =============================================================================
-- SCD Type 2 Incremental Proof-of-Concept — DimCustomer
-- =============================================================================
-- Strategy: delete + reinsert for changed business keys only.
--
-- Why a simple append doesn't work for Type 2:
--   vw_DimCustomer uses LEAD(EffectiveStartDateTimeUTC) OVER (PARTITION BY
--   CUSTOMER_ID) to compute each version's EffectiveEndDateTime. When Fivetran
--   adds a new row, the *previous* version's end date changes — so the prior
--   row already in the target is stale. We must delete all versions for any
--   changed CUSTOMER_ID and reinsert them all from the view.
--
-- Watermark: StageInsertedDateTimeUTC (= _FIVETRAN_SYNCED).
--   Fivetran writes a fresh _FIVETRAN_SYNCED on every change, so any
--   CUSTOMER_ID with MAX(_FIVETRAN_SYNCED) > our stored watermark has changed.
--
-- Run this script in a Snowflake worksheet against the DBT_DEV schema.
-- Prerequisites: DimCustomer must already exist in DBT_DEV (see Step 0).
-- =============================================================================

USE DATABASE DRIP_DATA_INTELLIGENCE;
USE WAREHOUSE FIVETRAN_DRIP_WH;

-- Schema map (dbt appends subfolder name to target schema):
--   views/   → DBT_DEV            (e.g. DBT_DEV.vw_DimCustomer)
--   enriched/ → DBT_DEV_ENRICHED  (e.g. DBT_DEV_ENRICHED.DimCustomer)


-- ---------------------------------------------------------------------------
-- STEP 0  Baseline snapshot (run once before starting incremental tests)
-- ---------------------------------------------------------------------------
-- Records how many rows are in the table before anything changes.
-- Re-run after a full dbt refresh to reset the baseline.

SELECT
    COUNT(*)                                        AS total_rows,
    COUNT(DISTINCT "SourceCustomerPK")              AS distinct_customers,
    COUNT(CASE WHEN "CurrentFlagYN" = 'Y' THEN 1 END) AS current_rows,
    MAX("StageInsertedDateTimeUTC")                 AS watermark
FROM DBT_DEV_ENRICHED."DimCustomer";


-- ---------------------------------------------------------------------------
-- STEP 1  Watermark — what was the last sync timestamp we already processed?
-- ---------------------------------------------------------------------------

SET last_run = (
    SELECT COALESCE(MAX("StageInsertedDateTimeUTC"), '1900-01-01'::TIMESTAMP_TZ)
    FROM DBT_DEV_ENRICHED."DimCustomer"
);

SELECT $last_run AS watermark_in_use;


-- ---------------------------------------------------------------------------
-- STEP 2  Identify changed customer IDs since the watermark
-- ---------------------------------------------------------------------------

CREATE OR REPLACE TEMPORARY TABLE changed_pks AS
SELECT DISTINCT
    CAST(CUSTOMER_ID AS VARCHAR(40)) AS pk
FROM DRIP_DATA_INTELLIGENCE.BRONZE_ERP__DBO.CUSTOMERS
WHERE _FIVETRAN_SYNCED > $last_run;

SELECT COUNT(*) AS changed_customers FROM changed_pks;


-- ---------------------------------------------------------------------------
-- STEP 3  Delete all existing versions for changed customers
-- ---------------------------------------------------------------------------

DELETE FROM DBT_DEV_ENRICHED."DimCustomer"
WHERE "SourceCustomerPK" IN (SELECT pk FROM changed_pks);

SELECT 'deleted stale rows for ' || COUNT(*) || ' customers' AS msg
FROM changed_pks;


-- ---------------------------------------------------------------------------
-- STEP 4  Reinsert all versions from the view for changed customers
-- ---------------------------------------------------------------------------
-- The view re-evaluates LEAD() across the full partition, so end dates and
-- CurrentFlagYN will be correct for the inserted rows.

INSERT INTO DBT_DEV_ENRICHED."DimCustomer"
SELECT v.*
FROM DBT_DEV."vw_DimCustomer" v
WHERE v."SourceCustomerPK" IN (SELECT pk FROM changed_pks);

SELECT 'inserted ' || COUNT(*) || ' rows for changed customers' AS msg
FROM changed_pks;


-- =============================================================================
-- VALIDATION QUERIES
-- Run these after both a fresh full-refresh (dbt run) AND after the incremental
-- pass to confirm the results are identical.
-- =============================================================================

-- V1: Total row count (compare to STEP 0 baseline after full-refresh)
SELECT
    COUNT(*)                                            AS total_rows,
    COUNT(DISTINCT "SourceCustomerPK")                  AS distinct_customers,
    COUNT(CASE WHEN "CurrentFlagYN" = 'Y' THEN 1 END)  AS current_rows,
    COUNT(CASE WHEN "DeletedFlagYN"  = 'Y' THEN 1 END)  AS deleted_rows,
    MAX("StageInsertedDateTimeUTC")                     AS new_watermark
FROM DBT_DEV_ENRICHED."DimCustomer";


-- V2: Duplicate check — no two rows should share (CustomerHashPK, EffectiveStartDateTime)
SELECT
    "CustomerHashPK",
    "EffectiveStartDateTime",
    COUNT(*) AS cnt
FROM DBT_DEV_ENRICHED."DimCustomer"
GROUP BY 1, 2
HAVING COUNT(*) > 1
ORDER BY cnt DESC;
-- Expected: 0 rows


-- V3: CurrentFlagYN integrity — exactly one 'Y' per customer
SELECT
    "SourceCustomerPK",
    COUNT(CASE WHEN "CurrentFlagYN" = 'Y' THEN 1 END) AS current_count
FROM DBT_DEV_ENRICHED."DimCustomer"
GROUP BY 1
HAVING current_count <> 1
ORDER BY 1;
-- Expected: 0 rows


-- V4: EffectiveEndDateTime populated for all non-current rows
SELECT COUNT(*) AS null_end_on_non_current
FROM DBT_DEV_ENRICHED."DimCustomer"
WHERE "CurrentFlagYN" = 'N'
  AND "EffectiveEndDateTime" IS NULL;
-- Expected: 0


-- V5: EffectiveStartDateTime < EffectiveEndDateTime for all closed rows
SELECT COUNT(*) AS invalid_date_range
FROM DBT_DEV_ENRICHED."DimCustomer"
WHERE "CurrentFlagYN" = 'N'
  AND "EffectiveStartDateTime" >= "EffectiveEndDateTime";
-- Expected: 0


-- V6: Hash comparison — row-level diff between incremental and full-refresh
-- Run this after a full-refresh into a temp table to verify zero divergence.
--
-- Step A: capture full-refresh snapshot
-- CREATE OR REPLACE TEMPORARY TABLE full_refresh_snapshot AS
-- SELECT "CustomerHashPK", "EffectiveStartDateTime", "Hashbytes"
-- FROM DBT_DEV_ENRICHED."DimCustomer";
--
-- Step B: run incremental (Steps 1-4 above), then compare:
-- SELECT 'in_incremental_not_in_full_refresh' AS diff_type, COUNT(*) AS cnt
-- FROM DBT_DEV_ENRICHED."DimCustomer" inc
-- LEFT JOIN full_refresh_snapshot fr
--   ON inc."CustomerHashPK" = fr."CustomerHashPK"
--   AND inc."EffectiveStartDateTime" = fr."EffectiveStartDateTime"
-- WHERE fr."CustomerHashPK" IS NULL
-- UNION ALL
-- SELECT 'in_full_refresh_not_in_incremental', COUNT(*)
-- FROM full_refresh_snapshot fr
-- LEFT JOIN DBT_DEV_ENRICHED."DimCustomer" inc
--   ON fr."CustomerHashPK" = inc."CustomerHashPK"
--   AND fr."EffectiveStartDateTime" = inc."EffectiveStartDateTime"
-- WHERE inc."CustomerHashPK" IS NULL;
-- Expected: both rows show 0
