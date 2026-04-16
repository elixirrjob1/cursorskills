{{ config(materialized='view') }}

WITH date_spine AS (
    SELECT
        DATEADD(DAY, ROW_NUMBER() OVER (ORDER BY SEQ4()) - 1, '2020-01-01'::DATE) AS date_value
    FROM TABLE(GENERATOR(ROWCOUNT => 5844))
),

date_attrs AS (
    SELECT
        d.date_value,
        CAST(TO_CHAR(d.date_value, 'YYYYMMDD') AS INT)                   AS date_key,
        MOD(DAYOFWEEKISO(d.date_value), 7) + 1                           AS day_of_week,
        CASE DAYOFWEEKISO(d.date_value)
            WHEN 1 THEN 'Monday'    WHEN 2 THEN 'Tuesday'
            WHEN 3 THEN 'Wednesday' WHEN 4 THEN 'Thursday'
            WHEN 5 THEN 'Friday'    WHEN 6 THEN 'Saturday'
            WHEN 7 THEN 'Sunday'
        END                                                               AS day_name,
        DAY(d.date_value)                                                 AS day_of_month,
        DAYOFYEAR(d.date_value)                                           AS day_of_year,
        WEEKISO(d.date_value)                                             AS week_of_year,
        MONTH(d.date_value)                                               AS month_number,
        CASE MONTH(d.date_value)
            WHEN 1  THEN 'January'   WHEN 2  THEN 'February'
            WHEN 3  THEN 'March'     WHEN 4  THEN 'April'
            WHEN 5  THEN 'May'       WHEN 6  THEN 'June'
            WHEN 7  THEN 'July'      WHEN 8  THEN 'August'
            WHEN 9  THEN 'September' WHEN 10 THEN 'October'
            WHEN 11 THEN 'November'  WHEN 12 THEN 'December'
        END                                                               AS month_name,
        CASE MONTH(d.date_value)
            WHEN 1  THEN 'Jan' WHEN 2  THEN 'Feb'
            WHEN 3  THEN 'Mar' WHEN 4  THEN 'Apr'
            WHEN 5  THEN 'May' WHEN 6  THEN 'Jun'
            WHEN 7  THEN 'Jul' WHEN 8  THEN 'Aug'
            WHEN 9  THEN 'Sep' WHEN 10 THEN 'Oct'
            WHEN 11 THEN 'Nov' WHEN 12 THEN 'Dec'
        END                                                               AS month_abbrev,
        QUARTER(d.date_value)                                             AS quarter_number,
        'Q' || CAST(QUARTER(d.date_value) AS VARCHAR)                     AS quarter_name,
        YEAR(d.date_value)                                                AS year_number,
        TO_CHAR(d.date_value, 'YYYY-MM')                                  AS year_month,
        CAST(YEAR(d.date_value) AS VARCHAR) || '-Q' || CAST(QUARTER(d.date_value) AS VARCHAR)
                                                                          AS year_quarter,
        CASE
            WHEN MONTH(d.date_value) >= 7 THEN MONTH(d.date_value) - 6
            ELSE MONTH(d.date_value) + 6
        END                                                               AS fiscal_month_number,
        CAST(CEIL(
            CASE
                WHEN MONTH(d.date_value) >= 7 THEN MONTH(d.date_value) - 6
                ELSE MONTH(d.date_value) + 6
            END / 3.0
        ) AS INT)                                                         AS fiscal_quarter_number,
        CASE
            WHEN MONTH(d.date_value) >= 7 THEN YEAR(d.date_value) + 1
            ELSE YEAR(d.date_value)
        END                                                               AS fiscal_year_number,
        DAYOFWEEKISO(d.date_value) IN (6, 7)                              AS is_weekend,
        d.date_value = LAST_DAY(d.date_value, 'MONTH')                    AS is_last_day_of_month,
        d.date_value = LAST_DAY(d.date_value, 'QUARTER')                  AS is_last_day_of_quarter,
        d.date_value = LAST_DAY(d.date_value, 'YEAR')                     AS is_last_day_of_year
    FROM date_spine d
)

SELECT
    CAST(SHA2(COALESCE(CAST(a.date_value AS VARCHAR), '#@#@#@#@#'), 256) AS BINARY(32))
                                                                          AS date_hash_pk,
    a.date_value,
    a.date_key,
    a.day_of_week,
    a.day_name,
    a.day_of_month,
    a.day_of_year,
    a.week_of_year,
    a.month_number,
    a.month_name,
    a.month_abbrev,
    a.quarter_number,
    a.quarter_name,
    a.year_number,
    a.year_month,
    a.year_quarter,
    a.fiscal_month_number,
    a.fiscal_quarter_number,
    a.fiscal_year_number,
    a.is_weekend,
    FALSE                                                                 AS is_holiday,              -- not available in source
    CAST(NULL AS VARCHAR(50))                                             AS holiday_name,            -- not available in source
    a.is_last_day_of_month,
    a.is_last_day_of_quarter,
    a.is_last_day_of_year,
    CAST(SHA2(
        COALESCE(CAST(a.date_value AS VARCHAR), '#@#@#@#@#') || '|' ||
        COALESCE(CAST(a.date_key AS VARCHAR), '#@#@#@#@#') || '|' ||
        COALESCE(CAST(a.day_of_week AS VARCHAR), '#@#@#@#@#') || '|' ||
        COALESCE(CAST(a.day_name AS VARCHAR), '#@#@#@#@#') || '|' ||
        COALESCE(CAST(a.day_of_month AS VARCHAR), '#@#@#@#@#') || '|' ||
        COALESCE(CAST(a.day_of_year AS VARCHAR), '#@#@#@#@#') || '|' ||
        COALESCE(CAST(a.week_of_year AS VARCHAR), '#@#@#@#@#') || '|' ||
        COALESCE(CAST(a.month_number AS VARCHAR), '#@#@#@#@#') || '|' ||
        COALESCE(CAST(a.month_name AS VARCHAR), '#@#@#@#@#') || '|' ||
        COALESCE(CAST(a.month_abbrev AS VARCHAR), '#@#@#@#@#') || '|' ||
        COALESCE(CAST(a.quarter_number AS VARCHAR), '#@#@#@#@#') || '|' ||
        COALESCE(CAST(a.quarter_name AS VARCHAR), '#@#@#@#@#') || '|' ||
        COALESCE(CAST(a.year_number AS VARCHAR), '#@#@#@#@#') || '|' ||
        COALESCE(CAST(a.year_month AS VARCHAR), '#@#@#@#@#') || '|' ||
        COALESCE(CAST(a.year_quarter AS VARCHAR), '#@#@#@#@#') || '|' ||
        COALESCE(CAST(a.fiscal_month_number AS VARCHAR), '#@#@#@#@#') || '|' ||
        COALESCE(CAST(a.fiscal_quarter_number AS VARCHAR), '#@#@#@#@#') || '|' ||
        COALESCE(CAST(a.fiscal_year_number AS VARCHAR), '#@#@#@#@#') || '|' ||
        COALESCE(CAST(a.is_weekend AS VARCHAR), '#@#@#@#@#') || '|' ||
        COALESCE(CAST(a.is_last_day_of_month AS VARCHAR), '#@#@#@#@#') || '|' ||
        COALESCE(CAST(a.is_last_day_of_quarter AS VARCHAR), '#@#@#@#@#') || '|' ||
        COALESCE(CAST(a.is_last_day_of_year AS VARCHAR), '#@#@#@#@#')
    , 256) AS BINARY(32))                                                 AS hashbytes,
    CAST(NULL AS INT)                                                     AS etl_batch_id,
    CURRENT_TIMESTAMP()                                                   AS load_timestamp
FROM date_attrs a
