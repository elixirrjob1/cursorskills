{{ config(materialized='view') }}

WITH cteDATE_SPINE AS (
    SELECT
        DATEADD(DAY, SEQ4(), '2000-01-01'::DATE) AS DateValue
    FROM TABLE(GENERATOR(ROWCOUNT => 36525))
)

SELECT
    HASH(COALESCE(CAST(DateValue AS VARCHAR), '#@#@#@#@#')) AS "DateHashPK",
    DateValue AS "DateValue",
    CAST(TO_CHAR(DateValue, 'YYYYMMDD') AS INT) AS "DateKey",
    MOD(DAYOFWEEKISO(DateValue), 7) + 1 AS "DayOfWeek",
    DECODE(DAYOFWEEKISO(DateValue),
        1, 'Monday',
        2, 'Tuesday',
        3, 'Wednesday',
        4, 'Thursday',
        5, 'Friday',
        6, 'Saturday',
        7, 'Sunday'
    ) AS "DayName",
    DAY(DateValue) AS "DayOfMonth",
    DAYOFYEAR(DateValue) AS "DayOfYear",
    WEEKISO(DateValue) AS "WeekOfYear",
    MONTH(DateValue) AS "MonthNumber",
    DECODE(MONTH(DateValue),
        1, 'January',
        2, 'February',
        3, 'March',
        4, 'April',
        5, 'May',
        6, 'June',
        7, 'July',
        8, 'August',
        9, 'September',
        10, 'October',
        11, 'November',
        12, 'December'
    ) AS "MonthName",
    MONTHNAME(DateValue) AS "MonthAbbrev",
    QUARTER(DateValue) AS "QuarterNumber",
    'Q' || CAST(QUARTER(DateValue) AS VARCHAR) AS "QuarterName",
    YEAR(DateValue) AS "YearNumber",
    TO_CHAR(DateValue, 'YYYY-MM') AS "YearMonth",
    CAST(YEAR(DateValue) AS VARCHAR) || '-Q' || CAST(QUARTER(DateValue) AS VARCHAR) AS "YearQuarter",
    CAST(NULL AS INT) AS "FiscalMonthNumber", -- not available in source
    CAST(NULL AS INT) AS "FiscalQuarterNumber", -- not available in source
    CAST(NULL AS INT) AS "FiscalYearNumber", -- not available in source
    (DAYOFWEEKISO(DateValue) IN (6, 7)) AS "IsWeekend",
    FALSE AS "IsHoliday", -- not available in source
    CAST(NULL AS VARCHAR(50)) AS "HolidayName", -- not available in source
    (DateValue = LAST_DAY(DateValue)) AS "IsLastDayOfMonth",
    (DateValue = LAST_DAY(DateValue, 'QUARTER')) AS "IsLastDayOfQuarter",
    (DateValue = LAST_DAY(DateValue, 'YEAR')) AS "IsLastDayOfYear",
    CAST(SHA2_BINARY(
        COALESCE(CAST(DateValue AS VARCHAR), '#@#@#@#@#')
    , 256) AS BINARY(32)) AS "Hashbytes",
    CAST(0 AS INT) AS "EtlBatchId", -- not available in source
    '1900-01-01'::TIMESTAMP_NTZ AS "LoadTimestamp"
FROM cteDATE_SPINE
