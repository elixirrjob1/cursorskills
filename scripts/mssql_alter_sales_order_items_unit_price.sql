-- Source (Azure SQL / MSSQL): match Snowflake NUMBER(15,2) as NUMERIC(15,2).
-- Object names: table dbo.sales_order_items (plural), column unit_price
--   (there is no SALES_ORDER_ITEM singular table in this schema).
--
-- Run in SSMS / Azure Data Studio / sqlcmd against the Fivetran source database.
-- Then in Fivetran: reload connection schema (if needed) and sync.
--
-- If you get lock/timeout, run off-peak or after pausing heavy writers.

ALTER TABLE dbo.sales_order_items
ALTER COLUMN unit_price NUMERIC(15, 2) NOT NULL;
