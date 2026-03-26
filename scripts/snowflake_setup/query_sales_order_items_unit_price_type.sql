-- Inspect UNIT_PRICE type on Fivetran-landed sales_order_items (Snowflake).
-- Database: DRIP_DATA_INTELLIGENCE (adjust if different).
-- Schema: bronze_erp__dbo (from Fivetran name_in_destination for dbo).

SELECT
  table_catalog,
  table_schema,
  table_name,
  column_name,
  data_type,
  numeric_precision,
  numeric_scale,
  is_nullable
FROM DRIP_DATA_INTELLIGENCE.INFORMATION_SCHEMA.COLUMNS
WHERE UPPER(table_schema) = 'BRONZE_ERP__DBO'
  AND UPPER(table_name) = 'SALES_ORDER_ITEMS'
  AND UPPER(column_name) = 'UNIT_PRICE';
