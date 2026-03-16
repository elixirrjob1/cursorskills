-- Enable Change Data Capture (CDC) on Azure SQL Database for Fivetran
-- NOTE: CDC is more complex and may require additional permissions
-- Run this script on your Azure SQL Database: free-sql-db-3300567

USE [free-sql-db-3300567];
GO

-- Step 1: Enable CDC at the database level
-- Note: This requires sysadmin role or db_owner role
EXEC sys.sp_cdc_enable_db;

-- Step 2: Enable CDC for each table in the dbo schema
-- This creates CDC capture instances for each table

-- Enable CDC for customers table
EXEC sys.sp_cdc_enable_table
    @source_schema = N'dbo',
    @source_name = N'customers',
    @role_name = NULL,  -- No access control role needed for Fivetran
    @supports_net_changes = 1;

-- Enable CDC for employees table
EXEC sys.sp_cdc_enable_table
    @source_schema = N'dbo',
    @source_name = N'employees',
    @role_name = NULL,
    @supports_net_changes = 1;

-- Enable CDC for inventory table
EXEC sys.sp_cdc_enable_table
    @source_schema = N'dbo',
    @source_name = N'inventory',
    @role_name = NULL,
    @supports_net_changes = 1;

-- Enable CDC for products table
EXEC sys.sp_cdc_enable_table
    @source_schema = N'dbo',
    @source_name = N'products',
    @role_name = NULL,
    @supports_net_changes = 1;

-- Enable CDC for purchase_order_items table
EXEC sys.sp_cdc_enable_table
    @source_schema = N'dbo',
    @source_name = N'purchase_order_items',
    @role_name = NULL,
    @supports_net_changes = 1;

-- Enable CDC for purchase_orders table
EXEC sys.sp_cdc_enable_table
    @source_schema = N'dbo',
    @source_name = N'purchase_orders',
    @role_name = NULL,
    @supports_net_changes = 1;

-- Enable CDC for sales_order_items table
EXEC sys.sp_cdc_enable_table
    @source_schema = N'dbo',
    @source_name = N'sales_order_items',
    @role_name = NULL,
    @supports_net_changes = 1;

-- Enable CDC for sales_orders table
EXEC sys.sp_cdc_enable_table
    @source_schema = N'dbo',
    @source_name = N'sales_orders',
    @role_name = NULL,
    @supports_net_changes = 1;

-- Enable CDC for stores table
EXEC sys.sp_cdc_enable_table
    @source_schema = N'dbo',
    @source_name = N'stores',
    @role_name = NULL,
    @supports_net_changes = 1;

-- Enable CDC for suppliers table
EXEC sys.sp_cdc_enable_table
    @source_schema = N'dbo',
    @source_name = N'suppliers',
    @role_name = NULL,
    @supports_net_changes = 1;

-- Verify CDC is enabled
SELECT 
    s.name AS SchemaName,
    t.name AS TableName,
    c.capture_instance AS CaptureInstance,
    c.is_tracked_by_cdc AS IsTracked
FROM sys.tables t
INNER JOIN sys.schemas s ON t.schema_id = s.schema_id
LEFT JOIN cdc.change_tables c ON t.object_id = c.source_object_id
WHERE s.name = 'dbo'
ORDER BY t.name;

-- Check database-level CDC status
SELECT 
    name AS DatabaseName,
    is_cdc_enabled AS CDCEnabled
FROM sys.databases
WHERE name = 'free-sql-db-3300567';
