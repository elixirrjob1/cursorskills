-- Enable Change Tracking on Azure SQL Database for Fivetran
-- Run this script on your Azure SQL Database: free-sql-db-3300567

-- Step 1: Enable Change Tracking at the database level
ALTER DATABASE [free-sql-db-3300567]
SET CHANGE_TRACKING = ON
(CHANGE_RETENTION = 2 DAYS, AUTO_CLEANUP = ON);

-- Step 2: Enable Change Tracking on each table in the dbo schema
-- This enables Change Tracking for all tables that Fivetran will sync

USE [free-sql-db-3300567];
GO

-- Enable Change Tracking for each table
ALTER TABLE [dbo].[customers] ENABLE CHANGE_TRACKING;
ALTER TABLE [dbo].[employees] ENABLE CHANGE_TRACKING;
ALTER TABLE [dbo].[inventory] ENABLE CHANGE_TRACKING;
ALTER TABLE [dbo].[products] ENABLE CHANGE_TRACKING;
ALTER TABLE [dbo].[purchase_order_items] ENABLE CHANGE_TRACKING;
ALTER TABLE [dbo].[purchase_orders] ENABLE CHANGE_TRACKING;
ALTER TABLE [dbo].[sales_order_items] ENABLE CHANGE_TRACKING;
ALTER TABLE [dbo].[sales_orders] ENABLE CHANGE_TRACKING;
ALTER TABLE [dbo].[stores] ENABLE CHANGE_TRACKING;
ALTER TABLE [dbo].[suppliers] ENABLE CHANGE_TRACKING;

-- Verify Change Tracking is enabled
SELECT 
    t.name AS TableName,
    ct.is_track_columns_updated_on AS TrackColumnsUpdated
FROM sys.change_tracking_tables ct
INNER JOIN sys.tables t ON ct.object_id = t.object_id
WHERE t.schema_id = SCHEMA_ID('dbo')
ORDER BY t.name;

-- Check database-level Change Tracking status
SELECT 
    name AS DatabaseName,
    is_change_tracking_on AS ChangeTrackingEnabled
FROM sys.databases
WHERE name = 'free-sql-db-3300567';
