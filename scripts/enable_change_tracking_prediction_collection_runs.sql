-- Enable Change Tracking on prediction.collection_runs for Fivetran
-- Database-level Change Tracking must already be ON (see enable_change_tracking.sql)

USE [free-sql-db-3300567];
GO

ALTER TABLE [prediction].[collection_runs] ENABLE CHANGE_TRACKING;

-- Verify
SELECT 
    s.name AS SchemaName,
    t.name AS TableName,
    ct.is_track_columns_updated_on AS TrackColumnsUpdated
FROM sys.change_tracking_tables ct
INNER JOIN sys.tables t ON ct.object_id = t.object_id
INNER JOIN sys.schemas s ON t.schema_id = s.schema_id
WHERE s.name = 'prediction' AND t.name = 'collection_runs';
