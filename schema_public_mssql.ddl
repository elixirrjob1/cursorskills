-- MSSQL DDL: app schema with same structure as schema_mssql.json
-- Note: "public" is a reserved role in SQL Server; using "app" instead.
-- Run against your MSSQL instance; use schema_filter "app" when analyzing.

IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = 'app')
BEGIN
    EXEC('CREATE SCHEMA [app]');
END
GO

-- MSreplication_options
IF OBJECT_ID('[app].[MSreplication_options]', 'U') IS NOT NULL
    DROP TABLE [app].[MSreplication_options];
CREATE TABLE [app].[MSreplication_options] (
    optname         NVARCHAR(128) NOT NULL,
    value           BIT NOT NULL,
    major_version   INT NOT NULL,
    minor_version   INT NOT NULL,
    revision        INT NOT NULL,
    install_failures INT NOT NULL
);
GO

-- spt_fallback_db
IF OBJECT_ID('[app].[spt_fallback_db]', 'U') IS NOT NULL
    DROP TABLE [app].[spt_fallback_db];
CREATE TABLE [app].[spt_fallback_db] (
    xserver_name        VARCHAR(30) NOT NULL,
    xdttm_ins           DATETIME NOT NULL,
    xdttm_last_ins_upd   DATETIME NOT NULL,
    xfallback_dbid      SMALLINT NULL,
    name                VARCHAR(30) NOT NULL,
    dbid                SMALLINT NOT NULL,
    status              SMALLINT NOT NULL,
    version             SMALLINT NOT NULL
);
GO

-- spt_fallback_dev
IF OBJECT_ID('[app].[spt_fallback_dev]', 'U') IS NOT NULL
    DROP TABLE [app].[spt_fallback_dev];
CREATE TABLE [app].[spt_fallback_dev] (
    xserver_name        VARCHAR(30) NOT NULL,
    xdttm_ins           DATETIME NOT NULL,
    xdttm_last_ins_upd   DATETIME NOT NULL,
    xfallback_low        INT NULL,
    xfallback_drive      CHAR(2) NULL,
    low                 INT NOT NULL,
    high                INT NOT NULL,
    status              SMALLINT NOT NULL,
    name                VARCHAR(30) NOT NULL,
    phyname             VARCHAR(127) NOT NULL
);
GO

-- spt_fallback_usg
IF OBJECT_ID('[app].[spt_fallback_usg]', 'U') IS NOT NULL
    DROP TABLE [app].[spt_fallback_usg];
CREATE TABLE [app].[spt_fallback_usg] (
    xserver_name        VARCHAR(30) NOT NULL,
    xdttm_ins           DATETIME NOT NULL,
    xdttm_last_ins_upd   DATETIME NOT NULL,
    xfallback_vstart    INT NULL,
    dbid                SMALLINT NOT NULL,
    segmap              INT NOT NULL,
    lstart              INT NOT NULL,
    sizepg              INT NOT NULL,
    vstart              INT NOT NULL
);
GO

-- spt_monitor
IF OBJECT_ID('[app].[spt_monitor]', 'U') IS NOT NULL
    DROP TABLE [app].[spt_monitor];
CREATE TABLE [app].[spt_monitor] (
    lastrun         DATETIME NOT NULL,
    cpu_busy        INT NOT NULL,
    io_busy         INT NOT NULL,
    idle            INT NOT NULL,
    pack_received   INT NOT NULL,
    pack_sent       INT NOT NULL,
    connections     INT NOT NULL,
    pack_errors     INT NOT NULL,
    total_read      INT NOT NULL,
    total_write     INT NOT NULL,
    total_errors    INT NOT NULL
);
GO

-- Optional: insert sample row into spt_monitor (original had 1 row)
-- INSERT INTO public.spt_monitor (lastrun, cpu_busy, io_busy, idle, pack_received, pack_sent, connections, pack_errors, total_read, total_write, total_errors)
-- VALUES (GETDATE(), 31, 19, 5838, 39, 39, 70, 0, 0, 0, 0);
