-- =============================================
-- Metadata Database Schema for EDW_PSA to Snowflake Ingestion
-- =============================================

USE MetadataDB;
GO

-- =============================================
-- Table Configuration
-- =============================================
IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[TableConfig]') AND type in (N'U'))
BEGIN
    CREATE TABLE [dbo].[TableConfig] (
        [ConfigID] INT IDENTITY(1,1) PRIMARY KEY,
        [SourceSchema] NVARCHAR(128) NOT NULL,
        [SourceTable] NVARCHAR(128) NOT NULL,
        [DestinationSchema] NVARCHAR(128) NOT NULL,
        [DestinationTable] NVARCHAR(128) NOT NULL,
        [LoadType] NVARCHAR(20) NOT NULL DEFAULT 'FULL', -- FULL, INCREMENTAL
        [WatermarkColumn] NVARCHAR(128) NULL,
        [LastWatermarkValue] NVARCHAR(255) NULL,
        [IsActive] BIT NOT NULL DEFAULT 1,
        [Priority] INT NOT NULL DEFAULT 1,
        [CreatedDate] DATETIME2 DEFAULT GETDATE(),
        [ModifiedDate] DATETIME2 DEFAULT GETDATE(),
        CONSTRAINT [UK_TableConfig_SourceSchemaTable] UNIQUE ([SourceSchema], [SourceTable])
    );
    
    PRINT 'Created TableConfig table';
END
GO

-- =============================================
-- Execution Log
-- =============================================
IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[ExecutionLog]') AND type in (N'U'))
BEGIN
    CREATE TABLE [dbo].[ExecutionLog] (
        [LogID] INT IDENTITY(1,1) PRIMARY KEY,
        [PipelineRunId] NVARCHAR(255) NOT NULL,
        [SourceSchema] NVARCHAR(128) NOT NULL,
        [SourceTable] NVARCHAR(128) NOT NULL,
        [StartTime] DATETIME2 NOT NULL,
        [EndTime] DATETIME2 NULL,
        [Status] NVARCHAR(50) NOT NULL, -- Running, Succeeded, Failed
        [RowsCopied] BIGINT NULL,
        [SourceRowCount] BIGINT NULL,
        [DestinationRowCount] BIGINT NULL,
        [ErrorMessage] NVARCHAR(MAX) NULL,
        [CreatedDate] DATETIME2 DEFAULT GETDATE()
    );
    
    CREATE INDEX [IX_ExecutionLog_PipelineRunId] ON [dbo].[ExecutionLog] ([PipelineRunId]);
    CREATE INDEX [IX_ExecutionLog_SourceTable] ON [dbo].[ExecutionLog] ([SourceSchema], [SourceTable]);
    CREATE INDEX [IX_ExecutionLog_StartTime] ON [dbo].[ExecutionLog] ([StartTime]);
    
    PRINT 'Created ExecutionLog table';
END
GO

-- =============================================
-- Master Execution Log
-- =============================================
IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[MasterExecutionLog]') AND type in (N'U'))
BEGIN
    CREATE TABLE [dbo].[MasterExecutionLog] (
        [LogID] INT IDENTITY(1,1) PRIMARY KEY,
        [PipelineRunId] NVARCHAR(255) NOT NULL,
        [StartTime] DATETIME2 NOT NULL,
        [EndTime] DATETIME2 NULL,
        [Status] NVARCHAR(50) NOT NULL, -- Running, Succeeded, Failed
        [TableCount] INT NULL,
        [SuccessfulTables] INT NULL,
        [FailedTables] INT NULL,
        [ErrorMessage] NVARCHAR(MAX) NULL,
        [CreatedDate] DATETIME2 DEFAULT GETDATE()
    );
    
    CREATE INDEX [IX_MasterExecutionLog_PipelineRunId] ON [dbo].[MasterExecutionLog] ([PipelineRunId]);
    CREATE INDEX [IX_MasterExecutionLog_StartTime] ON [dbo].[MasterExecutionLog] ([StartTime]);
    
    PRINT 'Created MasterExecutionLog table';
END
GO

-- =============================================
-- Stored Procedure: LogExecutionStart
-- =============================================
IF EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[LogExecutionStart]') AND type in (N'P', N'PC'))
    DROP PROCEDURE [dbo].[LogExecutionStart];
GO

CREATE PROCEDURE [dbo].[LogExecutionStart]
    @PipelineRunId NVARCHAR(255),
    @SourceSchema NVARCHAR(128),
    @SourceTable NVARCHAR(128)
AS
BEGIN
    SET NOCOUNT ON;
    
    INSERT INTO [dbo].[ExecutionLog] (
        [PipelineRunId],
        [SourceSchema],
        [SourceTable],
        [StartTime],
        [Status]
    )
    VALUES (
        @PipelineRunId,
        @SourceSchema,
        @SourceTable,
        GETDATE(),
        'Running'
    );
    
    PRINT 'Logged execution start for ' + @SourceSchema + '.' + @SourceTable;
END
GO

-- =============================================
-- Stored Procedure: LogExecutionSuccess
-- =============================================
IF EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[LogExecutionSuccess]') AND type in (N'P', N'PC'))
    DROP PROCEDURE [dbo].[LogExecutionSuccess];
GO

CREATE PROCEDURE [dbo].[LogExecutionSuccess]
    @PipelineRunId NVARCHAR(255),
    @RowsCopied BIGINT = NULL,
    @SourceRowCount BIGINT = NULL,
    @DestinationRowCount BIGINT = NULL
AS
BEGIN
    SET NOCOUNT ON;
    
    UPDATE [dbo].[ExecutionLog]
    SET 
        [EndTime] = GETDATE(),
        [Status] = 'Succeeded',
        [RowsCopied] = @RowsCopied,
        [SourceRowCount] = @SourceRowCount,
        [DestinationRowCount] = @DestinationRowCount
    WHERE [PipelineRunId] = @PipelineRunId
        AND [Status] = 'Running';
    
    PRINT 'Logged execution success for pipeline run ' + @PipelineRunId;
END
GO

-- =============================================
-- Stored Procedure: LogExecutionFailure
-- =============================================
IF EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[LogExecutionFailure]') AND type in (N'P', N'PC'))
    DROP PROCEDURE [dbo].[LogExecutionFailure];
GO

CREATE PROCEDURE [dbo].[LogExecutionFailure]
    @PipelineRunId NVARCHAR(255),
    @ErrorMessage NVARCHAR(MAX)
AS
BEGIN
    SET NOCOUNT ON;
    
    UPDATE [dbo].[ExecutionLog]
    SET 
        [EndTime] = GETDATE(),
        [Status] = 'Failed',
        [ErrorMessage] = @ErrorMessage
    WHERE [PipelineRunId] = @PipelineRunId
        AND [Status] = 'Running';
    
    PRINT 'Logged execution failure for pipeline run ' + @PipelineRunId;
END
GO

-- =============================================
-- Stored Procedure: LogMasterExecutionStart
-- =============================================
IF EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[LogMasterExecutionStart]') AND type in (N'P', N'PC'))
    DROP PROCEDURE [dbo].[LogMasterExecutionStart];
GO

CREATE PROCEDURE [dbo].[LogMasterExecutionStart]
    @PipelineRunId NVARCHAR(255),
    @TableCount INT
AS
BEGIN
    SET NOCOUNT ON;
    
    INSERT INTO [dbo].[MasterExecutionLog] (
        [PipelineRunId],
        [StartTime],
        [Status],
        [TableCount]
    )
    VALUES (
        @PipelineRunId,
        GETDATE(),
        'Running',
        @TableCount
    );
    
    PRINT 'Logged master execution start for pipeline run ' + @PipelineRunId;
END
GO

-- =============================================
-- Stored Procedure: LogMasterExecutionSuccess
-- =============================================
IF EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[LogMasterExecutionSuccess]') AND type in (N'P', N'PC'))
    DROP PROCEDURE [dbo].[LogMasterExecutionSuccess];
GO

CREATE PROCEDURE [dbo].[LogMasterExecutionSuccess]
    @PipelineRunId NVARCHAR(255)
AS
BEGIN
    SET NOCOUNT ON;
    
    DECLARE @SuccessfulTables INT, @FailedTables INT;
    
    SELECT 
        @SuccessfulTables = COUNT(CASE WHEN [Status] = 'Succeeded' THEN 1 END),
        @FailedTables = COUNT(CASE WHEN [Status] = 'Failed' THEN 1 END)
    FROM [dbo].[ExecutionLog]
    WHERE [PipelineRunId] = @PipelineRunId;
    
    UPDATE [dbo].[MasterExecutionLog]
    SET 
        [EndTime] = GETDATE(),
        [Status] = 'Succeeded',
        [SuccessfulTables] = @SuccessfulTables,
        [FailedTables] = @FailedTables
    WHERE [PipelineRunId] = @PipelineRunId
        AND [Status] = 'Running';
    
    PRINT 'Logged master execution success for pipeline run ' + @PipelineRunId;
END
GO

-- =============================================
-- Stored Procedure: LogMasterExecutionFailure
-- =============================================
IF EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[LogMasterExecutionFailure]') AND type in (N'P', N'PC'))
    DROP PROCEDURE [dbo].[LogMasterExecutionFailure];
GO

CREATE PROCEDURE [dbo].[LogMasterExecutionFailure]
    @PipelineRunId NVARCHAR(255),
    @ErrorMessage NVARCHAR(MAX)
AS
BEGIN
    SET NOCOUNT ON;
    
    DECLARE @SuccessfulTables INT, @FailedTables INT;
    
    SELECT 
        @SuccessfulTables = COUNT(CASE WHEN [Status] = 'Succeeded' THEN 1 END),
        @FailedTables = COUNT(CASE WHEN [Status] = 'Failed' THEN 1 END)
    FROM [dbo].[ExecutionLog]
    WHERE [PipelineRunId] = @PipelineRunId;
    
    UPDATE [dbo].[MasterExecutionLog]
    SET 
        [EndTime] = GETDATE(),
        [Status] = 'Failed',
        [SuccessfulTables] = @SuccessfulTables,
        [FailedTables] = @FailedTables,
        [ErrorMessage] = @ErrorMessage
    WHERE [PipelineRunId] = @PipelineRunId
        AND [Status] = 'Running';
    
    PRINT 'Logged master execution failure for pipeline run ' + @PipelineRunId;
END
GO

-- =============================================
-- Stored Procedure: UpdateWatermark
-- =============================================
IF EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[UpdateWatermark]') AND type in (N'P', N'PC'))
    DROP PROCEDURE [dbo].[UpdateWatermark];
GO

CREATE PROCEDURE [dbo].[UpdateWatermark]
    @SourceSchema NVARCHAR(128),
    @SourceTable NVARCHAR(128),
    @NewWatermarkValue NVARCHAR(255)
AS
BEGIN
    SET NOCOUNT ON;
    
    UPDATE [dbo].[TableConfig]
    SET 
        [LastWatermarkValue] = @NewWatermarkValue,
        [ModifiedDate] = GETDATE()
    WHERE [SourceSchema] = @SourceSchema
        AND [SourceTable] = @SourceTable;
    
    PRINT 'Updated watermark for ' + @SourceSchema + '.' + @SourceTable + ' to ' + @NewWatermarkValue;
END
GO

-- =============================================
-- Views for Monitoring
-- =============================================

-- Current Execution Status View
IF EXISTS (SELECT * FROM sys.views WHERE object_id = OBJECT_ID(N'[dbo].[v_CurrentExecutionStatus]'))
    DROP VIEW [dbo].[v_CurrentExecutionStatus];
GO

CREATE VIEW [dbo].[v_CurrentExecutionStatus]
AS
SELECT 
    el.[PipelineRunId],
    el.[SourceSchema],
    el.[SourceTable],
    el.[StartTime],
    el.[EndTime],
    el.[Status],
    el.[RowsCopied],
    el.[SourceRowCount],
    el.[DestinationRowCount],
    CASE 
        WHEN el.[EndTime] IS NOT NULL THEN DATEDIFF(MINUTE, el.[StartTime], el.[EndTime])
        ELSE DATEDIFF(MINUTE, el.[StartTime], GETDATE())
    END AS [DurationMinutes],
    CASE 
        WHEN el.[SourceRowCount] > 0 AND el.[DestinationRowCount] > 0 THEN
            CASE WHEN el.[SourceRowCount] = el.[DestinationRowCount] THEN 'Valid' ELSE 'Invalid' END
        ELSE 'Unknown'
    END AS [DataValidation]
FROM [dbo].[ExecutionLog] el
WHERE el.[StartTime] >= DATEADD(DAY, -7, GETDATE()) -- Last 7 days
GO

-- Master Execution Summary View
IF EXISTS (SELECT * FROM sys.views WHERE object_id = OBJECT_ID(N'[dbo].[v_MasterExecutionSummary]'))
    DROP VIEW [dbo].[v_MasterExecutionSummary];
GO

CREATE VIEW [dbo].[v_MasterExecutionSummary]
AS
SELECT 
    mel.[PipelineRunId],
    mel.[StartTime],
    mel.[EndTime],
    mel.[Status],
    mel.[TableCount],
    mel.[SuccessfulTables],
    mel.[FailedTables],
    CASE 
        WHEN mel.[EndTime] IS NOT NULL THEN DATEDIFF(MINUTE, mel.[StartTime], mel.[EndTime])
        ELSE DATEDIFF(MINUTE, mel.[StartTime], GETDATE())
    END AS [DurationMinutes],
    CASE 
        WHEN mel.[TableCount] > 0 THEN 
            CAST(mel.[SuccessfulTables] AS FLOAT) / mel.[TableCount] * 100
        ELSE 0
    END AS [SuccessPercentage]
FROM [dbo].[MasterExecutionLog] mel
WHERE mel.[StartTime] >= DATEADD(DAY, -30, GETDATE()) -- Last 30 days
GO

PRINT 'Metadata database schema created successfully!';