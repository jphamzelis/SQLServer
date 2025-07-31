-- =============================================
-- Populate Table Configuration for EDW_PSA to Snowflake Ingestion
-- This script generates configuration entries for all tables in EDW_PSA
-- =============================================

USE MetadataDB;
GO

-- Clear existing configuration (optional - uncomment if needed)
-- DELETE FROM [dbo].[TableConfig];

-- =============================================
-- Insert configuration for all dbo schema tables
-- =============================================
INSERT INTO [dbo].[TableConfig] (
    [SourceSchema],
    [SourceTable], 
    [DestinationSchema],
    [DestinationTable],
    [LoadType],
    [WatermarkColumn],
    [IsActive],
    [Priority]
)
SELECT 
    'dbo' AS [SourceSchema],
    TABLE_NAME AS [SourceTable],
    'EDW_PSA' AS [DestinationSchema],
    TABLE_NAME AS [DestinationTable],
    CASE 
        -- Staging tables typically use FULL load
        WHEN TABLE_NAME LIKE '%_Staging' THEN 'FULL'
        WHEN TABLE_NAME LIKE '%_staging' THEN 'FULL'
        -- Hub tables can use incremental if they have date columns
        WHEN TABLE_NAME LIKE 'hub_%' THEN 'INCREMENTAL'
        -- Other tables default to FULL
        ELSE 'FULL'
    END AS [LoadType],
    CASE 
        -- Set watermark column for incremental tables
        WHEN TABLE_NAME LIKE 'hub_%' THEN 'ModifiedDate'
        ELSE NULL
    END AS [WatermarkColumn],
    1 AS [IsActive],
    CASE 
        -- High priority for smaller reference tables
        WHEN TABLE_NAME IN (
            'FlagTypes', 'hub_all_YesNo', 'hub_all_State', 
            'hub_AgencyType', 'hub_all_Lender', 'hub_BANKNAME'
        ) THEN 1
        -- Medium priority for staging tables
        WHEN TABLE_NAME LIKE '%_Staging' OR TABLE_NAME LIKE '%_staging' THEN 2
        -- Lower priority for large hub tables
        ELSE 3
    END AS [Priority]
FROM INFORMATION_SCHEMA.TABLES
WHERE TABLE_SCHEMA = 'dbo'
    AND TABLE_TYPE = 'BASE TABLE'
    AND TABLE_NAME NOT IN (
        -- Exclude system tables or tables that shouldn't be copied
        'sysdiagrams', 'dtproperties'
    );

-- =============================================
-- Insert configuration for archive schema tables
-- =============================================
INSERT INTO [dbo].[TableConfig] (
    [SourceSchema],
    [SourceTable], 
    [DestinationSchema],
    [DestinationTable],
    [LoadType],
    [WatermarkColumn],
    [IsActive],
    [Priority]
)
SELECT 
    'archive' AS [SourceSchema],
    TABLE_NAME AS [SourceTable],
    'EDW_PSA_ARCHIVE' AS [DestinationSchema],
    TABLE_NAME AS [DestinationTable],
    'FULL' AS [LoadType], -- Archive tables typically use full load
    NULL AS [WatermarkColumn],
    1 AS [IsActive],
    4 AS [Priority] -- Lower priority for archive tables
FROM INFORMATION_SCHEMA.TABLES
WHERE TABLE_SCHEMA = 'archive'
    AND TABLE_TYPE = 'BASE TABLE'
    AND TABLE_NAME NOT IN (
        -- Exclude system tables or tables that shouldn't be copied
        'sysdiagrams', 'dtproperties'
    );

-- =============================================
-- Manual configurations for specific tables that need special handling
-- =============================================

-- Update specific tables that should use incremental load with different watermark columns
UPDATE [dbo].[TableConfig] 
SET [WatermarkColumn] = 'CreatedDate'
WHERE [SourceTable] IN (
    'hub_core_audit_records',
    'hub_ExecutionLog',
    'hub_core_integration_bi_dailyfeeds'
) AND [SourceSchema] = 'dbo';

-- Update specific tables that should use incremental load with LastModified
UPDATE [dbo].[TableConfig] 
SET [WatermarkColumn] = 'LastModified'
WHERE [SourceTable] IN (
    'hub_core_customers',
    'hub_core_business_rules',
    'hub_AR_Customer'
) AND [SourceSchema] = 'dbo';

-- Update specific tables that should use incremental load with UpdatedDate
UPDATE [dbo].[TableConfig] 
SET [WatermarkColumn] = 'UpdatedDate'
WHERE [SourceTable] IN (
    'hub_clk_TimeEntries',
    'hub_clk_Users',
    'hub_clk_Projects'
) AND [SourceSchema] = 'dbo';

-- Disable specific tables that might be problematic or not needed
UPDATE [dbo].[TableConfig] 
SET [IsActive] = 0
WHERE [SourceTable] IN (
    -- Add tables that should not be copied
    -- 'problematic_table_name'
);

-- =============================================
-- Update priorities for performance optimization
-- =============================================

-- Set highest priority for small lookup tables (process first)
UPDATE [dbo].[TableConfig] 
SET [Priority] = 1
WHERE [SourceTable] IN (
    'FlagTypes', 'hub_all_YesNo', 'hub_all_State', 'hub_all_Team',
    'hub_AgencyType', 'hub_all_Lender', 'hub_BANKNAME', 'hub_all_Process',
    'hub_chk_Status', 'hub_AR_Division'
) AND [SourceSchema] = 'dbo';

-- Set medium priority for staging and medium-sized tables
UPDATE [dbo].[TableConfig] 
SET [Priority] = 2
WHERE ([SourceTable] LIKE '%_Staging' OR [SourceTable] LIKE '%_staging'
    OR [SourceTable] LIKE 'hub_clk_%' OR [SourceTable] LIKE 'hub_cor_%')
AND [SourceSchema] = 'dbo';

-- Set lower priority for large transaction tables
UPDATE [dbo].[TableConfig] 
SET [Priority] = 3
WHERE [SourceTable] IN (
    'hub_core_audit_records', 'hub_core_fulfillment_orders',
    'hub_AR_TransactionPaymentHistory', 'hub_AP_InvoiceHistoryDetail',
    'hub_AR_InvoiceHistoryDetail', 'hub_ASM_BACMonthlyDetail'
) AND [SourceSchema] = 'dbo';

-- =============================================
-- Display configuration summary
-- =============================================
SELECT 
    [SourceSchema],
    COUNT(*) AS [TableCount],
    SUM(CASE WHEN [LoadType] = 'FULL' THEN 1 ELSE 0 END) AS [FullLoadTables],
    SUM(CASE WHEN [LoadType] = 'INCREMENTAL' THEN 1 ELSE 0 END) AS [IncrementalTables],
    SUM(CASE WHEN [IsActive] = 1 THEN 1 ELSE 0 END) AS [ActiveTables],
    SUM(CASE WHEN [IsActive] = 0 THEN 1 ELSE 0 END) AS [InactiveTables]
FROM [dbo].[TableConfig]
GROUP BY [SourceSchema]

UNION ALL

SELECT 
    'TOTAL' AS [SourceSchema],
    COUNT(*) AS [TableCount],
    SUM(CASE WHEN [LoadType] = 'FULL' THEN 1 ELSE 0 END) AS [FullLoadTables],
    SUM(CASE WHEN [LoadType] = 'INCREMENTAL' THEN 1 ELSE 0 END) AS [IncrementalTables],
    SUM(CASE WHEN [IsActive] = 1 THEN 1 ELSE 0 END) AS [ActiveTables],
    SUM(CASE WHEN [IsActive] = 0 THEN 1 ELSE 0 END) AS [InactiveTables]
FROM [dbo].[TableConfig];

-- =============================================
-- Display priority distribution
-- =============================================
SELECT 
    [Priority],
    COUNT(*) AS [TableCount],
    CASE [Priority]
        WHEN 1 THEN 'High (Small lookup tables)'
        WHEN 2 THEN 'Medium (Staging and medium tables)'
        WHEN 3 THEN 'Low (Large transaction tables)'
        WHEN 4 THEN 'Lowest (Archive tables)'
        ELSE 'Other'
    END AS [Description]
FROM [dbo].[TableConfig]
WHERE [IsActive] = 1
GROUP BY [Priority]
ORDER BY [Priority];

-- =============================================
-- Display sample configuration
-- =============================================
SELECT TOP 20
    [SourceSchema],
    [SourceTable],
    [DestinationSchema],
    [DestinationTable],
    [LoadType],
    [WatermarkColumn],
    [Priority],
    [IsActive]
FROM [dbo].[TableConfig]
ORDER BY [Priority], [SourceSchema], [SourceTable];

PRINT 'Table configuration populated successfully!';
PRINT 'Review the configuration and adjust as needed before running the pipeline.';
PRINT 'Tables marked as IsActive = 0 will be skipped during execution.';