# Azure Data Factory Pipeline Design: EDW_PSA to Snowflake Ingestion

## Overview
This document outlines the design for an Azure Data Factory (ADF) pipeline to ingest all tables from the EDW_PSA (Enterprise Data Warehouse - Persistent Staging Area) to Snowflake. The solution is designed to handle hundreds of tables across multiple schemas with full automation and error handling.

## Architecture Components

### 1. High-Level Architecture
```
EDW_PSA (SQL Server) → ADF → Snowflake
     ↓                ↓         ↓
  - dbo schema    Pipeline   Target DB
  - archive schema   with      with
  - 400+ tables   Metadata   Same Structure
```

### 2. Pipeline Structure

#### Master Pipeline: `PL_Master_EDW_PSA_To_Snowflake`
- **Purpose**: Orchestrates the entire ingestion process
- **Components**:
  - Get table list from metadata
  - Execute child pipelines for each table
  - Handle global error scenarios
  - Send completion notifications

#### Child Pipeline: `PL_Copy_Table_EDW_PSA_To_Snowflake`
- **Purpose**: Handles individual table ingestion
- **Parameters**:
  - `sourceSchema`: Schema name (dbo, archive)
  - `tableName`: Table name
  - `destinationSchema`: Target Snowflake schema
  - `loadType`: FULL, INCREMENTAL
  - `watermarkColumn`: For incremental loads

### 3. Linked Services

#### Source: `LS_EDW_PSA_SqlServer`
```json
{
  "type": "SqlServer",
  "typeProperties": {
    "connectionString": {
      "type": "AzureKeyVaultSecret",
      "store": {
        "referenceName": "LS_KeyVault",
        "type": "LinkedServiceReference"
      },
      "secretName": "EDW-PSA-ConnectionString"
    }
  }
}
```

#### Destination: `LS_Snowflake_Target`
```json
{
  "type": "Snowflake",
  "typeProperties": {
    "connectionString": {
      "type": "AzureKeyVaultSecret",
      "store": {
        "referenceName": "LS_KeyVault",
        "type": "LinkedServiceReference"
      },
      "secretName": "Snowflake-ConnectionString"
    }
  }
}
```

#### Metadata Store: `LS_MetadataDB_SqlServer`
```json
{
  "type": "SqlServer",
  "typeProperties": {
    "connectionString": {
      "type": "AzureKeyVaultSecret",
      "store": {
        "referenceName": "LS_KeyVault",
        "type": "LinkedServiceReference"
      },
      "secretName": "MetadataDB-ConnectionString"
    }
  }
}
```

### 4. Datasets

#### Source Dataset: `DS_EDW_PSA_Generic`
```json
{
  "type": "SqlServerTable",
  "linkedServiceName": {
    "referenceName": "LS_EDW_PSA_SqlServer",
    "type": "LinkedServiceReference"
  },
  "parameters": {
    "schemaName": {
      "type": "string"
    },
    "tableName": {
      "type": "string"
    }
  },
  "typeProperties": {
    "schema": {
      "value": "@dataset().schemaName",
      "type": "Expression"
    },
    "table": {
      "value": "@dataset().tableName",
      "type": "Expression"
    }
  }
}
```

#### Destination Dataset: `DS_Snowflake_Generic`
```json
{
  "type": "SnowflakeTable",
  "linkedServiceName": {
    "referenceName": "LS_Snowflake_Target",
    "type": "LinkedServiceReference"
  },
  "parameters": {
    "schemaName": {
      "type": "string"
    },
    "tableName": {
      "type": "string"
    }
  },
  "typeProperties": {
    "schema": {
      "value": "@dataset().schemaName",
      "type": "Expression"
    },
    "table": {
      "value": "@dataset().tableName",
      "type": "Expression"
    }
  }
}
```

### 5. Metadata Framework

#### Table Configuration: `MetadataDB.dbo.TableConfig`
```sql
CREATE TABLE MetadataDB.dbo.TableConfig (
    ConfigID INT IDENTITY(1,1) PRIMARY KEY,
    SourceSchema NVARCHAR(128) NOT NULL,
    SourceTable NVARCHAR(128) NOT NULL,
    DestinationSchema NVARCHAR(128) NOT NULL,
    DestinationTable NVARCHAR(128) NOT NULL,
    LoadType NVARCHAR(20) NOT NULL, -- FULL, INCREMENTAL
    WatermarkColumn NVARCHAR(128) NULL,
    LastWatermarkValue NVARCHAR(255) NULL,
    IsActive BIT NOT NULL DEFAULT 1,
    Priority INT NOT NULL DEFAULT 1,
    CreatedDate DATETIME2 DEFAULT GETDATE(),
    ModifiedDate DATETIME2 DEFAULT GETDATE()
);
```

#### Execution Log: `MetadataDB.dbo.ExecutionLog`
```sql
CREATE TABLE MetadataDB.dbo.ExecutionLog (
    LogID INT IDENTITY(1,1) PRIMARY KEY,
    PipelineRunId NVARCHAR(255) NOT NULL,
    SourceSchema NVARCHAR(128) NOT NULL,
    SourceTable NVARCHAR(128) NOT NULL,
    StartTime DATETIME2 NOT NULL,
    EndTime DATETIME2 NULL,
    Status NVARCHAR(50) NOT NULL, -- Running, Succeeded, Failed
    RowsCopied BIGINT NULL,
    ErrorMessage NVARCHAR(MAX) NULL,
    CreatedDate DATETIME2 DEFAULT GETDATE()
);
```

## Pipeline Implementation

### Master Pipeline Activities

#### 1. Get Table List
```json
{
  "name": "GetTableList",
  "type": "Lookup",
  "typeProperties": {
    "source": {
      "type": "SqlServerSource",
      "sqlReaderQuery": "SELECT SourceSchema, SourceTable, DestinationSchema, DestinationTable, LoadType, WatermarkColumn, Priority FROM MetadataDB.dbo.TableConfig WHERE IsActive = 1 ORDER BY Priority, SourceSchema, SourceTable"
    },
    "dataset": {
      "referenceName": "DS_MetadataDB_Generic",
      "type": "DatasetReference"
    },
    "firstRowOnly": false
  }
}
```

#### 2. ForEach Table Loop
```json
{
  "name": "ForEachTable",
  "type": "ForEach",
  "dependsOn": [
    {
      "activity": "GetTableList",
      "dependencyConditions": ["Succeeded"]
    }
  ],
  "typeProperties": {
    "items": {
      "value": "@activity('GetTableList').output.value",
      "type": "Expression"
    },
    "batchCount": 5,
    "activities": [
      {
        "name": "ExecuteChildPipeline",
        "type": "ExecutePipeline",
        "typeProperties": {
          "pipeline": {
            "referenceName": "PL_Copy_Table_EDW_PSA_To_Snowflake",
            "type": "PipelineReference"
          },
          "parameters": {
            "sourceSchema": {
              "value": "@item().SourceSchema",
              "type": "Expression"
            },
            "sourceTable": {
              "value": "@item().SourceTable",
              "type": "Expression"
            },
            "destinationSchema": {
              "value": "@item().DestinationSchema",
              "type": "Expression"
            },
            "destinationTable": {
              "value": "@item().DestinationTable",
              "type": "Expression"
            },
            "loadType": {
              "value": "@item().LoadType",
              "type": "Expression"
            },
            "watermarkColumn": {
              "value": "@item().WatermarkColumn",
              "type": "Expression"
            }
          },
          "waitOnCompletion": true
        }
      }
    ]
  }
}
```

### Child Pipeline Activities

#### 1. Log Start
```json
{
  "name": "LogStart",
  "type": "SqlServerStoredProcedure",
  "typeProperties": {
    "storedProcedureName": "MetadataDB.dbo.LogExecutionStart",
    "storedProcedureParameters": {
      "PipelineRunId": {
        "value": "@pipeline().RunId",
        "type": "String"
      },
      "SourceSchema": {
        "value": "@pipeline().parameters.sourceSchema",
        "type": "String"
      },
      "SourceTable": {
        "value": "@pipeline().parameters.sourceTable",
        "type": "String"
      }
    }
  },
  "linkedServiceName": {
    "referenceName": "LS_MetadataDB_SqlServer",
    "type": "LinkedServiceReference"
  }
}
```

#### 2. Copy Data Activity
```json
{
  "name": "CopyTableData",
  "type": "Copy",
  "dependsOn": [
    {
      "activity": "LogStart",
      "dependencyConditions": ["Succeeded"]
    }
  ],
  "typeProperties": {
    "source": {
      "type": "SqlServerSource",
      "queryTimeout": "02:00:00",
      "partitionOption": "None"
    },
    "sink": {
      "type": "SnowflakeSink",
      "preCopyScript": {
        "value": "@if(equals(pipeline().parameters.loadType, 'FULL'), concat('TRUNCATE TABLE ', pipeline().parameters.destinationSchema, '.', pipeline().parameters.destinationTable), '')",
        "type": "Expression"
      }
    },
    "enableStaging": true,
    "stagingSettings": {
      "linkedServiceName": {
        "referenceName": "LS_AzureStorage",
        "type": "LinkedServiceReference"
      },
      "path": "staging/edw-psa-ingestion"
    },
    "parallelCopies": 4,
    "dataIntegrationUnits": 8
  },
  "inputs": [
    {
      "referenceName": "DS_EDW_PSA_Generic",
      "type": "DatasetReference",
      "parameters": {
        "schemaName": {
          "value": "@pipeline().parameters.sourceSchema",
          "type": "Expression"
        },
        "tableName": {
          "value": "@pipeline().parameters.sourceTable",
          "type": "Expression"
        }
      }
    }
  ],
  "outputs": [
    {
      "referenceName": "DS_Snowflake_Generic",
      "type": "DatasetReference",
      "parameters": {
        "schemaName": {
          "value": "@pipeline().parameters.destinationSchema",
          "type": "Expression"
        },
        "tableName": {
          "value": "@pipeline().parameters.destinationTable",
          "type": "Expression"
        }
      }
    }
  ]
}
```

#### 3. Log Success
```json
{
  "name": "LogSuccess",
  "type": "SqlServerStoredProcedure",
  "dependsOn": [
    {
      "activity": "CopyTableData",
      "dependencyConditions": ["Succeeded"]
    }
  ],
  "typeProperties": {
    "storedProcedureName": "MetadataDB.dbo.LogExecutionSuccess",
    "storedProcedureParameters": {
      "PipelineRunId": {
        "value": "@pipeline().RunId",
        "type": "String"
      },
      "RowsCopied": {
        "value": "@activity('CopyTableData').output.rowsCopied",
        "type": "Int64"
      }
    }
  },
  "linkedServiceName": {
    "referenceName": "LS_MetadataDB_SqlServer",
    "type": "LinkedServiceReference"
  }
}
```

#### 4. Log Failure
```json
{
  "name": "LogFailure",
  "type": "SqlServerStoredProcedure",
  "dependsOn": [
    {
      "activity": "CopyTableData",
      "dependencyConditions": ["Failed"]
    }
  ],
  "typeProperties": {
    "storedProcedureName": "MetadataDB.dbo.LogExecutionFailure",
    "storedProcedureParameters": {
      "PipelineRunId": {
        "value": "@pipeline().RunId",
        "type": "String"
      },
      "ErrorMessage": {
        "value": "@activity('CopyTableData').error.message",
        "type": "String"
      }
    }
  },
  "linkedServiceName": {
    "referenceName": "LS_MetadataDB_SqlServer",
    "type": "LinkedServiceReference"
  }
}
```

## Performance Optimization

### 1. Staging Configuration
- Use Azure Blob Storage for staging
- Enable parallel copy with 4-8 parallel streams
- Use appropriate Data Integration Units (8-16 DIUs)

### 2. Snowflake Optimization
- Use COPY INTO with staging files
- Configure appropriate warehouse size
- Implement clustering keys for large tables

### 3. Batch Processing
- Process tables in batches of 5-10 concurrent executions
- Prioritize smaller tables first
- Implement retry logic for failed tables

## Monitoring and Alerting

### 1. Azure Monitor Integration
- Pipeline run status alerts
- Data drift detection
- Performance monitoring

### 2. Custom Monitoring
- Row count validation
- Data quality checks
- Execution time tracking

### 3. Notification Framework
- Email alerts for failures
- Teams notifications for completion
- Dashboard for execution status

## Security Considerations

### 1. Authentication
- Service Principal authentication for ADF
- Key Vault for connection strings
- Managed Identity where possible

### 2. Network Security
- Private endpoints for data sources
- VNet integration for ADF
- Firewall rules for Snowflake

### 3. Data Security
- Encryption in transit and at rest
- Row-level security in Snowflake
- Audit logging for data access

## Deployment Strategy

### 1. Environment Promotion
- Development → Test → Production
- ARM templates for infrastructure
- CI/CD pipelines for deployment

### 2. Configuration Management
- Environment-specific parameters
- Table configuration management
- Version control for all components

## Cost Optimization

### 1. Compute Optimization
- Auto-pause for Snowflake warehouses
- Appropriate sizing for ADF DIUs
- Schedule-based execution

### 2. Storage Optimization
- Data compression in Snowflake
- Lifecycle policies for staging data
- Efficient data types mapping

## Maintenance and Operations

### 1. Regular Maintenance
- Update table configurations
- Monitor and tune performance
- Clean up old execution logs

### 2. Disaster Recovery
- Backup and restore procedures
- Cross-region replication
- Recovery time objectives (RTO/RPO)

## Implementation Timeline

### Phase 1 (Week 1-2): Infrastructure Setup
- Create linked services
- Set up metadata framework
- Configure security

### Phase 2 (Week 3-4): Pipeline Development
- Develop master and child pipelines
- Implement error handling
- Create monitoring solutions

### Phase 3 (Week 5-6): Testing and Validation
- Unit testing of pipelines
- Data validation testing
- Performance testing

### Phase 4 (Week 7-8): Production Deployment
- Deploy to production
- Monitor initial runs
- Documentation and training