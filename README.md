# EDW_PSA to Snowflake Ingestion Pipeline

A comprehensive Azure Data Factory (ADF) solution for ingesting all tables from the EDW_PSA (Enterprise Data Warehouse - Persistent Staging Area) to Snowflake with full automation, error handling, and monitoring capabilities.

## 🏗️ Architecture Overview

```
EDW_PSA (SQL Server) → Azure Data Factory → Snowflake
     ↓                      ↓                  ↓
  400+ Tables          Master Pipeline    Target Database
  - dbo schema         - Metadata driven  - Same structure
  - archive schema     - Error handling   - Optimized storage
                       - Monitoring
```

## 📋 Features

### Core Capabilities
- **Metadata-driven ingestion** of 400+ tables across multiple schemas
- **Configurable load types**: Full and incremental loading
- **Parallel processing** with configurable batch sizes
- **Comprehensive error handling** and retry mechanisms
- **Data validation** with row count verification
- **Audit logging** and execution tracking
- **Automated notifications** via Teams/Email
- **Performance optimization** with priority-based execution

### Technical Features
- **Azure Blob Storage staging** for large data transfers
- **Snowflake COPY INTO** optimization
- **Azure Key Vault** integration for secure connection management
- **Parameterized pipelines** for flexibility
- **Watermark-based incremental loading**
- **Real-time monitoring** and alerting

## 🗂️ Project Structure

```
📁 Project Root
├── 📄 ADF_Pipeline_Design.md          # Comprehensive design document
├── 📄 README.md                       # This file
│
├── 📁 LinkedServices/                 # ADF Linked Services
│   ├── LS_KeyVault.json
│   ├── LS_EDW_PSA_SqlServer.json
│   ├── LS_Snowflake_Target.json
│   ├── LS_MetadataDB_SqlServer.json
│   └── LS_AzureStorage.json
│
├── 📁 Datasets/                       # ADF Datasets
│   ├── DS_EDW_PSA_Generic.json
│   ├── DS_Snowflake_Generic.json
│   └── DS_MetadataDB_Generic.json
│
├── 📁 Pipelines/                      # ADF Pipelines
│   ├── PL_Master_EDW_PSA_To_Snowflake.json
│   └── PL_Copy_Table_EDW_PSA_To_Snowflake.json
│
├── 📁 SQL/                           # Database Scripts
│   ├── MetadataDB_Schema.sql
│   └── PopulateTableConfig.sql
│
└── 📁 PowerShell/                    # Deployment Scripts
    └── DeployADFComponents.ps1
```

## 🚀 Quick Start

### Prerequisites

1. **Azure Resources**:
   - Azure Data Factory V2
   - Azure Key Vault
   - Azure Storage Account
   - SQL Server database for metadata

2. **Access Requirements**:
   - Contributor access to Azure Data Factory
   - Access to EDW_PSA SQL Server database
   - Snowflake database access
   - Key Vault Secret Management permissions

3. **Tools**:
   - PowerShell with Azure PowerShell module
   - SQL Server Management Studio or equivalent
   - Azure CLI (optional)

### 5-Minute Setup

1. **Clone/Download** this repository
2. **Run** the PowerShell deployment script:
   ```powershell
   .\PowerShell\DeployADFComponents.ps1 -SubscriptionId "your-subscription-id" -ResourceGroupName "your-rg" -DataFactoryName "your-adf"
   ```
3. **Configure** connection strings in Key Vault
4. **Execute** the metadata database setup scripts
5. **Test** with a small subset of tables

## 📖 Detailed Setup Guide

### Step 1: Deploy Infrastructure Components

#### 1.1 Deploy ADF Components
```powershell
# Navigate to the project directory
cd /path/to/edw-psa-snowflake-ingestion

# Run deployment script (What-If mode first)
.\PowerShell\DeployADFComponents.ps1 `
    -SubscriptionId "12345678-1234-1234-1234-123456789012" `
    -ResourceGroupName "rg-data-platform" `
    -DataFactoryName "adf-edw-ingestion" `
    -KeyVaultName "kv-data-platform" `
    -WhatIf

# Run actual deployment
.\PowerShell\DeployADFComponents.ps1 `
    -SubscriptionId "12345678-1234-1234-1234-123456789012" `
    -ResourceGroupName "rg-data-platform" `
    -DataFactoryName "adf-edw-ingestion" `
    -KeyVaultName "kv-data-platform"
```

#### 1.2 Configure Key Vault Secrets
Add the following secrets to your Azure Key Vault:

```bash
# EDW_PSA SQL Server connection
az keyvault secret set --vault-name "kv-data-platform" --name "EDW-PSA-ConnectionString" --value "Server=server.database.windows.net;Database=EDW_PSA;Authentication=Active Directory Integrated;"

# Snowflake connection  
az keyvault secret set --vault-name "kv-data-platform" --name "Snowflake-ConnectionString" --value "Server=account.snowflakecomputing.com;Account=account;User=username;Password=password;Database=EDW_TARGET;Warehouse=COMPUTE_WH;"

# Metadata database connection
az keyvault secret set --vault-name "kv-data-platform" --name "MetadataDB-ConnectionString" --value "Server=server.database.windows.net;Database=MetadataDB;Authentication=Active Directory Integrated;"

# Azure Storage connection
az keyvault secret set --vault-name "kv-data-platform" --name "AzureStorage-ConnectionString" --value "DefaultEndpointsProtocol=https;AccountName=storageaccount;AccountKey=key;EndpointSuffix=core.windows.net"
```

### Step 2: Setup Metadata Database

#### 2.1 Create Metadata Schema
```sql
-- Execute in SQL Server Management Studio
-- Connect to your metadata database
sqlcmd -S server.database.windows.net -d MetadataDB -i "SQL\MetadataDB_Schema.sql"
```

#### 2.2 Populate Table Configuration
```sql
-- Review and execute the table configuration script
sqlcmd -S server.database.windows.net -d MetadataDB -i "SQL\PopulateTableConfig.sql"
```

### Step 3: Configure Snowflake Target

#### 3.1 Create Target Schemas
```sql
-- Execute in Snowflake
USE WAREHOUSE COMPUTE_WH;

-- Create schemas for different source schemas
CREATE SCHEMA IF NOT EXISTS EDW_PSA;
CREATE SCHEMA IF NOT EXISTS EDW_PSA_ARCHIVE;

-- Grant permissions
GRANT USAGE ON SCHEMA EDW_PSA TO ROLE DATA_ENGINEER;
GRANT CREATE TABLE ON SCHEMA EDW_PSA TO ROLE DATA_ENGINEER;
GRANT USAGE ON SCHEMA EDW_PSA_ARCHIVE TO ROLE DATA_ENGINEER;
GRANT CREATE TABLE ON SCHEMA EDW_PSA_ARCHIVE TO ROLE DATA_ENGINEER;
```

#### 3.2 Create Target Tables (Optional)
You can pre-create tables in Snowflake or let ADF create them automatically during the first run.

### Step 4: Test and Validate

#### 4.1 Test with Single Table
1. Go to Azure Data Factory Studio
2. Navigate to `PL_Copy_Table_EDW_PSA_To_Snowflake`
3. Click **Debug** and provide parameters:
   ```json
   {
     "sourceSchema": "dbo",
     "sourceTable": "FlagTypes",
     "destinationSchema": "EDW_PSA", 
     "destinationTable": "FlagTypes",
     "loadType": "FULL",
     "watermarkColumn": ""
   }
   ```

#### 4.2 Test Master Pipeline with Subset
1. Temporarily disable most tables in `TableConfig`:
   ```sql
   UPDATE MetadataDB.dbo.TableConfig 
   SET IsActive = 0 
   WHERE SourceTable NOT IN ('FlagTypes', 'hub_all_YesNo', 'hub_all_State');
   ```
2. Run `PL_Master_EDW_PSA_To_Snowflake`
3. Verify successful execution in monitoring views

#### 4.3 Enable All Tables
```sql
UPDATE MetadataDB.dbo.TableConfig 
SET IsActive = 1;
```

## ⚙️ Configuration

### Table Configuration Options

The `TableConfig` table controls how each table is processed:

| Column | Description | Example Values |
|--------|-------------|----------------|
| `SourceSchema` | Source schema name | `dbo`, `archive` |
| `SourceTable` | Source table name | `hub_AccountingRevenueByLender` |
| `DestinationSchema` | Target Snowflake schema | `EDW_PSA`, `EDW_PSA_ARCHIVE` |
| `DestinationTable` | Target table name | Same as source or renamed |
| `LoadType` | Loading strategy | `FULL`, `INCREMENTAL` |
| `WatermarkColumn` | Column for incremental loads | `ModifiedDate`, `CreatedDate` |
| `IsActive` | Enable/disable table | `1` (active), `0` (inactive) |
| `Priority` | Execution priority | `1` (high) to `4` (low) |

### Performance Tuning

#### Parallel Execution
- Modify `batchCount` in master pipeline (default: 5)
- Adjust `parallelCopies` in copy activity (default: 4)
- Configure `dataIntegrationUnits` (default: 8)

#### Priority-Based Execution
1. **Priority 1**: Small lookup tables (processed first)
2. **Priority 2**: Medium-sized staging tables
3. **Priority 3**: Large transaction tables
4. **Priority 4**: Archive tables (processed last)

#### Load Type Selection
- **FULL**: Truncate and reload entire table
- **INCREMENTAL**: Load only new/modified records based on watermark

### Monitoring Configuration

#### Teams Notifications
Update webhook URLs in master pipeline:
```json
{
  "url": "https://your-org.webhook.office.com/webhookb2/...",
  "method": "POST",
  "headers": {
    "Content-Type": "application/json"
  }
}
```

#### Custom Monitoring Queries
```sql
-- Current execution status
SELECT * FROM MetadataDB.dbo.v_CurrentExecutionStatus
WHERE PipelineRunId = 'your-run-id';

-- Master execution summary
SELECT * FROM MetadataDB.dbo.v_MasterExecutionSummary
ORDER BY StartTime DESC;

-- Failed table details
SELECT SourceSchema, SourceTable, ErrorMessage, StartTime
FROM MetadataDB.dbo.ExecutionLog
WHERE Status = 'Failed'
ORDER BY StartTime DESC;
```

## 🔧 Troubleshooting

### Common Issues

#### 1. Connection Failures
**Symptoms**: Pipeline fails with authentication errors
**Solution**: 
- Verify Key Vault access permissions
- Check connection string formats
- Test connections manually

#### 2. Snowflake Performance Issues
**Symptoms**: Slow copy operations
**Solution**:
- Increase Snowflake warehouse size
- Enable staging with compression
- Optimize data types mapping

#### 3. Memory/Timeout Issues
**Symptoms**: Large table copies fail
**Solution**:
- Increase copy activity timeout
- Use table partitioning
- Process large tables separately

#### 4. Data Validation Failures
**Symptoms**: Row count mismatches
**Solution**:
- Check for active transactions during copy
- Verify data type mappings
- Review Snowflake import settings

### Debugging Steps

1. **Check Execution Logs**:
   ```sql
   SELECT TOP 10 * FROM MetadataDB.dbo.ExecutionLog 
   ORDER BY StartTime DESC;
   ```

2. **Review Pipeline Run Details** in ADF Studio

3. **Validate Staging Files** in Azure Storage

4. **Check Snowflake Query History**

## 🚀 Performance Metrics

### Expected Performance
- **Small tables** (<1M rows): 2-5 minutes
- **Medium tables** (1M-10M rows): 10-30 minutes  
- **Large tables** (>10M rows): 30-120 minutes
- **Full pipeline** (400+ tables): 4-8 hours

### Optimization Recommendations
- Run during off-peak hours
- Use dedicated Snowflake warehouse
- Monitor and adjust DIU allocation
- Consider table partitioning for very large tables

## 📊 Monitoring and Alerts

### Built-in Monitoring
- Real-time pipeline execution status
- Row count validation
- Error logging and notification
- Performance metrics tracking

### Custom Dashboards
Create Power BI or Grafana dashboards using:
- `v_CurrentExecutionStatus` view
- `v_MasterExecutionSummary` view
- Azure Monitor metrics

### Alert Conditions
- Pipeline execution failures
- Data validation errors
- Performance degradation
- Unexpected row count changes

## 🛡️ Security Best Practices

### Access Control
- Use Azure RBAC for ADF access
- Implement least-privilege access
- Regular access reviews

### Data Protection
- Enable encryption in transit and at rest
- Use private endpoints where possible
- Implement network security groups

### Audit and Compliance
- Enable ADF activity logging
- Monitor data access patterns
- Maintain execution audit trails

## 📚 Additional Resources

- [Azure Data Factory Documentation](https://docs.microsoft.com/en-us/azure/data-factory/)
- [Snowflake Connector for ADF](https://docs.microsoft.com/en-us/azure/data-factory/connector-snowflake)
- [ADF Performance Tuning Guide](https://docs.microsoft.com/en-us/azure/data-factory/copy-activity-performance)

## 🤝 Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Test thoroughly
5. Submit a pull request

## 📝 License

This project is licensed under the MIT License - see the LICENSE file for details.

## 📞 Support

For issues and questions:
1. Check the troubleshooting section
2. Review ADF pipeline run details
3. Check execution logs in metadata database
4. Contact the data platform team

---

**Last Updated**: January 2024  
**Version**: 1.0.0