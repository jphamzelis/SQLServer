# =============================================
# Deploy ADF Components for EDW_PSA to Snowflake Ingestion
# This script deploys all linked services, datasets, and pipelines
# =============================================

param(
    [Parameter(Mandatory=$true)]
    [string]$SubscriptionId,
    
    [Parameter(Mandatory=$true)]
    [string]$ResourceGroupName,
    
    [Parameter(Mandatory=$true)]
    [string]$DataFactoryName,
    
    [Parameter(Mandatory=$false)]
    [string]$KeyVaultName = "",
    
    [Parameter(Mandatory=$false)]
    [string]$StorageAccountName = "",
    
    [Parameter(Mandatory=$false)]
    [switch]$WhatIf = $false
)

# Function to write colored output
function Write-ColorOutput {
    param(
        [string]$Message,
        [string]$Color = "White"
    )
    Write-Host $Message -ForegroundColor $Color
}

# Function to deploy ADF component
function Deploy-ADFComponent {
    param(
        [string]$ComponentPath,
        [string]$ComponentType,
        [string]$ComponentName
    )
    
    try {
        if ($WhatIf) {
            Write-ColorOutput "WHATIF: Would deploy $ComponentType '$ComponentName'" "Yellow"
            return $true
        }
        
        Write-ColorOutput "Deploying $ComponentType: $ComponentName..." "Cyan"
        
        # Deploy the component based on type
        switch ($ComponentType) {
            "LinkedService" {
                Set-AzDataFactoryV2LinkedService -ResourceGroupName $ResourceGroupName -DataFactoryName $DataFactoryName -Name $ComponentName -DefinitionFile $ComponentPath -Force
            }
            "Dataset" {
                Set-AzDataFactoryV2Dataset -ResourceGroupName $ResourceGroupName -DataFactoryName $DataFactoryName -Name $ComponentName -DefinitionFile $ComponentPath -Force
            }
            "Pipeline" {
                Set-AzDataFactoryV2Pipeline -ResourceGroupName $ResourceGroupName -DataFactoryName $DataFactoryName -Name $ComponentName -DefinitionFile $ComponentPath -Force
            }
        }
        
        Write-ColorOutput "✓ Successfully deployed $ComponentType: $ComponentName" "Green"
        return $true
    }
    catch {
        Write-ColorOutput "✗ Failed to deploy $ComponentType '$ComponentName': $($_.Exception.Message)" "Red"
        return $false
    }
}

# Main deployment script
try {
    Write-ColorOutput "Starting ADF Component Deployment..." "Green"
    Write-ColorOutput "Subscription: $SubscriptionId" "White"
    Write-ColorOutput "Resource Group: $ResourceGroupName" "White"
    Write-ColorOutput "Data Factory: $DataFactoryName" "White"
    Write-ColorOutput "What-If Mode: $WhatIf" "White"
    Write-ColorOutput "=" * 50 "Gray"
    
    # Connect to Azure
    Write-ColorOutput "Connecting to Azure..." "Cyan"
    Connect-AzAccount -SubscriptionId $SubscriptionId
    
    # Set the subscription context
    Set-AzContext -SubscriptionId $SubscriptionId
    
    # Verify Data Factory exists
    $adf = Get-AzDataFactoryV2 -ResourceGroupName $ResourceGroupName -Name $DataFactoryName -ErrorAction SilentlyContinue
    if (-not $adf) {
        Write-ColorOutput "Data Factory '$DataFactoryName' not found in resource group '$ResourceGroupName'" "Red"
        exit 1
    }
    
    Write-ColorOutput "Data Factory found: $($adf.DataFactoryName)" "Green"
    
    # Get script directory
    $scriptDir = Split-Path -Parent $MyInvocation.MyCommand.Definition
    $rootDir = Split-Path -Parent $scriptDir
    
    # Initialize counters
    $totalComponents = 0
    $successfulDeployments = 0
    $failedDeployments = 0
    
    # Deploy Linked Services (in correct order due to dependencies)
    Write-ColorOutput "`nDeploying Linked Services..." "Yellow"
    $linkedServices = @(
        @{ Name = "LS_KeyVault"; Path = "$rootDir\LinkedServices\LS_KeyVault.json" },
        @{ Name = "LS_EDW_PSA_SqlServer"; Path = "$rootDir\LinkedServices\LS_EDW_PSA_SqlServer.json" },
        @{ Name = "LS_Snowflake_Target"; Path = "$rootDir\LinkedServices\LS_Snowflake_Target.json" },
        @{ Name = "LS_MetadataDB_SqlServer"; Path = "$rootDir\LinkedServices\LS_MetadataDB_SqlServer.json" },
        @{ Name = "LS_AzureStorage"; Path = "$rootDir\LinkedServices\LS_AzureStorage.json" }
    )
    
    foreach ($ls in $linkedServices) {
        $totalComponents++
        if (Test-Path $ls.Path) {
            # Update Key Vault URL if provided
            if ($ls.Name -eq "LS_KeyVault" -and $KeyVaultName -ne "") {
                $content = Get-Content $ls.Path -Raw | ConvertFrom-Json
                $content.properties.typeProperties.baseUrl = "https://$KeyVaultName.vault.azure.net/"
                $tempPath = "$env:TEMP\$($ls.Name)_temp.json"
                $content | ConvertTo-Json -Depth 10 | Set-Content $tempPath
                $deployPath = $tempPath
            } else {
                $deployPath = $ls.Path
            }
            
            if (Deploy-ADFComponent -ComponentPath $deployPath -ComponentType "LinkedService" -ComponentName $ls.Name) {
                $successfulDeployments++
            } else {
                $failedDeployments++
            }
            
            # Clean up temp file
            if ($deployPath -ne $ls.Path -and (Test-Path $deployPath)) {
                Remove-Item $deployPath
            }
        } else {
            Write-ColorOutput "✗ Linked Service file not found: $($ls.Path)" "Red"
            $failedDeployments++
        }
    }
    
    # Deploy Datasets
    Write-ColorOutput "`nDeploying Datasets..." "Yellow"
    $datasets = @(
        @{ Name = "DS_EDW_PSA_Generic"; Path = "$rootDir\Datasets\DS_EDW_PSA_Generic.json" },
        @{ Name = "DS_Snowflake_Generic"; Path = "$rootDir\Datasets\DS_Snowflake_Generic.json" },
        @{ Name = "DS_MetadataDB_Generic"; Path = "$rootDir\Datasets\DS_MetadataDB_Generic.json" }
    )
    
    foreach ($ds in $datasets) {
        $totalComponents++
        if (Test-Path $ds.Path) {
            if (Deploy-ADFComponent -ComponentPath $ds.Path -ComponentType "Dataset" -ComponentName $ds.Name) {
                $successfulDeployments++
            } else {
                $failedDeployments++
            }
        } else {
            Write-ColorOutput "✗ Dataset file not found: $($ds.Path)" "Red"
            $failedDeployments++
        }
    }
    
    # Deploy Pipelines (child pipeline first, then master)
    Write-ColorOutput "`nDeploying Pipelines..." "Yellow"
    $pipelines = @(
        @{ Name = "PL_Copy_Table_EDW_PSA_To_Snowflake"; Path = "$rootDir\Pipelines\PL_Copy_Table_EDW_PSA_To_Snowflake.json" },
        @{ Name = "PL_Master_EDW_PSA_To_Snowflake"; Path = "$rootDir\Pipelines\PL_Master_EDW_PSA_To_Snowflake.json" }
    )
    
    foreach ($pl in $pipelines) {
        $totalComponents++
        if (Test-Path $pl.Path) {
            if (Deploy-ADFComponent -ComponentPath $pl.Path -ComponentType "Pipeline" -ComponentName $pl.Name) {
                $successfulDeployments++
            } else {
                $failedDeployments++
            }
        } else {
            Write-ColorOutput "✗ Pipeline file not found: $($pl.Path)" "Red"
            $failedDeployments++
        }
    }
    
    # Display deployment summary
    Write-ColorOutput "`n" + "=" * 50 "Gray"
    Write-ColorOutput "DEPLOYMENT SUMMARY" "Yellow"
    Write-ColorOutput "=" * 50 "Gray"
    Write-ColorOutput "Total Components: $totalComponents" "White"
    Write-ColorOutput "Successful Deployments: $successfulDeployments" "Green"
    Write-ColorOutput "Failed Deployments: $failedDeployments" "Red"
    
    if ($failedDeployments -eq 0) {
        Write-ColorOutput "`n✓ All components deployed successfully!" "Green"
        
        if (-not $WhatIf) {
            Write-ColorOutput "`nNext Steps:" "Yellow"
            Write-ColorOutput "1. Update Key Vault with connection strings:" "White"
            Write-ColorOutput "   - EDW-PSA-ConnectionString" "Gray"
            Write-ColorOutput "   - Snowflake-ConnectionString" "Gray"
            Write-ColorOutput "   - MetadataDB-ConnectionString" "Gray"
            Write-ColorOutput "   - AzureStorage-ConnectionString" "Gray"
            Write-ColorOutput "2. Run the MetadataDB schema creation script" "White"
            Write-ColorOutput "3. Populate the TableConfig table" "White"
            Write-ColorOutput "4. Test the pipelines with a small subset of tables" "White"
        }
    } else {
        Write-ColorOutput "`n✗ Some components failed to deploy. Please check the errors above." "Red"
        exit 1
    }
}
catch {
    Write-ColorOutput "✗ Deployment failed with error: $($_.Exception.Message)" "Red"
    Write-ColorOutput "Stack Trace: $($_.Exception.StackTrace)" "Gray"
    exit 1
}

Write-ColorOutput "`nDeployment completed." "Green"