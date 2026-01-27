# MDLH Setup - Databricks Notebooks

These notebooks sync Polaris catalog tables to Unity Catalog in Databricks, enabling you to access your MDLH (Metadata Lakehouse) data directly from Databricks.

## Overview

This directory contains two notebooks for managing MDLH setup in Databricks:

1. **`databricks_create_mdlh_setup.py`**: Creates foreign Iceberg tables in Databricks using metadata from a Polaris catalog. Use this notebook for initial setup.
2. **`databricks_refresh_mdlh_setup.py`**: Refreshes existing foreign Iceberg tables with updated metadata paths. Use this notebook to update tables after Polaris metadata changes.

Both notebooks:
- Connect to the Polaris catalog
- Get metadata paths for all tables
- Apply the same table name transformations
- Create or refresh Unity Catalog schemas and tables that reference Polaris Iceberg tables

## Prerequisites Setup

**⚠️ Important**: Please note that the Databricks target catalog and external storage location need to be created before running these notebooks.

**For Prerequisites Setup, please reach out to Atlan support.**

### Prerequisites

1. **Databricks Cluster**: Please use only **All Purpose compute** Databricks cluster with **16.4+ version** to run/schedule these notebooks.
2. **Foreign Iceberg Tables Private Preview**: Please make sure **"Foreign Iceberg Tables Private Preview"** is enabled to your Databricks account.
3. **Databricks Workspace** with Unity Catalog enabled
4. **Target Unity Catalog** created in Databricks (contact support for setup)
5. **External Storage Location** configured (contact support for setup)
6. **Polaris Credentials**:
   - Client ID
   - Client Secret
   - Polaris Catalog URI
   - Polaris Catalog Name
   - Polaris Warehouse Name

## Configuration

Before running the notebook, configure the following environment variables:

### Required Environment Variables

- `CLIENT_ID`: Your Polaris reader client ID
- `CLIENT_SECRET`: Your Polaris reader client secret
- `POLARIS_CATALOG_URI`: Polaris catalog API endpoint (e.g., `https://<tenant-domain>.atlan.com/api/polaris/api/catalog`)
- `CATALOG_NAME`: Polaris catalog name (optional, will be auto-detected)
- `WAREHOUSE_NAME`: Polaris warehouse name (optional, will be auto-detected)
- `DBX_CATALOG_NAME`: Target Unity Catalog name in Databricks (e.g., `ATLAN_CONTEXT_STORE`)

### Optional Environment Variables

- `HISTORY_NAMESPACE_SYNC`: Set to `"true"` to sync `atlan-history` namespace (default: `"false"`)

## Setup Instructions

### Option 1: Using Databricks Secrets

1. Store your credentials in Databricks Secrets:
   ```python
   # In a separate notebook or using Databricks CLI
   dbutils.secrets.put(scope="polaris", key="client_id", value="<your-client-id>")
   dbutils.secrets.put(scope="polaris", key="client_secret", value="<your-client-secret>")
   ```

2. Update the notebook to use secrets:
   ```python
   CLIENT_ID = dbutils.secrets.get(scope="polaris", key="client_id")
   CLIENT_SECRET = dbutils.secrets.get(scope="polaris", key="client_secret")
   ```

### Option 2: Using Environment Variables

Set environment variables in your Databricks cluster configuration or job settings:

- Go to **Compute** → Select your cluster → **Advanced Options** → **Environment Variables**
- Add the required environment variables

### Option 3: Direct Configuration in Notebook

Modify the configuration section in the notebook to set values directly (not recommended for production):

```python
CLIENT_ID = "<your-client-id>"
CLIENT_SECRET = "<your-client-secret>"
POLARIS_CATALOG_URI = "https://<tenant-domain>.atlan.com/api/polaris/api/catalog"
DBX_CATALOG_NAME = "ATLAN_CONTEXT_STORE"
```

## Running the Notebooks

### Initial Setup: Create Tables

Use `databricks_create_mdlh_setup.py` for the first-time setup:

1. **Import the Notebook**:
   - In Databricks, go to **Workspace** → **Import**
   - Upload `databricks_create_mdlh_setup.py` or clone from the repository

2. **Attach to Cluster**:
   - Ensure your cluster has Unity Catalog enabled
   - Attach the notebook to the cluster

3. **Configure Variables**:
   - Set all required environment variables (see Configuration section above)

4. **Run the Notebook**:
   - Click **Run All** or run cells sequentially
   - The notebook will:
     - Connect to the Polaris catalog
     - Detect the catalog automatically 
     - List all namespaces in the catalog
     - Create Unity Catalog schemas for each namespace
     - Create Unity Catalog tables that reference Polaris Iceberg tables using `CREATE TABLE IF NOT EXISTS`

### Refresh Tables: Update Metadata Paths

Use `databricks_refresh_mdlh_setup.py` to refresh existing tables with updated metadata:

1. **Import the Notebook**:
   - In Databricks, go to **Workspace** → **Import**
   - Upload `databricks_refresh_mdlh_setup.py` or clone from the repository

2. **Attach to Cluster**:
   - Ensure your cluster has Unity Catalog enabled
   - Attach the notebook to the cluster

3. **Configure Variables**:
   - Set all required environment variables (see Configuration section above)

4. **Run the Notebook**:
   - Click **Run All** or run cells sequentially
   - The notebook will:
     - Connect to the Polaris catalog
     - Get updated metadata paths for all tables
     - Refresh existing Databricks tables with new metadata paths using `REFRESH TABLE`
     - Apply the same table name transformations

## Scheduling the Notebooks

To keep your MDLH setup in sync, schedule the refresh notebook to run periodically:

1. **Create a Job**:
   - Go to **Workflows** → **Create Job**
   - Add `databricks_refresh_mdlh_setup.py` as a task
   - Configure the cluster or use an existing cluster

2. **Set Schedule**:
   - Click **Schedule** → **Add schedule**
   - Configure frequency (e.g., daily, hourly)
   - Set timezone and start time

3. **Configure Environment Variables**:
   - In job settings, add environment variables under **Task** → **Advanced** → **Environment Variables**

**Note**: Typically, you only need to run `databricks_create_mdlh_setup.py` once for initial setup. Schedule `databricks_refresh_mdlh_setup.py` to run periodically to keep metadata paths up to date.

## How It Works

### Create Notebook (`databricks_create_mdlh_setup.py`)

1. **Catalog Detection**: The notebook automatically detects the Polaris catalog by trying `atlan-wh` first, then `context_store`.

2. **Namespace Processing**: For each namespace found:
   - Creates a Unity Catalog schema if it doesn't exist
   - Lists all tables in the namespace
   - Creates Unity Catalog tables using `UNIFORM ICEBERG` format

3. **Table Creation**: Each table is created with:
   - `CREATE TABLE IF NOT EXISTS` (idempotent)
   - `UNIFORM ICEBERG` format for cross-catalog access
   - `METADATA_PATH` pointing to the Polaris table metadata location

### Refresh Notebook (`databricks_refresh_mdlh_setup.py`)

1. **Catalog Connection**: Connects to the Polaris catalog using the same detection logic.

2. **Metadata Path Updates**: For each existing table:
   - Retrieves the latest metadata path from Polaris
   - Refreshes the Databricks table with `REFRESH TABLE` and the new `METADATA_PATH`
   - Applies the same table name transformations as the create notebook

3. **Namespace Processing**: Processes all namespaces (respects `HISTORY_NAMESPACE_SYNC` flag)

4. **History Namespace**: By default, the `atlan-history` namespace is skipped. Set `HISTORY_NAMESPACE_SYNC=true` to include it.

## Troubleshooting

### Error: "No valid Polaris catalog found"
- Verify your `POLARIS_CATALOG_URI` is correct
- Check that `CLIENT_ID` and `CLIENT_SECRET` are valid
- Ensure your Polaris catalog is accessible from Databricks

### Error: "Failed creating schema"
- Verify you have permissions to create schemas in the target Unity Catalog
- Check that `DBX_CATALOG_NAME` is correct

### Error: "No metadata location for table"
- This indicates the Polaris table doesn't have metadata. Contact Atlan support.

### Tables not appearing
- Check the notebook logs for errors
- Verify Unity Catalog permissions
- Ensure the cluster has network access to Polaris

## Example Usage

After running the notebook, you can query the synced tables:

```sql
-- List all schemas
SHOW SCHEMAS IN <DBX_CATALOG_NAME>;

-- List tables in a schema
SHOW TABLES IN <DBX_CATALOG_NAME>.<namespace>;

-- Query a table
SELECT * FROM <DBX_CATALOG_NAME>.<namespace>.<table_name> LIMIT 10;
```

## Notes

- **Create Notebook**: Uses `CREATE TABLE IF NOT EXISTS`, so it's safe to run multiple times
- **Refresh Notebook**: Uses `REFRESH TABLE` to update existing tables with new metadata paths
- Tables are created as `UNIFORM ICEBERG` format, allowing cross-catalog queries
- The `atlan-history` namespace is skipped by default to avoid syncing large history tables
- All operations are logged for debugging purposes
- Both notebooks apply the same table name transformations for consistency

## Support

For issues or questions:
- Check the notebook logs for detailed error messages
- Verify all environment variables are set correctly
- Ensure network connectivity between Databricks and Polaris
- Contact Atlan support for Polaris-specific issues
