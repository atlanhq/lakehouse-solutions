# Databricks Foreign Iceberg Tables

Databricks does not currently support querying federated (external) Iceberg REST catalogs natively. These scripts provide a workaround by creating "foreign" Iceberg table references in Unity Catalog that point directly to the Atlan Lakehouse Iceberg metadata files stored in cloud object storage (Amazon S3 or ADLS).

Two scripts are provided:

- **Create** (`dbx_foreign_iceberg_tables_create.py`) — discovers all namespaces and tables in the Atlan Lakehouse catalog and registers them as foreign Iceberg tables in a target Unity Catalog.
- **Refresh** (`dbx_foreign_iceberg_tables_refresh.py`) — updates the metadata pointers for all previously created tables so they reflect the latest data.

Long-term, when Databricks adds native support for Iceberg REST Catalog federation, these scripts will no longer be needed.

## Prerequisites

### 1. Enable Databricks Private Preview Feature

Contact your Databricks account representative and request enablement of the following command:

```
CREATE TABLE ... UNIFORM ICEBERG ... METADATA_PATH
```

This is a Private Preview feature (Foreign Iceberg Tables Private Preview) and must be enabled on your Databricks workspace before proceeding. Once enabled, notify Atlan Support so they can continue with the setup.

### 2. Contact Atlan Support

After the Private Preview feature is confirmed enabled, reach out to Atlan Support. Atlan will provide:

- **Storage access details** (depends on your cloud):
  - **AWS**: IAM Role ARN and Amazon S3 Bucket Name
  - **Azure**: Service Principal credentials (Directory ID, Application ID, Client Secret) and Storage Account name
- **Catalog details**:
  - Catalog URI
  - Catalog Name
  - Warehouse Name
- **OAuth credentials** (Client ID and Client Secret)

You can also find your Catalog details in the Atlan UI:
**Workflows > Marketplace > Atlan Lakehouse > Connection Details**

### 3. Create a Storage Credential in Databricks

Create a [storage credential](https://docs.databricks.com/en/connect/unity-catalog/storage-credentials.html) in Unity Catalog using the details provided by Atlan. Follow the option that matches your cloud environment.

#### Option A: AWS (IAM Role)

Create a storage credential using the IAM Role ARN provided by Atlan.

1. Navigate to **Catalog Explorer > Credentials > Create Credential**
2. Select **AWS IAM Role** as the credential type
3. Enter the **IAM Role ARN** provided by Atlan
4. In **Advanced Options**, enable **Limit to read-only use**

Once created, share the following back with Atlan Support:
- The **IAM Role ARN** of the storage credential you created
- The **External ID** associated with the credential

Atlan will update their trust policy to allow your credential to access the Amazon S3 bucket.

#### Option B: Azure (Service Principal)

Create a storage credential using the Azure Service Principal credentials provided by Atlan.

**Using the Databricks CLI:**

```
databricks storage-credentials create --json '{
  "name": "<credential-name>",
  "azure_service_principal": {
    "directory_id": "<DIRECTORY_ID>",
    "application_id": "<APPLICATION_ID>",
    "client_secret": "<CLIENT_SECRET>"
  }
}'
```

Replace the placeholders with the Service Principal credentials provided by Atlan.

**Or using the UI:**

1. Navigate to **Catalog Explorer > Credentials > Create Credential**
2. Select **Azure Service Principal** as the credential type
3. Enter the **Directory (Tenant) ID**, **Application (Client) ID**, and **Client Secret** provided by Atlan

### 4. Create an External Location

Create an [external location](https://docs.databricks.com/en/connect/unity-catalog/external-locations.html) in Unity Catalog that points to the storage path provided by Atlan, using the storage credential from Step 3.

#### Option A: AWS (Amazon S3)

1. Navigate to **Catalog Explorer > External Locations > Create External Location**
2. Select **Manual** and choose **Amazon S3** as the storage type
3. Enter the **Amazon S3 path** provided by Atlan (e.g., `s3://<bucket-name>/<catalog-name>`)
4. Select the credential created in Step 3
5. Enable **Read-only mode** in advanced options

#### Option B: Azure (ADLS)

1. Navigate to **Catalog Explorer > External Locations > Create External Location**
2. Select **Manual** and choose **Azure Data Lake Storage** as the storage type
3. Enter the **ADLS path** provided by Atlan in the following format:
   ```
   abfss://objectstore@<storage-account-name>.dfs.core.windows.net/atlan-wh/
   ```
4. Select the credential created in Step 3
5. In **Advanced Options**, enable **Limit to read-only use**
6. Click **Test Connection** to validate the setup

### 5. Create a Target Catalog

Create a new Unity Catalog (or use an existing one) where the foreign Iceberg tables will be registered. This is the catalog name you will configure as `DBX_CATALOG_NAME`.

### 6. Install pyiceberg

The scripts require the `pyiceberg` library. This is handled automatically by the first line of each notebook:

```
%pip install pyiceberg
```

## Configuration

The scripts use the following configuration values, which can be set as environment variables or edited directly in the script:

| Variable | Description |
|----------|-------------|
| `CLIENT_ID` | OAuth Client ID provided by Atlan |
| `CLIENT_SECRET` | OAuth Client Secret provided by Atlan |
| `POLARIS_CATALOG_URI` | Catalog URI (e.g., `https://<tenant>.atlan.com/api/polaris/api/catalog`) |
| `CATALOG_NAME` | Polaris catalog name provided by Atlan |
| `WAREHOUSE_NAME` | Polaris warehouse name provided by Atlan |
| `DBX_CATALOG_NAME` | Target Unity Catalog name where tables will be created |
| `HISTORY_NAMESPACE_SYNC` | Set to `true` to include `atlan-history` namespace (default: `false`) |

## Usage

### Import as Databricks Notebooks

Both scripts are structured with `# COMMAND ----------` delimiters and can be imported directly as Databricks notebooks.

### Initial Setup

1. Import `dbx_foreign_iceberg_tables_create.py` as a notebook
2. Configure the variables in the Configuration cell
3. Run the notebook — it will:
   - Auto-detect the Polaris warehouse (`atlan-wh` preferred, `context_store` fallback)
   - Discover all namespaces and tables
   - Create schemas and foreign Iceberg tables in the target Unity Catalog

### Ongoing Refresh

1. Import `dbx_foreign_iceberg_tables_refresh.py` as a notebook
2. Configure the same variables as the create script
3. Schedule it to run periodically:
   - **Recommended**: Daily at ~4:00 AM (overnight compactions run at ~3:00 AM)
   - **Maximum frequency**: No more than every 30 minutes

## Notes

- By default, the `atlan-history` namespace is skipped. Set `HISTORY_NAMESPACE_SYNC=true` to include it.
- The create script uses `CREATE TABLE IF NOT EXISTS`, so it is safe to re-run.
- The refresh script uses `REFRESH TABLE` to update metadata pointers without recreating tables.
