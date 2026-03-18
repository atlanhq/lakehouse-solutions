# Setup Guide: Atlan Lakehouse on Azure (ADLS)

This guide covers the cloud-specific setup steps for connecting your **Azure Databricks** workspace to Atlan Lakehouse data stored in **Azure Data Lake Storage (ADLS)**.

## What Atlan Provides

After you contact Atlan Support (see [README](README.md#2-contact-atlan-support)), Atlan will provide:

- **Service Principal credentials**: Directory (Tenant) ID, Application (Client) ID, and Client Secret
- **Storage Account name**
- **Catalog details** (Catalog URI, Catalog Name, Warehouse Name)
- **OAuth credentials** (Client ID and Client Secret)

## Step 1: Create a Storage Credential

Create a [storage credential](https://docs.databricks.com/en/connect/unity-catalog/storage-credentials.html) in Unity Catalog using the Service Principal credentials provided by Atlan.

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

## Step 2: Create an External Location

Create an [external location](https://docs.databricks.com/en/connect/unity-catalog/external-locations.html) in Unity Catalog that points to the ADLS path provided by Atlan.

1. Navigate to **Catalog Explorer > External Locations > Create External Location**
2. Select **Manual** and choose **Azure Data Lake Storage** as the storage type
3. Enter the **ADLS path** provided by Atlan in the following format:
   ```
   abfss://objectstore@<storage-account-name>.dfs.core.windows.net/atlan-wh/
   ```
4. Select the credential created in Step 1
5. In **Advanced Options**, enable **Limit to read-only use**
6. Click **Test Connection** to validate the setup

## Step 3: Create a Target Catalog

Create a new Unity Catalog (or use an existing one) where the foreign Iceberg tables will be registered. This is the catalog name you will configure as `DBX_CATALOG_NAME`.

Ensure that the catalog storage is hosted on your **own Azure tenant**, not on Atlan's tenant.

## Next Steps

Return to the [main README](README.md#configuration) to configure and run the notebooks.
