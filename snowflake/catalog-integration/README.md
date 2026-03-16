# Snowflake Catalog Integration

Snowflake supports native Iceberg REST Catalog federation, allowing you to query the Atlan Lakehouse directly from Snowflake without any external scripts or scheduled refresh jobs. This guide walks through creating an External Volume, Catalog Integration, and Linked Database to connect your Snowflake environment to the Atlan Lakehouse.

## Prerequisites

### 1. Contact Atlan Support

Reach out to Atlan Support to initiate setup. Atlan will provide:

- **GCS Bucket Name** and **GCS Prefix** for storage access
- **Catalog details**:
  - Catalog URI
  - Catalog Name
- **OAuth credentials** (Polaris Reader ID and Reader Secret)

You can also find your Catalog details in the Atlan UI:
**Workflows > Marketplace > Atlan Lakehouse > Connection Details**

### 2. Create an External Volume

Create an external volume in Snowflake using GCS as the storage provider:

```sql
CREATE EXTERNAL VOLUME <volume_name>
  STORAGE_LOCATIONS = (
    (
      NAME = '<location_name>'
      STORAGE_PROVIDER = 'GCS'
      STORAGE_BASE_URL = 'gcs://<gcs-bucket-name>/<gcs-prefix>/'
    )
  )
  ALLOW_WRITES = FALSE;
```

**Example:**

```sql
CREATE EXTERNAL VOLUME mdlh_gcs_volume
  STORAGE_LOCATIONS = (
    (
      NAME = 'atlan-mdlh-gcs'
      STORAGE_PROVIDER = 'GCS'
      STORAGE_BASE_URL = 'gcs://atlan-vcluster-example/atlan-wh/'
    )
  )
  ALLOW_WRITES = FALSE;
```

### 3. Share the GCP Service Account with Atlan

Once the volume is created, retrieve the Snowflake GCP service account:

```sql
DESC EXTERNAL VOLUME <volume_name>;
```

Locate the `STORAGE_GCP_SERVICE_ACCOUNT` property in the output and send this service account email to Atlan Support. Atlan will use it to grant read access to the GCS bucket.

### 4. Wait for Atlan Confirmation

Atlan must grant the service account read access to the GCS bucket before you can proceed. You will be notified when this step is complete.

### 5. Verify External Volume Access

Confirm that Snowflake can access the bucket:

```sql
SELECT SYSTEM$VERIFY_EXTERNAL_VOLUME('<volume_name>');
```

### 6. Create a Catalog Integration

Create a catalog integration that connects to the Atlan Polaris catalog:

```sql
CREATE OR REPLACE CATALOG INTEGRATION <integration_name>
  CATALOG_SOURCE = POLARIS
  TABLE_FORMAT = ICEBERG
  CATALOG_NAMESPACE = 'atlan-ns'
  REST_CONFIG = (
    CATALOG_URI = '<catalog_uri>'
    CATALOG_NAME = '<catalog_name>'
  )
  REST_AUTHENTICATION = (
    TYPE = OAUTH
    OAUTH_CLIENT_ID = '<polaris_reader_id>'
    OAUTH_CLIENT_SECRET = '<polaris_reader_secret>'
    OAUTH_ALLOWED_SCOPES = ('PRINCIPAL_ROLE:lake_readers')
  )
  ENABLED = TRUE;
```

**Example:**

```sql
CREATE OR REPLACE CATALOG INTEGRATION atlan_mdlh_catalog
  CATALOG_SOURCE = POLARIS
  TABLE_FORMAT = ICEBERG
  CATALOG_NAMESPACE = 'atlan-ns'
  REST_CONFIG = (
    CATALOG_URI = 'https://example.atlan.com/api/polaris/api/catalog'
    CATALOG_NAME = 'atlan-wh'
  )
  REST_AUTHENTICATION = (
    TYPE = OAUTH
    OAUTH_CLIENT_ID = 'abc123'
    OAUTH_CLIENT_SECRET = 'secret456'
    OAUTH_ALLOWED_SCOPES = ('PRINCIPAL_ROLE:lake_readers')
  )
  ENABLED = TRUE;
```

### 7. Create a Linked Database

Create a database linked to the catalog integration. Snowflake will automatically sync the tables from the Atlan Lakehouse.

```sql
CREATE DATABASE <database_name>
  LINKED_CATALOG = (
    CATALOG = '<integration_name>',
    ALLOWED_NAMESPACES = ('atlan-ns')
  )
  EXTERNAL_VOLUME = '<volume_name>';
```

Verify the sync state:

```sql
SELECT SYSTEM$CATALOG_LINK_STATUS('<database_name>');
```

## Configuration

| Parameter | Description |
|-----------|-------------|
| `<volume_name>` | Name for the external volume (e.g., `mdlh_gcs_volume`) |
| `<location_name>` | Descriptive name for the storage location within the volume |
| `<gcs-bucket-name>` | GCS bucket name provided by Atlan |
| `<gcs-prefix>` | GCS path prefix provided by Atlan (e.g., `atlan-wh`) |
| `<integration_name>` | Name for the catalog integration (e.g., `atlan_mdlh_catalog`) |
| `<catalog_uri>` | Catalog URI provided by Atlan (e.g., `https://<tenant>.atlan.com/api/polaris/api/catalog`) |
| `<catalog_name>` | Polaris catalog name provided by Atlan (typically `atlan-wh`) |
| `<polaris_reader_id>` | OAuth Client ID (Polaris Reader ID) provided by Atlan |
| `<polaris_reader_secret>` | OAuth Client Secret (Polaris Reader Secret) provided by Atlan |
| `<database_name>` | Target linked database name in Snowflake |

## Notes

- This approach uses Snowflake's native Iceberg REST Catalog federation — no external scripts or scheduled refresh jobs are needed.
- The linked database automatically syncs table definitions from the Atlan Lakehouse catalog.
- All SQL commands require `ACCOUNTADMIN` or equivalent privileges.
- The external volume is configured as read-only (`ALLOW_WRITES = FALSE`).
