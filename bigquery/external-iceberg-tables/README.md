# BigQuery External Iceberg Tables

BigQuery does not currently support querying federated (external) Iceberg REST catalogs natively. This script provides a workaround by creating external Iceberg tables in BigQuery that point directly to the Atlan Metadata Lakehouse (MDLH) Iceberg metadata files stored in GCS.

A single script (`bq_external_iceberg_tables_create_refresh.py`) handles both initial creation and ongoing refresh using `CREATE OR REPLACE EXTERNAL TABLE`.

Long-term, when BigQuery adds native support for Iceberg REST Catalog federation, this script will no longer be needed.

## Prerequisites

### 1. Contact Atlan Support

Reach out to Atlan Support to initiate setup. Atlan will provide:

- The **GCS region** where the Lakehouse data resides
- **Catalog details**:
  - Catalog URI
  - Catalog Name
  - Warehouse Name
- **OAuth credentials** (Client ID and Client Secret)

You can also find your Catalog details in the Atlan UI:
**Workflows > Marketplace > Atlan Lakehouse > Connection Details**

### 2. Create a BigQuery External Connection

Create a Cloud Resource connection in the **same region** as the GCS data.

**Option A: BigQuery UI**

1. Navigate to **BigQuery > Explorer > your project > + Add data**
2. Search for **"cloud resource"** > select **Vertex AI** > **BigQuery federation**
3. Enter a connection name (e.g., `atlan-mdlh-conn`) and select the region provided by Atlan
4. Note the **Service Account ID** from the connection info page

**Option B: CLI**

```bash
bq mk --connection \
  --project_id=<PROJECT_ID> \
  --location=<REGION> \
  --connection_type=CLOUD_RESOURCE \
  atlan-mdlh-conn
```

View the connection to get the Service Account ID:

```bash
bq show --connection --location=<REGION> <CONNECTION_ID>
```

### 3. Share the Service Account ID with Atlan

Send the **Service Account ID** from your connection to Atlan Support. Atlan will use it to grant read access to the GCS bucket containing the Lakehouse data.

### 4. Wait for Atlan Confirmation

Atlan must enable data sharing before you can proceed to run the script. You will be notified when this step is complete.

### 5. Install Dependencies

```bash
pip install "pyiceberg[pyarrow]" google-cloud-bigquery
```

## Important: All Regions Must Match

The following four resources **must all be in the same region**, or BigQuery will throw location-related errors:

| Resource | Must match |
|----------|-----------|
| GCS bucket location | Same region |
| BigQuery connection location | Same region |
| BigQuery dataset location | Same region |
| Query execution location | Same region |

## Configuration

The script uses the following configuration values, which can be set as environment variables or edited directly in the script:

| Variable | Description |
|----------|-------------|
| `BQ_PROJECT_ID` | GCP project ID where the connection was created |
| `BQ_LOCATION` | GCS/BigQuery region (must match across all resources) |
| `BQ_CONNECTION_ID` | Connection name created in Step 2 |
| `POLARIS_CATALOG_URI` | Catalog URI provided by Atlan |
| `CLIENT_ID` | OAuth Client ID provided by Atlan |
| `CLIENT_SECRET` | OAuth Client Secret provided by Atlan |
| `ENABLE_HISTORY_NAMESPACE_SYNC` | Set to `true` to include `atlan-history` namespace (default: `false`) |

## Usage

### Initial Setup and Ongoing Refresh

1. Configure the variables in the script
2. Run the script:
   ```bash
   python bq_external_iceberg_tables_create_refresh.py
   ```
3. The script will:
   - Auto-detect the Polaris warehouse (`atlan-wh` preferred, `context_store` fallback)
   - Discover all namespaces and tables
   - Create BigQuery datasets per namespace (hyphens are converted to underscores for BQ compatibility)
   - Create or replace external Iceberg tables in each dataset

### Scheduling

Schedule the script to run periodically to keep tables in sync with the latest Lakehouse data:

- **Recommended**: Daily at ~4:00 AM (overnight compactions run at ~3:00 AM)
- **Maximum frequency**: No more than every 30 minutes

## Troubleshooting

| Error | Cause / Fix |
|-------|-------------|
| `Error while reading data ... Failed to expand table ... matched no files` | A compaction has deleted old manifest files. Re-run the script to refresh metadata. Schedule runs at ~2:30 AM to prevent recurrence. |
| `Not found: Dataset ... was not found in location ...` | Region mismatch. Ensure the connection, dataset, and GCS bucket are all in the same region. |
| `Access denied / service account cannot read bucket` | The connection's service account needs `roles/storage.objectViewer` on the GCS bucket. Confirm that Atlan has completed the data sharing step. |
| `Invalid source URI ... not supported in this region` | The URI type (GCS vs S3) must match the connection type and region. |

## Notes

- By default, the `atlan-history` namespace is skipped. Set `ENABLE_HISTORY_NAMESPACE_SYNC=true` to include it.
- The script uses `CREATE OR REPLACE EXTERNAL TABLE`, making it safe to re-run for both initial creation and refresh.
- BigQuery does not allow hyphens in dataset names, so namespaces like `atlan-ns` become `atlan_ns`.
