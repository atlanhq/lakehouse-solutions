# MDLH Object Store Sync

A native Snowflake Streamlit app that integrates Snowflake with the Atlan Lakehouse **without** the Iceberg REST catalog. Snowflake never contacts the Atlan catalog service — it reads Iceberg metadata and data directly from object storage via IAM, and a scheduled in-account task keeps each table's metadata pointer current.

Because there is no REST catalog in the path, this works on Snowflake Enterprise edition (no PrivateLink or Business Critical required) and needs no inbound access to your Snowflake account.

> S3 only for now. GCS and Azure ADLS support is planned.

## How It Works

Atlan publishes small pointer files at the catalog storage root:

```
<catalog-root>/_latest/
  _index.json            # lists enabled namespaces -> pointer file
  <namespace>.json       # per-namespace: current metadata location per table
```

The app reads these through a Snowflake stage (IAM only — no external access integration) and keeps Snowflake in sync:

- **Create** — tables in the pointer files that aren't registered yet are created as Iceberg tables on an object-store catalog integration, pointing at their current `metadata.json`.
- **Refresh** — tables whose pointer moved get `ALTER ICEBERG TABLE ... REFRESH '<new metadata path>'`. Refreshing a table that is already current is a harmless no-op.
- **Orphans** — tables registered in Snowflake but gone from the pointer files are flagged; dropping them is opt-in.

A stored procedure runs the same sync on a schedule via a Snowflake task, so pointers stay current without anyone opening the app.

All Snowflake resources the app creates are prefixed `atlan_mdlh_`:

| Resource | Name |
|---|---|
| External volume | `atlan_mdlh_external_volume` |
| Catalog integration (object store) | `atlan_mdlh_catalog_integration` |
| Storage integration | `atlan_mdlh_storage_integration` |
| Database | `atlan_mdlh_lakehouse` |
| Stage, file format, config table, sync procedure, sync task | `atlan_mdlh_lakehouse.atlan_mdlh_admin.*` |

## Prerequisites

- From Atlan: the **S3 base URL** (catalog root) and the **IAM role ARN** Snowflake will assume. The role needs `s3:GetObject` and `s3:ListBucket` scoped to the catalog root.
- A role that can create external volumes, catalog integrations, and storage integrations (typically `ACCOUNTADMIN`), plus `EXECUTE MANAGED TASK` if you use a serverless sync task.
- Snowflake Enterprise edition or higher.

## Setup

1. In the Snowflake web interface, select **Projects** → **Streamlit**, create a new app on a database **other than** `atlan_mdlh_lakehouse`, and paste in `MDLH_object_store_sync.py`.
2. On the **Bootstrap** tab, enter the base URL and IAM role ARN from Atlan, optionally a warehouse for the sync task (blank = serverless), and the sync interval. Preview the SQL, then run the bootstrap.
3. **Share with Atlan**: after bootstrap the app shows the Snowflake-generated IAM user ARN and external ID for both the external volume and the storage integration. Atlan adds these to the role's trust policy.
4. Click **Verify Access** — it validates the external volume and lists the pointer files. If no pointer files are found, ask Atlan to enable the metadata pointer workflow for your tenant.
5. On the **Sync** tab, run **Plan Sync** to see what will be created, then **Apply Plan**.
6. On the **Scheduled Sync** tab, resume the task. It is created suspended so nothing runs before access is verified.

## Notes

- The external volume is read-only (`ALLOW_WRITES = FALSE`); Snowflake never writes to the lakehouse bucket. Dropping tables (or the whole integration) removes Snowflake registrations only — no data in object storage is touched.
- Metadata file paths are resolved relative to the base URL. If a table's `metadataLocation` is outside the base URL, the plan reports it as an error rather than guessing.
- Iceberg format version: tables should be created pointing at their current-format metadata. An in-place v2 → v3 format bump on an existing table breaks `REFRESH`; the fix is to drop and re-create the affected table (the Sync tab's orphan + create flow handles this).
- The sync interval defaults to 120 minutes, matching how often Atlan publishes pointer files. Shorter intervals are safe but add no freshness.

## Teardown

The **Teardown** tab drops everything the app created (database, integrations, external volume) after a typed confirmation. Nothing in object storage is affected.
