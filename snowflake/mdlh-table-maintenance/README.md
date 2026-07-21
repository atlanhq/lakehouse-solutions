# MDLH Table Refresh Repair

A native Snowflake Streamlit app that finds Iceberg tables whose auto-refresh has stopped working and repairs them — refreshing their metadata and re-enabling auto-refresh.

> To understand why tables stop refreshing, and for manual repair options, see the [auto-refresh troubleshooting guide](../catalog-integration/TROUBLESHOOTING.md).

## How It Works

The app scans every Iceberg table in a schema and checks `SYSTEM$AUTO_REFRESH_STATUS` on each one — the authoritative signal for auto-refresh health. A table is flagged as broken when its `executionState` is anything other than `RUNNING`, or when a failure field is populated.

Optionally, you can also flag tables whose `LAST_ALTERED` timestamp is older than N days. This is off by default because a table can be healthy and simply have no new data; when enabled, these tables are listed separately (see the **Flagged By** column) and are not preselected for repair.

Repairing a table runs three statements. Snowflake rejects a manual `REFRESH` while auto-refresh is enabled, so it must be disabled first:

```sql
ALTER ICEBERG TABLE <database>.<schema>.<table> SET AUTO_REFRESH = FALSE;
ALTER ICEBERG TABLE <database>.<schema>.<table> REFRESH;
ALTER ICEBERG TABLE <database>.<schema>.<table> SET AUTO_REFRESH = TRUE;
```

If the refresh fails, the app re-enables auto-refresh so the table is not left worse off, and reports the error in the results.

## Prerequisites

- Permissions to query `INFORMATION_SCHEMA.TABLES`, call `SYSTEM$AUTO_REFRESH_STATUS`, and run `ALTER ICEBERG TABLE` on the target database
- Permission to create Streamlit apps, and a warehouse to run the app on

## Setup

1. In the Snowflake web interface, select **Projects** → **Streamlit**
2. Click **+ Streamlit App**
3. Configure the app:
   - **App Title**: e.g. "MDLH Table Refresh Repair"
   - **App Location**: a database **other than** the MDLH linked database
   - **Run on Warehouse**: select your app warehouse
4. Click **Create**
5. Replace the editor contents with `MDLH_table_refresh_repair.py` and save
6. Click **Run**

## Usage

1. **Select database and schema** — enter the database name and pick a schema from the dropdown.
2. **Scan** — click **Scan Tables**. Optionally enable the staleness threshold first. Results show each flagged table with why it was flagged (**Flagged By**), its refresh state, status detail, last-altered timestamp, and row count.
3. **Repair** — broken tables are preselected; adjust the selection if needed, preview the SQL, and click **Repair Selected Tables**. The results table shows success or failure per table with the error message for any failure.

## Notes

- The scan runs one `SYSTEM$AUTO_REFRESH_STATUS` call per Iceberg table, so large schemas take longer; progress is shown during the scan.
- Database names are resolved the way Snowflake resolves identifiers: plain names match case-insensitively; wrap the name in double quotes to match a case-sensitive name exactly.
- The app checks one schema at a time.
- A table whose status cannot be read (for example, due to missing privileges) is reported as `CHECK_FAILED` rather than skipped.

## Troubleshooting

| Symptom | Check |
|---|---|
| Unable to connect to Snowflake | The app must run as a native Snowflake Streamlit app |
| No schemas found | Database name is correct and your role has access to it |
| Error listing Iceberg tables | Your role has `SELECT` on `INFORMATION_SCHEMA.TABLES` |
| Repair fails | Your role has `ALTER` on the table; see the error message in the results |
| Tables keep breaking again | See the [troubleshooting guide](../catalog-integration/TROUBLESHOOTING.md) and contact Atlan Support with the `SYSTEM$AUTO_REFRESH_STATUS` output |
