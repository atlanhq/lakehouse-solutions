# Troubleshooting: Iceberg Tables Stop Auto-Refreshing

Snowflake automatically refreshes Iceberg table metadata from the Atlan Lakehouse catalog based on `REFRESH_INTERVAL_SECONDS`. Occasionally this auto-refresh can fail for individual tables (for example, after a transient catalog or storage error). When that happens, Snowflake suspends auto-refresh for the affected table and the table stops picking up new data — queries keep working, but return stale results.

This guide walks through diagnosing which tables have stopped refreshing and how to restore them.

## Symptoms

- Queries against one or more linked-database tables return stale data.
- `LAST_ALTERED` in `INFORMATION_SCHEMA.TABLES` stops advancing for the affected tables.
- The rest of the linked database continues to sync normally.

## Step 1: Check the Catalog Link Status

First rule out a database-level sync problem:

```sql
SELECT SYSTEM$CATALOG_LINK_STATUS('<database_name>');
```

If this reports a failure, the issue is with the catalog integration itself (e.g., expired OAuth credentials or an unreachable catalog URI) — fix that first before looking at individual tables.

## Step 2: Check the Refresh Status of a Table

For each suspect table, check its auto-refresh status:

```sql
SELECT SYSTEM$AUTO_REFRESH_STATUS('<database_name>."<schema_name>"."<table_name>"');
```

**Example:**

```sql
SELECT SYSTEM$AUTO_REFRESH_STATUS('mdlh_context_store."entity_metadata"."persona"');
```

If the output contains an error (e.g., an `executionState` other than `RUNNING`, or a populated failure/error detail), auto-refresh has failed for that table and Snowflake will not resume it on its own.

## Step 3: Check Which Metadata File the Table Points To

To confirm the table is stale, inspect the metadata file Snowflake is currently using:

```sql
SELECT SYSTEM$GET_ICEBERG_TABLE_INFORMATION('<database_name>."<schema_name>"."<table_name>"');
```

**Example:**

```sql
SELECT SYSTEM$GET_ICEBERG_TABLE_INFORMATION('mdlh_context_store."entity_metadata"."persona"');
```

The output includes the `metadataLocation` the table is pinned to. If this lags behind the latest metadata file in the Atlan Lakehouse, the table is serving stale data.

### Checking a Whole Schema at Once

To scan every Iceberg table in a schema instead of checking one at a time, generate the status-check statements and run the output:

```sql
SELECT
    TABLE_NAME,
    LAST_ALTERED,
    'SELECT SYSTEM$AUTO_REFRESH_STATUS(''' || TABLE_CATALOG || '."' || TABLE_SCHEMA || '"."' || TABLE_NAME || '"'');' AS status_check
FROM <database_name>.INFORMATION_SCHEMA.TABLES
WHERE TABLE_SCHEMA = '<schema_name>'
  AND IS_ICEBERG = 'YES'
ORDER BY LAST_ALTERED ASC;
```

`LAST_ALTERED` alone is a useful first signal — tables whose `LAST_ALTERED` stopped advancing days ago are the ones to check first. Note that `SYSTEM$AUTO_REFRESH_STATUS` requires sufficient privileges on each table; if it errors for you, ask an account admin to run the checks.

## Restoring Auto-Refresh

### Option 1 (Recommended): Use the MDLH Table Maintenance App

The [MDLH Table Maintenance](../mdlh-table-maintenance/) Streamlit app automates the whole process — it finds stale tables in a schema, refreshes their metadata, and re-enables auto-refresh in one click. Use it whenever more than a handful of tables are affected.

### Option 2: Repair Tables Manually

For each table where the refresh status shows an error, refresh the metadata and re-enable auto-refresh. Snowflake rejects a manual `REFRESH` while `AUTO_REFRESH = TRUE`, so disable auto-refresh first:

```sql
ALTER ICEBERG TABLE <database_name>."<schema_name>"."<table_name>" SET AUTO_REFRESH = FALSE;
ALTER ICEBERG TABLE <database_name>."<schema_name>"."<table_name>" REFRESH;
ALTER ICEBERG TABLE <database_name>."<schema_name>"."<table_name>" SET AUTO_REFRESH = TRUE;
```

> **Important:** If the `REFRESH` statement fails, still run the final `SET AUTO_REFRESH = TRUE` statement. Otherwise the table is left with auto-refresh explicitly disabled — a worse state than the suspended one you started from — and Snowflake will never resume it on its own.

If many tables are affected, generate the repair statements for the whole schema and run the output:

```sql
SELECT
    'ALTER ICEBERG TABLE ' || TABLE_CATALOG || '."' || TABLE_SCHEMA || '"."' || TABLE_NAME || '" SET AUTO_REFRESH = FALSE; ' ||
    'ALTER ICEBERG TABLE ' || TABLE_CATALOG || '."' || TABLE_SCHEMA || '"."' || TABLE_NAME || '" REFRESH; ' ||
    'ALTER ICEBERG TABLE ' || TABLE_CATALOG || '."' || TABLE_SCHEMA || '"."' || TABLE_NAME || '" SET AUTO_REFRESH = TRUE;' AS repair_statement
FROM <database_name>.INFORMATION_SCHEMA.TABLES
WHERE TABLE_SCHEMA = '<schema_name>'
  AND IS_ICEBERG = 'YES';
```

This works, but is cumbersome at scale — prefer Option 1 when repairing more than a few tables.

### Option 3 (Last Resort): Recreate the Catalog Integration

If repairs keep failing or the catalog link itself is broken, drop and recreate the catalog integration and linked database following steps 6–7 of the [setup guide](./README.md). This re-syncs every table from scratch. Note that this briefly interrupts access to the linked database, so schedule it accordingly.

## Verifying the Fix

After repairing, confirm each table is healthy again:

```sql
SELECT SYSTEM$AUTO_REFRESH_STATUS('<database_name>."<schema_name>"."<table_name>"');
```

The status should show no errors, and `LAST_ALTERED` should advance on the next refresh interval — the catalog integration's `REFRESH_INTERVAL_SECONDS` (900 seconds / 15 minutes as configured in the [setup guide](./README.md)).

## Preventing Recurrence

- Run the [MDLH Table Maintenance](../mdlh-table-maintenance/) app on a regular cadence (daily or weekly) to catch stale tables early.
- Keep `REFRESH_INTERVAL_SECONDS = 900` or higher — shorter intervals increase load on the catalog and the likelihood of throttling-related refresh failures.
- If specific tables fail repeatedly, capture the error from `SYSTEM$AUTO_REFRESH_STATUS` and contact Atlan Support.
