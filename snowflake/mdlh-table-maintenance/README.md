# Snowflake Native Streamlit App - Iceberg Table Refresh Repair

A native Snowflake Streamlit application that identifies stale Iceberg tables and provides an option to repair them by refreshing metadata and enabling auto-refresh.

## Features

- **🔍 Find Stale Tables**: Identifies Iceberg tables that haven't been refreshed in the last N days
- **📊 Detailed View**: Shows table name, last refresh timestamp, days since refresh, and row count
- **🔧 Repair Tables**: Refreshes selected tables and enables auto-refresh
- **✅ Results Tracking**: Shows success/failure status for each repair operation
- **📈 Statistics**: Displays summary metrics (total tables, average days since refresh, etc.)

## Installation

### Prerequisites

- Snowflake account with appropriate permissions to:
  - Query `INFORMATION_SCHEMA.TABLES`
  - Execute `ALTER ICEBERG TABLE` commands
  - Create Streamlit apps
- Access to databases containing Iceberg tables
- A warehouse for running the Streamlit app

### Setup in Snowflake

Follow these steps to set up the MDLH Table Refresh Repair app in your Snowflake environment:

1. **Navigate to Streamlit Apps**
   - In the Snowflake web interface, select **"Projects"** → **"Streamlit"** from the left panel menu

2. **Create New Streamlit App**
   - Click **"+ Streamlit App"** button

3. **Configure App Settings**
   - **App Title**: Enter a title (e.g., "MDLH Table Refresh Repair")
   - **App Location**: 
     - ⚠️ **IMPORTANT**: The app location should **NOT** be the MDLH linked database
     - Select a **different database name** other than where the MDLH catalog is set up
     - This prevents conflicts and ensures proper app functionality
   - **Run on Warehouse**: Select this option
   - **App Warehouse**: Select your app warehouse name from the dropdown

4. **Create the App**
   - Click **"Create"** button

5. **Add the Script**
   - Copy the contents of `snowflake_table_refresh_repair.py` file
   - Paste it into the Streamlit app editor
   - Save the app

6. **Run the App**
   - Click **"Run"** to start the app
   - The app will automatically connect using the native Snowflake connection

### Important Notes

- **App Location**: Always use a database separate from your MDLH catalog database to avoid conflicts
- **Warehouse**: Ensure the selected warehouse has sufficient resources to run the app
- **Permissions**: The app needs access to query `INFORMATION_SCHEMA.TABLES` and execute `ALTER ICEBERG TABLE` commands on the target database

## Usage

### Step 1: Select Database & Schema

1. Enter the **Database Name** (default: `ATLAN_CONTEXT_STORE`)
   - You can change this to any database name
2. Select the **Schema** from the dropdown
   - Schemas are automatically loaded from the selected database
3. Configure the **Days Threshold** (default: 1 day)
   - This determines how many days since last refresh to consider a table "stale"

### Step 3: Find Stale Tables

1. Click **"🔍 Find Stale Tables"**
2. The app will query `INFORMATION_SCHEMA.TABLES` to find tables that:
   - Have `ROW_COUNT > 0`
   - Have `LAST_ALTERED < DATEADD(day, -N, CURRENT_TIMESTAMP())`
3. Results are displayed in a table showing:
   - Table Name
   - Last Refreshed (timestamp)
   - Days Since Refresh
   - Row Count

### Step 4: Repair Tables

1. **Select Tables**: Choose which tables to repair (default: all selected)
2. **Preview SQL**: Click "Preview SQL Commands" to see what will be executed
3. **Repair**: Click **"🔧 Repair Selected Tables"**
4. The app will:
   - Run `ALTER ICEBERG TABLE <db>.<schema>.<table> REFRESH` for each table
   - Run `ALTER ICEBERG TABLE <db>.<schema>.<table> SET AUTO_REFRESH = TRUE` for each table
5. **View Results**: See success/failure status for each table

## SQL Queries Used

### Finding Stale Tables

```sql
SELECT
    TABLE_NAME,
    LAST_ALTERED,
    ROW_COUNT
FROM <database>.INFORMATION_SCHEMA.TABLES
WHERE TABLE_SCHEMA = '<schema>'
  AND ROW_COUNT > 0
  AND LAST_ALTERED < DATEADD(day, -<days_threshold>, CURRENT_TIMESTAMP())
ORDER BY LAST_ALTERED ASC
```

### Repairing Each Table

```sql
-- Refresh the table metadata
ALTER ICEBERG TABLE <database>.<schema>.<table> REFRESH;

-- Enable auto-refresh
ALTER ICEBERG TABLE <database>.<schema>.<table> SET AUTO_REFRESH = TRUE;
```

## Features Explained

### Stale Table Detection

The app identifies tables that:
- Are Iceberg tables (detected via `INFORMATION_SCHEMA.TABLES`)
- Have data (`ROW_COUNT > 0`)
- Haven't been refreshed recently (`LAST_ALTERED < threshold`)

### Repair Operation

For each selected table, the app:
1. **Refreshes metadata**: Updates the table's metadata from the Iceberg catalog
2. **Enables auto-refresh**: Sets `AUTO_REFRESH = TRUE` so the table stays up-to-date automatically

### Results Display

- ✅ **Success**: Table was successfully refreshed and auto-refresh enabled
- ❌ **Failed**: Shows error message for troubleshooting
- **Summary**: Total success/failure counts

## Example Workflow

1. **Connect** to Snowflake with your credentials
2. **Select** `MDLH_CONTEXT_STORE` database and `atlan-ns` schema
3. **Set threshold** to 1 day
4. **Find stale tables** - discovers 5 tables not refreshed in last 24 hours
5. **Select all tables** (or specific ones)
6. **Repair** - app refreshes all selected tables
7. **View results** - all 5 tables successfully repaired ✅

## Troubleshooting

### Connection Issues

- **"Unable to connect to Snowflake"**: 
  - Verify the app is properly deployed as a Native Streamlit App
  - Check that `st.connection("snowflake")` is configured in your app setup
  - Ensure the app has appropriate permissions

### Schema Loading Issues

- **"No schemas found"**: 
  - Verify the database name is correct
  - Check that you have access to the database
  - Ensure the database exists

### Query Issues

- **"Error finding stale tables"**: 
  - Verify the database/schema exists
  - Check that you have `SELECT` permissions on `INFORMATION_SCHEMA.TABLES`
  - Ensure the schema contains Iceberg tables

### Repair Issues

- **"Failed to repair"**: 
  - Check that you have `ALTER` permissions on the tables
  - Verify the tables are actually Iceberg tables
  - Check Snowflake error message for details

### Performance

- **Slow query**: If finding stale tables is slow:
  - The `INFORMATION_SCHEMA.TABLES` view can be slow for large numbers of tables
  - Consider filtering by specific table patterns if possible
  - Use a larger warehouse for faster queries

## Best Practices

1. **Regular Checks**: Run this check daily or weekly to catch stale tables early
2. **Monitor Patterns**: Track which tables frequently become stale
3. **Auto-Refresh**: Enable auto-refresh on all Iceberg tables to prevent this issue
4. **Scheduled Tasks**: Consider automating this check using Snowflake tasks
5. **Permissions**: Ensure users have appropriate `ALTER` permissions on Iceberg tables

## Security Notes

- **Native App Security**: As a native Snowflake app, connection is handled automatically by Snowflake
- **Permissions**: Users should have minimal required permissions:
  - `SELECT` on `INFORMATION_SCHEMA.TABLES`
  - `ALTER` on Iceberg tables they need to repair
- **Role-Based Access**: Use role-based access control to limit which tables can be repaired
- **Database Access**: Users can only access databases they have permissions for

## Limitations

- **Iceberg Tables Only**: This app is designed for Iceberg tables only
- **Single Schema**: Checks one schema at a time (can be run multiple times for different schemas)
- **Manual Selection**: Tables must be selected manually (no bulk "repair all" without selection)

