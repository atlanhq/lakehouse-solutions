# MDLH GOLD Layer – Databricks SQL Setup Guide

This document explains how to use the provided **SQL script** to set up the **MDLH GOLD layer** in your Databricks environment using Unity Catalog.

------------------------------------------------------------

## WHAT THIS SQL SCRIPT DOES

When you run the SQL script, it will:

- Create Unity Catalog **ATLAN** and schema **ATLAN_GOLD**
- Create all **MDLH GOLD views and materialized views** in **ATLAN.ATLAN_GOLD**
- Set up materialized views for **ASSETS** and **BASE_EDGES** that require periodic refresh
- Populate curated metadata views used by the Atlan MDLH Catalog

This GOLD layer provides ready-to-query metadata for assets, lineage, governance,
pipelines, and data quality.

------------------------------------------------------------

## BEFORE YOU RUN THE SQL

### 1) SET UP UNITY CATALOG CONTEXT

The SQL script contains the following statements at the beginning:

```sql
CREATE CATALOG IF NOT EXISTS ATLAN;

CREATE SCHEMA IF NOT EXISTS ATLAN.ATLAN_GOLD;

USE ATLAN_CONTEXT_STORE.entity_metadata;
```

**Important Notes**:

1. **ATLAN Catalog and ATLAN_GOLD Schema** (Cannot be changed):
   - The script creates the `ATLAN` catalog and `ATLAN_GOLD` schema where all Gold Layer views and materialized views will be created
   - These names are fixed and should not be changed

2. **MDLH Source Catalog and Schema** (Can be changed if needed):
   - The script uses `USE ATLAN_CONTEXT_STORE.entity_metadata;` to access your MDLH catalog source tables
   - `ATLAN_CONTEXT_STORE.entity_metadata` is the **recommended** MDLH catalog and schema name
   - **If your MDLH catalog is set up with a different catalog and/or schema name**, you must **replace** `ATLAN_CONTEXT_STORE.entity_metadata` in the `MDLH_Gold_layer_Databricks.sql` file with your actual catalog and schema name

**Example**: If your MDLH catalog is `MY_CATALOG` and schema is `my_schema`, replace:
```sql
USE ATLAN_CONTEXT_STORE.entity_metadata;
```

With:
```sql
USE MY_CATALOG.my_schema;
```

The script expects source entity tables (with `_entity` suffix) to be accessible from this catalog and schema context.

### 2) SOURCE TABLE NAMING

The script assumes the MDLH catalog tables are available with the `_entity` suffix:
- `Tag_Relationship_entity`
- `Custommetadata_Relationship_entity`
- `GLOSSARY_entity`
- `GlossaryTerm_entity`
- `GlossaryCategory_entity`
- And all other entity tables with `_entity` suffix

If your catalog tables use different names, you must update the table references in the SQL file.

------------------------------------------------------------

## WHERE OBJECTS ARE CREATED

All views and materialized views created by this script will be available under:

    ATLAN.ATLAN_GOLD

Examples:
- `ATLAN.ATLAN_GOLD.TAGS`
- `ATLAN.ATLAN_GOLD.ASSETS`
- `ATLAN.ATLAN_GOLD.LINEAGE`

------------------------------------------------------------

## GOLD LAYER OBJECTS CREATED

### ASSETS (Materialized View)
- Unified asset registry across SQL, BI, pipelines, data quality, and object domains
- Central lookup for resolving asset GUIDs to names, types, ownership, and tags
- **Requires periodic refresh**

### BASE_EDGES (Materialized View)
- Internal materialized view used to flatten and resolve lineage edges
- Resolves GUIDs to human-readable names and types from the asset registry
- **Requires periodic refresh**

### DATA_MESH_DETAILS
- Consolidates metadata for DataDomain and DataProduct typedefs under the Data Mesh supertype
- Provides data domain hierarchy, data product details, stakeholders, and port information

### RELATIONAL_ASSET_DETAILS
- Consolidated relational metadata for Databases, Schemas, Tables, Views, Columns,
  Queries, Materialized Views, Functions, and Procedures

### TAGS
- Information about tagged assets in Atlan, including classifications and propagation

### CUSTOM_METADATA
- Custom metadata attributes defined and applied within Atlan

### README
- Sanitized Readme asset view with URL-decoded text and HTML removed for readability

### GLOSSARY_DETAILS
- Glossaries, glossary categories, and glossary terms with hierarchy information

### DATA_QUALITY_DETAILS
- Data quality assets including Atlan-native and third-party rules (Anomalo, Soda, Monte Carlo)

### PIPELINE_DETAILS
- Orchestration and pipeline assets such as Airflow, dbt, Fivetran, ADF, Matillion, etc.

### LINEAGE
- Recursive view providing complete multi-hop upstream and downstream lineage


INTERNAL LINEAGE OBJECTS (DO NOT USE)
------------------------------------

The following objects are created only to improve lineage performance.
They are used internally by the LINEAGE view and should not be queried directly.

LINEAGE_EDGES
- Internal view for lineage derivation across multiple process types

BASE_EDGES
- Internal table used to flatten and resolve lineage edges

------------------------------------------------------------
## EXECUTION STEPS

### Step 1: Execute the SQL Script

1. Open the Databricks SQL Editor
2. Set the Unity Catalog context (see "BEFORE YOU RUN THE SQL" section above)
3. Open the `MDLH_Gold_layer_Databricks.sql` file
4. Execute the entire script

**Note**: Initial execution time depends on the number of assets, metadata volume, and cluster size. Materialized views populate during creation.

### Step 2: Validate Setup

```sql
USE CATALOG ATLAN;
USE SCHEMA ATLAN_GOLD;

-- List all views
SHOW VIEWS;

-- List all materialized views
SHOW MATERIALIZED VIEWS;

-- Verify ASSETS materialized view
SELECT COUNT(*) AS asset_count FROM ATLAN.ATLAN_GOLD.ASSETS;

-- Example: Query TAGS view
SELECT COUNT(*) AS tag_count FROM ATLAN.ATLAN_GOLD.TAGS;

-- View materialized view details including last refresh time
DESCRIBE EXTENDED ATLAN.ATLAN_GOLD.ASSETS;
```

### Step 3: Set Up Scheduled Refresh Jobs

The **ASSETS** and **BASE_EDGES** materialized views require periodic refresh. Use Databricks Jobs with the provided SQL file.

#### Create Refresh Job

1. **Navigate to Databricks Jobs**:
   - Go to "Workflows" → "Jobs" → "Create job"

2. **Configure Job**:
   - **Name**: `Refresh ATLAN Gold Layer Materialized Views`
   - **Type**: SQL
   - **SQL Task**: Add a new SQL task

3. **SQL Task Configuration**:
   - **SQL Warehouse**: Select your SQL warehouse
   - **File Path**: Upload or reference the `refresh_materialized_views.sql` file
   - **Note**: SQL Jobs do not allow direct SQL queries, so you must use the SQL file

4. **Configure Schedule**:
   - Click on **"Schedules & Triggers"** tab
   - Click **"Add trigger"**
   - Select **"Schedule"**
   - Choose one of the following:
     - **Fixed time every day**: Set specific time (e.g., 2:00 AM UTC daily)
     - **Cron schedule**: Use cron expression (e.g., `0 * * * *` for hourly)
   - **Timezone**: Select your preferred timezone (UTC recommended)

5. **Save and Enable**: Save the job and ensure it's enabled

#### Refresh SQL File

The `refresh_materialized_views.sql` file contains:

```sql
USE CATALOG ATLAN;

REFRESH MATERIALIZED VIEW ATLAN.ATLAN_GOLD.ASSETS;
REFRESH MATERIALIZED VIEW ATLAN.ATLAN_GOLD.BASE_EDGES;
```

**Note**: The `USE SCHEMA` statement is not required since the SQL statements are already fully qualified with `ATLAN.ATLAN_GOLD`.

**Recommended Schedule Options**:
- **Hourly**: `0 * * * *` (runs at minute 0 of every hour)
- **Every 6 hours**: `0 */6 * * *`
- **Daily at midnight**: `0 0 * * *`
- **Fixed time daily**: Set specific time using "Fixed time every day" option

------------------------------------------------------------

## TROUBLESHOOTING

### "Table or view not found" errors
- Verify Unity Catalog context is set correctly using `USE CATALOG` and `USE SCHEMA`
- Check that all source entity tables exist with `_entity` suffix
- Ensure proper permissions to access source tables

### Materialized view refresh fails
- Verify source tables are accessible
- Check SQL warehouse has sufficient resources
- Review job logs for specific error messages

### Jobs not running on schedule
- Verify job is enabled
- Check schedule configuration in "Schedules & Triggers"
- Ensure SQL warehouse is running

------------------------------------------------------------

## SUMMARY

**Key Steps**:
1. ✅ Update Unity Catalog context in `MDLH_Gold_layer_Databricks.sql` if your MDLH catalog uses different names (see "BEFORE YOU RUN THE SQL")
2. ✅ Execute `MDLH_Gold_layer_Databricks.sql` script
3. ✅ Validate setup using provided queries
4. ✅ Create Databricks Job using `refresh_materialized_views.sql` file
5. ✅ Configure schedule in "Schedules & Triggers" tab
6. ✅ Verify job execution using `DESCRIBE EXTENDED` command (see SAMPLE QUERIES section)

**Important**: The ASSETS and BASE_EDGES materialized views require periodic refresh. Use the `refresh_materialized_views.sql` file in a scheduled Databricks Job.

------------------------------------------------------------

## SAMPLE QUERIES

### Query Asset Count by Type

```sql
USE CATALOG ATLAN;
USE SCHEMA ATLAN_GOLD;

SELECT 
    asset_type,
    COUNT(*) AS count
FROM ASSETS
GROUP BY asset_type
ORDER BY count DESC;
```

### Query Data Mesh Details

```sql
USE CATALOG ATLAN;
USE SCHEMA ATLAN_GOLD;

SELECT 
    guid,
    asset_type,
    data_products,
    subdomains,
    stakeholders
FROM DATA_MESH_DETAILS
WHERE asset_type = 'DataDomain';
```

### Query Lineage for a Specific Asset

```sql
USE CATALOG ATLAN;
USE SCHEMA ATLAN_GOLD;

SELECT 
    direction,
    start_name,
    related_name,
    related_type,
    level
FROM LINEAGE
WHERE start_guid = 'your-asset-guid-here'
ORDER BY level, direction;
```

### Verify Materialized View Refresh Status

After setting up the scheduled job, you can verify the schedule and check the last refresh time:

```sql
USE CATALOG ATLAN;
USE SCHEMA ATLAN_GOLD;

-- View materialized view details including last refresh time
DESCRIBE EXTENDED ATLAN.ATLAN_GOLD.ASSETS;
```

This will show information about the materialized view including when it was last refreshed, which helps verify that the scheduled job is running correctly.
