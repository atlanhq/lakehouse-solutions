# MDLH GOLD Layer – BigQuery SQL Setup Guide

This document explains how to use the provided **SQL script** to set up the **MDLH GOLD layer** in your BigQuery environment.

------------------------------------------------------------

## WHAT THIS SQL SCRIPT DOES

When you run the SQL script, it will:

- Create BigQuery dataset **ATLAN_GOLD**
- Create all **MDLH GOLD views and tables** in **ATLAN_GOLD**
- Create intermediate views **ASSETS_PART1**, **ASSETS_PART2**, and **ASSETS_PART3** to work around BigQuery's 256KB SQL statement limit
- Create **ASSETS** and **BASE_EDGES** tables that require periodic refresh
- Populate curated metadata views used by the Atlan MDLH Catalog

This GOLD layer provides ready-to-query metadata for assets, lineage, governance,
pipelines, and data quality.

------------------------------------------------------------

## BEFORE YOU RUN THE SQL

### 1) UPDATE SOURCE TABLE REFERENCES

The SQL script uses fully qualified table names for all source tables. You must replace the project and dataset references with your actual MDLH catalog location.

**Important**: Replace all occurrences of `development-platform-370010.atlan_wh_us_east_1` with your `<GCP Project ID>.<BigQuery dataset>` where you have set up the MDLH catalog.

**Example**: If your GCP project is `my-gcp-project` and your BigQuery dataset is `mdlh_catalog`, you would replace:
```sql
FROM `development-platform-370010.atlan_wh_us_east_1.Tag_Relationship`;
```

With:
```sql
FROM `my-gcp-project.mdlh_catalog.Tag_Relationship`;
```

**How to replace**:
- Use a text editor with find-and-replace functionality
- Find: `development-platform-370010.atlan_wh_us_east_1`
- Replace with: `<your-gcp-project-id>.<your-bigquery-dataset>`
- Replace all occurrences in the file

### 2) SOURCE TABLE NAMING

The script assumes the MDLH catalog tables are available with the `_entity` suffix:
- `Tag_Relationship`
- `Custommetadata_Relationship`
- `GLOSSARY_entity`
- `GlossaryTerm_entity`
- `GlossaryCategory_entity`
- And all other entity tables with `_entity` suffix

If your catalog tables use different names, you must update the table references in the SQL file.

------------------------------------------------------------

## WHERE OBJECTS ARE CREATED

All views and tables created by this script will be available under:

    ATLAN_GOLD

Examples:
- `ATLAN_GOLD.TAGS`
- `ATLAN_GOLD.ASSETS`
- `ATLAN_GOLD.RELATIONAL_ASSET_DETAILS`
- `ATLAN_GOLD.LINEAGE`

------------------------------------------------------------

## GOLD LAYER OBJECTS CREATED

### ASSETS (Table)
- Unified asset registry across SQL, BI, pipelines, data quality, and object domains
- Central lookup for resolving asset GUIDs to names, types, ownership, and tags
- **Requires periodic refresh** (see "Set Up Scheduled Refresh" section below)

**Note**: The ASSETS table is built from three intermediate views:
- **ASSETS_PART1** (internal view)
- **ASSETS_PART2** (internal view)
- **ASSETS_PART3** (internal view)

These intermediate views are created to work around BigQuery's 256KB SQL statement size limit. The ASSETS table combines all three parts using UNION ALL.

### BASE_EDGES (Table)
- Internal table used to flatten and resolve lineage edges
- Resolves GUIDs to human-readable names and types from the asset registry
- **Requires periodic refresh** (see "Set Up Scheduled Refresh" section below)

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

------------------------------------------------------------

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

### Step 1: Update Source Table References

1. Open the `MDLH_Gold_layer.sql` file in a text editor
2. Use find-and-replace to replace all occurrences of `development-platform-370010.atlan_wh_us_east_1` with your `<GCP Project ID>.<BigQuery dataset>`
3. Save the file

### Step 2: Execute the SQL Script

1. Open the BigQuery Console (https://console.cloud.google.com/bigquery)
2. Select your GCP project
3. Open the BigQuery SQL Editor
4. Open the `MDLH_Gold_layer.sql` file
5. Execute the entire script

**Note**: Initial execution time depends on the number of assets, metadata volume, and BigQuery slot capacity. Tables populate during creation.

### Step 3: Validate Setup

```sql
-- List all views in ATLAN_GOLD dataset
SELECT table_name, table_type
FROM `your-project-id.ATLAN_GOLD.INFORMATION_SCHEMA.TABLES`
WHERE table_schema = 'ATLAN_GOLD'
ORDER BY table_name;

-- Verify ASSETS table
SELECT COUNT(*) AS asset_count FROM ATLAN_GOLD.ASSETS;

-- Example: Query TAGS view
SELECT COUNT(*) AS tag_count FROM ATLAN_GOLD.TAGS;

-- View table details
SELECT * FROM `your-project-id.ATLAN_GOLD.INFORMATION_SCHEMA.TABLES`
WHERE table_schema = 'ATLAN_GOLD' AND table_name = 'ASSETS';
```

### Step 4: Set Up Scheduled Refresh

The **ASSETS** and **BASE_EDGES** tables require periodic refresh. Use BigQuery Scheduled Queries to automate this.

#### Create Scheduled Query

1. **Navigate to BigQuery Scheduled Queries**:
   - In BigQuery Console, go to "Scheduled queries" in the left navigation
   - Click "Create scheduled query"

2. **Configure Scheduled Query**:
   - **Name**: `refresh_mdlh_gold_table`
   - **Description**: (Optional) Refresh ATLAN_GOLD.ASSETS and ATLAN_GOLD.BASE_EDGES tables

3. **Add SQL Statements**:
   - In the SQL editor, add the following SQL statements in the same order:

```sql
CREATE OR REPLACE TABLE ATLAN_GOLD.ASSETS
OPTIONS(description='A unified asset registry view that consolidates all assets across SQL, BI, pipeline, data quality, and object domains. It serves as the central lookup source for resolving GUIDs to human-readable names, types, and associated metadata including tags and ownership details.')
AS
WITH tag_agg AS (
    SELECT
        asset_guid,
        ARRAY_AGG(DISTINCT tag_name) AS tag_names
    FROM ATLAN_GOLD.TAGS
    GROUP BY asset_guid
)
SELECT guid, asset_type, asset_name, asset_qualified_name, description, readme_guid, status, created_at, created_by, updated_at, updated_by, certificate_status, certificate_updated_by, certificate_updated_at, connector_name, connector_qualified_name,source_created_at, source_created_by, source_updated_at, source_updated_by, owner_users, term_guids, popularity_score, tag.tag_names AS tags, has_lineage FROM ATLAN_GOLD.ASSETS_PART1 asset LEFT JOIN tag_agg tag ON asset.guid = tag.asset_guid UNION ALL
SELECT guid, asset_type, asset_name, asset_qualified_name, description, readme_guid, status, created_at, created_by, updated_at, updated_by, certificate_status, certificate_updated_by, certificate_updated_at, connector_name, connector_qualified_name,source_created_at, source_created_by, source_updated_at, source_updated_by, owner_users, term_guids, popularity_score, tag.tag_names AS tags, has_lineage FROM ATLAN_GOLD.ASSETS_PART2 asset LEFT JOIN tag_agg tag ON asset.guid = tag.asset_guid UNION ALL
SELECT guid, asset_type, asset_name, asset_qualified_name, description, readme_guid, status, created_at, created_by, updated_at, updated_by, certificate_status, certificate_updated_by, certificate_updated_at, connector_name, connector_qualified_name,source_created_at, source_created_by, source_updated_at, source_updated_by, owner_users, term_guids, popularity_score, tag.tag_names AS tags, has_lineage FROM ATLAN_GOLD.ASSETS_PART3 asset LEFT JOIN tag_agg tag ON asset.guid = tag.asset_guid;

CREATE OR REPLACE TABLE ATLAN_GOLD.BASE_EDGES 
OPTIONS(description='Internal table used for flattening lineage edges by resolving GUIDs to human-readable names and types from the asset registry')
AS
SELECT DISTINCT
    e.process_guid,
    e.input_guid,
    COALESCE(i.asset_name, e.input_guid) AS input_name,
    COALESCE(i.asset_type, 'unknown') AS input_type,
    e.output_guid,
    COALESCE(o.asset_name, e.output_guid) AS output_name,
    COALESCE(o.asset_type, 'unknown') AS output_type
FROM ATLAN_GOLD.LINEAGE_EDGES e
LEFT JOIN (
    SELECT DISTINCT guid, asset_name, asset_type
    FROM ATLAN_GOLD.ASSETS
) i ON e.input_guid = i.guid
LEFT JOIN (
    SELECT DISTINCT guid, asset_name, asset_type
    FROM ATLAN_GOLD.ASSETS
) o ON e.output_guid = o.guid
WHERE e.input_guid IS NOT NULL 
  AND e.output_guid IS NOT NULL;
```

4. **Configure Schedule**:
   - Click "Schedule options"
   - Choose one of the following:
     - **Daily**: Set specific time (e.g., 2:00 AM UTC daily)
     - **Hourly**: Runs every hour
     - **Custom**: Use cron expression (e.g., `0 * * * *` for hourly)
   - **Timezone**: Select your preferred timezone (UTC recommended)
   - **Start date and end date**: (Optional) Set schedule window

5. **Save**: Click "Save" button to create the scheduled query

**Recommended Schedule Options**:
- **Hourly**: Runs every hour
- **Every 6 hours**: Use custom cron `0 */6 * * *`
- **Daily at midnight**: Use daily schedule with time 00:00

**Note**: The scheduled query will run both `CREATE OR REPLACE TABLE` statements in sequence, refreshing both ASSETS and BASE_EDGES tables.

------------------------------------------------------------

## TROUBLESHOOTING

### "Table or view not found" errors
- Verify that you've replaced all occurrences of `development-platform-370010.atlan_wh_us_east_1` with your actual project and dataset
- Check that all source entity tables exist in your MDLH catalog dataset
- Ensure proper permissions to access source tables

### "Query exceeded 256KB limit" errors
- The script uses intermediate views (ASSETS_PART1, ASSETS_PART2, ASSETS_PART3) to work around this limit
- If you encounter this error, you may need to further split the ASSETS creation into more parts

### Scheduled query not running
- Verify the scheduled query is enabled
- Check schedule configuration in "Scheduled queries" section
- Review execution history for error messages
- Ensure you have proper permissions to create/replace tables

### Table refresh fails
- Verify source tables are accessible
- Check BigQuery slot capacity and quotas
- Review scheduled query execution logs for specific error messages

------------------------------------------------------------

## SUMMARY

**Key Steps**:
1. ✅ Replace `development-platform-370010.atlan_wh_us_east_1` with your `<GCP Project ID>.<BigQuery dataset>` in `MDLH_Gold_layer.sql`
2. ✅ Execute `MDLH_Gold_layer.sql` script in BigQuery Console
3. ✅ Validate setup using provided queries
4. ✅ Create BigQuery Scheduled Query named `refresh_mdlh_gold_table`
5. ✅ Add both `CREATE OR REPLACE TABLE` statements (ASSETS and BASE_EDGES) to the scheduled query
6. ✅ Configure schedule (daily or hourly as per your requirement)
7. ✅ Save the scheduled query

**Important**: The ASSETS and BASE_EDGES tables require periodic refresh. Use BigQuery Scheduled Queries to automate the refresh process.

**Note**: The intermediate views ASSETS_PART1, ASSETS_PART2, and ASSETS_PART3 are internal views created to work around BigQuery's 256KB SQL statement limit. These views are used internally by the ASSETS table and should not be queried directly.

------------------------------------------------------------

## SAMPLE QUERIES

### Query Asset Count by Type

```sql
SELECT 
    asset_type,
    COUNT(*) AS count
FROM ATLAN_GOLD.ASSETS
GROUP BY asset_type
ORDER BY count DESC;
```

### Query Data Mesh Details

```sql
SELECT 
    guid,
    asset_type,
    data_products,
    subdomains,
    stakeholders
FROM ATLAN_GOLD.DATA_MESH_DETAILS
WHERE asset_type = 'DataDomain';
```

### Query Lineage for a Specific Asset

```sql
SELECT 
    direction,
    start_name,
    related_name,
    related_type,
    level
FROM ATLAN_GOLD.LINEAGE
WHERE start_guid = 'your-asset-guid-here'
ORDER BY level, direction;
```

### Query TAGS View

```sql
SELECT 
    asset_guid,
    asset_type,
    tag_name,
    tag_value
FROM ATLAN_GOLD.TAGS
WHERE tag_name = 'PII'
LIMIT 100;
```

### Verify Scheduled Query Execution

After setting up the scheduled query, you can verify its execution:

1. Go to "Scheduled queries" in BigQuery Console
2. Click on `refresh_mdlh_gold_table`
3. View the "Execution history" tab to see when the query last ran and its status
4. Check the "Last run" timestamp to confirm the schedule is working

### Check Table Last Modified Time

```sql
SELECT 
    table_name,
    creation_time,
    last_modified_time,
    row_count,
    size_bytes
FROM `your-project-id.ATLAN_GOLD.INFORMATION_SCHEMA.TABLES`
WHERE table_schema = 'ATLAN_GOLD' 
  AND table_name IN ('ASSETS', 'BASE_EDGES')
ORDER BY last_modified_time DESC;
```

This will show when the tables were last modified, which helps verify that the scheduled query is running correctly.
