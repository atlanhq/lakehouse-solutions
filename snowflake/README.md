MDLH GOLD Layer – Snowflake SQL Setup Guide
=======================================

This document explains how to use the provided **SQL script** to set up the **MDLH GOLD layer** in your Snowflake environment.
It is intended to be **distributed together with the SQL file** and used as a step-by-step execution guide.

------------------------------------------------------------

WHAT THIS SQL SCRIPT DOES
-------------------------

When you run the SQL script, it will:

- Create a database named **ATLAN_GOLD**
- Create all **MDLH GOLD views and dynamic tables** under the **PUBLIC** schema
- Configure Snowflake **Dynamic Tables** for automatic refresh
- Populate curated metadata views used by the Atlan MDLH Catalog

This GOLD layer provides ready-to-query metadata for assets, lineage, governance,
pipelines, and data quality.

------------------------------------------------------------

BEFORE YOU RUN THE SQL
---------------------

Please review and update the following values inside the SQL file before execution.


1) TABLE REFRESH LAG
---------------------------

The default refresh schedule configuration is - every hour:

    SCHEDULE = 'USING CRON 0 * * * * UTC'

You may change this value if you require faster or slower refresh intervals.
Lower values increase freshness but may increase compute usage.

------------------------------------------------------------

2) MDLH CONTEXT STORE REFERENCE
------------------------------

The script assumes the MDLH catalog is linked to:

    ATLAN_CONTEXT_STORE."entity_metadata"

If your catalog was created using:
- A different database name, or
- A different schema name

You must replace all occurrences of the above reference in the SQL file
with your actual catalog-linked database and schema.

------------------------------------------------------------

WHERE OBJECTS ARE CREATED
-------------------------

All views and tables created by this script will be available under:

    ATLAN_GOLD.PUBLIC

------------------------------------------------------------

GOLD LAYER OBJECTS CREATED
-------------------------

Below is a description of the main views and tables created by the SQL script.

ASSETS
- Unified asset registry across SQL, BI, pipelines, data quality, and object domains
- Central lookup for resolving asset GUIDs to names, types, ownership, and tags

RELATIONAL_ASSET_DETAILS
- Consolidated relational metadata for Databases, Schemas, Tables, Views, Columns,
  Queries, Materialized Views, Functions, and Procedures

TAGS
- Information about tagged assets in Atlan, including classifications and propagation

CUSTOM_METADATA
- Custom metadata attributes defined and applied within Atlan

README
- Sanitized Readme asset view with URL-decoded text and HTML removed for readability

GLOSSARY_DETAILS
- Glossaries, glossary categories, and glossary terms with hierarchy information

DATA_QUALITY_DETAILS
- Data quality assets including Atlan-native and third-party rules

PIPELINE_DETAILS
- Orchestration and pipeline assets such as Airflow, dbt, Fivetran, ADF, Matillion, etc.

LINEAGE
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

EXECUTION NOTES
---------------

- Initial execution time depends on:
  - Number of assets in the catalog
  - Metadata volume
  - Warehouse size
- Tables populate asynchronously
- Lineage views may take additional time during the first refresh cycle
- Please wait until the SQL script completes successfully before querying results

------------------------------------------------------------

BASIC VALIDATION
----------------

After the script completes, you can validate setup using:

    SHOW DATABASES LIKE 'ATLAN_GOLD';
    SHOW VIEWS IN SCHEMA ATLAN_GOLD.PUBLIC;
    SHOW TABLES IN SCHEMA ATLAN_GOLD.PUBLIC;

------------------------------------------------------------

SUMMARY
-------

This SQL script sets up the **MDLH GOLD layer** in Snowflake and prepares curated,
analytics-ready metadata views required by the Atlan catalog.

Ensure warehouse, refresh lag, and context store references are updated
before running the script.
