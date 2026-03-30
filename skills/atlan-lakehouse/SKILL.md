---
name: atlan-lakehouse
description: >
  Use this skill when the user wants to query Atlan catalog metadata or usage analytics at scale,
  build reports on assets/glossaries/tags/user adoption, or analyze metadata from the lakehouse.
  Works across platforms: Snowflake (Cortex Code), Databricks (Genie Code), and Python (PyIceberg).
  Includes the GOLD namespace (curated, pre-joined star-schema tables for simplified asset metadata queries),
  plus SQL template library for metadata completeness, glossary export,
  and comprehensive usage analytics (active users, feature adoption, engagement, retention, health scoring).
license: Apache-2.0
compatibility: SQL (Snowflake, Databricks), Python 3.9+ with PyIceberg
metadata:
  author: atlan-platform-team
  version: "1.0.0"
  category: data-integration
  keywords: atlan-lakehouse, iceberg, polaris, pyiceberg, analytics, usage-analytics, snowflake, cortex, databricks, genie, lineage, glossary, metadata-completeness, gold-namespace
---

# Atlan Lakehouse Skill

This skill teaches you how to connect to and query the Atlan Lakehouse across any platform.

## What is the Atlan Lakehouse?

The **Atlan Lakehouse** is an Apache Iceberg-based data lakehouse that stores metadata and product telemetry for an Atlan tenant. Key characteristics:

- **Managed by Atlan**: Setup, maintenance, and data sync handled automatically
- **15-minute sync**: Changes in Atlan reflect in the lakehouse within ~15 minutes
- **Read-only**: Designed for reporting and analytics workloads
- **Iceberg REST Catalog**: Uses Polaris as the catalog interface
- **Cross-platform**: Data is accessible from Snowflake, Databricks, or any Iceberg-compatible engine

### Namespaces

| Namespace          | Contents                                                                                                                                    |
| ------------------ | ------------------------------------------------------------------------------------------------------------------------------------------- |
| `GOLD`             | **Start here for asset metadata queries.** Curated star-schema layer with pre-joined tables for simplified analytics. Central `ASSETS` table joined to detail tables (`RELATIONAL_ASSET_DETAILS`, `GLOSSARY_DETAILS`, `DATA_QUALITY_DETAILS`, `PIPELINE_DETAILS`, `BI_ASSET_DETAILS`, `DATA_MESH_DETAILS`) via `guid`. Does **not** contain tags, custom metadata, readmes, or lineage — use `ENTITY_METADATA` for those. |
| `ENTITY_METADATA`  | Catalog metadata: one table per asset type in Atlan's metamodel (e.g., `TABLE`, `COLUMN`, `VIEW`, `GLOSSARYTERM`, `DBTMODEL`, `DATADOMAIN`, etc.), plus relationship/consolidated tables like `TAGS`, `CUSTOM_METADATA`, `GLOSSARY_DETAILS`, `README`, `DATA_MESH_DETAILS`, and lineage process tables (`PROCESS`, `COLUMN_PROCESS`, `BI_PROCESS`). Named `atlan-ns` on tenants onboarded before February 2026. **Important:** There is one table per asset type — the supertype tables (e.g., `ASSET`, `SQL`, `BI`, `SAAS`, `CLOUD`) are structural parents in Atlan's type hierarchy and typically have 0 rows. Query the specific type table that matches the assets you need (e.g., `TABLE` for Table assets, `COLUMN` for Column assets). Many tables will also have 0 rows if the tenant does not use that connector — for example, `AIRFLOWDAG` is empty if the tenant does not use Airflow. This is expected. |
| `ENTITY_HISTORY`   | Historical snapshots mirroring every table in `ENTITY_METADATA`, with added `snapshot_timestamp` and `snapshot_date` columns for temporal analysis, change tracking, and audit trails. |
| `USAGE_ANALYTICS`  | Product telemetry: page views, user actions, and user identity snapshots.                                                                   |
| `OBSERVABILITY`    | Workflow and job execution metrics. Contains a single table `JOB_METRICS` with lifecycle timestamps, status codes, and workflow-specific `custom_metrics` (JSON). Use to track DQ scores, job success/failure rates, retry patterns, and pipeline duration. |

#### `USAGE_ANALYTICS` tables

| Table    | Key columns                                                                                         |
| -------- | --------------------------------------------------------------------------------------------------- |
| `PAGES`  | page name, tab, asset GUID, connector name, asset type, user ID, domain, timestamp                 |
| `TRACKS` | user ID, event_text, domain, timestamp                                                              |
| `USERS`  | user ID, email, username, role, license_type, job_role, created_at                                 |

#### `OBSERVABILITY` tables

| Table         | Key columns                                                                                                   |
| ------------- | ------------------------------------------------------------------------------------------------------------- |
| `JOB_METRICS` | tenant_id, service_name, job_name, job_instance_id, started_at, completed_at, status_code, error_message, custom_metrics (JSON), attempt_number, retry_count |

> **`custom_metrics` is a JSON string** whose schema varies by `job_name`. Key job types: `AtlasDqOrchestrationWorkflow` (DQ scores), `AtlasTypeDefDqWorkflow` (per-type DQ), `UsageAnalyticsCountValidationWorkflow` (usage table validation), `AtlasBulkTypedefRefreshWorkflow` (metadata sync), `AtlasNotificationProcessorWorkflow` (incremental sync), `AtlasReconciliationWorkflow` (reconciliation). Use `SELECT DISTINCT job_name` to discover all job types in a tenant.

#### `GOLD` tables

| Table                       | Purpose                                                                                          |
| --------------------------- | ------------------------------------------------------------------------------------------------ |
| `ASSETS`                    | Central hub table — every catalog asset with core attributes (name, type, status, owners, certification, description, popularity, lineage flag). **Start every query here.** |
| `RELATIONAL_ASSET_DETAILS`  | Database, schema, table, view, column, function, and materialized view attributes (row counts, sizes, costs, column data types, query access). |
| `GLOSSARY_DETAILS`          | Glossary, category, and term attributes (terms, categories, assigned entities, anchor glossary). |
| `DATA_QUALITY_DETAILS`      | Data quality checks across Anomalo, Soda, Monte Carlo, and native Atlan DQ rules (check status, last run, priority, dimensions). |
| `PIPELINE_DETAILS`          | Pipeline and ETL process attributes across ADF, Airflow, dbt, Fivetran, Matillion (input/output process GUIDs). |
| `BI_ASSET_DETAILS`          | BI tool attributes for PowerBI, Tableau, Looker, Sigma (workspace, project, folder, site references). |
| `DATA_MESH_DETAILS`         | Data domain and data product attributes (subdomains, stakeholders, criticality, sensitivity, visibility, input/output ports). |

> **What's NOT in GOLD:** Tags, custom metadata, readmes, and lineage are not in the GOLD namespace. For these, use hybrid queries that join GOLD tables to `ENTITY_METADATA` tables: `TAGS` (classification tags), `CUSTOM_METADATA` (custom metadata attributes), `README` (readme content), `PROCESS` / `COLUMN_PROCESS` / `BI_PROCESS` (lineage relationships).

## When to Use

Activate this skill when:

- User needs to connect to the Atlan Lakehouse from any platform (Snowflake, Databricks, or Python)
- User wants to query Atlan catalog metadata or run metadata governance reports
- User wants simplified asset metadata queries using pre-joined tables (GOLD namespace)
- User needs cross-type asset reports spanning relational, BI, pipeline, DQ, glossary, or data mesh assets
- User needs to assess metadata completeness, tag/description/owner coverage
- User needs to export and analyze glossary terms with categories and assigned entities
- User wants to track historical metadata changes or generate audit trails
- User wants to analyze product adoption: DAU/WAU/MAU, feature engagement, retention, or engagement depth
- User wants to build usage dashboards or customer health scorecards
- User wants to identify churned/reactivated users, power users, or engagement tiers
- User wants to monitor Lakehouse job health, DQ score trends, pipeline success rates, or job duration

## Platform Routing — READ THIS FIRST

You MUST follow exactly one connection path. Do NOT mix paths or fall back to PyIceberg when SQL is available.

**Rule 1: If you can execute SQL directly — use SQL. Do NOT use Python/PyIceberg.**
This applies when: a Snowflake MCP tool is available, you are in Cortex Code, a Databricks MCP tool is available, you are in Genie Code, or the user mentions any of these. Ask the user which database/catalog contains their Atlan Lakehouse, then run SQL queries directly. Skip the entire "Python / PyIceberg" section below — do not install packages, do not set up OAuth credentials, do not write Python scripts.

**Rule 2: Only use PyIceberg when no SQL execution environment exists.**
This applies when: you are in a plain terminal (Claude Code CLI), a Jupyter notebook, or a standalone Python script with no database connection.

### How to detect your environment

| Signal | Platform | Action |
|--------|----------|--------|
| Snowflake MCP tool available, or user mentions Cortex Code / Snowflake | **Snowflake** | Run SQL directly against the lakehouse database |
| Databricks MCP tool available, or user mentions Genie Code / Databricks | **Databricks** | Run SQL directly against the lakehouse catalog |
| None of the above | **Python** | Use PyIceberg with OAuth credentials |

## Query Planning — Namespace Selection

After determining your platform, choose the right namespace for the query. Follow this decision tree:

1. **Asset metadata** (completeness, ownership, descriptions, certification, asset inventory, glossary, DQ checks, pipelines, BI assets, data mesh)?
   → **Start with the `GOLD` namespace.** Use `GOLD.ASSETS` as the hub table, join to detail tables as needed.

2. **Asset metadata + tags, custom metadata, readmes, or lineage?**
   → **Hybrid approach:** Start from `GOLD.ASSETS` for core asset data, then join to `ENTITY_METADATA` tables for what GOLD doesn't have:
   - `ENTITY_METADATA.TAGS` — classification tags on assets
   - `ENTITY_METADATA.CUSTOM_METADATA` — custom metadata attributes
   - `ENTITY_METADATA.README` — readme content
   - `ENTITY_METADATA.PROCESS` / `COLUMN_PROCESS` / `BI_PROCESS` — lineage relationships
   - Use `ENTITY_METADATA` directly if GOLD adds no value for the specific query.

3. **Usage analytics** (DAU/WAU/MAU, feature adoption, engagement, retention, health scoring)?
   → **`USAGE_ANALYTICS` namespace** (`PAGES`, `TRACKS`, `USERS`).

4. **Job/pipeline health** (DQ scores, job success rates, pipeline duration)?
   → **`OBSERVABILITY` namespace** (`JOB_METRICS`).

5. **Historical/temporal changes** (audit trails, point-in-time snapshots)?
   → **`ENTITY_HISTORY` namespace** (mirrors `ENTITY_METADATA` with `snapshot_timestamp` / `snapshot_date`).

## Connecting to the Atlan Lakehouse

### Snowflake / Cortex Code

When running inside Snowflake (e.g., Cortex Code), the lakehouse data is already available as a Snowflake database. No credentials or PyIceberg setup needed.

**Ask the user**: "Which Snowflake database contains your Atlan Lakehouse data?" The database name is customer-defined. Once known, query tables directly:

```sql
-- List schemas (namespaces)
SHOW SCHEMAS IN DATABASE <lakehouse_database>;

-- Query GOLD namespace (preferred for asset metadata)
SELECT * FROM <lakehouse_database>.GOLD.ASSETS LIMIT 10;
SELECT * FROM <lakehouse_database>.GOLD.RELATIONAL_ASSET_DETAILS LIMIT 10;

-- Query ENTITY_METADATA (for tags, custom metadata, readmes, lineage)
SELECT * FROM <lakehouse_database>.ENTITY_METADATA.TABLE LIMIT 10;  -- use concrete type tables (TABLE, COLUMN, VIEW, etc.), not ASSET
SELECT * FROM <lakehouse_database>.USAGE_ANALYTICS.TRACKS LIMIT 10;
```

Use the database name as `{{DATABASE}}` in all SQL templates below. The schema maps to the namespace (e.g., `GOLD`, `ENTITY_METADATA`, `USAGE_ANALYTICS`).

### Databricks / Genie Code

When running inside Databricks (e.g., Genie Code), the lakehouse data is already available as an Iceberg catalog in Unity Catalog. No credentials or PyIceberg setup needed.

**Ask the user**: "Which catalog in Unity Catalog contains your Atlan Lakehouse data?" The catalog name is customer-defined. Once known, query tables directly:

```sql
-- List schemas (namespaces)
SHOW SCHEMAS IN <lakehouse_catalog>;

-- Query GOLD namespace (preferred for asset metadata)
SELECT * FROM <lakehouse_catalog>.GOLD.ASSETS LIMIT 10;
SELECT * FROM <lakehouse_catalog>.GOLD.RELATIONAL_ASSET_DETAILS LIMIT 10;

-- Query ENTITY_METADATA (for tags, custom metadata, readmes, lineage)
SELECT * FROM <lakehouse_catalog>.ENTITY_METADATA.TABLE LIMIT 10;  -- use concrete type tables (TABLE, COLUMN, VIEW, etc.), not ASSET
SELECT * FROM <lakehouse_catalog>.USAGE_ANALYTICS.TRACKS LIMIT 10;
```

Use the catalog name as `{{DATABASE}}` in all SQL templates below. The schema maps to the namespace (e.g., `GOLD`, `ENTITY_METADATA`, `USAGE_ANALYTICS`).

### Python / PyIceberg

For generic Python environments (Claude Code, notebooks, scripts), connect via the Polaris REST catalog.

#### Python Dependencies

```bash
python -c "import pyiceberg" 2>/dev/null || uv pip install "pyiceberg[s3fs,adlfs,gcsfs]" pyarrow pandas 2>/dev/null || pip install "pyiceberg[s3fs,adlfs,gcsfs]" pyarrow pandas 2>/dev/null || pip3 install "pyiceberg[s3fs,adlfs,gcsfs]" pyarrow pandas
```

#### Required Credentials

| Credential           | Description                 | How to obtain                                                                                   |
| -------------------- | --------------------------- | ----------------------------------------------------------------------------------------------- |
| `ATLAN_LAKEHOUSE_CLIENT_ID`     | Polaris OAuth client ID     | Marketplace -> Atlan Lakehouse tile -> View connection details                                    |
| `ATLAN_LAKEHOUSE_CLIENT_SECRET` | Polaris OAuth client secret | Marketplace -> Atlan Lakehouse tile -> View connection details                                    |
| `ATLAN_TENANT`       | Your Atlan tenant subdomain | e.g., `tenant` for tenant.atlan.com                                                             |
| `ATLAN_LAKEHOUSE_CATALOG_NAME`  | Polaris catalog name        | Marketplace -> Atlan Lakehouse tile -> View connection details (e.g., `context_store`)            |
| `ATLAN_LAKEHOUSE_ROLE`          | Polaris reader role          | Marketplace -> Atlan Lakehouse tile -> View connection details (typically `lake_readers`)         |

See [Enable Lakehouse — Next Steps](https://docs.atlan.com/platform/lakehouse/how-tos/enable-lakehouse#next-steps) for a walkthrough of the credential retrieval process.

#### How to provide credentials

Check for environment variables first. If any are missing, ask the user to provide them.

1. **Environment variables** (check first — no user action needed):
   ```python
   import os
   client_id = os.environ.get("ATLAN_LAKEHOUSE_CLIENT_ID")
   client_secret = os.environ.get("ATLAN_LAKEHOUSE_CLIENT_SECRET")
   tenant = os.environ.get("ATLAN_TENANT")
   catalog_name = os.environ.get("ATLAN_LAKEHOUSE_CATALOG_NAME")
   role = os.environ.get("ATLAN_LAKEHOUSE_ROLE")
   ```

2. **Ask the user** — if any environment variables are missing, ask the user to provide their credentials. They can either:
   - Paste them inline (the agent should set them as environment variables for the session)
   - Export them before starting:
     ```bash
     export ATLAN_LAKEHOUSE_CLIENT_ID="..."
     export ATLAN_LAKEHOUSE_CLIENT_SECRET="..."
     export ATLAN_TENANT="your-tenant"
     export ATLAN_LAKEHOUSE_CATALOG_NAME="..."
     export ATLAN_LAKEHOUSE_ROLE="lake_readers"
     ```

**IMPORTANT:** Never write credentials to disk, commit them to git, or include them in script output. Keep them in environment variables or in-memory only.

#### PyIceberg Connection

```python
import os
from pyiceberg.catalog import load_catalog

# Load credentials from environment variables (ask user if any are missing)
env_keys = ("ATLAN_LAKEHOUSE_CLIENT_ID", "ATLAN_LAKEHOUSE_CLIENT_SECRET", "ATLAN_TENANT", "ATLAN_LAKEHOUSE_CATALOG_NAME", "ATLAN_LAKEHOUSE_ROLE")
missing = [k for k in env_keys if k not in os.environ]
if missing:
    raise EnvironmentError(f"Missing environment variables: {', '.join(missing)}")

creds = {
    "client_id": os.environ["ATLAN_LAKEHOUSE_CLIENT_ID"],
    "client_secret": os.environ["ATLAN_LAKEHOUSE_CLIENT_SECRET"],
    "tenant": os.environ["ATLAN_TENANT"],
    "catalog_name": os.environ["ATLAN_LAKEHOUSE_CATALOG_NAME"],
    "role": os.environ["ATLAN_LAKEHOUSE_ROLE"],
}

# Create catalog connection
catalog = load_catalog(
    "mdlh",
    type="rest",
    uri=f"https://{creds['tenant']}.atlan.com/api/polaris/api/catalog",
    credential=f"{creds['client_id']}:{creds['client_secret']}",
    warehouse=creds["catalog_name"],
    scope=f"PRINCIPAL_ROLE:{creds['role']}",
)

# List available namespaces
namespaces = catalog.list_namespaces()
print(namespaces)

# List tables in a namespace — try entity_metadata first, fall back to atlan-ns
try:
    tables = catalog.list_tables("entity_metadata")
    entity_ns = "entity_metadata"
except Exception:
    tables = catalog.list_tables("atlan-ns")
    entity_ns = "atlan-ns"
print(f"Entity namespace: {entity_ns}")
print(tables)

# List usage analytics tables
ua_tables = catalog.list_tables("usage_analytics")
print(ua_tables)

# Inspect a table schema
table = catalog.load_table("usage_analytics.tracks")
print(table.schema())
```

> **Case sensitivity:** Polaris returns lowercase namespace, table, and column names (e.g., `usage_analytics.tracks`, column `timestamp` not `TIMESTAMP`). Snowflake/Databricks use uppercase. When using PyIceberg:
> - Use lowercase for namespace and table names: `catalog.load_table("usage_analytics.tracks")`
> - Use lowercase for column references in filters and projections: `df["timestamp"]`, not `df["TIMESTAMP"]`
> - The SQL templates in this skill use uppercase — agents must lowercase all identifiers when generating PyIceberg/pandas/Polars code.

> **Namespace fallback:** The entity metadata namespace is named `entity_metadata` on most tenants, but `atlan-ns` on tenants onboarded before February 2026. Always try `entity_metadata` first; if it fails with a `NoSuchNamespaceError`, fall back to `atlan-ns`.

#### Querying with DataFrames

Once connected, query using your preferred DataFrame library:

- **pandas** (recommended): [ref](https://pandas.pydata.org/docs/dev/reference/api/pandas.read_iceberg.html)
- **Daft**: [ref](https://docs.daft.io/en/stable/connectors/iceberg/)
- **Polars**: [ref](https://docs.pola.rs/api/python/dev/reference/api/polars.scan_iceberg.html)

---

## SQL Template Reference

All templates below are written in Snowflake SQL as the canonical dialect. **Agents should adapt syntax to the target engine on the fly** (e.g., `CONVERT_TIMEZONE` -> `FROM_UTC_TIMESTAMP` for Databricks, `TIMESTAMP_MILLIS` for BigQuery, `TABLE(FLATTEN(...))` -> `LATERAL VIEW EXPLODE(...)` for Databricks, etc.).

Templates use `{{PLACEHOLDER}}` parameters:

| Parameter | Format | Example | Usage |
|-----------|--------|---------|-------|
| `{{DATABASE}}` | Unquoted | `MY_LAKEHOUSE_DB` | Database/catalog name (ask user) |
| `{{SCHEMA}}` | Unquoted | `GOLD`, `ENTITY_METADATA`, `USAGE_ANALYTICS` | Schema/namespace name |
| `{{DOMAIN}}` | Single-quoted | `'acme.atlan.com'` | Your Atlan tenant domain (e.g., `'acme.atlan.com'`). Derive from tenant name: `{tenant}.atlan.com` |
| `{{START_DATE}}` | Single-quoted | `'2025-01-01'` | Date range start |
| `{{END_DATE}}` | Single-quoted | `'2025-12-31'` | Date range end |
| `{{MONTHS_BACK}}` | Bare integer | `6` | Lookback months |
| `{{RETENTION_DAYS}}` | Bare integer | `14` | Retention window |

---

## GOLD Namespace Reference

The GOLD namespace is a **curated, star-schema layer** designed for simplified asset metadata queries. It is the **preferred starting point** for entity/asset metadata use cases.

### Architecture

- **`ASSETS`** is the central hub table containing every catalog asset with common attributes
- **Detail tables** branch out with domain-specific columns for particular asset categories
- All tables use **`guid`** as the primary key for consistent joins
- Data syncs from the underlying `ENTITY_METADATA` namespace in real time

### ASSETS Table (Hub)

Start every query from `ASSETS`, filter to the assets you need, then join to detail tables.

| Column | Description |
|--------|-------------|
| `guid` | Globally-unique asset identifier (primary key, join key for all detail tables) |
| `asset_type` | Asset type (e.g., `Table`, `Column`, `Dashboard`, `DataDomain`) |
| `status` | Asset status (`ACTIVE`, `DELETED`) |
| `asset_name` | Asset name |
| `display_name` | Human-readable display name |
| `asset_qualified_name` | Fully-qualified asset path |
| `description` | System-generated or source catalog description |
| `user_description` | User-authored description within Atlan |
| `readme_guid` | Identifier for the asset's readme |
| `created_at` | Creation timestamp (milliseconds since epoch) |
| `created_by` | Creator user or account |
| `updated_at` | Last update timestamp (milliseconds since epoch) |
| `updated_by` | Last account to update the asset |
| `certificate_status` | Certification status (e.g., `VERIFIED`) |
| `certificate_updated_by` | User who last updated certification |
| `certificate_updated_at` | Certification update timestamp (milliseconds) |
| `connector_name` | Connector type (e.g., `snowflake`, `databricks`, `tableau`) |
| `connector_qualified_name` | Fully-qualified connection name |
| `source_created_at` | Source system creation time (milliseconds) |
| `source_created_by` | Source system creator |
| `source_updated_at` | Source system update time (milliseconds) |
| `source_updated_by` | Source system updater |
| `owner_users` | Array of owning users |
| `owner_groups` | Array of owning groups |
| `term_guids` | Array of associated glossary term GUIDs |
| `popularity_score` | Asset engagement/popularity metric |
| `has_lineage` | Whether this asset has lineage connections |
| `domain_guids` | Array of associated DataDomain GUIDs |
| `product_guids` | Array of associated DataProduct GUIDs |
| `asset_ai_generated_description` | AI-generated description |
| `user_defined_type` | Custom entity type; NULL for native types |

### RELATIONAL_ASSET_DETAILS

Attributes for databases, schemas, tables, views, columns, functions, and materialized views.

| Column | Description |
|--------|-------------|
| `guid` | Asset GUID (join to `ASSETS.guid`) |
| `asset_type` | Type (`Database`, `Schema`, `Table`, `View`, `Column`, etc.) |
| `status` | Asset status |
| `database_name` | Database containing this asset |
| `database_qualified_name` | Fully-qualified database name |
| `database_schemas` | GUIDs for schemas within this database |
| `schema_name` | Schema containing this asset |
| `schema_qualified_name` | Fully-qualified schema name |
| `source_read_query_cost` | Total cost of read queries at source |
| `source_total_cost` | Total cost of all operations at source |
| `source_cost_unit` | Cost unit (e.g., `credits`) |
| `column_datatype` | Column data type |
| `column_is_nullable` | Whether column values can be null |
| `column_queries` | Queries accessing this column |
| `column_recent_users` | Recent users who read this column |
| `column_table_name` | Table containing this column |
| `column_total_read_count` | Total read operation count at source |
| `column_view_name` | View containing this column |
| `table_columns` | GUIDs of columns within this table |
| `table_column_count` | Number of columns in this table |
| `table_queries` | Queries accessing this table |
| `table_recent_users` | Recent users who read this table |
| `table_row_count` | Number of rows |
| `table_size_bytes` | Size in bytes |
| `table_total_read_count` | Total read operation count at source |
| `view_columns` | GUIDs of columns within this view |
| `view_definition` | SQL definition of this view |
| `view_queries` | Queries accessing this view |
| `view_recent_users` | Recent users who read this view |
| `view_total_read_count` | Total read operation count at source |
| `materialised_view_columns` | GUIDs of columns within this materialized view |
| `materialised_view_definition` | Definition of this materialized view |
| `schema_tables` | GUIDs for tables within this schema |
| `schema_views` | GUIDs for views within this schema |
| `schema_materialised_views` | GUIDs for materialized views within this schema |
| `schema_procedures` | GUIDs for stored procedures within this schema |
| `procedure_definition` | Stored procedure definition |
| `function_language` | Function language |
| `function_return_type` | Function return data type |
| `function_schema` | Schema containing this function |
| `function_type` | Function type |
| `query_columns` | GUIDs for columns this query accesses |
| `query_parent` | Collection/folder containing this query |
| `query_raw_query_text` | Raw query text |
| `query_tables` | GUIDs for tables this query accesses |
| `query_views` | GUIDs for views this query accesses |

### GLOSSARY_DETAILS

Glossary, category, and term attributes.

| Column | Description |
|--------|-------------|
| `guid` | Asset GUID (join to `ASSETS.guid`) |
| `asset_type` | Type (`Glossary`, `GlossaryTerm`, `GlossaryCategory`) |
| `status` | Asset status |
| `terms` | Term GUIDs belonging to this glossary/category |
| `categories` | Category GUIDs associated with this glossary/term |
| `assigned_entities` | Asset GUIDs to which this term has been applied |
| `readme_guid` | Identifier for the asset's readme |
| `anchor_guid` | GUID of the parent glossary |

### DATA_QUALITY_DETAILS

Data quality checks across Anomalo, Soda, Monte Carlo, and native Atlan DQ rules.

| Column | Description |
|--------|-------------|
| `guid` | Asset GUID (join to `ASSETS.guid`) |
| `asset_type` | Type (`AnomaloCheck`, `SodaCheck`, `MCMonitor`, etc.) |
| `status` | Asset status |
| `source_url` | Source URL for this DQ check |
| `anomalo_check_type` | Anomalo check type |
| `anomalo_check_status` | Most recent Anomalo check status |
| `anomalo_check_last_run_completed_at` | Last Anomalo execution timestamp |
| `anomalo_check_linked_asset_qualified_name` | Qualified name of associated asset |
| `soda_check_id` | Soda check identifier |
| `soda_check_definition` | Soda check definition |
| `soda_check_evaluation_status` | Most recent Soda check status |
| `soda_check_last_scan_at` | Last Soda execution timestamp |
| `soda_check_columns` | Columns associated with this Soda check |
| `soda_check_assets` | Assets associated with this Soda check |
| `mc_monitor_id` | Monte Carlo monitor identifier |
| `mc_monitor_type` | Monte Carlo monitor type |
| `mc_monitor_status` | Most recent Monte Carlo status |
| `mc_monitor_rule_last_execution_at` | Last Monte Carlo execution timestamp |
| `mc_monitor_assets` | Assets associated with this Monte Carlo monitor |
| `dq_rule_base_dataset_qualified_name` | Qualified name of dataset the rule applies to |
| `dq_rule_base_column_qualified_name` | Qualified name of column the rule applies to |
| `dq_rule_dimension` | Dimension (completeness, accuracy, etc.) |
| `dq_rule_latest_result` | Latest rule execution result |
| `dq_rule_alert_priority` | Priority level (`LOW`, `NORMAL`, `URGENT`) |

### PIPELINE_DETAILS

Pipeline and ETL process attributes across ADF, Airflow, dbt, Fivetran, and Matillion.

| Column | Description |
|--------|-------------|
| `guid` | Asset GUID (join to `ASSETS.guid`) |
| `asset_type` | Type (`AirflowDag`, `DbtModel`, `FivetranConnector`, etc.) |
| `status` | Asset status |
| `input_guids_to_processes` | Processes using this asset as an input |
| `output_guids_from_processes` | Processes producing this asset as output |

### BI_ASSET_DETAILS

BI tool attributes for PowerBI, Tableau, Looker, and Sigma.

| Column | Description |
|--------|-------------|
| `guid` | Asset GUID (join to `ASSETS.guid`) |
| `asset_type` | Type (`PowerBIDashboard`, `TableauWorkbook`, `SigmaPage`, etc.) |
| `status` | Asset status |
| `workspace_qualified_name` | PowerBI workspace qualified name |
| `project_qualified_name` | Tableau project qualified name |
| `folder_name` | Looker folder display name |
| `site_qualified_name` | Tableau site qualified name |

### DATA_MESH_DETAILS

Data domain and data product attributes.

| Column | Description |
|--------|-------------|
| `guid` | Asset GUID (join to `ASSETS.guid`) |
| `asset_type` | Type (`DataDomain`, `DataProduct`) |
| `status` | Asset status |
| `data_products` | GUIDs of data products within a domain |
| `parent_domain` | GUID of parent data domain |
| `stakeholders` | Assigned stakeholder GUIDs |
| `subdomains` | Child data domain GUIDs |
| `data_product_status` | Data product state |
| `criticality` | Business importance level |
| `sensitivity` | Data sensitivity classification |
| `visibility` | Access/visibility scope |
| `input_port_guids` | Input port GUIDs |
| `output_port_guids` | Output port GUIDs |
| `data_domain` | GUID of the containing data domain |
| `assets_dsl` | Search definition for asset inclusion |
| `assets_playbook_filter` | Filtering logic for product assets |

### GOLD Namespace Best Practices

1. **Always start from `ASSETS`**: Filter to the assets you need, then join to detail tables.
2. **Filter early**: Apply `status = 'ACTIVE'`, `asset_type`, and `connector_name` filters before joins to minimize scanned data.
3. **Join on `guid`**: Always join detail tables using `guid`, not asset names or qualified names.
4. **Pair GUID lookups with `asset_type`**: Narrows the search space significantly.
5. **Avoid functions in WHERE clauses**: Wrapping columns in `LOWER()` etc. disables query optimization.
6. **Handle NULLs explicitly**: Use `COALESCE()` or explicit NULL checks for optional fields (owners, descriptions).
7. **Test with `LIMIT 100`**: Validate queries before running against full datasets.

### GOLD Namespace Templates

#### Asset Inventory: Relational Assets with Details

```sql
SELECT
    a.asset_name,
    a.asset_type,
    a.connector_name,
    a.asset_qualified_name,
    a.certificate_status,
    a.owner_users,
    a.description,
    r.database_name,
    r.schema_name,
    r.table_row_count,
    r.table_size_bytes,
    r.table_column_count,
    r.source_read_query_cost,
    r.source_total_cost,
    r.source_cost_unit
FROM {{DATABASE}}.GOLD.ASSETS a
LEFT JOIN {{DATABASE}}.GOLD.RELATIONAL_ASSET_DETAILS r ON a.guid = r.guid
WHERE a.status = 'ACTIVE'
  AND a.asset_type IN ('Table', 'View', 'MaterializedView')
  AND a.connector_name = 'snowflake'  -- adjust to your connector
ORDER BY r.table_row_count DESC NULLS LAST;
```

#### Asset Inventory: BI Assets Across Tools

```sql
SELECT
    a.asset_name,
    a.asset_type,
    a.connector_name,
    a.certificate_status,
    a.owner_users,
    a.description,
    a.popularity_score,
    b.workspace_qualified_name,
    b.project_qualified_name,
    b.folder_name,
    b.site_qualified_name
FROM {{DATABASE}}.GOLD.ASSETS a
LEFT JOIN {{DATABASE}}.GOLD.BI_ASSET_DETAILS b ON a.guid = b.guid
WHERE a.status = 'ACTIVE'
  AND a.asset_type IN ('PowerBIDashboard', 'PowerBIReport', 'TableauWorkbook', 'TableauDashboard', 'LookerDashboard', 'SigmaPage')
ORDER BY a.popularity_score DESC NULLS LAST;
```

#### Asset Inventory: Data Quality Summary

```sql
SELECT
    a.asset_name,
    a.asset_type,
    a.connector_name,
    dq.source_url,
    COALESCE(dq.anomalo_check_status, dq.soda_check_evaluation_status, dq.mc_monitor_status, dq.dq_rule_latest_result) AS latest_status,
    COALESCE(dq.anomalo_check_last_run_completed_at, dq.soda_check_last_scan_at, dq.mc_monitor_rule_last_execution_at) AS last_run_at,
    dq.dq_rule_dimension,
    dq.dq_rule_alert_priority
FROM {{DATABASE}}.GOLD.ASSETS a
INNER JOIN {{DATABASE}}.GOLD.DATA_QUALITY_DETAILS dq ON a.guid = dq.guid
WHERE a.status = 'ACTIVE'
ORDER BY dq.dq_rule_alert_priority DESC NULLS LAST, a.asset_name;
```

#### Asset Inventory: Pipeline Dependencies

```sql
SELECT
    a.asset_name,
    a.asset_type,
    a.connector_name,
    a.certificate_status,
    a.owner_users,
    p.input_guids_to_processes,
    p.output_guids_from_processes
FROM {{DATABASE}}.GOLD.ASSETS a
INNER JOIN {{DATABASE}}.GOLD.PIPELINE_DETAILS p ON a.guid = p.guid
WHERE a.status = 'ACTIVE'
ORDER BY a.connector_name, a.asset_type, a.asset_name;
```

#### Metadata Completeness: Asset Enrichment Tracking (GOLD + ENTITY_METADATA Hybrid)

Measures description, certification, ownership, term, tag, and custom metadata coverage by asset type. Uses `GOLD.ASSETS` for core data and joins to `ENTITY_METADATA.TAGS` and `ENTITY_METADATA.CUSTOM_METADATA` for tag and custom metadata coverage.

```sql
WITH tag_stats AS (
    SELECT
        t.ASSET_GUID,
        COUNT(*) AS tag_count
    FROM {{DATABASE}}.ENTITY_METADATA.TAGS t
    GROUP BY t.ASSET_GUID
),
cm_stats AS (
    SELECT
        cm.ASSET_GUID,
        SUM(
            CASE
                WHEN cm.ATTRIBUTE_VALUE IS NULL THEN 0
                WHEN IS_ARRAY(TRY_PARSE_JSON(cm.ATTRIBUTE_VALUE))
                THEN CASE WHEN ARRAY_SIZE(TRY_PARSE_JSON(cm.ATTRIBUTE_VALUE)) > 0 THEN 1 ELSE 0 END
                WHEN cm.ATTRIBUTE_VALUE IS NOT NULL THEN 1
                ELSE 0
            END
        ) AS linked_cm_prop_count
    FROM {{DATABASE}}.ENTITY_METADATA.CUSTOM_METADATA cm
    GROUP BY cm.ASSET_GUID
),
entity_stats AS (
    SELECT
        a.asset_type,
        COUNT(*) AS total_count,
        COUNT(CASE WHEN a.description IS NOT NULL AND a.description <> '' THEN 1 END) AS with_description,
        COUNT(CASE WHEN LOWER(a.certificate_status) = 'verified' THEN 1 END) AS certified,
        COUNT(CASE WHEN a.owner_users IS NOT NULL AND ARRAY_SIZE(a.owner_users) > 0 THEN 1 END) AS with_owners,
        COUNT(CASE WHEN a.term_guids IS NOT NULL AND ARRAY_SIZE(a.term_guids) > 0 THEN 1 END) AS with_terms,
        COUNT(CASE WHEN ts.tag_count > 0 THEN 1 END) AS with_tags,
        COUNT(CASE WHEN cm.linked_cm_prop_count > 0 THEN 1 END) AS with_linked_cm_props
    FROM {{DATABASE}}.GOLD.ASSETS a
    LEFT JOIN tag_stats ts ON a.guid = ts.ASSET_GUID
    LEFT JOIN cm_stats cm ON a.guid = cm.ASSET_GUID
    WHERE a.status = 'ACTIVE'
      AND a.asset_type IN ('Table', 'Schema', 'TableauDashboard', 'TableauWorkbook',
                           'DataDomain', 'DataProduct', 'AtlasGlossaryTerm',
                           'AtlasGlossaryCategory', 'AtlasGlossary')
    GROUP BY a.asset_type
)
SELECT
    asset_type, total_count,
    with_description,
    ROUND((with_description * 100.0) / NULLIF(total_count, 0), 2) AS description_coverage_pct,
    with_tags,
    ROUND((with_tags * 100.0) / NULLIF(total_count, 0), 2) AS tag_coverage_pct,
    certified,
    ROUND((certified * 100.0) / NULLIF(total_count, 0), 2) AS certification_coverage_pct,
    with_owners,
    ROUND((with_owners * 100.0) / NULLIF(total_count, 0), 2) AS ownership_coverage_pct,
    with_terms,
    ROUND((with_terms * 100.0) / NULLIF(total_count, 0), 2) AS term_coverage_pct,
    with_linked_cm_props,
    ROUND((with_linked_cm_props * 100.0) / NULLIF(total_count, 0), 2) AS custom_metadata_coverage_pct
FROM entity_stats
ORDER BY asset_type;
```

#### Metadata Completeness: By Data Domain (GOLD)

Domain-level enrichment statistics with an overall enrichment score.

```sql
WITH assets_with_enrichment AS (
    SELECT
        COALESCE(d_assets.asset_name, 'No Domain Assigned') AS domain_name,
        a.guid AS asset_guid,
        a.asset_name, a.asset_type,
        CASE WHEN a.term_guids IS NOT NULL AND ARRAY_SIZE(a.term_guids) > 0 THEN 1 ELSE 0 END AS has_terms,
        CASE WHEN a.readme_guid IS NOT NULL THEN 1 ELSE 0 END AS has_readme,
        CASE WHEN a.description IS NOT NULL AND LENGTH(TRIM(a.description)) > 0 THEN 1 ELSE 0 END AS has_description
    FROM {{DATABASE}}.GOLD.ASSETS a
    LEFT JOIN {{DATABASE}}.GOLD.DATA_MESH_DETAILS dmd ON a.guid = dmd.guid
    LEFT JOIN {{DATABASE}}.GOLD.ASSETS d_assets ON dmd.data_domain = d_assets.guid
    WHERE a.status = 'ACTIVE'
)
SELECT
    domain_name,
    COUNT(*) AS total_assets,
    SUM(has_terms) AS assets_with_terms,
    ROUND((SUM(has_terms) * 100.0) / NULLIF(COUNT(*), 0), 2) AS pct_with_terms,
    SUM(has_readme) AS assets_with_readme,
    ROUND((SUM(has_readme) * 100.0) / NULLIF(COUNT(*), 0), 2) AS pct_with_readme,
    SUM(has_description) AS assets_with_description,
    ROUND((SUM(has_description) * 100.0) / NULLIF(COUNT(*), 0), 2) AS pct_with_description,
    ROUND(((SUM(has_terms) * 100.0) / NULLIF(COUNT(*), 0)
         + (SUM(has_readme) * 100.0) / NULLIF(COUNT(*), 0)
         + (SUM(has_description) * 100.0) / NULLIF(COUNT(*), 0)) / 3.0, 2
    ) AS overall_enrichment_score
FROM assets_with_enrichment
GROUP BY domain_name
ORDER BY CASE WHEN domain_name = 'No Domain Assigned' THEN 1 ELSE 0 END, overall_enrichment_score DESC;
```

#### Glossary: Comprehensive Term Export (GOLD)

Retrieves glossary terms with parent glossary, categories, and assigned entity details using GOLD tables.

```sql
WITH glossary_terms AS (
    SELECT
        a.guid AS term_guid, a.asset_name AS term_name, a.asset_qualified_name AS term_qualified_name,
        a.description AS term_description, a.status AS term_status,
        a.certificate_status AS term_certificate_status, a.owner_users AS term_owner_users,
        TO_TIMESTAMP_LTZ(a.created_at / 1000) AS term_created_at,
        TO_TIMESTAMP_LTZ(a.updated_at / 1000) AS term_updated_at,
        a.created_by AS term_created_by, a.updated_by AS term_updated_by,
        g.anchor_guid AS glossary_guid,
        g.categories AS term_categories,
        a.readme_guid AS term_readme_guid,
        g.assigned_entities AS term_assigned_entities
    FROM {{DATABASE}}.GOLD.ASSETS a
    INNER JOIN {{DATABASE}}.GOLD.GLOSSARY_DETAILS g ON a.guid = g.guid
    WHERE a.asset_type = 'AtlasGlossaryTerm' AND a.status = 'ACTIVE'
),
glossary_info AS (
    SELECT a.guid AS glossary_guid, a.asset_name AS glossary_name,
           a.asset_qualified_name AS glossary_qualified_name, a.description AS glossary_description,
           a.certificate_status AS glossary_certificate_status, a.owner_users AS glossary_owner_users
    FROM {{DATABASE}}.GOLD.ASSETS a
    INNER JOIN {{DATABASE}}.GOLD.GLOSSARY_DETAILS g ON a.guid = g.guid
    WHERE a.asset_type = 'AtlasGlossary' AND a.status = 'ACTIVE'
),
category_info AS (
    SELECT a.guid AS category_guid, a.asset_name AS category_name,
           a.asset_qualified_name AS category_qualified_name, a.description AS category_description
    FROM {{DATABASE}}.GOLD.ASSETS a
    INNER JOIN {{DATABASE}}.GOLD.GLOSSARY_DETAILS g ON a.guid = g.guid
    WHERE a.asset_type = 'AtlasGlossaryCategory' AND a.status = 'ACTIVE'
),
term_categories_expanded AS (
    SELECT t.term_guid, cat_flat.VALUE::STRING AS category_guid
    FROM glossary_terms t, TABLE(FLATTEN(input => t.term_categories)) AS cat_flat
    WHERE t.term_categories IS NOT NULL
),
term_assigned_assets AS (
    SELECT t.term_guid, asset_flat.VALUE::STRING AS asset_guid
    FROM glossary_terms t, TABLE(FLATTEN(input => t.term_assigned_entities)) AS asset_flat
    WHERE t.term_assigned_entities IS NOT NULL
),
asset_details AS (
    SELECT taa.term_guid, a.guid AS asset_guid, a.asset_name, a.asset_qualified_name, a.asset_type
    FROM term_assigned_assets taa
    INNER JOIN {{DATABASE}}.GOLD.ASSETS a ON a.guid = taa.asset_guid
    WHERE a.status = 'ACTIVE'
)
SELECT
    t.term_guid, t.term_name, t.term_qualified_name, t.term_description,
    t.term_status, t.term_certificate_status, t.term_owner_users,
    t.term_created_at, t.term_updated_at, t.term_created_by, t.term_updated_by,
    DATEDIFF(day, t.term_updated_at, CURRENT_TIMESTAMP()) AS days_since_update,
    g.glossary_guid, g.glossary_name, g.glossary_qualified_name, g.glossary_description,
    g.glossary_certificate_status, g.glossary_owner_users,
    ARRAY_AGG(DISTINCT c.category_guid) AS category_guids,
    ARRAY_AGG(DISTINCT c.category_name) AS category_names,
    COUNT(DISTINCT c.category_guid) AS category_count,
    ARRAY_AGG(DISTINCT ad.asset_guid) AS assigned_asset_guids,
    ARRAY_AGG(DISTINCT ad.asset_name) AS assigned_asset_names,
    ARRAY_AGG(DISTINCT ad.asset_type) AS assigned_asset_types,
    COUNT(DISTINCT ad.asset_guid) AS assigned_asset_count
FROM glossary_terms t
LEFT JOIN glossary_info g ON g.glossary_guid = t.glossary_guid
LEFT JOIN term_categories_expanded tce ON tce.term_guid = t.term_guid
LEFT JOIN category_info c ON c.category_guid = tce.category_guid
LEFT JOIN asset_details ad ON ad.term_guid = t.term_guid
GROUP BY t.term_guid, t.term_name, t.term_qualified_name, t.term_description,
         t.term_status, t.term_certificate_status, t.term_owner_users,
         t.term_created_at, t.term_updated_at, t.term_created_by, t.term_updated_by,
         g.glossary_guid, g.glossary_name, g.glossary_qualified_name, g.glossary_description,
         g.glossary_certificate_status, g.glossary_owner_users
ORDER BY g.glossary_name, t.term_name;
```

---

## Entity Metadata Templates

> **For most asset metadata queries, prefer the GOLD namespace (above) which provides simpler, pre-joined tables.** Use `ENTITY_METADATA` directly when you need: tags (`TAGS` table), custom metadata (`CUSTOM_METADATA` table), readmes (`README` table), lineage (`PROCESS` / `COLUMN_PROCESS` / `BI_PROCESS` tables), or per-type tables not covered by GOLD.

These query the `ENTITY_METADATA` namespace. Key relationship/consolidated tables include `TAGS`, `CUSTOM_METADATA`, `GLOSSARY_DETAILS`, `README`, `DATA_MESH_DETAILS`, and lineage process tables (`PROCESS`, `COLUMN_PROCESS`, `BI_PROCESS`). Asset data lives in **per-type tables** (e.g., `TABLE`, `COLUMN`, `VIEW`, `DBTMODEL`, `GLOSSARYTERM`). Use `catalog.list_tables("ENTITY_METADATA")` or `SHOW TABLES IN <db>.ENTITY_METADATA` to discover the full set.

> **Supertype tables have 0 rows.** Atlan's metamodel uses a type hierarchy. Abstract supertype tables like `ASSET`, `SQL`, `BI`, `SAAS`, `CLOUD`, `NOSQL`, `OBJECTSTORE`, `CATALOG`, `INFRASTRUCTURE`, and `EVENTSTORE` exist in the namespace but contain no data — they are structural parents only. Always query the concrete type table instead (e.g., `TABLE` not `ASSET`, `SNOWFLAKE` not `SQL`). Similarly, tables for unused connectors (e.g., `AIRFLOWDAG` on a tenant that does not use Airflow) will have 0 rows. When building cross-type queries, `UNION ALL` across the specific type tables you need rather than querying a supertype.

> **Note:** The metadata completeness and glossary templates have been moved to the GOLD Namespace Templates section above, which provides simpler, pre-joined queries. Lineage query templates will be added in a future update using the `PROCESS`, `COLUMN_PROCESS`, and `BI_PROCESS` tables. In the meantime, you can use `GOLD.ASSETS.has_lineage` to check lineage coverage without joining process tables.

---

## Usage Analytics Templates

These query the `USAGE_ANALYTICS` namespace. All templates below use `{{DATABASE}}.{{SCHEMA}}.TABLE` references where `{{SCHEMA}}` is `USAGE_ANALYTICS` (or whatever the customer's schema is named).

### Key Conventions

These conventions are critical for correct results across all usage analytics queries:

**Domain**: Your domain is your Atlan tenant hostname (e.g., `acme.atlan.com`). For Snowflake/Databricks, ask the user for their domain. For PyIceberg, derive it from the tenant name: `{ATLAN_TENANT}.atlan.com`.

**Domain**: Always derive domain from `PAGES.domain` (100% populated). The `TRACKS` table has no reliable domain column. For TRACKS, build a user-to-domain lookup:
```sql
-- Standard user_domains CTE (used in most queries)
WITH user_domains AS (
    SELECT user_id, MAX(domain) AS domain
    FROM {{DATABASE}}.{{SCHEMA}}.PAGES
    WHERE domain IS NOT NULL
    GROUP BY user_id
)
```

**Identity**: Use `user_id` (UUID) as the primary key, not email. Most active user_ids (~98%) have no matching `USERS` record. Always LEFT JOIN to USERS for optional enrichment.

**Noise filtering**: Exclude these known noise events from all TRACKS queries:
```sql
AND event_text NOT IN (
    'atlan_analaytics_aggregateinfo_fetch',
    'api_error_emit', 'api_evaluator_cancelled', 'api_evaluator_succeeded',
    'Experiment Started', '$experiment_started',
    'web_vital_metric_inp_track', 'web_vital_metric_ttfb_track',
    'performance_metric_user_timing_discovery_search',
    'performance_metric_user_timing_app_bootstrap',
    'web_vital_metric_fcp_track', 'web_vital_metric_lcp_track'
)
AND event_text NOT LIKE 'workflow_%'
```

**Sessions**: No session ID column is populated in the data. Derive sessions from 30-minute inactivity gaps using this pattern:
```sql
-- Derive sessions from time gaps
raw_events AS (
    SELECT user_id, TIMESTAMP,
        LAG(TIMESTAMP) OVER (PARTITION BY user_id ORDER BY TIMESTAMP) AS prev_ts
    FROM ( /* combined TRACKS + PAGES */ )
),
session_boundaries AS (
    SELECT user_id, TIMESTAMP,
        CASE WHEN prev_ts IS NULL THEN 1
             WHEN DATEDIFF('second', prev_ts, TIMESTAMP) > 1800 THEN 1
             ELSE 0 END AS is_new_session
    FROM raw_events
),
session_numbered AS (
    SELECT user_id, TIMESTAMP,
        SUM(is_new_session) OVER (PARTITION BY user_id ORDER BY TIMESTAMP) AS session_id
    FROM session_boundaries
),
derived_sessions AS (
    SELECT user_id, session_id,
        DATE(MIN(TIMESTAMP)) AS session_date,
        MIN(TIMESTAMP) AS session_start, MAX(TIMESTAMP) AS session_end,
        COUNT(*) AS event_count,
        DATEDIFF('second', MIN(TIMESTAMP), MAX(TIMESTAMP)) AS duration_seconds
    FROM session_numbered
    GROUP BY user_id, session_id
)
```

**Timezone**: All timestamps are stored in UTC. Convert to the user's local timezone as needed: `CONVERT_TIMEZONE('UTC', '<timezone>', TIMESTAMP)`.

**Feature area mapping**: Map raw page names and event prefixes to logical feature areas:
```
Discovery/Search: pages=discovery, events=discovery_search_*, discovery_filter_*
Chrome Extension:  pages=reverse-metadata-sidebar, events=chrome_*
Insights/SQL:      pages=saved_query/insights, events=insights_*
Governance:        pages=glossary/term/category/classifications, events=governance_*, gtc_tree_*
AI Copilot:        events=atlan_ai_*
Lineage:           events=lineage_*
Asset Profile:     pages=asset_profile/overview
Admin:             pages=users/personas/config/sso/api-access/api_keys/policyManager
Workflows:         pages=workflows-home/workflows-profile/runs/playbook
Data Quality:      pages=monitor
Data Products:     events=products_home_*
```

---

### 0. Schema Profiling

#### Table Profiler

Run these standalone queries to understand data shape and coverage.

```sql
-- TRACKS shape
SELECT 'TRACKS' AS table_name, COUNT(*) AS total_rows, COUNT(DISTINCT user_id) AS distinct_users,
    MIN(TIMESTAMP) AS earliest_event, MAX(TIMESTAMP) AS latest_event,
    COUNT(DISTINCT DATE(TIMESTAMP)) AS days_with_data, COUNT(DISTINCT event_text) AS distinct_event_types
FROM {{DATABASE}}.{{SCHEMA}}.TRACKS;

-- PAGES shape
SELECT 'PAGES' AS table_name, COUNT(*) AS total_rows, COUNT(DISTINCT user_id) AS distinct_users,
    MIN(TIMESTAMP) AS earliest_event, MAX(TIMESTAMP) AS latest_event,
    COUNT(DISTINCT DATE(TIMESTAMP)) AS days_with_data, COUNT(domain) AS has_domain,
    COUNT(DISTINCT name) AS distinct_page_names
FROM {{DATABASE}}.{{SCHEMA}}.PAGES;

-- USERS shape
SELECT 'USERS' AS table_name, COUNT(*) AS total_rows, COUNT(DISTINCT id) AS distinct_users,
    MIN(received_at) AS earliest_received, MAX(received_at) AS latest_received,
    COUNT(email) AS has_email, COUNT(role) AS has_role, COUNT(license_type) AS has_license_type
FROM {{DATABASE}}.{{SCHEMA}}.USERS;

-- User ID overlap between tables
SELECT COUNT(DISTINCT p.user_id) AS pages_users,
    COUNT(DISTINCT CASE WHEN u.id IS NOT NULL THEN p.user_id END) AS pages_matched_to_users,
    COUNT(DISTINCT CASE WHEN u.id IS NULL THEN p.user_id END) AS pages_unmatched
FROM {{DATABASE}}.{{SCHEMA}}.PAGES p
LEFT JOIN (SELECT DISTINCT id FROM {{DATABASE}}.{{SCHEMA}}.USERS) u ON u.id = p.user_id;

-- Domain coverage
SELECT domain, COUNT(DISTINCT user_id) AS distinct_users, COUNT(*) AS total_events,
    MIN(TIMESTAMP) AS earliest, MAX(TIMESTAMP) AS latest
FROM {{DATABASE}}.{{SCHEMA}}.PAGES
WHERE domain IS NOT NULL GROUP BY domain ORDER BY total_events DESC;

-- Top event types (noise filtered)
SELECT event_text, COUNT(*) AS cnt, COUNT(DISTINCT user_id) AS unique_users
FROM {{DATABASE}}.{{SCHEMA}}.TRACKS
WHERE event_text NOT IN (
    'atlan_analaytics_aggregateinfo_fetch', 'api_error_emit', 'api_evaluator_cancelled', 'api_evaluator_succeeded',
    'Experiment Started', '$experiment_started', 'web_vital_metric_inp_track', 'web_vital_metric_ttfb_track',
    'performance_metric_user_timing_discovery_search', 'performance_metric_user_timing_app_bootstrap',
    'web_vital_metric_fcp_track', 'web_vital_metric_lcp_track')
AND event_text NOT LIKE 'workflow_%'
GROUP BY event_text ORDER BY cnt DESC LIMIT 30;
```

#### Discover Events

```sql
WITH user_domains AS (
    SELECT user_id, MAX(domain) AS domain
    FROM {{DATABASE}}.{{SCHEMA}}.PAGES WHERE domain IS NOT NULL GROUP BY user_id
)
SELECT t.event_text, COUNT(*) AS total_occurrences, COUNT(DISTINCT t.user_id) AS unique_users,
    COUNT(DISTINCT ud.domain) AS domains_using, MIN(DATE(t.TIMESTAMP)) AS first_seen, MAX(DATE(t.TIMESTAMP)) AS last_seen
FROM {{DATABASE}}.{{SCHEMA}}.TRACKS t
INNER JOIN user_domains ud ON ud.user_id = t.user_id
WHERE t.event_text NOT IN (
    'atlan_analaytics_aggregateinfo_fetch', 'api_error_emit', 'api_evaluator_cancelled', 'api_evaluator_succeeded',
    'Experiment Started', '$experiment_started', 'web_vital_metric_inp_track', 'web_vital_metric_ttfb_track',
    'performance_metric_user_timing_discovery_search', 'performance_metric_user_timing_app_bootstrap',
    'web_vital_metric_fcp_track', 'web_vital_metric_lcp_track')
AND t.event_text NOT LIKE 'workflow_%'
AND t.TIMESTAMP >= DATEADD('month', -3, CURRENT_TIMESTAMP())
GROUP BY t.event_text ORDER BY total_occurrences DESC;
```

#### Discover Pages

```sql
SELECT p.name AS page_name, COUNT(*) AS total_views, COUNT(DISTINCT p.user_id) AS unique_users,
    COUNT(DISTINCT p.domain) AS domains_using, MIN(DATE(p.TIMESTAMP)) AS first_seen, MAX(DATE(p.TIMESTAMP)) AS last_seen
FROM {{DATABASE}}.{{SCHEMA}}.PAGES p
WHERE p.name IS NOT NULL AND p.TIMESTAMP >= DATEADD('month', -3, CURRENT_TIMESTAMP())
GROUP BY p.name ORDER BY total_views DESC;
```

---

### 1. Active Users

#### MAU by Domain (with month-over-month delta)

```sql
WITH user_domains AS (
    SELECT user_id, MAX(domain) AS domain
    FROM {{DATABASE}}.{{SCHEMA}}.PAGES WHERE domain IS NOT NULL GROUP BY user_id
),
activity_events AS (
    SELECT t.user_id, ud.domain,
        DATE_TRUNC('MONTH', CONVERT_TIMEZONE('UTC', 'Asia/Kolkata', t.TIMESTAMP)) AS event_month
    FROM {{DATABASE}}.{{SCHEMA}}.TRACKS t
    INNER JOIN user_domains ud ON ud.user_id = t.user_id
    WHERE t.event_text NOT IN (
        'atlan_analaytics_aggregateinfo_fetch', 'api_error_emit', 'api_evaluator_cancelled', 'api_evaluator_succeeded',
        'Experiment Started', '$experiment_started', 'web_vital_metric_inp_track', 'web_vital_metric_ttfb_track',
        'performance_metric_user_timing_discovery_search', 'performance_metric_user_timing_app_bootstrap',
        'web_vital_metric_fcp_track', 'web_vital_metric_lcp_track')
    AND t.event_text NOT LIKE 'workflow_%' AND t.TIMESTAMP >= {{START_DATE}}
    UNION ALL
    SELECT p.user_id, p.domain,
        DATE_TRUNC('MONTH', CONVERT_TIMEZONE('UTC', 'Asia/Kolkata', p.TIMESTAMP)) AS event_month
    FROM {{DATABASE}}.{{SCHEMA}}.PAGES p WHERE p.TIMESTAMP >= {{START_DATE}}
),
mau_counts AS (
    SELECT domain, event_month, COUNT(DISTINCT user_id) AS mau
    FROM activity_events WHERE domain = {{DOMAIN}} GROUP BY domain, event_month
)
SELECT domain, event_month, mau,
    LAG(mau) OVER (PARTITION BY domain ORDER BY event_month) AS prev_month_mau,
    mau - LAG(mau) OVER (PARTITION BY domain ORDER BY event_month) AS mau_delta,
    ROUND(100.0 * (mau - LAG(mau) OVER (PARTITION BY domain ORDER BY event_month))
        / NULLIF(LAG(mau) OVER (PARTITION BY domain ORDER BY event_month), 0), 1) AS mau_change_pct
FROM mau_counts ORDER BY domain, event_month DESC;
```

#### DAU by Domain

```sql
WITH user_domains AS (
    SELECT user_id, MAX(domain) AS domain
    FROM {{DATABASE}}.{{SCHEMA}}.PAGES WHERE domain IS NOT NULL GROUP BY user_id
),
activity_events AS (
    SELECT t.user_id, ud.domain,
        DATE(CONVERT_TIMEZONE('UTC', 'Asia/Kolkata', t.TIMESTAMP)) AS event_date
    FROM {{DATABASE}}.{{SCHEMA}}.TRACKS t
    INNER JOIN user_domains ud ON ud.user_id = t.user_id
    WHERE t.event_text NOT IN (
        'atlan_analaytics_aggregateinfo_fetch', 'api_error_emit', 'api_evaluator_cancelled', 'api_evaluator_succeeded',
        'Experiment Started', '$experiment_started', 'web_vital_metric_inp_track', 'web_vital_metric_ttfb_track',
        'performance_metric_user_timing_discovery_search', 'performance_metric_user_timing_app_bootstrap',
        'web_vital_metric_fcp_track', 'web_vital_metric_lcp_track')
    AND t.event_text NOT LIKE 'workflow_%' AND t.TIMESTAMP >= {{START_DATE}}
    UNION ALL
    SELECT p.user_id, p.domain,
        DATE(CONVERT_TIMEZONE('UTC', 'Asia/Kolkata', p.TIMESTAMP)) AS event_date
    FROM {{DATABASE}}.{{SCHEMA}}.PAGES p WHERE p.TIMESTAMP >= {{START_DATE}}
)
SELECT domain, event_date, COUNT(DISTINCT user_id) AS dau
FROM activity_events WHERE domain = {{DOMAIN}}
GROUP BY domain, event_date ORDER BY domain, event_date DESC;
```

#### WAU by Domain

```sql
WITH user_domains AS (
    SELECT user_id, MAX(domain) AS domain
    FROM {{DATABASE}}.{{SCHEMA}}.PAGES WHERE domain IS NOT NULL GROUP BY user_id
),
activity_events AS (
    SELECT t.user_id, ud.domain,
        DATE_TRUNC('WEEK', CONVERT_TIMEZONE('UTC', 'Asia/Kolkata', t.TIMESTAMP)) AS event_week
    FROM {{DATABASE}}.{{SCHEMA}}.TRACKS t
    INNER JOIN user_domains ud ON ud.user_id = t.user_id
    WHERE t.event_text NOT IN (
        'atlan_analaytics_aggregateinfo_fetch', 'api_error_emit', 'api_evaluator_cancelled', 'api_evaluator_succeeded',
        'Experiment Started', '$experiment_started', 'web_vital_metric_inp_track', 'web_vital_metric_ttfb_track',
        'performance_metric_user_timing_discovery_search', 'performance_metric_user_timing_app_bootstrap',
        'web_vital_metric_fcp_track', 'web_vital_metric_lcp_track')
    AND t.event_text NOT LIKE 'workflow_%' AND t.TIMESTAMP >= {{START_DATE}}
    UNION ALL
    SELECT p.user_id, p.domain,
        DATE_TRUNC('WEEK', CONVERT_TIMEZONE('UTC', 'Asia/Kolkata', p.TIMESTAMP)) AS event_week
    FROM {{DATABASE}}.{{SCHEMA}}.PAGES p WHERE p.TIMESTAMP >= {{START_DATE}}
)
SELECT domain, event_week, COUNT(DISTINCT user_id) AS wau
FROM activity_events WHERE domain = {{DOMAIN}}
GROUP BY domain, event_week ORDER BY domain, event_week DESC;
```

#### Stickiness Ratio (DAU/MAU)

> Stickiness >0.3 = strong daily engagement; 0.1-0.3 = moderate; <0.1 = episodic use.

```sql
WITH user_domains AS (
    SELECT user_id, MAX(domain) AS domain
    FROM {{DATABASE}}.{{SCHEMA}}.PAGES WHERE domain IS NOT NULL GROUP BY user_id
),
activity_events AS (
    SELECT t.user_id, ud.domain,
        DATE(CONVERT_TIMEZONE('UTC', 'Asia/Kolkata', t.TIMESTAMP)) AS event_date,
        DATE_TRUNC('MONTH', CONVERT_TIMEZONE('UTC', 'Asia/Kolkata', t.TIMESTAMP)) AS event_month
    FROM {{DATABASE}}.{{SCHEMA}}.TRACKS t
    INNER JOIN user_domains ud ON ud.user_id = t.user_id
    WHERE t.event_text NOT IN (
        'atlan_analaytics_aggregateinfo_fetch', 'api_error_emit', 'api_evaluator_cancelled', 'api_evaluator_succeeded',
        'Experiment Started', '$experiment_started', 'web_vital_metric_inp_track', 'web_vital_metric_ttfb_track',
        'performance_metric_user_timing_discovery_search', 'performance_metric_user_timing_app_bootstrap',
        'web_vital_metric_fcp_track', 'web_vital_metric_lcp_track')
    AND t.event_text NOT LIKE 'workflow_%' AND t.TIMESTAMP >= {{START_DATE}}
    UNION ALL
    SELECT p.user_id, p.domain,
        DATE(CONVERT_TIMEZONE('UTC', 'Asia/Kolkata', p.TIMESTAMP)) AS event_date,
        DATE_TRUNC('MONTH', CONVERT_TIMEZONE('UTC', 'Asia/Kolkata', p.TIMESTAMP)) AS event_month
    FROM {{DATABASE}}.{{SCHEMA}}.PAGES p WHERE p.TIMESTAMP >= {{START_DATE}}
),
daily_users AS (
    SELECT domain, event_month, event_date, COUNT(DISTINCT user_id) AS dau
    FROM activity_events WHERE domain = {{DOMAIN}} GROUP BY domain, event_month, event_date
),
monthly_users AS (
    SELECT domain, event_month, COUNT(DISTINCT user_id) AS mau
    FROM activity_events WHERE domain = {{DOMAIN}} GROUP BY domain, event_month
)
SELECT m.domain, m.event_month, m.mau, ROUND(AVG(d.dau), 1) AS avg_dau,
    ROUND(AVG(d.dau) / NULLIF(m.mau, 0), 3) AS stickiness_ratio,
    CASE WHEN AVG(d.dau) / NULLIF(m.mau, 0) >= 0.3 THEN 'Strong'
         WHEN AVG(d.dau) / NULLIF(m.mau, 0) >= 0.1 THEN 'Moderate'
         ELSE 'Episodic' END AS engagement_level
FROM monthly_users m
JOIN daily_users d ON d.domain = m.domain AND d.event_month = m.event_month
GROUP BY m.domain, m.event_month, m.mau ORDER BY m.domain, m.event_month DESC;
```

#### User Roster by Domain

Full user list with activity status, event counts, and last activity.

```sql
WITH user_domains AS (
    SELECT user_id, MAX(domain) AS domain
    FROM {{DATABASE}}.{{SCHEMA}}.PAGES WHERE domain IS NOT NULL GROUP BY user_id
),
activity_events AS (
    SELECT t.user_id, CONVERT_TIMEZONE('UTC', 'Asia/Kolkata', t.TIMESTAMP) AS event_ts
    FROM {{DATABASE}}.{{SCHEMA}}.TRACKS t
    INNER JOIN user_domains ud ON ud.user_id = t.user_id AND ud.domain = {{DOMAIN}}
    WHERE t.event_text NOT IN (
        'atlan_analaytics_aggregateinfo_fetch', 'api_error_emit', 'api_evaluator_cancelled', 'api_evaluator_succeeded',
        'Experiment Started', '$experiment_started', 'web_vital_metric_inp_track', 'web_vital_metric_ttfb_track',
        'performance_metric_user_timing_discovery_search', 'performance_metric_user_timing_app_bootstrap',
        'web_vital_metric_fcp_track', 'web_vital_metric_lcp_track')
    AND t.event_text NOT LIKE 'workflow_%' AND t.TIMESTAMP >= {{START_DATE}}
    UNION ALL
    SELECT p.user_id, CONVERT_TIMEZONE('UTC', 'Asia/Kolkata', p.TIMESTAMP) AS event_ts
    FROM {{DATABASE}}.{{SCHEMA}}.PAGES p
    WHERE p.domain = {{DOMAIN}} AND p.TIMESTAMP >= {{START_DATE}}
),
user_activity AS (
    SELECT user_id, COUNT(*) AS total_events, COUNT(DISTINCT DATE(event_ts)) AS active_days,
        MIN(event_ts) AS first_activity, MAX(event_ts) AS last_activity
    FROM activity_events GROUP BY user_id
),
user_meta AS (
    SELECT id, email, username, role, MAX(license_type) AS license_type,
        MAX(job_role) AS job_role, MIN(created_at) AS user_created_at
    FROM {{DATABASE}}.{{SCHEMA}}.USERS WHERE email IS NOT NULL GROUP BY id, email, username, role
)
SELECT a.user_id, um.email, um.username, um.role, um.license_type, um.job_role, um.user_created_at,
    a.total_events, a.active_days, a.first_activity, a.last_activity,
    DATEDIFF('day', a.last_activity, CURRENT_TIMESTAMP()) AS days_since_last_activity,
    CASE WHEN a.last_activity >= DATEADD('day', -30, CURRENT_TIMESTAMP()) THEN 'Active'
         WHEN a.last_activity >= DATEADD('day', -90, CURRENT_TIMESTAMP()) THEN 'Inactive'
         WHEN a.last_activity IS NULL THEN 'Never Active'
         ELSE 'Churned' END AS status
FROM user_activity a LEFT JOIN user_meta um ON um.id = a.user_id
ORDER BY a.total_events DESC;
```

---

### 2. Feature Adoption

#### Top Pages by Domain

```sql
SELECT p.domain, p.name AS page_name, p.tab, COUNT(*) AS page_views,
    COUNT(DISTINCT p.user_id) AS unique_users,
    ROUND(COUNT(*) / NULLIF(COUNT(DISTINCT p.user_id), 0), 1) AS views_per_user
FROM {{DATABASE}}.{{SCHEMA}}.PAGES p
WHERE p.TIMESTAMP >= {{START_DATE}} AND p.name IS NOT NULL AND p.domain = {{DOMAIN}}
GROUP BY p.domain, p.name, p.tab ORDER BY page_views DESC LIMIT 50;
```

#### Top Events by Domain

```sql
WITH user_domains AS (
    SELECT user_id, MAX(domain) AS domain
    FROM {{DATABASE}}.{{SCHEMA}}.PAGES WHERE domain IS NOT NULL GROUP BY user_id
)
SELECT ud.domain, t.event_text, COUNT(*) AS event_count,
    COUNT(DISTINCT t.user_id) AS unique_users,
    ROUND(COUNT(*) / NULLIF(COUNT(DISTINCT t.user_id), 0), 1) AS events_per_user
FROM {{DATABASE}}.{{SCHEMA}}.TRACKS t
INNER JOIN user_domains ud ON ud.user_id = t.user_id
WHERE t.TIMESTAMP >= {{START_DATE}} AND t.event_text IS NOT NULL
  AND t.event_text NOT IN (
      'atlan_analaytics_aggregateinfo_fetch', 'api_error_emit', 'api_evaluator_cancelled', 'api_evaluator_succeeded',
      'Experiment Started', '$experiment_started', 'web_vital_metric_inp_track', 'web_vital_metric_ttfb_track',
      'performance_metric_user_timing_discovery_search', 'performance_metric_user_timing_app_bootstrap',
      'web_vital_metric_fcp_track', 'web_vital_metric_lcp_track')
  AND t.event_text NOT LIKE 'workflow_%' AND ud.domain = {{DOMAIN}}
GROUP BY ud.domain, t.event_text ORDER BY event_count DESC LIMIT 50;
```

#### Feature Adoption Matrix

User x feature boolean matrix per month. Shows which features each user engaged with.

```sql
WITH user_domains AS (
    SELECT user_id, MAX(domain) AS domain
    FROM {{DATABASE}}.{{SCHEMA}}.PAGES WHERE domain IS NOT NULL GROUP BY user_id
),
page_features AS (
    SELECT p.user_id, p.domain,
        DATE_TRUNC('MONTH', CONVERT_TIMEZONE('UTC', 'Asia/Kolkata', p.TIMESTAMP)) AS activity_month,
        CASE WHEN p.name = 'discovery' THEN 'Discovery'
             WHEN p.name = 'reverse-metadata-sidebar' THEN 'Chrome Extension'
             WHEN p.name IN ('saved_query', 'insights') THEN 'Insights/SQL'
             WHEN p.name IN ('glossary', 'term', 'category', 'classifications', 'custom_metadata') THEN 'Governance'
             WHEN p.name IN ('asset_profile', 'overview') THEN 'Asset Profile'
             WHEN p.name IN ('users', 'personas', 'config', 'sso', 'api-access', 'api_keys', 'policyManager', 'manage') THEN 'Admin'
             WHEN p.name IN ('workflows-home', 'workflows-profile', 'runs', 'playbook') THEN 'Workflows'
             WHEN p.name = 'monitor' THEN 'Data Quality'
             WHEN p.name = 'marketplace' THEN 'Marketplace'
             ELSE 'Other' END AS feature_area
    FROM {{DATABASE}}.{{SCHEMA}}.PAGES p WHERE p.TIMESTAMP >= {{START_DATE}} AND p.name IS NOT NULL
),
event_features AS (
    SELECT t.user_id, ud.domain,
        DATE_TRUNC('MONTH', CONVERT_TIMEZONE('UTC', 'Asia/Kolkata', t.TIMESTAMP)) AS activity_month,
        CASE WHEN t.event_text LIKE 'discovery_%' THEN 'Discovery'
             WHEN t.event_text LIKE 'chrome_%' THEN 'Chrome Extension'
             WHEN t.event_text LIKE 'insights_%' THEN 'Insights/SQL'
             WHEN t.event_text LIKE 'governance_%' OR t.event_text LIKE 'gtc_tree_%' THEN 'Governance'
             WHEN t.event_text LIKE 'atlan_ai_%' THEN 'AI Copilot'
             WHEN t.event_text LIKE 'lineage_%' THEN 'Lineage'
             WHEN t.event_text LIKE 'products_home_%' THEN 'Data Products'
             ELSE NULL END AS feature_area
    FROM {{DATABASE}}.{{SCHEMA}}.TRACKS t
    INNER JOIN user_domains ud ON ud.user_id = t.user_id
    WHERE t.TIMESTAMP >= {{START_DATE}}
      AND t.event_text NOT IN (
          'atlan_analaytics_aggregateinfo_fetch', 'api_error_emit', 'api_evaluator_cancelled', 'api_evaluator_succeeded',
          'Experiment Started', '$experiment_started', 'web_vital_metric_inp_track', 'web_vital_metric_ttfb_track',
          'performance_metric_user_timing_discovery_search', 'performance_metric_user_timing_app_bootstrap',
          'web_vital_metric_fcp_track', 'web_vital_metric_lcp_track')
      AND t.event_text NOT LIKE 'workflow_%'
),
all_features AS (
    SELECT user_id, domain, activity_month, feature_area FROM page_features WHERE feature_area != 'Other'
    UNION
    SELECT user_id, domain, activity_month, feature_area FROM event_features WHERE feature_area IS NOT NULL
)
SELECT af.domain, af.activity_month, af.user_id, um.email, um.username, um.role,
    MAX(CASE WHEN af.feature_area = 'Discovery' THEN 1 ELSE 0 END) AS used_discovery,
    MAX(CASE WHEN af.feature_area = 'Chrome Extension' THEN 1 ELSE 0 END) AS used_chrome_ext,
    MAX(CASE WHEN af.feature_area = 'Insights/SQL' THEN 1 ELSE 0 END) AS used_insights,
    MAX(CASE WHEN af.feature_area = 'Governance' THEN 1 ELSE 0 END) AS used_governance,
    MAX(CASE WHEN af.feature_area = 'AI Copilot' THEN 1 ELSE 0 END) AS used_ai_copilot,
    MAX(CASE WHEN af.feature_area = 'Lineage' THEN 1 ELSE 0 END) AS used_lineage,
    MAX(CASE WHEN af.feature_area = 'Asset Profile' THEN 1 ELSE 0 END) AS used_asset_profile,
    MAX(CASE WHEN af.feature_area = 'Admin' THEN 1 ELSE 0 END) AS used_admin,
    MAX(CASE WHEN af.feature_area = 'Workflows' THEN 1 ELSE 0 END) AS used_workflows,
    MAX(CASE WHEN af.feature_area = 'Data Quality' THEN 1 ELSE 0 END) AS used_data_quality,
    MAX(CASE WHEN af.feature_area = 'Data Products' THEN 1 ELSE 0 END) AS used_data_products,
    MAX(CASE WHEN af.feature_area = 'Marketplace' THEN 1 ELSE 0 END) AS used_marketplace
FROM all_features af
LEFT JOIN (SELECT id, email, username, role FROM {{DATABASE}}.{{SCHEMA}}.USERS WHERE email IS NOT NULL GROUP BY id, email, username, role) um ON um.id = af.user_id
WHERE af.domain = {{DOMAIN}}
GROUP BY af.domain, af.activity_month, af.user_id, um.email, um.username, um.role
ORDER BY af.activity_month DESC, af.user_id;
```

#### Feature Trend Weekly

Week-over-week unique users per feature area.

```sql
WITH user_domains AS (
    SELECT user_id, MAX(domain) AS domain
    FROM {{DATABASE}}.{{SCHEMA}}.PAGES WHERE domain IS NOT NULL GROUP BY user_id
),
all_activity AS (
    SELECT user_id, domain,
        DATE_TRUNC('WEEK', CONVERT_TIMEZONE('UTC', 'Asia/Kolkata', TIMESTAMP)) AS event_week,
        CASE WHEN name = 'discovery' THEN 'Discovery'
             WHEN name = 'reverse-metadata-sidebar' THEN 'Chrome Extension'
             WHEN name IN ('saved_query', 'insights') THEN 'Insights/SQL'
             WHEN name IN ('glossary', 'term', 'category', 'classifications', 'custom_metadata') THEN 'Governance'
             WHEN name IN ('asset_profile', 'overview') THEN 'Asset Profile'
             WHEN name = 'monitor' THEN 'Data Quality'
             ELSE NULL END AS feature_area
    FROM {{DATABASE}}.{{SCHEMA}}.PAGES WHERE TIMESTAMP >= {{START_DATE}} AND name IS NOT NULL
    UNION ALL
    SELECT t.user_id, ud.domain,
        DATE_TRUNC('WEEK', CONVERT_TIMEZONE('UTC', 'Asia/Kolkata', t.TIMESTAMP)) AS event_week,
        CASE WHEN t.event_text LIKE 'discovery_%' THEN 'Discovery'
             WHEN t.event_text LIKE 'chrome_%' THEN 'Chrome Extension'
             WHEN t.event_text LIKE 'insights_%' THEN 'Insights/SQL'
             WHEN t.event_text LIKE 'governance_%' OR t.event_text LIKE 'gtc_tree_%' THEN 'Governance'
             WHEN t.event_text LIKE 'atlan_ai_%' THEN 'AI Copilot'
             WHEN t.event_text LIKE 'lineage_%' THEN 'Lineage'
             ELSE NULL END AS feature_area
    FROM {{DATABASE}}.{{SCHEMA}}.TRACKS t
    INNER JOIN user_domains ud ON ud.user_id = t.user_id
    WHERE t.TIMESTAMP >= {{START_DATE}}
      AND t.event_text NOT IN (
          'atlan_analaytics_aggregateinfo_fetch', 'api_error_emit', 'api_evaluator_cancelled', 'api_evaluator_succeeded',
          'Experiment Started', '$experiment_started', 'web_vital_metric_inp_track', 'web_vital_metric_ttfb_track',
          'performance_metric_user_timing_discovery_search', 'performance_metric_user_timing_app_bootstrap',
          'web_vital_metric_fcp_track', 'web_vital_metric_lcp_track')
      AND t.event_text NOT LIKE 'workflow_%'
)
SELECT event_week, feature_area, COUNT(DISTINCT user_id) AS unique_users, COUNT(*) AS total_events
FROM all_activity
WHERE feature_area IS NOT NULL AND domain = {{DOMAIN}}
GROUP BY event_week, feature_area ORDER BY event_week DESC, unique_users DESC;
```

#### Feature Engagement Quadrant

Plots each feature by reach (unique users) vs depth (avg events per user).

```sql
WITH user_domains AS (
    SELECT user_id, MAX(domain) AS domain
    FROM {{DATABASE}}.{{SCHEMA}}.PAGES WHERE domain IS NOT NULL GROUP BY user_id
),
page_events AS (
    SELECT p.user_id, CASE
            WHEN p.name = 'discovery' THEN 'Discovery/Search'
            WHEN p.name = 'reverse-metadata-sidebar' THEN 'Chrome Extension'
            WHEN p.name IN ('saved_query', 'insights') THEN 'Insights/SQL'
            WHEN p.name IN ('glossary', 'term', 'category', 'classifications', 'custom_metadata') THEN 'Governance'
            WHEN p.name IN ('asset_profile', 'overview') THEN 'Asset Profile'
            WHEN p.name = 'monitor' THEN 'Data Quality'
            WHEN p.name IN ('workflows-home', 'workflows-profile', 'runs', 'playbook') THEN 'Workflows'
            ELSE NULL END AS feature
    FROM {{DATABASE}}.{{SCHEMA}}.PAGES p
    WHERE p.domain = {{DOMAIN}} AND p.TIMESTAMP >= {{START_DATE}} AND p.name IS NOT NULL
),
track_events AS (
    SELECT t.user_id, CASE
            WHEN t.event_text LIKE 'discovery_search%' THEN 'Discovery/Search'
            WHEN t.event_text LIKE 'chrome_%' THEN 'Chrome Extension'
            WHEN t.event_text LIKE 'insights_%' THEN 'Insights/SQL'
            WHEN t.event_text LIKE 'governance_%' OR t.event_text LIKE 'gtc_tree_%' THEN 'Governance'
            WHEN t.event_text LIKE 'atlan_ai_%' THEN 'AI Copilot'
            WHEN t.event_text LIKE 'lineage_%' THEN 'Lineage'
            ELSE NULL END AS feature
    FROM {{DATABASE}}.{{SCHEMA}}.TRACKS t
    INNER JOIN user_domains ud ON ud.user_id = t.user_id AND ud.domain = {{DOMAIN}}
    WHERE t.TIMESTAMP >= {{START_DATE}}
      AND t.event_text NOT IN (
          'atlan_analaytics_aggregateinfo_fetch', 'api_error_emit', 'api_evaluator_cancelled', 'api_evaluator_succeeded',
          'Experiment Started', '$experiment_started', 'web_vital_metric_inp_track', 'web_vital_metric_ttfb_track',
          'performance_metric_user_timing_discovery_search', 'performance_metric_user_timing_app_bootstrap',
          'web_vital_metric_fcp_track', 'web_vital_metric_lcp_track')
      AND t.event_text NOT LIKE 'workflow_%'
),
combined AS (
    SELECT user_id, feature FROM page_events WHERE feature IS NOT NULL
    UNION ALL
    SELECT user_id, feature FROM track_events WHERE feature IS NOT NULL
),
per_user AS (
    SELECT feature, user_id, COUNT(*) AS events FROM combined GROUP BY feature, user_id
)
SELECT feature, COUNT(DISTINCT user_id) AS unique_users, SUM(events) AS total_events,
    ROUND(AVG(events), 1) AS avg_events_per_user, ROUND(MEDIAN(events), 1) AS median_events_per_user
FROM per_user GROUP BY feature ORDER BY unique_users DESC;
```

#### Connector Usage

Which data source connectors and asset types customers interact with.

```sql
SELECT p.domain, p.connector_name, p.type_name AS asset_type,
    COUNT(*) AS interactions, COUNT(DISTINCT p.user_id) AS unique_users,
    COUNT(DISTINCT p.asset_guid) AS unique_assets_viewed
FROM {{DATABASE}}.{{SCHEMA}}.PAGES p
WHERE p.TIMESTAMP >= {{START_DATE}} AND p.connector_name IS NOT NULL AND p.domain = {{DOMAIN}}
GROUP BY p.domain, p.connector_name, p.type_name ORDER BY interactions DESC;
```

---

### 3. Engagement Depth

#### Session Duration (Monthly)

```sql
WITH user_domains AS (
    SELECT user_id, MAX(domain) AS domain
    FROM {{DATABASE}}.{{SCHEMA}}.PAGES WHERE domain IS NOT NULL GROUP BY user_id
),
raw_events AS (
    SELECT user_id, TIMESTAMP,
        CONVERT_TIMEZONE('UTC', 'Asia/Kolkata', TIMESTAMP) AS event_ts,
        LAG(TIMESTAMP) OVER (PARTITION BY user_id ORDER BY TIMESTAMP) AS prev_ts
    FROM (
        SELECT p.user_id, p.TIMESTAMP FROM {{DATABASE}}.{{SCHEMA}}.PAGES p
        INNER JOIN user_domains ud ON ud.user_id = p.user_id
        WHERE p.TIMESTAMP >= {{START_DATE}} AND ud.domain = {{DOMAIN}}
        UNION ALL
        SELECT t.user_id, t.TIMESTAMP FROM {{DATABASE}}.{{SCHEMA}}.TRACKS t
        INNER JOIN user_domains ud ON ud.user_id = t.user_id
        WHERE t.TIMESTAMP >= {{START_DATE}} AND ud.domain = {{DOMAIN}}
          AND t.event_text NOT IN (
              'atlan_analaytics_aggregateinfo_fetch', 'api_error_emit', 'api_evaluator_cancelled', 'api_evaluator_succeeded',
              'Experiment Started', '$experiment_started', 'web_vital_metric_inp_track', 'web_vital_metric_ttfb_track',
              'performance_metric_user_timing_discovery_search', 'performance_metric_user_timing_app_bootstrap',
              'web_vital_metric_fcp_track', 'web_vital_metric_lcp_track')
          AND t.event_text NOT LIKE 'workflow_%'
    ) AS combined
),
session_boundaries AS (
    SELECT user_id, TIMESTAMP, event_ts,
        CASE WHEN prev_ts IS NULL THEN 1 WHEN DATEDIFF('second', prev_ts, TIMESTAMP) > 1800 THEN 1 ELSE 0 END AS is_new_session
    FROM raw_events
),
session_numbered AS (
    SELECT user_id, TIMESTAMP, event_ts,
        SUM(is_new_session) OVER (PARTITION BY user_id ORDER BY TIMESTAMP) AS session_id
    FROM session_boundaries
),
sessions AS (
    SELECT sn.user_id, ud.domain, sn.session_id,
        DATE_TRUNC('MONTH', MIN(sn.event_ts)) AS session_month,
        COUNT(*) AS events_in_session,
        DATEDIFF('minute', MIN(sn.TIMESTAMP), MAX(sn.TIMESTAMP)) AS duration_minutes
    FROM session_numbered sn
    INNER JOIN user_domains ud ON ud.user_id = sn.user_id
    GROUP BY sn.user_id, ud.domain, sn.session_id
    HAVING COUNT(*) > 1
)
SELECT domain, session_month, COUNT(*) AS total_sessions, COUNT(DISTINCT user_id) AS unique_users,
    ROUND(AVG(duration_minutes), 1) AS avg_session_minutes,
    ROUND(MEDIAN(duration_minutes), 1) AS median_session_minutes,
    ROUND(AVG(events_in_session), 1) AS avg_events_per_session,
    MAX(duration_minutes) AS max_session_minutes
FROM sessions WHERE duration_minutes > 0 AND duration_minutes < 480
GROUP BY domain, session_month ORDER BY session_month DESC;
```

#### Power Users

Top users by composite activity score (40% active days + 30% feature breadth + 30% log-scaled event volume).

```sql
WITH user_domains AS (
    SELECT user_id, MAX(domain) AS domain
    FROM {{DATABASE}}.{{SCHEMA}}.PAGES WHERE domain IS NOT NULL GROUP BY user_id
),
all_activity AS (
    SELECT p.user_id, p.TIMESTAMP, ud.domain,
        CASE WHEN p.name = 'discovery' THEN 'Discovery'
             WHEN p.name IN ('saved_query', 'insights') THEN 'Insights'
             WHEN p.name IN ('glossary', 'term', 'category') THEN 'Governance'
             WHEN p.name IN ('asset_profile', 'overview') THEN 'Asset Profile'
             WHEN p.name = 'reverse-metadata-sidebar' THEN 'Chrome Extension'
             WHEN p.name = 'monitor' THEN 'Data Quality'
             ELSE 'Other' END AS feature_area
    FROM {{DATABASE}}.{{SCHEMA}}.PAGES p INNER JOIN user_domains ud ON ud.user_id = p.user_id
    WHERE p.TIMESTAMP >= {{START_DATE}} AND p.name IS NOT NULL AND ud.domain = {{DOMAIN}}
    UNION ALL
    SELECT t.user_id, t.TIMESTAMP, ud.domain,
        CASE WHEN t.event_text LIKE 'discovery_%' THEN 'Discovery'
             WHEN t.event_text LIKE 'insights_%' THEN 'Insights'
             WHEN t.event_text LIKE 'governance_%' THEN 'Governance'
             WHEN t.event_text LIKE 'atlan_ai_%' THEN 'AI Copilot'
             WHEN t.event_text LIKE 'lineage_%' THEN 'Lineage'
             WHEN t.event_text LIKE 'chrome_%' THEN 'Chrome Extension'
             ELSE 'Other' END AS feature_area
    FROM {{DATABASE}}.{{SCHEMA}}.TRACKS t INNER JOIN user_domains ud ON ud.user_id = t.user_id
    WHERE t.TIMESTAMP >= {{START_DATE}} AND ud.domain = {{DOMAIN}}
      AND t.event_text NOT IN (
          'atlan_analaytics_aggregateinfo_fetch', 'api_error_emit', 'api_evaluator_cancelled', 'api_evaluator_succeeded',
          'Experiment Started', '$experiment_started', 'web_vital_metric_inp_track', 'web_vital_metric_ttfb_track',
          'performance_metric_user_timing_discovery_search', 'performance_metric_user_timing_app_bootstrap',
          'web_vital_metric_fcp_track', 'web_vital_metric_lcp_track')
      AND t.event_text NOT LIKE 'workflow_%'
),
user_scores AS (
    SELECT user_id, COUNT(*) AS total_events,
        COUNT(DISTINCT DATE(CONVERT_TIMEZONE('UTC', 'Asia/Kolkata', TIMESTAMP))) AS active_days,
        COUNT(DISTINCT CASE WHEN feature_area != 'Other' THEN feature_area END) AS feature_breadth
    FROM all_activity GROUP BY user_id
)
SELECT us.user_id, u.email, u.username, MAX(u.role) AS role, MAX(u.job_role) AS job_role,
    us.total_events, us.active_days, us.feature_breadth,
    ROUND(40.0 * us.active_days / NULLIF(MAX(us.active_days) OVER (), 0)
        + 30.0 * us.feature_breadth / NULLIF(MAX(us.feature_breadth) OVER (), 0)
        + 30.0 * LN(1 + us.total_events) / NULLIF(MAX(LN(1 + us.total_events)) OVER (), 0), 1) AS power_score
FROM user_scores us LEFT JOIN {{DATABASE}}.{{SCHEMA}}.USERS u ON u.id = us.user_id
GROUP BY us.user_id, u.email, u.username, us.total_events, us.active_days, us.feature_breadth
ORDER BY power_score DESC LIMIT 25;
```

#### Engagement Tiers

Classify users into Power/Heavy/Light/Dormant per month.

```sql
WITH user_domains AS (
    SELECT user_id, MAX(domain) AS domain
    FROM {{DATABASE}}.{{SCHEMA}}.PAGES WHERE domain IS NOT NULL GROUP BY user_id
),
domain_users AS (SELECT user_id FROM user_domains WHERE domain = {{DOMAIN}}),
activity_events AS (
    SELECT user_id, DATE_TRUNC('MONTH', CONVERT_TIMEZONE('UTC', 'Asia/Kolkata', TIMESTAMP)) AS event_month
    FROM (
        SELECT t.user_id, t.TIMESTAMP FROM {{DATABASE}}.{{SCHEMA}}.TRACKS t
        INNER JOIN user_domains ud ON ud.user_id = t.user_id
        WHERE ud.domain = {{DOMAIN}}
          AND t.event_text NOT IN (
              'atlan_analaytics_aggregateinfo_fetch', 'api_error_emit', 'api_evaluator_cancelled', 'api_evaluator_succeeded',
              'Experiment Started', '$experiment_started', 'web_vital_metric_inp_track', 'web_vital_metric_ttfb_track',
              'performance_metric_user_timing_discovery_search', 'performance_metric_user_timing_app_bootstrap',
              'web_vital_metric_fcp_track', 'web_vital_metric_lcp_track')
          AND t.event_text NOT LIKE 'workflow_%'
        UNION ALL
        SELECT p.user_id, p.TIMESTAMP FROM {{DATABASE}}.{{SCHEMA}}.PAGES p
        INNER JOIN user_domains ud ON ud.user_id = p.user_id WHERE ud.domain = {{DOMAIN}}
    ) WHERE TIMESTAMP >= {{START_DATE}}
),
months AS (SELECT DISTINCT event_month FROM activity_events),
user_months AS (SELECT du.user_id, m.event_month FROM domain_users du CROSS JOIN months m),
user_activity AS (
    SELECT user_id, event_month, COUNT(*) AS event_count
    FROM activity_events GROUP BY user_id, event_month
),
user_tiered AS (
    SELECT um.event_month, um.user_id, COALESCE(ua.event_count, 0) AS event_count,
        CASE WHEN COALESCE(ua.event_count, 0) = 0 THEN 'Dormant'
             WHEN PERCENT_RANK() OVER (PARTITION BY um.event_month ORDER BY COALESCE(ua.event_count, 0)) >= 0.9 THEN 'Power'
             WHEN COALESCE(ua.event_count, 0) >= MEDIAN(CASE WHEN ua.event_count > 0 THEN ua.event_count END)
                 OVER (PARTITION BY um.event_month) THEN 'Heavy'
             ELSE 'Light' END AS tier
    FROM user_months um LEFT JOIN user_activity ua ON ua.user_id = um.user_id AND ua.event_month = um.event_month
)
SELECT event_month, tier, COUNT(*) AS user_count,
    ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (PARTITION BY event_month), 1) AS pct_of_users
FROM user_tiered GROUP BY event_month, tier
ORDER BY event_month DESC, CASE tier WHEN 'Power' THEN 1 WHEN 'Heavy' THEN 2 WHEN 'Light' THEN 3 ELSE 4 END;
```

#### Average Pageviews per User Daily

```sql
WITH daily_pageviews AS (
    SELECT p.user_id,
        DATE(CONVERT_TIMEZONE('UTC', 'Asia/Kolkata', p.TIMESTAMP)) AS event_date,
        COUNT(*) AS pageview_count
    FROM {{DATABASE}}.{{SCHEMA}}.PAGES p
    WHERE p.TIMESTAMP >= {{START_DATE}} AND p.domain = {{DOMAIN}}
    GROUP BY p.user_id, event_date
)
SELECT event_date, COUNT(DISTINCT user_id) AS active_users, SUM(pageview_count) AS total_pageviews,
    ROUND(SUM(pageview_count)::FLOAT / NULLIF(COUNT(DISTINCT user_id), 0), 2) AS avg_pageviews_per_user
FROM daily_pageviews GROUP BY event_date ORDER BY event_date DESC;
```

---

### 4. Retention

#### Monthly Retention Cohort

Triangular retention matrix: % of users who first appeared in month X that returned in month X+1, X+2, etc.

```sql
WITH user_domains AS (
    SELECT user_id, MAX(domain) AS domain
    FROM {{DATABASE}}.{{SCHEMA}}.PAGES WHERE domain IS NOT NULL GROUP BY user_id
),
user_months AS (
    SELECT DISTINCT sub.user_id, ud.domain,
        DATE_TRUNC('MONTH', CONVERT_TIMEZONE('UTC', 'Asia/Kolkata', sub.TIMESTAMP)) AS activity_month
    FROM (
        SELECT t.user_id, t.TIMESTAMP FROM {{DATABASE}}.{{SCHEMA}}.TRACKS t
        INNER JOIN user_domains ud2 ON ud2.user_id = t.user_id
        WHERE t.event_text NOT IN (
            'atlan_analaytics_aggregateinfo_fetch', 'api_error_emit', 'api_evaluator_cancelled', 'api_evaluator_succeeded',
            'Experiment Started', '$experiment_started', 'web_vital_metric_inp_track', 'web_vital_metric_ttfb_track',
            'performance_metric_user_timing_discovery_search', 'performance_metric_user_timing_app_bootstrap',
            'web_vital_metric_fcp_track', 'web_vital_metric_lcp_track')
        AND t.event_text NOT LIKE 'workflow_%' AND ud2.domain = {{DOMAIN}}
        UNION ALL
        SELECT p.user_id, p.TIMESTAMP FROM {{DATABASE}}.{{SCHEMA}}.PAGES p WHERE p.domain = {{DOMAIN}}
    ) sub
    INNER JOIN user_domains ud ON ud.user_id = sub.user_id
    WHERE sub.TIMESTAMP >= {{START_DATE}}
),
cohorts AS (
    SELECT user_id, domain, MIN(activity_month) AS cohort_month
    FROM user_months GROUP BY user_id, domain
),
retention AS (
    SELECT c.cohort_month, DATEDIFF('month', c.cohort_month, um.activity_month) AS months_since_start,
        COUNT(DISTINCT um.user_id) AS active_users
    FROM cohorts c INNER JOIN user_months um ON um.user_id = c.user_id AND um.domain = c.domain
    GROUP BY c.cohort_month, months_since_start
),
cohort_sizes AS (
    SELECT cohort_month, COUNT(DISTINCT user_id) AS cohort_size FROM cohorts GROUP BY cohort_month
)
SELECT r.cohort_month, cs.cohort_size, r.months_since_start, r.active_users,
    ROUND(100.0 * r.active_users / cs.cohort_size, 1) AS retention_pct
FROM retention r JOIN cohort_sizes cs ON cs.cohort_month = r.cohort_month
ORDER BY r.cohort_month, r.months_since_start;
```

#### Activation Funnel

How quickly new users take their first action (% activated within 1d, 7d, 14d, 30d).

```sql
WITH user_domains AS (
    SELECT user_id, MAX(domain) AS domain
    FROM {{DATABASE}}.{{SCHEMA}}.PAGES WHERE domain IS NOT NULL GROUP BY user_id
),
users_deduped AS (
    SELECT id, MAX(role) AS role, MIN(created_at) AS user_created_at
    FROM {{DATABASE}}.{{SCHEMA}}.USERS GROUP BY id
),
first_activity AS (
    SELECT user_id, MIN(CONVERT_TIMEZONE('UTC', 'Asia/Kolkata', TIMESTAMP)) AS first_event_ts
    FROM (
        SELECT t.user_id, t.TIMESTAMP FROM {{DATABASE}}.{{SCHEMA}}.TRACKS t
        INNER JOIN user_domains ud ON ud.user_id = t.user_id
        WHERE t.event_text NOT IN (
            'atlan_analaytics_aggregateinfo_fetch', 'api_error_emit', 'api_evaluator_cancelled', 'api_evaluator_succeeded',
            'Experiment Started', '$experiment_started', 'web_vital_metric_inp_track', 'web_vital_metric_ttfb_track',
            'performance_metric_user_timing_discovery_search', 'performance_metric_user_timing_app_bootstrap',
            'web_vital_metric_fcp_track', 'web_vital_metric_lcp_track')
        AND t.event_text NOT LIKE 'workflow_%' AND ud.domain = {{DOMAIN}}
        UNION ALL
        SELECT p.user_id, p.TIMESTAMP FROM {{DATABASE}}.{{SCHEMA}}.PAGES p WHERE p.domain = {{DOMAIN}}
    ) GROUP BY user_id
),
new_users AS (
    SELECT ud.user_id, u.role, u.user_created_at,
        DATE_TRUNC('MONTH', u.user_created_at) AS creation_month,
        fa.first_event_ts,
        DATEDIFF('day', u.user_created_at, fa.first_event_ts) AS days_to_first_action
    FROM user_domains ud
    INNER JOIN users_deduped u ON u.id = ud.user_id
    LEFT JOIN first_activity fa ON fa.user_id = ud.user_id
    WHERE ud.domain = {{DOMAIN}} AND u.user_created_at >= {{START_DATE}}
)
SELECT creation_month, COUNT(*) AS total_new_users,
    COUNT(CASE WHEN days_to_first_action <= 1 THEN 1 END) AS activated_1d,
    COUNT(CASE WHEN days_to_first_action <= 7 THEN 1 END) AS activated_7d,
    COUNT(CASE WHEN days_to_first_action <= 14 THEN 1 END) AS activated_14d,
    COUNT(CASE WHEN days_to_first_action <= 30 THEN 1 END) AS activated_30d,
    COUNT(CASE WHEN days_to_first_action IS NULL THEN 1 END) AS never_activated,
    ROUND(100.0 * COUNT(CASE WHEN days_to_first_action <= 1 THEN 1 END) / COUNT(*), 1) AS pct_1d,
    ROUND(100.0 * COUNT(CASE WHEN days_to_first_action <= 7 THEN 1 END) / COUNT(*), 1) AS pct_7d,
    ROUND(100.0 * COUNT(CASE WHEN days_to_first_action <= 30 THEN 1 END) / COUNT(*), 1) AS pct_30d
FROM new_users GROUP BY creation_month ORDER BY creation_month DESC;
```

#### Churned Users

Users active last month but not this month.

```sql
WITH user_domains AS (
    SELECT user_id, MAX(domain) AS domain
    FROM {{DATABASE}}.{{SCHEMA}}.PAGES WHERE domain IS NOT NULL GROUP BY user_id
),
user_months AS (
    SELECT DISTINCT sub.user_id,
        DATE_TRUNC('MONTH', CONVERT_TIMEZONE('UTC', 'Asia/Kolkata', sub.TIMESTAMP)) AS activity_month
    FROM (
        SELECT t.user_id, t.TIMESTAMP FROM {{DATABASE}}.{{SCHEMA}}.TRACKS t
        INNER JOIN user_domains ud ON ud.user_id = t.user_id
        WHERE t.event_text NOT IN (
            'atlan_analaytics_aggregateinfo_fetch', 'api_error_emit', 'api_evaluator_cancelled', 'api_evaluator_succeeded',
            'Experiment Started', '$experiment_started', 'web_vital_metric_inp_track', 'web_vital_metric_ttfb_track',
            'performance_metric_user_timing_discovery_search', 'performance_metric_user_timing_app_bootstrap',
            'web_vital_metric_fcp_track', 'web_vital_metric_lcp_track')
        AND t.event_text NOT LIKE 'workflow_%' AND ud.domain = {{DOMAIN}}
        UNION ALL
        SELECT p.user_id, p.TIMESTAMP FROM {{DATABASE}}.{{SCHEMA}}.PAGES p WHERE p.domain = {{DOMAIN}}
    ) sub WHERE sub.TIMESTAMP >= DATEADD('month', -3, CURRENT_TIMESTAMP())
),
prev_month_users AS (
    SELECT user_id FROM user_months
    WHERE activity_month = DATE_TRUNC('MONTH', DATEADD('month', -1, CURRENT_TIMESTAMP()))
),
curr_month_users AS (
    SELECT DISTINCT user_id FROM user_months
    WHERE activity_month = DATE_TRUNC('MONTH', CURRENT_TIMESTAMP())
)
SELECT p.user_id, u.email, u.username, u.role, u.job_role,
    DATE_TRUNC('MONTH', DATEADD('month', -1, CURRENT_TIMESTAMP())) AS last_active_month
FROM prev_month_users p
LEFT JOIN curr_month_users c ON c.user_id = p.user_id
LEFT JOIN (SELECT id, MAX(email) AS email, MAX(username) AS username, MAX(role) AS role, MAX(job_role) AS job_role
           FROM {{DATABASE}}.{{SCHEMA}}.USERS GROUP BY id) u ON u.id = p.user_id
WHERE c.user_id IS NULL ORDER BY p.user_id;
```

#### Reactivated Users

Users inactive for 30+ days who returned.

```sql
WITH user_domains AS (
    SELECT user_id, MAX(domain) AS domain
    FROM {{DATABASE}}.{{SCHEMA}}.PAGES WHERE domain IS NOT NULL GROUP BY user_id
),
user_activity_days AS (
    SELECT DISTINCT sub.user_id,
        DATE(CONVERT_TIMEZONE('UTC', 'Asia/Kolkata', sub.TIMESTAMP)) AS activity_date
    FROM (
        SELECT t.user_id, t.TIMESTAMP FROM {{DATABASE}}.{{SCHEMA}}.TRACKS t
        INNER JOIN user_domains ud ON ud.user_id = t.user_id
        WHERE t.event_text NOT IN (
            'atlan_analaytics_aggregateinfo_fetch', 'api_error_emit', 'api_evaluator_cancelled', 'api_evaluator_succeeded',
            'Experiment Started', '$experiment_started', 'web_vital_metric_inp_track', 'web_vital_metric_ttfb_track',
            'performance_metric_user_timing_discovery_search', 'performance_metric_user_timing_app_bootstrap',
            'web_vital_metric_fcp_track', 'web_vital_metric_lcp_track')
        AND t.event_text NOT LIKE 'workflow_%' AND ud.domain = {{DOMAIN}}
        UNION ALL
        SELECT p.user_id, p.TIMESTAMP FROM {{DATABASE}}.{{SCHEMA}}.PAGES p WHERE p.domain = {{DOMAIN}}
    ) sub WHERE sub.TIMESTAMP >= {{START_DATE}}
),
with_gaps AS (
    SELECT user_id, activity_date,
        LAG(activity_date) OVER (PARTITION BY user_id ORDER BY activity_date) AS prev_activity_date,
        DATEDIFF('day', LAG(activity_date) OVER (PARTITION BY user_id ORDER BY activity_date), activity_date) AS gap_days
    FROM user_activity_days
)
SELECT g.user_id, u.email, u.username, u.role,
    g.prev_activity_date AS last_active_before_gap, g.activity_date AS reactivation_date, g.gap_days
FROM with_gaps g
LEFT JOIN (SELECT id, MAX(email) AS email, MAX(username) AS username, MAX(role) AS role
           FROM {{DATABASE}}.{{SCHEMA}}.USERS GROUP BY id) u ON u.id = g.user_id
WHERE g.gap_days >= 30 ORDER BY g.activity_date DESC, g.gap_days DESC;
```

#### Aggregate Retention Rate (Weekly)

Of users with any activity, what % had a pageview within 7 days?

```sql
WITH user_domains AS (
    SELECT user_id, MAX(domain) AS domain
    FROM {{DATABASE}}.{{SCHEMA}}.PAGES WHERE domain IS NOT NULL GROUP BY user_id
),
first_activity AS (
    SELECT sub.user_id,
        MIN(DATE(CONVERT_TIMEZONE('UTC', 'Asia/Kolkata', sub.TIMESTAMP))) AS first_activity_date
    FROM (
        SELECT t.user_id, t.TIMESTAMP FROM {{DATABASE}}.{{SCHEMA}}.TRACKS t
        INNER JOIN user_domains ud ON ud.user_id = t.user_id
        WHERE t.TIMESTAMP >= {{START_DATE}}
          AND t.event_text NOT IN (
              'atlan_analaytics_aggregateinfo_fetch', 'api_error_emit', 'api_evaluator_cancelled', 'api_evaluator_succeeded',
              'Experiment Started', '$experiment_started', 'web_vital_metric_inp_track', 'web_vital_metric_ttfb_track',
              'performance_metric_user_timing_discovery_search', 'performance_metric_user_timing_app_bootstrap',
              'web_vital_metric_fcp_track', 'web_vital_metric_lcp_track')
          AND t.event_text NOT LIKE 'workflow_%' AND ud.domain = {{DOMAIN}}
        UNION ALL
        SELECT p.user_id, p.TIMESTAMP FROM {{DATABASE}}.{{SCHEMA}}.PAGES p
        WHERE p.TIMESTAMP >= {{START_DATE}} AND p.domain = {{DOMAIN}}
    ) sub GROUP BY sub.user_id
),
pageview_within_7d AS (
    SELECT fa.user_id, fa.first_activity_date,
        MIN(DATE(CONVERT_TIMEZONE('UTC', 'Asia/Kolkata', p.TIMESTAMP))) AS first_pv_date
    FROM first_activity fa
    INNER JOIN {{DATABASE}}.{{SCHEMA}}.PAGES p ON p.user_id = fa.user_id
        AND DATE(CONVERT_TIMEZONE('UTC', 'Asia/Kolkata', p.TIMESTAMP))
            BETWEEN fa.first_activity_date AND DATEADD('day', 7, fa.first_activity_date)
    WHERE p.TIMESTAMP >= {{START_DATE}} AND p.domain = {{DOMAIN}}
    GROUP BY fa.user_id, fa.first_activity_date
)
SELECT DATE_TRUNC('WEEK', fa.first_activity_date) AS cohort_week,
    COUNT(DISTINCT fa.user_id) AS users_with_activity,
    COUNT(DISTINCT pv.user_id) AS users_with_pageview_7d,
    ROUND(100.0 * COUNT(DISTINCT pv.user_id) / NULLIF(COUNT(DISTINCT fa.user_id), 0), 1) AS retention_rate_pct
FROM first_activity fa LEFT JOIN pageview_within_7d pv ON pv.user_id = fa.user_id
WHERE fa.first_activity_date <= DATEADD('day', -7, CURRENT_DATE())
GROUP BY cohort_week ORDER BY cohort_week DESC;
```

---

### 5. Customer Health

#### Customer Health Scorecard

Composite 0-100 health score per domain. Combines: license utilization (25%), MAU trend (25%), feature breadth (25%), retention (25%).

> Score >= 70 = Healthy, 40-69 = At Risk, <40 = Critical.

```sql
WITH user_domains AS (
    SELECT user_id, MAX(domain) AS domain
    FROM {{DATABASE}}.{{SCHEMA}}.PAGES WHERE domain IS NOT NULL GROUP BY user_id
),
activity_events AS (
    SELECT sub.user_id, ud.domain,
        DATE(CONVERT_TIMEZONE('UTC', 'Asia/Kolkata', sub.TIMESTAMP)) AS event_date,
        DATE_TRUNC('MONTH', CONVERT_TIMEZONE('UTC', 'Asia/Kolkata', sub.TIMESTAMP)) AS event_month
    FROM (
        SELECT t.user_id, t.TIMESTAMP FROM {{DATABASE}}.{{SCHEMA}}.TRACKS t
        INNER JOIN user_domains ud2 ON ud2.user_id = t.user_id
        WHERE t.event_text NOT IN (
            'atlan_analaytics_aggregateinfo_fetch', 'api_error_emit', 'api_evaluator_cancelled', 'api_evaluator_succeeded',
            'Experiment Started', '$experiment_started', 'web_vital_metric_inp_track', 'web_vital_metric_ttfb_track',
            'performance_metric_user_timing_discovery_search', 'performance_metric_user_timing_app_bootstrap',
            'web_vital_metric_fcp_track', 'web_vital_metric_lcp_track')
        AND t.event_text NOT LIKE 'workflow_%'
        UNION ALL
        SELECT p.user_id, p.TIMESTAMP FROM {{DATABASE}}.{{SCHEMA}}.PAGES p
    ) sub INNER JOIN user_domains ud ON ud.user_id = sub.user_id
    WHERE sub.TIMESTAMP >= {{START_DATE}}
),
total_users AS (
    SELECT domain, COUNT(DISTINCT user_id) AS total_user_count FROM user_domains GROUP BY domain
),
mau_monthly AS (
    SELECT domain, event_month, COUNT(DISTINCT user_id) AS mau
    FROM activity_events
    WHERE event_month >= DATEADD('month', -3, DATE_TRUNC('MONTH', CURRENT_TIMESTAMP()))
    GROUP BY domain, event_month
),
mau_summary AS (
    SELECT domain,
        MAX(CASE WHEN event_month = DATE_TRUNC('MONTH', CURRENT_TIMESTAMP()) THEN mau END) AS current_mau,
        MAX(CASE WHEN event_month = DATEADD('month', -1, DATE_TRUNC('MONTH', CURRENT_TIMESTAMP())) THEN mau END) AS prev_mau
    FROM mau_monthly GROUP BY domain
),
feature_breadth AS (
    SELECT ud.domain, COUNT(DISTINCT
        CASE WHEN p.name = 'discovery' THEN 'Discovery'
             WHEN p.name IN ('saved_query', 'insights') THEN 'Insights'
             WHEN p.name IN ('glossary', 'term', 'category') THEN 'Governance'
             WHEN p.name IN ('asset_profile', 'overview') THEN 'Asset Profile'
             WHEN p.name = 'reverse-metadata-sidebar' THEN 'Chrome Extension'
             WHEN p.name = 'monitor' THEN 'Data Quality' ELSE NULL END) AS features_used
    FROM {{DATABASE}}.{{SCHEMA}}.PAGES p INNER JOIN user_domains ud ON ud.user_id = p.user_id
    WHERE p.TIMESTAMP >= DATE_TRUNC('MONTH', CURRENT_TIMESTAMP()) AND p.name IS NOT NULL
    GROUP BY ud.domain
),
retention AS (
    SELECT curr.domain,
        COUNT(DISTINCT CASE WHEN prev.user_id IS NOT NULL THEN curr.user_id END) AS retained,
        COUNT(DISTINCT curr.user_id) AS prev_active
    FROM (SELECT DISTINCT user_id, domain FROM activity_events
          WHERE event_month = DATEADD('month', -1, DATE_TRUNC('MONTH', CURRENT_TIMESTAMP()))) curr
    LEFT JOIN (SELECT DISTINCT user_id FROM activity_events
               WHERE event_month = DATE_TRUNC('MONTH', CURRENT_TIMESTAMP())) prev ON prev.user_id = curr.user_id
    GROUP BY curr.domain
)
SELECT ms.domain, tu.total_user_count,
    COALESCE(ms.current_mau, 0) AS current_mau, COALESCE(ms.prev_mau, 0) AS prev_mau,
    ROUND(100.0 * COALESCE(ms.current_mau, 0) / NULLIF(tu.total_user_count, 0), 1) AS license_util_pct,
    COALESCE(fb.features_used, 0) AS features_used,
    ROUND(100.0 * COALESCE(r.retained, 0) / NULLIF(r.prev_active, 0), 1) AS retention_pct,
    ROUND(
        25.0 * LEAST(1.0, COALESCE(ms.current_mau, 0) / NULLIF(tu.total_user_count, 0))
        + 25.0 * CASE WHEN COALESCE(ms.prev_mau, 0) = 0 THEN 0.5
                      WHEN ms.current_mau >= ms.prev_mau THEN 1.0
                      WHEN ms.current_mau >= ms.prev_mau * 0.8 THEN 0.5
                      ELSE 0.0 END
        + 25.0 * COALESCE(fb.features_used, 0) / 6.0
        + 25.0 * COALESCE(r.retained, 0) / NULLIF(r.prev_active, 1)
    , 0) AS health_score,
    CASE WHEN ROUND(25.0 * LEAST(1.0, COALESCE(ms.current_mau, 0) / NULLIF(tu.total_user_count, 0))
        + 25.0 * CASE WHEN COALESCE(ms.prev_mau, 0) = 0 THEN 0.5 WHEN ms.current_mau >= ms.prev_mau THEN 1.0
                      WHEN ms.current_mau >= ms.prev_mau * 0.8 THEN 0.5 ELSE 0.0 END
        + 25.0 * COALESCE(fb.features_used, 0) / 6.0
        + 25.0 * COALESCE(r.retained, 0) / NULLIF(r.prev_active, 1), 0) >= 70 THEN 'Healthy'
         WHEN ROUND(25.0 * LEAST(1.0, COALESCE(ms.current_mau, 0) / NULLIF(tu.total_user_count, 0))
        + 25.0 * CASE WHEN COALESCE(ms.prev_mau, 0) = 0 THEN 0.5 WHEN ms.current_mau >= ms.prev_mau THEN 1.0
                      WHEN ms.current_mau >= ms.prev_mau * 0.8 THEN 0.5 ELSE 0.0 END
        + 25.0 * COALESCE(fb.features_used, 0) / 6.0
        + 25.0 * COALESCE(r.retained, 0) / NULLIF(r.prev_active, 1), 0) >= 40 THEN 'At Risk'
         ELSE 'Critical' END AS health_status
FROM mau_summary ms
JOIN total_users tu ON tu.domain = ms.domain
LEFT JOIN feature_breadth fb ON fb.domain = ms.domain
LEFT JOIN retention r ON r.domain = ms.domain
ORDER BY health_score DESC;
```

#### License Utilization

Active vs total users by role and license type.

```sql
WITH user_domains AS (
    SELECT user_id, MAX(domain) AS domain
    FROM {{DATABASE}}.{{SCHEMA}}.PAGES WHERE domain IS NOT NULL GROUP BY user_id
),
active_users AS (
    SELECT DISTINCT sub.user_id FROM (
        SELECT t.user_id, t.TIMESTAMP FROM {{DATABASE}}.{{SCHEMA}}.TRACKS t
        INNER JOIN user_domains ud ON ud.user_id = t.user_id
        WHERE t.event_text NOT IN (
            'atlan_analaytics_aggregateinfo_fetch', 'api_error_emit', 'api_evaluator_cancelled', 'api_evaluator_succeeded',
            'Experiment Started', '$experiment_started', 'web_vital_metric_inp_track', 'web_vital_metric_ttfb_track',
            'performance_metric_user_timing_discovery_search', 'performance_metric_user_timing_app_bootstrap',
            'web_vital_metric_fcp_track', 'web_vital_metric_lcp_track')
        AND t.event_text NOT LIKE 'workflow_%' AND ud.domain = {{DOMAIN}}
        UNION ALL
        SELECT p.user_id, p.TIMESTAMP FROM {{DATABASE}}.{{SCHEMA}}.PAGES p WHERE p.domain = {{DOMAIN}}
    ) sub WHERE sub.TIMESTAMP >= DATEADD('month', -1, CURRENT_TIMESTAMP())
)
SELECT ud.domain, u.role, u.license_type,
    COUNT(DISTINCT ud.user_id) AS total_users,
    COUNT(DISTINCT CASE WHEN a.user_id IS NOT NULL THEN ud.user_id END) AS active_users,
    COUNT(DISTINCT CASE WHEN a.user_id IS NULL THEN ud.user_id END) AS inactive_users,
    ROUND(100.0 * COUNT(DISTINCT CASE WHEN a.user_id IS NOT NULL THEN ud.user_id END)
        / NULLIF(COUNT(DISTINCT ud.user_id), 0), 1) AS utilization_pct
FROM user_domains ud
LEFT JOIN (SELECT id, MAX(role) AS role, MAX(license_type) AS license_type
           FROM {{DATABASE}}.{{SCHEMA}}.USERS GROUP BY id) u ON u.id = ud.user_id
LEFT JOIN active_users a ON a.user_id = ud.user_id
WHERE ud.domain = {{DOMAIN}}
GROUP BY ud.domain, u.role, u.license_type ORDER BY total_users DESC;
```

#### Role Distribution

```sql
WITH user_domains AS (
    SELECT user_id, MAX(domain) AS domain
    FROM {{DATABASE}}.{{SCHEMA}}.PAGES WHERE domain IS NOT NULL GROUP BY user_id
),
active_last_30d AS (
    SELECT DISTINCT sub.user_id FROM (
        SELECT t.user_id, t.TIMESTAMP FROM {{DATABASE}}.{{SCHEMA}}.TRACKS t
        INNER JOIN user_domains ud ON ud.user_id = t.user_id
        WHERE t.event_text NOT IN (
            'atlan_analaytics_aggregateinfo_fetch', 'api_error_emit', 'api_evaluator_cancelled', 'api_evaluator_succeeded',
            'Experiment Started', '$experiment_started', 'web_vital_metric_inp_track', 'web_vital_metric_ttfb_track',
            'performance_metric_user_timing_discovery_search', 'performance_metric_user_timing_app_bootstrap',
            'web_vital_metric_fcp_track', 'web_vital_metric_lcp_track')
        AND t.event_text NOT LIKE 'workflow_%' AND ud.domain = {{DOMAIN}}
        UNION ALL
        SELECT p.user_id, p.TIMESTAMP FROM {{DATABASE}}.{{SCHEMA}}.PAGES p WHERE p.domain = {{DOMAIN}}
    ) sub WHERE sub.TIMESTAMP >= DATEADD('day', -30, CURRENT_TIMESTAMP())
)
SELECT ud.domain, u.role, u.job_role,
    COUNT(DISTINCT ud.user_id) AS total_users,
    COUNT(DISTINCT CASE WHEN a.user_id IS NOT NULL THEN ud.user_id END) AS active_users,
    ROUND(100.0 * COUNT(DISTINCT CASE WHEN a.user_id IS NOT NULL THEN ud.user_id END)
        / NULLIF(COUNT(DISTINCT ud.user_id), 0), 1) AS active_pct
FROM user_domains ud
LEFT JOIN (SELECT id, MAX(role) AS role, MAX(job_role) AS job_role
           FROM {{DATABASE}}.{{SCHEMA}}.USERS GROUP BY id) u ON u.id = ud.user_id
LEFT JOIN active_last_30d a ON a.user_id = ud.user_id
WHERE ud.domain = {{DOMAIN}}
GROUP BY ud.domain, u.role, u.job_role ORDER BY total_users DESC;
```

---

## Observability Templates

These query the `OBSERVABILITY` namespace. The single table `JOB_METRICS` stores one row per job execution. The `custom_metrics` column is a JSON string whose schema varies by `job_name`.

> **Partition pruning:** `JOB_METRICS` is partitioned by month on `started_at`. Always include a time-range filter on `started_at` to avoid full table scans.

### DQ Score Over Time

Trends the `dq_score` from `AtlasDqOrchestrationWorkflow` runs by day. Use to monitor whether Lakehouse data quality is stable, improving, or degrading.

```sql
SELECT
    DATE(started_at)                            AS run_date,
    job_instance_id,
    PARSE_JSON(custom_metrics):dq_score::FLOAT  AS dq_score,
    PARSE_JSON(custom_metrics):total_typedefs::INT
                                                AS total_typedefs,
    PARSE_JSON(custom_metrics):total_mismatch_count::INT
                                                AS total_mismatches,
    DATEDIFF('second', started_at, completed_at)
                                                AS duration_seconds
FROM {{DATABASE}}.OBSERVABILITY.JOB_METRICS
WHERE job_name = 'AtlasDqOrchestrationWorkflow'
  AND started_at >= DATEADD('day', -30, CURRENT_TIMESTAMP())
ORDER BY run_date DESC;
```

### Job Success and Failure Rates

Counts job executions by status for each job type over a configurable window. Identifies jobs that are failing frequently.

```sql
SELECT
    job_name,
    COUNT(*)                                                AS total_runs,
    COUNT(CASE WHEN status_code = 200 THEN 1 END)          AS successes,
    COUNT(CASE WHEN status_code != 200 THEN 1 END)         AS failures,
    ROUND(
        100.0 * COUNT(CASE WHEN status_code = 200 THEN 1 END)
        / NULLIF(COUNT(*), 0), 2
    )                                                       AS success_rate_pct,
    MAX(CASE WHEN status_code != 200
             THEN error_message END)                        AS latest_error
FROM {{DATABASE}}.OBSERVABILITY.JOB_METRICS
WHERE started_at >= DATEADD('day', -7, CURRENT_TIMESTAMP())
GROUP BY job_name
ORDER BY failures DESC, total_runs DESC;
```

### Job Duration Analysis

Calculates duration statistics per job type and flags long-running executions. Use to detect performance regressions.

```sql
WITH job_durations AS (
    SELECT
        job_name,
        job_instance_id,
        DATE(started_at)                                AS run_date,
        DATEDIFF('second', started_at, completed_at)    AS duration_seconds
    FROM {{DATABASE}}.OBSERVABILITY.JOB_METRICS
    WHERE started_at >= DATEADD('day', -30, CURRENT_TIMESTAMP())
      AND completed_at IS NOT NULL
)
SELECT
    job_name,
    COUNT(*)                                            AS total_runs,
    ROUND(AVG(duration_seconds), 1)                     AS avg_duration_sec,
    ROUND(MEDIAN(duration_seconds), 1)                  AS p50_duration_sec,
    ROUND(PERCENTILE_CONT(0.95)
          WITHIN GROUP (ORDER BY duration_seconds), 1)  AS p95_duration_sec,
    MAX(duration_seconds)                               AS max_duration_sec
FROM job_durations
GROUP BY job_name
ORDER BY avg_duration_sec DESC;
```

---

## Troubleshooting

| Issue            | Solution                                                                          |
| ---------------- | --------------------------------------------------------------------------------- |
| OAuth Failed     | Check Client ID/Secret and role name; re-retrieve credentials from Marketplace    |
| Table Not Found  | Try `atlan-ns` instead of `entity_metadata` (tenants onboarded before Feb 2026)   |
| Empty Results    | Verify the entity type has data; check `{{DOMAIN}}` filter matches your tenant    |
| Table Has 0 Rows | This is expected for **supertype tables** (`ASSET`, `SQL`, `BI`, `SAAS`, `CLOUD`, etc.) — these are abstract parents in Atlan's type hierarchy and contain no data. Query the concrete type table instead (e.g., `TABLE`, `COLUMN`, `VIEW`). Also expected for unused connectors (e.g., `AIRFLOWDAG` if the tenant doesn't use Airflow). |
| Wrong Database   | Ask the user which database/catalog contains their Atlan Lakehouse                |
| Wrong Catalog    | Ask the user for the catalog name from their Lakehouse connection details          |
| Column Not Found (PyIceberg) | Polaris returns lowercase column names — use `timestamp` not `TIMESTAMP` |

> **Storage credentials:** PyIceberg works with any Iceberg REST Catalog (IRC) regardless of the underlying storage (S3, Azure Storage, GCS). The Polaris catalog vends temporary storage credentials automatically — you do not need to configure cloud storage credentials separately.

### S3 Access Issues (PyIceberg only)

If you encounter S3 access errors when scanning tables, add AWS credentials to the catalog configuration:

```python
catalog = load_catalog(
    "mdlh",
    type="rest",
    uri=f"https://{creds['tenant']}.atlan.com/api/polaris/api/catalog",
    credential=f"{creds['client_id']}:{creds['client_secret']}",
    warehouse=creds["catalog_name"],
    scope=f"PRINCIPAL_ROLE:{creds['role']}",
    **{
        "s3.access-key-id": os.environ["AWS_ACCESS_KEY_ID"],
        "s3.secret-access-key": os.environ["AWS_SECRET_ACCESS_KEY"],
        "s3.region": os.environ["AWS_REGION"],
    }
)
```
