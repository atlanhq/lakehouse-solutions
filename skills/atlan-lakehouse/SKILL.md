---
name: atlan-lakehouse
description: >
  Use this skill when the user wants to query Atlan catalog metadata or usage analytics at scale,
  build reports on assets/glossaries/tags/user adoption, or analyze metadata from the lakehouse.
  Works across platforms: Snowflake (Cortex Code), Databricks (Genie Code), and Python (PyIceberg).
  Includes the GOLD namespace (curated, pre-joined star-schema tables for simplified asset metadata queries),
  plus SQL template library for metadata completeness, metadata export, glossary export,
  lineage analysis (impact analysis, root cause analysis, coverage, hub identification, dashboard impact, tag propagation),
  and comprehensive usage analytics (active users, feature adoption, engagement, retention, health scoring).
license: Apache-2.0
compatibility: SQL (Snowflake, Databricks), Python 3.9+ with PyIceberg
metadata:
  author: atlan-platform-team
  version: "1.1.0"
  category: data-integration
  keywords: atlan-lakehouse, iceberg, polaris, pyiceberg, analytics, usage-analytics, snowflake, cortex, databricks, genie, lineage, lineage-impact, lineage-root-cause, lineage-export, lineage-tags, lineage-hubs, downstream-dashboards, metadata-export, metadata-completeness, glossary, gold-namespace, data-governance
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
- User needs to export asset metadata for AI applications, data marketplaces, or reverse sync workflows
- User needs to export and analyze glossary terms with categories and assigned entities
- User wants to track historical metadata changes or generate audit trails
- **Lineage use cases** — any of:
  - Find orphaned/unused assets with no lineage (cleanup candidates)
  - Detect circular dependencies in the data pipeline
  - Calculate lineage coverage percentages (overall or by connector)
  - Identify the most connected "hub" assets with the highest blast radius
  - Export the full lineage graph for impact analysis or post-deployment validation
  - Perform impact analysis before modifying a table — find all downstream dependencies
  - Perform root cause analysis — trace data quality issues back upstream
  - Find all BI dashboards (Tableau, Power BI, Looker, etc.) downstream of a source table
  - Measure tag/governance label propagation across lineage hops
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

2. **Asset metadata + tags, custom metadata, readmes, lineage, or metadata export?**
   → **Hybrid approach:** Start from `GOLD.ASSETS` for core asset data, then join to `ENTITY_METADATA` tables for what GOLD doesn't have:
   - `ENTITY_METADATA.TAGS` — classification tags on assets
   - `ENTITY_METADATA.CUSTOM_METADATA` — custom metadata attributes
   - `ENTITY_METADATA.README` — readme content
   - `ENTITY_METADATA.PROCESS` / `COLUMN_PROCESS` / `BI_PROCESS` — lineage relationships
   - Use `ENTITY_METADATA` directly if GOLD adds no value for the specific query.

3. **Lineage analysis** (impact, root cause, coverage, hub assets, tag propagation, downstream dashboards)?
   → **Hybrid approach:** Use `GOLD.ASSETS` joined to the customer-managed `LINEAGE` table (created separately — see [Set up lineage tables](https://docs.atlan.com/platform/lakehouse/references/set-up-lineage-tables)). The `LINEAGE` table columns are: `start_guid`, `start_name`, `start_type`, `related_guid`, `related_name`, `related_type`, `direction` (`UPSTREAM`/`DOWNSTREAM`), `level` (hop depth).

4. **Usage analytics** (DAU/WAU/MAU, feature adoption, engagement, retention, health scoring)?
   → **`USAGE_ANALYTICS` namespace** (`PAGES`, `TRACKS`, `USERS`).

5. **Job/pipeline health** (DQ scores, job success rates, pipeline duration)?
   → **`OBSERVABILITY` namespace** (`JOB_METRICS`).

6. **Historical/temporal changes** (audit trails, point-in-time snapshots)?
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

## Reference Documentation

Detailed schemas, conventions, and SQL templates are organized in the `references/` directory:

| Reference | Description |
|-----------|-------------|
| [GOLD Namespace Schema](references/gold-namespace-schema.md) | Complete column-level schema for all GOLD tables (ASSETS, RELATIONAL_ASSET_DETAILS, GLOSSARY_DETAILS, DATA_QUALITY_DETAILS, PIPELINE_DETAILS, BI_ASSET_DETAILS, DATA_MESH_DETAILS) plus best practices |
| [GOLD Namespace Templates](references/gold-namespace-templates.md) | SQL templates for asset inventory, metadata completeness, and glossary export using GOLD tables |
| [Lineage Templates](references/lineage-templates.md) | SQL templates for all lineage use cases: assets without lineage, circular dependencies, coverage summary, lineage hub identification, full export, impact analysis, root cause analysis, downstream dashboard impact, and tag propagation across systems (Snowflake, Databricks, BigQuery) |
| [Metadata Export Templates](references/metadata-export-templates.md) | SQL templates for exporting asset metadata to data marketplaces, AI applications, and reverse sync workflows — including basic export and custom metadata enrichment (Snowflake, Databricks, BigQuery) |
| [Entity Metadata Reference](references/entity-metadata-reference.md) | ENTITY_METADATA namespace guide: supertype vs concrete tables, relationship tables, discovery |
| [Usage Analytics Conventions](references/usage-analytics-conventions.md) | Critical conventions for usage analytics queries: domain handling, identity, noise filtering, session derivation, timezone, feature area mapping |
| [Usage Analytics Templates](references/usage-analytics-templates.md) | SQL templates for schema profiling, active users (DAU/WAU/MAU), feature adoption, engagement depth, retention, and customer health scoring |
| [Observability Templates](references/observability-templates.md) | SQL templates for DQ score tracking, job success/failure rates, and job duration analysis |

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
