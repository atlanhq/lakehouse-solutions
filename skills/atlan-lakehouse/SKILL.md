---
name: atlan-lakehouse
description: >
  Use this skill when the user wants to query Atlan catalog metadata or usage analytics at scale,
  build reports on assets/glossaries/tags/user adoption, or analyze metadata from the lakehouse.
  Works across platforms: Snowflake (Cortex Code), Databricks (Genie Code), and Python (PyIceberg).
  Includes SQL template library for metadata completeness, lineage analysis, glossary export,
  and comprehensive usage analytics (active users, feature adoption, engagement, retention, health scoring).
license: Apache-2.0
compatibility: SQL (Snowflake, Databricks), Python 3.9+ with PyIceberg
metadata:
  author: atlan-platform-team
  version: "1.0.0"
  category: data-integration
  keywords: atlan-lakehouse, iceberg, polaris, pyiceberg, analytics, usage-analytics, snowflake, cortex, databricks, genie, lineage, glossary, metadata-completeness
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
| `ENTITY_METADATA`  | Catalog metadata: one table per asset type in Atlan's metamodel (e.g., `TABLE`, `COLUMN`, `VIEW`, `GLOSSARYTERM`, `DBTMODEL`, `DATADOMAIN`, etc.), plus relationship tables like `LINEAGE`, `CUSTOM_METADATA`, `GLOSSARY_DETAILS`, `README`, and `DATA_MESH_DETAILS`. Named `atlan-ns` on tenants onboarded before February 2026. **Important:** There is one table per asset type — the supertype tables (e.g., `ASSET`, `SQL`, `BI`, `SAAS`, `CLOUD`) are structural parents in Atlan's type hierarchy and typically have 0 rows. Query the specific type table that matches the assets you need (e.g., `TABLE` for Table assets, `COLUMN` for Column assets). Many tables will also have 0 rows if the tenant does not use that connector — for example, `AIRFLOWDAG` is empty if the tenant does not use Airflow. This is expected. |
| `ENTITY_HISTORY`   | Historical snapshots mirroring every table in `ENTITY_METADATA`, with added `snapshot_timestamp` and `snapshot_date` columns for temporal analysis, change tracking, and audit trails. |
| `USAGE_ANALYTICS`  | Product telemetry: page views, user actions, and user identity snapshots.                                                                   |

#### `USAGE_ANALYTICS` tables

| Table    | Key columns                                                                                         |
| -------- | --------------------------------------------------------------------------------------------------- |
| `PAGES`  | page name, tab, asset GUID, connector name, asset type, user ID, domain, timestamp                 |
| `TRACKS` | user ID, event_text, domain, timestamp                                                              |
| `USERS`  | user ID, email, username, role, license_type, job_role, created_at                                 |

## When to Use

Activate this skill when:

- User needs to connect to the Atlan Lakehouse from any platform (Snowflake, Databricks, or Python)
- User wants to query Atlan catalog metadata or run metadata governance reports
- User needs to assess metadata completeness, tag/description/owner coverage
- User wants to analyze lineage coverage, find orphaned assets, or detect circular dependencies
- User needs to export and analyze glossary terms with categories and assigned entities
- User wants to track historical metadata changes or generate audit trails
- User wants to analyze product adoption: DAU/WAU/MAU, feature engagement, retention, or engagement depth
- User wants to build usage dashboards or customer health scorecards
- User wants to identify churned/reactivated users, power users, or engagement tiers

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

## Connecting to the Atlan Lakehouse

### Snowflake / Cortex Code

When running inside Snowflake (e.g., Cortex Code), the lakehouse data is already available as a Snowflake database. No credentials or PyIceberg setup needed.

**Ask the user**: "Which Snowflake database contains your Atlan Lakehouse data?" The database name is customer-defined. Once known, query tables directly:

```sql
-- List schemas (namespaces)
SHOW SCHEMAS IN DATABASE <lakehouse_database>;

-- Query tables
SELECT * FROM <lakehouse_database>.ENTITY_METADATA.TABLE LIMIT 10;  -- use concrete type tables (TABLE, COLUMN, VIEW, etc.), not ASSET
SELECT * FROM <lakehouse_database>.USAGE_ANALYTICS.TRACKS LIMIT 10;
```

Use the database name as `{{DATABASE}}` in all SQL templates below. The schema maps to the namespace (e.g., `ENTITY_METADATA`, `USAGE_ANALYTICS`).

### Databricks / Genie Code

When running inside Databricks (e.g., Genie Code), the lakehouse data is already available as an Iceberg catalog in Unity Catalog. No credentials or PyIceberg setup needed.

**Ask the user**: "Which catalog in Unity Catalog contains your Atlan Lakehouse data?" The catalog name is customer-defined. Once known, query tables directly:

```sql
-- List schemas (namespaces)
SHOW SCHEMAS IN <lakehouse_catalog>;

-- Query tables
SELECT * FROM <lakehouse_catalog>.ENTITY_METADATA.TABLE LIMIT 10;  -- use concrete type tables (TABLE, COLUMN, VIEW, etc.), not ASSET
SELECT * FROM <lakehouse_catalog>.USAGE_ANALYTICS.TRACKS LIMIT 10;
```

Use the catalog name as `{{DATABASE}}` in all SQL templates below. The schema maps to the namespace.

### Python / PyIceberg

For generic Python environments (Claude Code, notebooks, scripts), connect via the Polaris REST catalog.

#### Python Dependencies

```bash
python -c "import pyiceberg" 2>/dev/null || pip install "pyiceberg[s3fs,adlfs,gcsfs]" pyarrow pandas
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
| `{{SCHEMA}}` | Unquoted | `USAGE_ANALYTICS` | Schema/namespace name |
| `{{DOMAIN}}` | Single-quoted | `'acme.atlan.com'` | Your Atlan tenant domain (e.g., `'acme.atlan.com'`). Derive from tenant name: `{tenant}.atlan.com` |
| `{{START_DATE}}` | Single-quoted | `'2025-01-01'` | Date range start |
| `{{END_DATE}}` | Single-quoted | `'2025-12-31'` | Date range end |
| `{{MONTHS_BACK}}` | Bare integer | `6` | Lookback months |
| `{{RETENTION_DAYS}}` | Bare integer | `14` | Retention window |

---

## Entity Metadata Templates

These query the `ENTITY_METADATA` namespace. Key relationship/consolidated tables include `LINEAGE`, `GLOSSARY_DETAILS`, `README`, `CUSTOM_METADATA`, and `DATA_MESH_DETAILS`. Asset data lives in **per-type tables** (e.g., `TABLE`, `COLUMN`, `VIEW`, `DBTMODEL`, `GLOSSARYTERM`). Use `catalog.list_tables("ENTITY_METADATA")` or `SHOW TABLES IN <db>.ENTITY_METADATA` to discover the full set.

> **Supertype tables have 0 rows.** Atlan's metamodel uses a type hierarchy. Abstract supertype tables like `ASSET`, `SQL`, `BI`, `SAAS`, `CLOUD`, `NOSQL`, `OBJECTSTORE`, `CATALOG`, `INFRASTRUCTURE`, and `EVENTSTORE` exist in the namespace but contain no data — they are structural parents only. Always query the concrete type table instead (e.g., `TABLE` not `ASSET`, `SNOWFLAKE` not `SQL`). Similarly, tables for unused connectors (e.g., `AIRFLOWDAG` on a tenant that does not use Airflow) will have 0 rows. When building cross-type queries, `UNION ALL` across the specific type tables you need rather than querying a supertype.

### Metadata Completeness: Asset Enrichment Tracking

Measures description, tag, certification, ownership, and custom metadata coverage by asset type.

```sql
WITH cm_stats AS (
    SELECT
        alt.GUID AS asset_guid,
        SUM(
            CASE
                WHEN cm.ATTRIBUTE_VALUE IS NULL THEN 0
                WHEN IS_ARRAY(TRY_PARSE_JSON(cm.ATTRIBUTE_VALUE))
                THEN CASE WHEN ARRAY_SIZE(TRY_PARSE_JSON(cm.ATTRIBUTE_VALUE)) > 0 THEN 1 ELSE 0 END
                WHEN cm.ATTRIBUTE_VALUE IS NOT NULL THEN 1
                ELSE 0
            END
        ) AS linked_cm_prop_count
    FROM ASSETS alt
    LEFT JOIN CUSTOM_METADATA cm ON alt.GUID = cm.ASSET_GUID
    GROUP BY alt.GUID
),
entity_stats AS (
    SELECT
        ASSET_TYPE,
        COUNT(*) AS total_count,
        COUNT(CASE WHEN DESCRIPTION IS NOT NULL AND DESCRIPTION <> '' THEN 1 END) AS with_description,
        COUNT(CASE WHEN DESCRIPTION IS NULL OR DESCRIPTION = '' THEN 1 END) AS without_description,
        COUNT(CASE WHEN LOWER(CERTIFICATE_STATUS) = 'verified' THEN 1 END) AS certified,
        COUNT(CASE WHEN LOWER(CERTIFICATE_STATUS) != 'verified' OR CERTIFICATE_STATUS IS NULL THEN 1 END) AS uncertified,
        COUNT(CASE WHEN TAGS IS NOT NULL AND ARRAY_SIZE(TAGS) > 0 THEN 1 END) AS with_tags,
        COUNT(CASE WHEN TAGS IS NULL OR ARRAY_SIZE(TAGS) = 0 THEN 1 END) AS without_tags,
        COUNT(CASE WHEN OWNER_USERS IS NOT NULL AND ARRAY_SIZE(OWNER_USERS) > 0 THEN 1 END) AS with_owners,
        COUNT(CASE WHEN OWNER_USERS IS NULL OR ARRAY_SIZE(OWNER_USERS) = 0 THEN 1 END) AS without_owners,
        COUNT(CASE WHEN linked_cm_prop_count > 0 THEN 1 END) AS with_linked_cm_props,
        COUNT(CASE WHEN linked_cm_prop_count = 0 OR linked_cm_prop_count IS NULL THEN 1 END) AS without_linked_cm_props
    FROM ASSETS alt
    LEFT JOIN cm_stats cm ON alt.GUID = cm.asset_guid
    WHERE ASSET_TYPE IN ('Table', 'Schema', 'TableauDashboard', 'TableauWorkbook',
                         'DataDomain', 'DataProduct', 'AtlasGlossaryTerm',
                         'AtlasGlossaryCategory', 'AtlasGlossary')
    GROUP BY ASSET_TYPE
)
SELECT
    ASSET_TYPE, total_count,
    with_description, without_description,
    ROUND((with_description * 100.0) / NULLIF(total_count, 0), 2) AS description_coverage_pct,
    with_tags, without_tags,
    ROUND((with_tags * 100.0) / NULLIF(total_count, 0), 2) AS tag_coverage_pct,
    certified, uncertified,
    ROUND((certified * 100.0) / NULLIF(total_count, 0), 2) AS certification_coverage_pct,
    with_owners, without_owners,
    ROUND((with_owners * 100.0) / NULLIF(total_count, 0), 2) AS ownership_coverage_pct,
    with_linked_cm_props, without_linked_cm_props,
    ROUND((with_linked_cm_props * 100.0) / NULLIF(total_count, 0), 2) AS custom_metadata_coverage_pct
FROM entity_stats
ORDER BY ASSET_TYPE;
```

### Metadata Completeness: By Data Domain

Domain-level enrichment statistics with an overall enrichment score.

```sql
WITH domains AS (
    SELECT a.GUID AS domain_guid, a.ASSET_NAME AS domain_name
    FROM ASSETS a
    WHERE a.ASSET_TYPE = 'DataDomain' AND a.STATUS = 'ACTIVE'
),
assets_with_enrichment AS (
    SELECT
        COALESCE(d.domain_guid, 'UNASSIGNED') AS domain_guid,
        COALESCE(d.domain_name, 'No Domain Assigned') AS domain_name,
        a.GUID AS asset_guid,
        a.ASSET_NAME, a.ASSET_TYPE,
        CASE WHEN a.TAGS IS NOT NULL AND ARRAY_SIZE(a.TAGS) > 0 THEN 1 ELSE 0 END AS has_tags,
        CASE WHEN a.TERM_GUIDS IS NOT NULL AND ARRAY_SIZE(a.TERM_GUIDS) > 0 THEN 1 ELSE 0 END AS has_terms,
        CASE WHEN a.README_GUID IS NOT NULL THEN 1 ELSE 0 END AS has_readme,
        CASE WHEN a.DESCRIPTION IS NOT NULL AND LENGTH(TRIM(a.DESCRIPTION)) > 0 THEN 1 ELSE 0 END AS has_description
    FROM ASSETS a
    LEFT JOIN DATA_MESH_DETAILS dmd ON a.GUID = dmd.GUID
    LEFT JOIN domains d ON dmd.DATA_DOMAIN = d.domain_guid
    WHERE a.STATUS = 'ACTIVE'
)
SELECT
    domain_name, domain_guid,
    COUNT(*) AS total_assets,
    SUM(has_tags) AS assets_with_tags,
    ROUND((SUM(has_tags) * 100.0) / NULLIF(COUNT(*), 0), 2) AS pct_with_tags,
    SUM(has_terms) AS assets_with_terms,
    ROUND((SUM(has_terms) * 100.0) / NULLIF(COUNT(*), 0), 2) AS pct_with_terms,
    SUM(has_readme) AS assets_with_readme,
    ROUND((SUM(has_readme) * 100.0) / NULLIF(COUNT(*), 0), 2) AS pct_with_readme,
    SUM(has_description) AS assets_with_description,
    ROUND((SUM(has_description) * 100.0) / NULLIF(COUNT(*), 0), 2) AS pct_with_description,
    ROUND(((SUM(has_tags) * 100.0) / NULLIF(COUNT(*), 0) + (SUM(has_terms) * 100.0) / NULLIF(COUNT(*), 0)
         + (SUM(has_readme) * 100.0) / NULLIF(COUNT(*), 0) + (SUM(has_description) * 100.0) / NULLIF(COUNT(*), 0)) / 4.0, 2
    ) AS overall_enrichment_score
FROM assets_with_enrichment
GROUP BY domain_name, domain_guid
ORDER BY CASE WHEN domain_guid = 'UNASSIGNED' THEN 1 ELSE 0 END, overall_enrichment_score DESC;
```

### Lineage: Assets Without Lineage

Identifies active data assets with no lineage connections. Includes cleanup recommendations.

```sql
SELECT
    A.GUID, A.ASSET_NAME, A.ASSET_TYPE, A.ASSET_QUALIFIED_NAME, A.CONNECTOR_NAME,
    A.DESCRIPTION, A.CERTIFICATE_STATUS, A.STATUS, A.OWNER_USERS, A.TAGS, A.HAS_LINEAGE,
    TO_TIMESTAMP_LTZ(A.CREATED_AT / 1000) AS CREATED_DATE,
    TO_TIMESTAMP_LTZ(A.UPDATED_AT / 1000) AS LAST_UPDATED,
    DATEDIFF(day, TO_TIMESTAMP_LTZ(A.CREATED_AT / 1000), CURRENT_TIMESTAMP()) AS DAYS_SINCE_CREATION,
    DATEDIFF(day, TO_TIMESTAMP_LTZ(A.UPDATED_AT / 1000), CURRENT_TIMESTAMP()) AS DAYS_SINCE_UPDATE,
    CASE
        WHEN A.CERTIFICATE_STATUS = 'DEPRECATED' THEN 'SAFE TO DELETE - Already deprecated'
        WHEN DATEDIFF(day, TO_TIMESTAMP_LTZ(A.UPDATED_AT / 1000), CURRENT_TIMESTAMP()) > 180 AND A.STATUS = 'ACTIVE' THEN 'REVIEW FOR DELETION - No activity in 6+ months'
        WHEN DATEDIFF(day, TO_TIMESTAMP_LTZ(A.CREATED_AT / 1000), CURRENT_TIMESTAMP()) <= 7 THEN 'KEEP - Recently created, may not be connected yet'
        WHEN A.OWNER_USERS IS NULL THEN 'INVESTIGATE - No owner, likely test/temp asset'
        ELSE 'REVIEW - May be intentionally standalone'
    END AS CLEANUP_RECOMMENDATION
FROM ASSETS A
WHERE A.HAS_LINEAGE = FALSE
    AND A.ASSET_TYPE IN ('Table', 'View', 'MaterializedView')
    AND A.STATUS = 'ACTIVE'
    AND DATEDIFF(day, TO_TIMESTAMP_LTZ(A.UPDATED_AT / 1000), CURRENT_TIMESTAMP()) > 90
ORDER BY DAYS_SINCE_UPDATE DESC, A.ASSET_NAME;
```

### Lineage: Circular Dependencies

Detects assets with circular lineage where data flows back to itself.

```sql
SELECT
    A.GUID AS ASSET_GUID, A.ASSET_NAME, A.ASSET_TYPE,
    A.ASSET_QUALIFIED_NAME AS ASSET_PATH, A.CONNECTOR_NAME,
    A.OWNER_USERS, A.CERTIFICATE_STATUS,
    TO_TIMESTAMP_LTZ(A.UPDATED_AT / 1000) AS LAST_UPDATED,
    L.DIRECTION AS LINEAGE_DIRECTION,
    L.LEVEL AS CIRCULAR_PATH_LENGTH,
    L.RELATED_NAME AS RELATED_ASSET_NAME,
    L.RELATED_TYPE AS RELATED_ASSET_TYPE,
    CASE
        WHEN L.LEVEL = 1 THEN 'DIRECT SELF-REFERENCE - Review immediately'
        WHEN L.LEVEL <= 3 THEN 'SHORT CIRCULAR PATH - May cause performance issues'
        ELSE 'LONG CIRCULAR PATH - Complex dependency chain'
    END AS CIRCULAR_DEPENDENCY_SEVERITY
FROM ASSETS A
INNER JOIN LINEAGE L ON A.GUID = L.START_GUID
WHERE L.RELATED_GUID = A.GUID
    AND A.ASSET_TYPE IN ('Table', 'View', 'MaterializedView', 'DbtModel')
    AND A.STATUS = 'ACTIVE'
ORDER BY L.LEVEL ASC, A.ASSET_NAME;
```

### Lineage: Overall Coverage Summary

High-level dashboard view of lineage coverage.

```sql
SELECT
    COUNT(*) AS TOTAL_ASSETS,
    SUM(CASE WHEN HAS_LINEAGE = TRUE THEN 1 ELSE 0 END) AS ASSETS_WITH_LINEAGE,
    SUM(CASE WHEN HAS_LINEAGE = FALSE THEN 1 ELSE 0 END) AS ASSETS_WITHOUT_LINEAGE,
    ROUND((SUM(CASE WHEN HAS_LINEAGE = TRUE THEN 1 ELSE 0 END) * 100.0) / COUNT(*), 2) AS LINEAGE_COVERAGE_PCT
FROM ASSETS
WHERE STATUS = 'ACTIVE';
```

### Lineage: Coverage by Connector

Compare lineage coverage across different data platforms.

```sql
SELECT
    CONNECTOR_NAME, ASSET_TYPE,
    COUNT(*) AS TOTAL_ASSETS,
    SUM(CASE WHEN HAS_LINEAGE = TRUE THEN 1 ELSE 0 END) AS WITH_LINEAGE,
    SUM(CASE WHEN HAS_LINEAGE = FALSE THEN 1 ELSE 0 END) AS WITHOUT_LINEAGE,
    ROUND((SUM(CASE WHEN HAS_LINEAGE = TRUE THEN 1 ELSE 0 END) * 100.0) / COUNT(*), 2) AS LINEAGE_COVERAGE_PCT
FROM ASSETS
WHERE STATUS = 'ACTIVE' AND NULLIF(CONNECTOR_NAME, '') IS NOT NULL
GROUP BY CONNECTOR_NAME, ASSET_TYPE
ORDER BY CONNECTOR_NAME, LINEAGE_COVERAGE_PCT DESC, TOTAL_ASSETS DESC;
```

### Lineage: Most Connected Assets (Hubs)

Identifies hub tables with the highest upstream/downstream connections. Classifies criticality.

```sql
SELECT
    A.ASSET_NAME, A.ASSET_TYPE, A.ASSET_QUALIFIED_NAME, A.CONNECTOR_NAME,
    A.CERTIFICATE_STATUS, A.OWNER_USERS,
    TO_TIMESTAMP_LTZ(A.UPDATED_AT / 1000) AS LAST_UPDATED,
    DATEDIFF(day, TO_TIMESTAMP_LTZ(A.UPDATED_AT / 1000), CURRENT_TIMESTAMP()) AS DAYS_SINCE_UPDATE,
    COUNT(CASE WHEN L.DIRECTION = 'UPSTREAM' AND L.LEVEL = 1 THEN 1 END) AS UPSTREAM_COUNT,
    COUNT(CASE WHEN L.DIRECTION = 'DOWNSTREAM' AND L.LEVEL = 1 THEN 1 END) AS DOWNSTREAM_COUNT,
    COUNT(CASE WHEN L.LEVEL = 1 THEN 1 END) AS TOTAL_CONNECTIONS,
    CASE
        WHEN COUNT(CASE WHEN L.DIRECTION = 'DOWNSTREAM' AND L.LEVEL = 1 THEN 1 END) >= 20 THEN 'CRITICAL - 20+ downstream dependencies'
        WHEN COUNT(CASE WHEN L.DIRECTION = 'DOWNSTREAM' AND L.LEVEL = 1 THEN 1 END) >= 10 THEN 'HIGH IMPACT - 10-19 downstream dependencies'
        WHEN COUNT(CASE WHEN L.DIRECTION = 'DOWNSTREAM' AND L.LEVEL = 1 THEN 1 END) >= 5 THEN 'MODERATE IMPACT - 5-9 downstream dependencies'
        ELSE 'LOW IMPACT - <5 downstream dependencies'
    END AS CRITICALITY_LEVEL
FROM ASSETS A
LEFT JOIN LINEAGE L ON A.GUID = L.START_GUID
WHERE A.HAS_LINEAGE = TRUE
    AND A.ASSET_TYPE IN ('Table', 'View', 'MaterializedView')
    AND A.STATUS = 'ACTIVE'
GROUP BY A.ASSET_NAME, A.ASSET_TYPE, A.ASSET_QUALIFIED_NAME, A.CONNECTOR_NAME, A.CERTIFICATE_STATUS, A.OWNER_USERS, A.UPDATED_AT
HAVING COUNT(CASE WHEN L.LEVEL = 1 THEN 1 END) > 0
ORDER BY TOTAL_CONNECTIONS DESC;
```

### Glossary: Comprehensive Term Export

Retrieves glossary terms with parent glossary, categories, readme descriptions, and assigned entity details.

```sql
WITH glossary_terms AS (
    SELECT
        t.GUID AS term_guid, t.NAME AS term_name, t.QUALIFIED_NAME AS term_qualified_name,
        t.DESCRIPTION AS term_description, t.STATUS AS term_status,
        t.CERTIFICATE_STATUS AS term_certificate_status, t.OWNER_USERS AS term_owner_users,
        TO_TIMESTAMP_LTZ(t.CREATED_TIME / 1000) AS term_created_at,
        TO_TIMESTAMP_LTZ(t.UPDATED_TIME / 1000) AS term_updated_at,
        t.CREATED_BY AS term_created_by, t.UPDATED_BY AS term_updated_by,
        t.ANCHOR_GUID AS glossary_guid,
        t.CATEGORIES AS term_categories,
        t.README_GUID AS term_readme_guid,
        t.ASSIGNED_ENTITIES AS term_assigned_entities
    FROM GLOSSARY_DETAILS t
    WHERE t.ASSET_TYPE = 'AtlasGlossaryTerm' AND t.STATUS = 'ACTIVE'
),
glossary_info AS (
    SELECT g.GUID AS glossary_guid, g.NAME AS glossary_name,
           g.QUALIFIED_NAME AS glossary_qualified_name, g.DESCRIPTION AS glossary_description,
           g.CERTIFICATE_STATUS AS glossary_certificate_status, g.OWNER_USERS AS glossary_owner_users
    FROM GLOSSARY_DETAILS g
    WHERE g.ASSET_TYPE = 'AtlasGlossary' AND g.STATUS = 'ACTIVE'
),
category_info AS (
    SELECT c.GUID AS category_guid, c.NAME AS category_name,
           c.QUALIFIED_NAME AS category_qualified_name, c.DESCRIPTION AS category_description
    FROM GLOSSARY_DETAILS c
    WHERE c.ASSET_TYPE = 'AtlasGlossaryCategory' AND c.STATUS = 'ACTIVE'
),
readme_info AS (
    SELECT r.GUID AS readme_guid, r.DESCRIPTION AS readme_description
    FROM README r WHERE r.STATUS = 'ACTIVE'
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
    SELECT taa.term_guid, a.GUID AS asset_guid, a.ASSET_NAME, a.ASSET_QUALIFIED_NAME, a.ASSET_TYPE
    FROM term_assigned_assets taa
    INNER JOIN ASSETS a ON a.GUID = taa.asset_guid
    WHERE a.STATUS = 'ACTIVE'
)
SELECT
    t.term_guid, t.term_name, t.term_qualified_name, t.term_description,
    t.term_status, t.term_certificate_status, t.term_owner_users,
    t.term_created_at, t.term_updated_at, t.term_created_by, t.term_updated_by,
    DATEDIFF(day, t.term_updated_at, CURRENT_TIMESTAMP()) AS days_since_update,
    g.glossary_guid, g.glossary_name, g.glossary_qualified_name, g.glossary_description,
    g.glossary_certificate_status, g.glossary_owner_users,
    t.term_readme_guid, r.readme_description AS term_readme_description,
    ARRAY_AGG(DISTINCT c.category_guid) AS category_guids,
    ARRAY_AGG(DISTINCT c.category_name) AS category_names,
    COUNT(DISTINCT c.category_guid) AS category_count,
    ARRAY_AGG(DISTINCT ad.asset_guid) AS assigned_asset_guids,
    ARRAY_AGG(DISTINCT ad.ASSET_NAME) AS assigned_asset_names,
    ARRAY_AGG(DISTINCT ad.ASSET_TYPE) AS assigned_asset_types,
    COUNT(DISTINCT ad.asset_guid) AS assigned_asset_count
FROM glossary_terms t
LEFT JOIN glossary_info g ON g.glossary_guid = t.glossary_guid
LEFT JOIN readme_info r ON r.readme_guid = t.term_readme_guid
LEFT JOIN term_categories_expanded tce ON tce.term_guid = t.term_guid
LEFT JOIN category_info c ON c.category_guid = tce.category_guid
LEFT JOIN asset_details ad ON ad.term_guid = t.term_guid
GROUP BY t.term_guid, t.term_name, t.term_qualified_name, t.term_description,
         t.term_status, t.term_certificate_status, t.term_owner_users,
         t.term_created_at, t.term_updated_at, t.term_created_by, t.term_updated_by,
         g.glossary_guid, g.glossary_name, g.glossary_qualified_name, g.glossary_description,
         g.glossary_certificate_status, g.glossary_owner_users,
         t.term_readme_guid, r.readme_description
ORDER BY g.glossary_name, t.term_name;
```

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
