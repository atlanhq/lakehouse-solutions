# Entity Metadata Reference

> **For most asset metadata queries, prefer the GOLD namespace which provides simpler, pre-joined tables.** Use `ENTITY_METADATA` directly when you need: tags (`TAGS` table), custom metadata (`CUSTOM_METADATA` table), readmes (`README` table), lineage (`PROCESS` / `COLUMN_PROCESS` / `BI_PROCESS` tables), or per-type tables not covered by GOLD.

## Overview

The `ENTITY_METADATA` namespace contains one table per asset type in Atlan's metamodel (e.g., `TABLE`, `COLUMN`, `VIEW`, `GLOSSARYTERM`, `DBTMODEL`, `DATADOMAIN`, etc.), plus relationship/consolidated tables like `TAGS`, `CUSTOM_METADATA`, `GLOSSARY_DETAILS`, `README`, `DATA_MESH_DETAILS`, and lineage process tables (`PROCESS`, `COLUMN_PROCESS`, `BI_PROCESS`).

**Namespace naming:** Named `entity_metadata` on most tenants. Named `atlan-ns` on tenants onboarded before February 2026.

## Key Relationship/Consolidated Tables

| Table | Purpose |
|-------|---------|
| `TAGS` | Classification tags on assets |
| `CUSTOM_METADATA` | Custom metadata attributes |
| `GLOSSARY_DETAILS` | Glossary term/category relationships |
| `README` | Readme content |
| `DATA_MESH_DETAILS` | Data domain and product attributes |
| `PROCESS` | Table-level lineage relationships |
| `COLUMN_PROCESS` | Column-level lineage relationships |
| `BI_PROCESS` | BI tool lineage relationships |

## Supertype Tables Have 0 Rows

Atlan's metamodel uses a type hierarchy. Abstract supertype tables exist in the namespace but contain no data — they are structural parents only. **Always query the concrete type table instead.**

| Supertype (0 rows) | Query instead |
|---------------------|---------------|
| `ASSET` | `TABLE`, `COLUMN`, `VIEW`, etc. |
| `SQL` | `SNOWFLAKE`, `DATABRICKS`, etc. |
| `BI` | `TABLEAUWORKBOOK`, `POWERBIDASHBOARD`, etc. |
| `SAAS` | Specific SaaS connector tables |
| `CLOUD` | Specific cloud connector tables |
| `NOSQL` | Specific NoSQL connector tables |
| `OBJECTSTORE` | Specific object store tables |
| `CATALOG` | Specific catalog tables |
| `INFRASTRUCTURE` | Specific infrastructure tables |
| `EVENTSTORE` | Specific event store tables |

Similarly, tables for unused connectors (e.g., `AIRFLOWDAG` on a tenant that does not use Airflow) will have 0 rows. When building cross-type queries, `UNION ALL` across the specific type tables you need rather than querying a supertype.

## Discovering Tables

```sql
-- Snowflake
SHOW TABLES IN <database>.ENTITY_METADATA;

-- Databricks
SHOW TABLES IN <catalog>.ENTITY_METADATA;
```

```python
# PyIceberg
tables = catalog.list_tables("entity_metadata")
```

## Notes

- The metadata completeness and glossary templates are in the [GOLD Namespace Templates](gold-namespace-templates.md), which provides simpler, pre-joined queries.
- Lineage query templates will be added in a future update using the `PROCESS`, `COLUMN_PROCESS`, and `BI_PROCESS` tables. In the meantime, you can use `GOLD.ASSETS.has_lineage` to check lineage coverage without joining process tables.
