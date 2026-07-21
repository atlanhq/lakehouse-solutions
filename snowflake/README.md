# Snowflake Solutions

Solutions for the Atlan Metadata Lakehouse (MDLH) on Snowflake.

## Available Solutions

| Solution | Description |
|----------|-------------|
| [Catalog Integration](./catalog-integration/) | Connect Snowflake to the Atlan Lakehouse via native Iceberg REST Catalog federation — create external volumes, catalog integrations, and linked databases |
| [Gold Layer](./gold-layer/) | Deploy the MDLH Gold Layer — curated, analytics-ready metadata views and dynamic tables in Snowflake |
| [MDLH Table Maintenance](./mdlh-table-maintenance/) | Native Streamlit app to detect and repair stale Iceberg tables |
| [MDLH Object Store Sync](./mdlh-object-store-sync/) | Native Streamlit app to integrate Snowflake with the Atlan Lakehouse via object storage only — no Iceberg REST catalog, works on Enterprise edition |

## Troubleshooting

- [Iceberg tables stop auto-refreshing](./catalog-integration/TROUBLESHOOTING.md) — diagnose stale tables and restore auto-refresh
