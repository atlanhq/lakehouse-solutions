# GOLD Namespace Schema Reference

The GOLD namespace is a **curated, star-schema layer** designed for simplified asset metadata queries. It is the **preferred starting point** for entity/asset metadata use cases.

## Architecture

- **`ASSETS`** is the central hub table containing every catalog asset with common attributes
- **Detail tables** branch out with domain-specific columns for particular asset categories
- All tables use **`guid`** as the primary key for consistent joins
- Data syncs from the underlying `ENTITY_METADATA` namespace in real time

## ASSETS Table (Hub)

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

## RELATIONAL_ASSET_DETAILS

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

## GLOSSARY_DETAILS

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

## DATA_QUALITY_DETAILS

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

## PIPELINE_DETAILS

Pipeline and ETL process attributes across ADF, Airflow, dbt, Fivetran, and Matillion.

| Column | Description |
|--------|-------------|
| `guid` | Asset GUID (join to `ASSETS.guid`) |
| `asset_type` | Type (`AirflowDag`, `DbtModel`, `FivetranConnector`, etc.) |
| `status` | Asset status |
| `input_guids_to_processes` | Processes using this asset as an input |
| `output_guids_from_processes` | Processes producing this asset as output |

## BI_ASSET_DETAILS

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

## DATA_MESH_DETAILS

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

## Best Practices

1. **Always start from `ASSETS`**: Filter to the assets you need, then join to detail tables.
2. **Filter early**: Apply `status = 'ACTIVE'`, `asset_type`, and `connector_name` filters before joins to minimize scanned data.
3. **Join on `guid`**: Always join detail tables using `guid`, not asset names or qualified names.
4. **Pair GUID lookups with `asset_type`**: Narrows the search space significantly.
5. **Avoid functions in WHERE clauses**: Wrapping columns in `LOWER()` etc. disables query optimization.
6. **Handle NULLs explicitly**: Use `COALESCE()` or explicit NULL checks for optional fields (owners, descriptions).
7. **Test with `LIMIT 100`**: Validate queries before running against full datasets.
