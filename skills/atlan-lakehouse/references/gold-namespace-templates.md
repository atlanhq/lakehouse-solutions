# GOLD Namespace SQL Templates

All templates use `{{PLACEHOLDER}}` parameters. See the [SQL Template Reference](../SKILL.md#sql-template-reference) in the main skill file for parameter definitions.

All templates are written in Snowflake SQL as the canonical dialect. **Agents should adapt syntax to the target engine on the fly** (e.g., `CONVERT_TIMEZONE` -> `FROM_UTC_TIMESTAMP` for Databricks, etc.).

---

## Asset Inventory: Relational Assets with Details

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

## Asset Inventory: BI Assets Across Tools

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

## Asset Inventory: Data Quality Summary

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

## Asset Inventory: Pipeline Dependencies

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

## Metadata Completeness: Asset Enrichment Tracking (GOLD + ENTITY_METADATA Hybrid)

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

## Metadata Completeness: By Data Domain (GOLD)

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

## Glossary: Comprehensive Term Export (GOLD)

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
