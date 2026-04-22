# Metadata Export SQL Templates

Templates for exporting enriched asset metadata from the Atlan Lakehouse to power AI applications, data marketplaces, reverse sync workflows, and governance dashboards.

All templates use `{{PLACEHOLDER}}` parameters. See the [SQL Template Reference](../SKILL.md#sql-template-reference) in the main skill file for parameter definitions.

> **Namespaces used:** `GOLD.ASSETS` (core asset data), `ENTITY_METADATA.Readme` (README content), `ENTITY_METADATA.TagRelationship` (tags), `ENTITY_METADATA.CustomMetadata` (custom metadata attributes).

---

## 1. Basic Asset Metadata Export

Export core asset metadata — descriptions, certification, owners, tags, README, popularity, and lineage flag — for downstream tools such as data marketplaces or AI search indexes.

```sql
-- ASSET METADATA EXPORT: Core attributes with README and tags
-- Snowflake
USE {{DATABASE}};
SELECT
    A.asset_name,
    A.guid,
    A.asset_qualified_name,
    A.asset_type,
    A.description,
    A.user_description,
    A.status,
    A.certificate_status,
    A.owner_users,
    tr.TAGS,
    R.description AS README_TEXT,
    A.popularity_score,
    A.has_lineage
FROM {{DATABASE}}.GOLD.ASSETS A
LEFT JOIN {{DATABASE}}.ENTITY_METADATA.Readme R ON A.readme_guid = R.GUID
LEFT JOIN (
    SELECT guid, ARRAY_AGG(tagName) AS TAGS
    FROM {{DATABASE}}.ENTITY_METADATA.TagRelationship
    WHERE status = 'ACTIVE'
    GROUP BY guid
) tr ON A.guid = tr.guid
WHERE
    A.asset_type IN ('Table', 'Column', 'View')
    AND A.connector_name IN ('snowflake', 'redshift', 'bigquery')
    AND R.description IS NOT NULL  -- only assets with README documentation
LIMIT 10;
```

---

## 2. Asset Enrichment with Custom Metadata

Export assets enriched with a named custom metadata set (e.g., "AI Readiness" scores). Useful for building data product catalogs or readiness dashboards.

### Snowflake

```sql
-- ASSET ENRICHMENT WITH CUSTOM METADATA SCORES — Snowflake
USE {{DATABASE}};
WITH CM_DETAILS AS (
    SELECT
        CM.GUID,
        PARSE_JSON(
            '{' ||
            LISTAGG(
                '"' || CM.attributeName || '":"' ||
                REPLACE(COALESCE(CM.attributeValue, ''), '"', '\\"') || '"',
                ','
            ) WITHIN GROUP (ORDER BY CM.attributeName)
            || '}'
        ) AS CM_ATTRIBUTES_JSON
    FROM {{DATABASE}}.ENTITY_METADATA.CustomMetadata CM
    WHERE
        CM.STATUS = 'ACTIVE'
        AND CM.customMetadataSetName = 'AI Readiness'  -- 🔧 CUSTOMIZE: your CM set name
        AND CM.attributeValue IS NOT NULL
    GROUP BY CM.GUID
)
SELECT
    A.asset_name,
    A.guid,
    A.asset_qualified_name,
    A.asset_type,
    A.description,
    A.user_description,
    R.description AS README_TEXT,
    A.status,
    A.certificate_status,
    A.owner_users,
    tr.TAGS,
    A.popularity_score,
    A.has_lineage,
    CMS.CM_ATTRIBUTES_JSON
FROM {{DATABASE}}.GOLD.ASSETS A
LEFT JOIN {{DATABASE}}.ENTITY_METADATA.Readme R ON A.readme_guid = R.GUID
LEFT JOIN CM_DETAILS CMS ON A.guid = CMS.GUID
LEFT JOIN (
    SELECT guid, ARRAY_AGG(tagName) AS TAGS
    FROM {{DATABASE}}.ENTITY_METADATA.TagRelationship
    WHERE status = 'ACTIVE'
    GROUP BY guid
) tr ON A.guid = tr.guid
WHERE
    A.asset_type IN ('Table', 'Column', 'View')
    AND A.connector_name IN ('snowflake', 'redshift', 'bigquery')
    AND A.status = 'ACTIVE'
    AND CMS.CM_ATTRIBUTES_JSON IS NOT NULL
    AND A.owner_users IS NOT NULL
LIMIT 5;
```

### Databricks

```sql
-- ASSET ENRICHMENT WITH CUSTOM METADATA SCORES — Databricks
WITH CM_JSON AS (
    SELECT
        CM.GUID,
        collect_list(CM.attributeName) AS SCORE_ATTRIBUTE_NAMES,
        collect_list(CM.attributeValue) AS SCORE_ATTRIBUTE_VALUES,
        concat(
            '{',
            concat_ws(
                ',',
                collect_list(
                    concat(
                        '"', CM.attributeName, '":"',
                        regexp_replace(coalesce(CM.attributeValue, ''), '"', '\\\\"'), '"'
                    )
                )
            ),
            '}'
        ) AS CM_ATTRIBUTES_JSON
    FROM {{DATABASE}}.ENTITY_METADATA.CustomMetadata CM
    WHERE
        CM.STATUS = 'ACTIVE'
        AND CM.customMetadataSetName = 'AI Readiness'  -- 🔧 CUSTOMIZE
        AND CM.attributeValue IS NOT NULL
    GROUP BY CM.GUID
)
SELECT
    A.asset_name,
    A.guid,
    A.asset_qualified_name,
    A.asset_type,
    A.description,
    A.user_description,
    R.description AS README_TEXT,
    A.status,
    A.certificate_status,
    A.owner_users,
    tr.TAGS,
    A.popularity_score,
    A.has_lineage,
    CMS.CM_ATTRIBUTES_JSON
FROM {{DATABASE}}.GOLD.ASSETS A
LEFT JOIN {{DATABASE}}.ENTITY_METADATA.Readme R ON A.readme_guid = R.GUID
LEFT JOIN CM_JSON CMS ON A.guid = CMS.GUID
LEFT JOIN (
    SELECT guid, collect_list(tagName) AS TAGS
    FROM {{DATABASE}}.ENTITY_METADATA.TagRelationship
    WHERE status = 'ACTIVE'
    GROUP BY guid
) tr ON A.guid = tr.guid
WHERE
    A.connector_name IN ('snowflake', 'redshift', 'bigquery', 'oracle')
    AND A.status = 'ACTIVE'
    AND CMS.CM_ATTRIBUTES_JSON IS NOT NULL
    AND A.owner_users IS NOT NULL
    AND size(A.owner_users) > 0
LIMIT 10;
```

### BigQuery

```sql
-- ASSET ENRICHMENT WITH CUSTOM METADATA SCORES — BigQuery
WITH CM_JSON AS (
    SELECT
        CM.GUID,
        PARSE_JSON(
            CONCAT(
                '{',
                STRING_AGG(
                    CONCAT('"', CM.attributeName, '":"', IFNULL(CM.attributeValue, ''), '"'),
                    ',' ORDER BY CM.attributeName
                ),
                '}'
            )
        ) AS CM_ATTRIBUTES_JSON
    FROM {{DATABASE}}.ENTITY_METADATA.CustomMetadata CM
    WHERE
        CM.STATUS = 'ACTIVE'
        AND CM.customMetadataSetName = 'AI Readiness'  -- 🔧 CUSTOMIZE
    GROUP BY CM.GUID
)
SELECT
    A.asset_name,
    A.guid,
    A.asset_qualified_name,
    A.asset_type,
    A.description,
    A.user_description,
    R.description AS README_TEXT,
    A.status,
    A.certificate_status,
    A.owner_users,
    tr.TAGS,
    A.popularity_score,
    A.has_lineage,
    CMS.CM_ATTRIBUTES_JSON,
    -- Extract specific attributes from the JSON
    JSON_EXTRACT_SCALAR(CMS.CM_ATTRIBUTES_JSON, '$.data_quality_score') AS data_quality_score,
    JSON_EXTRACT_SCALAR(CMS.CM_ATTRIBUTES_JSON, '$.ai_readiness_score') AS ai_readiness_score
FROM {{DATABASE}}.GOLD.ASSETS A
LEFT JOIN {{DATABASE}}.ENTITY_METADATA.Readme R ON A.readme_guid = R.GUID
LEFT JOIN CM_JSON CMS ON A.guid = CMS.GUID
LEFT JOIN (
    SELECT guid, ARRAY_AGG(tagName) AS TAGS
    FROM {{DATABASE}}.ENTITY_METADATA.TagRelationship
    WHERE status = 'ACTIVE'
    GROUP BY guid
) tr ON A.guid = tr.guid
WHERE
    A.connector_name IN ('snowflake', 'redshift', 'bigquery', 'oracle')
    AND A.status = 'ACTIVE'
    AND CMS.CM_ATTRIBUTES_JSON IS NOT NULL
    AND A.owner_users IS NOT NULL
    AND ARRAY_LENGTH(A.owner_users) > 0
LIMIT 10;
```

---

## Common Customizations

```sql
-- Broaden to all certified assets (not just those with README)
-- Remove: AND R.description IS NOT NULL

-- Narrow to a single connector
AND A.connector_name = 'snowflake'

-- Include only verified/certified assets
AND A.certificate_status = 'VERIFIED'

-- Include assets with high popularity
AND A.popularity_score > 0.5

-- Include all asset types (not just Table/Column/View)
-- Remove the asset_type filter entirely, or expand:
AND A.asset_type IN ('Table', 'Column', 'View', 'Dashboard', 'AtlasGlossaryTerm')

-- Change the custom metadata set name
AND CM.customMetadataSetName = 'Data Governance'  -- 🔧 replace with your set name
```
