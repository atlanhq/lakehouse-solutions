# Lineage SQL Templates

All templates use `{{PLACEHOLDER}}` parameters. See the [SQL Template Reference](../SKILL.md#sql-template-reference) in the main skill file for parameter definitions.

All templates are written in Snowflake SQL as the canonical dialect unless otherwise noted. **Agents should adapt syntax to the target engine on the fly.**

> **Important — LINEAGE table:** These templates reference a `LINEAGE` table that is **not** part of the native GOLD namespace. It must be created separately in your warehouse. See [Set up lineage tables](https://docs.atlan.com/platform/lakehouse/references/set-up-lineage-tables). The `LINEAGE` table has columns: `start_guid`, `start_name`, `start_type`, `related_guid`, `related_name`, `related_type`, `direction` (`UPSTREAM` or `DOWNSTREAM`), `level` (hop depth, 1 = direct).

---

## 1. Assets Without Lineage

Find orphaned or unused assets — active tables/views with no lineage that haven't been updated in 90+ days — for cleanup prioritization.

```sql
-- ASSETS WITHOUT LINEAGE: Find Orphaned/Unused Assets for Cleanup
-- gold.assets: Lakehouse catalog (GOLD namespace)
SELECT
    a.guid,
    a.asset_name,
    a.asset_type,
    a.asset_qualified_name,
    a.connector_name,
    a.description,
    a.certificate_status,
    a.status,
    a.owner_users,
    a.tags,
    a.has_lineage,
    TO_TIMESTAMP_LTZ(a.created_at / 1000) AS CREATED_DATE,
    TO_TIMESTAMP_LTZ(a.updated_at / 1000) AS LAST_UPDATED,
    DATEDIFF(day, TO_TIMESTAMP_LTZ(a.created_at / 1000), CURRENT_TIMESTAMP()) AS DAYS_SINCE_CREATION,
    DATEDIFF(day, TO_TIMESTAMP_LTZ(a.updated_at / 1000), CURRENT_TIMESTAMP()) AS DAYS_SINCE_UPDATE,
    CASE
        WHEN a.certificate_status = 'DEPRECATED'
        THEN '🔴 SAFE TO DELETE - Already deprecated'
        WHEN DATEDIFF(day, TO_TIMESTAMP_LTZ(a.updated_at / 1000), CURRENT_TIMESTAMP()) > 180
             AND a.status = 'ACTIVE'
        THEN '🟡 REVIEW FOR DELETION - No activity in 6+ months'
        WHEN DATEDIFF(day, TO_TIMESTAMP_LTZ(a.created_at / 1000), CURRENT_TIMESTAMP()) <= 7
        THEN '🟢 KEEP - Recently created, may not be connected yet'
        WHEN (a.owner_users IS NULL)
        THEN '🟡 INVESTIGATE - No owner, likely test/temp asset'
        ELSE '🟢 REVIEW - May be intentionally standalone'
    END AS CLEANUP_RECOMMENDATION
FROM {{DATABASE}}.GOLD.ASSETS a
WHERE
    a.has_lineage = FALSE
    AND a.asset_type IN ('Table', 'View', 'MaterializedView')
    AND a.status = 'ACTIVE'
    AND a.connector_name IN ('snowflake', 'redshift', 'bigquery', 'databricks')
    AND DATEDIFF(day, TO_TIMESTAMP_LTZ(a.updated_at / 1000), CURRENT_TIMESTAMP()) > 90
ORDER BY DAYS_SINCE_UPDATE DESC, a.asset_name;
```

**BigQuery variant** — replace timestamp expressions:
- `TO_TIMESTAMP_LTZ(a.created_at / 1000)` → `TIMESTAMP_MILLIS(CAST(a.created_at AS INT64))`
- `DATEDIFF(day, ..., CURRENT_TIMESTAMP())` → `TIMESTAMP_DIFF(CURRENT_TIMESTAMP(), ..., DAY)`

---

## 2. Circular Dependencies

Detect assets that appear in their own lineage path — self-referencing patterns that may indicate recursive queries or design issues.

```sql
-- CIRCULAR DEPENDENCIES: Find Assets with Lineage to Themselves
-- gold.assets: Lakehouse catalog (GOLD namespace)
-- LINEAGE: Customer-managed table
SELECT
    a.guid AS ASSET_GUID,
    a.asset_name,
    a.asset_type,
    a.asset_qualified_name AS ASSET_PATH,
    a.connector_name,
    a.owner_users,
    a.certificate_status,
    TO_TIMESTAMP_LTZ(a.updated_at / 1000) AS LAST_UPDATED,
    l.direction AS LINEAGE_DIRECTION,
    l.level AS CIRCULAR_PATH_LENGTH,
    l.related_name AS RELATED_ASSET_NAME,
    l.related_type AS RELATED_ASSET_TYPE,
    CASE
        WHEN l.level = 1 THEN '⚠️ DIRECT SELF-REFERENCE - Review immediately'
        WHEN l.level <= 3 THEN '⚠️ SHORT CIRCULAR PATH - May cause performance issues'
        ELSE '⚠️ LONG CIRCULAR PATH - Complex dependency chain'
    END AS CIRCULAR_DEPENDENCY_SEVERITY
FROM {{DATABASE}}.GOLD.ASSETS a
INNER JOIN LINEAGE l ON a.guid = l.start_guid
WHERE
    l.related_guid = a.guid  -- the lineage loops back to itself
    AND a.asset_type IN ('Table', 'View', 'MaterializedView', 'DbtModel')
    AND a.status = 'ACTIVE'
ORDER BY l.level ASC, a.asset_name;
```

---

## 3. Lineage Coverage Summary

Platform-wide metrics on what percentage of active assets have lineage.

```sql
-- LINEAGE COVERAGE SUMMARY: Overall Platform Metrics
SELECT
    COUNT(*) AS TOTAL_ASSETS,
    SUM(CASE WHEN has_lineage = TRUE THEN 1 ELSE 0 END) AS ASSETS_WITH_LINEAGE,
    SUM(CASE WHEN has_lineage = FALSE THEN 1 ELSE 0 END) AS ASSETS_WITHOUT_LINEAGE,
    ROUND(
        (SUM(CASE WHEN has_lineage = TRUE THEN 1 ELSE 0 END) * 100.0) / COUNT(*), 2
    ) AS LINEAGE_COVERAGE_PCT
FROM {{DATABASE}}.GOLD.ASSETS
WHERE status = 'ACTIVE';
```

## Lineage Coverage by Connector

Break down lineage coverage per data platform and asset type.

```sql
-- LINEAGE COVERAGE BY CONNECTOR: Platform-Specific Metrics
SELECT
    connector_name,
    asset_type,
    COUNT(*) AS TOTAL_ASSETS,
    SUM(CASE WHEN has_lineage = TRUE THEN 1 ELSE 0 END) AS WITH_LINEAGE,
    SUM(CASE WHEN has_lineage = FALSE THEN 1 ELSE 0 END) AS WITHOUT_LINEAGE,
    ROUND(
        (SUM(CASE WHEN has_lineage = TRUE THEN 1 ELSE 0 END) * 100.0) / COUNT(*), 2
    ) AS LINEAGE_COVERAGE_PCT
FROM {{DATABASE}}.GOLD.ASSETS
WHERE
    status = 'ACTIVE'
    AND NULLIF(connector_name, '') IS NOT NULL
GROUP BY connector_name, asset_type
ORDER BY connector_name, LINEAGE_COVERAGE_PCT DESC, TOTAL_ASSETS DESC;
```

---

## 4. Identify Lineage Hubs (Most Connected Assets)

Find the most critical tables/views — those with the highest number of direct upstream and downstream connections. Use for blast-radius assessment.

```sql
-- MOST CONNECTED ASSETS: Identify Critical Lineage Hubs
-- Snowflake / Databricks
SELECT
    a.asset_name,
    a.asset_type,
    a.asset_qualified_name,
    a.connector_name,
    a.certificate_status,
    a.owner_users,
    TO_TIMESTAMP_LTZ(a.updated_at / 1000) AS LAST_UPDATED,
    DATEDIFF(day, TO_TIMESTAMP_LTZ(a.updated_at / 1000), CURRENT_TIMESTAMP()) AS DAYS_SINCE_UPDATE,
    COUNT(CASE WHEN l.direction = 'UPSTREAM' AND l.level = 1 THEN 1 END) AS UPSTREAM_COUNT,
    COUNT(CASE WHEN l.direction = 'DOWNSTREAM' AND l.level = 1 THEN 1 END) AS DOWNSTREAM_COUNT,
    COUNT(CASE WHEN l.level = 1 THEN 1 END) AS TOTAL_CONNECTIONS,
    CASE
        WHEN COUNT(CASE WHEN l.direction = 'DOWNSTREAM' AND l.level = 1 THEN 1 END) >= 20
        THEN '🔴 CRITICAL - 20+ downstream dependencies'
        WHEN COUNT(CASE WHEN l.direction = 'DOWNSTREAM' AND l.level = 1 THEN 1 END) >= 10
        THEN '🟠 HIGH IMPACT - 10-19 downstream dependencies'
        WHEN COUNT(CASE WHEN l.direction = 'DOWNSTREAM' AND l.level = 1 THEN 1 END) >= 5
        THEN '🟡 MODERATE IMPACT - 5-9 downstream dependencies'
        ELSE '🟢 LOW IMPACT - <5 downstream dependencies'
    END AS CRITICALITY_LEVEL
FROM {{DATABASE}}.GOLD.ASSETS a
LEFT JOIN LINEAGE l ON a.guid = l.start_guid
WHERE
    a.has_lineage = TRUE
    AND a.asset_type IN ('Table', 'View', 'MaterializedView')
    AND a.status = 'ACTIVE'
GROUP BY
    a.asset_name, a.asset_type, a.asset_qualified_name,
    a.connector_name, a.certificate_status, a.owner_users, a.updated_at
HAVING COUNT(CASE WHEN l.level = 1 THEN 1 END) > 0
ORDER BY TOTAL_CONNECTIONS DESC;
```

**BigQuery variant** — replace:
- `TO_TIMESTAMP_LTZ(a.updated_at / 1000)` → `TIMESTAMP_MILLIS(a.updated_at)`
- `DATEDIFF(day, ..., CURRENT_TIMESTAMP())` → `TIMESTAMP_DIFF(CURRENT_TIMESTAMP(), ..., DAY)`

---

## 5. Full Lineage Export

Export the complete lineage graph for all recently-updated assets. Useful as a foundation for impact analysis, root cause analysis, and post-deployment validation.

```sql
-- FULL LINEAGE EXPORT
-- Snowflake / Databricks
SELECT
    a.guid,
    a.asset_name,
    a.asset_qualified_name,
    a.asset_type,
    TO_TIMESTAMP_LTZ(a.updated_at / 1000) AS LAST_UPDATED,
    DATEDIFF(day, TO_TIMESTAMP_LTZ(a.updated_at / 1000), CURRENT_TIMESTAMP()) AS DAYS_SINCE_UPDATE,
    l.start_name,
    l.start_type,
    l.related_name,
    l.related_type,
    l.direction,
    l.level
FROM {{DATABASE}}.GOLD.ASSETS a
INNER JOIN LINEAGE l ON a.guid = l.start_guid
WHERE
    a.has_lineage
    AND a.connector_name IN ('snowflake', 'redshift', 'bigquery')
    AND DATEDIFF(day, TO_TIMESTAMP_LTZ(a.updated_at / 1000), CURRENT_TIMESTAMP()) <= 2
GROUP BY
    a.guid, a.asset_name, a.asset_qualified_name, a.asset_type, a.updated_at,
    l.start_name, l.start_type, l.related_name, l.related_type, l.direction, l.level
ORDER BY a.updated_at DESC, a.asset_qualified_name, l.direction, l.level
LIMIT 200;
```

**BigQuery variant:**
```sql
-- FULL LINEAGE EXPORT — BigQuery
SELECT
    a.guid,
    a.asset_name,
    a.asset_qualified_name,
    a.asset_type,
    TIMESTAMP_MILLIS(CAST(a.updated_at AS INT64)) AS LAST_UPDATED,
    TIMESTAMP_DIFF(CURRENT_TIMESTAMP(), TIMESTAMP_MILLIS(CAST(a.updated_at AS INT64)), DAY) AS DAYS_SINCE_UPDATE,
    l.start_name,
    l.start_type,
    l.related_name,
    l.related_type,
    l.direction,
    l.level
FROM {{DATABASE}}.GOLD.ASSETS a
JOIN LINEAGE l ON a.guid = l.start_guid
WHERE
    a.has_lineage
    AND a.connector_name IN ('snowflake', 'redshift', 'bigquery')
    AND TIMESTAMP_DIFF(CURRENT_TIMESTAMP(), TIMESTAMP_MILLIS(CAST(a.updated_at AS INT64)), DAY) <= 2
ORDER BY a.updated_at DESC, a.asset_qualified_name, l.direction, l.level
LIMIT 200;
```

---

## 6. Lineage Impact Analysis

Before making changes to a table — renaming columns, deprecating assets, changing types, migrating schemas — find all downstream dependencies and their owners to notify.

```sql
-- IMPACT ANALYSIS: Find All Downstream Dependencies Before Making Changes
-- Snowflake
SELECT
    a.asset_name AS SOURCE_ASSET,
    a.asset_type AS SOURCE_TYPE,
    a.asset_qualified_name AS SOURCE_PATH,
    a.certificate_status AS SOURCE_CERT,
    TO_TIMESTAMP_LTZ(a.updated_at / 1000) AS SOURCE_LAST_UPDATED,
    l.related_name AS IMPACTED_ASSET,
    l.related_type AS IMPACTED_TYPE,
    l.level AS DEPENDENCY_DEPTH,
    ra.asset_qualified_name AS IMPACTED_PATH,
    ra.owner_users AS IMPACTED_OWNERS,
    ra.connector_name AS IMPACTED_SYSTEM,
    ra.certificate_status AS IMPACTED_CERT_STATUS,
    TO_TIMESTAMP_LTZ(ra.updated_at / 1000) AS IMPACTED_LAST_UPDATED,
    CASE
        WHEN l.level = 1 THEN 'DIRECT DEPENDENCY - HIGH PRIORITY'
        WHEN l.level = 2 THEN 'SECONDARY DEPENDENCY - MEDIUM PRIORITY'
        ELSE 'INDIRECT DEPENDENCY - LOW PRIORITY'
    END AS NOTIFICATION_PRIORITY
FROM {{DATABASE}}.GOLD.ASSETS a
INNER JOIN LINEAGE l ON a.guid = l.start_guid
LEFT JOIN {{DATABASE}}.GOLD.ASSETS ra ON l.related_guid = ra.guid
WHERE
    -- 🔧 CUSTOMIZE: replace with the qualified name or GUID of the asset you're changing
    a.asset_qualified_name = 'default/snowflake/123/ANALYTICS/DIM_CUSTOMERS'
    AND a.asset_type = 'Table'
    AND l.direction = 'DOWNSTREAM'
ORDER BY l.level ASC, l.related_type, l.related_name;
```

**BigQuery variant** — filter by GUID and replace timestamp expressions:
```sql
-- IMPACT ANALYSIS — BigQuery
SELECT
    a.asset_name AS SOURCE_ASSET,
    a.asset_type AS SOURCE_TYPE,
    a.asset_qualified_name AS SOURCE_PATH,
    a.certificate_status AS SOURCE_CERT,
    TIMESTAMP_MILLIS(a.updated_at) AS SOURCE_LAST_UPDATED,
    l.related_name AS IMPACTED_ASSET,
    l.related_type AS IMPACTED_TYPE,
    l.level AS DEPENDENCY_DEPTH,
    ra.asset_qualified_name AS IMPACTED_PATH,
    ra.owner_users AS IMPACTED_OWNERS,
    ra.connector_name AS IMPACTED_SYSTEM,
    ra.certificate_status AS IMPACTED_CERT_STATUS,
    TIMESTAMP_MILLIS(ra.updated_at) AS IMPACTED_LAST_UPDATED,
    CASE
        WHEN l.level = 1 THEN 'DIRECT DEPENDENCY - HIGH PRIORITY'
        WHEN l.level = 2 THEN 'SECONDARY DEPENDENCY - MEDIUM PRIORITY'
        ELSE 'INDIRECT DEPENDENCY - LOW PRIORITY'
    END AS NOTIFICATION_PRIORITY
FROM {{DATABASE}}.GOLD.ASSETS a
INNER JOIN LINEAGE l ON a.guid = l.start_guid
LEFT JOIN {{DATABASE}}.GOLD.ASSETS ra ON l.related_guid = ra.guid
WHERE
    a.guid = 'ff0f00d5-dde5-4cf9-b199-2ab09a961bd2'  -- 🔧 replace with asset GUID
    AND l.direction = 'DOWNSTREAM'
ORDER BY l.level ASC, l.related_type, l.related_name;
```

---

## 7. Lineage Root Cause Analysis

Trace data quality issues backward through the pipeline to identify upstream sources contributing to the problem. Results are ordered by hop distance (closest sources first).

```sql
-- ROOT CAUSE ANALYSIS: Trace Upstream to Find Problem Sources
-- Snowflake / Databricks / BigQuery (same query, adjust timestamp functions)
SELECT
    a.asset_name AS PROBLEM_ASSET,
    a.asset_type AS PROBLEM_ASSET_TYPE,
    a.asset_qualified_name AS PROBLEM_ASSET_PATH,
    a.owner_users AS PROBLEM_ASSET_OWNERS,
    l.related_name AS UPSTREAM_ASSET,
    l.related_type AS UPSTREAM_ASSET_TYPE,
    l.level AS HOPS_FROM_PROBLEM,
    ra.asset_qualified_name AS UPSTREAM_QUALIFIED_NAME,
    ra.owner_users AS UPSTREAM_OWNERS,
    ra.certificate_status AS UPSTREAM_CERT_STATUS,
    ra.connector_name AS UPSTREAM_CONNECTOR
FROM {{DATABASE}}.GOLD.ASSETS a
INNER JOIN LINEAGE l ON a.guid = l.start_guid
LEFT JOIN {{DATABASE}}.GOLD.ASSETS ra ON l.related_guid = ra.guid
WHERE
    a.guid = 'ff0f00d5-dde5-4cf9-b199-2ab09a961bd2'  -- 🔧 replace with the problem asset's GUID
    AND l.direction = 'UPSTREAM'
    AND l.level <= 5
    AND l.related_type IN ('Table', 'View', 'MaterializedView', 'Column')
ORDER BY l.level ASC, l.related_name;
```

---

## 8. Downstream Impacted Dashboards

Before modifying a source table or view, identify every BI dashboard downstream — across Tableau, Power BI, Looker, Thoughtspot, MicroStrategy, Qlik, and Sigma — along with impact severity and recommended actions.

```sql
-- DOWNSTREAM DASHBOARD IMPACT: Find All BI Dashboards Affected by Data Changes
-- Snowflake
SELECT
    a.asset_name AS SOURCE_DATA_ASSET,
    a.asset_type AS SOURCE_TYPE,
    a.asset_qualified_name AS SOURCE_PATH,
    TO_TIMESTAMP_LTZ(a.updated_at / 1000) AS SOURCE_LAST_UPDATED,
    l.level AS HOPS_TO_DASHBOARD,
    l.related_name AS IMPACTED_DASHBOARD_NAME,
    l.related_type AS IMPACTED_DASHBOARD_TYPE,
    ra.asset_qualified_name AS IMPACTED_DASHBOARD_PATH,
    ra.owner_users AS DASHBOARD_OWNERS,
    ra.description AS DASHBOARD_DESCRIPTION,
    TO_TIMESTAMP_LTZ(ra.updated_at / 1000) AS DASHBOARD_LAST_UPDATED,
    DATEDIFF(day, TO_TIMESTAMP_LTZ(ra.updated_at / 1000), CURRENT_TIMESTAMP()) AS DAYS_SINCE_DASHBOARD_UPDATE,
    CASE
        WHEN l.level = 1 THEN '🔴 DIRECT - Will break immediately'
        WHEN l.level = 2 THEN '🟡 INDIRECT - May break through intermediate assets'
        ELSE '🟢 DISTANT - Lower risk but monitor'
    END AS IMPACT_SEVERITY,
    CASE
        WHEN l.level = 1 THEN 'Update dashboard queries before deployment'
        WHEN l.level = 2 THEN 'Test dashboard after deployment'
        ELSE 'Monitor dashboard post-deployment'
    END AS RECOMMENDED_ACTION
FROM {{DATABASE}}.GOLD.ASSETS a
INNER JOIN LINEAGE l ON a.guid = l.start_guid
LEFT JOIN {{DATABASE}}.GOLD.ASSETS ra ON l.related_guid = ra.guid
WHERE
    a.guid = 'ff0f00d5-dde5-4cf9-b199-2ab09a961bd2'  -- 🔧 replace with source table GUID
    AND l.direction = 'DOWNSTREAM'
    AND l.related_type IN (
        'TableauDashboard', 'TableauWorkbook', 'TableauWorksheet',
        'PowerBIDashboard', 'PowerBIReport', 'PowerBIDataset',
        'LookerDashboard', 'LookerLook',
        'ThoughtspotLiveboard', 'ThoughtspotAnswer',
        'MicroStrategyDossier', 'QlikApp', 'SigmaWorkbook'
    )
    AND l.level <= 3
ORDER BY l.level ASC, l.related_type, l.related_name;
```

**Databricks / BigQuery** — same query; replace timestamp functions:
- Databricks: `from_unixtime(a.updated_at / 1000)`, `datediff(CURRENT_DATE(), ...)`
- BigQuery: `TIMESTAMP_MILLIS(a.updated_at)`, `TIMESTAMP_DIFF(CURRENT_TIMESTAMP(), ..., DAY)`

---

## 9. Tag Propagation Across Lineage

Measure how governance tags (PII, Confidential, Finance, etc.) flow from tagged upstream assets to all downstream consumers. Identify where tags are dropped, partially propagated, or replaced.

### Snowflake

```sql
-- TAG PROPAGATION ANALYSIS — Snowflake
WITH tagged_sources AS (
    SELECT
        guid, asset_name, asset_type, asset_qualified_name, connector_name,
        tags, certificate_status, owner_users,
        TO_TIMESTAMP_LTZ(updated_at / 1000) AS LAST_UPDATED
    FROM {{DATABASE}}.GOLD.ASSETS
    WHERE
        status = 'ACTIVE'
        AND connector_name = 'snowflake'  -- 🔧 CUSTOMIZE: your connector(s)
        AND tags IS NOT NULL
        AND ARRAY_SIZE(tags) > 0
        AND (
            ARRAY_CONTAINS('PII'::VARIANT, tags)
            OR ARRAY_CONTAINS('Confidential'::VARIANT, tags)
            OR ARRAY_CONTAINS('Finance'::VARIANT, tags)
            OR ARRAY_CONTAINS('Cost_center'::VARIANT, tags)
        )
),
downstream_lineage AS (
    SELECT
        ts.guid AS SOURCE_GUID, ts.asset_name AS SOURCE_NAME, ts.asset_type AS SOURCE_TYPE,
        ts.connector_name AS SOURCE_CONNECTOR, ts.tags AS SOURCE_TAGS,
        l.related_guid AS DOWNSTREAM_GUID, l.related_name AS DOWNSTREAM_NAME,
        l.related_type AS DOWNSTREAM_TYPE, l.level AS LINEAGE_DEPTH,
        ra.tags AS DOWNSTREAM_TAGS, ra.connector_name AS DOWNSTREAM_CONNECTOR
    FROM tagged_sources ts
    INNER JOIN LINEAGE l ON ts.guid = l.start_guid
    LEFT JOIN {{DATABASE}}.GOLD.ASSETS ra ON l.related_guid = ra.guid
    WHERE l.direction = 'DOWNSTREAM'
)
SELECT
    SOURCE_NAME, SOURCE_TYPE, SOURCE_CONNECTOR, SOURCE_TAGS,
    LINEAGE_DEPTH, DOWNSTREAM_NAME, DOWNSTREAM_TYPE, DOWNSTREAM_CONNECTOR, DOWNSTREAM_TAGS,
    CASE
        WHEN DOWNSTREAM_TAGS IS NULL OR ARRAY_SIZE(DOWNSTREAM_TAGS) = 0
        THEN '🔴 NO tags - Complete tag drop'
        WHEN ARRAYS_OVERLAP(SOURCE_TAGS, DOWNSTREAM_TAGS)
        THEN '🟢 tags PROPAGATED - Some or all tags present'
        ELSE '🟡 DIFFERENT tags - Tagged but not inherited from source'
    END AS TAG_PROPAGATION_STATUS,
    ARRAY_EXCEPT(SOURCE_TAGS, COALESCE(DOWNSTREAM_TAGS, ARRAY_CONSTRUCT())) AS DROPPED_TAGS,
    CASE
        WHEN DOWNSTREAM_TAGS IS NOT NULL AND ARRAY_SIZE(DOWNSTREAM_TAGS) > 0
        THEN ROUND(
            (ARRAY_SIZE(ARRAY_INTERSECTION(SOURCE_TAGS, DOWNSTREAM_TAGS)) * 100.0) / ARRAY_SIZE(SOURCE_TAGS), 2
        )
        ELSE 0
    END AS TAG_INHERITANCE_PCT
FROM downstream_lineage
ORDER BY SOURCE_NAME, LINEAGE_DEPTH, DOWNSTREAM_NAME;
```

### Databricks

```sql
-- TAG PROPAGATION ANALYSIS — Databricks
WITH tagged_sources AS (
    SELECT
        guid, asset_name, asset_type, asset_qualified_name, connector_name,
        tags, certificate_status, owner_users,
        from_unixtime(updated_at / 1000) AS LAST_UPDATED
    FROM {{DATABASE}}.GOLD.ASSETS
    WHERE
        status = 'ACTIVE'
        AND connector_name = 'snowflake'  -- 🔧 CUSTOMIZE
        AND tags IS NOT NULL
        AND size(tags) > 0
        AND (
            array_contains(tags, 'PII')
            OR array_contains(tags, 'Confidential')
            OR array_contains(tags, 'Finance')
            OR array_contains(tags, 'Cost_center')
        )
),
downstream_lineage AS (
    SELECT
        ts.guid AS SOURCE_GUID, ts.asset_name AS SOURCE_NAME, ts.asset_type AS SOURCE_TYPE,
        ts.connector_name AS SOURCE_CONNECTOR, ts.tags AS SOURCE_TAGS,
        ts.last_updated AS SOURCE_LAST_UPDATED,
        l.related_guid AS DOWNSTREAM_GUID, l.related_name AS DOWNSTREAM_NAME,
        l.related_type AS DOWNSTREAM_TYPE, l.level AS LINEAGE_DEPTH,
        ra.tags AS DOWNSTREAM_TAGS, ra.connector_name AS DOWNSTREAM_CONNECTOR,
        ra.asset_qualified_name AS DOWNSTREAM_QUALIFIED_NAME,
        ra.owner_users AS DOWNSTREAM_OWNERS, ra.certificate_status AS DOWNSTREAM_CERT,
        from_unixtime(ra.updated_at / 1000) AS DOWNSTREAM_LAST_UPDATED,
        datediff(CURRENT_DATE(), from_unixtime(ra.updated_at / 1000)) AS DOWNSTREAM_DAYS_SINCE_UPDATE
    FROM tagged_sources ts
    INNER JOIN LINEAGE l ON ts.guid = l.start_guid
    LEFT JOIN {{DATABASE}}.GOLD.ASSETS ra ON l.related_guid = ra.guid
    WHERE l.direction = 'DOWNSTREAM'
)
SELECT
    SOURCE_NAME, SOURCE_TYPE, SOURCE_CONNECTOR, SOURCE_TAGS, SOURCE_LAST_UPDATED,
    LINEAGE_DEPTH, DOWNSTREAM_NAME, DOWNSTREAM_TYPE, DOWNSTREAM_CONNECTOR,
    DOWNSTREAM_QUALIFIED_NAME, DOWNSTREAM_TAGS, DOWNSTREAM_OWNERS, DOWNSTREAM_CERT,
    DOWNSTREAM_LAST_UPDATED, DOWNSTREAM_DAYS_SINCE_UPDATE,
    CASE
        WHEN DOWNSTREAM_TAGS IS NULL OR size(DOWNSTREAM_TAGS) = 0
        THEN '🔴 NO tags - Complete tag drop'
        WHEN arrays_overlap(SOURCE_TAGS, DOWNSTREAM_TAGS)
        THEN '🟢 tags PROPAGATED - Some or all tags present'
        ELSE '🟡 DIFFERENT tags - Tagged but not inherited from source'
    END AS TAG_PROPAGATION_STATUS,
    array_except(SOURCE_TAGS, COALESCE(DOWNSTREAM_TAGS, array())) AS DROPPED_TAGS,
    array_except(COALESCE(DOWNSTREAM_TAGS, array()), SOURCE_TAGS) AS NEW_TAGS_ADDED,
    CASE
        WHEN DOWNSTREAM_TAGS IS NOT NULL AND size(DOWNSTREAM_TAGS) > 0
        THEN ROUND(
            (size(array_intersect(SOURCE_TAGS, DOWNSTREAM_TAGS)) * 100.0) / size(SOURCE_TAGS), 2
        )
        ELSE 0
    END AS TAG_INHERITANCE_PCT
FROM downstream_lineage
ORDER BY SOURCE_NAME, LINEAGE_DEPTH, DOWNSTREAM_NAME;
```

### BigQuery

```sql
-- TAG PROPAGATION ANALYSIS — BigQuery
WITH tagged_sources AS (
    SELECT
        guid, asset_name, asset_type, asset_qualified_name, connector_name,
        tags, certificate_status, owner_users
    FROM {{DATABASE}}.GOLD.ASSETS
    WHERE
        status = 'ACTIVE'
        AND connector_name = 'snowflake'  -- 🔧 CUSTOMIZE
        AND tags IS NOT NULL
        AND ARRAY_LENGTH(tags) > 0
        AND (
            'PII' IN UNNEST(tags)
            OR 'Confidential' IN UNNEST(tags)
            OR 'Finance' IN UNNEST(tags)
            OR 'Cost_center' IN UNNEST(tags)
        )
),
downstream_lineage AS (
    SELECT
        ts.guid AS SOURCE_GUID, ts.asset_name AS SOURCE_NAME, ts.asset_type AS SOURCE_TYPE,
        ts.connector_name AS SOURCE_CONNECTOR, ts.tags AS SOURCE_TAGS,
        l.related_guid AS DOWNSTREAM_GUID, l.related_name AS DOWNSTREAM_NAME,
        l.related_type AS DOWNSTREAM_TYPE, l.level AS LINEAGE_DEPTH,
        ra.tags AS DOWNSTREAM_TAGS, ra.connector_name AS DOWNSTREAM_CONNECTOR
    FROM tagged_sources ts
    INNER JOIN LINEAGE l ON ts.guid = l.start_guid
    LEFT JOIN {{DATABASE}}.GOLD.ASSETS ra ON l.related_guid = ra.guid
    WHERE l.direction = 'DOWNSTREAM'
)
SELECT
    SOURCE_NAME, SOURCE_TYPE, SOURCE_CONNECTOR, SOURCE_TAGS,
    LINEAGE_DEPTH, DOWNSTREAM_NAME, DOWNSTREAM_TYPE, DOWNSTREAM_CONNECTOR, DOWNSTREAM_TAGS,
    CASE
        WHEN DOWNSTREAM_TAGS IS NULL OR ARRAY_LENGTH(DOWNSTREAM_TAGS) = 0
        THEN '🔴 NO tags - Complete tag drop'
        WHEN EXISTS (
            SELECT 1 FROM UNNEST(SOURCE_TAGS) AS st WHERE st IN UNNEST(DOWNSTREAM_TAGS)
        )
        THEN '🟢 tags PROPAGATED - Some or all tags present'
        ELSE '🟡 DIFFERENT tags - Tagged but not inherited from source'
    END AS TAG_PROPAGATION_STATUS,
    ARRAY(
        SELECT st FROM UNNEST(SOURCE_TAGS) AS st
        WHERE st NOT IN (SELECT dt FROM UNNEST(IFNULL(DOWNSTREAM_TAGS, [])) AS dt)
    ) AS DROPPED_TAGS,
    CASE
        WHEN DOWNSTREAM_TAGS IS NOT NULL AND ARRAY_LENGTH(DOWNSTREAM_TAGS) > 0
        THEN ROUND(
            (ARRAY_LENGTH(ARRAY(
                SELECT st FROM UNNEST(SOURCE_TAGS) AS st
                WHERE st IN (SELECT dt FROM UNNEST(DOWNSTREAM_TAGS) AS dt)
            )) * 100.0) / ARRAY_LENGTH(SOURCE_TAGS), 2
        )
        ELSE 0
    END AS TAG_INHERITANCE_PCT
FROM downstream_lineage
ORDER BY SOURCE_NAME, LINEAGE_DEPTH, DOWNSTREAM_NAME;
```

### Common customization filters

Add to `tagged_sources` WHERE clause to narrow results:

```sql
-- Filter by multiple connectors
AND connector_name IN ('snowflake', 'bigquery', 'databricks')

-- Filter by certification status
AND certificate_status = 'VERIFIED'

-- Filter by specific asset types
AND asset_type IN ('Table', 'View', 'MaterializedView')

-- Filter by time window (last 90 days)
-- Snowflake: AND TO_TIMESTAMP_LTZ(updated_at / 1000) >= DATEADD(day, -90, CURRENT_DATE())
-- Databricks: AND FROM_UNIXTIME(updated_at / 1000) >= DATE_SUB(CURRENT_DATE(), 90)
-- BigQuery:   AND TIMESTAMP_MILLIS(updated_at) >= DATE_SUB(CURRENT_DATE(), INTERVAL 90 DAY)

-- Limit lineage depth (add to final SELECT WHERE clause)
WHERE LINEAGE_DEPTH <= 3
```
