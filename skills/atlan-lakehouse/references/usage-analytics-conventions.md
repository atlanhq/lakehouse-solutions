# Usage Analytics Conventions

These conventions are critical for correct results across all usage analytics queries. All templates query the `USAGE_ANALYTICS` namespace using `{{DATABASE}}.{{SCHEMA}}.TABLE` references where `{{SCHEMA}}` is `USAGE_ANALYTICS` (or whatever the customer's schema is named).

## Domain

Your domain is your Atlan tenant hostname (e.g., `acme.atlan.com`). For Snowflake/Databricks, ask the user for their domain. For PyIceberg, derive it from the tenant name: `{ATLAN_TENANT}.atlan.com`.

Always derive domain from `PAGES.domain` (100% populated). The `TRACKS` table has no reliable domain column. For TRACKS, build a user-to-domain lookup:
```sql
-- Standard user_domains CTE (used in most queries)
WITH user_domains AS (
    SELECT user_id, MAX(domain) AS domain
    FROM {{DATABASE}}.{{SCHEMA}}.PAGES
    WHERE domain IS NOT NULL
    GROUP BY user_id
)
```

## Identity

Use `user_id` (UUID) as the primary key, not email. Most active user_ids (~98%) have no matching `USERS` record. Always LEFT JOIN to USERS for optional enrichment.

## Noise Filtering

Exclude these known noise events from all TRACKS queries:
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

## Sessions

No session ID column is populated in the data. Derive sessions from 30-minute inactivity gaps using this pattern:
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

## Timezone

All timestamps are stored in UTC. Convert to the user's local timezone as needed: `CONVERT_TIMEZONE('UTC', '<timezone>', TIMESTAMP)`.

## Feature Area Mapping

Map raw page names and event prefixes to logical feature areas:
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
