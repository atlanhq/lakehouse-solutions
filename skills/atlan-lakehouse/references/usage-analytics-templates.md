# Usage Analytics SQL Templates

All templates use `{{PLACEHOLDER}}` parameters. See the [SQL Template Reference](../SKILL.md#sql-template-reference) in the main skill file for parameter definitions.

All templates are written in Snowflake SQL as the canonical dialect. **Agents should adapt syntax to the target engine on the fly.**

> **Important:** Before using these templates, read the [Usage Analytics Conventions](usage-analytics-conventions.md) for critical information on domain handling, identity, noise filtering, session derivation, timezone, and feature area mapping.

---

## 0. Schema Profiling

### Table Profiler

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

### Discover Events

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

### Discover Pages

```sql
SELECT p.name AS page_name, COUNT(*) AS total_views, COUNT(DISTINCT p.user_id) AS unique_users,
    COUNT(DISTINCT p.domain) AS domains_using, MIN(DATE(p.TIMESTAMP)) AS first_seen, MAX(DATE(p.TIMESTAMP)) AS last_seen
FROM {{DATABASE}}.{{SCHEMA}}.PAGES p
WHERE p.name IS NOT NULL AND p.TIMESTAMP >= DATEADD('month', -3, CURRENT_TIMESTAMP())
GROUP BY p.name ORDER BY total_views DESC;
```

---

## 1. Active Users

### MAU by Domain (with month-over-month delta)

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

### DAU by Domain

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

### WAU by Domain

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

### Stickiness Ratio (DAU/MAU)

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

### User Roster by Domain

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

## 2. Feature Adoption

### Top Pages by Domain

```sql
SELECT p.domain, p.name AS page_name, p.tab, COUNT(*) AS page_views,
    COUNT(DISTINCT p.user_id) AS unique_users,
    ROUND(COUNT(*) / NULLIF(COUNT(DISTINCT p.user_id), 0), 1) AS views_per_user
FROM {{DATABASE}}.{{SCHEMA}}.PAGES p
WHERE p.TIMESTAMP >= {{START_DATE}} AND p.name IS NOT NULL AND p.domain = {{DOMAIN}}
GROUP BY p.domain, p.name, p.tab ORDER BY page_views DESC LIMIT 50;
```

### Top Events by Domain

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

### Feature Adoption Matrix

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

### Feature Trend Weekly

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

### Feature Engagement Quadrant

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

### Connector Usage

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

## 3. Engagement Depth

### Session Duration (Monthly)

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

### Power Users

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

### Engagement Tiers

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

### Average Pageviews per User Daily

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

## 4. Retention

### Monthly Retention Cohort

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

### Activation Funnel

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

### Churned Users

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

### Reactivated Users

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

### Aggregate Retention Rate (Weekly)

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

## 5. Customer Health

### Customer Health Scorecard

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

### License Utilization

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

### Role Distribution

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
