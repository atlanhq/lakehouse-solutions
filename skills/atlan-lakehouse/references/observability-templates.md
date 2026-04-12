# Observability SQL Templates

These query the `OBSERVABILITY` namespace. The single table `JOB_METRICS` stores one row per job execution. The `custom_metrics` column is a JSON string whose schema varies by `job_name`.

> **Partition pruning:** `JOB_METRICS` is partitioned by month on `started_at`. Always include a time-range filter on `started_at` to avoid full table scans.

All templates use `{{PLACEHOLDER}}` parameters. See the [SQL Template Reference](../SKILL.md#sql-template-reference) in the main skill file for parameter definitions.

---

## DQ Score Over Time

Trends the `dq_score` from `AtlasDqOrchestrationWorkflow` runs by day. Use to monitor whether Lakehouse data quality is stable, improving, or degrading.

```sql
SELECT
    DATE(started_at)                            AS run_date,
    job_instance_id,
    PARSE_JSON(custom_metrics):dq_score::FLOAT  AS dq_score,
    PARSE_JSON(custom_metrics):total_typedefs::INT
                                                AS total_typedefs,
    PARSE_JSON(custom_metrics):total_mismatch_count::INT
                                                AS total_mismatches,
    DATEDIFF('second', started_at, completed_at)
                                                AS duration_seconds
FROM {{DATABASE}}.OBSERVABILITY.JOB_METRICS
WHERE job_name = 'AtlasDqOrchestrationWorkflow'
  AND started_at >= DATEADD('day', -30, CURRENT_TIMESTAMP())
ORDER BY run_date DESC;
```

## Job Success and Failure Rates

Counts job executions by status for each job type over a configurable window. Identifies jobs that are failing frequently.

```sql
SELECT
    job_name,
    COUNT(*)                                                AS total_runs,
    COUNT(CASE WHEN status_code = 200 THEN 1 END)          AS successes,
    COUNT(CASE WHEN status_code != 200 THEN 1 END)         AS failures,
    ROUND(
        100.0 * COUNT(CASE WHEN status_code = 200 THEN 1 END)
        / NULLIF(COUNT(*), 0), 2
    )                                                       AS success_rate_pct,
    MAX(CASE WHEN status_code != 200
             THEN error_message END)                        AS latest_error
FROM {{DATABASE}}.OBSERVABILITY.JOB_METRICS
WHERE started_at >= DATEADD('day', -7, CURRENT_TIMESTAMP())
GROUP BY job_name
ORDER BY failures DESC, total_runs DESC;
```

## Job Duration Analysis

Calculates duration statistics per job type and flags long-running executions. Use to detect performance regressions.

```sql
WITH job_durations AS (
    SELECT
        job_name,
        job_instance_id,
        DATE(started_at)                                AS run_date,
        DATEDIFF('second', started_at, completed_at)    AS duration_seconds
    FROM {{DATABASE}}.OBSERVABILITY.JOB_METRICS
    WHERE started_at >= DATEADD('day', -30, CURRENT_TIMESTAMP())
      AND completed_at IS NOT NULL
)
SELECT
    job_name,
    COUNT(*)                                            AS total_runs,
    ROUND(AVG(duration_seconds), 1)                     AS avg_duration_sec,
    ROUND(MEDIAN(duration_seconds), 1)                  AS p50_duration_sec,
    ROUND(PERCENTILE_CONT(0.95)
          WITHIN GROUP (ORDER BY duration_seconds), 1)  AS p95_duration_sec,
    MAX(duration_seconds)                               AS max_duration_sec
FROM job_durations
GROUP BY job_name
ORDER BY avg_duration_sec DESC;
```
