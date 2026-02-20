-- ============================================================
-- SLA METRICS (DASHBOARD-READY)
-- ============================================================
-- SLA compliance metrics by endpoint and over time:
--   - SLA thresholds: <500ms, <1s, <2s
--   - hourly and daily compliance
--   - rolling SLA drift using window functions
--
-- Expected placeholders:
--   ${PROJECT_ID}
--   ${DATASET}
--
-- Outputs:
--   - view:   sla_kpis_view
--   - table:  dashboard_sla_hourly
--   - table:  dashboard_sla_daily
--   - table:  dashboard_sla_rolling_drift
-- ============================================================

-- ============================================================
-- VIEW: SLA KPIS PER ENDPOINT (GLOBAL)
-- ============================================================

CREATE OR REPLACE VIEW `${PROJECT_ID}.${DATASET}.sla_kpis_view` AS
SELECT
    api_path,
    COUNT(*) AS total_requests,
    SAFE_DIVIDE(COUNTIF(latency_ms < 500), COUNT(*)) AS sla_lt_500ms_rate,
    SAFE_DIVIDE(COUNTIF(latency_ms < 1000), COUNT(*)) AS sla_lt_1s_rate,
    SAFE_DIVIDE(COUNTIF(latency_ms < 2000), COUNT(*)) AS sla_lt_2s_rate,
    APPROX_QUANTILES(latency_ms, 100)[OFFSET(95)] AS p95_latency_ms,
    APPROX_QUANTILES(latency_ms, 100)[OFFSET(99)] AS p99_latency_ms
FROM `${PROJECT_ID}.${DATASET}.normalized_requests_view`
GROUP BY api_path;


-- ============================================================
-- TABLE: HOURLY SLA COMPLIANCE
-- ============================================================

CREATE OR REPLACE TABLE `${PROJECT_ID}.${DATASET}.dashboard_sla_hourly` AS
SELECT
    TIMESTAMP_TRUNC(event_timestamp, HOUR) AS hour_ts,
    api_path,
    COUNT(*) AS total_requests,
    SAFE_DIVIDE(COUNTIF(latency_ms < 500), COUNT(*)) AS sla_lt_500ms_rate,
    SAFE_DIVIDE(COUNTIF(latency_ms < 1000), COUNT(*)) AS sla_lt_1s_rate,
    SAFE_DIVIDE(COUNTIF(latency_ms < 2000), COUNT(*)) AS sla_lt_2s_rate,
    APPROX_QUANTILES(latency_ms, 100)[OFFSET(95)] AS p95_latency_ms
FROM `${PROJECT_ID}.${DATASET}.normalized_requests_view`
GROUP BY hour_ts, api_path;


-- ============================================================
-- TABLE: DAILY SLA COMPLIANCE
-- ============================================================

CREATE OR REPLACE TABLE `${PROJECT_ID}.${DATASET}.dashboard_sla_daily` AS
SELECT
    DATE(event_timestamp) AS day,
    api_path,
    COUNT(*) AS total_requests,
    SAFE_DIVIDE(COUNTIF(latency_ms < 500), COUNT(*)) AS sla_lt_500ms_rate,
    SAFE_DIVIDE(COUNTIF(latency_ms < 1000), COUNT(*)) AS sla_lt_1s_rate,
    SAFE_DIVIDE(COUNTIF(latency_ms < 2000), COUNT(*)) AS sla_lt_2s_rate,
    APPROX_QUANTILES(latency_ms, 100)[OFFSET(95)] AS p95_latency_ms
FROM `${PROJECT_ID}.${DATASET}.normalized_requests_view`
GROUP BY day, api_path;


-- ============================================================
-- TABLE: ROLLING SLA DRIFT (LAG + ROLLING WINDOWS)
-- ============================================================
-- Uses:
--   - SUM() OVER(...)
--   - COUNT() OVER(...)
--   - LAG()
-- ============================================================

CREATE OR REPLACE TABLE `${PROJECT_ID}.${DATASET}.dashboard_sla_rolling_drift` AS
WITH base AS (
    SELECT
        event_timestamp,
        api_path,
        CASE WHEN latency_ms < 500 THEN 1 ELSE 0 END AS is_sla_500ms
    FROM `${PROJECT_ID}.${DATASET}.normalized_requests_view`
),
rolling AS (
    SELECT
        event_timestamp,
        api_path,

        SUM(is_sla_500ms) OVER (
            PARTITION BY api_path
            ORDER BY event_timestamp
            RANGE BETWEEN INTERVAL 1 HOUR PRECEDING AND CURRENT ROW
        ) AS rolling_sla_ok_1h,

        COUNT(*) OVER (
            PARTITION BY api_path
            ORDER BY event_timestamp
            RANGE BETWEEN INTERVAL 1 HOUR PRECEDING AND CURRENT ROW
        ) AS rolling_total_1h

    FROM base
),
rates AS (
    SELECT
        event_timestamp,
        api_path,
        SAFE_DIVIDE(rolling_sla_ok_1h, rolling_total_1h) AS rolling_sla_rate_500ms_1h
    FROM rolling
),
trend AS (
    SELECT
        *,
        LAG(rolling_sla_rate_500ms_1h) OVER (
            PARTITION BY api_path
            ORDER BY event_timestamp
        ) AS prev_rolling_sla_rate_500ms_1h
    FROM rates
)
SELECT
    event_timestamp,
    api_path,
    rolling_sla_rate_500ms_1h,
    prev_rolling_sla_rate_500ms_1h,
    (rolling_sla_rate_500ms_1h - prev_rolling_sla_rate_500ms_1h) AS sla_rate_delta
FROM trend
WHERE prev_rolling_sla_rate_500ms_1h IS NOT NULL;
