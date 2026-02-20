-- ============================================================
-- ANOMALY DETECTION (DASHBOARD-READY)
-- ============================================================
-- Detects latency anomalies and error spikes using statistical baselines
-- and window functions from normalized_requests_view
--
-- Expected placeholders:
--   ${PROJECT_ID}
--   ${DATASET}
--
-- Outputs:
--   - table: dashboard_anomaly_latency_zscore
--   - table: dashboard_anomaly_error_spikes
--   - table: dashboard_endpoint_degradation_daily
-- ============================================================

-- ============================================================
-- TABLE: LATENCY ANOMALIES USING Z-SCORE (PER ENDPOINT)
-- ============================================================
-- Uses:
--   - AVG() OVER(...)
--   - STDDEV() OVER(...)
-- ============================================================

CREATE OR REPLACE TABLE `${PROJECT_ID}.${DATASET}.dashboard_anomaly_latency_zscore` AS
WITH stats AS (
    SELECT
        event_timestamp,
        api_path,
        latency_ms,
        AVG(latency_ms) OVER (
            PARTITION BY api_path
        ) AS avg_latency_ms,
        STDDEV(latency_ms) OVER (
            PARTITION BY api_path
        ) AS std_latency_ms
    FROM `${PROJECT_ID}.${DATASET}.normalized_requests_view`
),
scored AS (
    SELECT
        event_timestamp,
        api_path,
        latency_ms,
        avg_latency_ms,
        std_latency_ms,
        SAFE_DIVIDE((latency_ms - avg_latency_ms), NULLIF(std_latency_ms, 0)) AS z_score
    FROM stats
)
SELECT
    event_timestamp,
    api_path,
    latency_ms,
    avg_latency_ms,
    std_latency_ms,
    z_score
FROM scored
WHERE ABS(z_score) >= 3;


-- ============================================================
-- TABLE: ERROR SPIKES VS BASELINE (ROLLING VS 24H BASELINE)
-- ============================================================
-- Uses:
--   - SUM() OVER(...) and COUNT() OVER(...) rolling windows
-- ============================================================

CREATE OR REPLACE TABLE `${PROJECT_ID}.${DATASET}.dashboard_anomaly_error_spikes` AS
WITH base AS (
    SELECT
        event_timestamp,
        api_path,
        CASE WHEN status_code >= 500 THEN 1 ELSE 0 END AS is_5xx
    FROM `${PROJECT_ID}.${DATASET}.normalized_requests_view`
),
windowed AS (
    SELECT
        event_timestamp,
        api_path,

        -- Rolling 1 hour
        SUM(is_5xx) OVER (
            PARTITION BY api_path
            ORDER BY event_timestamp
            RANGE BETWEEN INTERVAL 1 HOUR PRECEDING AND CURRENT ROW
        ) AS rolling_5xx_1h,

        COUNT(*) OVER (
            PARTITION BY api_path
            ORDER BY event_timestamp
            RANGE BETWEEN INTERVAL 1 HOUR PRECEDING AND CURRENT ROW
        ) AS rolling_total_1h,

        -- Rolling 24 hours baseline
        SUM(is_5xx) OVER (
            PARTITION BY api_path
            ORDER BY event_timestamp
            RANGE BETWEEN INTERVAL 24 HOUR PRECEDING AND CURRENT ROW
        ) AS rolling_5xx_24h,

        COUNT(*) OVER (
            PARTITION BY api_path
            ORDER BY event_timestamp
            RANGE BETWEEN INTERVAL 24 HOUR PRECEDING AND CURRENT ROW
        ) AS rolling_total_24h

    FROM base
),
rates AS (
    SELECT
        event_timestamp,
        api_path,
        SAFE_DIVIDE(rolling_5xx_1h, rolling_total_1h) AS rate_5xx_1h,
        SAFE_DIVIDE(rolling_5xx_24h, rolling_total_24h) AS rate_5xx_24h
    FROM windowed
),
alerts AS (
    SELECT
        event_timestamp,
        api_path,
        rate_5xx_1h,
        rate_5xx_24h,
        (rate_5xx_1h - rate_5xx_24h) AS rate_diff,
        CASE
            WHEN rate_5xx_1h >= 0.10 AND (rate_5xx_1h - rate_5xx_24h) >= 0.05 THEN 1
            ELSE 0
        END AS is_spike
    FROM rates
)
SELECT
    event_timestamp,
    api_path,
    rate_5xx_1h,
    rate_5xx_24h,
    rate_diff
FROM alerts
WHERE is_spike = 1;


-- ============================================================
-- TABLE: ENDPOINT DEGRADATION DAILY (P95 TREND USING LAG)
-- ============================================================
-- Uses:
--   - APPROX_QUANTILES()
--   - LAG()
-- ============================================================

CREATE OR REPLACE TABLE `${PROJECT_ID}.${DATASET}.dashboard_endpoint_degradation_daily` AS
WITH daily AS (
    SELECT
        DATE(event_timestamp) AS day,
        api_path,
        APPROX_QUANTILES(latency_ms, 100)[OFFSET(95)] AS p95_latency_ms,
        COUNT(*) AS total_requests
    FROM `${PROJECT_ID}.${DATASET}.normalized_requests_view`
    GROUP BY day, api_path
),
trend AS (
    SELECT
        *,
        LAG(p95_latency_ms) OVER (
            PARTITION BY api_path
            ORDER BY day
        ) AS prev_p95_latency_ms
    FROM daily
),
scored AS (
    SELECT
        day,
        api_path,
        total_requests,
        p95_latency_ms,
        prev_p95_latency_ms,
        SAFE_DIVIDE((p95_latency_ms - prev_p95_latency_ms), NULLIF(prev_p95_latency_ms, 0)) AS p95_change_ratio
    FROM trend
)
SELECT
    *
FROM scored
WHERE prev_p95_latency_ms IS NOT NULL
  AND p95_change_ratio >= 0.20
  AND total_requests >= 50;
