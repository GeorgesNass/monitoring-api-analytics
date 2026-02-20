-- ============================================================
-- LATENCY METRICS (DASHBOARD-READY)
-- ============================================================
-- Builds latency KPIs and rankings from normalized_requests_view
--
-- Expected placeholders:
--   ${PROJECT_ID}
--   ${DATASET}
--
-- Outputs:
--   - view:   latency_kpis_view
--   - table:  dashboard_latency_hourly
--   - table:  dashboard_latency_endpoint_rank_daily
-- ============================================================

-- ============================================================
-- VIEW: LATENCY KPIS (P50/P95/P99 + AVG + MAX) PER ENDPOINT
-- ============================================================

CREATE OR REPLACE VIEW `${PROJECT_ID}.${DATASET}.latency_kpis_view` AS
SELECT
    api_path,
    COUNT(*) AS total_requests,
    AVG(latency_ms) AS avg_latency_ms,
    MAX(latency_ms) AS max_latency_ms,
    APPROX_QUANTILES(latency_ms, 100)[OFFSET(50)] AS p50_latency_ms,
    APPROX_QUANTILES(latency_ms, 100)[OFFSET(95)] AS p95_latency_ms,
    APPROX_QUANTILES(latency_ms, 100)[OFFSET(99)] AS p99_latency_ms
FROM `${PROJECT_ID}.${DATASET}.normalized_requests_view`
GROUP BY api_path;


-- ============================================================
-- TABLE: HOURLY LATENCY METRICS (FOR TIME-SERIES PANELS)
-- ============================================================

CREATE OR REPLACE TABLE `${PROJECT_ID}.${DATASET}.dashboard_latency_hourly` AS
SELECT
    TIMESTAMP_TRUNC(event_timestamp, HOUR) AS hour_ts,
    api_path,
    COUNT(*) AS total_requests,
    AVG(latency_ms) AS avg_latency_ms,
    APPROX_QUANTILES(latency_ms, 100)[OFFSET(50)] AS p50_latency_ms,
    APPROX_QUANTILES(latency_ms, 100)[OFFSET(95)] AS p95_latency_ms,
    APPROX_QUANTILES(latency_ms, 100)[OFFSET(99)] AS p99_latency_ms,
    MAX(latency_ms) AS max_latency_ms
FROM `${PROJECT_ID}.${DATASET}.normalized_requests_view`
GROUP BY hour_ts, api_path;


-- ============================================================
-- TABLE: ENDPOINT RANKING PER DAY USING WINDOW FUNCTIONS
-- ============================================================
-- Uses:
--   - RANK() OVER(...)
--   - DENSE_RANK() OVER(...)
--   - AVG() OVER(...)
--   - SUM() OVER(...)
-- ============================================================

CREATE OR REPLACE TABLE `${PROJECT_ID}.${DATASET}.dashboard_latency_endpoint_rank_daily` AS
WITH daily AS (
    SELECT
        DATE(event_timestamp) AS day,
        api_path,
        COUNT(*) AS total_requests,
        AVG(latency_ms) AS avg_latency_ms,
        APPROX_QUANTILES(latency_ms, 100)[OFFSET(95)] AS p95_latency_ms
    FROM `${PROJECT_ID}.${DATASET}.normalized_requests_view`
    GROUP BY day, api_path
),
ranked AS (
    SELECT
        *,
        RANK() OVER(
            PARTITION BY day
            ORDER BY p95_latency_ms DESC
        ) AS rank_p95_slowest,
        DENSE_RANK() OVER(
            PARTITION BY day
            ORDER BY avg_latency_ms DESC
        ) AS dense_rank_avg_slowest,
        AVG(avg_latency_ms) OVER(
            PARTITION BY day
        ) AS day_avg_latency_ms,
        SUM(total_requests) OVER(
            PARTITION BY day
        ) AS day_total_requests
    FROM daily
)
SELECT
    day,
    api_path,
    total_requests,
    avg_latency_ms,
    p95_latency_ms,
    day_avg_latency_ms,
    day_total_requests,
    rank_p95_slowest,
    dense_rank_avg_slowest
FROM ranked;
