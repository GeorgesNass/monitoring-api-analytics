-- ============================================================
-- ERROR METRICS (DASHBOARD-READY)
-- ============================================================
-- Builds error KPIs and rolling error rates from normalized_requests_view
--
-- Expected placeholders:
--   ${PROJECT_ID}
--   ${DATASET}
--
-- Outputs:
--   - view:   error_kpis_view
--   - table:  dashboard_errors_hourly
--   - table:  dashboard_errors_rolling
--   - table:  dashboard_top_error_endpoints_daily
-- ============================================================

-- ============================================================
-- VIEW: ERROR KPIS PER ENDPOINT (COUNTS + RATES)
-- ============================================================

CREATE OR REPLACE VIEW `${PROJECT_ID}.${DATASET}.error_kpis_view` AS
SELECT
    api_path,
    COUNT(*) AS total_requests,
    COUNTIF(status_code BETWEEN 400 AND 499) AS total_4xx,
    COUNTIF(status_code >= 500) AS total_5xx,
    SAFE_DIVIDE(COUNTIF(status_code BETWEEN 400 AND 499), COUNT(*)) AS rate_4xx,
    SAFE_DIVIDE(COUNTIF(status_code >= 500), COUNT(*)) AS rate_5xx
FROM `${PROJECT_ID}.${DATASET}.normalized_requests_view`
GROUP BY api_path;


-- ============================================================
-- TABLE: HOURLY ERROR METRICS (FOR TIME-SERIES PANELS)
-- ============================================================

CREATE OR REPLACE TABLE `${PROJECT_ID}.${DATASET}.dashboard_errors_hourly` AS
SELECT
    TIMESTAMP_TRUNC(event_timestamp, HOUR) AS hour_ts,
    api_path,
    COUNT(*) AS total_requests,
    COUNTIF(status_code BETWEEN 400 AND 499) AS total_4xx,
    COUNTIF(status_code >= 500) AS total_5xx,
    SAFE_DIVIDE(COUNTIF(status_code >= 500), COUNT(*)) AS rate_5xx,
    SAFE_DIVIDE(COUNTIF(status_code BETWEEN 400 AND 499), COUNT(*)) AS rate_4xx
FROM `${PROJECT_ID}.${DATASET}.normalized_requests_view`
GROUP BY hour_ts, api_path;


-- ============================================================
-- TABLE: ROLLING ERROR RATES USING WINDOW FUNCTIONS
-- ============================================================
-- Uses:
--   - SUM() OVER(...)
--   - COUNT() OVER(...)
--   - LAG()
--   - LEAD()
-- ============================================================

CREATE OR REPLACE TABLE `${PROJECT_ID}.${DATASET}.dashboard_errors_rolling` AS
WITH base AS (
    SELECT
        event_timestamp,
        api_path,
        status_code,
        CASE WHEN status_code >= 500 THEN 1 ELSE 0 END AS is_5xx,
        CASE WHEN status_code BETWEEN 400 AND 499 THEN 1 ELSE 0 END AS is_4xx
    FROM `${PROJECT_ID}.${DATASET}.normalized_requests_view`
),
rolling AS (
    SELECT
        event_timestamp,
        api_path,

        -- Rolling 1 hour window
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

        -- Rolling 24 hour window
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

        rolling_5xx_1h,
        rolling_total_1h,
        SAFE_DIVIDE(rolling_5xx_1h, rolling_total_1h) AS rolling_rate_5xx_1h,

        rolling_5xx_24h,
        rolling_total_24h,
        SAFE_DIVIDE(rolling_5xx_24h, rolling_total_24h) AS rolling_rate_5xx_24h
    FROM rolling
),
trend AS (
    SELECT
        *,
        LAG(rolling_rate_5xx_1h) OVER (
            PARTITION BY api_path
            ORDER BY event_timestamp
        ) AS prev_rate_5xx_1h,
        LEAD(rolling_rate_5xx_1h) OVER (
            PARTITION BY api_path
            ORDER BY event_timestamp
        ) AS next_rate_5xx_1h
    FROM rates
)
SELECT
    event_timestamp,
    api_path,
    rolling_5xx_1h,
    rolling_total_1h,
    rolling_rate_5xx_1h,
    prev_rate_5xx_1h,
    next_rate_5xx_1h,
    rolling_5xx_24h,
    rolling_total_24h,
    rolling_rate_5xx_24h
FROM trend;


-- ============================================================
-- TABLE: TOP ERROR ENDPOINTS PER DAY WITH RANKING
-- ============================================================
-- Uses:
--   - ROW_NUMBER()
--   - RANK()
-- ============================================================

CREATE OR REPLACE TABLE `${PROJECT_ID}.${DATASET}.dashboard_top_error_endpoints_daily` AS
WITH daily AS (
    SELECT
        DATE(event_timestamp) AS day,
        api_path,
        COUNT(*) AS total_requests,
        COUNTIF(status_code >= 500) AS total_5xx,
        SAFE_DIVIDE(COUNTIF(status_code >= 500), COUNT(*)) AS rate_5xx
    FROM `${PROJECT_ID}.${DATASET}.normalized_requests_view`
    GROUP BY day, api_path
),
ranked AS (
    SELECT
        *,
        RANK() OVER (
            PARTITION BY day
            ORDER BY total_5xx DESC
        ) AS rank_by_5xx,
        ROW_NUMBER() OVER (
            PARTITION BY day
            ORDER BY rate_5xx DESC
        ) AS rownum_by_rate
    FROM daily
)
SELECT
    day,
    api_path,
    total_requests,
    total_5xx,
    rate_5xx,
    rank_by_5xx,
    rownum_by_rate
FROM ranked
WHERE rank_by_5xx <= 20 OR rownum_by_rate <= 20;
