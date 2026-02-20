-- ============================================================
-- USER STATS (DASHBOARD-READY)
-- ============================================================
-- Builds user activity, auth errors, and rolling usage metrics
-- from normalized_requests_view
--
-- Expected placeholders:
--   ${PROJECT_ID}
--   ${DATASET}
--
-- Outputs:
--   - view:   user_kpis_view
--   - table:  dashboard_user_activity_daily
--   - table:  dashboard_user_auth_errors_daily
--   - table:  dashboard_user_rolling_activity
-- ============================================================

-- ============================================================
-- VIEW: USER KPIS (TOP USERS, ERROR RATES)
-- ============================================================

CREATE OR REPLACE VIEW `${PROJECT_ID}.${DATASET}.user_kpis_view` AS
SELECT
    user_uid,
    COUNT(*) AS total_requests,
    COUNTIF(status_code = 401) AS total_401,
    COUNTIF(status_code = 403) AS total_403,
    COUNTIF(status_code BETWEEN 400 AND 499) AS total_4xx,
    COUNTIF(status_code >= 500) AS total_5xx,
    SAFE_DIVIDE(COUNTIF(status_code BETWEEN 400 AND 499), COUNT(*)) AS rate_4xx,
    SAFE_DIVIDE(COUNTIF(status_code >= 500), COUNT(*)) AS rate_5xx,
    AVG(latency_ms) AS avg_latency_ms
FROM `${PROJECT_ID}.${DATASET}.normalized_requests_view`
GROUP BY user_uid;


-- ============================================================
-- TABLE: USER ACTIVITY DAILY WITH RANKING
-- ============================================================
-- Uses:
--   - RANK()
--   - DENSE_RANK()
--   - AVG() OVER(...)
-- ============================================================

CREATE OR REPLACE TABLE `${PROJECT_ID}.${DATASET}.dashboard_user_activity_daily` AS
WITH daily AS (
    SELECT
        DATE(event_timestamp) AS day,
        user_uid,
        COUNT(*) AS total_requests,
        AVG(latency_ms) AS avg_latency_ms
    FROM `${PROJECT_ID}.${DATASET}.normalized_requests_view`
    GROUP BY day, user_uid
),
ranked AS (
    SELECT
        *,
        RANK() OVER (
            PARTITION BY day
            ORDER BY total_requests DESC
        ) AS rank_by_volume,
        DENSE_RANK() OVER (
            PARTITION BY day
            ORDER BY avg_latency_ms DESC
        ) AS dense_rank_by_latency,
        AVG(total_requests) OVER (
            PARTITION BY day
        ) AS day_avg_requests_per_user
    FROM daily
)
SELECT
    day,
    user_uid,
    total_requests,
    avg_latency_ms,
    day_avg_requests_per_user,
    rank_by_volume,
    dense_rank_by_latency
FROM ranked;


-- ============================================================
-- TABLE: AUTH ERRORS DAILY (401/403) PER USER
-- ============================================================

CREATE OR REPLACE TABLE `${PROJECT_ID}.${DATASET}.dashboard_user_auth_errors_daily` AS
SELECT
    DATE(event_timestamp) AS day,
    user_uid,
    COUNTIF(status_code = 401) AS total_401,
    COUNTIF(status_code = 403) AS total_403,
    COUNT(*) AS total_requests,
    SAFE_DIVIDE(COUNTIF(status_code IN (401, 403)), COUNT(*)) AS auth_error_rate
FROM `${PROJECT_ID}.${DATASET}.normalized_requests_view`
GROUP BY day, user_uid;


-- ============================================================
-- TABLE: USER ROLLING ACTIVITY WITH LAG/LEAD
-- ============================================================
-- Uses:
--   - SUM() OVER(...) rolling window
--   - COUNT() OVER(...) rolling window
--   - LAG()
--   - LEAD()
-- ============================================================

CREATE OR REPLACE TABLE `${PROJECT_ID}.${DATASET}.dashboard_user_rolling_activity` AS
WITH base AS (
    SELECT
        event_timestamp,
        user_uid,
        api_path,
        CASE WHEN status_code >= 500 THEN 1 ELSE 0 END AS is_5xx
    FROM `${PROJECT_ID}.${DATASET}.normalized_requests_view`
),
rolling AS (
    SELECT
        event_timestamp,
        user_uid,

        COUNT(*) OVER (
            PARTITION BY user_uid
            ORDER BY event_timestamp
            RANGE BETWEEN INTERVAL 1 HOUR PRECEDING AND CURRENT ROW
        ) AS rolling_requests_1h,

        SUM(is_5xx) OVER (
            PARTITION BY user_uid
            ORDER BY event_timestamp
            RANGE BETWEEN INTERVAL 1 HOUR PRECEDING AND CURRENT ROW
        ) AS rolling_5xx_1h

    FROM base
),
rates AS (
    SELECT
        event_timestamp,
        user_uid,
        rolling_requests_1h,
        rolling_5xx_1h,
        SAFE_DIVIDE(rolling_5xx_1h, rolling_requests_1h) AS rolling_rate_5xx_1h
    FROM rolling
),
trend AS (
    SELECT
        *,
        LAG(rolling_requests_1h) OVER (
            PARTITION BY user_uid
            ORDER BY event_timestamp
        ) AS prev_rolling_requests_1h,
        LEAD(rolling_requests_1h) OVER (
            PARTITION BY user_uid
            ORDER BY event_timestamp
        ) AS next_rolling_requests_1h
    FROM rates
)
SELECT
    event_timestamp,
    user_uid,
    rolling_requests_1h,
    prev_rolling_requests_1h,
    next_rolling_requests_1h,
    rolling_5xx_1h,
    rolling_rate_5xx_1h
FROM trend;
