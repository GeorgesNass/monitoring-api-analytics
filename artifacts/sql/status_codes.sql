-- ============================================================
-- STATUS CODES METRICS (DASHBOARD-READY)
-- ============================================================
-- Builds comprehensive HTTP status code metrics:
--   - per code (200..599)
--   - per family (2xx/3xx/4xx/5xx)
--   - per endpoint
--   - time series by hour/day
--   - top endpoints per error code
--
-- Expected placeholders:
--   ${PROJECT_ID}
--   ${DATASET}
--
-- Outputs:
--   - view:   status_code_kpis_view
--   - table:  dashboard_status_codes_daily
--   - table:  dashboard_status_codes_hourly
--   - table:  dashboard_status_families_daily
--   - table:  dashboard_top_endpoints_per_error_code_daily
-- ============================================================

-- ============================================================
-- VIEW: STATUS CODE KPIS PER ENDPOINT
-- ============================================================

CREATE OR REPLACE VIEW `${PROJECT_ID}.${DATASET}.status_code_kpis_view` AS
SELECT
    api_path,
    status_code,
    COUNT(*) AS total_requests,
    SAFE_DIVIDE(COUNT(*), SUM(COUNT(*)) OVER (PARTITION BY api_path)) AS pct_within_endpoint
FROM `${PROJECT_ID}.${DATASET}.normalized_requests_view`
GROUP BY api_path, status_code;


-- ============================================================
-- TABLE: DAILY STATUS CODES (FOR LOOKER STUDIO PIVOTS)
-- ============================================================

CREATE OR REPLACE TABLE `${PROJECT_ID}.${DATASET}.dashboard_status_codes_daily` AS
SELECT
    DATE(event_timestamp) AS day,
    api_path,
    status_code,
    COUNT(*) AS total_requests
FROM `${PROJECT_ID}.${DATASET}.normalized_requests_view`
GROUP BY day, api_path, status_code;


-- ============================================================
-- TABLE: HOURLY STATUS CODES (FOR TIME SERIES)
-- ============================================================

CREATE OR REPLACE TABLE `${PROJECT_ID}.${DATASET}.dashboard_status_codes_hourly` AS
SELECT
    TIMESTAMP_TRUNC(event_timestamp, HOUR) AS hour_ts,
    api_path,
    status_code,
    COUNT(*) AS total_requests
FROM `${PROJECT_ID}.${DATASET}.normalized_requests_view`
GROUP BY hour_ts, api_path, status_code;


-- ============================================================
-- TABLE: DAILY STATUS FAMILIES (2xx/3xx/4xx/5xx)
-- ============================================================

CREATE OR REPLACE TABLE `${PROJECT_ID}.${DATASET}.dashboard_status_families_daily` AS
SELECT
    DATE(event_timestamp) AS day,
    api_path,
    CONCAT(CAST(DIV(status_code, 100) AS STRING), "xx") AS status_family,
    COUNT(*) AS total_requests,
    SAFE_DIVIDE(COUNT(*), SUM(COUNT(*)) OVER (PARTITION BY DATE(event_timestamp), api_path)) AS pct_within_day_endpoint
FROM `${PROJECT_ID}.${DATASET}.normalized_requests_view`
GROUP BY day, api_path, status_family;


-- ============================================================
-- TABLE: TOP ENDPOINTS PER ERROR CODE DAILY
-- ============================================================
-- Uses:
--   - ROW_NUMBER()
--   - RANK()
-- ============================================================

CREATE OR REPLACE TABLE `${PROJECT_ID}.${DATASET}.dashboard_top_endpoints_per_error_code_daily` AS
WITH daily AS (
    SELECT
        DATE(event_timestamp) AS day,
        status_code,
        api_path,
        COUNT(*) AS total_requests
    FROM `${PROJECT_ID}.${DATASET}.normalized_requests_view`
    WHERE status_code >= 400
    GROUP BY day, status_code, api_path
),
ranked AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY day, status_code
            ORDER BY total_requests DESC
        ) AS rownum_by_volume,
        RANK() OVER (
            PARTITION BY day, status_code
            ORDER BY total_requests DESC
        ) AS rank_by_volume
    FROM daily
)
SELECT
    day,
    status_code,
    api_path,
    total_requests,
    rownum_by_volume,
    rank_by_volume
FROM ranked
WHERE rownum_by_volume <= 20;
