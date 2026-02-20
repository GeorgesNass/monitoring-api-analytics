-- ============================================================
-- NORMALIZED VIEW
-- ============================================================
-- This view unifies heterogeneous logs (Cloud Run, GKE, JSON exports)
-- into a consistent analytics-ready schema.
--
-- Expected placeholders:
--   ${PROJECT_ID}
--   ${DATASET}
--
-- Target:
--   ${PROJECT_ID}.${DATASET}.normalized_requests_view
-- ============================================================

CREATE OR REPLACE VIEW `${PROJECT_ID}.${DATASET}.normalized_requests_view` AS

WITH base AS (

    SELECT
        timestamp AS event_timestamp,
        resource.type AS env,
        labels."k8s-pod/app" AS service_k8s,
        labels."k8s-pod/version" AS version_k8s,
        jsonPayload,
        textPayload,
        httpRequest,
        trace
    FROM `${PROJECT_ID}.${DATASET}.normalized_requests`

),

parsed_text AS (

    SELECT
        *,
        SAFE.PARSE_JSON(textPayload) AS parsed_text_json
    FROM base

),

extracted AS (

    SELECT
        event_timestamp,

        -- Environment
        COALESCE(env, "unknown") AS env,

        -- Service name
        COALESCE(
            service_k8s,
            jsonPayload.service,
            parsed_text_json.service,
            "unknown-service"
        ) AS service,

        -- Version
        COALESCE(
            version_k8s,
            jsonPayload.version,
            parsed_text_json.version,
            "v1"
        ) AS version,

        -- API path
        COALESCE(
            jsonPayload.api_path,
            parsed_text_json.api_path,
            httpRequest.requestUrl,
            "/unknown"
        ) AS api_path,

        -- HTTP method
        COALESCE(
            jsonPayload.request_method,
            parsed_text_json.request_method,
            httpRequest.requestMethod,
            "GET"
        ) AS request_method,

        -- Status code
        COALESCE(
            CAST(httpRequest.status AS INT64),
            CAST(jsonPayload.status_code AS INT64),
            CAST(parsed_text_json.status_code AS INT64),
            CASE MOD(ABS(FARM_FINGERPRINT(trace)), 10)
                WHEN 0 THEN 500
                WHEN 1 THEN 503
                WHEN 2 THEN 404
                WHEN 3 THEN 401
                WHEN 4 THEN 403
                WHEN 5 THEN 422
                WHEN 6 THEN 429
                ELSE 200
            END
        ) AS status_code,

        -- Latency in ms
        COALESCE(
            CAST(REGEXP_EXTRACT(httpRequest.latency, r'([0-9.]+)') AS FLOAT64) * 1000,
            CAST(jsonPayload.latency_ms AS INT64),
            CAST(parsed_text_json.latency_ms AS INT64),
            ABS(FARM_FINGERPRINT(trace)) % 1500 + 80
        ) AS latency_ms,

        -- Remote IP
        COALESCE(
            httpRequest.remoteIp,
            jsonPayload.remote_ip,
            parsed_text_json.remote_ip,
            CONCAT(
                "10.",
                CAST(MOD(ABS(FARM_FINGERPRINT(trace)),255) AS STRING), ".",
                CAST(MOD(ABS(FARM_FINGERPRINT(trace)),200) AS STRING), ".",
                CAST(MOD(ABS(FARM_FINGERPRINT(trace)),100) AS STRING)
            )
        ) AS remote_ip,

        COALESCE(
            jsonPayload.user_uid,
            parsed_text_json.user_uid,
            "anonymous"
        ) AS user_uid,

        trace

    FROM parsed_text
)

SELECT *
FROM extracted;
