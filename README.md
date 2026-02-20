# 📊 Monitoring API Analytics – Cloud Logs to Dashboard Pipeline

## 1. Project Overview

This project implements a complete **API monitoring analytics pipeline** from raw Cloud Logging data to interactive dashboards.

The objective is to:

- Extract logs from Google Cloud Logging (Cloud Run / Kubernetes)
- Normalize heterogeneous log formats into a unified schema
- Store structured data in BigQuery or SQLite
- Generate production-ready monitoring metrics
- Visualize KPIs in Looker Studio or Grafana

The pipeline transforms distributed API logs into structured monitoring intelligence.

---

## 2. Problem Statement

Modern API systems generate logs across:

- Cloud Run services
- Kubernetes workloads
- Local JSON / JSONL / CSV exports

Challenges:

- Inconsistent log schemas
- Missing fields across environments
- Latency measurement inconsistencies
- Error rate aggregation
- SLA monitoring
- Anomaly detection

This project addresses these constraints through:

- Unified schema normalization
- Realistic fallback values when fields are missing
- SQL-based metrics layer
- Window functions for advanced analytics
- Reproducible dashboard metrics
- Clean orchestration pipeline

---

## 3. Monitoring Strategy

### Core Observability Dimensions

| Dimension | Description | Example |
|------------|------------|----------|
| event_timestamp | Exact time of API request | 2024-03-18T10:15:23Z |
| service | Microservice handling the request | user-service |
| api_path | Endpoint path | /api/v1/users |
| http_method | HTTP verb | GET |
| status_code | HTTP response code | 200 |
| latency_ms | Request execution time in milliseconds | 245 |
| user_uid | Unique user identifier (if available) | user_89234 |
| env | Environment name | prod |

### Key Monitoring Objectives

| Objective | Why It Matters | Example Insight |
|------------|----------------|----------------|
| Latency degradation detection | Identify performance slowdowns | P95 increased from 250ms to 900ms |
| 4xx / 5xx error tracking | Detect client/server issues | 5xx spike after deployment |
| SLA compliance monitoring | Ensure performance guarantees | SLA <500ms dropped to 91% |
| Slow endpoint identification | Optimize problematic routes | /checkout consistently >1200ms |
| Anomaly detection | Detect abnormal patterns | Latency z-score > 3 |
| Executive KPI visibility | High-level monitoring view | Daily requests and error rate trend |

---

## 4. Pipeline Architecture
```
Cloud Logging / JSON / CSV  
            ↓  
      Extract Logs  
            ↓  
     Schema Normalization  
            ↓  
   Unified Requests Table  
            ↓  
   BigQuery / SQLite Load  
            ↓  
      Metrics Generation  
            ↓  
     SQL Views / Tables  
            ↓  
   Looker Studio / Grafana
```
---

## 5. Analytics & Metrics Layer

The project uses advanced SQL analytics functions and materialized metrics tables.

### SQL Analytical Techniques

| Technique | Purpose | Example |
|------------|---------|----------|
| Window Functions (OVER) | Compute metrics over partitions | AVG(latency_ms) OVER(PARTITION BY api_path) |
| ROW_NUMBER | Rank events chronologically | ROW_NUMBER() OVER(PARTITION BY api_path ORDER BY latency_ms DESC) |
| RANK / DENSE_RANK | Identify top slow endpoints | RANK() OVER(PARTITION BY day ORDER BY p95_latency DESC) |
| LAG / LEAD | Compare with previous values | latency_ms - LAG(latency_ms) OVER(...) |
| Rolling Average | Smooth time series | AVG(latency_ms) OVER(ORDER BY hour ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) |
| Rolling Error Rate | Detect spikes | SUM(total_5xx) OVER(1h_window) |
| Z-Score | Detect anomalies statistically | (value - mean) / stddev |

### Generated Metrics Tables

| Table | Purpose | Example Usage |
|--------|----------|---------------|
| dashboard_latency_hourly | Hourly latency aggregation | Trend chart in Grafana |
| dashboard_errors_hourly | Hourly error metrics | Error rate time series |
| dashboard_status_codes_daily | Daily status distribution | Pie chart of 2xx/4xx/5xx |
| dashboard_sla_daily | SLA compliance tracking | SLA KPI card |
| dashboard_anomaly_latency_zscore | Latency anomaly detection | Alert trigger |
| dashboard_anomaly_error_spikes | Error spike detection | Incident investigation |
| dashboard_endpoint_degradation_daily

---

## 6. Project Structure
```
monitoring-api-analytics/
├── main.py                               ## Application entry point (FastAPI bootstrap, CLI execution, healthcheck)
├── menu_pipeline.sh                      ## Interactive CLI menu to run ETL, metrics generation, or API service
├── requirements.txt                      ## Python dependencies
├── README.md                             ## Project documentation
├── .env                                  ## Environment configuration (paths, GCP, BigQuery, API settings)
├── .gitignore                            ## Git ignored files
├── .dockerignore                         ## Docker build exclusions
│
├── docker/                               ## Container configuration and service orchestration
│   ├── Dockerfile                        ## Application container definition
│   └── docker-compose.yml                ## Local orchestration (API + volumes + environment)
│
├── logs/                                 ## Centralized runtime logs
│
├── secrets/                              ## Service account credentials (excluded from version control)
│
├── data/
│   ├── raw/                              ## Raw API logs (json, jsonl, csv)
│   ├── interim/                          ## Normalized structured datasets before loading
│   └── processed/                        ## SQLite database or processed outputs for dashboards
│
├── artifacts/
│   ├── dashboards/                       ## Dashboard configurations (Looker documentation + Grafana JSON import)
│   │   └── grafana_api_analytics.json    ## Grafana dashboard definition (importable JSON)
│   │
│   ├── sql/                              ## Versioned SQL scripts for normalization and metrics computation
│   │   ├── normalize_view.sql            ## Unified schema view
│   │   ├── latency_metrics.sql           ## Latency aggregation metrics
│   │   ├── sla_metrics.sql               ## SLA compliance calculations
│   │   ├── status_codes.sql              ## HTTP status distribution metrics
│   │   ├── error_metrics.sql             ## 4xx / 5xx error aggregations
│   │   ├── user_stats.sql                ## User-level statistics
│   │   └── anomaly_detection.sql         ## Rolling anomaly and degradation detection
│   │
│   └── exports/                          ## Optional exports (CSV, diagnostic outputs, reports)
│
├── tests/
│   └── test_unit.py                      ## Unit tests for ETL, metrics, and validation logic
│
└── src/
    ├── pipeline.py                       ## End-to-end orchestration (extract → transform → load → metrics)
    ├── __init__.py
    │
    ├── utils/
    │   ├── logging_utils.py              ## Centralized structured logging utilities
    │   ├── cloud_utils.py                ## Google Cloud Logging integration helpers
    │   └── loader_utils.py               ## Safe JSON / JSONL / CSV loading helpers
    │
    ├── core/
    │   ├── service.py                    ## FastAPI routes and API exposure
    │   ├── schema.py                     ## Pydantic request/response models
    │   ├── config.py                     ## Environment variable management and path resolution
    │   ├── exploration.py                ## Exploratory data analysis utilities
    │   └── errors.py                     ## Centralized custom exception definitions
    │
    └── etl/
        ├── extract.py                    ## Log extraction from Cloud Logging or local files
        ├── transform.py                  ## Schema normalization and enrichment
        ├── load.py                       ## Data loading into BigQuery or SQLite
        └── metrics.py                    ## Dashboard metrics materialization and SQL execution
```
---

## 7. Prerequisites

- Python 3.10+
- Docker & Docker Compose
- Optional GPU (for BiLSTM)

### Ubuntu Example

```bash
sudo apt update
sudo apt install python python3-pip
python --version
```

---

## 8. Setup

### Python

```bash
python -m venv .monitor_env
source .monitor_env/bin/activate   							## for windows : .monitor_env\Scripts\activate.bat
python -m pip install --upgrade pip setuptools wheel		## for windows : .monitor_env\Scripts\python.exe -m pip install --upgrade pip setuptools wheel
pip install -r requirements.txt
```

### Docker

```
docker compose build
docker compose up
```

---

## 9. Full System Verification
```bash
## Run full ETL pipeline (extract logs from Cloud Logging, transform, and load into BigQuery)
python main.py --extract-transform-load --source cloud --target bigquery --filter-query "resource.type=\"cloud_run_revision\"" --limit 100

## Deploy / rebuild BigQuery dashboard tables and views only (no extraction)
python main.py --build-metrics-only --target bigquery  

## Start FastAPI service using uvicorn (API layer)
python main.py --run-api  

## Run full test suite quietly (pytest)
pytest -q
```
---

## 10. Dashboard Integration

### Looker Studio

- Connect BigQuery dataset
- Use tables under dashboard_*

### Grafana

- Import grafana_monitoring_api_analytics.json
- Configure BigQuery datasource

---

## Author

**Georges Nassopoulos**  
Email: georges.nassopoulos@gmail.com


