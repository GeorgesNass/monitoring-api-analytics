# 📊 Monitoring API Analytics – Cloud Logs to Dashboard Pipeline

The pipeline transforms distributed API logs into **structured monitoring intelligence and operational dashboards**.

---

## 🎯 Project Overview

Main capabilities:

* Extract logs from **Google Cloud Logging** (Cloud Run / Kubernetes)
* Normalize heterogeneous log formats into a **unified schema**
* Store structured data in **BigQuery or SQLite**
* Generate **production-ready monitoring metrics**
* Visualize KPIs in **Looker Studio or Grafana**

The system converts distributed API logs into **actionable monitoring analytics**.

---

## ⚙️ Tech Stack

Core technologies used in the project:

* Python
* FastAPI
* Docker & Docker Compose
* Google Cloud Logging
* BigQuery
* SQLite
* SQL analytics (window functions)
* Grafana
* Looker Studio

---

## 📂 Project Structure

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

## ❓ Problem Statement

Modern API infrastructures generate logs across multiple environments:

* Cloud Run services
* Kubernetes workloads
* Local JSON / JSONL / CSV exports

Key challenges include:

* inconsistent log schemas
* missing fields across environments
* latency measurement inconsistencies
* error rate aggregation
* SLA monitoring
* anomaly detection

This project addresses these issues through:

* unified schema normalization
* fallback values for missing fields
* SQL-based analytics layer
* window functions for advanced metrics
* reproducible dashboard datasets

---

## 🧠 Approach / Methodology / Strategy

The platform transforms heterogeneous logs into **structured monitoring analytics**.

Core principles:

* **Unified schema normalization** for heterogeneous logs
* **SQL-driven analytics layer**
* **Latency and error monitoring metrics**
* **Reproducible dashboard datasets**

### Monitoring Ecosystem

| Component             | Role                                            |
| --------------------- | ----------------------------------------------- |
| Log Extraction        | Retrieve logs from Cloud Logging or local files |
| Schema Normalization  | Convert logs to unified schema                  |
| Metrics Generation    | Compute monitoring metrics                      |
| SQL Analytics         | Window functions and statistical calculations   |
| Dashboard Integration | Grafana and Looker Studio visualization         |

### Observability Dimensions

| Dimension       | Description                   |
| --------------- | ----------------------------- |
| event_timestamp | Timestamp of API request      |
| service         | Microservice handling request |
| api_path        | Endpoint path                 |
| http_method     | HTTP method                   |
| status_code     | HTTP response code            |
| latency_ms      | Request execution time        |
| user_uid        | Unique user identifier        |
| env             | Deployment environment        |

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

## 🏗 Pipeline Architecture

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

## 📊 Exploratory Data Analysis

The project provides analytical tools to explore monitoring datasets:

* latency distribution analysis
* error rate diagnostics
* SLA compliance analysis
* anomaly detection via statistical metrics

Generated outputs can be exported to:

```
artifacts/exports/
```

---

## 🔧 Setup & Installation

In this section we explain the minimum OS verification, python usage and docker setup.

### 1. Requirements

* Python 3.10+
* Docker & Docker Compose
* Google Cloud credentials (optional)

### 2. OS prerequists

Verify that you have the necessairy packages installed.

#### Windows / WSL2 (recommended)

```bash
# PowerShell
wsl --status
wsl --install
wsl --list --online
wsl --install -d Ubuntu
wsl -d Ubuntu

docker --version
docker compose version
```

#### Ubuntu

```bash
sudo apt update
sudo apt install -y python3 python3-venv python3-pip build-essential curl git
python3 --version
```

### 3. Python environment

```bash
python -m venv .monitor_env
source .monitor_env/bin/activate   							## for windows : .monitor_env\Scripts\activate.bat
python -m pip install --upgrade pip setuptools wheel		## for windows : .monitor_env\Scripts\python.exe -m pip install --upgrade pip setuptools wheel
pip install -r requirements.txt
```

### 4. Docker setup

```bash
docker compose -f docker/docker-compose.yml build
docker compose -f docker/docker-compose.yml up
```

---

## ▶️ Usage & End-to-End Testing

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

## 📛 Common Errors & Troubleshooting

| Error                                | Cause                           | Solution                   |
| ------------------------------------ | ------------------------------- | -------------------------- |
| Cloud Logging authentication failure | Missing GCP credentials         | Configure service account  |
| BigQuery connection error            | Incorrect dataset configuration | Check `.env` variables     |
| Schema mismatch                      | Unexpected log structure        | Update normalization logic |
| Docker container startup failure     | Environment misconfiguration    | Rebuild containers         |

---

## 👤 Author

**Georges Nassopoulos**
[georges.nassopoulos@gmail.com](mailto:georges.nassopoulos@gmail.com)

**Status:** AI Engineering / Cloud Infrastructure Project