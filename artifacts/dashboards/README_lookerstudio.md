# Monitoring API Analytics -- Looker Studio Dashboard

  -----------------------------------------------------------------------
  Field                               Value
  ----------------------------------- -----------------------------------
  Version                             1.0.0

  Author                              Georges Nassopoulos

  Purpose                             Recreate the Monitoring API
                                      Analytics dashboard in Looker
                                      Studio using BigQuery assets

  Architecture                        BigQuery (metrics) → Looker Studio
                                      (visualization only)
  -----------------------------------------------------------------------

------------------------------------------------------------------------

## 1️⃣ Overview

  Step   Action
  ------ -----------------------------------
  1      Run metrics deployment
  2      Connect BigQuery in Looker Studio
  3      Create pages & charts
  4      Apply global filters
  5      Save & share report

------------------------------------------------------------------------

## 2️⃣ Deploy Metrics (Required)

``` bash
python main.py --build-metrics-only --target bigquery
```

  Category          Required BigQuery Objects
  ----------------- --------------------------------------
  Core              normalized_requests_view
  Latency           dashboard_latency_hourly
  Latency KPIs      latency_kpis_view
  Errors            dashboard_errors_hourly
  Error KPIs        error_kpis_view
  Status Codes      dashboard_status_codes_daily
  Status Families   dashboard_status_families_daily
  SLA               dashboard_sla_daily
  SLA Drift         dashboard_sla_rolling_drift
  Anomaly           dashboard_anomaly_latency_zscore
  Anomaly           dashboard_anomaly_error_spikes
  Degradation       dashboard_endpoint_degradation_daily

------------------------------------------------------------------------

## 3️⃣ Create Data Source

1.  Go to https://lookerstudio.google.com\
2.  Create → Data Source\
3.  Select BigQuery\
4.  Choose Project & Dataset\
5.  Add required tables\
6.  Validate schema\
7.  Save

------------------------------------------------------------------------

## 4️⃣ Dashboard Structure

### PAGE 1 -- Executive Overview

  --------------------------------------------------------------------------------------------
  Component                    Source                     Metric
  ---------------------------- -------------------------- ------------------------------------
  Total Requests               dashboard_latency_hourly   SUM(total_requests)

  P95 Latency                  latency_kpis_view          p95_latency_ms

  Error Rate 5xx               dashboard_errors_hourly    SUM(total_5xx)/SUM(total_requests)

  SLA \<500ms                  dashboard_sla_daily        sla_lt_500ms_rate
  --------------------------------------------------------------------------------------------

### PAGE 2 -- Latency Deep Dive

Source: latency_kpis_view

Columns: - api_path\
- avg_latency_ms\
- p95_latency_ms\
- p99_latency_ms\
- total_requests

Sort by p95_latency_ms DESC

------------------------------------------------------------------------

### PAGE 3 -- Errors

Source: error_kpis_view

Columns: - api_path\
- total_4xx\
- total_5xx\
- error_rate

------------------------------------------------------------------------

### PAGE 4 -- Status Codes

Pivot: - Rows: api_path\
- Columns: status_code\
- Metric: total_requests

Pie: - Dimension: status_family\
- Metric: total_requests

------------------------------------------------------------------------

### PAGE 5 -- SLA

-   SLA Trend → dashboard_sla_daily\
-   SLA Drift → dashboard_sla_rolling_drift

------------------------------------------------------------------------

### PAGE 6 -- Anomaly Detection

-   dashboard_anomaly_latency_zscore\
-   dashboard_anomaly_error_spikes\
-   dashboard_endpoint_degradation_daily

------------------------------------------------------------------------

## 5️⃣ Versioning Strategy

  Layer            Versioned
  ---------------- -----------
  SQL Metrics      Yes
  BigQuery Views   Yes
  Looker Layout    No

------------------------------------------------------------------------

## 6️⃣ Production Notes

-   Partition BigQuery tables by DATE(event_timestamp)
-   Use scheduled queries
-   Apply row-level security if needed
-   Separate reports per environment

------------------------------------------------------------------------

End of document.
