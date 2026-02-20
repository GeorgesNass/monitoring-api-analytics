'''
__author__ = "Georges Nassopoulos"
__copyright__ = None
__version__ = "1.0.0"
__email__ = "georges.nassopoulos@gmail.com"
__status__ = "Dev"
__desc__ = "Metrics builder: deploy BigQuery views/tables from SQL files and optionally materialize dashboard tables in SQLite."
'''

from __future__ import annotations

import re
import sqlite3
from pathlib import Path
from typing import Dict, List, Optional, Literal

import pandas as pd
from google.cloud import bigquery
from google.oauth2 import service_account

from src.core.config import settings
from src.core.errors import LoadError
from src.utils.logging_utils import get_logger, log_execution_time_and_path

## ============================================================
## LOGGER
## ============================================================
logger = get_logger(__name__)

## ============================================================
## CONSTANTS
## ============================================================
TargetType = Literal["bigquery", "sqlite"]

DEFAULT_SQL_DIR = Path("artifacts/sql")

## SQL files we expect for deployment
SQL_FILES_ORDERED = [
    "normalize_view.sql",
    "latency_metrics.sql",
    "error_metrics.sql",
    "user_stats.sql",
    "anomaly_detection.sql",
]

## ============================================================
## SQL FILE HELPERS
## ============================================================
def _read_sql_file(sql_path: Path) -> str:
    """
        Read SQL file content

        High-level workflow:
            1) Validate file exists
            2) Read content as UTF-8
            3) Return raw SQL string

        Args:
            sql_path: Path to SQL file

        Returns:
            SQL content
    """
    
    if not sql_path.exists():
        raise LoadError(f"SQL file not found: {sql_path}")

    return sql_path.read_text(encoding="utf-8")

def _strip_sql_comments(sql_text: str) -> str:
    """
        Strip SQL comments to simplify parsing

        High-level workflow:
            1) Remove line comments starting with --
            2) Remove block comments /* ... */
            3) Return cleaned SQL

        Args:
            sql_text: Raw SQL text

        Returns:
            SQL text without comments
    """
    
    sql_text = re.sub(r"/\*.*?\*/", "", sql_text, flags=re.DOTALL)
    sql_text = re.sub(r"--.*?$", "", sql_text, flags=re.MULTILINE)
    
    return sql_text.strip()

def _split_sql_statements(sql_text: str) -> List[str]:
    """
        Split SQL into executable statements

        High-level workflow:
            1) Strip comments
            2) Split by semicolon
            3) Filter empty statements

        Args:
            sql_text: Raw SQL content

        Returns:
            List of SQL statements
    """
    
    cleaned = _strip_sql_comments(sql_text)
    statements = [s.strip() for s in cleaned.split(";") if s.strip()]
    
    return statements

## ============================================================
## BIGQUERY CLIENT
## ============================================================
def _create_bigquery_client() -> bigquery.Client:
    """
        Create BigQuery client

        High-level workflow:
            1) Load service account credentials if provided
            2) Create bigquery client
            3) Return client

        Returns:
            bigquery.Client instance
    """
    
    try:
        creds_path = settings.google_credentials_path.strip() if settings.google_credentials_path else ""

        if creds_path and ("<" in creds_path or "YOUR_" in creds_path):
            raise LoadError(
                "Invalid GOOGLE_APPLICATION_CREDENTIALS value (placeholder detected). "
                "Please set GOOGLE_APPLICATION_CREDENTIALS to a real service account JSON path."
            )

        if creds_path:
            credentials = service_account.Credentials.from_service_account_file(
                creds_path
            )
            return bigquery.Client(
                project=settings.project_id,
                credentials=credentials,
            )

        return bigquery.Client(project=settings.project_id)

    except LoadError:
        raise

    except Exception as exc:
        logger.exception("Failed to create BigQuery client")
        raise LoadError(str(exc)) from exc

## ============================================================
## BIGQUERY DEPLOYMENT
## ============================================================
@log_execution_time_and_path
def deploy_bigquery_sql_from_dir(
    sql_dir: str | Path = DEFAULT_SQL_DIR,
    dataset: Optional[str] = None,
) -> None:
    """
        Deploy SQL assets into BigQuery (views/tables)

        Notes:
            - This executes SQL files as-is
            - SQL files should contain explicit CREATE OR REPLACE statements

        High-level workflow:
            1) Resolve dataset and sql directory
            2) Read SQL files in predefined order
            3) Execute statements sequentially in BigQuery

        Args:
            sql_dir: Directory containing SQL files
            dataset: Override dataset (default from settings)
    """
    
    sql_dir_path = Path(sql_dir)
    dataset_name = dataset or settings.bigquery_dataset

    if not dataset_name:
        raise LoadError("BigQuery dataset is not set (BQ_DATASET)")

    client = _create_bigquery_client()

    for file_name in SQL_FILES_ORDERED:
        sql_path = sql_dir_path / file_name
        sql_text = _read_sql_file(sql_path)

        ## Allow placeholders in SQL files
        sql_text = sql_text.replace("${PROJECT_ID}", str(settings.project_id or ""))
        sql_text = sql_text.replace("${DATASET}", str(dataset_name))

        statements = _split_sql_statements(sql_text)

        logger.info("Deploying SQL file: %s (%d statements)", file_name, len(statements))

        for stmt in statements:
            try:
                job = client.query(stmt)
                job.result()
            except Exception as exc:
                logger.exception("BigQuery SQL execution failed for file=%s", file_name)
                raise LoadError(f"BigQuery SQL execution failed: {file_name} | {str(exc)}") from exc

    logger.info("BigQuery SQL deployment completed successfully")

## ============================================================
## SQLITE MATERIALIZATION
## ============================================================
def _connect_sqlite(sqlite_path: str | Path) -> sqlite3.Connection:
    """
        Create SQLite connection

        Args:
            sqlite_path: Path to sqlite database file

        Returns:
            sqlite3.Connection
    """
    
    sqlite_path = Path(sqlite_path)
    sqlite_path.parent.mkdir(parents=True, exist_ok=True)

    return sqlite3.connect(sqlite_path)

@log_execution_time_and_path
def materialize_sqlite_dashboard_tables(
    sqlite_path: str | Path,
    normalized_table: str = "normalized_requests",
) -> None:
    """
        Materialize dashboard-ready tables in SQLite

        Notes:
            - This assumes normalized_requests already exists in SQLite
            - Tables are created as simple aggregations compatible with Grafana panels

        High-level workflow:
            1) Connect to SQLite
            2) Create dashboard tables if not exist
            3) Populate tables via INSERT SELECT aggregations

        Args:
            sqlite_path: SQLite DB path
            normalized_table: Source table containing normalized requests
    """
    
    conn = _connect_sqlite(sqlite_path)

    try:
        cur = conn.cursor()

        ## Dashboard table: latency_daily
        cur.execute(
            f"""
            CREATE TABLE IF NOT EXISTS dashboard_latency_daily (
                day TEXT,
                api_path TEXT,
                total_requests INTEGER,
                avg_latency_ms REAL,
                max_latency_ms INTEGER
            )
            """
        )

        ## Dashboard table: errors_hourly
        cur.execute(
            f"""
            CREATE TABLE IF NOT EXISTS dashboard_errors_hourly (
                hour TEXT,
                api_path TEXT,
                total_requests INTEGER,
                total_4xx INTEGER,
                total_5xx INTEGER,
                error_rate_5xx REAL
            )
            """
        )

        ## Dashboard table: status_codes
        cur.execute(
            f"""
            CREATE TABLE IF NOT EXISTS dashboard_status_codes (
                day TEXT,
                api_path TEXT,
                status_code INTEGER,
                total_requests INTEGER
            )
            """
        )

        ## Optional: clear existing dashboard tables
        cur.execute("DELETE FROM dashboard_latency_daily")
        cur.execute("DELETE FROM dashboard_errors_hourly")
        cur.execute("DELETE FROM dashboard_status_codes")

        ## Insert latency_daily
        cur.execute(
            f"""
            INSERT INTO dashboard_latency_daily
            SELECT
                substr(event_timestamp, 1, 10) AS day,
                api_path,
                COUNT(*) AS total_requests,
                AVG(latency_ms) AS avg_latency_ms,
                MAX(latency_ms) AS max_latency_ms
            FROM {normalized_table}
            GROUP BY day, api_path
            """
        )

        ## Insert errors_hourly
        cur.execute(
            f"""
            INSERT INTO dashboard_errors_hourly
            SELECT
                substr(event_timestamp, 1, 13) || ':00:00' AS hour,
                api_path,
                COUNT(*) AS total_requests,
                SUM(CASE WHEN status_code BETWEEN 400 AND 499 THEN 1 ELSE 0 END) AS total_4xx,
                SUM(CASE WHEN status_code >= 500 THEN 1 ELSE 0 END) AS total_5xx,
                CAST(SUM(CASE WHEN status_code >= 500 THEN 1 ELSE 0 END) AS REAL) / COUNT(*) AS error_rate_5xx
            FROM {normalized_table}
            GROUP BY hour, api_path
            """
        )

        ## Insert status_codes
        cur.execute(
            f"""
            INSERT INTO dashboard_status_codes
            SELECT
                substr(event_timestamp, 1, 10) AS day,
                api_path,
                status_code,
                COUNT(*) AS total_requests
            FROM {normalized_table}
            GROUP BY day, api_path, status_code
            """
        )

        conn.commit()
        logger.info("SQLite dashboard tables materialized successfully")

    except Exception as exc:
        logger.exception("Failed to materialize SQLite dashboard tables")
        raise LoadError(str(exc)) from exc

    finally:
        conn.close()

## ============================================================
## PUBLIC METRICS ENTRYPOINT
## ============================================================
@log_execution_time_and_path
def build_metrics(
    target: TargetType,
    sql_dir: str | Path = DEFAULT_SQL_DIR,
    sqlite_path: Optional[str | Path] = None,
) -> None:
    """
        Build metrics assets for dashboard consumption

        High-level workflow:
            1) If target=bigquery, deploy SQL views/tables from artifacts/sql
            2) If target=sqlite, materialize dashboard tables from normalized_requests

        Args:
            target: bigquery or sqlite
            sql_dir: Directory containing SQL assets
            sqlite_path: Optional sqlite DB path override
    """
    
    if target == "bigquery":
        deploy_bigquery_sql_from_dir(sql_dir=sql_dir)
        return

    if target == "sqlite":
        resolved_sqlite_path = sqlite_path or settings.local_sqlite_path
        materialize_sqlite_dashboard_tables(sqlite_path=resolved_sqlite_path)
        return

    raise LoadError(f"Unsupported target for metrics build: {target}")