'''
__author__ = "Georges Nassopoulos"
__copyright__ = None
__version__ = "1.0.0"
__email__ = "georges.nassopoulos@gmail.com"
__status__ = "Dev"
__desc__ = "Load layer for storing normalized logs into BigQuery or SQLite."
'''

from pathlib import Path
from typing import Literal, Optional

import pandas as pd
import sqlite3

from src.core.config import settings
from src.core.errors import LoadError
from src.utils.logging_utils import get_logger, log_execution_time_and_path

logger = get_logger(__name__)

## ============================================================
## TARGET CONSTANTS
## ============================================================
TargetType = Literal["bigquery", "sqlite"]

## ============================================================
## BIGQUERY LOAD
## ============================================================
def _create_bigquery_client():
    """
        Create BigQuery client

        Returns:
            bigquery.Client instance
    """
    
    try:
        # Lazy import to avoid crashing when BigQuery is not installed
        from google.cloud import bigquery
        from google.oauth2 import service_account

        if settings.google_credentials_path:
            credentials = service_account.Credentials.from_service_account_file(
                settings.google_credentials_path
            )
            return bigquery.Client(
                project=settings.project_id,
                credentials=credentials,
            )

        return bigquery.Client(
            project=settings.project_id,
        )

    except Exception as exc:
        logger.exception("Failed to create BigQuery client")
        raise LoadError(str(exc)) from exc

@log_execution_time_and_path
def load_to_bigquery(
    df: pd.DataFrame,
    table_id: Optional[str] = None,
) -> None:
    """
        Load normalized DataFrame into BigQuery table

        High-level workflow:
            1) Create BigQuery client
            2) Create load job from DataFrame
            3) Append data into destination table

        Args:
            df: Normalized dataset
            table_id: Full table id (project.dataset.table)
    """
    
    try:
        # Lazy import here as well for safety
        from google.cloud import bigquery

        client = _create_bigquery_client()

        if not table_id:
            table_id = (
                f"{settings.project_id}."
                f"{settings.bigquery_dataset}."
                f"{settings.bigquery_table}"
            )

        job_config = bigquery.LoadJobConfig(
            write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
            autodetect=True,
        )

        job = client.load_table_from_dataframe(
            df,
            table_id,
            job_config=job_config,
        )
        job.result()

        logger.info(
            "Loaded %s rows into BigQuery table %s",
            len(df),
            table_id,
        )

    except Exception as exc:
        logger.exception("Failed to load data into BigQuery")
        raise LoadError(str(exc)) from exc

## ============================================================
## SQLITE LOAD
## ============================================================
@log_execution_time_and_path
def load_to_sqlite(
    df: pd.DataFrame,
    sqlite_path: Optional[str | Path] = None,
    table_name: str = "normalized_requests",
) -> None:
    """
        Load normalized DataFrame into SQLite database

        High-level workflow:
            1) Ensure folder exists
            2) Connect to SQLite
            3) Append DataFrame to SQL table

        Args:
            df: Normalized dataset
            sqlite_path: Path to SQLite DB file
            table_name: Destination table name
    """
    
    try:
        if not sqlite_path:
            sqlite_path = settings.local_sqlite_path

        sqlite_path = Path(sqlite_path)
        sqlite_path.parent.mkdir(parents=True, exist_ok=True)

        with sqlite3.connect(sqlite_path) as conn:
            df.to_sql(
                table_name,
                conn,
                if_exists="append",
                index=False,
            )

        logger.info(
            "Loaded %s rows into SQLite DB %s table %s",
            len(df),
            sqlite_path,
            table_name,
        )

    except Exception as exc:
        logger.exception("Failed to load data into SQLite")
        raise LoadError(str(exc)) from exc

## ============================================================
## PUBLIC LOAD ENTRYPOINT
## ============================================================
@log_execution_time_and_path
def load_data(
    df: pd.DataFrame,
    target: TargetType,
) -> None:
    """
        Load normalized data into chosen target

        Args:
            df: Normalized dataset
            target: bigquery or sqlite
    """
    
    if target == "bigquery":
        load_to_bigquery(df)
        return

    if target == "sqlite":
        load_to_sqlite(df)
        return

    raise LoadError(f"Unsupported target: {target}")