'''
__author__ = "Georges Nassopoulos"
__copyright__ = None
__version__ = "1.0.0"
__email__ = "georges.nassopoulos@gmail.com"
__status__ = "Dev"
__desc__ = "End-to-end pipeline orchestration: extract logs, normalize schema, load into storage, and build dashboard metrics."
'''

from pathlib import Path
from typing import Optional

import pandas as pd

from src.core.errors import ExtractionError
from src.etl.extract import extract_logs
from src.etl.transform import normalize_logs
from src.etl.load import load_data
from src.etl.metrics import build_metrics
from src.utils.logging_utils import get_logger, log_execution_time_and_path

logger = get_logger(__name__)

## ============================================================
## PIPELINE ORCHESTRATION
## ============================================================
@log_execution_time_and_path
def run_pipeline(
    source: str,
    target: str,
    start_from: Optional[str] = None,
    file_path: Optional[str | Path] = None,
    filter_query: Optional[str] = None,
    limit: int = 5000,
) -> None:
    """
        Execute monitoring ETL pipeline

        High-level workflow:
            1) Extract logs from cloud or file
            2) Normalize into unified schema
            3) Load into selected storage target
            4) Build dashboard metrics (BigQuery views or SQLite tables)

        Args:
            source: cloud, json, jsonl, csv
            target: bigquery or sqlite
            start_from: Optional pipeline step to start from
            file_path: Raw file path if source is file
            filter_query: Cloud Logging filter expression if source is cloud
            limit: Maximum cloud entries
    """

    logger.info("Starting pipeline with source=%s target=%s", source, target)

    raw_data: list[dict] | pd.DataFrame

    if start_from in (None, "extract"):
        raw_data = extract_logs(
            source=source,
            file_path=file_path,
            filter_query=filter_query,
            limit=limit,
        )
    else:
        raise ExtractionError("start_from must be None or 'extract'")

    ## Normalize
    df = normalize_logs(raw_data)

    ## Load
    load_data(df, target=target)

    ## Build metrics for dashboard
    build_metrics(target=target)

    logger.info("Pipeline + metrics completed successfully")