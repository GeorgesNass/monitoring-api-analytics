'''
__author__ = "Georges Nassopoulos"
__copyright__ = None
__version__ = "1.0.0"
__email__ = "georges.nassopoulos@gmail.com"
__status__ = "Dev"
__desc__ = "Extraction layer for monitoring logs from Cloud Logging API or local raw files (JSON/JSONL/CSV)."
'''

from pathlib import Path
from typing import Any, Dict, List, Literal, Optional

import pandas as pd

from src.core.errors import ExtractionError
from src.utils.cloud_utils import fetch_logs
from src.utils.loader_utils import load_csv, load_json, load_jsonl
from src.utils.logging_utils import get_logger, log_execution_time_and_path

logger = get_logger(__name__)

## ============================================================
## EXTRACTION CONSTANTS
## ============================================================
SourceType = Literal["cloud", "json", "jsonl", "csv"]

## ============================================================
## CLOUD EXTRACTION
## ============================================================
@log_execution_time_and_path
def extract_from_cloud(
    filter_query: str,
    limit: int = 5000,
) -> List[Dict[str, Any]]:
    """
        Extract logs from Google Cloud Logging

        High-level workflow:
            1) Query Cloud Logging with filter
            2) Convert entries to dict
            3) Return list of raw log records

        Args:
            filter_query: Logging filter expression
            limit: Maximum entries to fetch

        Returns:
            List of raw log entries
    """
    
    records = fetch_logs(
        filter_query=filter_query,
        limit=limit,
    )

    if not records:
        logger.warning("No logs returned from Cloud Logging query")

    return records

## ============================================================
## FILE EXTRACTION
## ============================================================
@log_execution_time_and_path
def extract_from_file(
    source_type: SourceType,
    file_path: str | Path,
) -> List[Dict[str, Any]] | pd.DataFrame:
    """
        Extract logs from local files

        High-level workflow:
            1) Load JSON / JSONL / CSV
            2) Return consistent raw structure

        Args:
            source_type: Source file type (json, jsonl, csv)
            file_path: Path to the file

        Returns:
            List of raw records (json/jsonl) or DataFrame (csv)
    """
    
    path = Path(file_path)

    if source_type == "json":
        return load_json(path)

    if source_type == "jsonl":
        return load_jsonl(path)

    if source_type == "csv":
        return load_csv(path)

    raise ExtractionError(f"Unsupported source_type: {source_type}")

## ============================================================
## PUBLIC EXTRACTION ENTRYPOINT
## ============================================================
@log_execution_time_and_path
def extract_logs(
    source: SourceType,
    file_path: Optional[str | Path] = None,
    filter_query: Optional[str] = None,
    limit: int = 5000,
) -> List[Dict[str, Any]] | pd.DataFrame:
    """
        Extract logs from selected source

        High-level workflow:
            1) If source=cloud, require filter_query
            2) If source=file, require file_path
            3) Return extracted raw dataset

        Args:
            source: Extraction source (cloud/json/jsonl/csv)
            file_path: File path for local extraction
            filter_query: Cloud Logging filter query
            limit: Maximum number of cloud entries

        Returns:
            Extracted raw dataset (list of dicts or DataFrame)
    """
    
    if source == "cloud":
        if not filter_query:
            raise ExtractionError("filter_query is required when source='cloud'")

        return extract_from_cloud(
            filter_query=filter_query,
            limit=limit,
        )

    if not file_path:
        raise ExtractionError("file_path is required for file-based sources")

    return extract_from_file(
        source_type=source,
        file_path=file_path,
    )