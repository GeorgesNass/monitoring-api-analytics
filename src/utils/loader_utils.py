'''
__author__ = "Georges Nassopoulos"
__copyright__ = None
__version__ = "1.0.0"
__email__ = "georges.nassopoulos@gmail.com"
__status__ = "Dev"
__desc__ = "Safe CSV / JSON / JSONL loading utilities with validation and structured logging."
'''

import json
from pathlib import Path
from typing import Any, Dict, List

import pandas as pd

from src.core.errors import ExtractionError
from src.utils.logging_utils import get_logger

logger = get_logger(__name__)

## ============================================================
## FILE VALIDATION
## ============================================================
def validate_file_exists(file_path: Path) -> None:
    """
        Validate file existence before loading

        High-level workflow:
            1) Check if file exists
            2) Raise ExtractionError if missing

        Args:
            file_path: Path to file

        Raises:
            ExtractionError: If file does not exist
    """
    
    if not file_path.exists():
        logger.error("File not found: %s", file_path)
        raise ExtractionError(f"File not found: {file_path}")

## ============================================================
## JSON LOADING
## ============================================================
def load_json(file_path: str | Path) -> List[Dict[str, Any]]:
    """
        Load JSON file safely

        High-level workflow:
            1) Validate file
            2) Load JSON content
            3) Return parsed data

        Args:
            file_path: Path to JSON file

        Returns:
            List of JSON records
    """
    
    path = Path(file_path)
    validate_file_exists(path)

    try:
        with path.open("r", encoding="utf-8") as file:
            data = json.load(file)

        logger.info("Loaded JSON file: %s", path)
        return data

    except Exception as exc:
        logger.exception("Error loading JSON file")
        raise ExtractionError(str(exc)) from exc

## ============================================================
## JSONL LOADING
## ============================================================
def load_jsonl(file_path: str | Path) -> List[Dict[str, Any]]:
    """
        Load JSONL file safely

        High-level workflow:
            1) Validate file
            2) Parse line-by-line JSON
            3) Return list of records

        Args:
            file_path: Path to JSONL file

        Returns:
            List of JSON records
    """
    
    path = Path(file_path)
    validate_file_exists(path)

    records: List[Dict[str, Any]] = []

    try:
        with path.open("r", encoding="utf-8") as file:
            for line in file:
                if line.strip():
                    records.append(json.loads(line))

        logger.info("Loaded JSONL file: %s", path)
        return records

    except Exception as exc:
        logger.exception("Error loading JSONL file")
        raise ExtractionError(str(exc)) from exc

## ============================================================
## CSV LOADING
## ============================================================
def load_csv(file_path: str | Path) -> pd.DataFrame:
    """
        Load CSV file safely into pandas DataFrame

        High-level workflow:
            1) Validate file
            2) Read CSV into DataFrame
            3) Return structured DataFrame

        Args:
            file_path: Path to CSV file

        Returns:
            pandas DataFrame
    """
    
    path = Path(file_path)
    validate_file_exists(path)

    try:
        df = pd.read_csv(path)
        logger.info("Loaded CSV file: %s", path)
        return df

    except Exception as exc:
        logger.exception("Error loading CSV file")
        raise ExtractionError(str(exc)) from exc