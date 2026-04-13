'''
__author__ = "Georges Nassopoulos"
__copyright__ = None
__version__ = "1.0.0"
__email__ = "georges.nassopoulos@gmail.com"
__status__ = "Dev"
__desc__ = "Utility functions for normalization, schema validation, cross-source checks, business rules and data quality for monitoring API analytics."
'''

from __future__ import annotations

import re
from datetime import datetime
from typing import Any, Dict, List

from src.utils import get_logger
from src.core.errors import ValidationError, DataError

## ============================================================
## LOGGER INITIALIZATION
## ============================================================
logger = get_logger("data_utils")

def normalize_data(data: Dict[str, Any]) -> Dict[str, Any]:
    """
        Normalize data fields

        Args:
            data: Input dictionary

        Returns:
            Normalized dictionary
    """

    ## Initialize normalized container
    normalized = {}

    for key, value in data.items():

        ## Normalize string values
        if isinstance(value, str):
            logger.debug(f"Normalizing string field: {key}")
            value = value.strip().lower()
            value = re.sub(r"\s+", " ", value)

        ## Normalize list values
        if isinstance(value, list):
            logger.debug(f"Normalizing list field: {key}")
            value = [
                v.strip().lower() if isinstance(v, str) else v
                for v in value
            ]

        ## Store normalized value
        normalized[key] = value

    return normalized

def validate_schema(data: Dict[str, Any]) -> List[Dict]:
    """
        Validate required schema fields

        Args:
            data: Input dictionary

        Returns:
            List of issues
    """

    ## Initialize schema validation
    issues = []
    required_fields = ["timestamp", "endpoint", "status_code"]

    for field in required_fields:

        ## Check missing fields
        if field not in data:
            logger.error(f"Missing required field: {field}")
            issues.append({
                "rule": "schema",
                "level": "error",
                "message": f"Missing field: {field}",
            })

    return issues

def validate_types(data: Dict[str, Any]) -> List[Dict]:
    """
        Validate field types

        Args:
            data: Input dictionary

        Returns:
            List of issues
    """

    ## Initialize type validation
    issues = []

    ## Validate timestamp type
    if "timestamp" in data and not isinstance(data["timestamp"], (str, datetime)):
        logger.error("Invalid type for timestamp")
        issues.append({
            "rule": "type_timestamp",
            "level": "error",
            "message": "timestamp must be string or datetime",
        })

    ## Validate status_code type
    if "status_code" in data and not isinstance(data["status_code"], int):
        logger.error("Invalid type for status_code")
        issues.append({
            "rule": "type_status_code",
            "level": "error",
            "message": "status_code must be int",
        })

    ## Validate latency
    if "latency" in data and not isinstance(data["latency"], (int, float)):
        logger.error("Invalid type for latency")
        issues.append({
            "rule": "type_latency",
            "level": "error",
            "message": "latency must be numeric",
        })

    return issues

def parse_date(value: Any) -> Any:
    """
        Try to parse date string

        Args:
            value: Input value

        Returns:
            datetime or original value
    """

    ## Return early for non-string values
    if not isinstance(value, str):
        return value

    formats = ["%Y-%m-%d", "%Y-%m-%d %H:%M:%S"]

    for fmt in formats:
        try:
            parsed = datetime.strptime(value, fmt)
            logger.debug(f"Parsed date: {value}")
            return parsed
        except Exception:
            continue

    logger.warning(f"Failed to parse date: {value}")
    return value

def compare_sources(data: Dict[str, Any]) -> List[Dict]:
    """
        Compare values from multiple sources

        Args:
            data: Input dictionary

        Returns:
            List of issues
    """

    ## Initialize cross-source issues
    issues = []

    ## Compare API vs computed metrics
    if "success_rate" in data and "total_requests" in data and "success_requests" in data:

        total = data["total_requests"]
        success = data["success_requests"]

        if total > 0:
            computed = success / total

            if abs(computed - data["success_rate"]) > 0.05:
                logger.warning("Cross-source mismatch on success_rate")
                issues.append({
                    "rule": "cross_success_rate",
                    "level": "warning",
                    "message": "Mismatch between computed and reported success_rate",
                })

    return issues

def check_business_rules(data: Dict[str, Any]) -> List[Dict]:
    """
        Apply business rules

        Args:
            data: Input dictionary

        Returns:
            List of issues
    """

    ## Initialize rules
    issues = []

    ## Status code range
    if "status_code" in data:
        if not (100 <= data["status_code"] <= 599):
            logger.error("Invalid HTTP status_code")
            issues.append({
                "rule": "business_status_code",
                "level": "error",
                "message": "status_code must be between 100 and 599",
            })

    ## Latency rule
    if "latency" in data:
        if data["latency"] < 0:
            logger.error("Negative latency detected")
            issues.append({
                "rule": "business_latency",
                "level": "error",
                "message": "latency must be >= 0",
            })

    return issues

def compute_quality_score(data: Dict[str, Any]) -> float:
    """
        Compute quality score

        Args:
            data: Input dictionary

        Returns:
            Score
    """

    ## Basic completeness score
    filled_fields = sum(1 for v in data.values() if v is not None)
    total_fields = len(data)

    if total_fields == 0:
        logger.warning("Empty data for quality score")
        return 0.0

    score = filled_fields / total_fields

    logger.debug(f"Quality score: {score}")

    return score

def detect_duplicates(data: Dict[str, Any]) -> List[Any]:
    """
        Detect duplicate values

        Args:
            data: Input dictionary

        Returns:
            List of duplicates
    """

    ## Initialize tracking
    seen = set()
    duplicates = []

    for value in data.values():

        ## Skip complex structures
        if isinstance(value, (list, dict)):
            continue

        ## Detect duplicates
        if value in seen:
            logger.warning(f"Duplicate detected: {value}")
            duplicates.append(value)
        else:
            seen.add(value)

    return duplicates