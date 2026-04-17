'''
__author__ = "Georges Nassopoulos"
__copyright__ = None
__version__ = "1.0.0"
__email__ = "georges.nassopoulos@gmail.com"
__status__ = "Dev"
__desc__ = "Centralized anomaly detection for monitoring metrics: z-score, IQR, streaming checks."
'''

from __future__ import annotations

from typing import Any, Dict, List, Optional, Union

import numpy as np
import pandas as pd

from src.utils.logging_utils import get_logger
from src.utils.stats_utils import (
    compute_mean_std,
    compute_iqr_bounds,
    update_running_stats,
)

try:
    from src.core.errors import ValidationError, DataError
except Exception:
    ValidationError = ValueError
    DataError = RuntimeError

## ============================================================
## LOGGER
## ============================================================
logger = get_logger("data_quality")

## ============================================================
## ISSUE HANDLING
## ============================================================
def _create_issue(
    rule: str,
    level: str,
    message: str,
    details: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """
        Create standardized issue object

        High-level workflow:
            1) Build issue dictionary
            2) Attach optional details

        Args:
            rule: Rule name
            level: Severity level
            message: Description
            details: Optional metadata

        Returns:
            Issue dictionary
    """

    issue = {
        "rule": rule,
        "level": level,
        "message": message,
        "details": details or {},
    }

    logger.debug(f"Issue created: {rule} - {level}")

    return issue

def _add_issue(
    issues: List[Dict[str, Any]],
    rule: str,
    level: str,
    message: str,
    details: Optional[Dict[str, Any]] = None,
) -> None:
    """
        Append issue and log it

        Args:
            issues: Issue list
            rule: Rule name
            level: Severity
            message: Description
            details: Metadata
    """

    issue = _create_issue(rule, level, message, details)
    issues.append(issue)

    if level == "error":
        logger.error(f"{rule} - {message}")
    else:
        logger.warning(f"{rule} - {message}")

## ============================================================
## Z-SCORE DETECTION
## ============================================================
def _detect_zscore(
    series: pd.Series,
    threshold: float,
    issues: List[Dict[str, Any]],
    column: str,
) -> pd.Series:
    """
        Detect outliers using z-score

        High-level workflow:
            1) Compute mean and std
            2) Compute z-score
            3) Flag anomalies

        Args:
            series: Numerical pandas Series
            threshold: Z-score threshold
            issues: Issue container
            column: Column name

        Returns:
            Boolean mask of outliers
    """

    ## compute statistics
    mean, std = compute_mean_std(series)

    if std == 0:
        return pd.Series(False, index=series.index)

    ## compute z-score
    z_scores = (series - mean) / std
    mask = z_scores.abs() > threshold

    if mask.any():
        _add_issue(
            issues,
            "zscore_outliers",
            "warning",
            f"Outliers detected in column {column}",
            {"count": int(mask.sum())},
        )

    return mask

## ============================================================
## IQR DETECTION
## ============================================================
def _detect_iqr(
    series: pd.Series,
    multiplier: float,
    issues: List[Dict[str, Any]],
    column: str,
) -> pd.Series:
    """
        Detect outliers using IQR

        High-level workflow:
            1) Compute bounds
            2) Flag anomalies

        Args:
            series: Numerical pandas Series
            multiplier: IQR multiplier
            issues: Issue container
            column: Column name

        Returns:
            Boolean mask of outliers
    """

    ## compute bounds
    lower, upper = compute_iqr_bounds(series, multiplier)

    ## detect anomalies
    mask = (series < lower) | (series > upper)

    if mask.any():
        _add_issue(
            issues,
            "iqr_outliers",
            "warning",
            f"IQR outliers detected in column {column}",
            {"count": int(mask.sum())},
        )

    return mask

## ============================================================
## MAIN ENTRYPOINT
## ============================================================
def run_data_quality(
    data: Union[pd.DataFrame, List[float], np.ndarray, Dict[str, Any]],
    method: str = "zscore",
    z_threshold: float = 3.0,
    iqr_multiplier: float = 1.5,
    strict: bool = False,
) -> Dict[str, Any]:
    """
        Run anomaly detection pipeline for monitoring metrics

        High-level workflow:
            1) Normalize input data
            2) Detect invalid values (NaN / inf)
            3) Apply anomaly detection
            4) Compute anomaly score
            5) Return structured result

        Design choice:
            - Optimized for API monitoring metrics
            - Supports streaming-friendly structures

        Args:
            data: Input dataset (dict / DataFrame / array)
            method: Detection method (zscore or iqr)
            z_threshold: Z-score threshold
            iqr_multiplier: IQR multiplier
            strict: Raise error if anomalies detected

        Returns:
            Result dictionary with issues and score
    """

    issues: List[Dict[str, Any]] = []

    try:
        ## normalize input
        if isinstance(data, dict):
            df = pd.DataFrame([data])
        elif not isinstance(data, pd.DataFrame):
            df = pd.DataFrame(data)
        else:
            df = data.copy()

        ## select numeric columns
        columns = df.select_dtypes(include=[np.number]).columns.tolist()

        if not columns:
            logger.warning("No numeric columns found")
            return {"is_valid": True, "issues": [], "score": 1.0}

        global_mask = pd.Series(False, index=df.index)

        ## iterate columns
        for col in columns:
            series = df[col].astype(float)

            ## detect invalid values
            invalid_mask = series.isna() | np.isinf(series)

            if invalid_mask.any():
                _add_issue(
                    issues,
                    "invalid_values",
                    "error",
                    f"NaN or inf detected in {col}",
                    {"count": int(invalid_mask.sum())},
                )

            ## anomaly detection
            if method == "zscore":
                mask = _detect_zscore(series, z_threshold, issues, col)
            elif method == "iqr":
                mask = _detect_iqr(series, iqr_multiplier, issues, col)
            else:
                raise ValidationError("Invalid anomaly detection method")

            global_mask = global_mask | mask | invalid_mask

        ## compute score
        score = 1.0 - float(global_mask.mean())

        ## streaming hint (log only)
        if len(df) == 1:
            logger.debug("Single-point (streaming-like) data processed")

        errors = [i for i in issues if i["level"] == "error"]

        result = {
            "is_valid": len(errors) == 0,
            "errors": len(errors),
            "warnings": len(issues) - len(errors),
            "score": score,
            "issues": issues,
        }

        logger.info(f"Data quality score: {score}")

        if strict and errors:
            logger.error("Strict mode failure")
            raise ValidationError("Data quality failed")

        return result

    except ValidationError:
        raise

    except Exception as exc:
        logger.exception(f"Unexpected error: {exc}")
        raise DataError("Data quality pipeline failed") from exc