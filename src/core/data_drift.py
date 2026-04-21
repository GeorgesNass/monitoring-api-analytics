'''
__author__ = "Georges Nassopoulos"
__copyright__ = None
__version__ = "1.0.0"
__email__ = "georges.nassopoulos@gmail.com"
__status__ = "Dev"
__desc__ = "Centralized API monitoring data drift detection: latency, errors and endpoint metrics."
'''

from __future__ import annotations

from typing import Any, Dict, List, Optional

import pandas as pd

from src.utils.logging_utils import get_logger
from src.utils.drift_utils import (
    compute_ks_test,
    compute_chi2_test,
    compute_api_metrics_stats,
    generate_evidently_report
)

try:
    from src.core.errors import ValidationError, DataError
except Exception:
    ValidationError = ValueError
    DataError = RuntimeError

## ============================================================
## LOGGER
## ============================================================
logger = get_logger("data_drift")

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

        Args:
            rule: Rule name
            level: Severity level
            message: Description
            details: Optional metadata

        Returns:
            Issue dictionary
    """

    ## build issue structure
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

    ## create issue
    issue = _create_issue(rule, level, message, details)

    ## append issue
    issues.append(issue)

    ## log depending on severity
    if level == "error":
        logger.error(f"{rule} - {message}")
    else:
        logger.warning(f"{rule} - {message}")

## ============================================================
## DRIFT DETECTION
## ============================================================
def _detect_numeric_drift(
    ref: pd.Series,
    cur: pd.Series,
    column: str,
    threshold: float,
    issues: List[Dict[str, Any]],
) -> float:
    """
        Detect drift for numerical metric using KS test

        Args:
            ref: Reference series
            cur: Current series
            column: Metric name
            threshold: p-value threshold
            issues: Issue container

        Returns:
            p_value
    """

    ## compute KS test
    stat, p_value = compute_ks_test(ref, cur)

    ## detect drift
    if p_value < threshold:
        _add_issue(
            issues,
            "drift_numeric",
            "warning",
            f"Drift detected in metric {column}",
            {"p_value": float(p_value)},
        )

    return float(p_value)

def _detect_categorical_drift(
    ref: pd.Series,
    cur: pd.Series,
    column: str,
    threshold: float,
    issues: List[Dict[str, Any]],
) -> float:
    """
        Detect drift for categorical metric using Chi2 test

        Args:
            ref: Reference series
            cur: Current series
            column: Metric name
            threshold: p-value threshold
            issues: Issue container

        Returns:
            p_value
    """

    ## compute Chi2 test
    stat, p_value = compute_chi2_test(ref, cur)

    ## detect drift
    if p_value < threshold:
        _add_issue(
            issues,
            "drift_categorical",
            "warning",
            f"Drift detected in metric {column}",
            {"p_value": float(p_value)},
        )

    return float(p_value)

## ============================================================
## MAIN ENTRYPOINT
## ============================================================
def run_data_drift(
    df_ref: pd.DataFrame,
    df_current: pd.DataFrame,
    p_value_threshold: float = 0.05,
    strict: bool = False,
) -> Dict[str, Any]:
    """
        Run API monitoring data drift detection pipeline

        High-level workflow:
            1) Compute API metrics (latency, errors, throughput)
            2) Detect drift per metric
            3) Aggregate issues
            4) Compute drift score

        Args:
            df_ref: Reference dataset
            df_current: Current dataset
            p_value_threshold: Statistical threshold
            strict: Raise error if drift detected

        Returns:
            Result dictionary with drift score and issues
    """

    ## initialize issues
    issues: List[Dict[str, Any]] = []

    try:
        ## validate inputs
        if df_ref.empty or df_current.empty:
            raise ValidationError("Empty datasets provided")

        ## compute API metrics
        ref_stats = compute_api_metrics_stats(df_ref)
        cur_stats = compute_api_metrics_stats(df_current)

        drift_flags: List[bool] = []

        ## global metrics drift
        for col in ref_stats.columns:
            ref_series = ref_stats[col]
            cur_series = cur_stats[col]

            p_value = _detect_numeric_drift(
                ref_series, cur_series, col, p_value_threshold, issues
            )

            drift_flags.append(p_value < p_value_threshold)

        ## endpoint categorical drift
        if "endpoint" in df_ref.columns:
            p_value = _detect_categorical_drift(
                df_ref["endpoint"], df_current["endpoint"],
                "endpoint", p_value_threshold, issues
            )
            drift_flags.append(p_value < p_value_threshold)

        ## status code drift
        if "status_code" in df_ref.columns:
            p_value = _detect_categorical_drift(
                df_ref["status_code"], df_current["status_code"],
                "status_code", p_value_threshold, issues
            )
            drift_flags.append(p_value < p_value_threshold)

        ## compute global drift score
        drift_score = 1.0 - (sum(drift_flags) / len(drift_flags)) if drift_flags else 1.0

        ## extract errors
        errors = [i for i in issues if i["level"] == "error"]

        ## build result
        result = {
            "is_drift_ok": len(errors) == 0,
            "errors": len(errors),
            "warnings": len(issues) - len(errors),
            "drift_score": drift_score,
            "issues": issues,
        }

        logger.info(f"API drift score: {drift_score}")

        ## EVIDENTLY REPORT
        try:
            report_paths = generate_evidently_report(ref_stats, cur_stats)
            result["evidently_report"] = report_paths
        except Exception as e:
            logger.warning(f"Evidently failed: {e}")
            
        ## strict mode
        if strict and drift_score < 1.0:
            logger.error("Strict mode failure")
            raise ValidationError("Data drift detected")

        return result

    except ValidationError:
        raise

    except Exception as exc:
        logger.exception(f"Unexpected error: {exc}")
        raise DataError("Data drift pipeline failed") from exc