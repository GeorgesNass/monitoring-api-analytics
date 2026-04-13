'''
__author__ = "Georges Nassopoulos"
__copyright__ = None
__version__ = "1.0.0"
__email__ = "georges.nassopoulos@gmail.com"
__status__ = "Dev"
__desc__ = "Centralized data consistency checks for monitoring API analytics: schema, types, cross-source, business rules, metrics validation and data quality."
'''

from __future__ import annotations

from pathlib import Path
from typing import Any, Dict, List, Optional

from src.utils.logging_utils import get_logger
from src.utils.data_utils import (
    normalize_data,
    validate_schema,
    validate_types,
    compare_sources,
    check_business_rules,
    compute_quality_score,
    detect_duplicates,
)

try:
    from src.core.errors import ValidationError, DataError
except Exception:
    ValidationError = ValueError
    DataError = RuntimeError

## ============================================================
## LOGGER
## ============================================================
logger = get_logger("data_consistency")

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

    ## Build issue object
    issue = {
        "rule": rule,
        "level": level,
        "message": message,
        "details": details or {},
    }

    logger.debug(f"Issue created: {rule}")

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

    ## Create issue
    issue = _create_issue(rule, level, message, details)

    ## Append to list
    issues.append(issue)

    ## Log issue
    if level == "error":
        logger.error(f"{rule} - {message}")
    else:
        logger.warning(f"{rule} - {message}")

## ============================================================
## VALIDATIONS
## ============================================================
def _validate_file(
    file_path: Optional[str | Path],
    issues: List[Dict[str, Any]],
) -> Optional[Path]:
    """
        Validate file input

        Args:
            file_path: Path input
            issues: Issue list

        Returns:
            Path or None
    """

    ## Skip if not provided
    if file_path is None:
        logger.debug("No file path provided")
        return None

    path = Path(file_path)

    ## Check existence
    if not path.exists():
        logger.error(f"File not found: {path}")
        _add_issue(issues, "file_exists", "error", "File does not exist", {"file": str(path)})
        return None

    ## Check file type
    if not path.is_file():
        logger.error(f"Invalid file path: {path}")
        _add_issue(issues, "file_type", "error", "Path is not a file")
        return None

    return path

def _validate_metrics(
    data: Dict[str, Any],
    issues: List[Dict[str, Any]],
) -> None:
    """
        Validate monitoring metrics consistency

        Args:
            data: Input data
            issues: Issue list
    """

    ## Validate latency
    latency = data.get("latency")
    if latency is not None:
        if not isinstance(latency, (int, float)) or latency < 0:
            logger.error("Invalid latency value")
            _add_issue(issues, "metric_latency", "error", "Latency must be >= 0")

    ## Validate success rate
    success_rate = data.get("success_rate")
    if success_rate is not None:
        if not (0 <= success_rate <= 1):
            logger.error("Invalid success_rate")
            _add_issue(issues, "metric_success_rate", "error", "success_rate must be between 0 and 1")

    ## Validate count consistency
    total = data.get("total_requests")
    success = data.get("success_requests")

    if total is not None and success is not None:
        if success > total:
            logger.error("Invalid request counts")
            _add_issue(issues, "metric_counts", "error", "success_requests > total_requests")

    ## Validate recomputed success rate
    if total and success is not None and total > 0:
        computed = success / total
        if success_rate is not None and abs(computed - success_rate) > 0.05:
            logger.warning("Mismatch in success_rate computation")
            _add_issue(
                issues,
                "metric_consistency",
                "warning",
                "success_rate inconsistent with counts",
                {"computed": computed, "reported": success_rate},
            )

## ============================================================
## MAIN ENTRYPOINT
## ============================================================
def run_data_consistency(
    data: Dict[str, Any],
    file_path: Optional[str | Path] = None,
    strict: bool = False,
) -> Dict[str, Any]:
    """
        Run full consistency pipeline

        High-level workflow:
            1) Normalize data
            2) Validate schema and types
            3) Validate cross-source consistency
            4) Apply business rules
            5) Validate monitoring metrics
            6) Detect duplicates
            7) Compute quality score

        Args:
            data: Input data
            file_path: Optional file path
            strict: Raise error if inconsistency

        Returns:
            Result dictionary
    """

    ## Initialize issues
    issues: List[Dict[str, Any]] = []

    try:
        ## Normalize data
        data = normalize_data(data)

        ## Validate file
        path = _validate_file(file_path, issues)

        ## Validate schema
        schema_issues = validate_schema(data)
        for s in schema_issues:
            _add_issue(issues, s["rule"], s["level"], s["message"])

        ## Validate types
        type_issues = validate_types(data)
        for t in type_issues:
            _add_issue(issues, t["rule"], t["level"], t["message"])

        ## Validate cross-source
        cross_issues = compare_sources(data)
        for c in cross_issues:
            _add_issue(issues, c["rule"], c["level"], c["message"])

        ## Apply business rules
        business_issues = check_business_rules(data)
        for b in business_issues:
            _add_issue(issues, b["rule"], b["level"], b["message"])

        ## Validate monitoring metrics
        _validate_metrics(data, issues)

        ## Detect duplicates
        duplicates = detect_duplicates(data)
        if duplicates:
            _add_issue(issues, "duplicates", "warning", "Duplicate values detected", {"values": duplicates})

        ## Compute quality score
        quality_score = compute_quality_score(data)

        ## Count errors
        errors = [i for i in issues if i["level"] == "error"]

        ## Build result
        result = {
            "is_consistent": len(errors) == 0,
            "errors": len(errors),
            "warnings": len(issues) - len(errors),
            "quality_score": quality_score,
            "issues": issues,
            "file": str(path) if path else None,
        }

        logger.info(f"Consistency result: {result['is_consistent']}")

        ## Strict mode
        if strict and errors:
            logger.error("Strict mode failure")
            raise ValidationError("Data consistency failed")

        return result

    except ValidationError:
        raise

    except Exception as exc:
        logger.exception(f"Unexpected error: {exc}")
        raise DataError("Consistency pipeline failed") from exc