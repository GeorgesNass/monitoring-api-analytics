'''
__author__ = "Georges Nassopoulos"
__copyright__ = None
__version__ = "1.0.0"
__email__ = "georges.nassopoulos@gmail.com"
__status__ = "Dev"
__desc__ = "Centralized custom exceptions and structured helpers for the monitoring API analytics project."
'''

from __future__ import annotations

from pathlib import Path
from typing import Any, Dict, Optional, Type

from src.utils.logging_utils import get_logger

## ============================================================
## LOGGER
## ============================================================
logger = get_logger("errors")

## ============================================================
## ERROR CODES
## ============================================================
ERROR_CODE_CONFIGURATION = "configuration_error"
ERROR_CODE_VALIDATION = "validation_error"
ERROR_CODE_RESOURCE_NOT_FOUND = "resource_not_found"
ERROR_CODE_EXTRACTION = "extraction_error"
ERROR_CODE_TRANSFORMATION = "transformation_error"
ERROR_CODE_LOAD = "load_error"
ERROR_CODE_STORAGE = "storage_error"
ERROR_CODE_EXTERNAL_SERVICE = "external_service_error"
ERROR_CODE_PIPELINE = "pipeline_error"
ERROR_CODE_INTERNAL = "internal_error"

## ============================================================
## BASE APPLICATION ERROR
## ============================================================
class MonitoringBaseError(Exception):
    """
        Base exception for monitoring application

        High-level workflow:
            1) Provide a unified application error type
            2) Support structured error handling across the project
            3) Preserve contextual details for debugging and logging

        Args:
            message: Human-readable error message
            error_code: Normalized application error code
            details: Optional structured error context
            cause: Original exception if available
            is_retryable: Whether retry may succeed
    """

    def __init__(
        self,
        message: str,
        error_code: str = ERROR_CODE_INTERNAL,
        details: Optional[Dict[str, Any]] = None,
        cause: Optional[Exception] = None,
        is_retryable: bool = False,
    ) -> None:
        ## Store normalized error metadata
        self.message = message
        self.error_code = error_code
        self.details = details or {}
        self.cause = cause
        self.is_retryable = is_retryable

        super().__init__(self.message)

    def to_dict(self) -> Dict[str, Any]:
        """
            Convert the exception into a structured dictionary

            Returns:
                A normalized error payload
        """

        return {
            "error_type": self.__class__.__name__,
            "message": self.message,
            "error_code": self.error_code,
            "details": self.details,
            "cause_type": self.cause.__class__.__name__
            if self.cause
            else None,
            "is_retryable": self.is_retryable,
        }

## ============================================================
## CONFIGURATION ERRORS
## ============================================================
class ConfigurationError(MonitoringBaseError):
    """
        Raised when configuration validation fails

        Args:
            message: Description of missing or invalid configuration
            error_code: Normalized application error code
            details: Optional structured error context
            cause: Original exception if available
            is_retryable: Whether retry may succeed
    """

## ============================================================
## EXTRACTION ERRORS
## ============================================================
class ExtractionError(MonitoringBaseError):
    """
        Raised during log extraction phase

        Args:
            message: Description of extraction failure
            error_code: Normalized application error code
            details: Optional structured error context
            cause: Original exception if available
            is_retryable: Whether retry may succeed
    """

## ============================================================
## TRANSFORMATION ERRORS
## ============================================================
class TransformationError(MonitoringBaseError):
    """
        Raised during data normalization phase

        Args:
            message: Description of transformation failure
            error_code: Normalized application error code
            details: Optional structured error context
            cause: Original exception if available
            is_retryable: Whether retry may succeed
    """

## ============================================================
## LOAD ERRORS
## ============================================================
class LoadError(MonitoringBaseError):
    """
        Raised during data loading phase

        Design choice:
            - Covers BigQuery, SQLite, parquet, CSV or equivalent sinks
            - Keeps one main error family for persistence failures

        Args:
            message: Description of loading failure
            error_code: Normalized application error code
            details: Optional structured error context
            cause: Original exception if available
            is_retryable: Whether retry may succeed
    """

## ============================================================
## VALIDATION ERRORS
## ============================================================
class DataValidationError(MonitoringBaseError):
    """
        Raised when data integrity validation fails

        Args:
            message: Description of validation issue
            error_code: Normalized application error code
            details: Optional structured error context
            cause: Original exception if available
            is_retryable: Whether retry may succeed
    """

## ============================================================
## ADDITIONAL ERRORS
## ============================================================
class ResourceNotFoundError(MonitoringBaseError):
    """
        Raised when a required file, folder or SQL artifact is missing
    """

class StorageError(MonitoringBaseError):
    """
        Raised when a storage backend access or write operation fails
    """

class ExternalServiceError(MonitoringBaseError):
    """
        Raised when a remote service or provider fails
    """

class PipelineError(MonitoringBaseError):
    """
        Raised when orchestration of the ETL workflow fails
    """

class UnknownMonitoringError(MonitoringBaseError):
    """
        Raised when an unexpected exception must be normalized
    """

## ============================================================
## GENERIC HELPERS
## ============================================================
def raise_project_error(
    exc_type: Type[MonitoringBaseError],
    message: str,
    *,
    error_code: str,
    details: Optional[Dict[str, Any]] = None,
    cause: Optional[Exception] = None,
    is_retryable: bool = False,
) -> None:
    """
        Log and raise a structured project exception

        High-level workflow:
            1) Build a normalized payload
            2) Add original cause metadata when available
            3) Log the error in a consistent format
            4) Raise the target project exception

        Args:
            exc_type: Exception class to raise
            message: Human-readable error message
            error_code: Normalized application error code
            details: Optional contextual details
            cause: Original exception if available
            is_retryable: Whether retry may succeed

        Raises:
            MonitoringBaseError: Always
    """

    ## Build a normalized payload
    payload = details.copy() if details else {}

    ## Attach original cause metadata when available
    if cause is not None:
        payload["cause_message"] = str(cause)
        payload["cause_type"] = cause.__class__.__name__

    ## Emit a structured error log
    logger.error(
        "Monitoring API analytics error | type=%s | code=%s | "
        "message=%s | retryable=%s | details=%s",
        exc_type.__name__,
        error_code,
        message,
        is_retryable,
        payload,
    )

    ## Raise the target project exception
    raise exc_type(
        message=message,
        error_code=error_code,
        details=payload,
        cause=cause,
        is_retryable=is_retryable,
    )

def wrap_exception(
    exc: Exception,
    *,
    exc_type: Type[MonitoringBaseError],
    message: str,
    error_code: str,
    details: Optional[Dict[str, Any]] = None,
    is_retryable: bool = False,
) -> MonitoringBaseError:
    """
        Wrap a raw exception into a structured project exception

        High-level workflow:
            1) Preserve the original exception
            2) Merge it into the structured payload
            3) Return a normalized project error instance

        Args:
            exc: Original exception
            exc_type: Target structured exception type
            message: Human-readable error message
            error_code: Normalized application error code
            details: Optional contextual details
            is_retryable: Whether retry may succeed

        Returns:
            A structured project exception instance
    """

    ## Start from existing details when provided
    payload = details.copy() if details else {}

    ## Attach original cause metadata
    payload["cause_message"] = str(exc)
    payload["cause_type"] = exc.__class__.__name__

    ## Return a normalized wrapped exception
    return exc_type(
        message=message,
        error_code=error_code,
        details=payload,
        cause=exc,
        is_retryable=is_retryable,
    )

def log_unhandled_exception(
    exc: Exception,
    *,
    context: Optional[Dict[str, Any]] = None,
) -> UnknownMonitoringError:
    """
        Normalize an unexpected exception into a project-specific error

        High-level workflow:
            1) Build a safe execution context
            2) Preserve the original exception metadata
            3) Log the unhandled failure
            4) Return a normalized unknown error

        Args:
            exc: Original unexpected exception
            context: Optional execution context

        Returns:
            A normalized unknown project exception
    """

    ## Build a safe payload from optional context
    payload = context.copy() if context else {}

    ## Attach original cause metadata
    payload["cause_message"] = str(exc)
    payload["cause_type"] = exc.__class__.__name__

    ## Log the unexpected failure
    logger.error(
        "Unhandled monitoring-api-analytics exception | type=%s | "
        "details=%s",
        exc.__class__.__name__,
        payload,
    )
    logger.debug("Unhandled traceback", exc_info=True)

    ## Return a normalized unknown project error
    return UnknownMonitoringError(
        message="An unexpected monitoring-api-analytics error occurred",
        error_code=ERROR_CODE_INTERNAL,
        details=payload,
        cause=exc,
        is_retryable=False,
    )

## ============================================================
## SPECIALIZED HELPERS
## ============================================================
def log_and_raise_configuration_error(
    message: str,
    *,
    details: Optional[Dict[str, Any]] = None,
) -> None:
    """
        Log and raise a configuration error

        Args:
            message: Human-readable configuration error message
            details: Optional configuration context

        Raises:
            ConfigurationError: Always
    """

    ## Raise a structured configuration error
    raise_project_error(
        exc_type=ConfigurationError,
        message=message,
        error_code=ERROR_CODE_CONFIGURATION,
        details=details,
        is_retryable=False,
    )

def log_and_raise_missing_path(
    path: str | Path,
    *,
    resource_name: str = "Required resource",
) -> None:
    """
        Log and raise a missing resource error

        Args:
            path: Missing filesystem path
            resource_name: Human-readable resource label

        Raises:
            ResourceNotFoundError: Always
    """

    ## Normalize the path for logs and payloads
    normalized_path = str(Path(path))

    ## Raise a structured missing resource error
    raise_project_error(
        exc_type=ResourceNotFoundError,
        message=f"{resource_name} not found",
        error_code=ERROR_CODE_RESOURCE_NOT_FOUND,
        details={"path": normalized_path},
        is_retryable=False,
    )

def log_and_raise_extraction_error(
    message: str,
    *,
    details: Optional[Dict[str, Any]] = None,
    cause: Optional[Exception] = None,
) -> None:
    """
        Log and raise an extraction error

        Args:
            message: Human-readable extraction error message
            details: Optional extraction context
            cause: Original exception if available

        Raises:
            ExtractionError: Always
    """

    ## Raise a structured extraction error
    raise_project_error(
        exc_type=ExtractionError,
        message=message,
        error_code=ERROR_CODE_EXTRACTION,
        details=details,
        cause=cause,
        is_retryable=True,
    )

def log_and_raise_transformation_error(
    message: str,
    *,
    details: Optional[Dict[str, Any]] = None,
    cause: Optional[Exception] = None,
) -> None:
    """
        Log and raise a transformation error

        Args:
            message: Human-readable transformation error message
            details: Optional transformation context
            cause: Original exception if available

        Raises:
            TransformationError: Always
    """

    ## Raise a structured transformation error
    raise_project_error(
        exc_type=TransformationError,
        message=message,
        error_code=ERROR_CODE_TRANSFORMATION,
        details=details,
        cause=cause,
        is_retryable=False,
    )

def log_and_raise_load_error(
    message: str,
    *,
    details: Optional[Dict[str, Any]] = None,
    cause: Optional[Exception] = None,
) -> None:
    """
        Log and raise a load error

        Args:
            message: Human-readable load error message
            details: Optional load context
            cause: Original exception if available

        Raises:
            LoadError: Always
    """

    ## Raise a structured load error
    raise_project_error(
        exc_type=LoadError,
        message=message,
        error_code=ERROR_CODE_LOAD,
        details=details,
        cause=cause,
        is_retryable=True,
    )

def log_and_raise_storage_error(
    message: str,
    *,
    details: Optional[Dict[str, Any]] = None,
    cause: Optional[Exception] = None,
) -> None:
    """
        Log and raise a storage error

        Args:
            message: Human-readable storage error message
            details: Optional storage context
            cause: Original exception if available

        Raises:
            StorageError: Always
    """

    ## Raise a structured storage error
    raise_project_error(
        exc_type=StorageError,
        message=message,
        error_code=ERROR_CODE_STORAGE,
        details=details,
        cause=cause,
        is_retryable=True,
    )

def log_and_raise_validation_error(
    message: str,
    *,
    details: Optional[Dict[str, Any]] = None,
) -> None:
    """
        Log and raise a validation error

        Args:
            message: Human-readable validation error message
            details: Optional validation context

        Raises:
            DataValidationError: Always
    """

    ## Raise a structured validation error
    raise_project_error(
        exc_type=DataValidationError,
        message=message,
        error_code=ERROR_CODE_VALIDATION,
        details=details,
        is_retryable=False,
    )

def log_and_raise_external_service_error(
    message: str,
    *,
    details: Optional[Dict[str, Any]] = None,
    cause: Optional[Exception] = None,
) -> None:
    """
        Log and raise an external service error

        Args:
            message: Human-readable external service error message
            details: Optional service context
            cause: Original exception if available

        Raises:
            ExternalServiceError: Always
    """

    ## Raise a structured external service error
    raise_project_error(
        exc_type=ExternalServiceError,
        message=message,
        error_code=ERROR_CODE_EXTERNAL_SERVICE,
        details=details,
        cause=cause,
        is_retryable=True,
    )

def log_and_raise_pipeline_error(
    message: str,
    *,
    details: Optional[Dict[str, Any]] = None,
    cause: Optional[Exception] = None,
) -> None:
    """
        Log and raise a pipeline error

        Args:
            message: Human-readable pipeline error message
            details: Optional pipeline context
            cause: Original exception if available

        Raises:
            PipelineError: Always
    """

    ## Raise a structured pipeline error
    raise_project_error(
        exc_type=PipelineError,
        message=message,
        error_code=ERROR_CODE_PIPELINE,
        details=details,
        cause=cause,
        is_retryable=False,
    )