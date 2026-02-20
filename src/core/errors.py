'''
__author__ = "Georges Nassopoulos"
__copyright__ = None
__version__ = "1.0.0"
__email__ = "georges.nassopoulos@gmail.com"
__status__ = "Dev"
__desc__ = "Centralized custom exceptions for the monitoring API analytics project."
'''


## ============================================================
## BASE APPLICATION ERROR
## ============================================================
class MonitoringBaseError(Exception):
    """
        Base exception for monitoring application

        High-level workflow:
            1) Provide unified error type
            2) Allow structured error handling
            3) Support extension via inheritance

        Args:
            message: Human-readable error message
    """

    def __init__(self, message: str) -> None:
        self.message = message
        super().__init__(self.message)

## ============================================================
## CONFIGURATION ERRORS
## ============================================================
class ConfigurationError(MonitoringBaseError):
    """
        Raised when configuration validation fails

        Args:
            message: Description of missing or invalid configuration
    """

## ============================================================
## EXTRACTION ERRORS
## ============================================================
class ExtractionError(MonitoringBaseError):
    """
        Raised during log extraction phase

        Args:
            message: Description of extraction failure
    """

## ============================================================
## TRANSFORMATION ERRORS
## ============================================================
class TransformationError(MonitoringBaseError):
    """
        Raised during data normalization phase

        Args:
            message: Description of transformation failure
    """

## ============================================================
## LOAD ERRORS
## ============================================================
class LoadError(MonitoringBaseError):
    """
        Raised during data loading phase (BigQuery or SQLite)

        Args:
            message: Description of loading failure
    """

## ============================================================
## VALIDATION ERRORS
## ============================================================
class DataValidationError(MonitoringBaseError):
    """
        Raised when data integrity validation fails

        Args:
            message: Description of validation issue
    """