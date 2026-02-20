'''
__author__ = "Georges Nassopoulos"
__copyright__ = None
__version__ = "1.0.0"
__email__ = "georges.nassopoulos@gmail.com"
__status__ = "Dev"
__desc__ = "Centralized logging configuration with structured formatting and execution time decorator."
'''

import logging
import os
import sys
import time
from functools import wraps
from pathlib import Path
from typing import Callable, Any

## ============================================================
## LOGGER CONFIGURATION
## ============================================================
LOG_DIR = Path("logs")
LOG_DIR.mkdir(parents=True, exist_ok=True)

LOG_FILE = LOG_DIR / "application.log"

def get_logger(name: str) -> logging.Logger:
    """
        Create and configure a logger instance

        High-level workflow:
            1) Create logger
            2) Attach console handler
            3) Attach file handler
            4) Apply unified formatter

        Args:
            name: Logger name (usually __name__)

        Returns:
            Configured logging.Logger instance
    """
    
    logger = logging.getLogger(name)
    logger.setLevel(logging.DEBUG)

    if logger.handlers:
        return logger

    formatter = logging.Formatter(
        fmt="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    ## Console handler
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(logging.INFO)
    console_handler.setFormatter(formatter)

    ## File handler
    file_handler = logging.FileHandler(LOG_FILE)
    file_handler.setLevel(logging.DEBUG)
    file_handler.setFormatter(formatter)

    logger.addHandler(console_handler)
    logger.addHandler(file_handler)
    logger.propagate = False

    return logger

## ============================================================
## EXECUTION TIME DECORATOR
## ============================================================
def log_execution_time_and_path(func: Callable) -> Callable:
    """
        Decorator to log execution time and file path on error

        High-level workflow:
            1) Capture start time
            2) Execute function
            3) Log execution duration
            4) Log absolute path on exception

        Args:
            func: Function to wrap

        Returns:
            Wrapped function with logging
    """

    @wraps(func)
    def wrapper(*args: Any, **kwargs: Any) -> Any:
        logger = get_logger(func.__module__)
        start_time = time.time()

        try:
            result = func(*args, **kwargs)
            duration = round(time.time() - start_time, 4)
            logger.info(
                "Function '%s' executed in %s seconds",
                func.__name__,
                duration,
            )
            return result

        except Exception as exc:
            duration = round(time.time() - start_time, 4)
            abs_path = Path(os.getcwd()).resolve()

            logger.error(
                "Error in function '%s' after %s seconds",
                func.__name__,
                duration,
            )
            logger.error("Execution path: %s", abs_path)

            ## Console clean: log only message (no stacktrace)
            logger.error("%s", str(exc))

            ## Keep traceback only in DEBUG (file handler is DEBUG)
            if logger.isEnabledFor(logging.DEBUG):
                logger.debug("Traceback:", exc_info=True)

            raise

    return wrapper