'''
__author__ = "Georges Nassopoulos"
__copyright__ = None
__version__ = "1.0.0"
__email__ = "georges.nassopoulos@gmail.com"
__status__ = "Dev"
__desc__ = "Statistical utilities for monitoring metrics: mean/std, IQR, extremes and streaming stats."
'''

from __future__ import annotations

from typing import Tuple, Union

import numpy as np
import pandas as pd

from src.utils.logging_utils import get_logger

try:
    from src.core.errors import ValidationError, DataError
except Exception:
    ValidationError = ValueError
    DataError = RuntimeError

## ============================================================
## LOGGER
## ============================================================
logger = get_logger("stats_utils")

## ============================================================
## BASIC STATISTICS
## ============================================================
def compute_mean_std(series: pd.Series) -> Tuple[float, float]:
    """
        Compute mean and standard deviation

        High-level workflow:
            1) Validate input
            2) Convert to numeric
            3) Compute mean and std

        Args:
            series: Numerical pandas Series

        Returns:
            Tuple (mean, std)
    """

    raw = None

    try:
        ## validate input
        if series is None or len(series) == 0:
            logger.error("Empty series provided")
            raise ValidationError("Series is empty")

        ## convert to numeric
        raw = series
        s = series.astype(float)

        ## compute stats
        mean = float(s.mean())
        std = float(s.std(ddof=0))

        logger.debug(f"Mean={mean}, Std={std}")

        return mean, std

    except ValidationError:
        raise

    except Exception as exc:
        logger.exception(f"Error computing mean/std: {exc}")
        raise DataError("Failed to compute mean/std") from exc

## ============================================================
## IQR COMPUTATION
## ============================================================
def compute_iqr_bounds(
    series: pd.Series,
    multiplier: float = 1.5,
) -> Tuple[float, float]:
    """
        Compute IQR bounds

        High-level workflow:
            1) Validate input
            2) Compute Q1 and Q3
            3) Compute IQR bounds

        Args:
            series: Numerical pandas Series
            multiplier: IQR multiplier

        Returns:
            Tuple (lower_bound, upper_bound)
    """

    raw = None

    try:
        ## validate input
        if series is None or len(series) == 0:
            logger.error("Empty series for IQR")
            raise ValidationError("Series is empty")

        ## convert
        raw = series
        s = series.astype(float)

        ## compute quartiles
        q1 = float(s.quantile(0.25))
        q3 = float(s.quantile(0.75))
        iqr = q3 - q1

        ## compute bounds
        lower = q1 - multiplier * iqr
        upper = q3 + multiplier * iqr

        logger.debug(f"IQR bounds: {lower}, {upper}")

        return lower, upper

    except ValidationError:
        raise

    except Exception as exc:
        logger.exception(f"Error computing IQR: {exc}")
        raise DataError("Failed to compute IQR") from exc

## ============================================================
## EXTREME VALUES
## ============================================================
def detect_extremes(series: pd.Series) -> Tuple[float, float]:
    """
        Detect extreme values

        High-level workflow:
            1) Validate input
            2) Compute min and max

        Args:
            series: Numerical pandas Series

        Returns:
            Tuple (min_value, max_value)
    """

    try:
        ## validate input
        if series is None or len(series) == 0:
            logger.error("Empty series for extremes")
            raise ValidationError("Series is empty")

        ## convert
        s = series.astype(float)

        ## compute extremes
        min_val = float(s.min())
        max_val = float(s.max())

        logger.debug(f"Extremes: min={min_val}, max={max_val}")

        return min_val, max_val

    except ValidationError:
        raise

    except Exception as exc:
        logger.exception(f"Error detecting extremes: {exc}")
        raise DataError("Failed to detect extremes") from exc

## ============================================================
## WINSORIZATION
## ============================================================
def winsorize_series(
    series: pd.Series,
    lower: float,
    upper: float,
) -> pd.Series:
    """
        Apply winsorization

        High-level workflow:
            1) Validate input
            2) Clip values within bounds

        Args:
            series: Numerical pandas Series
            lower: Lower bound
            upper: Upper bound

        Returns:
            Winsorized Series
    """

    try:
        ## validate input
        if series is None or len(series) == 0:
            logger.error("Empty series for winsorization")
            raise ValidationError("Series is empty")

        ## convert
        s = series.astype(float)

        ## clip values
        clipped = s.clip(lower=lower, upper=upper)

        logger.debug("Winsorization applied")

        return clipped

    except ValidationError:
        raise

    except Exception as exc:
        logger.exception(f"Error during winsorization: {exc}")
        raise DataError("Winsorization failed") from exc

## ============================================================
## STREAMING STATS
## ============================================================
def update_running_stats(
    current_mean: float,
    current_count: int,
    new_value: Union[int, float],
) -> Tuple[float, int]:
    """
        Update running mean (streaming)

        High-level workflow:
            1) Validate inputs
            2) Update count
            3) Update mean incrementally

        Args:
            current_mean: Current mean
            current_count: Current count
            new_value: New value

        Returns:
            Tuple (mean, count)
    """

    try:
        ## validate inputs
        if current_count < 0:
            logger.error("Invalid count")
            raise ValidationError("Count must be >= 0")

        ## update count
        new_count = current_count + 1

        ## update mean
        new_mean = current_mean + (new_value - current_mean) / new_count

        logger.debug(f"Updated mean={new_mean}, count={new_count}")

        return new_mean, new_count

    except ValidationError:
        raise

    except Exception as exc:
        logger.exception(f"Streaming stats error: {exc}")
        raise DataError("Streaming stats failed") from exc