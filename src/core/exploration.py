'''
__author__ = "Georges Nassopoulos"
__copyright__ = None
__version__ = "1.0.0"
__email__ = "georges.nassopoulos@gmail.com"
__status__ = "Dev"
__desc__ = "Exploratory analysis utilities for monitoring datasets: latency, status codes, endpoints, and time aggregations."
'''

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Optional
import pandas as pd

from src.core.config import settings
from src.utils.logging_utils import get_logger, log_execution_time_and_path

## Initialize module logger
logger = get_logger(__name__)

## ============================================================
## DEFAULT OUTPUT PATHS
## ============================================================
@dataclass(frozen=True)
class ExplorationPaths:
    """
        Centralize default export locations for exploration outputs

        Attributes:
            exports_dir: Root folder for exported exploration files
            tables_dir: Folder for CSV summary tables
    """

    exports_dir: Path
    tables_dir: Path


def _build_default_paths() -> ExplorationPaths:
    """
        Build default exploration export paths

        Returns:
            ExplorationPaths with resolved folders
    """
    
    ## FIX: settings.paths does not exist in config.py
    exports_dir = Path("artifacts") / "exports" / "exploration"

    ## Define tables directory for CSV outputs
    tables_dir = exports_dir / "tables"

    ## Ensure directories exist on disk
    exports_dir.mkdir(parents=True, exist_ok=True)
    tables_dir.mkdir(parents=True, exist_ok=True)

    return ExplorationPaths(exports_dir=exports_dir, tables_dir=tables_dir)

## ============================================================
## VALIDATION HELPERS
## ============================================================
def _ensure_columns(df: pd.DataFrame, required: list[str]) -> None:
    """
        Validate required columns exist in DataFrame

        Args:
            df: Input DataFrame
            required: List of required columns

        Raises:
            ValueError: If one or more required columns are missing
    """

    missing = [c for c in required if c not in df.columns]
    if missing:
        raise ValueError(f"Missing required columns: {missing}")


def _safe_to_datetime(series: pd.Series) -> pd.Series:
    """
        Convert a series to pandas datetime safely

        Args:
            series: Input series

        Returns:
            Datetime series (NaT for invalid rows)
    """

    return pd.to_datetime(series, errors="coerce", utc=True)

## ============================================================
## CORE EXPLORATION TABLES
## ============================================================
@log_execution_time_and_path
def compute_status_code_distribution(df: pd.DataFrame) -> pd.DataFrame:
    """
        Compute status code distribution

        High-level workflow:
            1) Validate presence of status_code
            2) Count occurrences
            3) Compute percentages

        Args:
            df: Normalized monitoring DataFrame

        Returns:
            DataFrame with status_code, total_requests, pct
    """
    
    ## Validate required columns
    _ensure_columns(df, ["status_code"])

    ## Build status code counts table
    out = (
        df.assign(status_code=df["status_code"].astype("Int64"))
        .groupby("status_code", dropna=False)
        .size()
        .reset_index(name="total_requests")
        .sort_values("total_requests", ascending=False)
        .reset_index(drop=True)
    )

    ## Compute denominator for percent column
    total = int(out["total_requests"].sum()) if not out.empty else 0

    ## Compute percent column safely (avoid division by zero)
    out["pct"] = out["total_requests"].apply(lambda x: float(x) / total if total else 0.0)

    return out


@log_execution_time_and_path
def compute_status_family_distribution(df: pd.DataFrame) -> pd.DataFrame:
    """
        Compute HTTP status family distribution (2xx/3xx/4xx/5xx)

        Args:
            df: Normalized monitoring DataFrame

        Returns:
            DataFrame with status_family, total_requests, pct
    """
    
    ## Validate required columns
    _ensure_columns(df, ["status_code"])

    ## Cast status codes to nullable integer
    status = df["status_code"].astype("Int64")

    ## Compute family bucket labels (e.g., 2xx, 4xx)
    family = (status // 100).astype("Int64").astype(str) + "xx"

    ## Aggregate counts by family
    out = (
        pd.DataFrame({"status_family": family})
        .groupby("status_family", dropna=False)
        .size()
        .reset_index(name="total_requests")
        .sort_values("total_requests", ascending=False)
        .reset_index(drop=True)
    )

    ## Compute denominator for percent column
    total = int(out["total_requests"].sum()) if not out.empty else 0

    ## Compute percent column safely (avoid division by zero)
    out["pct"] = out["total_requests"].apply(lambda x: float(x) / total if total else 0.0)

    return out


@log_execution_time_and_path
def compute_latency_summary(df: pd.DataFrame) -> pd.DataFrame:
    """
        Compute latency summary statistics

        Args:
            df: Normalized monitoring DataFrame

        Returns:
            One-row DataFrame with count, mean, p50, p95, p99, min, max
    """
    
    ## Validate required columns
    _ensure_columns(df, ["latency_ms"])

    ## Convert to numeric and drop invalid values
    lat = pd.to_numeric(df["latency_ms"], errors="coerce").dropna()

    ## Return empty-stat row if no valid latency values are available
    if lat.empty:
        return pd.DataFrame(
            [{
                "count": 0,
                "mean_ms": None,
                "p50_ms": None,
                "p95_ms": None,
                "p99_ms": None,
                "min_ms": None,
                "max_ms": None,
            }]
        )

    ## Compute summary metrics in a single-row dataframe
    return pd.DataFrame(
        [{
            "count": int(lat.shape[0]),
            "mean_ms": float(lat.mean()),
            "p50_ms": float(lat.quantile(0.50)),
            "p95_ms": float(lat.quantile(0.95)),
            "p99_ms": float(lat.quantile(0.99)),
            "min_ms": float(lat.min()),
            "max_ms": float(lat.max()),
        }]
    )


@log_execution_time_and_path
def compute_top_endpoints(
    df: pd.DataFrame,
    top_k: int = 20,
    sort_by: str = "p95_latency_ms",
) -> pd.DataFrame:
    """
        Compute top endpoints table with request counts and latency percentiles

        High-level workflow:
            1) Validate required columns
            2) Group by api_path
            3) Aggregate latency percentiles and request counts
            4) Sort and return top-k

        Args:
            df: Normalized monitoring DataFrame
            top_k: Maximum number of endpoints to return
            sort_by: Column used to sort results (p95_latency_ms, p99_latency_ms, total_requests)

        Returns:
            DataFrame with api_path, total_requests, avg_latency_ms, p50_latency_ms, p95_latency_ms, p99_latency_ms
    """
    
    ## Validate required columns
    _ensure_columns(df, ["api_path", "latency_ms"])

    ## Enforce a safe default when invalid top_k is passed
    if top_k <= 0:
        top_k = 20

    ## Prepare a copy and ensure latency_ms is numeric
    tmp = df.copy()
    tmp["latency_ms"] = pd.to_numeric(tmp["latency_ms"], errors="coerce")

    ## Drop rows without endpoint path
    tmp = tmp.dropna(subset=["api_path"])

    ## Build percentile aggregation helper
    def _p(q: float) -> callable:
        return lambda s: float(pd.to_numeric(s, errors="coerce").dropna().quantile(q)) if s is not None else None

    ## Aggregate per-endpoint request counts and latency stats
    out = (
        tmp.groupby("api_path")
        .agg(
            total_requests=("api_path", "size"),
            avg_latency_ms=("latency_ms", "mean"),
            p50_latency_ms=("latency_ms", _p(0.50)),
            p95_latency_ms=("latency_ms", _p(0.95)),
            p99_latency_ms=("latency_ms", _p(0.99)),
        )
        .reset_index()
    )

    ## Fallback to default sort column if an invalid one is provided
    if sort_by not in out.columns:
        sort_by = "p95_latency_ms"

    ## Sort and return only the top_k endpoints
    out = out.sort_values(sort_by, ascending=False).head(int(top_k)).reset_index(drop=True)
    return out


@log_execution_time_and_path
def compute_hourly_volume(df: pd.DataFrame) -> pd.DataFrame:
    """
        Compute hourly request volume

        Args:
            df: Normalized monitoring DataFrame

        Returns:
            DataFrame with hour_ts and total_requests
    """
    
    ## Validate required columns
    _ensure_columns(df, ["event_timestamp"])

    ## Convert timestamps safely to UTC datetime
    ts = _safe_to_datetime(df["event_timestamp"])

    ## Aggregate request counts by hour
    out = (
        pd.DataFrame({"event_timestamp": ts})
        .dropna()
        .assign(hour_ts=lambda x: x["event_timestamp"].dt.floor("H"))
        .groupby("hour_ts")
        .size()
        .reset_index(name="total_requests")
        .sort_values("hour_ts")
        .reset_index(drop=True)
    )
    return out


@log_execution_time_and_path
def compute_daily_volume(df: pd.DataFrame) -> pd.DataFrame:
    """
        Compute daily request volume

        Args:
            df: Normalized monitoring DataFrame

        Returns:
            DataFrame with day and total_requests
    """
    
    ## Validate required columns
    _ensure_columns(df, ["event_timestamp"])

    ## Convert timestamps safely to UTC datetime
    ts = _safe_to_datetime(df["event_timestamp"])

    ## Aggregate request counts by day
    out = (
        pd.DataFrame({"event_timestamp": ts})
        .dropna()
        .assign(day=lambda x: x["event_timestamp"].dt.date)
        .groupby("day")
        .size()
        .reset_index(name="total_requests")
        .sort_values("day")
        .reset_index(drop=True)
    )
    return out


## ============================================================
## EXPORT UTILITIES
## ============================================================
def _export_csv(df: pd.DataFrame, out_path: Path) -> None:
    """
        Export DataFrame to CSV with safe directory creation

        Args:
            df: DataFrame to export
            out_path: Output file path
    """
    
    out_path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(out_path, index=False)


@log_execution_time_and_path
def run_exploration(
    df: pd.DataFrame,
    output_dir: Optional[str | Path] = None,
    top_k_endpoints: int = 20,
) -> dict[str, Path]:
    """
        Run a lightweight exploration suite and export results to CSV

        High-level workflow:
            1) Compute core distributions and summaries
            2) Export tables to artifacts/exports/exploration/tables
            3) Return map of exported file paths

        Args:
            df: Normalized monitoring DataFrame
            output_dir: Optional override output directory
            top_k_endpoints: Number of endpoints for top list

        Returns:
            Dict mapping table name to exported CSV path
    """
    
    ## Build default export directories
    paths = _build_default_paths()

    ## Override output directory when explicitly provided by caller
    if output_dir is not None:
        out_root = Path(output_dir).expanduser().resolve()
        tables_dir = out_root / "tables"

        ## Create directories if they do not exist yet
        out_root.mkdir(parents=True, exist_ok=True)
        tables_dir.mkdir(parents=True, exist_ok=True)

        ## Replace default export paths with custom ones
        paths = ExplorationPaths(exports_dir=out_root, tables_dir=tables_dir)

    logger.info("Running exploration | output_dir=%s", str(paths.exports_dir))
    exports: dict[str, Path] = {}

    ## Compute exploration tables
    status_code_df = compute_status_code_distribution(df)
    status_family_df = compute_status_family_distribution(df)
    latency_df = compute_latency_summary(df)
    top_endpoints_df = compute_top_endpoints(df, top_k=top_k_endpoints, sort_by="p95_latency_ms")
    hourly_volume_df = compute_hourly_volume(df)
    daily_volume_df = compute_daily_volume(df)

    ## Register all computed tables for export
    tables = {
        "status_codes": status_code_df,
        "status_families": status_family_df,
        "latency_summary": latency_df,
        "top_endpoints": top_endpoints_df,
        "hourly_volume": hourly_volume_df,
        "daily_volume": daily_volume_df,
    }

    ## Export each table to CSV and track output file paths
    for name, table_df in tables.items():
    
        out_path = paths.tables_dir / f"{name}.csv"
        _export_csv(table_df, out_path)
        exports[name] = out_path

        logger.info("Exploration export: %s -> %s", name, str(out_path))

    logger.info("Exploration completed")

    return exports