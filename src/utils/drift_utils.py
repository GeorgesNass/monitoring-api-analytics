'''
__author__ = "Georges Nassopoulos"
__copyright__ = None
__version__ = "1.0.0"
__email__ = "georges.nassopoulos@gmail.com"
__status__ = "Dev"
__desc__ = "Drift utilities for API monitoring: statistical tests, metrics extraction and reporting."
'''

from __future__ import annotations

from typing import Dict, Tuple, Any

import json
import numpy as np
import pandas as pd

from pathlib import Path
from scipy.stats import ks_2samp, chi2_contingency
from evidently.report import Report
from evidently.metric_preset import DataDriftPreset

from src.utils.logging_utils import get_logger

try:
    from src.core.errors import ValidationError, DataError
except Exception:
    ValidationError = ValueError
    DataError = RuntimeError

## ============================================================
## LOGGER
## ============================================================
logger = get_logger("drift_utils")

def compute_ks_test(ref: pd.Series, cur: pd.Series) -> Tuple[float, float]:
    """
        Compute Kolmogorov-Smirnov test

        Args:
            ref: Reference series
            cur: Current series

        Returns:
            statistic, p_value
    """

    ## drop NaN values
    ref_clean = ref.dropna()
    cur_clean = cur.dropna()

    ## handle empty cases
    if ref_clean.empty or cur_clean.empty:
        return 0.0, 1.0

    ## compute KS test
    stat, p_value = ks_2samp(ref_clean, cur_clean)

    return float(stat), float(p_value)

def compute_chi2_test(ref: pd.Series, cur: pd.Series) -> Tuple[float, float]:
    """
        Compute Chi-square test

        Args:
            ref: Reference series
            cur: Current series

        Returns:
            statistic, p_value
    """

    ## compute value counts
    ref_counts = ref.value_counts()
    cur_counts = cur.value_counts()

    ## align categories
    all_index = ref_counts.index.union(cur_counts.index)
    ref_aligned = ref_counts.reindex(all_index, fill_value=0)
    cur_aligned = cur_counts.reindex(all_index, fill_value=0)

    ## build contingency table
    table = np.array([ref_aligned.values, cur_aligned.values])

    ## handle invalid table
    if table.sum() == 0:
        return 0.0, 1.0

    ## compute Chi2 test
    stat, p_value, _, _ = chi2_contingency(table)

    return float(stat), float(p_value)

def compute_api_metrics_stats(df: pd.DataFrame) -> pd.DataFrame:
    """
        Compute API monitoring metrics for drift detection

        Args:
            df: Input dataset

        Returns:
            DataFrame with aggregated API metrics
    """

    if df.empty:
        return pd.DataFrame()

    ## initialize container
    data: Dict[str, Any] = {}

    ## latency stats
    if "latency" in df.columns:
        data["latency_mean"] = df["latency"].astype(float)
        data["latency_std"] = df["latency"].astype(float)

    ## error rate
    if "status_code" in df.columns:
        data["is_error"] = df["status_code"].astype(int) >= 400

    ## payload size
    if "payload_size" in df.columns:
        data["payload_size"] = df["payload_size"].astype(float)

    ## throughput proxy (requests count per batch)
    data["request_count"] = pd.Series([len(df)] * len(df))

    return pd.DataFrame(data)

def generate_drift_report(metrics: Dict[str, Any], output_dir: str = "reports") -> Dict[str, str]:
    """
        Generate drift report files (JSON + HTML)

        Args:
            metrics: Drift metrics
            output_dir: Output directory

        Returns:
            Dict with report paths
    """

    ## ensure directory exists
    path = Path(output_dir)
    path.mkdir(parents=True, exist_ok=True)

    ## JSON report
    json_path = path / "drift_report.json"
    with open(json_path, "w", encoding="utf-8") as f:
        json.dump(metrics, f, indent=2)

    ## HTML report
    html_path = path / "drift_report.html"
    html_content = "<html><body><h1>API Data Drift Report</h1><pre>"
    html_content += json.dumps(metrics, indent=2)
    html_content += "</pre></body></html>"

    with open(html_path, "w", encoding="utf-8") as f:
        f.write(html_content)

    return {
        "report_json": str(json_path),
        "report_html": str(html_path),
    }
    
def generate_evidently_report(
    df_ref: pd.DataFrame,
    df_cur: pd.DataFrame,
    output_dir: str = "reports",
) -> Dict[str, str]:
    """
        Generate Evidently data drift report for API monitoring

        High-level workflow:
            1) Initialize Evidently report
            2) Run drift analysis
            3) Save HTML report

        Args:
            df_ref: Reference dataset
            df_cur: Current dataset
            output_dir: Output directory

        Returns:
            Dictionary with report path
    """

    report = Report(metrics=[DataDriftPreset()])

    report.run(
        reference_data=df_ref,
        current_data=df_cur,
    )

    path = Path(output_dir)
    path.mkdir(parents=True, exist_ok=True)

    html_path = path / "evidently_report.html"
    report.save_html(str(html_path))

    return {
        "evidently_html": str(html_path),
    }