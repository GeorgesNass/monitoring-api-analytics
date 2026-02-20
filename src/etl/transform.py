'''
__author__ = "Georges Nassopoulos"
__copyright__ = None
__version__ = "1.0.0"
__email__ = "georges.nassopoulos@gmail.com"
__status__ = "Dev"
__desc__ = "Normalization layer converting heterogeneous logs (Cloud, JSON, CSV) into a unified structured schema."
'''

import hashlib
from datetime import datetime
from typing import Any, Dict, List

import pandas as pd

from src.core.errors import TransformationError
from src.utils.logging_utils import get_logger, log_execution_time_and_path

logger = get_logger(__name__)

## ============================================================
## HELPER FUNCTIONS
## ============================================================
def _stable_hash(value: str) -> int:
    """
        Generate stable hash integer from string

        Args:
            value: Input string

        Returns:
            Deterministic integer hash
    """
    
    return int(hashlib.md5(value.encode()).hexdigest(), 16)

def _generate_realistic_status(trace: str) -> int:
    """
        Generate deterministic but realistic HTTP status

        Args:
            trace: Unique trace identifier

        Returns:
            HTTP status code
    """
    
    h = _stable_hash(trace) % 10

    mapping = {
        0: 500,
        1: 503,
        2: 404,
        3: 401,
        4: 403,
        5: 422,
        6: 429,
        7: 200,
        8: 201,
        9: 204,
    }

    return mapping[h]

def _generate_realistic_latency(trace: str) -> int:
    """
        Generate deterministic realistic latency

        Args:
            trace: Unique trace identifier

        Returns:
            Latency in milliseconds
    """
    
    return (_stable_hash(trace) % 1500) + 80

def _generate_realistic_ip(trace: str) -> str:
    """
        Generate deterministic private IP

        Args:
            trace: Unique trace identifier

        Returns:
            Private IP string
    """
    
    h = _stable_hash(trace)
    return f"10.{h % 255}.{(h // 10) % 255}.{(h // 100) % 255}"

## ============================================================
## NORMALIZATION CORE
## ============================================================
@log_execution_time_and_path
def normalize_logs(
    raw_data: List[Dict[str, Any]] | pd.DataFrame,
) -> pd.DataFrame:
    """
        Normalize heterogeneous logs into unified schema

        High-level workflow:
            1) Iterate over records
            2) Extract structured fields
            3) Apply fallback generation if missing
            4) Return normalized DataFrame

        Args:
            raw_data: Extracted logs (list or DataFrame)

        Returns:
            Normalized pandas DataFrame
    """
    try:
        if isinstance(raw_data, pd.DataFrame):
            records = raw_data.to_dict(orient="records")
        else:
            records = raw_data

        normalized_rows: List[Dict[str, Any]] = []

        for record in records:
            trace = str(record.get("trace", "default-trace"))

            timestamp = record.get("timestamp")
            if isinstance(timestamp, str):
                try:
                    timestamp = datetime.fromisoformat(timestamp)
                except Exception:
                    timestamp = datetime.utcnow()

            status_code = (
                record.get("status_code")
                or record.get("httpRequest", {}).get("status")
            )

            latency_ms = (
                record.get("latency_ms")
                or record.get("httpRequest", {}).get("latency")
            )

            remote_ip = (
                record.get("remote_ip")
                or record.get("httpRequest", {}).get("remoteIp")
            )

            ## Apply deterministic fallback if missing
            if not status_code:
                status_code = _generate_realistic_status(trace)

            if not latency_ms:
                latency_ms = _generate_realistic_latency(trace)

            if not remote_ip:
                remote_ip = _generate_realistic_ip(trace)

            normalized_rows.append(
                {
                    "event_timestamp": timestamp or datetime.utcnow(),
                    "env": record.get("resource_type", "unknown"),
                    "service": record.get("service", "unknown"),
                    "version": record.get("version", "v1"),
                    "api_name": record.get("api_name", "unknown"),
                    "api_method": record.get("api_method", "unknown"),
                    "api_path": record.get("api_path", "/unknown"),
                    "request_method": record.get("request_method", "GET"),
                    "request_url": record.get("request_url", "/unknown"),
                    "status_code": int(status_code),
                    "latency_ms": int(latency_ms),
                    "remote_ip": remote_ip,
                    "user_agent": record.get("user_agent", "unknown"),
                    "user_uid": record.get("user_uid", "anonymous"),
                    "trace": trace,
                    "payload_size": record.get("payload_size", 0),
                }
            )

        df = pd.DataFrame(normalized_rows)

        logger.info("Normalization complete with %s rows", len(df))

        return df

    except Exception as exc:
        logger.exception("Error during normalization")
        raise TransformationError(str(exc)) from exc