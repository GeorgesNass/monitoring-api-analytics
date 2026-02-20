'''
__author__ = "Georges Nassopoulos"
__copyright__ = None
__version__ = "1.0.0"
__email__ = "georges.nassopoulos@gmail.com"
__status__ = "Dev"
__desc__ = "Unit tests for ETL normalization and load layers (pytest)."
'''

from __future__ import annotations

from pathlib import Path

import pandas as pd
import pytest

from src.etl.load import load_to_sqlite
from src.etl.transform import normalize_logs

## ============================================================
## TEST FIXTURES
## ============================================================
@pytest.fixture()
def tmp_sqlite_path(tmp_path: Path) -> Path:
    """
        Provide a temporary sqlite path for tests

        High-level workflow:
            1) Create sqlite file path under pytest temp directory
            2) Return resolved path

        Args:
            tmp_path: pytest temporary directory

        Returns:
            Temporary sqlite database path
    """
    
    return tmp_path / "test_monitoring.db"

@pytest.fixture()
def raw_minimal_record() -> dict:
    """
        Provide a minimal raw record with missing fields

        High-level workflow:
            1) Build minimal record without status_code, latency_ms, remote_ip
            2) Leave trace missing to trigger default fallback behavior

        Returns:
            Minimal raw record dictionary
    """
    
    return {
        "timestamp": "2026-02-17T10:00:00",
        "service": "api-service",
        "api_path": "/v1/test",
        "request_method": "GET",
        ## Missing: status_code, latency_ms, remote_ip
        ## Missing: trace -> will fallback to default-trace
    }

@pytest.fixture()
def raw_http_request_record() -> dict:
    """
        Provide a raw record simulating Cloud Run httpRequest fields

        High-level workflow:
            1) Build record with httpRequest.status, httpRequest.latency, httpRequest.remoteIp
            2) Include resource_type to validate env mapping
            3) Include trace to ensure fallback logic is not used

        Returns:
            Raw record dictionary containing httpRequest
    """
    
    return {
        "timestamp": "2026-02-17T11:00:00",
        "resource_type": "cloud_run_revision",
        "service": "cloudrun-service",
        "trace": "trace-123",
        "httpRequest": {
            "status": 404,
            "requestMethod": "POST",
            "requestUrl": "https://example.com/v1/resource",
            "remoteIp": "1.2.3.4",
            "userAgent": "pytest-agent",
            "latency": 123,
        },
    }

## ============================================================
## NORMALIZATION TESTS
## ============================================================
def test_normalize_logs_generates_fallback_values(
    raw_minimal_record: dict,
) -> None:
    """
        Ensure normalization generates deterministic realistic fallback values

        High-level workflow:
            1) Normalize a minimal record
            2) Verify fallback fields exist
            3) Validate fallback values are realistic

        Args:
            raw_minimal_record: Minimal raw record missing core fields
    """
    
    ## Normalize raw record
    df = normalize_logs([raw_minimal_record])

    ## Validate DataFrame structure
    assert isinstance(df, pd.DataFrame)
    assert len(df) == 1

    row = df.iloc[0].to_dict()

    ## Validate fallback fields exist
    assert "status_code" in row
    assert "latency_ms" in row
    assert "remote_ip" in row

    ## Validate types
    assert isinstance(row["status_code"], int)
    assert isinstance(row["latency_ms"], int)
    assert isinstance(row["remote_ip"], str)

    ## Validate deterministic realistic values
    assert row["latency_ms"] >= 80
    assert row["status_code"] in {200, 201, 204, 401, 403, 404, 422, 429, 500, 503}
    assert row["remote_ip"].startswith("10.")

def test_normalize_logs_uses_http_request_fields(
    raw_http_request_record: dict,
) -> None:
    """
        Ensure normalization uses httpRequest fields when present

        High-level workflow:
            1) Normalize record containing httpRequest
            2) Verify status_code, latency_ms and remote_ip come from httpRequest
            3) Verify env uses record resource_type

        Args:
            raw_http_request_record: Record containing httpRequest fields
    """
    
    ## Normalize record
    df = normalize_logs([raw_http_request_record])

    ## Validate DataFrame structure
    assert len(df) == 1

    row = df.iloc[0].to_dict()

    ## Validate values extracted from httpRequest
    assert row["status_code"] == 404
    assert row["remote_ip"] == "1.2.3.4"
    assert row["latency_ms"] == 123

    ## Validate env mapping
    assert row["env"] == "cloud_run_revision"

def test_normalize_logs_returns_expected_columns(
    raw_minimal_record: dict,
) -> None:
    """
        Ensure normalized output includes expected schema columns

        High-level workflow:
            1) Normalize minimal record
            2) Validate output schema contains required columns

        Args:
            raw_minimal_record: Minimal raw record missing core fields
    """
    
    df = normalize_logs([raw_minimal_record])

    expected_cols = {
        "event_timestamp",
        "env",
        "service",
        "version",
        "api_name",
        "api_method",
        "api_path",
        "request_method",
        "request_url",
        "status_code",
        "latency_ms",
        "remote_ip",
        "user_agent",
        "user_uid",
        "trace",
        "payload_size",
    }

    ## Validate schema columns
    assert expected_cols.issubset(set(df.columns))

## ============================================================
## SQLITE LOAD TESTS
## ============================================================
def test_load_to_sqlite_creates_db_and_table(
    tmp_sqlite_path: Path,
    raw_minimal_record: dict,
) -> None:
    """
        Ensure loading to sqlite creates db file and inserts rows

        High-level workflow:
            1) Normalize minimal record
            2) Load dataset into sqlite
            3) Verify sqlite file exists and is non-empty

        Args:
            tmp_sqlite_path: Temporary sqlite database path
            raw_minimal_record: Minimal raw record missing core fields
    """
    
    ## Normalize
    df = normalize_logs([raw_minimal_record])

    ## Load into sqlite
    load_to_sqlite(
        df=df,
        sqlite_path=tmp_sqlite_path,
        table_name="normalized_requests",
    )

    ## Validate sqlite file created
    assert tmp_sqlite_path.exists()
    assert tmp_sqlite_path.stat().st_size > 0

def test_load_to_sqlite_appends_rows(
    tmp_sqlite_path: Path,
    raw_minimal_record: dict,
) -> None:
    """
        Ensure sqlite loader appends rows without crashing

        High-level workflow:
            1) Normalize same record twice
            2) Load twice using append mode
            3) Verify sqlite file exists and remains non-empty

        Args:
            tmp_sqlite_path: Temporary sqlite database path
            raw_minimal_record: Minimal raw record missing core fields
    """
    
    ## Normalize twice
    df1 = normalize_logs([raw_minimal_record])
    df2 = normalize_logs([raw_minimal_record])

    ## Append twice
    load_to_sqlite(
        df=df1,
        sqlite_path=tmp_sqlite_path,
        table_name="normalized_requests",
    )
    load_to_sqlite(
        df=df2,
        sqlite_path=tmp_sqlite_path,
        table_name="normalized_requests",
    )

    ## Validate sqlite file created
    assert tmp_sqlite_path.exists()
    assert tmp_sqlite_path.stat().st_size > 0
