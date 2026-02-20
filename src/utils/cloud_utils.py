'''
__author__ = "Georges Nassopoulos"
__copyright__ = None
__version__ = "1.0.0"
__email__ = "georges.nassopoulos@gmail.com"
__status__ = "Dev"
__desc__ = "Cloud Logging extraction utilities using Google Cloud Logging API."
'''

from typing import List, Dict, Any, Optional
from pathlib import Path

from google.auth.exceptions import DefaultCredentialsError
from google.cloud import logging as gcp_logging
from google.oauth2 import service_account

from src.core.config import settings
from src.core.errors import ExtractionError
from src.utils.logging_utils import get_logger

logger = get_logger(__name__)

## ============================================================
## CLIENT FACTORY
## ============================================================
def create_logging_client() -> gcp_logging.Client:
    """
        Create Google Cloud Logging client

        High-level workflow:
            1) Load service account credentials
            2) Instantiate logging client
            3) Return client instance

        Returns:
            Google Cloud Logging client
    """

    try:
        if settings.google_credentials_path:
            credentials_path = Path(settings.google_credentials_path).expanduser().resolve()

            if not credentials_path.exists():
                raise ExtractionError(
                    f"Service account file not found: {credentials_path}. "
                    f"Please set GOOGLE_APPLICATION_CREDENTIALS correctly."
                )

            credentials = service_account.Credentials.from_service_account_file(
                credentials_path
            )

            client = gcp_logging.Client(
                project=settings.project_id,
                credentials=credentials,
            )
        else:
            client = gcp_logging.Client(
                project=settings.project_id,
            )

        logger.info("Cloud Logging client initialized")
        return client

    except ExtractionError:
        raise

    except Exception:
        ## Do NOT log traceback here (keep console clean)
        raise ExtractionError("Failed to initialize Cloud Logging client") from None
        
## ============================================================
## LOG EXTRACTION
## ============================================================
def fetch_logs(
    filter_query: str,
    limit: int = 1000,
    order_by: str = "timestamp desc",
) -> List[Dict[str, Any]]:
    """
        Fetch logs from Cloud Logging

        High-level workflow:
            1) Initialize logging client
            2) Apply filter query
            3) Iterate over entries
            4) Convert to structured dict list

        Args:
            filter_query: Logging filter expression
            limit: Maximum number of entries
            order_by: Sorting order

        Returns:
            List of log entries as dictionaries
    """
    client = create_logging_client()

    try:
        entries = client.list_entries(
            filter_=filter_query,
            order_by=order_by,
            page_size=limit,
        )

        results: List[Dict[str, Any]] = []

        for entry in entries:
            entry_dict: Dict[str, Any] = {
                "timestamp": entry.timestamp,
                "resource_type": entry.resource.type,
                "severity": entry.severity,
                "log_name": entry.log_name,
                "labels": entry.labels,
            }

            if hasattr(entry, "json_payload") and entry.json_payload:
                entry_dict["jsonPayload"] = dict(entry.json_payload)

            if hasattr(entry, "text_payload") and entry.text_payload:
                entry_dict["textPayload"] = entry.text_payload

            if hasattr(entry, "http_request") and entry.http_request:
                entry_dict["httpRequest"] = {
                    "status": entry.http_request.status,
                    "requestMethod": entry.http_request.request_method,
                    "requestUrl": entry.http_request.request_url,
                    "remoteIp": entry.http_request.remote_ip,
                    "userAgent": entry.http_request.user_agent,
                    "latency": entry.http_request.latency,
                }

            results.append(entry_dict)

        logger.info("Fetched %s log entries", len(results))
        return results

    except Exception as exc:
        logger.exception("Error fetching logs from Cloud Logging")
        raise ExtractionError(str(exc)) from exc