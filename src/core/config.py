'''
__author__ = "Georges Nassopoulos"
__copyright__ = None
__version__ = "1.0.0"
__email__ = "georges.nassopoulos@gmail.com"
__status__ = "Dev"
__desc__ = "Centralized environment configuration loader using .env and OS variables."
'''

import os
from pathlib import Path
from typing import Optional

from dotenv import load_dotenv

from src.utils.logging_utils import get_logger

## ============================================================
## ENVIRONMENT LOADING
## ============================================================
ENV_PATH = Path(".env")

if ENV_PATH.exists():
    load_dotenv(dotenv_path=ENV_PATH)

logger = get_logger(__name__)

## ============================================================
## CONFIGURATION CLASS
## ============================================================
class Settings:
    """
        Centralized configuration management

        High-level workflow:
            1) Load environment variables
            2) Validate required fields
            3) Provide typed access to configuration

        Design choice:
            - Use environment variables as single source of truth
            - Avoid duplication with YAML configuration

        Attributes:
            project_id: GCP project ID
            bigquery_dataset: Target BigQuery dataset
            bigquery_table: Target BigQuery table
            google_credentials_path: Path to service account JSON
            local_sqlite_path: Path to local SQLite DB
            environment: Deployment environment
    """

    def __init__(self) -> None:
        ## Core GCP settings
        self.project_id: Optional[str] = os.getenv("GCP_PROJECT_ID")
        self.bigquery_dataset: Optional[str] = os.getenv("BQ_DATASET")
        self.bigquery_table: Optional[str] = os.getenv("BQ_TABLE")

        ## Service account
        self.google_credentials_path: Optional[str] = os.getenv(
            "GOOGLE_APPLICATION_CREDENTIALS"
        )

        ## Local fallback
        self.local_sqlite_path: str = os.getenv(
            "SQLITE_DB_PATH",
            "data/processed/monitoring.db",
        )

        ## Environment
        self.environment: str = os.getenv(
            "APP_ENV",
            "dev",
        )

        ## Logging level
        self.log_level: str = os.getenv(
            "LOG_LEVEL",
            "INFO",
        )

        ## Sanitize placeholders to avoid breaking Google auth (DefaultCredentialsError)
        self.google_credentials_path = self._sanitize_placeholder(
            value=self.google_credentials_path,
            env_key="GOOGLE_APPLICATION_CREDENTIALS",
        )

        self.project_id = self._sanitize_placeholder(
            value=self.project_id,
            env_key="GCP_PROJECT_ID",
        )

        self.bigquery_dataset = self._sanitize_placeholder(
            value=self.bigquery_dataset,
            env_key="BQ_DATASET",
        )

        self.bigquery_table = self._sanitize_placeholder(
            value=self.bigquery_table,
            env_key="BQ_TABLE",
        )

        self._validate()

    ## ============================================================
    ## SANITIZE PLACEHOLDERS
    ## ============================================================
    def _sanitize_placeholder(self, value: Optional[str], env_key: str) -> Optional[str]:
        """
            Replace placeholder env values with None and remove env var to prevent
            Google client libraries from trying to load fake credential paths.

            Args:
                value: Raw env var value
                env_key: Environment variable key

            Returns:
                Cleaned value or None
        """

        if value is None:
            return None

        cleaned = value.strip()
        if not cleaned:
            return None

        is_placeholder = ("<" in cleaned and ">" in cleaned) or ("YOUR_" in cleaned)
        if is_placeholder:
            ## Remove the env var so google.auth.default() won't try to load it
            if env_key in os.environ:
                os.environ.pop(env_key, None)
            return None

        return cleaned

    ## ============================================================
    ## VALIDATION
    ## ============================================================
    def _validate(self) -> None:
        """
            Validate required configuration fields

            High-level workflow:
                1) Check mandatory variables
                2) Log warnings if missing
                3) Allow local-only execution if needed
        """
        
        if self.environment == "prod":
            required_fields = [
                self.project_id,
                self.bigquery_dataset,
                self.bigquery_table,
                self.google_credentials_path,
            ]

            if not all(required_fields):
                logger.error(
                    "Missing required environment variables for production"
                )
                raise ValueError(
                    "Missing required environment variables for production"
                )

        logger.info("Configuration loaded successfully")

## ============================================================
## GLOBAL SETTINGS INSTANCE
## ============================================================
settings = Settings()