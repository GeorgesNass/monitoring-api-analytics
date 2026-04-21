'''
__author__ = "Georges Nassopoulos"
__copyright__ = None
__version__ = "1.0.0"
__email__ = "georges.nassopoulos@gmail.com"
__status__ = "Dev"
__desc__ = "Unified configuration loader for monitoring-api-analytics: dotenv, env parsing, GCP, BigQuery, SQLite, paths, profiles, secrets and runtime metadata."
'''

from __future__ import annotations

import json
import os
import platform
import uuid
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from functools import lru_cache
from pathlib import Path
from typing import Any, Optional, Tuple

from src.utils.logging_utils import get_logger

try:
    from src.core.errors import ConfigurationError
except Exception:
    class ConfigurationError(ValueError):
        """
            Fallback configuration error when the project error module is unavailable
        """

logger = get_logger(__name__)

## ============================================================
## PLACEHOLDER TOKENS
## ============================================================
PLACEHOLDER_PREFIXES: Tuple[str, ...] = ("<YOUR_", "YOUR_", "CHANGE_ME", "REPLACE_ME", "TODO")

## ============================================================
## OS / SYSTEM CONSTANTS
## ============================================================
SYSTEM_NAME = platform.system().lower()
IS_WINDOWS = SYSTEM_NAME == "windows"
IS_LINUX = SYSTEM_NAME == "linux"
IS_MACOS = SYSTEM_NAME == "darwin"
DEFAULT_ENCODING = "utf-8"
CSV_SEPARATOR = ";"

## ============================================================
## STABLE DOMAIN CONSTANTS
## ============================================================
DEFAULT_APP_NAME = "monitoring-api-analytics"
DEFAULT_APP_VERSION = "1.0.0"
DEFAULT_ENVIRONMENT = "dev"
DEFAULT_PROFILE = "local"

DEFAULT_DATA_DIR = "data"
DEFAULT_RAW_DIR = "data/raw"
DEFAULT_PROCESSED_DIR = "data/processed"
DEFAULT_LOGS_DIR = "logs"
DEFAULT_SECRETS_DIR = "secrets"
DEFAULT_ARTIFACTS_DIR = "artifacts"
DEFAULT_EXPORTS_DIR = "artifacts/exports"

DEFAULT_SQLITE_DB_PATH = "data/processed/monitoring.db"
DEFAULT_BQ_LOCATION = "EU"
DEFAULT_BATCH_SIZE = 1000
DEFAULT_MAX_WORKERS = 4
DEFAULT_REQUEST_TIMEOUT_SECONDS = 120

SUPPORTED_INPUT_EXTENSIONS = (".csv", ".json", ".jsonl", ".txt", ".parquet")
SUPPORTED_OUTPUT_EXTENSIONS = (".csv", ".json", ".jsonl", ".db")

## ============================================================
## GCP JSON LOADING
## ============================================================

def _read_json_secret(path: Path) -> dict[str, Any]:
    """
        Read a JSON secret file from disk

        Args:
            path: Path to JSON file

        Returns:
            Parsed JSON as dictionary

        Raises:
            FileNotFoundError: If file does not exist
            ValueError: If JSON is invalid
    """

    ## Ensure file exists
    if not path.exists():
        raise FileNotFoundError(f"Missing JSON secret file: {path}")

    ## Load JSON content
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception as exc:
        raise ValueError(f"Invalid JSON content in {path}: {exc}") from exc


## ============================================================
## GCP CONFIG RESOLUTION
## ============================================================
def _load_gcp_config(project_root: Path) -> GcpConfig:
    """
        Load GCP configuration from JSON secret file

        Args:
            project_root: Project root directory

        Returns:
            GcpConfig instance populated from JSON

        Raises:
            FileNotFoundError: If config file is missing
            ValueError: If JSON content is invalid
    """

    ## Resolve path from environment
    raw_path = _get_env("GCP_CONFIG_FILE", "")
    resolved_path = _resolve_path(raw_path, project_root=project_root)

    ## Load JSON config
    gcp_json = _read_json_secret(resolved_path) if resolved_path else {}

    ## Build config object
    return GcpConfig(
        project_id=gcp_json.get("project_id", ""),
        bigquery_dataset=gcp_json.get("bigquery_dataset", ""),
        bigquery_table=gcp_json.get("bigquery_table", ""),
    )
    
## ============================================================
## CONFIG MODELS
## ============================================================
@dataclass(frozen=True)
class ExecutionMetadata:
    """
        Execution metadata

        Args:
            run_id: Unique runtime identifier
            started_at_utc: UTC timestamp when config was built
            hostname: Current host name
            platform_name: Current operating system name
            profile: Active runtime profile
            environment: Active environment
    """

    run_id: str
    started_at_utc: str
    hostname: str
    platform_name: str
    profile: str
    environment: str

@dataclass(frozen=True)
class PathsConfig:
    """
        Filesystem paths configuration

        Args:
            project_root: Project root directory
            src_dir: Source directory
            data_dir: Data root directory
            raw_dir: Raw data directory
            processed_dir: Processed data directory
            artifacts_dir: Artifacts directory
            exports_dir: Exports directory
            logs_dir: Logs directory
            secrets_dir: Secrets directory
            local_sqlite_path: Local SQLite database path
            google_credentials_path: Optional service account JSON path
    """

    project_root: Path
    src_dir: Path
    data_dir: Path
    raw_dir: Path
    processed_dir: Path
    artifacts_dir: Path
    exports_dir: Path
    logs_dir: Path
    secrets_dir: Path
    local_sqlite_path: Path
    google_credentials_path: Optional[Path]

@dataclass(frozen=True)
class RuntimeConfig:
    """
        Runtime configuration

        Args:
            environment: Environment name
            profile: Active runtime profile
            debug: Whether debug mode is enabled
            log_level: Logging level
            use_bigquery: Whether BigQuery is enabled
            use_sqlite_fallback: Whether SQLite fallback is enabled
            batch_size: Batch size for data operations
            max_workers: Maximum worker count
            request_timeout_seconds: Request timeout
            batch_sleep_seconds: Sleep delay between batches
            allowed_origins: Allowed HTTP origins for future API usage
            anomaly_detection_enabled: Enable anomaly detection
            anomaly_method: Detection method (zscore / iqr)
            z_threshold: Z-score threshold
            iqr_multiplier: IQR multiplier
            anomaly_strict_mode: Strict validation mode
            monitoring_streaming_enabled: Enable streaming-oriented monitoring checks
            drift_detection_enabled: Enable API data drift detection
            drift_p_value_threshold: Statistical p-value threshold for drift detection
            drift_medium_threshold: Threshold for medium drift severity
            drift_high_threshold: Threshold for high drift severity
            drift_strict_mode: Fail pipeline if drift is too high            
    """

    environment: str
    profile: str
    debug: bool
    log_level: str
    use_bigquery: bool
    use_sqlite_fallback: bool
    batch_size: int
    max_workers: int
    request_timeout_seconds: int
    batch_sleep_seconds: float
    allowed_origins: list[str]
    anomaly_detection_enabled: bool
    anomaly_method: str
    z_threshold: float
    iqr_multiplier: float
    anomaly_strict_mode: bool
    monitoring_streaming_enabled: bool
    drift_detection_enabled: bool
    drift_p_value_threshold: float
    drift_medium_threshold: float
    drift_high_threshold: float
    drift_strict_mode: bool    

@dataclass(frozen=True)
class GcpConfig:
    """
        GCP and BigQuery configuration

        Args:
            project_id: GCP project ID
            bigquery_dataset: BigQuery dataset name
            bigquery_table: BigQuery table name
            bigquery_location: BigQuery location
    """

    project_id: str
    bigquery_dataset: str
    bigquery_table: str
    bigquery_location: str

@dataclass(frozen=True)
class SecretsConfig:
    """
        Secret values resolved from env or files

        Args:
            api_key: Optional application API key
            google_credentials_json: Optional service account JSON content
    """

    api_key: str
    google_credentials_json: str

@dataclass(frozen=True)
class DataConsistencyConfig:
    """
        Data consistency configuration

        Args:
            enabled: Enable consistency checks
            strict_mode: Raise errors if inconsistencies
            max_error_ratio: Max allowed error ratio
            allow_warnings: Allow warnings without failing
    """

    enabled: bool
    strict_mode: bool
    max_error_ratio: float
    allow_warnings: bool


@dataclass(frozen=True)
class AppConfig:
    """
        Unified application configuration

        Args:
            app_name: Application name
            app_version: Application version
            execution: Execution metadata
            paths: Filesystem paths configuration
            runtime: Runtime configuration
            gcp: GCP and BigQuery configuration
            secrets: Secret values
    """

    app_name: str
    app_version: str
    execution: ExecutionMetadata
    paths: PathsConfig
    runtime: RuntimeConfig
    gcp: GcpConfig
    secrets: SecretsConfig
    data_consistency: DataConsistencyConfig
        
## ============================================================
## DOTENV / ENV HELPERS
## ============================================================
def _resolve_project_root() -> Path:
    """
        Resolve the project root path

        Returns:
            Absolute project root path
    """

    ## Prefer explicit project root override when available
    project_root_raw = os.getenv("PROJECT_ROOT", "").strip()
    return Path(project_root_raw).expanduser().resolve() if project_root_raw else Path(__file__).resolve().parents[2]

def _load_dotenv_if_present() -> None:
    """
        Load a local .env file if available

        Returns:
            None
    """

    ## Import dotenv lazily to avoid hard dependency issues
    try:
        from dotenv import load_dotenv
    except ImportError:
        return

    ## Load only when a project-level .env file exists
    env_path = _resolve_project_root() / ".env"
    if env_path.exists():
        load_dotenv(dotenv_path=env_path, override=False)

def _is_placeholder(value: str) -> bool:
    """
        Detect placeholder-like values

        Args:
            value: Raw environment value

        Returns:
            True if the value looks like a placeholder
    """

    ## Normalize before inspection
    normalized = value.strip().upper()
    return any(token in normalized for token in PLACEHOLDER_PREFIXES) or ("<" in normalized and ">" in normalized)

def _sanitize_placeholder(value: Optional[str], env_key: str) -> Optional[str]:
    """
        Replace placeholder env values with None and optionally remove env var

        Args:
            value: Raw environment value
            env_key: Environment variable key

        Returns:
            Cleaned value or None
    """

    ## Return None when missing
    if value is None:
        return None

    ## Normalize whitespace
    cleaned = value.strip()
    if not cleaned:
        return None

    ## Remove fake placeholders from runtime env
    if _is_placeholder(cleaned):
        if env_key in os.environ:
            os.environ.pop(env_key, None)
        return None

    return cleaned

def _get_env(name: str, default: str = "") -> str:
    """
        Read an environment variable safely

        Args:
            name: Environment variable name
            default: Default fallback value

        Returns:
            Normalized string value
    """

    ## Read raw value and normalize
    value = os.getenv(name, default)
    return (value if value is not None else default).strip()

def _get_env_bool(name: str, default: bool) -> bool:
    """
        Parse a boolean environment variable

        Args:
            name: Environment variable name
            default: Default fallback value

        Returns:
            Parsed boolean value

        Raises:
            ConfigurationError: If the value is invalid
    """

    ## Read and normalize raw value
    raw = _get_env(name, str(default)).lower()
    if raw in {"true", "1", "yes", "y", "on"}:
        return True
    if raw in {"false", "0", "no", "n", "off"}:
        return False
    raise ConfigurationError(f"Invalid boolean value for {name}: {raw}")

def _get_env_int(name: str, default: int) -> int:
    """
        Parse an integer environment variable

        Args:
            name: Environment variable name
            default: Default fallback value

        Returns:
            Parsed integer value

        Raises:
            ConfigurationError: If the value is invalid
    """

    ## Parse integer strictly
    try:
        return int(_get_env(name, str(default)))
    except (TypeError, ValueError) as exc:
        raise ConfigurationError(f"{name} must be an integer") from exc

def _get_env_float(name: str, default: float) -> float:
    """
        Parse a float environment variable

        Args:
            name: Environment variable name
            default: Default fallback value

        Returns:
            Parsed float value
    """

    raw = None

    try:
        ## read raw value
        raw = _get_env(name, str(default))

        ## convert to float
        value = float(raw)

        return value

    except (TypeError, ValueError) as exc:
        logger.error(f"Invalid float value for {name}: {raw}")
        raise ConfigurationError(f"{name} must be a float") from exc
        
def _get_env_list(name: str, default: Optional[list[str]] = None, *, separator: str = ",") -> list[str]:
    """
        Parse a list-like environment variable

        Args:
            name: Environment variable name
            default: Default fallback list
            separator: Separator used in the raw value

        Returns:
            Parsed list of strings
    """

    ## Read raw list value
    raw = _get_env(name, "")
    if not raw:
        return list(default or [])
    return [item.strip() for item in raw.split(separator) if item.strip()]

def _expand_env_vars(value: str) -> str:
    """
        Expand shell variables and user home in a string

        Args:
            value: Raw string value

        Returns:
            Expanded string
    """

    ## Expand shell variables
    return os.path.expandvars(value)

def _resolve_path(path_value: str, project_root: Path) -> Path:
    """
        Resolve a path against the project root

        Args:
            path_value: Raw path value
            project_root: Project root directory

        Returns:
            Resolved path
    """

    ## Expand shell variables and user home
    path_obj = Path(_expand_env_vars(path_value)).expanduser()
    return path_obj.resolve() if path_obj.is_absolute() else (project_root / path_obj).resolve()

def _get_env_path(name: str, default: str, project_root: Path) -> Path:
    """
        Read and resolve a path environment variable

        Args:
            name: Environment variable name
            default: Default path value
            project_root: Project root directory

        Returns:
            Resolved path
    """

    ## Resolve env override or default path
    return _resolve_path(_get_env(name, default), project_root)

def _read_secret_value(direct_key: str, file_key: str, *, project_root: Path, default: str = "") -> str:
    """
        Read a secret from env directly or from a file path

        Args:
            direct_key: Environment variable containing the secret
            file_key: Environment variable containing the secret file path
            project_root: Project root directory
            default: Default fallback value

        Returns:
            Secret value or default
    """

    ## Prefer direct env secret value first
    direct_value = _sanitize_placeholder(_get_env(direct_key, default), direct_key)
    if direct_value:
        return direct_value

    ## Fallback to file-based secret
    secret_file_raw = _sanitize_placeholder(_get_env(file_key, ""), file_key)
    if not secret_file_raw:
        return default

    ## Resolve and read secret file when available
    secret_file = _resolve_path(secret_file_raw, project_root)
    if secret_file.exists() and secret_file.is_file():
        return secret_file.read_text(encoding=DEFAULT_ENCODING).strip()
    return default

## ============================================================
## PROFILE HELPERS
## ============================================================
def _get_profiled_env(name: str, default: str, profile: str) -> str:
    """
        Read an env value with optional profile override

        Args:
            name: Base environment variable name
            default: Default fallback value
            profile: Active runtime profile

        Returns:
            Resolved string value
    """

    ## Prefer profile-specific override when present
    override_key = f"{profile.upper()}_{name}"
    return _get_env(override_key, default) if os.getenv(override_key) is not None else _get_env(name, default)

def _get_profiled_env_bool(name: str, default: bool, profile: str) -> bool:
    """
        Read a boolean env value with optional profile override

        Args:
            name: Base environment variable name
            default: Default fallback value
            profile: Active runtime profile

        Returns:
            Parsed boolean value
    """

    ## Prefer profile-specific override when present
    override_key = f"{profile.upper()}_{name}"
    return _get_env_bool(override_key, default) if os.getenv(override_key) is not None else _get_env_bool(name, default)

def _get_profiled_env_int(name: str, default: int, profile: str) -> int:
    """
        Read an integer env value with optional profile override

        Args:
            name: Base environment variable name
            default: Default fallback value
            profile: Active runtime profile

        Returns:
            Parsed integer value
    """

    ## Prefer profile-specific override when present
    override_key = f"{profile.upper()}_{name}"
    return _get_env_int(override_key, default) if os.getenv(override_key) is not None else _get_env_int(name, default)

def _get_profiled_env_float(name: str, default: float, profile: str) -> float:
    """
        Read a float env value with optional profile override

        Args:
            name: Base environment variable name
            default: Default fallback value
            profile: Active runtime profile

        Returns:
            Parsed float value
    """

    ## Prefer profile-specific override when present
    override_key = f"{profile.upper()}_{name}"
    return _get_env_float(override_key, default) if os.getenv(override_key) is not None else _get_env_float(name, default)

## ============================================================
## VALIDATION / BUILD HELPERS
## ============================================================
def _validate_required_placeholders(keys: list[str]) -> None:
    """
        Validate that required values are not unresolved placeholders

        Args:
            keys: Environment keys to inspect

        Returns:
            None

        Raises:
            ConfigurationError: If placeholders are detected
    """

    ## Collect keys still using placeholder values
    invalid_keys = [key for key in keys if (value := _get_env(key, "")) and _is_placeholder(value)]
    if invalid_keys:
        raise ConfigurationError("Placeholder values detected for: " + ", ".join(invalid_keys))

def _validate_positive_int(value: int, field_name: str) -> None:
    """
        Validate that an integer is strictly positive

        Args:
            value: Value to validate
            field_name: Human-readable field name

        Returns:
            None

        Raises:
            ConfigurationError: If the value is invalid
    """

    ## Reject non-positive values
    if value <= 0:
        raise ConfigurationError(f"{field_name} must be > 0. Got: {value}")

def _validate_non_negative_float(value: float, field_name: str) -> None:
    """
        Validate that a float is non-negative

        Args:
            value: Value to validate
            field_name: Human-readable field name

        Returns:
            None

        Raises:
            ConfigurationError: If the value is invalid
    """

    ## Reject negative values
    if value < 0.0:
        raise ConfigurationError(f"{field_name} must be >= 0. Got: {value}")

def _validate_environment(config: AppConfig) -> None:
    """
        Validate production constraints

        Args:
            config: Structured configuration

        Returns:
            None

        Raises:
            ConfigurationError: If production constraints are violated
    """

    ## Enforce production requirements
    if config.runtime.environment == "prod":
        required_fields = [
            config.gcp.project_id,
            config.gcp.bigquery_dataset,
            config.gcp.bigquery_table,
            str(config.paths.google_credentials_path) if config.paths.google_credentials_path else "",
        ]
        if not all(required_fields):
            raise ConfigurationError("Missing required environment variables for production")

def _ensure_directories_exist(paths: list[Path]) -> None:
    """
        Ensure runtime directories exist

        Args:
            paths: Directories to create if missing

        Returns:
            None
    """

    ## Create all runtime directories safely
    for directory in paths:
        directory.mkdir(parents=True, exist_ok=True)

def _validate_config(config: AppConfig) -> None:
    """
        Validate the final structured configuration

        Args:
            config: Structured application configuration

        Returns:
            None
        """

    ## Validate runtime numeric parameters
    _validate_positive_int(config.runtime.batch_size, "BATCH_SIZE")
    _validate_positive_int(config.runtime.max_workers, "MAX_WORKERS")
    _validate_positive_int(config.runtime.request_timeout_seconds, "REQUEST_TIMEOUT_SECONDS")
    _validate_non_negative_float(config.runtime.batch_sleep_seconds, "BATCH_SLEEP_SECONDS")

    ## Validate path suffixes
    if config.paths.local_sqlite_path.suffix.lower() not in {".db", ".sqlite", ".sqlite3"}:
        raise ConfigurationError("SQLITE_DB_PATH must point to a SQLite database file")

    ## Validate data consistency config
    if not (0.0 <= config.data_consistency.max_error_ratio <= 1.0):
        raise ConfigurationError("DATA_CONSISTENCY_MAX_ERROR_RATIO must be between 0 and 1")
  
    ## Validate strict_mode logic
    if config.data_consistency.strict_mode and not config.data_consistency.enabled:
        raise ConfigurationError("DATA_CONSISTENCY_STRICT requires DATA_CONSISTENCY_ENABLED=True")
        
    ## Validate production constraints
    _validate_environment(config)

    ## Validate anomaly config
    if config.runtime.z_threshold <= 0:
        raise ConfigurationError("Z_THRESHOLD must be > 0")

    if config.runtime.iqr_multiplier <= 0:
        raise ConfigurationError("IQR_MULTIPLIER must be > 0")

    if config.runtime.anomaly_method not in {"zscore", "iqr"}:
        raise ConfigurationError("ANOMALY_METHOD must be 'zscore' or 'iqr'")

    ## Validate drift parameters
    if not (0 < config.runtime.drift_p_value_threshold < 1):
        raise ConfigurationError("DRIFT_P_VALUE_THRESHOLD must be between 0 and 1")

    if config.runtime.drift_medium_threshold < 0:
        raise ConfigurationError("DRIFT_MEDIUM_THRESHOLD must be >= 0")

    if config.runtime.drift_high_threshold < config.runtime.drift_medium_threshold:
        raise ConfigurationError("DRIFT_HIGH_THRESHOLD must be >= DRIFT_MEDIUM_THRESHOLD")
        
## ============================================================
## EXPORT HELPERS
## ============================================================
def config_to_dict(config: AppConfig) -> dict[str, Any]:
    """
        Convert AppConfig into a serializable dictionary

        Args:
            config: Structured configuration object

        Returns:
            Serializable dictionary
    """

    ## Convert dataclass tree into a plain dictionary
    payload = asdict(config)

    ## Normalize Path objects recursively
    def _normalize(value: Any) -> Any:
        if isinstance(value, Path):
            return str(value)
        if isinstance(value, dict):
            return {key: _normalize(val) for key, val in value.items()}
        if isinstance(value, list):
            return [_normalize(item) for item in value]
        return value

    return _normalize(payload)

def config_to_json(config: AppConfig) -> str:
    """
        Convert AppConfig into a JSON string

        Args:
            config: Structured configuration object

        Returns:
            JSON string
    """

    ## Serialize normalized configuration to JSON
    return json.dumps(config_to_dict(config), indent=2, ensure_ascii=False)

## ============================================================
## CONFIG FACTORY
## ============================================================
@lru_cache(maxsize=1)
def get_config() -> AppConfig:
    """
        Build full application configuration from environment variables

        High-level workflow:
            1) Load optional project-level .env
            2) Resolve project root and active profile
            3) Sanitize placeholders for GCP values
            4) Build execution, paths, runtime, GCP and secrets sections
            5) Validate and cache the final AppConfig

        Returns:
            AppConfig instance
    """

    ## Load optional local .env file first
    _load_dotenv_if_present()

    ## Resolve project root and active runtime profile
    project_root = _resolve_project_root()
    environment = _get_env("APP_ENV", DEFAULT_ENVIRONMENT).lower()
    profile = _get_env("PROFILE", DEFAULT_PROFILE).lower()

    ## Sanitize raw GCP-related values
    gcp = _load_gcp_config(project_root=project_root)
    google_credentials_raw = _sanitize_placeholder(_get_env("GOOGLE_APPLICATION_CREDENTIALS", ""), "GOOGLE_APPLICATION_CREDENTIALS")

    ## Validate placeholder values for non-sanitized secrets
    _validate_required_placeholders(["APP_ENV", "PROFILE", "API_KEY"])

    ## Build execution metadata
    execution = ExecutionMetadata(
        run_id=_get_env("RUN_ID", str(uuid.uuid4())),
        started_at_utc=datetime.now(timezone.utc).isoformat(),
        hostname=platform.node(),
        platform_name=SYSTEM_NAME,
        profile=profile,
        environment=environment,
    )

    ## Resolve filesystem paths
    google_credentials_path = _resolve_path(google_credentials_raw, project_root) if google_credentials_raw else None
    paths = PathsConfig(
        project_root=project_root,
        src_dir=(project_root / "src").resolve(),
        data_dir=_get_env_path("DATA_DIR", DEFAULT_DATA_DIR, project_root),
        raw_dir=_get_env_path("RAW_DIR", DEFAULT_RAW_DIR, project_root),
        processed_dir=_get_env_path("PROCESSED_DIR", DEFAULT_PROCESSED_DIR, project_root),
        artifacts_dir=_get_env_path("ARTIFACTS_DIR", DEFAULT_ARTIFACTS_DIR, project_root),
        exports_dir=_get_env_path("EXPORTS_DIR", DEFAULT_EXPORTS_DIR, project_root),
        logs_dir=_get_env_path("LOGS_DIR", DEFAULT_LOGS_DIR, project_root),
        secrets_dir=_get_env_path("SECRETS_DIR", DEFAULT_SECRETS_DIR, project_root),
        local_sqlite_path=_get_env_path("SQLITE_DB_PATH", DEFAULT_SQLITE_DB_PATH, project_root),
        google_credentials_path=google_credentials_path,
    )

    ## Ensure runtime directories exist
    _ensure_directories_exist([
        paths.data_dir, paths.raw_dir, paths.processed_dir, paths.artifacts_dir,
        paths.exports_dir, paths.logs_dir, paths.secrets_dir, paths.local_sqlite_path.parent,
    ])

    ## Build runtime section
    runtime = RuntimeConfig(
        environment=environment,
        profile=profile,
        debug=_get_profiled_env_bool("DEBUG", environment == "dev", profile),
        log_level=_get_profiled_env("LOG_LEVEL", "INFO", profile),
        use_bigquery=_get_profiled_env_bool("USE_BIGQUERY", True, profile),
        use_sqlite_fallback=_get_profiled_env_bool("USE_SQLITE_FALLBACK", True, profile),
        batch_size=_get_profiled_env_int("BATCH_SIZE", DEFAULT_BATCH_SIZE, profile),
        max_workers=_get_profiled_env_int("MAX_WORKERS", DEFAULT_MAX_WORKERS, profile),
        request_timeout_seconds=_get_profiled_env_int("REQUEST_TIMEOUT_SECONDS", DEFAULT_REQUEST_TIMEOUT_SECONDS, profile),
        batch_sleep_seconds=_get_profiled_env_float("BATCH_SLEEP_SECONDS", 0.0, profile),
        allowed_origins=_get_env_list("ALLOWED_ORIGINS", ["*"]),
        anomaly_detection_enabled=_get_env_bool("ANOMALY_DETECTION_ENABLED", True),
        anomaly_method=_get_env("ANOMALY_METHOD", "zscore"),
        z_threshold=_get_env_float("Z_THRESHOLD", 3.0),
        iqr_multiplier=_get_env_float("IQR_MULTIPLIER", 1.5),
        anomaly_strict_mode=_get_env_bool("ANOMALY_STRICT_MODE", False),
        monitoring_streaming_enabled=_get_env_bool("MONITORING_STREAMING_ENABLED", True),
        drift_detection_enabled=_get_env_bool("DRIFT_DETECTION_ENABLED", True),
        drift_p_value_threshold=_get_env_float("DRIFT_P_VALUE_THRESHOLD", 0.05),
        drift_medium_threshold=_get_env_float("DRIFT_MEDIUM_THRESHOLD", 0.2),
        drift_high_threshold=_get_env_float("DRIFT_HIGH_THRESHOLD", 0.5),
        drift_strict_mode=_get_env_bool("DRIFT_STRICT_MODE", False),        
    )

    ## Build data consistency config
    data_consistency = DataConsistencyConfig(
        enabled=_get_profiled_env_bool("DATA_CONSISTENCY_ENABLED", True, profile),
        strict_mode=_get_profiled_env_bool("DATA_CONSISTENCY_STRICT", False, profile),
        max_error_ratio=_get_profiled_env_float("DATA_CONSISTENCY_MAX_ERROR_RATIO", 0.1, profile),
        allow_warnings=_get_profiled_env_bool("DATA_CONSISTENCY_ALLOW_WARNINGS", True, profile),
    )
    
    ## Build GCP / BigQuery section
    gcp = GcpConfig(
        project_id=gcp.project_id,
        bigquery_dataset=gcp.bigquery_dataset,
        bigquery_table=gcp.bigquery_table,
        bigquery_location=_get_profiled_env("BQ_LOCATION", DEFAULT_BQ_LOCATION, profile),
    )
    
    ## Resolve optional secrets
    secrets = SecretsConfig(
        api_key=_read_secret_value("API_KEY", "API_KEY_FILE", project_root=project_root),
        google_credentials_json=_read_secret_value("GOOGLE_CREDENTIALS_JSON", "GOOGLE_CREDENTIALS_JSON_FILE", project_root=project_root),
    )

    ## Build final structured config
    config = AppConfig(
        app_name=_get_env("APP_NAME", DEFAULT_APP_NAME),
        app_version=_get_env("APP_VERSION", DEFAULT_APP_VERSION),
        execution=execution,
        paths=paths,
        runtime=runtime,
        gcp=gcp,
        secrets=secrets,
        data_consistency=data_consistency,
    )

    ## Validate final configuration
    _validate_config(config)

    ## Log concise configuration summary
    logger.info(
        "Configuration loaded | app=%s | env=%s | profile=%s | bigquery=%s | sqlite=%s | dataset=%s | run_id=%s",
        config.app_name,
        config.runtime.environment,
        config.runtime.profile,
        config.runtime.use_bigquery,
        config.runtime.use_sqlite_fallback,
        config.gcp.bigquery_dataset or "none",
        config.execution.run_id,
    )
    return config

def load_config() -> AppConfig:
    """
        Backward-compatible alias for configuration loading

        Returns:
            AppConfig instance
    """

    ## Keep compatibility with existing imports
    return get_config()

def build_config() -> AppConfig:
    """
        Backward-compatible config builder

        Returns:
            AppConfig instance
    """

    ## Preserve an additional public entrypoint
    return get_config()

## ============================================================
## BACKWARD-COMPATIBLE SETTINGS WRAPPER
## ============================================================
class Settings:
    """
        Backward-compatible settings wrapper

        High-level workflow:
            1) Build the unified AppConfig
            2) Expose legacy attributes used by the existing code

        Design choice:
            - Preserve the old Settings API
            - Avoid breaking imports such as settings.project_id
    """

    def __init__(self) -> None:
        ## Build the unified configuration once
        config = get_config()

        ## Map legacy public attributes
        self.project_id: Optional[str] = config.gcp.project_id or None
        self.bigquery_dataset: Optional[str] = config.gcp.bigquery_dataset or None
        self.bigquery_table: Optional[str] = config.gcp.bigquery_table or None
        self.google_credentials_path: Optional[str] = (
            str(config.paths.google_credentials_path)
            if config.paths.google_credentials_path else None
        )
        self.local_sqlite_path: str = str(config.paths.local_sqlite_path)
        self.environment: str = config.runtime.environment
        self.log_level: str = config.runtime.log_level

## ============================================================
## PUBLIC SINGLETONS
## ============================================================
CONFIG: AppConfig = get_config()
config = CONFIG
settings = Settings()