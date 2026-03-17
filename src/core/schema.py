'''
__author__ = "Georges Nassopoulos"
__copyright__ = None
__version__ = "1.0.0"
__email__ = "georges.nassopoulos@gmail.com"
__status__ = "Dev"
__desc__ = "Pydantic schemas for monitoring-api-analytics requests, responses, pipeline runs, metrics, and exports."
'''

from __future__ import annotations

import re
from dataclasses import asdict, dataclass
from datetime import datetime
from typing import Any, Literal

from pydantic import BaseModel, Field, field_validator, model_validator

try:
    from pydantic_settings import BaseSettings, SettingsConfigDict
except ImportError:  # pragma: no cover
    BaseSettings = BaseModel  # type: ignore[misc, assignment]
    SettingsConfigDict = dict  # type: ignore[misc, assignment]

## ============================================================
## COMMON TYPES AND PATTERNS
## ============================================================
SourceType = Literal["cloud", "json", "csv", "api", "parquet"]
TargetType = Literal["bigquery", "sqlite", "postgres", "duckdb", "csv"]
PipelineStep = Literal[
    "extract", "load", "transform", "validate", "aggregate", "dashboard", "report"
]
JobStatusName = Literal["pending", "running", "success", "failed", "cancelled"]
LogLevelName = Literal["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"]

SAFE_NAME_PATTERN = re.compile(r"^[a-zA-Z0-9._:/\-]+$")
SAFE_FILE_PATTERN = re.compile(r"^[a-zA-Z0-9._/\-]+$")
URL_PATTERN = re.compile(r"^https?://[^\s]+$")

## ============================================================
## BASE SCHEMAS
## ============================================================
class BaseSchema(BaseModel):
    """
        Base schema with shared validation and serialization helpers

        Returns:
            A reusable Pydantic base model
    """

    model_config = {
        "extra": "forbid",
        "populate_by_name": True,
        "str_strip_whitespace": True,
    }

    def to_dict(self) -> dict[str, Any]:
        """
            Convert the model to a Python dictionary

            Returns:
                Serialized model as dictionary
        """

        return self.model_dump()

    def to_json(self) -> str:
        """
            Convert the model to a JSON string

            Returns:
                Serialized model as JSON
        """

        return self.model_dump_json()

    def to_record(self) -> dict[str, Any]:
        """
            Convert the model to a row-oriented dictionary

            Returns:
                Flat dictionary representation
        """

        return self.model_dump(mode="json")

    def to_pandas(self) -> Any:
        """
            Convert the model to a one-row pandas DataFrame

            Returns:
                A pandas DataFrame with one row
        """

        ## Import pandas lazily to avoid a hard dependency at import time
        import pandas as pd

        return pd.DataFrame([self.to_record()])

class WarningMixin(BaseSchema):
    """
        Mixin exposing warnings in response payloads

        Args:
            warnings: Warning messages list
    """

    warnings: list[str] = Field(default_factory=list)

## ============================================================
## SETTINGS AND CONFIG SCHEMAS
## ============================================================
@dataclass(frozen=True)
class PipelineRuntimeConfig:
    """
        Typed runtime configuration for monitoring pipeline execution

        Args:
            source: Default input source type
            target: Default target type
            environment: Runtime environment
            request_timeout_seconds: Request timeout in seconds
            enable_dashboard_export: Whether dashboard export is enabled
    """

    source: str
    target: str
    environment: str
    request_timeout_seconds: int
    enable_dashboard_export: bool

    def to_dict(self) -> dict[str, Any]:
        """
            Convert the dataclass to a dictionary

            Returns:
                Serialized dataclass as dictionary
        """

        return asdict(self)

class AppSettings(BaseSettings):
    """
        Settings model for monitoring-api-analytics

        Args:
            app_name: Application name
            environment: Runtime environment
            default_source: Default source type
            default_target: Default target type
            request_timeout_seconds: Timeout in seconds
            enable_dashboard_export: Whether dashboard export is enabled
    """

    model_config = SettingsConfigDict(
        extra="ignore",
        env_prefix="MONITORING_API_",
        case_sensitive=False,
    )

    app_name: str = "monitoring-api-analytics"
    environment: str = "dev"
    default_source: SourceType = "cloud"
    default_target: TargetType = "bigquery"
    request_timeout_seconds: int = Field(default=60, ge=1, le=3600)
    enable_dashboard_export: bool = True

class PipelineConfig(BaseSchema):
    """
        Pipeline execution configuration schema

        Args:
            source: Input source type
            target: Output target type
            start_from: Optional pipeline step
            batch_size: Batch size
            retry_count: Retry count
    """

    source: SourceType = "cloud"
    target: TargetType = "bigquery"
    start_from: PipelineStep | None = None
    batch_size: int = Field(default=1000, ge=1, le=10_000_000)
    retry_count: int = Field(default=0, ge=0, le=20)

## ============================================================
## COMMON OPERATIONAL SCHEMAS
## ============================================================
class HealthResponse(BaseSchema):
    """
        Healthcheck response model

        High-level workflow:
            1) Provide service status
            2) Expose environment information
            3) Confirm response timestamp

        Args:
            status: Service status indicator
            environment: Deployment environment
            timestamp: Response generation timestamp
    """

    status: str = Field(..., examples=["ok"])
    environment: str = Field(..., examples=["dev"])
    timestamp: datetime = Field(default_factory=datetime.utcnow)

class ErrorResponse(BaseSchema):
    """
        Standard API error response

        Args:
            error: Normalized error code
            message: Human-readable message
            origin: Component where the error happened
            details: Diagnostic details
            request_id: Optional request correlation id
    """

    error: str = Field(..., min_length=1)
    message: str = Field(..., min_length=1)
    origin: str = Field(default="unknown", min_length=1)
    details: dict[str, Any] = Field(default_factory=dict)
    request_id: str = Field(default="n/a", min_length=1)

class GenericResponse(BaseSchema):
    """
        Generic API response model

        High-level workflow:
            1) Provide operation result
            2) Return descriptive message
            3) Include optional metadata

        Args:
            success: Boolean indicating operation result
            message: Human-readable message
            metadata: Optional metadata payload
    """

    success: bool
    message: str
    metadata: dict[str, Any] = Field(default_factory=dict)

class StatusResponse(BaseSchema):
    """
        Generic status response schema

        Args:
            status: Current status
            message: Optional message
            progress: Optional progress between 0 and 100
            metadata: Optional metadata
    """

    status: str = Field(..., min_length=1)
    message: str = Field(default="")
    progress: float | None = Field(default=None, ge=0.0, le=100.0)
    metadata: dict[str, Any] = Field(default_factory=dict)

class StructuredLogEvent(BaseSchema):
    """
        Structured log schema

        Args:
            level: Log level
            event: Event name
            message: Human-readable message
            logger_name: Logger name
            context: Additional context
            timestamp: Event timestamp
    """

    level: LogLevelName
    event: str = Field(..., min_length=1)
    message: str = Field(..., min_length=1)
    logger_name: str = Field(default="monitoring-api-analytics", min_length=1)
    context: dict[str, Any] = Field(default_factory=dict)
    timestamp: datetime = Field(default_factory=datetime.utcnow)

class QueueEvent(BaseSchema):
    """
        Message queue or event bus schema

        Args:
            event_id: Unique event identifier
            event_type: Event type
            source: Event source
            payload: Event payload
            timestamp: Event timestamp
    """

    event_id: str = Field(..., min_length=1)
    event_type: str = Field(..., min_length=1)
    source: str = Field(..., min_length=1)
    payload: dict[str, Any] = Field(default_factory=dict)
    timestamp: datetime = Field(default_factory=datetime.utcnow)

    @field_validator("event_id", "event_type", "source")
    @classmethod
    def validate_safe_names(cls, value: str) -> str:
        """
            Validate safe identifier-like strings

            Args:
                value: Candidate identifier string

            Returns:
                The validated identifier string

            Raises:
                ValueError: If the value contains unsupported characters
        """

        ## Ensure identifiers remain API and filesystem friendly
        if not SAFE_NAME_PATTERN.match(value):
            raise ValueError("value contains unsupported characters")
        return value

class MetricPoint(BaseSchema):
    """
        Monitoring metric point schema

        Args:
            name: Metric name
            value: Metric value
            unit: Optional metric unit
            tags: Optional metric tags
            timestamp: Metric timestamp
    """

    name: str = Field(..., min_length=1)
    value: float
    unit: str | None = None
    tags: dict[str, str] = Field(default_factory=dict)
    timestamp: datetime = Field(default_factory=datetime.utcnow)

class MonitoringResponse(WarningMixin):
    """
        Monitoring response schema

        Args:
            metrics: Metric points list
            summary: Aggregated summary
    """

    metrics: list[MetricPoint] = Field(default_factory=list)
    summary: dict[str, float] = Field(default_factory=dict)

## ============================================================
## PIPELINE REQUEST AND DATASET SCHEMAS
## ============================================================
class PipelineRequest(BaseSchema):
    """
        Pipeline execution request model

        High-level workflow:
            1) Define execution mode
            2) Specify source type and target storage
            3) Allow partial pipeline execution

        Args:
            source: Data source type
            target: Target storage type
            start_from: Optional step to start from
    """

    source: SourceType = Field(..., examples=["cloud"])
    target: TargetType = Field(..., examples=["bigquery"])
    start_from: PipelineStep | None = Field(default=None, examples=["transform"])

class DatasetRecord(BaseSchema):
    """
        Generic analytics dataset record schema

        Args:
            record_id: Record identifier
            source: Source label or table name
            payload: Record payload
            event_timestamp: Optional event timestamp
    """

    record_id: str = Field(..., min_length=1)
    source: str = Field(..., min_length=1)
    payload: dict[str, Any] = Field(default_factory=dict)
    event_timestamp: datetime | None = None

    @field_validator("record_id", "source")
    @classmethod
    def validate_safe_values(cls, value: str) -> str:
        """
            Validate safe identifier-like values

            Args:
                value: Candidate identifier

            Returns:
                The validated identifier

            Raises:
                ValueError: If the identifier contains unsupported characters
        """

        ## Keep identifiers safe for logs and exports
        if not SAFE_NAME_PATTERN.match(value):
            raise ValueError("value contains unsupported characters")
        return value

class DatasetInput(BaseSchema):
    """
        Dataset input schema

        Args:
            name: Dataset name
            records: Dataset records
    """

    name: str = Field(..., min_length=1)
    records: list[DatasetRecord] = Field(default_factory=list)

    @field_validator("records")
    @classmethod
    def validate_non_empty_records(
        cls, value: list[DatasetRecord]
    ) -> list[DatasetRecord]:
        """
            Validate that the dataset contains at least one record

            Args:
                value: Dataset records

            Returns:
                The validated records list

            Raises:
                ValueError: If the records list is empty
        """

        ## Prevent empty dataset payloads
        if not value:
            raise ValueError("records must contain at least one item")
        return value

class DatasetOutput(BaseSchema):
    """
        Dataset output schema

        Args:
            name: Dataset name
            row_count: Number of rows
            artifacts: Generated artifacts list
    """

    name: str = Field(..., min_length=1)
    row_count: int = Field(..., ge=0)
    artifacts: list[str] = Field(default_factory=list)

    @field_validator("artifacts")
    @classmethod
    def validate_artifacts(cls, value: list[str]) -> list[str]:
        """
            Validate artifact path strings

            Args:
                value: Candidate artifact path list

            Returns:
                The validated artifact path list

            Raises:
                ValueError: If one artifact path contains unsupported characters
        """

        ## Ensure artifact paths remain safe to persist and log
        for artifact_path in value:
            if not SAFE_FILE_PATTERN.match(artifact_path):
                raise ValueError("artifacts contain unsupported path characters")
        return value

class PipelineTask(BaseSchema):
    """
        Pipeline task schema

        Args:
            task_id: Task identifier
            task_name: Task name
            status: Task status
            progress: Task progress percentage
            input_payload: Task input payload
            output_payload: Task output payload
    """

    task_id: str = Field(..., min_length=1)
    task_name: str = Field(..., min_length=1)
    status: JobStatusName = "pending"
    progress: float = Field(default=0.0, ge=0.0, le=100.0)
    input_payload: dict[str, Any] = Field(default_factory=dict)
    output_payload: dict[str, Any] = Field(default_factory=dict)

class PipelineJob(BaseSchema):
    """
        Pipeline job schema

        Args:
            job_id: Job identifier
            status: Job status
            tasks: Job tasks
            progress: Job progress percentage
            metadata: Job metadata
    """

    job_id: str = Field(..., min_length=1)
    status: JobStatusName = "pending"
    tasks: list[PipelineTask] = Field(default_factory=list)
    progress: float = Field(default=0.0, ge=0.0, le=100.0)
    metadata: dict[str, Any] = Field(default_factory=dict)

    @model_validator(mode="after")
    def validate_job_progress(self) -> "PipelineJob":
        """
            Validate progress consistency between the job and its tasks

            Returns:
                The validated pipeline job

            Raises:
                ValueError: If job progress is below the minimum task progress
        """

        ## Keep parent progress coherent with child task progress
        if self.tasks and self.progress < min(task.progress for task in self.tasks):
            raise ValueError("job progress cannot be below the minimum task progress")
        return self

## ============================================================
## ANALYTICS AND EXPORT SCHEMAS
## ============================================================
class AggregationRequest(BaseSchema):
    """
        Aggregation request schema

        Args:
            metric_names: Metrics to aggregate
            group_by: Group-by dimensions
            start_date: Optional inclusive start timestamp
            end_date: Optional inclusive end timestamp
    """

    metric_names: list[str] = Field(default_factory=list)
    group_by: list[str] = Field(default_factory=list)
    start_date: datetime | None = None
    end_date: datetime | None = None

    @field_validator("metric_names", "group_by")
    @classmethod
    def validate_name_lists(cls, value: list[str]) -> list[str]:
        """
            Validate metric and grouping name lists

            Args:
                value: Candidate names list

            Returns:
                A cleaned deduplicated names list

            Raises:
                ValueError: If one value is empty or invalid
        """

        ## Strip names, reject empties, and preserve insertion order
        cleaned_values: list[str] = []
        for item in value:
            cleaned_item = item.strip()
            if not cleaned_item:
                raise ValueError("list items must not be empty")
            if not SAFE_NAME_PATTERN.match(cleaned_item):
                raise ValueError("list item contains unsupported characters")
            if cleaned_item not in cleaned_values:
                cleaned_values.append(cleaned_item)
        return cleaned_values

    @model_validator(mode="after")
    def validate_date_range(self) -> "AggregationRequest":
        """
            Validate aggregation date range consistency

            Returns:
                The validated aggregation request

            Raises:
                ValueError: If end_date is before start_date
        """

        ## Ensure the requested time range is chronologically valid
        if self.start_date and self.end_date and self.end_date < self.start_date:
            raise ValueError("end_date must be greater than or equal to start_date")
        return self

class AggregationResponse(WarningMixin):
    """
        Aggregation response schema

        Args:
            rows: Aggregated rows
            row_count: Number of aggregated rows
    """

    rows: list[dict[str, Any]] = Field(default_factory=list)
    row_count: int = Field(default=0, ge=0)

    @model_validator(mode="after")
    def validate_row_count(self) -> "AggregationResponse":
        """
            Validate aggregated row count consistency

            Returns:
                The validated aggregation response

            Raises:
                ValueError: If row_count does not match rows length
        """

        ## Ensure exported row_count matches the real payload size
        if self.row_count != len(self.rows):
            raise ValueError("row_count must match len(rows)")
        return self

class DashboardLink(BaseSchema):
    """
        Dashboard link schema

        Args:
            name: Dashboard name
            url: Dashboard URL
    """

    name: str = Field(..., min_length=1)
    url: str = Field(..., min_length=1)

    @field_validator("url")
    @classmethod
    def validate_url(cls, value: str) -> str:
        """
            Validate dashboard URL

            Args:
                value: Candidate URL

            Returns:
                The validated URL

            Raises:
                ValueError: If the URL format is invalid
        """

        ## Restrict links to HTTP(S) URLs
        if not URL_PATTERN.match(value):
            raise ValueError("url must start with http:// or https://")
        return value

class ExportRequest(BaseSchema):
    """
        Export request schema

        Args:
            job_id: Pipeline job identifier
            export_format: Export format
            include_dashboard_links: Whether dashboard links must be included
    """

    job_id: str = Field(..., min_length=1)
    export_format: Literal["json", "csv", "parquet", "html"] = "json"
    include_dashboard_links: bool = True

    @field_validator("job_id")
    @classmethod
    def validate_job_id(cls, value: str) -> str:
        """
            Validate pipeline job identifier

            Args:
                value: Candidate job identifier

            Returns:
                The validated job identifier

            Raises:
                ValueError: If the identifier contains unsupported characters
        """

        ## Keep exported job identifiers safe for storage and logging
        if not SAFE_NAME_PATTERN.match(value):
            raise ValueError("job_id contains unsupported characters")
        return value

class ExportResponse(WarningMixin):
    """
        Export response schema

        Args:
            exports: Exported artifact paths
            dashboards: Optional dashboard links
            message: Human-readable summary
    """

    exports: list[str] = Field(default_factory=list)
    dashboards: list[DashboardLink] = Field(default_factory=list)
    message: str = Field(default="")

    @field_validator("exports")
    @classmethod
    def validate_export_paths(cls, value: list[str]) -> list[str]:
        """
            Validate export path strings

            Args:
                value: Candidate export path list

            Returns:
                The validated export path list

            Raises:
                ValueError: If one export path contains unsupported characters
        """

        ## Ensure export paths are safe to persist and log
        for export_path in value:
            if not SAFE_FILE_PATTERN.match(export_path):
                raise ValueError("exports contain unsupported path characters")
        return value