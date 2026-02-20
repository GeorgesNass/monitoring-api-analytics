'''
__author__ = "Georges Nassopoulos"
__copyright__ = None
__version__ = "1.0.0"
__email__ = "georges.nassopoulos@gmail.com"
__status__ = "Dev"
__desc__ = "Pydantic schemas for API requests and responses."
'''

from datetime import datetime
from typing import Optional

from pydantic import BaseModel, Field

## ============================================================
## HEALTHCHECK SCHEMA
## ============================================================
class HealthResponse(BaseModel):
    """
        Healthcheck response model

        High-level workflow:
            1) Provide service status
            2) Expose environment information
            3) Confirm application uptime

        Attributes:
            status: Service status indicator
            environment: Deployment environment
            timestamp: Response generation timestamp
    """

    status: str = Field(..., example="ok")
    environment: str = Field(..., example="dev")
    timestamp: datetime

## ============================================================
## PIPELINE REQUEST SCHEMA
## ============================================================
class PipelineRequest(BaseModel):
    """
        Pipeline execution request model

        High-level workflow:
            1) Define execution mode
            2) Specify source type
            3) Allow partial pipeline execution

        Attributes:
            source: Data source type (cloud, json, csv)
            target: Target storage (bigquery, sqlite)
            start_from: Optional step to start from
    """

    source: str = Field(..., example="cloud")
    target: str = Field(..., example="bigquery")
    start_from: Optional[str] = Field(
        None,
        example="transform",
    )

## ============================================================
## GENERIC RESPONSE SCHEMA
## ============================================================
class GenericResponse(BaseModel):
    """
        Generic API response model

        High-level workflow:
            1) Provide operation result
            2) Return descriptive message
            3) Include optional metadata

        Attributes:
            success: Boolean indicating operation result
            message: Human-readable message
    """

    success: bool
    message: str