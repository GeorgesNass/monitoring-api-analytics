'''
__author__ = "Georges Nassopoulos"
__copyright__ = None
__version__ = "1.0.0"
__email__ = "georges.nassopoulos@gmail.com"
__status__ = "Dev"
__desc__ = "FastAPI service exposing healthcheck and pipeline execution endpoints."
'''

from datetime import datetime

from fastapi import FastAPI, HTTPException

from src.core.config import settings
from src.core.schema import (
    HealthResponse,
    PipelineRequest,
    GenericResponse,
)
from src.core.errors import MonitoringBaseError
from src.pipeline import run_pipeline
from src.utils.logging_utils import get_logger

logger = get_logger(__name__)

## ============================================================
## APP FACTORY
## ============================================================
def create_app() -> FastAPI:
    """
        Create FastAPI application instance

        High-level workflow:
            1) Initialize FastAPI app
            2) Register routes
            3) Attach global exception handling

        Returns:
            Configured FastAPI instance
    """
    app = FastAPI(
        title="Monitoring API Analytics",
        version="1.0.0",
    )

    register_routes(app)
    register_exception_handlers(app)

    return app

## ============================================================
## ROUTES
## ============================================================
def register_routes(app: FastAPI) -> None:
    """
        Register API routes

        Args:
            app: FastAPI instance
    """

    @app.get(
        "/health",
        response_model=HealthResponse,
    )
    def healthcheck() -> HealthResponse:
        """
            Healthcheck endpoint

            Returns:
                HealthResponse object
        """
        
        return HealthResponse(
            status="ok",
            environment=settings.environment,
            timestamp=datetime.utcnow(),
        )

    @app.post(
        "/pipeline/run",
        response_model=GenericResponse,
    )
    
    def execute_pipeline(request: PipelineRequest) -> GenericResponse:
        """
            Execute monitoring pipeline

            High-level workflow:
                1) Receive execution parameters
                2) Trigger pipeline orchestration
                3) Return execution status

            Args:
                request: Pipeline execution request

            Returns:
                GenericResponse with execution result
        """
        
        try:
            run_pipeline(
                source=request.source,
                target=request.target,
                start_from=request.start_from,
            )

            return GenericResponse(
                success=True,
                message="Pipeline executed successfully",
            )

        except MonitoringBaseError as exc:
            logger.error("Pipeline error: %s", exc.message)
            raise HTTPException(
                status_code=400,
                detail=exc.message,
            )

        except Exception as exc:
            logger.exception("Unexpected error during pipeline execution")
            raise HTTPException(
                status_code=500,
                detail=str(exc),
            )

## ============================================================
## EXCEPTION HANDLERS
## ============================================================
def register_exception_handlers(app: FastAPI) -> None:
    """
        Register global exception handlers

        Args:
            app: FastAPI instance
    """

    @app.exception_handler(MonitoringBaseError)
    async def monitoring_exception_handler(_, exc: MonitoringBaseError):
        return HTTPException(
            status_code=400,
            detail=exc.message,
        )

## ============================================================
## APPLICATION INSTANCE
## ============================================================
app = create_app()