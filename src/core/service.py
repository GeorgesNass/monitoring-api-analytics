'''
__author__ = "Georges Nassopoulos"
__copyright__ = None
__version__ = "1.0.0"
__email__ = "georges.nassopoulos@gmail.com"
__status__ = "Dev"
__desc__ = "FastAPI service exposing healthcheck and pipeline execution endpoints."
'''

from datetime import datetime

from fastapi import FastAPI, HTTPException, Depends
from fastapi.security import OAuth2PasswordBearer

## JWT / SECURITY IMPORTS
from core.auth import (
    get_current_active_user,
    login_user,
    logout_user,
    refresh_access_token,
)
from core.security import JWTMiddleware, require_roles

from src.core.config import settings
from src.core.schema import (
    HealthResponse,
    PipelineRequest,
    GenericResponse,
)
from src.core.data_consistency import run_data_consistency
from src.core.errors import MonitoringBaseError
from src.pipeline import run_pipeline
from src.utils.logging_utils import get_logger

logger = get_logger(__name__)

oauth2_scheme = OAuth2PasswordBearer(tokenUrl="login")

## Fake DB (TO REPLACE)
fake_db = {
    "admin": {
        "username": "admin",
        "hashed_password": "$2b$12$examplehash",
        "roles": ["admin"],
        "scopes": ["all"],
        "is_active": True,
    }
}

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

    ## Add JWT middleware
    app.add_middleware(JWTMiddleware)

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

    @app.post("/login")
    def login(data: dict):
        """
            Authenticate user and return JWT tokens

            Args:
                data: Raw credentials payload containing username and password

            Returns:
                Access token, refresh token and token type
        """

        return login_user(data["username"], data["password"], fake_db)

    @app.post("/refresh")
    def refresh(data: dict):
        """
            Refresh an access token using a valid refresh token

            Args:
                data: Raw payload containing refresh_token

            Returns:
                New access token pair
        """

        return refresh_access_token(data["refresh_token"])

    @app.post("/logout")
    def logout(token: str = Depends(oauth2_scheme)):
        """
            Logout the current user by revoking the provided token

            Args:
                token: Bearer token extracted from Authorization header

            Returns:
                Logout status payload
        """

        logout_user(token)

        return {"status": "logged_out"}

    @app.get(
        "/health",
        response_model=HealthResponse,
    )
    def healthcheck(
        user=Depends(get_current_active_user),
    ) -> HealthResponse:
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
    def execute_pipeline(
        request: PipelineRequest,
        user=Depends(require_roles(["admin", "service"])),
    ) -> GenericResponse:
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
            
            ## ============================================================
            ## DATA CONSISTENCY CHECK
            ## ============================================================

            ## Build monitoring payload
            data = {
                "endpoint": request.source,
                "target": request.target,
                "timestamp": datetime.utcnow().isoformat(),
            }

            ## Run consistency
            consistency_result = run_data_consistency(
                data=data,
                strict=settings.data_consistency.strict_mode,
            )

            ## Log result
            logger.info(f"Consistency OK: {consistency_result['is_consistent']}")

            ## Block if needed
            if not consistency_result["is_consistent"]:
                logger.warning(f"Issues detected: {consistency_result['issues']}")

                if settings.data_consistency.strict_mode:
                    raise HTTPException(
                        status_code=400,
                        detail="Data consistency check failed",
                    )

            if not consistency_result["is_consistent"] and not settings.data_consistency.allow_warnings:
                raise HTTPException(status_code=400, detail="Data consistency warnings not allowed")
    
            ## Run monitoring pipeline
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
        ## Return HTTP error payload
        raise HTTPException(
            status_code=400,
            detail=exc.message,
        )

## ============================================================
## APPLICATION INSTANCE
## ============================================================
app = create_app()