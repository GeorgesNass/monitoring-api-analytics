'''
__author__ = "Georges Nassopoulos"
__copyright__ = None
__version__ = "1.0.0"
__email__ = "georges.nassopoulos@gmail.com"
__status__ = "Dev"
__desc__ = "Main CLI entry point for monitoring-api-analytics (ETL + metrics, and API service)."
'''

from __future__ import annotations

import argparse
import sys
import time
from pathlib import Path
from typing import Optional

import uvicorn

from src.core.config import settings
from src.core.errors import (
    ConfigurationError,
    ExtractionError,
    TransformationError,
    LoadError,
)
from src.etl.metrics import build_metrics
from src.pipeline import run_pipeline
from src.utils.logging_utils import get_logger

## ============================================================
## CONSTANTS
## ============================================================
APP_VERSION = "1.0.0"
EXIT_SUCCESS = 0
EXIT_FAILURE = 1
EXIT_PLATFORM_ERROR = 2

## ============================================================
## LOGGER
## ============================================================
logger = get_logger("main")

## ============================================================
## ARG PARSER
## ============================================================
def _build_parser() -> argparse.ArgumentParser:
    """
        Build CLI parser

        Returns:
            Configured ArgumentParser
    """

    parser = argparse.ArgumentParser(
        description="Monitoring API Analytics (extract, transform, load, metrics, API)",
        add_help=True,
    )

    parser.add_argument("--version", action="version", version=f"%(prog)s {APP_VERSION}")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--validate-config", action="store_true")

    ## Actions
    parser.add_argument("--extract-transform-load", action="store_true")
    parser.add_argument("--build-metrics-only", action="store_true")
    parser.add_argument("--run-api", action="store_true")

    ## Pipeline
    parser.add_argument("--source", type=str, default="cloud")
    parser.add_argument("--target", type=str, default="bigquery")
    parser.add_argument("--file-path", type=str, default="")
    parser.add_argument("--filter-query", type=str, default="")
    parser.add_argument("--limit", type=int, default=5000)

    ## API
    parser.add_argument("--host", type=str, default="0.0.0.0")
    parser.add_argument("--port", type=int, default=8000)
    parser.add_argument("--reload", action="store_true")

    return parser

## ============================================================
## HELPERS
## ============================================================
def _build_summary(
    action: str,
    success: bool,
    start: float,
    details: Optional[dict] = None,
) -> dict:
    """
        Build standardized execution summary

        Args:
            action: Executed action
            success: Execution status
            start: Start time
            details: Optional details

        Returns:
            Summary dictionary
    """

    return {
        "action": action,
        "success": success,
        "duration_seconds": round(time.monotonic() - start, 3),
        "details": details or {},
    }

## ============================================================
## MAIN
## ============================================================
def main() -> int:
    """
        Main CLI entry point

        Workflow:
            - extract-transform-load: full ETL + metrics
            - build-metrics-only: metrics only
            - run-api: start FastAPI service

        Returns:
            Exit code
    """

    start_time = time.monotonic()
    parser = _build_parser()
    args = parser.parse_args()

    try:
        if args.validate_config:
            logger.info("Config OK | env=%s", settings.environment)
            logger.info("Summary | %s", _build_summary("validate-config", True, start_time))
            return EXIT_SUCCESS

        if not any([args.extract_transform_load, args.build_metrics_only, args.run_api]):
            parser.print_help()
            logger.info("Summary | %s", _build_summary("help", True, start_time))
            return EXIT_SUCCESS

        if args.dry_run:
            logger.info(
                "Dry-run | etl=%s metrics=%s api=%s",
                bool(args.extract_transform_load),
                bool(args.build_metrics_only),
                bool(args.run_api),
            )
            logger.info("Summary | %s", _build_summary("dry-run", True, start_time))
            return EXIT_SUCCESS

        ## METRICS ONLY
        if args.build_metrics_only:
            logger.info("Building metrics | target=%s", args.target)
            build_metrics(target=args.target)
            logger.info("Metrics build completed")

        ## PIPELINE
        if args.extract_transform_load:
            file_path = (
                Path(args.file_path).expanduser().resolve()
                if args.file_path.strip()
                else None
            )

            filter_query = args.filter_query.strip() if args.filter_query.strip() else None

            logger.info(
                "Running pipeline | source=%s target=%s limit=%d",
                args.source,
                args.target,
                int(args.limit),
            )

            run_pipeline(
                source=args.source,
                target=args.target,
                file_path=file_path,
                filter_query=filter_query,
                limit=int(args.limit),
            )

            logger.info("Pipeline completed")

        ## API
        if args.run_api:
            reload_mode = bool(args.reload) or (settings.environment != "prod")

            logger.info(
                "Starting API | host=%s port=%d reload=%s",
                args.host,
                int(args.port),
                reload_mode,
            )

            uvicorn.run(
                "src.core.service:app",
                host=args.host,
                port=int(args.port),
                reload=reload_mode,
            )

        logger.info("Summary | %s", _build_summary("run", True, start_time))
        return EXIT_SUCCESS

    except KeyboardInterrupt:
        logger.warning("Interrupted")
        logger.warning("Summary | %s", _build_summary("interrupt", False, start_time))
        return EXIT_FAILURE

    except (ConfigurationError, ExtractionError, TransformationError, LoadError) as exc:
        logger.error("Known error: %s", str(exc))
        logger.error("Summary | %s", _build_summary("known-error", False, start_time))
        return EXIT_PLATFORM_ERROR

    except Exception as exc:
        logger.exception("Unhandled error: %s", exc)
        logger.error("Summary | %s", _build_summary("unhandled-error", False, start_time))
        return EXIT_FAILURE

## ============================================================
## ENTRYPOINT
## ============================================================
if __name__ == "__main__":
    sys.exit(main())