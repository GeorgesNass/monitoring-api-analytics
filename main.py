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
from pathlib import Path
import sys
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
## LOGGER
## ============================================================
logger = get_logger("main")

## ============================================================
## CLI ARGUMENTS
## ============================================================
def _build_parser() -> argparse.ArgumentParser:
    """
        Build argument parser for CLI usage

        Returns:
            Configured ArgumentParser
    """
    
    parser = argparse.ArgumentParser(
        description="Monitoring API Analytics (extract, normalize, load, metrics, and serve API).",
    )

    ## Main action flags
    parser.add_argument(
        "--extract-transform-load",
        action="store_true",
        help="Run full pipeline (extract -> transform -> load -> build-metrics).",
    )
    parser.add_argument(
        "--build-metrics-only",
        action="store_true",
        help="Deploy metrics assets only (BigQuery views/tables or SQLite dashboard tables).",
    )
    parser.add_argument(
        "--run-api",
        action="store_true",
        help="Run FastAPI service (uvicorn).",
    )

    ## Pipeline parameters
    parser.add_argument(
        "--source",
        type=str,
        default="cloud",
        help="Log source: cloud | json | jsonl | csv (default: cloud).",
    )
    parser.add_argument(
        "--target",
        type=str,
        default="bigquery",
        help="Target: bigquery | sqlite (default: bigquery).",
    )
    parser.add_argument(
        "--file-path",
        type=str,
        default="",
        help="Path to local file if source is json/jsonl/csv.",
    )
    parser.add_argument(
        "--filter-query",
        type=str,
        default="",
        help="Cloud Logging filter expression if source is cloud.",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=5000,
        help="Max number of cloud log entries (default: 5000).",
    )

    ## API options
    parser.add_argument(
        "--host",
        type=str,
        default="0.0.0.0",
        help="API host (default: 0.0.0.0).",
    )
    parser.add_argument(
        "--port",
        type=int,
        default=8000,
        help="API port (default: 8000).",
    )
    parser.add_argument(
        "--reload",
        action="store_true",
        help="Enable auto-reload (dev mode).",
    )

    return parser

## ============================================================
## MAIN EXECUTION
## ============================================================
def main() -> None:
    """
        Main CLI entry point

        Workflow notes:
            - extract-transform-load runs extract -> transform -> load -> build_metrics
            - build-metrics-only deploys SQL assets / materializes dashboard tables
            - run-api starts FastAPI server via uvicorn
            - source=cloud requires filter-query
            - source=json/jsonl/csv requires file-path
    """
    
    try:
        parser = _build_parser()
        args = parser.parse_args()

        if not any([args.extract_transform_load, args.build_metrics_only, args.run_api]):
            parser.print_help()
            return

        ## Build metrics only
        if args.build_metrics_only:
            logger.info("Building metrics only | target=%s", args.target)
            build_metrics(target=args.target)
            logger.info("Metrics build completed successfully")

        ## Run full pipeline (includes metrics)
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

            logger.info("Pipeline completed successfully")

        ## Run API server
        if args.run_api:
            reload_mode = bool(args.reload) or (settings.environment != "prod")

            logger.info(
                "Starting API server | host=%s port=%d reload=%s",
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

    except (ConfigurationError, ExtractionError, TransformationError, LoadError) as exc:
        print(f"\nERROR: {exc}\n")
        sys.exit(2)

if __name__ == "__main__":
    main()