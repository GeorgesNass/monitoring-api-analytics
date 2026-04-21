#!/usr/bin/env bash

###############################################################################
# Monitoring API Analytics - Pipeline Menu
# Author: Georges Nassopoulos
# Version: 1.1.0
# Description:
#   CLI menu to run the monitoring-api-analytics workflows:
#   - run full pipeline (extract -> transform -> load -> metrics)
#   - build metrics only
#   - run FastAPI service
#   - run tests
#   - run data drift detection
###############################################################################

set -euo pipefail

PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PYTHON_BIN="${PYTHON_BIN:-python3}"

echo "=============================================="
echo " Monitoring API Analytics - Pipeline Menu"
echo "=============================================="
echo "Project root: ${PROJECT_ROOT}"
echo ""

## ---------------------------------------------------------------------------
## Helpers
## ---------------------------------------------------------------------------

pause() {
  read -rp "Press ENTER to continue..."
}

run_python() {
  echo ""
  echo ">>> $*"
  $PYTHON_BIN "$@"
}

## ---------------------------------------------------------------------------
## Menu
## ---------------------------------------------------------------------------

while true; do
  echo ""
  echo "Select an action:"
  echo " 1) Run full pipeline (extract -> transform -> load -> metrics) (with data consistency + data quality)"
  echo " 2) Build metrics only (with data consistency + data quality)"
  echo " 3) Run API (uvicorn)"
  echo " 4) Run tests (pytest)"
  echo " 5) Run data drift"
  echo " 6) Full pipeline + drift"
  echo " 0) Exit"
  echo ""

  read -rp "Your choice: " choice

  case "$choice" in
    1)
      read -rp "Source (cloud|json|jsonl|csv) [default: cloud]: " SOURCE
      read -rp "Target (bigquery|sqlite) [default: bigquery]: " TARGET
      read -rp "Limit (cloud only) [default: 5000]: " LIMIT

      SOURCE="${SOURCE:-cloud}"
      TARGET="${TARGET:-bigquery}"
      LIMIT="${LIMIT:-5000}"

      if [[ "${SOURCE}" == "cloud" ]]; then
        read -rp 'Cloud Logging filter query [default: resource.type="cloud_run_revision"]: ' FILTER_QUERY
        FILTER_QUERY="${FILTER_QUERY:-resource.type=\"cloud_run_revision\"}"

        run_python main.py \
          --extract-transform-load \
          --source "${SOURCE}" \
          --target "${TARGET}" \
          --filter-query "${FILTER_QUERY}" \
          --limit "${LIMIT}"
      else
        read -rp "Local file path [default: ./data/raw/sample.json]: " FILE_PATH
        FILE_PATH="${FILE_PATH:-./data/raw/sample.json}"

        run_python main.py \
          --extract-transform-load \
          --source "${SOURCE}" \
          --target "${TARGET}" \
          --file-path "${FILE_PATH}" \
          --limit "${LIMIT}"
      fi

      pause
      ;;
    2)
      read -rp "Target (bigquery|sqlite) [default: bigquery]: " TARGET
      TARGET="${TARGET:-bigquery}"

      run_python main.py --build-metrics-only --target "${TARGET}"
      pause
      ;;
    3)
      read -rp "Host [default: 0.0.0.0]: " HOST
      read -rp "Port [default: 8000]: " PORT
      read -rp "Reload? (y/n) [default: n]: " RELOAD

      HOST="${HOST:-0.0.0.0}"
      PORT="${PORT:-8000}"
      RELOAD="${RELOAD:-n}"

      if [[ "$RELOAD" == "y" || "$RELOAD" == "Y" ]]; then
        run_python main.py --run-api --host "$HOST" --port "$PORT" --reload
      else
        run_python main.py --run-api --host "$HOST" --port "$PORT"
      fi

      pause
      ;;
    4)
      echo ""
      echo ">>> Running pytest"
      cd "${PROJECT_ROOT}"
      $PYTHON_BIN -m pytest -q
      pause
      ;;
    5)
      ## DATA DRIFT ONLY
      read -rp "Reference dataset path [default: ./artifacts/reference.csv]: " REF_PATH
      REF_PATH="${REF_PATH:-./artifacts/reference.csv}"

      read -rp "Current dataset path [default: ./artifacts/current.csv]: " CUR_PATH
      CUR_PATH="${CUR_PATH:-./artifacts/current.csv}"

      run_python main.py --mode drift --ref "$REF_PATH" --current "$CUR_PATH"
      pause
      ;;
    6)
      ## FULL PIPELINE + DRIFT
      read -rp "Source (cloud|json|jsonl|csv) [default: cloud]: " SOURCE
      SOURCE="${SOURCE:-cloud}"

      read -rp "Reference dataset path [default: ./artifacts/reference.csv]: " REF_PATH
      REF_PATH="${REF_PATH:-./artifacts/reference.csv}"

      run_python main.py \
        --extract-transform-load \
        --source "${SOURCE}" \
        --with-drift \
        --ref "${REF_PATH}"

      pause
      ;;
    0)
      echo "Bye"
      exit 0
      ;;
    *)
      echo "Invalid choice."
      pause
      ;;
  esac
done