#!/bin/bash
#
# run_synthea_data_pipeline.sh
#
# A complete pipeline for processing Synthea data:
# 1. Preprocess CSV files to fix formatting issues
# 2. Load processed data into PostgreSQL staging tables
#
# This script handles the malformed Synthea CSV files where data rows
# don't have proper separators while headers do.

set -euo pipefail

# Get script directory and project root
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

# Default configuration
INPUT_DIR="./synthea-output"
PROCESSED_DIR="./synthea-processed"
OVERWRITE_PROCESSED=false
DEBUG=false
FORCE_LOAD=false
MAX_WORKERS=4

# Process command line arguments
while [[ $# -gt 0 ]]; do
  case "$1" in
    --input-dir)
      INPUT_DIR="$2"
      shift 2
      ;;
    --processed-dir)
      PROCESSED_DIR="$2"
      shift 2
      ;;
    --overwrite-processed)
      OVERWRITE_PROCESSED=true
      shift
      ;;
    --debug)
      DEBUG=true
      shift
      ;;
    --force-load)
      FORCE_LOAD=true
      shift
      ;;
    --max-workers)
      MAX_WORKERS="$2"
      shift 2
      ;;
    -h|--help)
      echo "Usage: run_synthea_data_pipeline.sh [OPTIONS]"
      echo "Options:"
      echo "  --input-dir DIR           Directory with Synthea output files (default: ./synthea-output)"
      echo "  --processed-dir DIR       Directory for processed files (default: ./synthea-processed)"
      echo "  --overwrite-processed     Overwrite existing processed files"
      echo "  --debug                   Enable debug logging"
      echo "  --force-load              Force overwrite of existing database tables"
      echo "  --max-workers N           Maximum number of parallel workers (default: 4)"
      echo "  -h, --help                Show this help message"
      exit 0
      ;;
    *)
      echo "Unknown option: $1"
      echo "Use --help for usage information"
      exit 1
      ;;
  esac
done

# Configure paths
PYTHON="${PYTHON_PATH:-python3}"
PREPROCESS_SCRIPT="$PROJECT_ROOT/python/preprocess_synthea_csv.py"
LOAD_SCRIPT="$PROJECT_ROOT/scripts/load_synthea_staging.sh"

# Print banner
echo "====================================================================="
echo "               SYNTHEA DATA PROCESSING PIPELINE                      "
echo "====================================================================="
echo "Configuration:"
echo "  Input directory:       $INPUT_DIR"
echo "  Processed directory:   $PROCESSED_DIR"
echo "  Overwrite processed:   $OVERWRITE_PROCESSED"
echo "  Debug mode:            $DEBUG"
echo "  Force DB load:         $FORCE_LOAD"
echo "  Max workers:           $MAX_WORKERS"
echo "====================================================================="

# Ensure the scripts exist
if [ ! -f "$PREPROCESS_SCRIPT" ]; then
  echo "ERROR: Preprocess script not found: $PREPROCESS_SCRIPT"
  exit 1
fi

if [ ! -f "$LOAD_SCRIPT" ]; then
  echo "ERROR: Load script not found: $LOAD_SCRIPT"
  exit 1
fi

# 1. Preprocess Synthea CSV files
echo "STEP 1: Preprocessing Synthea CSV files"
echo "-----------------------------------------------------------------------"

PREPROCESS_CMD="$PYTHON $PREPROCESS_SCRIPT --input-dir $INPUT_DIR --output-dir $PROCESSED_DIR"

if [ "$OVERWRITE_PROCESSED" = true ]; then
  PREPROCESS_CMD="$PREPROCESS_CMD --overwrite"
fi

if [ "$DEBUG" = true ]; then
  PREPROCESS_CMD="$PREPROCESS_CMD --debug"
fi

echo "Running: $PREPROCESS_CMD"
START_TIME=$(date +%s)

# Execute preprocessing
eval "$PREPROCESS_CMD"
PREPROCESS_STATUS=$?

END_TIME=$(date +%s)
DURATION=$((END_TIME - START_TIME))

if [ $PREPROCESS_STATUS -ne 0 ]; then
  echo "ERROR: Preprocessing failed with status $PREPROCESS_STATUS"
  echo "See logs for details."
  exit $PREPROCESS_STATUS
fi

echo "Preprocessing completed in $DURATION seconds"
echo ""

# 2. Load processed data into database
echo "STEP 2: Loading processed data into database"
echo "-----------------------------------------------------------------------"

LOAD_CMD="SYNTHEA_DATA_DIR=$PROCESSED_DIR $LOAD_SCRIPT"

if [ "$FORCE_LOAD" = true ]; then
  LOAD_CMD="$LOAD_CMD --force"
fi

echo "Running: $LOAD_CMD"
START_TIME=$(date +%s)

# Execute database load
eval "$LOAD_CMD"
LOAD_STATUS=$?

END_TIME=$(date +%s)
DURATION=$((END_TIME - START_TIME))

if [ $LOAD_STATUS -ne 0 ]; then
  echo "ERROR: Database loading failed with status $LOAD_STATUS"
  echo "See logs for details."
  exit $LOAD_STATUS
fi

echo "Database loading completed in $DURATION seconds"
echo ""

# Final summary
echo "====================================================================="
echo "                      PIPELINE COMPLETE                              "
echo "====================================================================="
echo "Synthea data has been successfully:"
echo "  1. Preprocessed to fix CSV formatting issues"
echo "  2. Loaded into staging database tables"
echo ""
echo "You can now proceed with further ETL processing to convert"
echo "the staging data into OMOP CDM format."
echo "====================================================================="

exit 0
