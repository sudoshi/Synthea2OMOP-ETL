#!/bin/bash
# run_optimized_import.sh - Script to run the optimized Synthea to OMOP ETL process

# Set default values
SYNTHEA_DIR="./synthea-output"
MAX_WORKERS=4
SKIP_OPTIMIZATION=false
SKIP_VALIDATION=false
DEBUG=false

# Parse command line arguments
while [[ $# -gt 0 ]]; do
  case $1 in
    --synthea-dir)
      SYNTHEA_DIR="$2"
      shift 2
      ;;
    --max-workers)
      MAX_WORKERS="$2"
      shift 2
      ;;
    --skip-optimization)
      SKIP_OPTIMIZATION=true
      shift
      ;;
    --skip-validation)
      SKIP_VALIDATION=true
      shift
      ;;
    --debug)
      DEBUG=true
      shift
      ;;
    *)
      echo "Unknown option: $1"
      exit 1
      ;;
  esac
done

# Build command with arguments
CMD="python3 optimized_synthea_to_omop.py --synthea-dir $SYNTHEA_DIR --max-workers $MAX_WORKERS"

if [ "$SKIP_OPTIMIZATION" = true ]; then
  CMD="$CMD --skip-optimization"
fi

if [ "$SKIP_VALIDATION" = true ]; then
  CMD="$CMD --skip-validation"
fi

if [ "$DEBUG" = true ]; then
  CMD="$CMD --debug"
fi

# Print configuration
echo "Running optimized Synthea to OMOP ETL with the following configuration:"
echo "  Synthea directory: $SYNTHEA_DIR"
echo "  Max workers: $MAX_WORKERS"
echo "  Skip optimization: $SKIP_OPTIMIZATION"
echo "  Skip validation: $SKIP_VALIDATION"
echo "  Debug mode: $DEBUG"
echo ""
echo "Command: $CMD"
echo ""

# Execute the command
echo "Starting ETL process..."
$CMD

# Check exit status
EXIT_CODE=$?
if [ $EXIT_CODE -eq 0 ]; then
  echo "ETL process completed successfully."
else
  echo "ETL process failed with exit code $EXIT_CODE."
fi

exit $EXIT_CODE
