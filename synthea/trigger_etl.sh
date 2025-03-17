#!/bin/bash
# trigger_etl.sh
#
# Script to trigger the ETL process after Synthea data generation is complete.

set -euo pipefail

# Get project root directory
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

# Load environment variables from .env file if it exists
if [ -f "$PROJECT_ROOT/.env" ]; then
    echo "Loading configuration from $PROJECT_ROOT/.env"
    set -a  # automatically export all variables
    source "$PROJECT_ROOT/.env"
    set +a
else
    echo "Warning: .env file not found in $PROJECT_ROOT"
    echo "Using default configuration values"
fi

# Wait for Synthea to complete
echo "Waiting for Synthea data generation to complete..."
while [ ! -f "${SYNTHEA_OUTPUT_DIR:-/synthea-output}/.complete" ]; do
  sleep 5
done

echo "Synthea data generation complete. Starting ETL process..."

# Load data into staging
echo "Loading Synthea data into staging tables..."
"$PROJECT_ROOT/scripts/load_synthea_staging.sh"

# Run ETL
echo "Running ETL process..."
"$PROJECT_ROOT/run_etl_population_to_omop_optimized.py"

echo "ETL process complete."
