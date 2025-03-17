#!/bin/bash
#
# run_synthea_and_etl.sh
#
# Script to run Synthea data generation and trigger the ETL process.

set -euo pipefail

# Get project root directory
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$SCRIPT_DIR"

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

# Function to display usage information
usage() {
    echo "Usage: $0 [options]"
    echo "Options:"
    echo "  -p, --population <number>   Number of patients to generate (default: 1000)"
    echo "  -s, --seed <number>         Random seed for reproducibility (default: 1)"
    echo "  -S, --state <name>          State name (default: Massachusetts)"
    echo "  -c, --city <name>           City name (default: Bedford)"
    echo "  -g, --gender <M|F>          Gender filter (default: both)"
    echo "  -a, --age <range>           Age range (default: all)"
    echo "  -m, --module <name>         Module to run (default: all)"
    echo "  -h, --help                  Display this help message"
    exit 1
}

# Parse command line arguments
POPULATION="${SYNTHEA_POPULATION:-1000}"
SEED="${SYNTHEA_SEED:-1}"
STATE="${SYNTHEA_STATE:-Massachusetts}"
CITY="${SYNTHEA_CITY:-Bedford}"
GENDER="${SYNTHEA_GENDER:-}"
AGE="${SYNTHEA_AGE:-}"
MODULE="${SYNTHEA_MODULE:-}"

while [[ $# -gt 0 ]]; do
    case "$1" in
        -p|--population)
            POPULATION="$2"
            shift 2
            ;;
        -s|--seed)
            SEED="$2"
            shift 2
            ;;
        -S|--state)
            STATE="$2"
            shift 2
            ;;
        -c|--city)
            CITY="$2"
            shift 2
            ;;
        -g|--gender)
            GENDER="$2"
            shift 2
            ;;
        -a|--age)
            AGE="$2"
            shift 2
            ;;
        -m|--module)
            MODULE="$2"
            shift 2
            ;;
        -h|--help)
            usage
            ;;
        *)
            echo "Unknown option: $1"
            usage
            ;;
    esac
done

echo "Starting Synthea data generation with the following parameters:"
echo "  Population: $POPULATION"
echo "  Seed: $SEED"
echo "  State: $STATE"
echo "  City: $CITY"
echo "  Gender: $GENDER"
echo "  Age: $AGE"
echo "  Module: $MODULE"

# Run Synthea container
echo "Starting Synthea container..."
docker-compose run --rm \
    -e POPULATION="$POPULATION" \
    -e SEED="$SEED" \
    -e STATE="$STATE" \
    -e CITY="$CITY" \
    -e GENDER="$GENDER" \
    -e AGE="$AGE" \
    -e MODULE="$MODULE" \
    synthea

echo "Synthea data generation complete."

# Load data into staging
echo "Loading Synthea data into staging tables..."
"$PROJECT_ROOT/scripts/load_synthea_staging.sh"

# Run ETL
echo "Running ETL process..."
"$PROJECT_ROOT/run_etl_population_to_omop_optimized.py"

echo "ETL process complete."
