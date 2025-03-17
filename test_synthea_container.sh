#!/bin/bash
#
# test_synthea_container.sh
#
# Script to test the Synthea container by generating a small dataset.

set -euo pipefail

echo "Building Synthea container..."
docker-compose build synthea

echo "Running Synthea container with a small test population (10 patients)..."
docker-compose run --rm \
    -e POPULATION=10 \
    -e SEED=1 \
    -e STATE=Massachusetts \
    -e CITY=Bedford \
    synthea

echo "Checking for output files..."
# Use docker to check if the marker file exists in the volume
if ! docker run --rm -v synthea2omop-etl_synthea-output:/output alpine:latest test -f /output/.complete; then
    echo "Error: Could not find marker file. Container may not have completed successfully."
    exit 1
fi

# Count CSV files in the output directory using docker
csv_count=$(docker run --rm -v synthea2omop-etl_synthea-output:/output alpine:latest find /output -name "*.csv" | wc -l)
if [ "$csv_count" -eq 0 ]; then
    echo "Error: No CSV files found in the output directory."
    exit 1
fi

echo "Success! Synthea container generated $csv_count CSV files."
echo "You can now run the full ETL process with: ./run_synthea_and_etl.sh"
