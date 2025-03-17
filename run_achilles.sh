#!/bin/bash

# Script to run Achilles analysis directly

# Check if PostgreSQL JDBC driver exists, download if not
if [ ! -f "./achilles/drivers/postgresql-42.2.23.jar" ]; then
    echo "PostgreSQL JDBC driver not found. Downloading..."
    ./achilles/download_jdbc_driver.sh
fi

# Default configuration
DB_HOST=${DB_HOST:-"localhost"}
DB_PORT=${DB_PORT:-"5432"}
API_PORT=${API_PORT:-"5081"}
DB_NAME=${DB_NAME:-"synthea"}
DB_USER=${DB_USER:-"postgres"}
DB_PASSWORD=${DB_PASSWORD:-"acumenus"}
CDM_SCHEMA=${CDM_SCHEMA:-"omop"}
RESULTS_SCHEMA=${RESULTS_SCHEMA:-"achilles_results"}
VOCAB_SCHEMA=${VOCAB_SCHEMA:-"omop"}

# Create temporary config file
TEMP_DIR=$(mktemp -d)
CONFIG_FILE="${TEMP_DIR}/config.json"
PROGRESS_FILE="${TEMP_DIR}/progress.json"
RESULTS_FILE="${TEMP_DIR}/results.json"

cat > "${CONFIG_FILE}" << EOF
{
  "dbms": "postgresql",
  "server": "${DB_HOST}/${DB_NAME}",
  "port": "${DB_PORT}",
  "user": "${DB_USER}",
  "password": "${DB_PASSWORD}",
  "pathToDriver": "./achilles/drivers",
  "cdmDatabaseSchema": "${CDM_SCHEMA}",
  "resultsDatabaseSchema": "${RESULTS_SCHEMA}",
  "vocabDatabaseSchema": "${VOCAB_SCHEMA}",
  "sourceName": "Synthea",
  "createTable": true,
  "smallCellCount": 5,
  "cdmVersion": "5.4",
  "createIndices": true,
  "numThreads": 4,
  "tempAchillesPrefix": "tmpach",
  "dropScratchTables": true,
  "sqlOnly": false,
  "outputFolder": "./achilles/output",
  "verboseMode": true,
  "optimizeAtlasCache": true,
  "defaultAnalysesOnly": true,
  "updateGivenAnalysesOnly": false,
  "excludeAnalysisIds": false,
  "sqlDialect": "postgresql",
  "progressFile": "${PROGRESS_FILE}",
  "resultsFile": "${RESULTS_FILE}"
}
EOF

echo "Starting Achilles analysis..."
echo "Configuration:"
echo "  Database: ${DB_HOST}:${DB_PORT}/${DB_NAME}"
echo "  CDM Schema: ${CDM_SCHEMA}"
echo "  Results Schema: ${RESULTS_SCHEMA}"
echo "  Vocabulary Schema: ${VOCAB_SCHEMA}"
echo ""

# Build and run the Achilles Docker container
docker build -t achilles-r ./achilles

docker run --rm \
  -v "${CONFIG_FILE}:/app/config.json" \
  -v "${PROGRESS_FILE}:/app/progress.json" \
  -v "${RESULTS_FILE}:/app/results.json" \
  -v "$(pwd)/achilles/scripts:/app/scripts" \
  -v "$(pwd)/achilles/output:/app/output" \
  -v "$(pwd)/achilles/drivers:/drivers" \
  --network=host \
  achilles-r "/app/config.json"

# Check if analysis was successful
if [ $? -eq 0 ]; then
    echo "Achilles analysis completed successfully!"
    echo "Results are available in the ${RESULTS_SCHEMA} schema."
else
    echo "Achilles analysis failed. Check the logs for details."
fi

# Clean up temporary files
rm -rf "${TEMP_DIR}"
