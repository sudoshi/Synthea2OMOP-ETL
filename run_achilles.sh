#!/bin/bash
#
# run_achilles.sh - Run Achilles analysis on OMOP CDM database
#
# This script runs the Achilles analysis tool to generate descriptive statistics
# and data quality metrics for an OMOP CDM database.
#

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
BOLD='\033[1m'
NC='\033[0m'

# Get project root directory
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR" && pwd)"

# Load environment variables from .env file if it exists
if [ -f "$PROJECT_ROOT/.env" ]; then
    source "$PROJECT_ROOT/.env"
fi

# Default configuration
DB_HOST="${DB_HOST:-localhost}"
DB_PORT="${DB_PORT:-5432}"
DB_NAME="${DB_NAME:-ohdsi}"
DB_USER="${DB_USER:-postgres}"
DB_PASSWORD="${DB_PASSWORD:-acumenus}"
CDM_SCHEMA="${CDM_SCHEMA:-omop}"
RESULTS_SCHEMA="${RESULTS_SCHEMA:-achilles_results}"
VOCAB_SCHEMA="${VOCAB_SCHEMA:-omop}"
THREADS="${THREADS:-4}"

# Print help
print_help() {
  echo -e "${BOLD}USAGE${NC}"
  echo "  $0 [options]"
  echo ""
  echo -e "${BOLD}OPTIONS${NC}"
  echo "  --help                   Show this help message and exit"
  echo "  --host <host>            Database host (default: $DB_HOST)"
  echo "  --port <port>            Database port (default: $DB_PORT)"
  echo "  --db <database>          Database name (default: $DB_NAME)"
  echo "  --user <username>        Database username (default: $DB_USER)"
  echo "  --password <password>    Database password"
  echo "  --cdm-schema <schema>    CDM schema name (default: $CDM_SCHEMA)"
  echo "  --results-schema <schema> Results schema name (default: $RESULTS_SCHEMA)"
  echo "  --vocab-schema <schema>  Vocabulary schema name (default: $VOCAB_SCHEMA)"
  echo "  --threads <num>          Number of threads to use (default: $THREADS)"
  echo ""
}

# Parse command line arguments
while [ $# -gt 0 ]; do
  case "$1" in
    --help)
      print_help
      exit 0
      ;;
    --host)
      DB_HOST="$2"
      shift 2
      ;;
    --port)
      DB_PORT="$2"
      shift 2
      ;;
    --db)
      DB_NAME="$2"
      shift 2
      ;;
    --user)
      DB_USER="$2"
      shift 2
      ;;
    --password)
      DB_PASSWORD="$2"
      shift 2
      ;;
    --cdm-schema)
      CDM_SCHEMA="$2"
      shift 2
      ;;
    --results-schema)
      RESULTS_SCHEMA="$2"
      shift 2
      ;;
    --vocab-schema)
      VOCAB_SCHEMA="$2"
      shift 2
      ;;
    --threads)
      THREADS="$2"
      shift 2
      ;;
    *)
      echo -e "${RED}Unknown option: $1${NC}"
      print_help
      exit 1
      ;;
  esac
done

# Log function
log() {
  local timestamp=$(date "+%Y-%m-%d %H:%M:%S")
  echo -e "${timestamp} $1"
}

log "${BLUE}======================================================================${NC}"
log "${BOLD}                      ACHILLES ANALYSIS TOOL                          ${NC}"
log "${BLUE}======================================================================${NC}"
log "Starting Achilles analysis with the following settings:"
log "  Database: ${YELLOW}${DB_NAME}${NC} on ${YELLOW}${DB_HOST}:${DB_PORT}${NC}"
log "  CDM Schema: ${YELLOW}${CDM_SCHEMA}${NC}"
log "  Results Schema: ${YELLOW}${RESULTS_SCHEMA}${NC}"
log "  Vocabulary Schema: ${YELLOW}${VOCAB_SCHEMA}${NC}"
log "  Threads: ${YELLOW}${THREADS}${NC}"
log "${BLUE}======================================================================${NC}"
log ""

# Check if PostgreSQL JDBC driver exists, download if not
DRIVER_DIR="${PROJECT_ROOT}/achilles/drivers"
DRIVER_PATH="${DRIVER_DIR}/postgresql-42.2.23.jar"

if [ ! -f "${DRIVER_PATH}" ]; then
    log "${YELLOW}PostgreSQL JDBC driver not found. Downloading...${NC}"
    mkdir -p "${DRIVER_DIR}"
    curl -L -o "${DRIVER_PATH}" "https://jdbc.postgresql.org/download/postgresql-42.2.23.jar"
    log "${GREEN}PostgreSQL JDBC driver downloaded successfully.${NC}"
fi

# Create temporary config file
TEMP_DIR=$(mktemp -d)
CONFIG_FILE="${TEMP_DIR}/config.json"
PROGRESS_FILE="${TEMP_DIR}/progress.json"
RESULTS_FILE="${TEMP_DIR}/results.json"
OUTPUT_DIR="${PROJECT_ROOT}/achilles/output"

mkdir -p "${OUTPUT_DIR}"

log "Creating configuration file..."
cat > "${CONFIG_FILE}" << EOF
{
  "dbms": "postgresql",
  "server": "${DB_HOST}/${DB_NAME}",
  "port": "${DB_PORT}",
  "user": "${DB_USER}",
  "password": "${DB_PASSWORD}",
  "pathToDriver": "${DRIVER_DIR}",
  "cdmDatabaseSchema": "${CDM_SCHEMA}",
  "resultsDatabaseSchema": "${RESULTS_SCHEMA}",
  "vocabDatabaseSchema": "${VOCAB_SCHEMA}",
  "sourceName": "Synthea",
  "createTable": true,
  "smallCellCount": 5,
  "cdmVersion": "5.4",
  "createIndices": true,
  "numThreads": 1,
  "tempAchillesPrefix": "tmpach",
  "dropScratchTables": true,
  "sqlOnly": false,
  "outputFolder": "${OUTPUT_DIR}",
  "verboseMode": true,
  "optimizeAtlasCache": true,
  "defaultAnalysesOnly": true,
  "updateGivenAnalysesOnly": false,
  "excludeAnalysisIds": [],
  "sqlDialect": "postgresql",
  "progressFile": "${PROGRESS_FILE}",
  "resultsFile": "${RESULTS_FILE}"
}
EOF

log "Configuration file created at ${CONFIG_FILE}"
log "Starting Achilles analysis..."

# Ensure results schema exists
log "Ensuring results schema ${RESULTS_SCHEMA} exists..."
export PGPASSWORD="${DB_PASSWORD}"
psql -h "${DB_HOST}" -p "${DB_PORT}" -U "${DB_USER}" -d "${DB_NAME}" -c "CREATE SCHEMA IF NOT EXISTS ${RESULTS_SCHEMA};"

# Check if R is installed
if ! command -v Rscript &> /dev/null; then
    log "${RED}Error: Rscript is not installed. Please install R before running Achilles.${NC}"
    exit 1
fi

# Check if required R packages are installed
log "Checking R packages..."
Rscript -e "if (!require('Achilles')) { install.packages('remotes', repos='https://cloud.r-project.org/'); remotes::install_github('OHDSI/Achilles') }"
Rscript -e "if (!require('DatabaseConnector')) install.packages('DatabaseConnector', repos='https://cloud.r-project.org/')"
Rscript -e "if (!require('SqlRender')) install.packages('SqlRender', repos='https://cloud.r-project.org/')"
Rscript -e "if (!require('jsonlite')) install.packages('jsonlite', repos='https://cloud.r-project.org/')"

# Run Achilles
log "${GREEN}Executing Achilles...${NC}"
Rscript "${PROJECT_ROOT}/achilles/scripts/run_achilles.R" "${CONFIG_FILE}"

# Check if analysis was successful
if [ $? -eq 0 ]; then
    log "${GREEN}Achilles analysis completed successfully!${NC}"
    log "Results are available in the ${YELLOW}${RESULTS_SCHEMA}${NC} schema."
    
    # Display some summary information
    if [ -f "${RESULTS_FILE}" ]; then
        SUMMARY=$(cat "${RESULTS_FILE}")
        log "Analysis summary:"
        log "${SUMMARY}"
    fi
else
    log "${RED}Achilles analysis failed. Check the logs for details.${NC}"
    if [ -f "${RESULTS_FILE}" ]; then
        ERROR=$(cat "${RESULTS_FILE}")
        log "Error details:"
        log "${ERROR}"
    fi
fi

# Clean up temporary files
rm -rf "${TEMP_DIR}"

log "${BLUE}======================================================================${NC}"
log "${BOLD}                      ACHILLES ANALYSIS COMPLETE                      ${NC}"
log "${BLUE}======================================================================${NC}"
