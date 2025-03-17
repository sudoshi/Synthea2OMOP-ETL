#!/usr/bin/env bash
#
# load_synthea_staging.sh
#
# A script to drop any existing table, recreate it with TEXT columns
# from the CSV header, and bulk load via \copy in a single-line command.
#
# Enhanced version: Uses configuration from .env file

set -euo pipefail

##############################################################################
# 1) CONFIGURATION
##############################################################################
# Get project root directory (parent of scripts directory)
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

# Use environment variables with defaults
DB_HOST="${DB_HOST:-localhost}"
DB_PORT="${DB_PORT:-5432}"
DB_NAME="${DB_NAME:-synthea}"
DB_USER="${DB_USER:-postgres}"
DB_PASSWORD="${DB_PASSWORD:-acumenus}"
DB_SCHEMA="${POPULATION_SCHEMA:-population}"
SYNTHEA_DATA_DIR="${SYNTHEA_DATA_DIR:-/synthea-output}"

# Export password for psql
export PGPASSWORD="$DB_PASSWORD"

# Change to the Synthea data directory
cd "$SYNTHEA_DATA_DIR"

echo "Loading CSV files from directory: $SYNTHEA_DATA_DIR"
echo "Using database: $DB_NAME at host: $DB_HOST (schema: $DB_SCHEMA)"

# Ensure the schema exists
psql -h "$DB_HOST" -p "$DB_PORT" -U "$DB_USER" -d "$DB_NAME" \
     -c "CREATE SCHEMA IF NOT EXISTS $DB_SCHEMA;"

# Create a log directory if it doesn't exist
LOG_DIR="$PROJECT_ROOT/logs"
mkdir -p "$LOG_DIR"
LOG_FILE="$LOG_DIR/synthea_load_$(date +%Y%m%d_%H%M%S).log"
echo "Logging to: $LOG_FILE"

# Function to log messages to both console and log file
log() {
    echo "$(date +"%Y-%m-%d %H:%M:%S") - $1" | tee -a "$LOG_FILE"
}

log "Starting Synthea data loading process"

##############################################################################
# 2) PROCESS EACH *.csv FILE
##############################################################################
# Count total CSV files for progress reporting
total_files=$(ls -1 *.csv 2>/dev/null | wc -l)
if [ "$total_files" -eq 0 ]; then
    log "No CSV files found in $SYNTHEA_DATA_DIR"
    exit 1
fi

log "Found $total_files CSV files to process"
current_file=0

for csv_file in *.csv; do
  # If no CSVs are found, the glob remains '*.csv'
  if [[ "$csv_file" == '*.csv' ]]; then
    log "No CSV files found in $SYNTHEA_DATA_DIR"
    break
  fi

  # Update progress
  ((current_file++))
  progress=$((current_file * 100 / total_files))
  
  # Derive table name from file name (strip .csv)
  table_name="${csv_file%.csv}"

  log "[$progress%] Processing file $current_file of $total_files: $csv_file"
  log "Will drop and recreate table: $DB_SCHEMA.$table_name"

  # 1) Drop the table if it exists
  psql -h "$DB_HOST" -p "$DB_PORT" -U "$DB_USER" -d "$DB_NAME" \
       -c "DROP TABLE IF EXISTS \"$DB_SCHEMA\".\"$table_name\" CASCADE;" 2>&1 | tee -a "$LOG_FILE"

  # 2) Read the first (header) line of the CSV
  header_line="$(head -n 1 "$csv_file")"

  # 3) Split header on commas into an array of column names
  IFS=',' read -r -a columns <<< "$header_line"

  # 4) Build a CREATE TABLE statement with all TEXT columns
  create_sql="CREATE TABLE \"$DB_SCHEMA\".\"$table_name\" ("
  for col in "${columns[@]}"; do
    # Remove any surrounding quotes
    trimmed_col="$(echo "$col" | sed -E 's/^\"|\"$//g')"
    # Escape internal double-quotes
    escaped_col="${trimmed_col//\"/\"\"}"
    # Quote the identifier
    create_sql+="\"$escaped_col\" TEXT, "
  done
  # Remove trailing comma, close parenthesis
  create_sql="${create_sql%, } );"

  # 5) Create the table
  psql -h "$DB_HOST" -p "$DB_PORT" -U "$DB_USER" -d "$DB_NAME" \
       -c "$create_sql" 2>&1 | tee -a "$LOG_FILE"

  # 6) Load the data using a single-line \copy command
  log "Loading data into $DB_SCHEMA.$table_name ..."
  psql -h "$DB_HOST" -p "$DB_PORT" -U "$DB_USER" -d "$DB_NAME" \
       -c "\copy \"$DB_SCHEMA\".\"$table_name\" FROM '${csv_file}' CSV HEADER DELIMITER ',' QUOTE '\"' ESCAPE '\"';" 2>&1 | tee -a "$LOG_FILE"

  # 7) Count rows and log
  row_count=$(psql -h "$DB_HOST" -p "$DB_PORT" -U "$DB_USER" -d "$DB_NAME" -t \
              -c "SELECT COUNT(*) FROM \"$DB_SCHEMA\".\"$table_name\";")
  log "Loaded $row_count rows into $DB_SCHEMA.$table_name"
  
  log "Finished loading $csv_file -> $DB_SCHEMA.$table_name"
done

log "All CSV files have been processed!"
log "Data is stored in staging tables under schema: $DB_SCHEMA"
log "All columns are TEXT. Transform/cast as needed."
log "Log file: $LOG_FILE"
