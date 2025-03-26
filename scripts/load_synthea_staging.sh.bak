#!/usr/bin/env bash

# load_synthea_staging.sh
#
# A script to load Synthea CSV files into PostgreSQL staging tables.
# Features:
# - Real-time progress bars with row counts
# - Table existence checks to prevent data loss
# - Per-second progress updates
# - Summary report with timing information
#
# Enhanced version: Uses configuration from .env file and integrates with tqdm progress reporting

set -euo pipefail

##############################################################################
# 1) CONFIGURATION AND PARAMETERS
##############################################################################
# Get project root directory (parent of scripts directory)
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

# Default settings for command-line options
OVERWRITE_MODE="ask"  # Options: ask, force, skip
PROGRESS_UPDATE_INTERVAL=1  # Update progress bar every X seconds
SHOW_PROGRESS_BAR=true

# Parse command line arguments
while [[ $# -gt 0 ]]; do
  case "$1" in
    --force)
      OVERWRITE_MODE="force"
      shift
      ;;
    --skip-existing)
      OVERWRITE_MODE="skip"
      shift
      ;;
    --progress-interval)
      PROGRESS_UPDATE_INTERVAL="$2"
      shift 2
      ;;
    --no-progress-bar)
      SHOW_PROGRESS_BAR=false
      shift
      ;;
    # Add help option
    -h|--help)
      echo "Usage: load_synthea_staging.sh [OPTIONS]"
      echo "Options:"
      echo "  --force                Overwrite existing tables without asking"
      echo "  --skip-existing        Skip tables that already have data"
      echo "  --progress-interval N  Update progress bar every N seconds (default: 1)"
      echo "  --no-progress-bar      Disable progress bar display"
      echo "  -h, --help             Show this help message"
      exit 0
      ;;
    *)
      echo "Unknown option: $1"
      echo "Use --help for usage information"
      exit 1
      ;;
  esac
done

# Store command-line provided variables (if they exist)
CLI_DB_HOST="${DB_HOST:-}" 2>/dev/null || CLI_DB_HOST=""
CLI_DB_PORT="${DB_PORT:-}" 2>/dev/null || CLI_DB_PORT=""
CLI_DB_NAME="${DB_NAME:-}" 2>/dev/null || CLI_DB_NAME=""
CLI_DB_USER="${DB_USER:-}" 2>/dev/null || CLI_DB_USER=""
CLI_DB_PASSWORD="${DB_PASSWORD:-}" 2>/dev/null || CLI_DB_PASSWORD=""
CLI_DB_SCHEMA="${DB_SCHEMA:-}" 2>/dev/null || CLI_DB_SCHEMA=""
CLI_SYNTHEA_DATA_DIR="${SYNTHEA_DATA_DIR:-}" 2>/dev/null || CLI_SYNTHEA_DATA_DIR=""

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

# Restore command-line provided variables (they take precedence)
[ -n "${CLI_DB_HOST}" ] && DB_HOST="$CLI_DB_HOST"
[ -n "${CLI_DB_PORT}" ] && DB_PORT="$CLI_DB_PORT"
[ -n "${CLI_DB_NAME}" ] && DB_NAME="$CLI_DB_NAME"
[ -n "${CLI_DB_USER}" ] && DB_USER="$CLI_DB_USER"
[ -n "${CLI_DB_PASSWORD}" ] && DB_PASSWORD="$CLI_DB_PASSWORD"
[ -n "${CLI_DB_SCHEMA}" ] && DB_SCHEMA="$CLI_DB_SCHEMA"
[ -n "${CLI_SYNTHEA_DATA_DIR}" ] && SYNTHEA_DATA_DIR="$CLI_SYNTHEA_DATA_DIR"

# Set defaults for any variables that aren't set yet
DB_HOST="${DB_HOST:-localhost}"
DB_PORT="${DB_PORT:-5432}"
DB_NAME="${DB_NAME:-ohdsi}"
DB_USER="${DB_USER:-postgres}"
DB_PASSWORD="${DB_PASSWORD:-acumenus}"
DB_SCHEMA="${DB_SCHEMA:-staging}"
SYNTHEA_DATA_DIR="${SYNTHEA_DATA_DIR:-./synthea-output}"

# Export password for psql
export PGPASSWORD="$DB_PASSWORD"

# Create a log directory if it doesn't exist
LOG_DIR="$PROJECT_ROOT/logs"
mkdir -p "$LOG_DIR"
LOG_FILE="$LOG_DIR/synthea_load_$(date +%Y%m%d_%H%M%S).log"

# Create a temp directory for progress tracking
TEMP_DIR="$PROJECT_ROOT/tmp"
mkdir -p "$TEMP_DIR"

# ANSI colors for enhanced output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[0;33m'
BLUE='\033[0;34m'
BOLD='\033[1m'
NC='\033[0m' # No Color

##############################################################################
# 2) HELPER FUNCTIONS
##############################################################################

# Function to log messages to both console and log file
log() {
    echo -e "$(date +"%Y-%m-%d %H:%M:%S") - $1" | tee -a "$LOG_FILE"
}

# Check if terminal supports colors
check_terminal_colors() {
    if [[ -t 1 ]] && [[ "$(tput colors 2>/dev/null)" -ge 8 ]]; then
        return 0
    else
        # Remove color codes
        RED=''
        GREEN=''
        YELLOW=''
        BLUE=''
        BOLD=''
        NC=''
        return 1
    fi
}

# Function to check if a table exists and has data
# Returns: 0 if table exists with data
#          1 if table doesn't exist
#          2 if table exists but has no data
table_exists_with_data() {
  local schema="$1"
  local table="$2"

  # Check if table exists
  local exists=$(psql -h "$DB_HOST" -p "$DB_PORT" -U "$DB_USER" -d "$DB_NAME" -t -c "SELECT EXISTS (
    SELECT FROM information_schema.tables
    WHERE table_schema = '$schema' AND table_name = '$table'
  );")

  # If table doesn't exist return 1 (false)
  if [[ $(echo "$exists" | tr -d '[:space:]') != "t" ]]; then
    return 1
  fi

  # Check if table has data
  local count=$(psql -h "$DB_HOST" -p "$DB_PORT" -U "$DB_USER" -d "$DB_NAME" -t -c "
    SELECT COUNT(*) FROM \"$schema\".\"$table\" LIMIT 1;
  ")

  # If count > 0 table has data return 0 (true)
  count=$(echo "$count" | tr -d '[:space:]')
  if [[ -n "$count" ]] && (( count > 0 )); then
    return 0
  else
    return 2  # Table exists but has no data
  fi
}

# Function to count rows in a CSV (excluding header)
count_csv_rows() {
  local file="$1"
  log "Counting rows in $file"
  if [[ ! -r "$file" ]]; then
    log "${RED}ERROR: Cannot read file $file${NC}"
    return 1
  fi
  local count=$(wc -l < "$file")
  local result=$((count - 1))  # Subtract 1 for header
  log "Found $result rows in $file (excluding header)"
  echo $result
}

# New function to update progress in a standardized format
update_progress() {
  local current="$1"
  local total="$2"
  local table_name="$3"
  
  # Only output to stderr if progress bar is enabled
  if [[ "$SHOW_PROGRESS_BAR" == "true" ]]; then
    # Format: progress: CURRENT/TOTAL MESSAGE
    echo "progress: $current/$total Loading $table_name" >&2
  fi
}

# Function to track loading progress in real-time
track_loading_progress() {
  local csv_file="$1"
  local table_schema="$2"
  local table_name="$3"
  local total_rows="$4"
  local pid_file="$TEMP_DIR/progress_${table_name}.pid"

  # Start a background process to monitor progress
  (
    # Store our PID for later cleanup
    echo $$ > "$pid_file"

    # Check row count every PROGRESS_UPDATE_INTERVAL seconds
    while true; do
      current=$(psql -h "$DB_HOST" -p "$DB_PORT" -U "$DB_USER" -d "$DB_NAME" -t -c "
        SELECT COUNT(*) FROM \"$table_schema\".\"$table_name\";
      " 2>/dev/null | tr -d '[:space:]')

      if [[ -n "$current" && "$current" =~ ^[0-9]+$ ]]; then
        # Update progress in standardized format for shell wrapper to detect
        update_progress "$current" "$total_rows" "$table_name"

        # If we've loaded all rows or more we're done
        if (( current >= total_rows )); then
          break
        fi
      fi

      sleep "$PROGRESS_UPDATE_INTERVAL"
    done

    # Final 100% progress
    update_progress "$total_rows" "$total_rows" "$table_name"

    # Clean up PID file
    rm -f "$pid_file"

  ) &

  # Return the pid of the background monitoring process
  echo $!
}

# Function to stop progress tracking
stop_progress_tracking() {
  local table_name="$1"
  local pid_file="$TEMP_DIR/progress_${table_name}.pid"

  if [[ -f "$pid_file" ]]; then
    local pid=$(cat "$pid_file")
    if [[ -n "$pid" ]]; then
      kill -9 "$pid" >/dev/null 2>&1 || true
    fi
    rm -f "$pid_file"
  fi
}

# Function to prompt for yes/no
ask_yes_no() {
  local prompt="$1"
  local answer

  while true; do
    read -p "$prompt [y/n]: " answer
    case "${answer}" in
      y|yes) return 0 ;;
      n|no) return 1 ;;
      *) echo "Please answer yes or no" ;;
    esac
  done
}

# Function to format time in seconds to min:sec
format_time() {
  local seconds="$1"
  local minutes=$((seconds / 60))
  local rem_seconds=$((seconds % 60))
  printf "%02d:%02d" $minutes $rem_seconds
}

##############################################################################
# 3) MAIN PROCESS
##############################################################################

# Check if terminal supports colors
check_terminal_colors

# Print header
echo -e "${BOLD}=====================================================================${NC}"
echo -e "${BOLD}               SYNTHEA TO STAGING DATA LOADER                        ${NC}"
echo -e "${BOLD}=====================================================================${NC}"
log "Starting Synthea data loading process with the following configuration:"
log "  Database: ${BLUE}$DB_NAME${NC} on ${BLUE}$DB_HOST:$DB_PORT${NC} (schema: ${BLUE}$DB_SCHEMA${NC})"
log "  Data directory: ${BLUE}$SYNTHEA_DATA_DIR${NC}"
log "  Overwrite mode: ${YELLOW}$OVERWRITE_MODE${NC}"
log "  Progress bar: ${YELLOW}$SHOW_PROGRESS_BAR${NC} (update interval: ${PROGRESS_UPDATE_INTERVAL}s)"
log "Logging to: ${BLUE}$LOG_FILE${NC}"

# Ensure we're in the Synthea data directory
if [ ! -d "$SYNTHEA_DATA_DIR" ]; then
  log "${RED}ERROR: Synthea data directory does not exist: $SYNTHEA_DATA_DIR${NC}"
  exit 1
fi
cd "$SYNTHEA_DATA_DIR" || {
  log "${RED}ERROR: Cannot change to directory: $SYNTHEA_DATA_DIR${NC}"
  exit 1
}
log "Successfully changed to directory: ${BLUE}$SYNTHEA_DATA_DIR${NC}"
log "Directory contains: $(ls -la | head -10)"

# Ensure the database schema exists
log "Ensuring schema ${BLUE}$DB_SCHEMA${NC} exists..."
psql -h "$DB_HOST" -p "$DB_PORT" -U "$DB_USER" -d "$DB_NAME" \
     -c "CREATE SCHEMA IF NOT EXISTS $DB_SCHEMA;" 2>&1 | tee -a "$LOG_FILE"

# Init summary table for final report
declare -a SUMMARY_TABLE=()

# Count total CSV files for progress reporting
total_files=$(ls -1 *.csv 2>/dev/null | wc -l || echo 0)
if [ "$total_files" -eq 0 ]; then
    log "${RED}ERROR: No CSV files found in $SYNTHEA_DATA_DIR${NC}"
    exit 1
fi

# Start the global timer
GLOBAL_START_TIME=$(date +%s)

log "Found ${BOLD}$total_files${NC} CSV files to process"
current_file=0

# Standardized progress format for start of stage
echo "progress: 0/$total_files Starting Loading to Staging" >&2

for csv_file in *.csv; do
  # If no CSVs are found the glob remains '*.csv'
  if [[ "$csv_file" == '*.csv' ]]; then
    log "${RED}ERROR: No CSV files found in $SYNTHEA_DATA_DIR${NC}"
    break
  fi

  # Update progress
  ((current_file++))
  progress=$((current_file * 100 / total_files))
  
  # Standardized progress format for shell wrapper to detect
  echo "progress: $current_file/$total_files Loading $csv_file" >&2

  # Start timer for this file
  FILE_START_TIME=$(date +%s)

  # Derive table name from file name (strip .csv)
  table_name="${csv_file%.csv}"

  log "[${YELLOW}$progress%${NC}] Processing file $current_file of $total_files: ${BOLD}$csv_file${NC}"

  # Count rows in CSV for progress tracking (faster method for large files)
  total_csv_rows=$(grep -c "" "$csv_file" 2>/dev/null || echo 1)
  
  # If only one row it's just the header
  if [[ "$total_csv_rows" -eq 1 ]]; then
    total_csv_rows=0
  else
    # There's at least one data row beyond the header
    total_csv_rows=$((total_csv_rows - 1))
  fi

  log "CSV contains ${BOLD}$total_csv_rows${NC} rows of data"

  # Check if table exists and has data
  if table_exists_with_data "$DB_SCHEMA" "$table_name"; then
    existing_rows=$(psql -h "$DB_HOST" -p "$DB_PORT" -U "$DB_USER" -d "$DB_NAME" -t \
                   -c "SELECT COUNT(*) FROM \"$DB_SCHEMA\".\"$table_name\";" | tr -d '[:space:]')

    log "Table ${BLUE}$DB_SCHEMA.$table_name${NC} already exists with ${BOLD}$existing_rows${NC} rows"

    # Decide what to do based on overwrite mode
    case "$OVERWRITE_MODE" in
      force)
        log "Overwrite mode is 'force' - will drop and recreate table"
        ;;
      skip)
        log "Overwrite mode is 'skip' - skipping this table"
        # Add to summary with "skipped" status
        SUMMARY_TABLE+=("$table_name|$existing_rows|$existing_rows|0|skipped")
        continue
        ;;
      ask)
        if ! ask_yes_no "Table $DB_SCHEMA.$table_name already exists with data. Overwrite?"; then
          log "User chose not to overwrite - skipping this table"
          # Add to summary with "skipped" status
          SUMMARY_TABLE+=("$table_name|$existing_rows|$existing_rows|0|skipped")
          continue
        fi
        log "User chose to overwrite the table"
        ;;
    esac
  elif table_exists_with_data "$DB_SCHEMA" "$table_name" 2>/dev/null; then
    log "Table ${BLUE}$DB_SCHEMA.$table_name${NC} exists but is empty"
    existing_rows=0
  else
    log "Table ${BLUE}$DB_SCHEMA.$table_name${NC} does not exist"
    existing_rows=0
  fi

  # 1) Drop the table if it exists
  log "Dropping table ${BLUE}$DB_SCHEMA.$table_name${NC} if it exists..."
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
    create_sql+="\"$escaped_col\" TEXT,"
  done
  # Remove trailing comma, close parenthesis
  create_sql="${create_sql%,} );"

  # 5) Create the table
  log "Creating table ${BLUE}$DB_SCHEMA.$table_name${NC}..."
  psql -h "$DB_HOST" -p "$DB_PORT" -U "$DB_USER" -d "$DB_NAME" \
       -c "$create_sql" 2>&1 | tee -a "$LOG_FILE"

  # 6) Start progress tracking in the background
  log "Loading data into ${BLUE}$DB_SCHEMA.$table_name${NC} (${BOLD}$total_csv_rows${NC} rows)..."

  # Start the progress tracker if enabled
  progress_pid=""
  if [[ "$SHOW_PROGRESS_BAR" == "true" ]]; then
    progress_pid=$(track_loading_progress "$csv_file" "$DB_SCHEMA" "$table_name" "$total_csv_rows")
  fi

  # 7) Load the data using a single-line \copy command
  set +e  # Temporarily disable exit on error
  psql -h "$DB_HOST" -p "$DB_PORT" -U "$DB_USER" -d "$DB_NAME" \
       -c "\copy \"$DB_SCHEMA\".\"$table_name\" FROM '${csv_file}' CSV HEADER DELIMITER ',' QUOTE '\"' ESCAPE '\\';" 2>&1 | tee -a "$LOG_FILE"
  LOAD_STATUS=$?
  set -e  # Re-enable exit on error

  # 8) Stop progress tracking
  if [[ -n "$progress_pid" ]]; then
    stop_progress_tracking "$table_name"
  fi

  # 9) Handle load status
  if [[ $LOAD_STATUS -ne 0 ]]; then
    log "${RED}ERROR: Failed to load data into $DB_SCHEMA.$table_name${NC}"
    log "See log file for details: $LOG_FILE"
    # Add to summary with "failed" status
    SUMMARY_TABLE+=("$table_name|$existing_rows|0|$(( $(date +%s) - FILE_START_TIME ))|failed")
    continue
  fi

  # 10) Count rows and log
  final_row_count=$(psql -h "$DB_HOST" -p "$DB_PORT" -U "$DB_USER" -d "$DB_NAME" -t \
                   -c "SELECT COUNT(*) FROM \"$DB_SCHEMA\".\"$table_name\";" | tr -d '[:space:]')

  processing_time=$(( $(date +%s) - FILE_START_TIME ))
  rows_per_second=$(( total_csv_rows / (processing_time > 0 ? processing_time : 1) ))

  log "${GREEN}Loaded $final_row_count rows into $DB_SCHEMA.$table_name${NC} (in $(format_time $processing_time), approx. $rows_per_second rows/sec)"

  # Add to summary with appropriate status
  if [[ $existing_rows -gt 0 ]]; then
    SUMMARY_TABLE+=("$table_name|$existing_rows|$final_row_count|$processing_time|replaced")
  else
    SUMMARY_TABLE+=("$table_name|$existing_rows|$final_row_count|$processing_time|new")
  fi

  log "Finished loading $csv_file -> $DB_SCHEMA.$table_name"
done

# Calculate total duration
GLOBAL_END_TIME=$(date +%s)
TOTAL_DURATION=$(( GLOBAL_END_TIME - GLOBAL_START_TIME ))

# Show final progress for this stage
echo "progress: $total_files/$total_files Loading to Staging Complete" >&2

# Print summary table
log "${BOLD}=====================================================================${NC}"
log "${BOLD}                     LOADING SUMMARY                                 ${NC}"
log "${BOLD}=====================================================================${NC}"
log "Total time: ${BOLD}$(format_time $TOTAL_DURATION)${NC}"
log ""
log "| Table Name | Before | After | Time (min:sec) | Status |"
log "|------------|--------|-------|----------------|--------|"
for summary in "${SUMMARY_TABLE[@]}"; do
  IFS='|' read -r table before after duration status <<< "$summary"
  # Color-code status
  status_colored="$status"
  if [[ "$status" == "failed" ]]; then
    status_colored="${RED}$status${NC}"
  elif [[ "$status" == "new" ]] || [[ "$status" == "replaced" ]]; then
    status_colored="${GREEN}$status${NC}"
  elif [[ "$status" == "skipped" ]]; then
    status_colored="${YELLOW}$status${NC}"
  fi
  log "| $table | $before | $after | $(format_time $duration) | $status_colored |"
done

log "${BOLD}=====================================================================${NC}"
log "${GREEN}All CSV files have been processed!${NC}"
log "Data is stored in staging tables under schema: ${BLUE}$DB_SCHEMA${NC}"
log "All columns are TEXT. Transform/cast as needed."
log "Log file: ${BLUE}$LOG_FILE${NC}"

# Clean up temp files
rm -rf "$TEMP_DIR"/*.pid 2>/dev/null || true

# Exit with success
exit 0
