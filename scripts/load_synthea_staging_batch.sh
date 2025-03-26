#!/bin/bash
#
# Script to load Synthea CSV files into the PostgreSQL staging schema
# Handles table creation and data loading with progress tracking
# Supports overriding DB connection settings via environment variables
# Implements batch processing for large files

set -e  # Exit on any error

# Constants
RED='\033[0;31m'
GREEN='\033[0;32m'
BLUE='\033[0;34m'
YELLOW='\033[0;33m'
BOLD='\033[1m'
NC='\033[0m'  # No Color

# Default values
DEFAULT_DB_HOST="localhost"
DEFAULT_DB_PORT="5432"
DEFAULT_DB_NAME="ohdsi"
DEFAULT_DB_USER="postgres"
DEFAULT_DB_SCHEMA="staging"
DEFAULT_DATA_DIR="/home/acumenus/GitHub/Synthea2OMOP-ETL/synthea-processed"
DEFAULT_PROGRESS_INTERVAL="1"  # seconds

# Global variables
FORCE_OVERWRITE=false
SKIP_EXISTING=false
SHOW_PROGRESS_BAR=false
SUMMARY_TABLE=()
START_TIME=$(date +%s)
LOG_DIR="./logs"
TEMP_DIR="./tmp"
LOG_FILE="${LOG_DIR}/synthea_load_$(date +%Y%m%d_%H%M%S).log"

# Control flags
set +o noclobber  # Allow overwriting files

# Function to load environment variables
load_env() {
  local env_file=".env"
  echo "Loading configuration from $(pwd)/$env_file"
  
  if [[ -f "$env_file" ]]; then
    while IFS= read -r line || [[ -n "$line" ]]; do
      # Skip comments and empty lines
      [[ -z "$line" || "$line" =~ ^# ]] && continue
      
      # Extract variable name and value
      if [[ "$line" =~ ^([A-Za-z_][A-Za-z0-9_]*)=(.*)$ ]]; then
        name="${BASH_REMATCH[1]}"
        value="${BASH_REMATCH[2]}"
        
        # Remove quotes if present
        value="${value%\"}"
        value="${value#\"}"
        value="${value%\'}"
        value="${value#\'}"
        
        # Set environment variable
        export "$name"="$value"
      fi
    done < "$env_file"
  else
    echo "Warning: $env_file not found. Using default values."
  fi
}

# Function for logging
log() {
  local timestamp=$(date '+%Y-%m-%d %H:%M:%S')
  echo -e "$timestamp - $1" | tee -a "$LOG_FILE"
}

# Function to display script usage
display_usage() {
  cat << EOF
Usage: ${0##*/} [OPTIONS] [FILE.csv ...]

Load Synthea CSV files into PostgreSQL staging tables.

Options:
  --force                 Overwrite existing tables even if they have data
  --skip-existing         Skip loading if table exists with data
  --progress              Show progress bars during loading
  --update-interval NUM   Update progress every NUM seconds (default: $DEFAULT_PROGRESS_INTERVAL)
  -h, --help              Display this help message and exit

Environment variables:
  DB_HOST        Database host (default: $DEFAULT_DB_HOST)
  DB_PORT        Database port (default: $DEFAULT_DB_PORT)
  DB_NAME        Database name (default: $DEFAULT_DB_NAME)
  DB_USER        Database username (default: $DEFAULT_DB_USER)
  DB_PASSWORD    Database password
  DB_SCHEMA      Database schema (default: $DEFAULT_DB_SCHEMA)
  SYNTHEA_DATA_DIR Data directory (default: $DEFAULT_DATA_DIR)

Examples:
  ${0##*/} --force --progress                     # Load all CSV files
  ${0##*/} --force encounters.csv patients.csv    # Load specific CSV files
EOF
}

# Function to check if a table exists and has data
table_exists_with_data() {
  local schema="$1"
  local table="$2"
  
  # Check if table exists
  local exists=$(psql -h "$DB_HOST" -p "$DB_PORT" -U "$DB_USER" -d "$DB_NAME" -t -c "SELECT EXISTS (
    SELECT FROM information_schema.tables 
    WHERE table_schema = '$schema' AND table_name = '$table'
  );")
  
  exists=$(echo "$exists" | tr -d '[:space:]')
  
  if [[ "$exists" != "t" ]]; then
    return 1  # Table doesn't exist
  fi
  
  # Check if table has data
  local count=$(psql -h "$DB_HOST" -p "$DB_PORT" -U "$DB_USER" -d "$DB_NAME" -t -c "
    SELECT CASE WHEN COUNT(*) > 0 THEN 't' ELSE 'f' END 
    FROM \"$schema\".\"$table\" LIMIT 1;
  ")
  
  count=$(echo "$count" | tr -d '[:space:]')
  
  if [[ "$count" == "t" ]]; then
    return 0  # Table exists and has data
  else
    return 2  # Table exists but is empty
  fi
}

# Function to update progress
update_progress() {
  local current="$1"
  local total="$2"
  local table="$3"
  
  # Calculate percentage
  local pct=0
  if [[ $total -gt 0 ]]; then
    pct=$(( (current * 100) / total ))
  fi
  
  # Build progress message
  local progress_msg="Loading $table: $current/$total rows ($pct%)"
  
  # Output progress for monitoring
  echo "$progress_msg" > "$TEMP_DIR/${table}_progress.txt"
}

# Function to track loading progress in the background
track_loading_progress() {
  local csv_file="$1"
  local schema="$2"
  local table="$3"
  local total_rows="$4"
  
  # Start background progress tracking
  (
    while true; do
      # Get current count in the table
      current_count=$(psql -h "$DB_HOST" -p "$DB_PORT" -U "$DB_USER" -d "$DB_NAME" -t \
                     -c "SELECT COUNT(*) FROM \"$schema\".\"$table\";" | tr -d '[:space:]')
      
      # Update progress
      update_progress "$current_count" "$total_rows" "$table"
      
      # Check if we're done or if we should continue
      if [[ -f "$TEMP_DIR/${table}_stop_progress" ]]; then
        rm "$TEMP_DIR/${table}_stop_progress"
        break
      fi
      
      # Wait before next update
      sleep "$PROGRESS_UPDATE_INTERVAL"
    done
  ) &
  
  # Return the process ID
  echo $!
}

# Function to stop progress tracking
stop_progress_tracking() {
  local table="$1"
  
  # Signal to stop progress tracking
  touch "$TEMP_DIR/${table}_stop_progress"
  
  # Remove progress file
  rm -f "$TEMP_DIR/${table}_progress.txt"
}

# Function to format time
format_time() {
  local seconds=$1
  
  if [[ $seconds -lt 60 ]]; then
    echo "${seconds}s"
  elif [[ $seconds -lt 3600 ]]; then
    printf "%dm %ds" $(($seconds/60)) $(($seconds%60))
  else
    printf "%dh %dm %ds" $(($seconds/3600)) $(($seconds%60/60)) $(($seconds%60%60))
  fi
}

# Function to count CSV rows (excluding header)
count_csv_rows() {
  local file="$1"
  local count
  
  # Subtract 1 for the header
  count=$(( $(wc -l < "$file") - 1 ))
  
  echo "$count"
}

# Process command-line arguments
CSV_FILES_TO_PROCESS=()

while [[ $# -gt 0 ]]; do
  case "$1" in
    --force)
      FORCE_OVERWRITE=true
      ;;
    --skip-existing)
      SKIP_EXISTING=true
      ;;
    --progress)
      SHOW_PROGRESS_BAR=true
      ;;
    --update-interval)
      shift
      PROGRESS_UPDATE_INTERVAL="$1"
      ;;
    -h|--help)
      display_usage
      exit 0
      ;;
    *.csv)
      # Add CSV file to the list of files to process
      CSV_FILES_TO_PROCESS+=("$1")
      ;;
    *)
      if [[ $1 == --* ]]; then
        echo "Unknown option: $1"
        display_usage
        exit 1
      else
        # Check if it might be a CSV file without .csv extension
        if [[ -f "$SYNTHEA_DATA_DIR/$1.csv" ]]; then
          CSV_FILES_TO_PROCESS+=("$1.csv")
        else
          echo "Unknown argument or file not found: $1"
          display_usage
          exit 1
        fi
      fi
      ;;
  esac
  shift
done

# Create directories if they don't exist
mkdir -p "$LOG_DIR" "$TEMP_DIR"

# Load environment variables
load_env

# Set defaults if not provided
DB_HOST="${DB_HOST:-$DEFAULT_DB_HOST}"
DB_PORT="${DB_PORT:-$DEFAULT_DB_PORT}"
DB_NAME="${DB_NAME:-$DEFAULT_DB_NAME}"
DB_USER="${DB_USER:-$DEFAULT_DB_USER}"
DB_SCHEMA="${DB_SCHEMA:-$DEFAULT_DB_SCHEMA}"
SYNTHEA_DATA_DIR="${SYNTHEA_DATA_DIR:-$DEFAULT_DATA_DIR}"
PROGRESS_UPDATE_INTERVAL="${PROGRESS_UPDATE_INTERVAL:-$DEFAULT_PROGRESS_INTERVAL}"

# Handle relative vs absolute paths for SYNTHEA_DATA_DIR
if [[ ! "$SYNTHEA_DATA_DIR" = /* ]]; then
  # Convert relative path to absolute
  SYNTHEA_DATA_DIR="$(readlink -f "$SYNTHEA_DATA_DIR")"
  log "Converting to absolute path: $SYNTHEA_DATA_DIR"
fi

# Log configuration
log "Starting Synthea data loading process with the following configuration:"
log "  Database: $DB_NAME on $DB_HOST:$DB_PORT (schema: $DB_SCHEMA)"
log "  Data directory: $SYNTHEA_DATA_DIR"
log "  Overwrite mode: $(if [[ "$FORCE_OVERWRITE" == "true" ]]; then echo "force"; elif [[ "$SKIP_EXISTING" == "true" ]]; then echo "skip"; else echo "ask"; fi)"
log "  Progress bar: $SHOW_PROGRESS_BAR (update interval: ${PROGRESS_UPDATE_INTERVAL}s)"
log "Logging to: $LOG_FILE"

# Change to data directory
if [[ ! -d "$SYNTHEA_DATA_DIR" ]]; then
  log "${RED}ERROR: Data directory $SYNTHEA_DATA_DIR does not exist${NC}"
  exit 1
fi

cd "$SYNTHEA_DATA_DIR"
log "Successfully changed to directory: $SYNTHEA_DATA_DIR"
log "Directory contains: $(ls -la | wc -l) files"

# Create schema if it doesn't exist
log "Ensuring schema $DB_SCHEMA exists..."
psql -h "$DB_HOST" -p "$DB_PORT" -U "$DB_USER" -d "$DB_NAME" -c "CREATE SCHEMA IF NOT EXISTS \"$DB_SCHEMA\";" 2>&1 | tee -a "$LOG_FILE"

# Get a list of CSV files to process
if [[ ${#CSV_FILES_TO_PROCESS[@]} -eq 0 ]]; then
  # No specific files provided, process all CSV files
  csv_files=(*.csv)
  log "No specific files requested, found ${#csv_files[@]} CSV files to process"
  
  if [[ ${#csv_files[@]} -eq 0 ]]; then
    log "${RED}ERROR: No CSV files found in $SYNTHEA_DATA_DIR${NC}"
    exit 1
  fi
else
  # Process only the specified files
  csv_files=("${CSV_FILES_TO_PROCESS[@]}")
  log "Processing ${#csv_files[@]} specific CSV files: ${csv_files[*]}"
  
  # Verify all requested files exist
  for file in "${csv_files[@]}"; do
    if [[ ! -f "$file" ]]; then
      log "${RED}ERROR: Requested file $file not found in $SYNTHEA_DATA_DIR${NC}"
      exit 1
    fi
  done
fi

# Start processing
for csv_file in "${csv_files[@]}"; do
  FILE_START_TIME=$(date +%s)
  
  # Skip if not a file
  if [[ ! -f "$csv_file" ]]; then
    continue
  fi
  
  # Extract table name from filename (remove .csv extension)
  table_name="${csv_file%.csv}"
  
  log "Processing $csv_file => $DB_SCHEMA.$table_name"
  
  # Check if table exists and has data
  if table_exists_with_data "$DB_SCHEMA" "$table_name"; then
    existing_status=$?
    existing_rows=$(psql -h "$DB_HOST" -p "$DB_PORT" -U "$DB_USER" -d "$DB_NAME" -t \
                   -c "SELECT COUNT(*) FROM \"$DB_SCHEMA\".\"$table_name\";" | tr -d '[:space:]')
    
    if [[ $existing_status -eq 0 ]]; then
      # Table exists and has data
      if [[ "$FORCE_OVERWRITE" == "true" ]]; then
        log "${YELLOW}Table $DB_SCHEMA.$table_name exists with $existing_rows rows. Dropping and recreating.${NC}"
        psql -h "$DB_HOST" -p "$DB_PORT" -U "$DB_USER" -d "$DB_NAME" \
             -c "DROP TABLE \"$DB_SCHEMA\".\"$table_name\";" 2>&1 | tee -a "$LOG_FILE"
      elif [[ "$SKIP_EXISTING" == "true" ]]; then
        log "${BLUE}Skipping $DB_SCHEMA.$table_name (already exists with $existing_rows rows)${NC}"
        # Add to summary with "skipped" status
        SUMMARY_TABLE+=("$table_name|$existing_rows|0|0|skipped")
        continue
      else
        log "${YELLOW}Table $DB_SCHEMA.$table_name exists with $existing_rows rows.${NC}"
        read -p "Do you want to drop and recreate it? [y/N] " -n 1 -r
        echo
        
        if [[ ! $REPLY =~ ^[Yy]$ ]]; then
          log "${BLUE}Skipping $DB_SCHEMA.$table_name${NC}"
          # Add to summary with "skipped" status
          SUMMARY_TABLE+=("$table_name|$existing_rows|0|0|skipped")
          continue
        fi
        
        log "Dropping table $DB_SCHEMA.$table_name"
        psql -h "$DB_HOST" -p "$DB_PORT" -U "$DB_USER" -d "$DB_NAME" \
             -c "DROP TABLE \"$DB_SCHEMA\".\"$table_name\";" 2>&1 | tee -a "$LOG_FILE"
      fi
    elif [[ $existing_status -eq 2 ]]; then
      # Table exists but is empty
      log "${BLUE}Table $DB_SCHEMA.$table_name exists but is empty. Will load data.${NC}"
    fi
  else
    existing_rows=0
  fi
  
  # Count total rows in CSV file
  total_csv_rows=$(count_csv_rows "$csv_file")
  
  # Extract the header from the CSV to create a table
  header=$(head -n 1 "$csv_file")
  IFS=',' read -ra columns <<< "$header"
  
  # Construct SQL for table creation
  create_sql="CREATE TABLE IF NOT EXISTS \"$DB_SCHEMA\".\"$table_name\" ("
  for i in "${!columns[@]}"; do
    col_name="${columns[$i]}"
    col_name="${col_name//[$'\t\r\n ']}"  # Remove any whitespace
    col_name="${col_name//\"}"            # Remove quotes
    
    if [[ $i -gt 0 ]]; then
      create_sql+=", "
    fi
    
    create_sql+="\"$col_name\" TEXT"
  done
  create_sql+=");"
  
  # Create the table
  log "Creating table $DB_SCHEMA.$table_name with ${#columns[@]} columns..."
  psql -h "$DB_HOST" -p "$DB_PORT" -U "$DB_USER" -d "$DB_NAME" \
       -c "$create_sql" 2>&1 | tee -a "$LOG_FILE"
  
  # Start progress tracking in the background
  log "Loading data into ${BLUE}$DB_SCHEMA.$table_name${NC} (${BOLD}$total_csv_rows${NC} rows)..."
  
  # Start the progress tracker if enabled
  progress_pid=""
  if [[ "$SHOW_PROGRESS_BAR" == "true" ]]; then
    progress_pid=$(track_loading_progress "$csv_file" "$DB_SCHEMA" "$table_name" "$total_csv_rows")
  fi
  
  # Load the data using batch processing for large files
  set +e  # Temporarily disable exit on error
  
  # File size threshold for batch processing (100MB)
  BATCH_THRESHOLD=$((100 * 1024 * 1024))
  FILE_SIZE=$(stat -c%s "$csv_file")
  
  if [[ $FILE_SIZE -gt $BATCH_THRESHOLD ]]; then
    log "Large file detected ($FILE_SIZE bytes) - using batch processing"
    
    # Create a temporary directory for batches
    BATCH_DIR="$TEMP_DIR/${table_name}_batches"
    mkdir -p "$BATCH_DIR"
    
    # Split the file into batches (preserving header)
    HEADER_FILE="$BATCH_DIR/header.csv"
    head -n1 "$csv_file" > "$HEADER_FILE"
    
    # Calculate approximate number of lines per batch (aiming for ~50MB per batch)
    LINES_PER_MB=$(wc -l < "$csv_file" | awk -v size="$FILE_SIZE" '{printf "%.0f", $1 / (size/1024/1024)}')
    BATCH_SIZE=$((LINES_PER_MB * 50))
    
    log "Splitting file into batches (approximately $BATCH_SIZE lines per batch)"
    tail -n +2 "$csv_file" | split -l "$BATCH_SIZE" - "$BATCH_DIR/batch_"
    
    BATCH_COUNT=$(find "$BATCH_DIR" -name "batch_*" | wc -l)
    log "Created $BATCH_COUNT batches for processing"
    
    # Process each batch
    BATCH_NUM=0
    BATCH_FAILURES=0
    
    for BATCH_FILE in "$BATCH_DIR"/batch_*; do
      ((BATCH_NUM++))
      TEMP_BATCH_FILE="${BATCH_FILE}.csv"
      
      # Add header to batch file
      cat "$HEADER_FILE" "$BATCH_FILE" > "$TEMP_BATCH_FILE"
      
      # Load this batch
      log "Loading batch $BATCH_NUM/$BATCH_COUNT"
      if ! psql -h "$DB_HOST" -p "$DB_PORT" -U "$DB_USER" -d "$DB_NAME" \
           -c "\copy \"$DB_SCHEMA\".\"$table_name\" FROM '${TEMP_BATCH_FILE}' CSV HEADER DELIMITER ',' QUOTE '\"' ESCAPE '\\';" 2>&1 | tee -a "$LOG_FILE"; then
        log "${RED}ERROR: Failed to load batch $BATCH_NUM${NC}"
        ((BATCH_FAILURES++))
      else
        # Commit after each batch to prevent transaction log buildup
        psql -h "$DB_HOST" -p "$DB_PORT" -U "$DB_USER" -d "$DB_NAME" -c "COMMIT;" 2>&1 | tee -a "$LOG_FILE"
      fi
      
      # Update progress after each batch
      progress_pct=$((BATCH_NUM * 100 / BATCH_COUNT))
      echo "progress: $progress_pct% Loading $table_name (batch $BATCH_NUM/$BATCH_COUNT)" >&2
      
      # Clean up temp batch file
      rm "$TEMP_BATCH_FILE"
    done
    
    # Clean up batch directory
    rm -rf "$BATCH_DIR"
    
    if [[ $BATCH_FAILURES -gt 0 ]]; then
      log "${RED}ERROR: $BATCH_FAILURES batch failures occurred while loading $table_name${NC}"
      LOAD_STATUS=1
    else
      LOAD_STATUS=0
    fi
  else
    # Standard processing for smaller files
    log "Standard loading for smaller file ($FILE_SIZE bytes)"
    psql -h "$DB_HOST" -p "$DB_PORT" -U "$DB_USER" -d "$DB_NAME" \
         -c "\copy \"$DB_SCHEMA\".\"$table_name\" FROM '${csv_file}' CSV HEADER DELIMITER ',' QUOTE '\"' ESCAPE '\\';" 2>&1 | tee -a "$LOG_FILE"
    LOAD_STATUS=$?
  fi
  
  set -e  # Re-enable exit on error
  
  # Stop progress tracking
  if [[ -n "$progress_pid" ]]; then
    stop_progress_tracking "$table_name"
  fi
  
  # Handle load status
  if [[ $LOAD_STATUS -ne 0 ]]; then
    log "${RED}ERROR: Failed to load data into $DB_SCHEMA.$table_name${NC}"
    log "See log file for details: $LOG_FILE"
    # Add to summary with "failed" status
    SUMMARY_TABLE+=("$table_name|$existing_rows|0|$(( $(date +%s) - FILE_START_TIME ))|failed")
    continue
  fi
  
  # Count rows and log
  final_row_count=$(psql -h "$DB_HOST" -p "$DB_PORT" -U "$DB_USER" -d "$DB_NAME" -t \
                   -c "SELECT COUNT(*) FROM \"$DB_SCHEMA\".\"$table_name\";" | tr -d '[:space:]')
  
  processing_time=$(( $(date +%s) - FILE_START_TIME ))
  rows_per_second=$(( total_csv_rows / (processing_time > 0 ? processing_time : 1) ))
  
  log "${GREEN}Loaded $final_row_count rows into $DB_SCHEMA.$table_name${NC} (in $(format_time $processing_time), approx. $rows_per_second rows/sec)"
  
  # Add to summary
  SUMMARY_TABLE+=("$table_name|$existing_rows|$final_row_count|$processing_time|success")
done

# Print summary
TOTAL_TIME=$(( $(date +%s) - START_TIME ))
log "=========================================="
log "Load completed in $(format_time $TOTAL_TIME)"
log "Summary of loaded tables:"
log "=========================================="
log "Table|Initial Rows|Final Rows|Processing Time|Status"
log "----------------------------------------"

for row in "${SUMMARY_TABLE[@]}"; do
  IFS='|' read -ra ROW_DATA <<< "$row"
  table_name="${ROW_DATA[0]}"
  initial_rows="${ROW_DATA[1]}"
  final_rows="${ROW_DATA[2]}"
  proc_time="${ROW_DATA[3]}"
  status="${ROW_DATA[4]}"
  
  # Format the time
  if [[ $proc_time -gt 0 ]]; then
    formatted_time=$(format_time $proc_time)
  else
    formatted_time="-"
  fi
  
  if [[ "$status" == "success" ]]; then
    status_color="${GREEN}"
  elif [[ "$status" == "skipped" ]]; then
    status_color="${BLUE}"
  else
    status_color="${RED}"
  fi
  
  log "$table_name|$initial_rows|$final_rows|$formatted_time|${status_color}$status${NC}"
done

log "=========================================="
log "Log saved to: $LOG_FILE"

exit 0
