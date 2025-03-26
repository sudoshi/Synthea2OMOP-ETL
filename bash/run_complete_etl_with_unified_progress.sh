#!/usr/bin/env bash
#
# run_complete_etl_with_unified_progress.sh
#
# A comprehensive ETL pipeline for processing Synthea data into OMOP CDM
# with real-time progress bars, checkpointing, and a unified user experience.
#
# Features:
# - Modern progress bars for all ETL steps
# - Checkpoint tracking to resume from interruptions
# - Consistent visual appearance across all stages
# - Detail progress reporting for tables and row counts
# - Colorized terminal output
# - Graceful error handling and recovery

set -euo pipefail

##############################################################################
# 1) CONFIGURATION AND PARAMETERS
##############################################################################
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
CHECKPOINT_FILE="$PROJECT_ROOT/.synthea_etl_checkpoint.json"
FORCE_RESTART=false
DISABLE_PROGRESS_BARS=false

# ANSI colors for enhanced output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[0;33m'
BLUE='\033[0;34m'
BOLD='\033[1m'
NC='\033[0m' # No Color

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
    --checkpoint-file)
      CHECKPOINT_FILE="$2"
      shift 2
      ;;
    --force-restart)
      FORCE_RESTART=true
      shift
      ;;
    --no-progress-bars)
      DISABLE_PROGRESS_BARS=true
      shift
      ;;
    -h|--help)
      echo "Usage: run_complete_etl_with_unified_progress.sh [OPTIONS]"
      echo "Options:"
      echo "  --input-dir DIR           Directory with Synthea output files (default: ./synthea-output)"
      echo "  --processed-dir DIR       Directory for processed files (default: ./synthea-processed)"
      echo "  --overwrite-processed     Overwrite existing preprocessed files"
      echo "  --debug                   Enable debug logging"
      echo "  --force-load              Force overwrite of existing database tables"
      echo "  --max-workers N           Maximum number of parallel workers (default: 4)"
      echo "  --checkpoint-file FILE    Path to checkpoint file (default: .synthea_etl_checkpoint.json)"
      echo "  --force-restart           Ignore checkpoints and start from beginning"
      echo "  --no-progress-bars        Disable progress bar display"
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
LOAD_SCRIPT="$PROJECT_ROOT/scripts/load_synthea_staging_batch.sh"
TYPING_SQL="$PROJECT_ROOT/sql/synthea_typing/instrumented_typing.sql"
OMOP_ETL_SQL="$PROJECT_ROOT/sql/etl/run_all_etl.sql"

# Load environment variables from .env file if it exists
if [ -f "$PROJECT_ROOT/.env" ]; then
    echo -e "${BLUE}Loading configuration from $PROJECT_ROOT/.env${NC}"
    set -a  # automatically export all variables
    source "$PROJECT_ROOT/.env"
    set +a
else
    echo -e "${YELLOW}Warning: .env file not found in $PROJECT_ROOT${NC}"
    echo -e "Using default configuration values"
fi

# Database connection settings from .env or defaults
DB_HOST="${DB_HOST:-localhost}"
DB_PORT="${DB_PORT:-5432}"
DB_NAME="${DB_NAME:-ohdsi}"
DB_USER="${DB_USER:-postgres}"
DB_PASSWORD="${DB_PASSWORD:-acumenus}"

# Export password for psql
export PGPASSWORD="$DB_PASSWORD"
PSQL_CMD="psql -h $DB_HOST -p $DB_PORT -U $DB_USER -d $DB_NAME"

# Create a log directory if it doesn't exist
LOG_DIR="$PROJECT_ROOT/logs"
mkdir -p "$LOG_DIR"
LOG_FILE="$LOG_DIR/etl_run_$(date +%Y%m%d_%H%M%S).log"

# Create a temp directory for progress tracking
TEMP_DIR="$PROJECT_ROOT/tmp"
mkdir -p "$TEMP_DIR"

# Check terminal capabilities
if [[ -t 1 ]] && [[ "$DISABLE_PROGRESS_BARS" == "false" ]]; then
  # Terminal is interactive, so we can display progress bars
  CAN_DISPLAY_PROGRESS=true
  # Get terminal width
  if command -v tput >/dev/null 2>&1; then
    TERM_WIDTH=$(tput cols)
  else
    TERM_WIDTH=80
  fi
  # Default progress bar width (adjust based on terminal width)
  PROGRESS_WIDTH=$((TERM_WIDTH > 80 ? 50 : 30))
else
  # No interactive terminal, disable progress bars
  CAN_DISPLAY_PROGRESS=false
  # Remove color codes
  RED=''
  GREEN=''
  YELLOW=''
  BLUE=''
  BOLD=''
  NC=''
fi

##############################################################################
# 2) HELPER FUNCTIONS
##############################################################################

# Function to log messages to both console and log file
log() {
    echo -e "$(date +"%Y-%m-%d %H:%M:%S") - $1" | tee -a "$LOG_FILE"
}

# Function to handle errors and cleanup
handle_error() {
    local stage="$1"
    local exit_code="$2"
    log "${RED}ERROR: $stage failed with exit code $exit_code${NC}"
    log "Check logs for details. Exiting."
    
    # Clean up any temporary files
    rm -f "$TEMP_DIR"/*.log 2>/dev/null || true
    
    exit $exit_code
}

# Trap for interrupts and errors
trap_handler() {
    local exit_code=$?
    log "${RED}Process interrupted or error occurred.${NC}"
    log "You can restart the pipeline later to continue from the last successful step."
    # Clean up temporary files
    rm -f "$TEMP_DIR"/*.log 2>/dev/null || true
    exit $exit_code
}

# Setup error trap 
trap 'trap_handler' INT TERM ERR

# Initialize or load checkpoint file
init_or_load_checkpoint() {
  if [ "$FORCE_RESTART" = true ]; then
    log "${YELLOW}Forcing restart: Initializing new checkpoint file${NC}"
    echo '{
  "completed_steps": [],
  "last_updated": "'$(date -Iseconds)'",
  "stats": {}
}' > "$CHECKPOINT_FILE"
    return
  fi

  if [ ! -f "$CHECKPOINT_FILE" ]; then
    log "Checkpoint file not found. Creating new one."
    echo '{
  "completed_steps": [],
  "last_updated": "'$(date -Iseconds)'",
  "stats": {}
}' > "$CHECKPOINT_FILE"
  else
    log "Found checkpoint file at: ${BLUE}$CHECKPOINT_FILE${NC}"
    log "Resuming from last checkpoint..."
    
    # Display completed steps
    if [ "$DEBUG" = true ]; then
      log "Completed steps:"
      grep -o '"completed_steps": \[[^]]*\]' "$CHECKPOINT_FILE" | sed 's/"completed_steps": \[\|\]//g' | tr ',' '\n' | sed 's/"//g' | sed 's/^/  - /'
    fi
  fi
}

# Check if a step is completed in the checkpoint file
is_step_completed() {
  local step="$1"
  if [ ! -f "$CHECKPOINT_FILE" ]; then
    return 1
  fi
  
  grep -q "\"$step\"" "$CHECKPOINT_FILE"
  return $?
}

# Mark a step as completed in the checkpoint file
mark_step_completed() {
  local step="$1"
  local duration="$2"
  
  if ! is_step_completed "$step"; then
    # Add step to completed_steps array
    # This is a bit hacky but works for our simple JSON format
    sed -i 's/"completed_steps": \[/"completed_steps": \["'$step'"/' "$CHECKPOINT_FILE"
    if [ "$(grep -o '"completed_steps": \[[^]]*\]' "$CHECKPOINT_FILE" | grep -v '"\]' | wc -l)" -gt 0 ]; then
      # If there are already items, add a comma
      sed -i 's/"'$step'"/"'$step'",/' "$CHECKPOINT_FILE"
    fi
    
    # Update last_updated timestamp
    sed -i 's/"last_updated": "[^"]*"/"last_updated": "'$(date -Iseconds)'"/' "$CHECKPOINT_FILE"
    
    # Add stats for this step
    # First, let's extract the current stats object
    local stats=$(grep -o '"stats": {[^}]*}' "$CHECKPOINT_FILE" | sed 's/"stats": {//' | sed 's/}//')
    
    # Check if stats is empty
    if [ -z "$stats" ]; then
      # If empty, add the first stat
      sed -i 's/"stats": {/"stats": {"'$step'": {"duration_seconds": '$duration'}}/' "$CHECKPOINT_FILE"
    else
      # If not empty, add the stat with a comma
      sed -i 's/"stats": {/"stats": {"'$step'": {"duration_seconds": '$duration'}, /' "$CHECKPOINT_FILE"
    fi
  fi
}

# Function to display a unified progress bar
display_progress_bar() {
  local current=$1
  local total=$2
  local title=$3
  local status_message=${4:-""}
  
  if [ "$CAN_DISPLAY_PROGRESS" = false ]; then
    return
  fi

  # Calculate percentage (safely handling division by zero)
  if [ "$total" -gt 0 ]; then
    local percentage=$(( current * 100 / total ))
  else
    local percentage=0
  fi
  
  # Calculate number of filled blocks
  local filled_blocks=$(( percentage * PROGRESS_WIDTH / 100 ))
  
  # Create the progress bar display
  local progress_bar="["
  for ((i=0; i<filled_blocks; i++)); do
    progress_bar+="█"
  done
  for ((i=filled_blocks; i<PROGRESS_WIDTH; i++)); do
    progress_bar+="░"
  done
  progress_bar+="]"
  
  # Format the title to a fixed width
  local title_formatted=$(printf "%-20s" "$title")
  
  # Clear line and display progress
  printf "\r\033[K${BOLD}%s${NC} %s %3d%% %s" "$title_formatted" "$progress_bar" "$percentage" "$status_message"
  
  # If we've reached 100%, add a newline
  if [ "$percentage" -eq 100 ]; then
    echo ""
  fi
}

# Function to monitor output for progress markers
monitor_process_output() {
  local cmd="$1"
  local title="$2"
  local total=${3:-100}
  local temp_file="$TEMP_DIR/${title// /_}_progress.log"
  
  # Run the command and redirect output to our temp file
  log "Executing: $cmd"
  eval "$cmd" > "$temp_file" 2>&1 &
  local pid=$!
  
  # Initial progress
  display_progress_bar 0 $total "$title" "Starting..."
  
  # Monitor the output file for progress indicators
  local current=0
  local last_update=0
  local last_line_count=0
  
  while kill -0 $pid 2>/dev/null; do
    # Check for progress lines in format: progress: N/M message
    if grep -q "progress:" "$temp_file" 2>/dev/null; then
      # Extract the latest progress line
      local progress_line=$(grep "progress:" "$temp_file" | tail -n 1)
      
      # Extract current/total and message using regex
      if [[ $progress_line =~ progress:[[:space:]]*([0-9]+)/([0-9]+)[[:space:]]*(.*) ]]; then
        current=${BASH_REMATCH[1]}
        # Use the total from the output if available
        if [[ -n "${BASH_REMATCH[2]}" ]] && [[ "${BASH_REMATCH[2]}" != "0" ]]; then
          total=${BASH_REMATCH[2]} 
        fi
        local message=${BASH_REMATCH[3]}
        display_progress_bar $current $total "$title" "$message"
        last_update=$(date +%s)
      elif [[ $progress_line =~ progress:[[:space:]]*([0-9]+)%[[:space:]]*(.*) ]]; then
        # Alternative percentage format
        local percentage=${BASH_REMATCH[1]}
        current=$(( percentage * total / 100 ))
        local message=${BASH_REMATCH[2]}
        display_progress_bar $current $total "$title" "$message" 
        last_update=$(date +%s)
      fi
    else
      # If no progress markers, check if file is growing
      if [ -f "$temp_file" ]; then
        local line_count=$(wc -l < "$temp_file")
        if [ "$line_count" -gt "$last_line_count" ]; then
          # File is growing, show activity
          last_line_count=$line_count
          current=$((current + 1))
          if [ $current -ge $total ]; then
            current=$(( total / 2 ))  # Reset to avoid hitting 100%
          fi
          display_progress_bar $current $total "$title" "Processing... ($line_count lines of output)"
          last_update=$(date +%s)
        fi
      fi
    fi
    
    # If no updates for a while, show spinner
    current_time=$(date +%s)
    if (( current_time - last_update > 5 )); then
      local spinner_chars=('⠋' '⠙' '⠹' '⠸' '⠼' '⠴' '⠦' '⠧' '⠇' '⠏')
      local spinner_idx=$(( (current_time % 10) ))
      display_progress_bar $current $total "$title" "Working... ${spinner_chars[$spinner_idx]}"
    fi
    
    sleep 0.2
  done
  
  # Process finished, wait for it and check exit status
  wait $pid
  local status=$?
  
  if [ $status -eq 0 ]; then
    # Success, show 100% completion
    display_progress_bar $total $total "$title" "✓ Complete"
  else
    # Error, show error status
    display_progress_bar $current $total "$title" "✗ Failed (status $status)"
    # Display error details from the log file
    if [ -f "$temp_file" ]; then
      log "${RED}Error output from $title:${NC}"
      tail -n 20 "$temp_file" | while IFS= read -r line; do
        log "  $line"
      done
    fi
    
    # Don't remove the temp file so we can inspect it
    return $status
  fi
  
  # Return process exit status
  return $status
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

# Print header
echo -e "${BOLD}=====================================================================${NC}"
echo -e "${BOLD}         COMPLETE SYNTHEA TO OMOP ETL PIPELINE WITH PROGRESS         ${NC}"
echo -e "${BOLD}=====================================================================${NC}"
echo -e "Configuration:"
echo -e "  Input directory:       ${BLUE}$INPUT_DIR${NC}"
echo -e "  Processed directory:   ${BLUE}$PROCESSED_DIR${NC}"
echo -e "  Database:              ${BLUE}$DB_NAME${NC} on ${BLUE}$DB_HOST:$DB_PORT${NC}"
echo -e "  Checkpoint file:       ${BLUE}$CHECKPOINT_FILE${NC}"
echo -e "  Overwrite processed:   ${YELLOW}$OVERWRITE_PROCESSED${NC}"
echo -e "  Debug mode:            ${YELLOW}$DEBUG${NC}"
echo -e "  Force restart:         ${YELLOW}$FORCE_RESTART${NC}"
echo -e "  Force DB load:         ${YELLOW}$FORCE_LOAD${NC}"
echo -e "  Max workers:           ${BLUE}$MAX_WORKERS${NC}"
echo -e "  Progress bars:         ${YELLOW}$([[ "$CAN_DISPLAY_PROGRESS" == "true" ]] && echo "Enabled" || echo "Disabled")${NC}"
echo -e "${BOLD}=====================================================================${NC}"

# Initialize/load checkpoint
init_or_load_checkpoint

# Ensure the scripts exist
if [ ! -d "$INPUT_DIR" ]; then
  log "${RED}ERROR: Input directory not found: $INPUT_DIR${NC}"
  exit 1
fi

if [ ! -f "$PREPROCESS_SCRIPT" ]; then
  log "${RED}ERROR: Preprocess script not found: $PREPROCESS_SCRIPT${NC}"
  exit 1
fi

if [ ! -f "$LOAD_SCRIPT" ]; then
  log "${RED}ERROR: Load script not found: $LOAD_SCRIPT${NC}"
  exit 1
fi

# 1. Preprocess Synthea CSV files
if is_step_completed "preprocessing" && [ "$FORCE_RESTART" = false ]; then
  log "${BOLD}STEP 1: [ALREADY COMPLETED]${NC} Preprocessing Synthea CSV files"
  echo ""
else
  log "${BOLD}STEP 1: Preprocessing Synthea CSV files${NC}"
  log "${BLUE}-----------------------------------------------------------------------${NC}"

  PREPROCESS_CMD="$PYTHON $PREPROCESS_SCRIPT --input-dir $INPUT_DIR --output-dir $PROCESSED_DIR"

  if [ "$OVERWRITE_PROCESSED" = true ]; then
    PREPROCESS_CMD="$PREPROCESS_CMD --overwrite"
  fi

  if [ "$DEBUG" = true ]; then
    PREPROCESS_CMD="$PREPROCESS_CMD --debug"
  fi
  
  if [ "$DISABLE_PROGRESS_BARS" = true ]; then
    PREPROCESS_CMD="$PREPROCESS_CMD --no-progress-bar"
  fi

  log "Running: ${YELLOW}$PREPROCESS_CMD${NC}"
  START_TIME=$(date +%s)

  # Execute preprocessing with progress monitoring
  monitor_process_output "$PREPROCESS_CMD" "Preprocessing" 100
  preprocess_status=$?
  
  # Special case for preprocessing: If files are skipped because they already exist, consider it a success
  if [ $preprocess_status -ne 0 ] && grep -q "Skipping .* (already exists)" "$TEMP_DIR/Preprocessing_progress.log"; then
    skipped_count=$(grep -c "Skipping .* (already exists)" "$TEMP_DIR/Preprocessing_progress.log" || echo 0)
    log "${BLUE}Files already processed: Skipped $skipped_count files${NC}"
    display_progress_bar 100 100 "Preprocessing" "✓ Complete (using existing files)"
  else
    # Regular error handling
    if [ $preprocess_status -ne 0 ]; then
      handle_error "Preprocessing" $preprocess_status
    fi
  fi

  # We've already handled preprocessing errors above, so this section is no longer needed

  END_TIME=$(date +%s)
  DURATION=$((END_TIME - START_TIME))

  log "${GREEN}Preprocessing completed in $(format_time $DURATION)${NC}"
  
  # Mark step as completed
  mark_step_completed "preprocessing" "$DURATION"
  
  echo ""
fi

# 2. Load processed data into population schema
if is_step_completed "staging_load" && [ "$FORCE_RESTART" = false ]; then
  log "${BOLD}STEP 2: [ALREADY COMPLETED]${NC} Loading processed data into population schema"
  echo ""
else
  log "${BOLD}STEP 2: Loading processed data into population schema with efficient batch processing${NC}"
  log "${BLUE}-----------------------------------------------------------------------${NC}"

  # Ensure processed directory exists
  if [ ! -d "$PROCESSED_DIR" ]; then
    log "${RED}ERROR: Processed directory not found: $PROCESSED_DIR${NC}"
    log "Did preprocessing step complete successfully?"
    exit 1
  fi

  # Pass progress bar option through to load script with explicit path setting
  # Use the new efficient population loader script for better batch processing and progress tracking
  PROCESSED_DIR_ABS="$(readlink -f "$PROCESSED_DIR")"
  LOAD_CMD="cd $PROJECT_ROOT && DB_PASSWORD=$DB_PASSWORD SYNTHEA_DATA_DIR=\"$PROCESSED_DIR_ABS\" DB_SCHEMA=population ./scripts/load_population_efficient.sh"
  if [ "$FORCE_LOAD" = true ]; then
    LOAD_CMD="$LOAD_CMD --force"
  fi
  
  if [ "$DISABLE_PROGRESS_BARS" = true ]; then
    LOAD_CMD="$LOAD_CMD --no-progress-bar"
  fi

  log "Running: ${YELLOW}$LOAD_CMD${NC}"
  START_TIME=$(date +%s)

  # Execute database load with progress monitoring
  # The load_population_efficient.sh script has built-in progress tracking
  if ! monitor_process_output "$LOAD_CMD" "Loading to Population" 100; then
    handle_error "Database loading" $?
  fi

  # Check for reported failures in the output
  if grep -q "ERROR:" "$TEMP_DIR/Loading_to_Population_progress.log"; then
    log "${RED}ERROR: Database loading reported errors despite returning a success code${NC}"
    handle_error "Database loading" 1
  fi

  END_TIME=$(date +%s)
  DURATION=$((END_TIME - START_TIME))

  log "${GREEN}Database loading completed in $(format_time $DURATION)${NC}"
  
  # Mark step as completed
  mark_step_completed "staging_load" "$DURATION"
  
  echo ""
fi

# 3. Transform staging TEXT data to properly typed tables
if is_step_completed "typing_transform" && [ "$FORCE_RESTART" = false ]; then
  log "${BOLD}STEP 3: [ALREADY COMPLETED]${NC} Transforming staging data to properly typed tables"
  echo ""
else
  log "${BOLD}STEP 3: Transforming staging data to properly typed tables${NC}"
  log "${BLUE}-----------------------------------------------------------------------${NC}"

  # Check if instrumented SQL exists, if not fall back to regular SQL
  if [ ! -f "$TYPING_SQL" ]; then
    log "${YELLOW}WARNING: Instrumented typing SQL not found: $TYPING_SQL${NC}"
    log "Falling back to standard typing SQL"
    TYPING_SQL="$PROJECT_ROOT/sql/synthea_typing/synthea-typedtables-transformation.sql"
    
    if [ ! -f "$TYPING_SQL" ]; then
      log "${RED}ERROR: Typing SQL file not found: $TYPING_SQL${NC}"
      exit 1
    fi
  fi

  log "Running SQL: ${YELLOW}$TYPING_SQL${NC}"
  START_TIME=$(date +%s)

  # Execute typing transformation with progress monitoring
  SQL_CMD="$PSQL_CMD -f $TYPING_SQL"
  if ! monitor_process_output "$SQL_CMD" "Typing Transform" 19; then  # 19 tables total
    handle_error "Type transformation" $?
  fi

  END_TIME=$(date +%s)
  DURATION=$((END_TIME - START_TIME))

  log "${GREEN}Type transformation completed in $(format_time $DURATION)${NC}"
  
  # Mark step as completed
  mark_step_completed "typing_transform" "$DURATION"
  
  echo ""
fi

# 3.5. Transfer data from population schema to staging schema
if is_step_completed "population_to_staging" && [ "$FORCE_RESTART" = false ]; then
  log "${BOLD}STEP 3.5: [ALREADY COMPLETED]${NC} Transferring data from population schema to staging schema"
  echo ""
else
  log "${BOLD}STEP 3.5: Transferring data from population schema to staging schema${NC}"
  log "${BLUE}-----------------------------------------------------------------------${NC}"
  
  # Check if transfer script exists
  TRANSFER_SCRIPT="$PROJECT_ROOT/scripts/transfer_population_to_staging.sh"
  if [ ! -f "$TRANSFER_SCRIPT" ]; then
    log "${RED}ERROR: Transfer script not found: $TRANSFER_SCRIPT${NC}"
    exit 1
  fi
  
  log "Running: ${YELLOW}$TRANSFER_SCRIPT${NC}"
  START_TIME=$(date +%s)
  
  # Execute transfer script with progress monitoring
  TRANSFER_CMD="$TRANSFER_SCRIPT --host $DB_HOST --port $DB_PORT --dbname $DB_NAME --user $DB_USER --password $DB_PASSWORD"
  if ! monitor_process_output "$TRANSFER_CMD" "Population_to_Staging" 19; then # 19 tables total
    handle_error "Population to staging transfer" $?
  fi
  
  END_TIME=$(date +%s)
  DURATION=$((END_TIME - START_TIME))
  
  log "${GREEN}Population to staging transfer completed in $(format_time $DURATION)${NC}"
  
  # Mark step as completed
  mark_step_completed "population_to_staging" "$DURATION"
  
  echo ""
fi

# 4. Map and transform typed data to OMOP CDM format
if is_step_completed "omop_transform" && [ "$FORCE_RESTART" = false ]; then
  log "${BOLD}STEP 4: [ALREADY COMPLETED]${NC} Transforming typed data to OMOP CDM format"
  echo ""
else
  log "${BOLD}STEP 4: Transforming typed data to OMOP CDM format${NC}"
  log "${BLUE}-----------------------------------------------------------------------${NC}"
  
  # Check if OMOP ETL SQL exists
  if [ ! -f "$OMOP_ETL_SQL" ]; then
    log "${RED}ERROR: OMOP ETL SQL not found: $OMOP_ETL_SQL${NC}"
    exit 1
  fi

  log "Running SQL: ${YELLOW}$OMOP_ETL_SQL${NC}"
  START_TIME=$(date +%s)

  # Execute OMOP ETL transformation with progress monitoring
  # Since this SQL script doesn't have progress reporting yet, we'll show an animated spinner
  SQL_CMD="$PSQL_CMD -f $OMOP_ETL_SQL"
  if ! monitor_process_output "$SQL_CMD" "OMOP Transform" 100; then
    handle_error "OMOP transformation" $?
  fi

  END_TIME=$(date +%s)
  DURATION=$((END_TIME - START_TIME))

  log "${GREEN}OMOP transformation completed in $(format_time $DURATION)${NC}"
  
  # Mark step as completed
  mark_step_completed "omop_transform" "$DURATION"
  
  echo ""
fi

# Calculate total ETL duration
total_duration=0
for step in "preprocessing" "staging_load" "typing_transform" "population_to_staging" "omop_transform"; do
  step_duration=$(grep -o "\"$step\":[^}]*\"duration_seconds\": [0-9]*" "$CHECKPOINT_FILE" | grep -o "[0-9]*" | tail -1)
  if [[ -n "$step_duration" ]]; then
    total_duration=$((total_duration + step_duration))
  fi
done

# Final summary
log "${BOLD}=====================================================================${NC}"
log "${BOLD}                      PIPELINE COMPLETE                              ${NC}"
log "${BOLD}=====================================================================${NC}"
log "Synthea data has been successfully processed through the ETL pipeline:"

# Check which steps were completed (either previously or in this run)
if is_step_completed "preprocessing"; then
  log "  ${GREEN}1. Preprocessed to fix CSV formatting issues${NC}"
fi
if is_step_completed "staging_load"; then
  log "  ${GREEN}2. Loaded into staging database tables (TEXT format)${NC}"
fi
if is_step_completed "typing_transform"; then
  log "  ${GREEN}3. Transformed to properly typed tables${NC}"
fi
if is_step_completed "population_to_staging"; then
  log "  ${GREEN}3.5. Transferred data from population schema to staging schema${NC}"
fi
if is_step_completed "omop_transform"; then
  log "  ${GREEN}4. Mapped and transformed to OMOP CDM format${NC}"
fi

log ""
log "Total ETL processing time: ${BOLD}$(format_time $total_duration)${NC}"
log "Checkpoint file: ${BLUE}$CHECKPOINT_FILE${NC}"
log "${GREEN}Your OMOP CDM database is now ready for use in the OHDSI tools.${NC}"
log "${BOLD}=====================================================================${NC}"

# Clean up temp files
rm -rf "$TEMP_DIR"/*.log 2>/dev/null || true

exit 0
