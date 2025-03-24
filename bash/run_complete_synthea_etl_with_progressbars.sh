#!/bin/bash
#
# run_complete_synthea_etl_with_progressbars.sh
#
# A comprehensive ETL pipeline for processing Synthea data into OMOP CDM
# with real-time progress bars and checkpointing.

set -euo pipefail

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

# Colors and formatting
RED='\033[0;31m'
GREEN='\033[0;32m'
BLUE='\033[0;34m'
YELLOW='\033[0;33m'
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
      echo "Usage: run_complete_synthea_etl_with_progressbars.sh [OPTIONS]"
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
LOAD_SCRIPT="$PROJECT_ROOT/scripts/load_synthea_staging.sh"
TYPING_SQL="$PROJECT_ROOT/sql/synthea_typing/synthea-typedtables-transformation.sql"
OMOP_ETL_SQL="$PROJECT_ROOT/sql/etl/run_all_etl.sql"

# Database connection settings from .env or defaults
DB_HOST="${DB_HOST:-localhost}"
DB_PORT="${DB_PORT:-5432}"
DB_NAME="${DB_NAME:-ohdsi}"
DB_USER="${DB_USER:-postgres}"
DB_PASSWORD="${DB_PASSWORD:-acumenus}"

# Export password for psql
export PGPASSWORD="$DB_PASSWORD"
PSQL_CMD="psql -h $DB_HOST -p $DB_PORT -U $DB_USER -d $DB_NAME"

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
fi

# Initialize or load checkpoint file
init_or_load_checkpoint() {
  if [ "$FORCE_RESTART" = true ]; then
    echo -e "${YELLOW}Forcing restart: Initializing new checkpoint file${NC}"
    echo '{
  "completed_steps": [],
  "last_updated": "'$(date -Iseconds)'",
  "stats": {}
}' > "$CHECKPOINT_FILE"
    return
  fi

  if [ ! -f "$CHECKPOINT_FILE" ]; then
    echo "Checkpoint file not found. Creating new one."
    echo '{
  "completed_steps": [],
  "last_updated": "'$(date -Iseconds)'",
  "stats": {}
}' > "$CHECKPOINT_FILE"
  else
    echo -e "Found checkpoint file at: ${BLUE}$CHECKPOINT_FILE${NC}"
    echo "Resuming from last checkpoint..."
    
    # Display completed steps
    if [ "$DEBUG" = true ]; then
      echo "Completed steps:"
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

# Function to display a spinner
display_spinner() {
  local pid=$1
  local message=$2
  local delay=0.1
  local spinstr='|/-\'
  
  if [ "$CAN_DISPLAY_PROGRESS" = false ]; then
    echo "$message... running"
    wait $pid
    return
  fi
  
  printf "${BLUE}%s${NC} " "$message"
  
  local temp
  while kill -0 $pid 2>/dev/null; do
    temp="${spinstr#?}"
    printf " [%c]" "${spinstr}"
    spinstr="${temp}${spinstr%$temp}"
    sleep ${delay}
    printf "\b\b\b"
  done
  printf "    \b\b\b\b"
  
  wait $pid
  local status=$?
  
  if [ $status -eq 0 ]; then
    printf "${GREEN}[DONE]${NC}\n"
  else
    printf "${RED}[FAILED]${NC}\n"
    exit $status
  fi
}

# Function to display a progress bar
display_progress_bar() {
  local current=$1
  local total=$2
  local title=$3
  local step_name=$4
  local status_message=${5:-""}
  
  if [ "$CAN_DISPLAY_PROGRESS" = false ]; then
    return
  fi

  # Calculate percentage
  local percentage=$(( current * 100 / total ))
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
  
  # Save the step status to a tmp file for progress tracking
  echo "$step_name:$current:$total" > "/tmp/synthea_etl_progress_$step_name.tmp"
  
  # If we've reached 100%, break line
  if [ "$percentage" -eq 100 ]; then
    printf "\n"
    rm -f "/tmp/synthea_etl_progress_$step_name.tmp" 2>/dev/null || true
  fi
}

# Function to monitor progress from a command that outputs progress information
monitor_progress() {
  local cmd="$1"
  local title="$2"
  local step_name="$3"
  local total=${4:-100}
  local temp_file="/tmp/synthea_etl_output_$step_name.tmp"
  
  # Run the command in background and redirect output to temp file
  eval "$cmd" > "$temp_file" 2>&1 &
  local cmd_pid=$!
  
  # Track progress
  local current=0
  display_progress_bar $current $total "$title" "$step_name" "Starting..."
  
  # Monitor the output file for progress indicators
  while kill -0 $cmd_pid 2>/dev/null; do
    # Check if progress info is available in output
    if grep -q "progress:" "$temp_file" 2>/dev/null; then
      # Extract progress info (assuming format "progress: N/M")
      local progress_line=$(grep "progress:" "$temp_file" | tail -n 1)
      if [[ $progress_line =~ progress:[[:space:]]*([0-9]+)/([0-9]+) ]]; then
        current=${BASH_REMATCH[1]}
        total=${BASH_REMATCH[2]}
        status_message=$(echo "$progress_line" | sed 's/.*progress: [0-9]*\/[0-9]* //')
        display_progress_bar $current $total "$title" "$step_name" "$status_message"
      fi
    elif grep -q "Processed [0-9]" "$temp_file" 2>/dev/null; then
      # Alternative format: "Processed N rows"
      local progress_line=$(grep "Processed [0-9]" "$temp_file" | tail -n 1)
      if [[ $progress_line =~ Processed[[:space:]]*([0-9]+) ]]; then
        current=${BASH_REMATCH[1]}
        status_message=$(echo "$progress_line" | sed 's/Processed [0-9]* //')
        display_progress_bar $current $total "$title" "$step_name" "$status_message"
      fi
    elif grep -q "Loading [0-9]" "$temp_file" 2>/dev/null; then
      # Another format: "Loading N%"
      local progress_line=$(grep "Loading [0-9]" "$temp_file" | tail -n 1)
      if [[ $progress_line =~ Loading[[:space:]]*([0-9]+)% ]]; then
        percentage=${BASH_REMATCH[1]}
        current=$((percentage * total / 100))
        display_progress_bar $current $total "$title" "$step_name" 
      fi
    else
      # Just update with indeterminate progress
      current=$((current + 1))
      if [ $current -ge $total ]; then
        current=$((total / 2))  # Reset to avoid hitting 100%
      fi
      display_progress_bar $current $total "$title" "$step_name" "Running..."
    fi
    
    sleep 0.2
  done
  
  # Command finished, show final progress
  wait $cmd_pid
  local status=$?
  
  # Show 100% completion
  display_progress_bar $total $total "$title" "$step_name" "Completed"
  
  # Display any errors from the command
  if [ $status -ne 0 ]; then
    echo -e "${RED}Command failed with status $status${NC}"
    echo "Error output:"
    cat "$temp_file"
    rm -f "$temp_file"
    exit $status
  fi
  
  rm -f "$temp_file"
}

# Print banner
echo -e "${BOLD}=====================================================================${NC}"
echo -e "${BOLD}      COMPLETE SYNTHEA TO OMOP ETL PIPELINE WITH PROGRESS BARS       ${NC}"
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
if [ ! -f "$PREPROCESS_SCRIPT" ]; then
  echo -e "${RED}ERROR: Preprocess script not found: $PREPROCESS_SCRIPT${NC}"
  exit 1
fi

if [ ! -f "$LOAD_SCRIPT" ]; then
  echo -e "${RED}ERROR: Load script not found: $LOAD_SCRIPT${NC}"
  exit 1
fi

if [ ! -f "$TYPING_SQL" ]; then
  echo -e "${RED}ERROR: Typing SQL script not found: $TYPING_SQL${NC}"
  exit 1
fi

if [ ! -f "$OMOP_ETL_SQL" ]; then
  echo -e "${RED}ERROR: OMOP ETL SQL script not found: $OMOP_ETL_SQL${NC}"
  exit 1
fi

# 1. Preprocess Synthea CSV files
if is_step_completed "preprocessing" && [ "$FORCE_RESTART" = false ]; then
  echo -e "${BOLD}STEP 1: [ALREADY COMPLETED]${NC} Preprocessing Synthea CSV files"
  echo ""
else
  echo -e "${BOLD}STEP 1: Preprocessing Synthea CSV files${NC}"
  echo -e "${BLUE}-----------------------------------------------------------------------${NC}"

  PREPROCESS_CMD="$PYTHON $PREPROCESS_SCRIPT --input-dir $INPUT_DIR --output-dir $PROCESSED_DIR"

  if [ "$OVERWRITE_PROCESSED" = true ]; then
    PREPROCESS_CMD="$PREPROCESS_CMD --overwrite"
  fi

  if [ "$DEBUG" = true ]; then
    PREPROCESS_CMD="$PREPROCESS_CMD --debug"
  fi

  echo -e "Running: ${YELLOW}$PREPROCESS_CMD${NC}"
  START_TIME=$(date +%s)

  # Execute preprocessing with progress monitoring
  monitor_progress "$PREPROCESS_CMD" "Preprocessing" "preprocess" 100
  
  END_TIME=$(date +%s)
  DURATION=$((END_TIME - START_TIME))

  echo -e "${GREEN}Preprocessing completed in $DURATION seconds${NC}"
  
  # Mark step as completed
  mark_step_completed "preprocessing" "$DURATION"
  
  echo ""
fi

# 2. Load processed data into staging schema
if is_step_completed "staging_load" && [ "$FORCE_RESTART" = false ]; then
  echo -e "${BOLD}STEP 2: [ALREADY COMPLETED]${NC} Loading processed data into staging schema"
  echo ""
else
  echo -e "${BOLD}STEP 2: Loading processed data into staging schema (TEXT format)${NC}"
  echo -e "${BLUE}-----------------------------------------------------------------------${NC}"

  # Pass progress bar option through to load script
  LOAD_CMD="SYNTHEA_DATA_DIR=$PROCESSED_DIR $LOAD_SCRIPT"
  if [ "$CAN_DISPLAY_PROGRESS" = false ]; then
    LOAD_CMD="$LOAD_CMD --no-progress-bar"
  fi

  if [ "$FORCE_LOAD" = true ]; then
    LOAD_CMD="$LOAD_CMD --force"
  fi

  echo -e "Running: ${YELLOW}$LOAD_CMD${NC}"
  START_TIME=$(date +%s)

  # Execute database load with progress monitoring
  if [ "$CAN_DISPLAY_PROGRESS" = true ]; then
    monitor_progress "$LOAD_CMD" "Loading to Staging" "staging" 100
  else
    # Just execute the command normally
    eval "$LOAD_CMD"
  fi

  END_TIME=$(date +%s)
  DURATION=$((END_TIME - START_TIME))

  echo -e "${GREEN}Database loading completed in $DURATION seconds${NC}"
  
  # Mark step as completed
  mark_step_completed "staging_load" "$DURATION"
  
  echo ""
fi

# 3. Transform staging TEXT data to properly typed tables
if is_step_completed "typing_transform" && [ "$FORCE_RESTART" = false ]; then
  echo -e "${BOLD}STEP 3: [ALREADY COMPLETED]${NC} Transforming staging data to properly typed tables"
  echo ""
else
  echo -e "${BOLD}STEP 3: Transforming staging data to properly typed tables${NC}"
  echo -e "${BLUE}-----------------------------------------------------------------------${NC}"

  echo -e "Running SQL: ${YELLOW}$TYPING_SQL${NC}"
  START_TIME=$(date +%s)

  # Execute typing transformation with a spinner (SQL doesn't report progress)
  if [ "$CAN_DISPLAY_PROGRESS" = true ]; then
    $PSQL_CMD -f "$TYPING_SQL" > /tmp/typing_output.log 2>&1 &
    display_spinner $! "Transforming to typed tables"
  else
    # Just execute the command normally
    $PSQL_CMD -f "$TYPING_SQL" > /tmp/typing_output.log 2>&1
  fi
  TYPING_STATUS=$?

  END_TIME=$(date +%s)
  DURATION=$((END_TIME - START_TIME))

  if [ $TYPING_STATUS -ne 0 ]; then
    echo -e "${RED}ERROR: Type transformation failed with status $TYPING_STATUS${NC}"
    echo "Error log:"
    cat /tmp/typing_output.log
    exit $TYPING_STATUS
  fi

  echo -e "${GREEN}Type transformation completed in $DURATION seconds${NC}"
  
  # Mark step as completed
  mark_step_completed "typing_transform" "$DURATION"
  
  echo ""
fi

# 4. Map and transform typed data to OMOP CDM format
if is_step_completed "omop_transform" && [ "$FORCE_RESTART" = false ]; then
  echo -e "${BOLD}STEP 4: [ALREADY COMPLETED]${NC} Transforming typed data to OMOP CDM format"
  echo ""
else
  echo -e "${BOLD}STEP 4: Transforming typed data to OMOP CDM format${NC}"
  echo -e "${BLUE}-----------------------------------------------------------------------${NC}"

  echo -e "Running SQL: ${YELLOW}$OMOP_ETL_SQL${NC}"
  START_TIME=$(date +%s)

  # Execute OMOP ETL transformation with a spinner (SQL doesn't report progress)
  if [ "$CAN_DISPLAY_PROGRESS" = true ]; then
    $PSQL_CMD -f "$OMOP_ETL_SQL" > /tmp/omop_etl_output.log 2>&1 &
    display_spinner $! "Transforming to OMOP format"
  else
    # Just execute the command normally
    $PSQL_CMD -f "$OMOP_ETL_SQL" > /tmp/omop_etl_output.log 2>&1
  fi
  OMOP_STATUS=$?

  END_TIME=$(date +%s)
  DURATION=$((END_TIME - START_TIME))

  if [ $OMOP_STATUS -ne 0 ]; then
    echo -e "${RED}ERROR: OMOP transformation failed with status $OMOP_STATUS${NC}"
    echo "Error log:"
    cat /tmp/omop_etl_output.log
    exit $OMOP_STATUS
  fi

  echo -e "${GREEN}OMOP transformation completed in $DURATION seconds${NC}"
  
  # Mark step as completed
  mark_step_completed "omop_transform" "$DURATION"
  
  echo ""
fi

# Final summary
echo -e "${BOLD}=====================================================================${NC}"
echo -e "${BOLD}                      PIPELINE COMPLETE                              ${NC}"
echo -e "${BOLD}=====================================================================${NC}"
echo "Synthea data has been successfully processed through the ETL pipeline:"

# Check which steps were completed (either previously or in this run)
if is_step_completed "preprocessing" || [ "$FORCE_RESTART" = false ]; then
  echo -e "  ${GREEN}1. Preprocessed to fix CSV formatting issues${NC}"
fi
if is_step_completed "staging_load" || [ "$FORCE_RESTART" = false ]; then
  echo -e "  ${GREEN}2. Loaded into staging database tables (TEXT format)${NC}"
fi
if is_step_completed "typing_transform" || [ "$FORCE_RESTART" = false ]; then
  echo -e "  ${GREEN}3. Transformed to properly typed tables${NC}"
fi
if is_step_completed "omop_transform" || [ "$FORCE_RESTART" = false ]; then
  echo -e "  ${GREEN}4. Mapped and transformed to OMOP CDM format${NC}"
fi

echo ""
echo -e "Checkpoint file: ${BLUE}$CHECKPOINT_FILE${NC}"
echo -e "${GREEN}Your OMOP CDM database is now ready for use in the OHDSI tools.${NC}"
echo -e "${BOLD}=====================================================================${NC}"

exit 0
