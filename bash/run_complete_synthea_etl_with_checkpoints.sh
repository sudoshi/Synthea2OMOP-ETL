#!/bin/bash
#
# run_complete_synthea_etl_with_checkpoints.sh
#
# A comprehensive ETL pipeline for processing Synthea data into OMOP CDM,
# with checkpoint management for resuming interrupted processes.
#
# 1. Preprocess CSV files to fix formatting issues
# 2. Load processed data into PostgreSQL staging tables (as TEXT)
# 3. Transform staging TEXT data to properly typed tables
# 4. Map and transform typed data to OMOP CDM format
#
# This script includes checkpoint functionality to resume from where
# it left off if the process is interrupted.

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
SKIP_PREPROCESSING=false
SKIP_STAGING_LOAD=false
SKIP_TYPING=false
SKIP_OMOP_TRANSFORM=false
CHECKPOINT_FILE="$PROJECT_ROOT/.synthea_etl_checkpoint.json"
FORCE_RESTART=false

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
    --skip-preprocessing)
      SKIP_PREPROCESSING=true
      shift
      ;;
    --skip-staging-load)
      SKIP_STAGING_LOAD=true
      shift
      ;;
    --skip-typing)
      SKIP_TYPING=true
      shift
      ;;
    --skip-omop-transform)
      SKIP_OMOP_TRANSFORM=true
      shift
      ;;
    --checkpoint-file)
      CHECKPOINT_FILE="$2"
      shift 2
      ;;
    --force-restart)
      FORCE_RESTART=true
      shift
      ;;
    -h|--help)
      echo "Usage: run_complete_synthea_etl_with_checkpoints.sh [OPTIONS]"
      echo "Options:"
      echo "  --input-dir DIR           Directory with Synthea output files (default: ./synthea-output)"
      echo "  --processed-dir DIR       Directory for processed files (default: ./synthea-processed)"
      echo "  --overwrite-processed     Overwrite existing preprocessed files"
      echo "  --debug                   Enable debug logging"
      echo "  --force-load              Force overwrite of existing database tables"
      echo "  --max-workers N           Maximum number of parallel workers (default: 4)"
      echo "  --skip-preprocessing      Skip the CSV preprocessing step"
      echo "  --skip-staging-load       Skip loading data into staging tables"
      echo "  --skip-typing             Skip transforming staging to typed tables"
      echo "  --skip-omop-transform     Skip transforming typed data to OMOP CDM"
      echo "  --checkpoint-file FILE    Path to checkpoint file (default: .synthea_etl_checkpoint.json)"
      echo "  --force-restart           Ignore checkpoints and start from beginning"
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

# Initialize or load checkpoint file
init_or_load_checkpoint() {
  if [ "$FORCE_RESTART" = true ]; then
    echo "Forcing restart: Initializing new checkpoint file"
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
    echo "Found checkpoint file at: $CHECKPOINT_FILE"
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

# Print banner
echo "====================================================================="
echo "         COMPLETE SYNTHEA TO OMOP ETL PIPELINE WITH CHECKPOINTS      "
echo "====================================================================="
echo "Configuration:"
echo "  Input directory:       $INPUT_DIR"
echo "  Processed directory:   $PROCESSED_DIR"
echo "  Database:              $DB_NAME on $DB_HOST:$DB_PORT"
echo "  Checkpoint file:       $CHECKPOINT_FILE"
echo "  Overwrite processed:   $OVERWRITE_PROCESSED"
echo "  Debug mode:            $DEBUG"
echo "  Force restart:         $FORCE_RESTART"
echo "  Force DB load:         $FORCE_LOAD"
echo "  Max workers:           $MAX_WORKERS"
echo "  Skip preprocessing:    $SKIP_PREPROCESSING"
echo "  Skip staging load:     $SKIP_STAGING_LOAD"
echo "  Skip typing:           $SKIP_TYPING"
echo "  Skip OMOP transform:   $SKIP_OMOP_TRANSFORM"
echo "====================================================================="

# Initialize/load checkpoint
init_or_load_checkpoint

# Ensure the scripts exist
if [ ! -f "$PREPROCESS_SCRIPT" ] && [ "$SKIP_PREPROCESSING" = false ]; then
  echo "ERROR: Preprocess script not found: $PREPROCESS_SCRIPT"
  exit 1
fi

if [ ! -f "$LOAD_SCRIPT" ] && [ "$SKIP_STAGING_LOAD" = false ]; then
  echo "ERROR: Load script not found: $LOAD_SCRIPT"
  exit 1
fi

if [ ! -f "$TYPING_SQL" ] && [ "$SKIP_TYPING" = false ]; then
  echo "ERROR: Typing SQL script not found: $TYPING_SQL"
  exit 1
fi

if [ ! -f "$OMOP_ETL_SQL" ] && [ "$SKIP_OMOP_TRANSFORM" = false ]; then
  echo "ERROR: OMOP ETL SQL script not found: $OMOP_ETL_SQL"
  exit 1
fi

# 1. Preprocess Synthea CSV files
if [ "$SKIP_PREPROCESSING" = false ]; then
  # Check if this step is already completed
  if is_step_completed "preprocessing" && [ "$FORCE_RESTART" = false ]; then
    echo "STEP 1: [ALREADY COMPLETED] Preprocessing Synthea CSV files"
    echo ""
  else
    echo "STEP 1: Preprocessing Synthea CSV files"
    echo "-----------------------------------------------------------------------"

    PREPROCESS_CMD="$PYTHON $PREPROCESS_SCRIPT --input-dir $INPUT_DIR --output-dir $PROCESSED_DIR"

    if [ "$OVERWRITE_PROCESSED" = true ]; then
      PREPROCESS_CMD="$PREPROCESS_CMD --overwrite"
    fi

    if [ "$DEBUG" = true ]; then
      PREPROCESS_CMD="$PREPROCESS_CMD --debug"
    fi

    echo "Running: $PREPROCESS_CMD"
    START_TIME=$(date +%s)

    # Execute preprocessing
    eval "$PREPROCESS_CMD"
    PREPROCESS_STATUS=$?

    END_TIME=$(date +%s)
    DURATION=$((END_TIME - START_TIME))

    if [ $PREPROCESS_STATUS -ne 0 ]; then
      echo "ERROR: Preprocessing failed with status $PREPROCESS_STATUS"
      echo "See logs for details."
      exit $PREPROCESS_STATUS
    fi

    echo "Preprocessing completed in $DURATION seconds"
    
    # Mark step as completed
    mark_step_completed "preprocessing" "$DURATION"
    
    echo ""
  fi
else
  echo "STEP 1: [SKIPPED] Preprocessing Synthea CSV files"
  echo ""
fi

# 2. Load processed data into staging schema
if [ "$SKIP_STAGING_LOAD" = false ]; then
  # Check if this step is already completed
  if is_step_completed "staging_load" && [ "$FORCE_RESTART" = false ]; then
    echo "STEP 2: [ALREADY COMPLETED] Loading processed data into staging schema"
    echo ""
  else
    echo "STEP 2: Loading processed data into staging schema (TEXT format)"
    echo "-----------------------------------------------------------------------"

    LOAD_CMD="SYNTHEA_DATA_DIR=$PROCESSED_DIR $LOAD_SCRIPT"

    if [ "$FORCE_LOAD" = true ]; then
      LOAD_CMD="$LOAD_CMD --force"
    fi

    echo "Running: $LOAD_CMD"
    START_TIME=$(date +%s)

    # Execute database load
    eval "$LOAD_CMD"
    LOAD_STATUS=$?

    END_TIME=$(date +%s)
    DURATION=$((END_TIME - START_TIME))

    if [ $LOAD_STATUS -ne 0 ]; then
      echo "ERROR: Database loading failed with status $LOAD_STATUS"
      echo "See logs for details."
      exit $LOAD_STATUS
    fi

    echo "Database loading completed in $DURATION seconds"
    
    # Mark step as completed
    mark_step_completed "staging_load" "$DURATION"
    
    echo ""
  fi
else
  echo "STEP 2: [SKIPPED] Loading processed data into staging schema"
  echo ""
fi

# 3. Transform staging TEXT data to properly typed tables
if [ "$SKIP_TYPING" = false ]; then
  # Check if this step is already completed
  if is_step_completed "typing_transform" && [ "$FORCE_RESTART" = false ]; then
    echo "STEP 3: [ALREADY COMPLETED] Transforming staging data to properly typed tables"
    echo ""
  else
    echo "STEP 3: Transforming staging data to properly typed tables"
    echo "-----------------------------------------------------------------------"

    echo "Running SQL: $TYPING_SQL"
    START_TIME=$(date +%s)

    # Execute typing transformation
    $PSQL_CMD -f "$TYPING_SQL" > /tmp/typing_output.log 2>&1
    TYPING_STATUS=$?

    END_TIME=$(date +%s)
    DURATION=$((END_TIME - START_TIME))

    if [ $TYPING_STATUS -ne 0 ]; then
      echo "ERROR: Type transformation failed with status $TYPING_STATUS"
      echo "Error log:"
      cat /tmp/typing_output.log
      exit $TYPING_STATUS
    fi

    echo "Type transformation completed in $DURATION seconds"
    
    # Mark step as completed
    mark_step_completed "typing_transform" "$DURATION"
    
    echo ""
  fi
else
  echo "STEP 3: [SKIPPED] Transforming staging data to properly typed tables"
  echo ""
fi

# 4. Map and transform typed data to OMOP CDM format
if [ "$SKIP_OMOP_TRANSFORM" = false ]; then
  # Check if this step is already completed
  if is_step_completed "omop_transform" && [ "$FORCE_RESTART" = false ]; then
    echo "STEP 4: [ALREADY COMPLETED] Transforming typed data to OMOP CDM format"
    echo ""
  else
    echo "STEP 4: Transforming typed data to OMOP CDM format"
    echo "-----------------------------------------------------------------------"

    echo "Running SQL: $OMOP_ETL_SQL"
    START_TIME=$(date +%s)

    # Execute OMOP ETL transformation
    $PSQL_CMD -f "$OMOP_ETL_SQL" > /tmp/omop_etl_output.log 2>&1
    OMOP_STATUS=$?

    END_TIME=$(date +%s)
    DURATION=$((END_TIME - START_TIME))

    if [ $OMOP_STATUS -ne 0 ]; then
      echo "ERROR: OMOP transformation failed with status $OMOP_STATUS"
      echo "Error log:"
      cat /tmp/omop_etl_output.log
      exit $OMOP_STATUS
    fi

    echo "OMOP transformation completed in $DURATION seconds"
    
    # Mark step as completed
    mark_step_completed "omop_transform" "$DURATION"
    
    echo ""
  fi
else
  echo "STEP 4: [SKIPPED] Transforming typed data to OMOP CDM format"
  echo ""
fi

# Final summary
echo "====================================================================="
echo "                      PIPELINE COMPLETE                              "
echo "====================================================================="
echo "Synthea data has been successfully processed through the ETL pipeline:"

# Check which steps were completed (either previously or in this run)
if is_step_completed "preprocessing" || [ "$SKIP_PREPROCESSING" = false ]; then
  echo "  1. Preprocessed to fix CSV formatting issues"
fi
if is_step_completed "staging_load" || [ "$SKIP_STAGING_LOAD" = false ]; then
  echo "  2. Loaded into staging database tables (TEXT format)"
fi
if is_step_completed "typing_transform" || [ "$SKIP_TYPING" = false ]; then
  echo "  3. Transformed to properly typed tables"
fi
if is_step_completed "omop_transform" || [ "$SKIP_OMOP_TRANSFORM" = false ]; then
  echo "  4. Mapped and transformed to OMOP CDM format"
fi

echo ""
echo "Checkpoint file: $CHECKPOINT_FILE"
echo "Your OMOP CDM database is now ready for use in the OHDSI tools."
echo "====================================================================="

exit 0
