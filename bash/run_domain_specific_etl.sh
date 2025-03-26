#!/bin/bash
# run_simplified_etl.sh
# A simplified ETL pipeline that loads directly to staging and transforms to OMOP
# Eliminates the population schema, typing step, and transfer step

set -e

# Default settings
DEFAULT_DB_HOST="localhost"
DEFAULT_DB_PORT="5432"
DEFAULT_DB_NAME="ohdsi"
DEFAULT_DB_USER="postgres"
DEFAULT_DB_SCHEMA="staging"
DEFAULT_SYNTHEA_DIR="./synthea-output"
DEFAULT_PROCESSED_DIR="./synthea-output/processed"
DEFAULT_MAX_WORKERS=4

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
BOLD='\033[1m'
NC='\033[0m'

# Get project root directory
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

# Load environment variables from .env file if it exists
if [ -f "$PROJECT_ROOT/.env" ]; then
    source "$PROJECT_ROOT/.env"
fi

# Set database connection parameters
DB_HOST="${DB_HOST:-$DEFAULT_DB_HOST}"
DB_PORT="${DB_PORT:-$DEFAULT_DB_PORT}"
DB_NAME="${DB_NAME:-$DEFAULT_DB_NAME}"
DB_USER="${DB_USER:-$DEFAULT_DB_USER}"
DB_SCHEMA="${DB_SCHEMA:-$DEFAULT_DB_SCHEMA}"
SYNTHEA_DATA_DIR="${SYNTHEA_DATA_DIR:-$DEFAULT_SYNTHEA_DIR}"
PROCESSED_DIR="${PROCESSED_DIR:-$DEFAULT_PROCESSED_DIR}"
MAX_WORKERS="${MAX_WORKERS:-$DEFAULT_MAX_WORKERS}"

# Initialize other variables
FORCE_RESTART=false
FORCE_LOAD=false
DISABLE_PROGRESS_BARS=false
VERBOSE=false

# Function to display usage information
usage() {
    echo "Usage: $0 [options]"
    echo "Options:"
    echo "  -h, --help                 Display this help message"
    echo "  -r, --restart              Force restart of ETL process"
    echo "  -f, --force-load           Force reload of staging data"
    echo "  -d, --data-dir DIR         Specify Synthea data directory (default: $DEFAULT_SYNTHEA_DIR)"
    echo "  -p, --processed-dir DIR    Specify directory for processed files (default: $DEFAULT_PROCESSED_DIR)"
    echo "  -w, --workers N            Specify maximum number of worker processes (default: $DEFAULT_MAX_WORKERS)"
    echo "  -n, --no-progress          Disable progress bars"
    echo "  -v, --verbose              Enable verbose output"
    exit 1
}

# Parse command line arguments
while [[ $# -gt 0 ]]; do
  key="$1"
  case $key in
    -h|--help)
      usage
      ;;
    -r|--restart)
      FORCE_RESTART=true
      shift
      ;;
    -f|--force-load)
      FORCE_LOAD=true
      shift
      ;;
    -d|--data-dir)
      SYNTHEA_DATA_DIR="$2"
      shift 2
      ;;
    -p|--processed-dir)
      PROCESSED_DIR="$2"
      shift 2
      ;;
    -w|--workers)
      MAX_WORKERS="$2"
      shift 2
      ;;
    -n|--no-progress)
      DISABLE_PROGRESS_BARS=true
      shift
      ;;
    -v|--verbose)
      VERBOSE=true
      shift
      ;;
    *)
      echo "Unknown option: $1"
      usage
      ;;
  esac
done

# Function to log messages
log() {
    echo -e "$(date '+%Y-%m-%d %H:%M:%S') $1"
}

# Function to handle errors
handle_error() {
    local step=$1
    local exit_code=$2
    
    log "${RED}ERROR: $step failed with exit code $exit_code${NC}"
    log "${RED}Check the logs for more information${NC}"
    
    # Clean up any temporary files or processes
    
    # Exit with the same error code
    exit $exit_code
}

# Function to format time in hours, minutes, seconds
format_time() {
    local seconds=$1
    local hours=$((seconds / 3600))
    local minutes=$(((seconds % 3600) / 60))
    local secs=$((seconds % 60))
    
    if [ $hours -gt 0 ]; then
        printf "%dh %dm %ds" $hours $minutes $secs
    elif [ $minutes -gt 0 ]; then
        printf "%dm %ds" $minutes $secs
    else
        printf "%ds" $secs
    fi
}

# Function to check if a step is already completed
is_step_completed() {
    local step=$1
    local checkpoint_file="$PROJECT_ROOT/.etl_checkpoint"
    
    if [ ! -f "$checkpoint_file" ]; then
        return 1
    fi
    
    if grep -q "^$step:completed" "$checkpoint_file"; then
        return 0
    else
        return 1
    fi
}

# Function to mark a step as completed
mark_step_completed() {
    local step=$1
    local duration=$2
    local checkpoint_file="$PROJECT_ROOT/.etl_checkpoint"
    
    # Create checkpoint file if it doesn't exist
    if [ ! -f "$checkpoint_file" ]; then
        touch "$checkpoint_file"
    fi
    
    # Remove any existing entry for this step
    sed -i "/^$step:/d" "$checkpoint_file"
    
    # Add the new entry
    echo "$step:completed:$duration" >> "$checkpoint_file"
    
    log "${GREEN}Step '$step' marked as completed (duration: $(format_time $duration))${NC}"
}

# Function to monitor process output and update progress
monitor_process_output() {
    local cmd=$1
    local step_name=$2
    local total_steps=$3
    local log_file="$PROJECT_ROOT/logs/${step_name}_$(date '+%Y%m%d_%H%M%S').log"
    
    # Create logs directory if it doesn't exist
    mkdir -p "$PROJECT_ROOT/logs"
  
    # Run the command and capture output
    if [ "$VERBOSE" = true ]; then
        eval "$cmd" 2>&1 | tee "$log_file"
        local exit_code=${PIPESTATUS[0]}
    else
        eval "$cmd" > "$log_file" 2>&1
        local exit_code=$?
    fi
  
    # Return the exit code of the command
    return $exit_code
}

# Function to create mapping tables for UUID-to-integer conversion
create_mapping_tables() {
    log "Creating mapping tables for UUID-to-integer conversion..."
    
    local psql_cmd="psql -h $DB_HOST -p $DB_PORT -d $DB_NAME -U $DB_USER -v ON_ERROR_STOP=1"
    
    # Create the mapping tables SQL
    cat > "$PROJECT_ROOT/temp_create_mapping_tables.sql" << EOF
-- Create schema if it doesn't exist
CREATE SCHEMA IF NOT EXISTS staging;

-- Create visit_map table
DROP TABLE IF EXISTS staging.visit_map;
CREATE TABLE staging.visit_map (
    encounter_id VARCHAR(255) PRIMARY KEY,
    visit_occurrence_id BIGINT NOT NULL
);

-- Create sequence for visit_occurrence_id
DROP SEQUENCE IF EXISTS staging.visit_occurrence_id_seq;
CREATE SEQUENCE staging.visit_occurrence_id_seq START 1;

-- Create person_map table
DROP TABLE IF EXISTS staging.person_map;
CREATE TABLE staging.person_map (
    patient_id VARCHAR(255) PRIMARY KEY,
    person_id BIGINT NOT NULL
);

-- Create sequence for person_id
DROP SEQUENCE IF EXISTS staging.person_id_seq;
CREATE SEQUENCE staging.person_id_seq START 1;

-- Create condition_map table
DROP TABLE IF EXISTS staging.condition_map;
CREATE TABLE staging.condition_map (
    condition_id VARCHAR(255) PRIMARY KEY,
    condition_occurrence_id BIGINT NOT NULL
);

-- Create sequence for condition_occurrence_id
DROP SEQUENCE IF EXISTS staging.condition_occurrence_id_seq;
CREATE SEQUENCE staging.condition_occurrence_id_seq START 1;

-- Create observation_map table
DROP TABLE IF EXISTS staging.observation_map;
CREATE TABLE staging.observation_map (
    observation_id VARCHAR(255) PRIMARY KEY,
    observation_id_int BIGINT NOT NULL
);

-- Create sequence for observation_id
DROP SEQUENCE IF EXISTS staging.observation_id_seq;
CREATE SEQUENCE staging.observation_id_seq START 1;

-- Create procedure_map table
DROP TABLE IF EXISTS staging.procedure_map;
CREATE TABLE staging.procedure_map (
    procedure_id VARCHAR(255) PRIMARY KEY,
    procedure_occurrence_id BIGINT NOT NULL
);

-- Create sequence for procedure_occurrence_id
DROP SEQUENCE IF EXISTS staging.procedure_occurrence_id_seq;
CREATE SEQUENCE staging.procedure_occurrence_id_seq START 1;

-- Create medication_map table
DROP TABLE IF EXISTS staging.medication_map;
CREATE TABLE staging.medication_map (
    medication_id VARCHAR(255) PRIMARY KEY,
    drug_exposure_id BIGINT NOT NULL
);

-- Create sequence for drug_exposure_id
DROP SEQUENCE IF EXISTS staging.drug_exposure_id_seq;
CREATE SEQUENCE staging.drug_exposure_id_seq START 1;
EOF

    # Execute the SQL
    if ! $psql_cmd -f "$PROJECT_ROOT/temp_create_mapping_tables.sql"; then
        rm -f "$PROJECT_ROOT/temp_create_mapping_tables.sql"
        return 1
    fi
    
    rm -f "$PROJECT_ROOT/temp_create_mapping_tables.sql"
    return 0
}

# Function to populate mapping tables with batch processing
populate_mapping_tables() {
    log "Populating mapping tables with batch processing..."
    
    local batch_size=100000  # Adjust based on your system's memory
    local psql_cmd="psql -h $DB_HOST -p $DB_PORT -d $DB_NAME -U $DB_USER -v ON_ERROR_STOP=1"
    
    # Tables to process
    local tables=("encounters:visit_map:encounter_id:visit_occurrence_id:visit_occurrence_id_seq" 
                 "patients:person_map:patient_id:person_id:person_id_seq" 
                 "conditions:condition_map:condition_id:condition_occurrence_id:condition_occurrence_id_seq" 
                 "observations:observation_map:observation_id:observation_id_int:observation_id_seq" 
                 "procedures:procedure_map:procedure_id:procedure_occurrence_id:procedure_occurrence_id_seq" 
                 "medications:medication_map:medication_id:drug_exposure_id:drug_exposure_id_seq")
    
    # Process each table
    for table_info in "${tables[@]}"; do
        # Parse table info
        IFS=':' read -r source_table map_table id_column map_id_column seq_name <<< "$table_info"
        
        log "Processing mapping for $source_table to $map_table..."
        
        # Get total count
        local count_sql="SELECT COUNT(DISTINCT id) FROM staging.$source_table;"
        local total_count=$($psql_cmd -t -c "$count_sql" | tr -d '[:space:]')
        
        if [ -z "$total_count" ] || [ "$total_count" -eq 0 ]; then
            log "${YELLOW}No records found in staging.$source_table, skipping...${NC}"
            continue
        fi
        
        log "Found ${YELLOW}$total_count${NC} distinct records in staging.$source_table"
        
        # Create temporary table for batch processing
        local setup_sql="
        DROP TABLE IF EXISTS staging.temp_${source_table}_ids;
        CREATE TABLE staging.temp_${source_table}_ids AS 
        SELECT DISTINCT id FROM staging.$source_table;
        CREATE INDEX IF NOT EXISTS idx_temp_${source_table}_ids ON staging.temp_${source_table}_ids(id);
        "
        
        log "Creating temporary table for batch processing..."
        if ! $psql_cmd -c "$setup_sql"; then
            log "${RED}Failed to create temporary table for $source_table${NC}"
            return 1
        fi
        
        # Process in batches
        local offset=0
        local batch_num=1
        local total_batches=$(( (total_count + batch_size - 1) / batch_size ))
        
        while [ $offset -lt $total_count ]; do
            log "Processing batch $batch_num of $total_batches for $source_table (offset: $offset, batch size: $batch_size)"
            
            local batch_sql="
            BEGIN;
            INSERT INTO staging.$map_table ($id_column, $map_id_column)
            SELECT id, nextval('staging.$seq_name')
            FROM staging.temp_${source_table}_ids
            ORDER BY id
            LIMIT $batch_size OFFSET $offset
            ON CONFLICT ($id_column) DO NOTHING;
            COMMIT;
            "
            
            if ! $psql_cmd -c "$batch_sql"; then
                log "${RED}Failed to process batch $batch_num for $source_table${NC}"
                # Clean up temporary table
                $psql_cmd -c "DROP TABLE IF EXISTS staging.temp_${source_table}_ids;"
                return 1
            fi
            
            offset=$((offset + batch_size))
            batch_num=$((batch_num + 1))
            
            # Calculate and display progress
            local progress=$((offset > total_count ? total_count : offset))
            local percent=$((progress * 100 / total_count))
            log "${GREEN}Progress: $progress/$total_count ($percent%)${NC}"
        done
        
        # Clean up temporary table
        $psql_cmd -c "DROP TABLE IF EXISTS staging.temp_${source_table}_ids;"
        
        log "${GREEN}Completed mapping for $source_table to $map_table${NC}"
    done
    
    return 0
}

# Function to create lookup tables
create_lookup_tables() {
    log "Creating lookup tables..."
    
    local psql_cmd="psql -h $DB_HOST -p $DB_PORT -d $DB_NAME -U $DB_USER -v ON_ERROR_STOP=1"
    
    # Create the lookup tables SQL
    cat > "$PROJECT_ROOT/temp_create_lookup_tables.sql" << EOF
-- Create schema if it doesn't exist
CREATE SCHEMA IF NOT EXISTS lookup;

-- Create gender lookup table
DROP TABLE IF EXISTS lookup.gender;
CREATE TABLE lookup.gender (
    source_code VARCHAR(255) PRIMARY KEY,
    target_concept_id INTEGER NOT NULL
);

-- Populate gender lookup table
INSERT INTO lookup.gender (source_code, target_concept_id) VALUES
('M', 8507), -- MALE
('F', 8532); -- FEMALE

-- Create race lookup table
DROP TABLE IF EXISTS lookup.race;
CREATE TABLE lookup.race (
    source_code VARCHAR(255) PRIMARY KEY,
    target_concept_id INTEGER NOT NULL
);

-- Populate race lookup table
INSERT INTO lookup.race (source_code, target_concept_id) VALUES
('white', 8527), -- White
('black', 8516), -- Black or African American
('asian', 8515), -- Asian
('native', 8657), -- American Indian or Alaska Native
('other', 8522); -- Other Race

-- Create ethnicity lookup table
DROP TABLE IF EXISTS lookup.ethnicity;
CREATE TABLE lookup.ethnicity (
    source_code VARCHAR(255) PRIMARY KEY,
    target_concept_id INTEGER NOT NULL
);

-- Populate ethnicity lookup table
INSERT INTO lookup.ethnicity (source_code, target_concept_id) VALUES
('hispanic', 38003563), -- Hispanic
('nonhispanic', 38003564); -- Not Hispanic

-- Create visit type lookup table
DROP TABLE IF EXISTS lookup.visit_type;
CREATE TABLE lookup.visit_type (
    source_code VARCHAR(255) PRIMARY KEY,
    target_concept_id INTEGER NOT NULL
);

-- Populate visit type lookup table
INSERT INTO lookup.visit_type (source_code, target_concept_id) VALUES
('ambulatory', 9202), -- Outpatient Visit
('wellness', 9202), -- Outpatient Visit
('emergency', 9203), -- Emergency Room Visit
('inpatient', 9201), -- Inpatient Visit
('urgentcare', 9203); -- Emergency Room Visit
EOF

    # Execute the SQL
    if ! $psql_cmd -f "$PROJECT_ROOT/temp_create_lookup_tables.sql"; then
        rm -f "$PROJECT_ROOT/temp_create_lookup_tables.sql"
        return 1
    fi
    
    rm -f "$PROJECT_ROOT/temp_create_lookup_tables.sql"
    return 0
}

# Function to create ETL progress tracking table
create_etl_progress_table() {
    log "Creating ETL progress tracking table..."
    
    local psql_cmd="psql -h $DB_HOST -p $DB_PORT -d $DB_NAME -U $DB_USER -v ON_ERROR_STOP=1"
    
    # Create the ETL progress tracking table SQL
    cat > "$PROJECT_ROOT/temp_create_etl_progress_table.sql" << EOF
-- Create schema if it doesn't exist
CREATE SCHEMA IF NOT EXISTS admin;

-- Create ETL progress tracking table
DROP TABLE IF EXISTS admin.etl_progress;
CREATE TABLE admin.etl_progress (
    step_id SERIAL PRIMARY KEY,
    step_name VARCHAR(255) NOT NULL,
    start_time TIMESTAMP NOT NULL,
    end_time TIMESTAMP,
    status VARCHAR(50) NOT NULL,
    records_processed BIGINT,
    error_message TEXT
);

-- Create function to log ETL step start
CREATE OR REPLACE FUNCTION admin.log_etl_step_start(p_step_name VARCHAR)
RETURNS INTEGER AS $$
DECLARE
    v_step_id INTEGER;
BEGIN
    INSERT INTO admin.etl_progress (step_name, start_time, status)
    VALUES (p_step_name, NOW(), 'RUNNING')
    RETURNING step_id INTO v_step_id;
    
    RETURN v_step_id;
END;
$$ LANGUAGE plpgsql;

-- Create function to log ETL step completion
CREATE OR REPLACE FUNCTION admin.log_etl_step_complete(p_step_id INTEGER, p_records_processed BIGINT)
RETURNS VOID AS $$
BEGIN
    UPDATE admin.etl_progress
    SET end_time = NOW(),
        status = 'COMPLETED',
        records_processed = p_records_processed
    WHERE step_id = p_step_id;
END;
$$ LANGUAGE plpgsql;

-- Create function to log ETL step error
CREATE OR REPLACE FUNCTION admin.log_etl_step_error(p_step_id INTEGER, p_error_message TEXT)
RETURNS VOID AS $$
BEGIN
    UPDATE admin.etl_progress
    SET end_time = NOW(),
        status = 'ERROR',
        error_message = p_error_message
    WHERE step_id = p_step_id;
END;
$$ LANGUAGE plpgsql;
EOF

    # Execute the SQL
    if ! $psql_cmd -f "$PROJECT_ROOT/temp_create_etl_progress_table.sql"; then
        rm -f "$PROJECT_ROOT/temp_create_etl_progress_table.sql"
        return 1
    fi
    
    rm -f "$PROJECT_ROOT/temp_create_etl_progress_table.sql"
    return 0
}

# Print header
log "${BOLD}=====================================================================${NC}"
log "${BOLD}                SYNTHEA TO OMOP ETL PIPELINE (SIMPLIFIED)            ${NC}"
log "${BOLD}=====================================================================${NC}"
log "Starting ETL process with the following settings:"
log "  Database: $DB_NAME on $DB_HOST:$DB_PORT"
log "  Synthea data directory: $SYNTHEA_DATA_DIR"
log "  Force restart: $FORCE_RESTART"
log "  Force load: $FORCE_LOAD"
log "  Disable progress bars: $DISABLE_PROGRESS_BARS"
log "  Verbose output: $VERBOSE"
log "${BOLD}=====================================================================${NC}"
log ""

# Create checkpoint file if it doesn't exist and force restart is enabled
if [ "$FORCE_RESTART" = true ]; then
    log "Force restart enabled. Removing checkpoint file."
    rm -f "$PROJECT_ROOT/.etl_checkpoint"
fi

# Step 1: Preprocess Synthea CSV files
if is_step_completed "preprocessing" && [ "$FORCE_RESTART" = false ]; then
  log "${BOLD}STEP 1: [ALREADY COMPLETED]${NC} Preprocessing Synthea CSV files"
  log ""
else
  log "${BOLD}STEP 1: Preprocessing Synthea CSV files${NC}"
  log "${BLUE}-----------------------------------------------------------------------${NC}"
  log "Processing CSV files from: ${YELLOW}$SYNTHEA_DATA_DIR${NC}"
  log "Saving processed files to: ${YELLOW}$PROCESSED_DIR${NC}"
  START_TIME=$(date +%s)
  
  # Execute preprocessing script
  PREPROCESS_CMD="python $PROJECT_ROOT/python/preprocess_synthea_csv.py --input-dir \"$SYNTHEA_DATA_DIR\" --output-dir \"$PROCESSED_DIR\""
  
  if [ "$DISABLE_PROGRESS_BARS" = true ]; then
    PREPROCESS_CMD="$PREPROCESS_CMD --no-progress-bar"
  fi
  
  if [ "$VERBOSE" = true ]; then
    PREPROCESS_CMD="$PREPROCESS_CMD --debug"
  fi
  
  log "Running: ${YELLOW}$PREPROCESS_CMD${NC}"
  
  # Execute preprocessing with progress monitoring
  if ! monitor_process_output "$PREPROCESS_CMD" "preprocessing" 100; then
    handle_error "Preprocessing" $?
  fi
  
  END_TIME=$(date +%s)
  DURATION=$((END_TIME - START_TIME))
  
  log "${GREEN}Preprocessing completed in $(format_time $DURATION)${NC}"
  
  # Mark step as completed
  mark_step_completed "preprocessing" "$DURATION"
  
  log ""
fi

# Step 2: Load Synthea data into staging tables
if is_step_completed "load_staging" && [ "$FORCE_RESTART" = false ] && [ "$FORCE_LOAD" = false ]; then
  log "${BOLD}STEP 2: [ALREADY COMPLETED]${NC} Loading Synthea data into staging tables"
  log ""
else
  log "${BOLD}STEP 2: Loading Synthea data into staging tables${NC}"
  log "${BLUE}-----------------------------------------------------------------------${NC}"
  
  START_TIME=$(date +%s)
  
  # Execute load staging script
  LOAD_STAGING_CMD="$PROJECT_ROOT/scripts/load_staging_efficient.sh \"$PROCESSED_DIR\" $MAX_WORKERS"
  
  if [ "$VERBOSE" = true ]; then
    LOAD_STAGING_CMD="$LOAD_STAGING_CMD -v"
  fi
  
  log "Running: ${YELLOW}$LOAD_STAGING_CMD${NC}"
  
  # Execute load staging with progress monitoring
  if ! monitor_process_output "$LOAD_STAGING_CMD" "load_staging" 100; then
    handle_error "Loading staging data" $?
  fi
  
  END_TIME=$(date +%s)
  DURATION=$((END_TIME - START_TIME))
  
  log "${GREEN}Loading staging data completed in $(format_time $DURATION)${NC}"
  
  # Mark step as completed
  mark_step_completed "load_staging" "$DURATION"
  
  log ""
fi

# Step 3: Create and populate mapping tables
if is_step_completed "mapping_tables" && [ "$FORCE_RESTART" = false ]; then
  log "${BOLD}STEP 3: [ALREADY COMPLETED]${NC} Creating and populating mapping tables"
  log ""
else
  log "${BOLD}STEP 3: Creating and populating mapping tables${NC}"
  log "${BLUE}-----------------------------------------------------------------------${NC}"
  
  START_TIME=$(date +%s)
  
  # Create mapping tables
  if ! create_mapping_tables; then
    handle_error "Creating mapping tables" $?
  fi
  
  # Populate mapping tables
  if ! populate_mapping_tables; then
    handle_error "Populating mapping tables" $?
  fi
  
  END_TIME=$(date +%s)
  DURATION=$((END_TIME - START_TIME))
  
  log "${GREEN}Creating and populating mapping tables completed in $(format_time $DURATION)${NC}"
  
  # Mark step as completed
  mark_step_completed "mapping_tables" "$DURATION"
  
  log ""
fi

# Step 4: Create lookup tables
if is_step_completed "lookup_tables" && [ "$FORCE_RESTART" = false ]; then
  log "${BOLD}STEP 4: [ALREADY COMPLETED]${NC} Creating lookup tables"
  log ""
else
  log "${BOLD}STEP 4: Creating lookup tables${NC}"
  log "${BLUE}-----------------------------------------------------------------------${NC}"
  
  START_TIME=$(date +%s)
  
  # Create lookup tables
  if ! create_lookup_tables; then
    handle_error "Creating lookup tables" $?
  fi
  
  END_TIME=$(date +%s)
  DURATION=$((END_TIME - START_TIME))
  
  log "${GREEN}Creating lookup tables completed in $(format_time $DURATION)${NC}"
  
  # Mark step as completed
  mark_step_completed "lookup_tables" "$DURATION"
  
  log ""
fi

# Step 5: Create ETL progress tracking table
if is_step_completed "etl_progress_table" && [ "$FORCE_RESTART" = false ]; then
  log "${BOLD}STEP 5: [ALREADY COMPLETED]${NC} Creating ETL progress tracking table"
  log ""
else
  log "${BOLD}STEP 5: Creating ETL progress tracking table${NC}"
  log "${BLUE}-----------------------------------------------------------------------${NC}"
  
  START_TIME=$(date +%s)
  
  # Create ETL progress tracking table
  if ! create_etl_progress_table; then
    handle_error "Creating ETL progress tracking table" $?
  fi
  
  END_TIME=$(date +%s)
  DURATION=$((END_TIME - START_TIME))
  
  log "${GREEN}Creating ETL progress tracking table completed in $(format_time $DURATION)${NC}"
  
  # Mark step as completed
  mark_step_completed "etl_progress_table" "$DURATION"
  
  log ""
fi

# Step 6: Transform staging data to OMOP CDM
if is_step_completed "transform_to_omop" && [ "$FORCE_RESTART" = false ]; then
  log "${BOLD}STEP 6: [ALREADY COMPLETED]${NC} Transforming staging data to OMOP CDM"
  log ""
else
  log "${BOLD}STEP 6: Transforming staging data to OMOP CDM${NC}"
  log "${BLUE}-----------------------------------------------------------------------${NC}"
  
  START_TIME=$(date +%s)
  
  # Execute transform script with batch processing for large datasets
  TRANSFORM_CMD="python $PROJECT_ROOT/python/optimized_synthea_to_omop.py --max-workers $MAX_WORKERS --batch-size 50000"
  
  if [ "$VERBOSE" = true ]; then
    TRANSFORM_CMD="$TRANSFORM_CMD --debug"
  fi
  
  # Add additional parameters for large dataset handling
  TRANSFORM_CMD="$TRANSFORM_CMD --single-connection --commit-frequency 10000"
  
  log "Running: ${YELLOW}$TRANSFORM_CMD${NC}"
  
  # Execute transform with progress monitoring
  if ! monitor_process_output "$TRANSFORM_CMD" "transform_to_omop" 100; then
    handle_error "Transforming to OMOP" $?
  fi
  
  END_TIME=$(date +%s)
  DURATION=$((END_TIME - START_TIME))
  
  log "${GREEN}Transforming to OMOP completed in $(format_time $DURATION)${NC}"
  
  # Mark step as completed
  mark_step_completed "transform_to_omop" "$DURATION"
  
  log ""
fi

# Step 7: Post-processing and cleanup
if is_step_completed "post_processing" && [ "$FORCE_RESTART" = false ]; then
  log "${BOLD}STEP 7: [ALREADY COMPLETED]${NC} Post-processing and cleanup"
  log ""
else
  log "${BOLD}STEP 7: Post-processing and cleanup${NC}"
  log "${BLUE}-----------------------------------------------------------------------${NC}"
  
  START_TIME=$(date +%s)
  
  # Execute post-processing SQL
  POST_PROCESSING_SQL="$PROJECT_ROOT/sql/etl/post_processing.sql"
  POST_PROCESSING_CMD="psql -h $DB_HOST -p $DB_PORT -d $DB_NAME -U $DB_USER -v ON_ERROR_STOP=1 -f \"$POST_PROCESSING_SQL\""
  
  log "Running: ${YELLOW}$POST_PROCESSING_CMD${NC}"
  
  # Execute post-processing with progress monitoring
  if ! monitor_process_output "$POST_PROCESSING_CMD" "post_processing" 100; then
    handle_error "Post-processing" $?
  fi
  
  END_TIME=$(date +%s)
  DURATION=$((END_TIME - START_TIME))
  
  log "${GREEN}Post-processing completed in $(format_time $DURATION)${NC}"
  
  # Mark step as completed
  mark_step_completed "post_processing" "$DURATION"
  
  log ""
fi

# Print completion message
log "${BOLD}${GREEN}=====================================================================${NC}"
log "${BOLD}${GREEN}                  ETL PROCESS COMPLETED SUCCESSFULLY               ${NC}"
log "${BOLD}${GREEN}=====================================================================${NC}"

# Export environment variables for other scripts
export DB_HOST DB_PORT DB_NAME DB_USER DB_SCHEMA
export SYNTHEA_DATA_DIR PROCESSED_DIR MAX_WORKERS
