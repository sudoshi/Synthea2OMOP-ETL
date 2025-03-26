#!/bin/bash
# run_simplified_etl.sh
# A simplified ETL pipeline that loads directly to staging and transforms to OMOP
# Eliminates the population schema, typing step, and transfer step

# Set strict error handling
set -e

# Default settings
FORCE_RESTART=false
FORCE_LOAD=false
DISABLE_PROGRESS_BARS=false
VERBOSE=false

# Color codes for terminal output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[0;33m'
BLUE='\033[0;34m'
BOLD='\033[1m'
NC='\033[0m' # No Color

# Get the directory of this script
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"

# Default paths
SYNTHEA_DATA_DIR=${SYNTHEA_DATA_DIR:-"$PROJECT_ROOT/data/synthea"}
PROCESSED_DIR="$SYNTHEA_DATA_DIR/processed"
CHECKPOINT_FILE="$PROJECT_ROOT/etl_checkpoint.json"
TEMP_DIR="/tmp/synthea_etl_$$"
OMOP_ETL_SQL="$PROJECT_ROOT/sql/etl/synthea-omop-ETL.sql"

# Database connection parameters
DB_HOST=${DB_HOST:-localhost}
DB_PORT=${DB_PORT:-5432}
DB_NAME=${DB_NAME:-synthea}
DB_USER=${DB_USER:-postgres}
DB_PASSWORD=${DB_PASSWORD:-postgres}

# Construct psql command with connection parameters
PSQL_CMD="PGPASSWORD=$DB_PASSWORD psql -h $DB_HOST -p $DB_PORT -U $DB_USER -d $DB_NAME"

# Create temp directory for progress tracking
mkdir -p "$TEMP_DIR"

# Function to display usage information
usage() {
  echo "Usage: $0 [options]"
  echo "Options:"
  echo "  --force-restart       Force restart of all ETL steps"
  echo "  --force-load          Force reload of data even if tables exist"
  echo "  --no-progress-bar     Disable progress bars"
  echo "  -v, --verbose         Enable verbose output"
  echo "  -h, --help            Display this help message"
  exit 1
}

# Parse command line arguments
while [[ $# -gt 0 ]]; do
  key="$1"
  case $key in
    --force-restart)
      FORCE_RESTART=true
      shift
      ;;
    --force-load)
      FORCE_LOAD=true
      shift
      ;;
    --no-progress-bar)
      DISABLE_PROGRESS_BARS=true
      shift
      ;;
    -v|--verbose)
      VERBOSE=true
      shift
      ;;
    -h|--help)
      usage
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
  
  # If we have a progress log for this step, show the last few lines
  local log_file="$TEMP_DIR/${step// /_}_progress.log"
  if [ -f "$log_file" ]; then
    log "${YELLOW}Last 10 lines of log:${NC}"
    tail -n 10 "$log_file"
  fi
  
  exit $exit_code
}

# Function to format time in hours, minutes, seconds
format_time() {
  local seconds=$1
  local hours=$((seconds / 3600))
  local minutes=$(( (seconds % 3600) / 60 ))
  local secs=$((seconds % 60))
  
  if [ $hours -gt 0 ]; then
    echo "${hours}h ${minutes}m ${secs}s"
  elif [ $minutes -gt 0 ]; then
    echo "${minutes}m ${secs}s"
  else
    echo "${secs}s"
  fi
}

# Function to check if a step is already completed
is_step_completed() {
  local step=$1
  
  if [ ! -f "$CHECKPOINT_FILE" ]; then
    return 1
  fi
  
  if grep -q "\"$step\":{\"status\":\"completed\"" "$CHECKPOINT_FILE"; then
    return 0
  else
    return 1
  fi
}

# Function to mark a step as completed
mark_step_completed() {
  local step=$1
  local duration=$2
  
  # Create checkpoint file if it doesn't exist
  if [ ! -f "$CHECKPOINT_FILE" ]; then
    echo "{}" > "$CHECKPOINT_FILE"
  fi
  
  # Use temporary file for JSON manipulation
  local temp_file=$(mktemp)
  
  # Read current checkpoint file
  local checkpoint_content=$(cat "$CHECKPOINT_FILE")
  
  # Check if the checkpoint file is empty or not valid JSON
  if [ -z "$checkpoint_content" ] || ! echo "$checkpoint_content" | jq . >/dev/null 2>&1; then
    checkpoint_content="{}"
  fi
  
  # Update the checkpoint with the completed step
  echo "$checkpoint_content" | \
    jq --arg step "$step" \
       --arg time "$(date -Iseconds)" \
       --arg duration "$duration" \
    '. + {($step): {"status": "completed", "completed_at": $time, "duration_seconds": $duration | tonumber}}' \
    > "$temp_file"
  
  # Replace the checkpoint file with the updated content
  mv "$temp_file" "$CHECKPOINT_FILE"
}

# Function to monitor process output and update progress
monitor_process_output() {
  local cmd=$1
  local step_name=$2
  local total_steps=$3
  local log_file="$TEMP_DIR/${step_name// /_}_progress.log"
  
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
  log "${BLUE}Creating mapping tables for UUID-to-integer conversion...${NC}"
  
  # Execute SQL to create mapping tables
  $PSQL_CMD <<EOF
-- Create person_map table if it doesn't exist
CREATE TABLE IF NOT EXISTS staging.person_map (
    source_patient_id text NOT NULL,
    person_id int4 NOT NULL,
    created_at timestamptz DEFAULT CURRENT_TIMESTAMP NULL,
    updated_at timestamptz DEFAULT CURRENT_TIMESTAMP NULL,
    CONSTRAINT person_map_person_id_key UNIQUE (person_id),
    CONSTRAINT person_map_pkey PRIMARY KEY (source_patient_id)
);

-- Create visit_map table if it doesn't exist
CREATE TABLE IF NOT EXISTS staging.visit_map (
    source_visit_id text NOT NULL,
    visit_occurrence_id int4 NOT NULL,
    person_id int4 NOT NULL,
    created_at timestamptz DEFAULT CURRENT_TIMESTAMP NULL,
    updated_at timestamptz DEFAULT CURRENT_TIMESTAMP NULL,
    CONSTRAINT visit_map_pkey PRIMARY KEY (source_visit_id),
    CONSTRAINT visit_map_visit_occurrence_id_key UNIQUE (visit_occurrence_id)
);

-- Create provider_map table if it doesn't exist
CREATE TABLE IF NOT EXISTS staging.provider_map (
    source_provider_id text NOT NULL,
    provider_id int4 NOT NULL,
    created_at timestamptz DEFAULT CURRENT_TIMESTAMP NULL,
    CONSTRAINT provider_map_pkey PRIMARY KEY (source_provider_id),
    CONSTRAINT provider_map_provider_id_key UNIQUE (provider_id)
);

-- Create care_site_map table if it doesn't exist
CREATE TABLE IF NOT EXISTS staging.care_site_map (
    source_care_site_id text NOT NULL,
    care_site_id int4 NOT NULL,
    created_at timestamptz DEFAULT CURRENT_TIMESTAMP NULL,
    CONSTRAINT care_site_map_care_site_id_key UNIQUE (care_site_id),
    CONSTRAINT care_site_map_pkey PRIMARY KEY (source_care_site_id)
);

-- Create observation_map table if it doesn't exist
CREATE TABLE IF NOT EXISTS staging.observation_map (
    source_observation_id text NOT NULL,
    observation_id int8 NOT NULL,
    CONSTRAINT observation_map_pkey PRIMARY KEY (source_observation_id)
);

-- Create measurement_map table if it doesn't exist
CREATE TABLE IF NOT EXISTS staging.measurement_map (
    source_measurement_id text NOT NULL,
    measurement_id int8 NOT NULL,
    CONSTRAINT measurement_map_pkey PRIMARY KEY (source_measurement_id)
);

-- Create sequences if they don't exist
CREATE SEQUENCE IF NOT EXISTS staging.person_seq START 1 INCREMENT 1;
CREATE SEQUENCE IF NOT EXISTS staging.visit_occurrence_seq START 1 INCREMENT 1;
CREATE SEQUENCE IF NOT EXISTS staging.provider_seq START 1 INCREMENT 1;
CREATE SEQUENCE IF NOT EXISTS staging.care_site_seq START 1 INCREMENT 1;
CREATE SEQUENCE IF NOT EXISTS staging.observation_seq START 1 INCREMENT 1;
CREATE SEQUENCE IF NOT EXISTS staging.measurement_seq START 1 INCREMENT 1;
CREATE SEQUENCE IF NOT EXISTS staging.condition_occurrence_seq START 1 INCREMENT 1;
CREATE SEQUENCE IF NOT EXISTS staging.procedure_occurrence_seq START 1 INCREMENT 1;
CREATE SEQUENCE IF NOT EXISTS staging.drug_exposure_seq START 1 INCREMENT 1;
CREATE SEQUENCE IF NOT EXISTS staging.observation_period_seq START 1 INCREMENT 1;
CREATE SEQUENCE IF NOT EXISTS staging.visit_seq START 1 INCREMENT 1;
CREATE SEQUENCE IF NOT EXISTS staging.cost_seq START 1 INCREMENT 1;
EOF

  if [ $? -ne 0 ]; then
    handle_error "Creating mapping tables" $?
  fi
  
  log "${GREEN}Mapping tables created successfully${NC}"
}

# Function to populate mapping tables
populate_mapping_tables() {
  log "${BLUE}Populating mapping tables for UUID-to-integer conversion...${NC}"
  
  # Execute SQL to populate mapping tables
  $PSQL_CMD <<EOF
-- Populate person_map table
INSERT INTO staging.person_map (source_patient_id, person_id)
SELECT 
    id, 
    nextval('staging.person_seq')
FROM staging.patients_raw p
WHERE NOT EXISTS (
    SELECT 1 FROM staging.person_map pm 
    WHERE pm.source_patient_id = p.id
);

-- Populate visit_map table
INSERT INTO staging.visit_map (source_visit_id, visit_occurrence_id, person_id)
SELECT 
    e.id, 
    nextval('staging.visit_occurrence_seq'),
    pm.person_id
FROM staging.encounters_raw e
JOIN staging.person_map pm ON e.patient_id = pm.source_patient_id
WHERE NOT EXISTS (
    SELECT 1 FROM staging.visit_map vm 
    WHERE vm.source_visit_id = e.id
);

-- Populate provider_map table
INSERT INTO staging.provider_map (source_provider_id, provider_id)
SELECT 
    id, 
    nextval('staging.provider_seq')
FROM staging.providers_raw p
WHERE NOT EXISTS (
    SELECT 1 FROM staging.provider_map pm 
    WHERE pm.source_provider_id = p.id
);

-- Create indexes for better performance
CREATE INDEX IF NOT EXISTS idx_person_map_person_id ON staging.person_map(person_id);
CREATE INDEX IF NOT EXISTS idx_visit_map_person_id ON staging.visit_map(person_id);
CREATE INDEX IF NOT EXISTS idx_visit_map_visit_occurrence_id ON staging.visit_map(visit_occurrence_id);
CREATE INDEX IF NOT EXISTS idx_observation_map_source_id ON staging.observation_map(source_observation_id);
CREATE INDEX IF NOT EXISTS idx_measurement_map_source_id ON staging.measurement_map(source_measurement_id);

-- Analyze tables for better query performance
ANALYZE staging.person_map;
ANALYZE staging.visit_map;
ANALYZE staging.provider_map;
EOF

  if [ $? -ne 0 ]; then
    handle_error "Populating mapping tables" $?
  fi
  
  log "${GREEN}Mapping tables populated successfully${NC}"
}

# Function to create lookup tables
create_lookup_tables() {
  log "${BLUE}Creating lookup tables...${NC}"
  
  # Execute SQL to create lookup tables
  $PSQL_CMD <<EOF
-- Create gender_lookup table if it doesn't exist
CREATE TABLE IF NOT EXISTS staging.gender_lookup (
    source_gender varchar(10) NOT NULL,
    gender_concept_id int4 NOT NULL,
    gender_source_concept_id int4 NULL,
    description varchar(255) NULL,
    CONSTRAINT gender_lookup_pkey PRIMARY KEY (source_gender)
);

-- Create race_lookup table if it doesn't exist
CREATE TABLE IF NOT EXISTS staging.race_lookup (
    source_race varchar(50) NOT NULL,
    race_concept_id int4 NOT NULL,
    race_source_concept_id int4 NULL,
    description varchar(255) NULL,
    CONSTRAINT race_lookup_pkey PRIMARY KEY (source_race)
);

-- Create ethnicity_lookup table if it doesn't exist
CREATE TABLE IF NOT EXISTS staging.ethnicity_lookup (
    source_ethnicity varchar(50) NOT NULL,
    ethnicity_concept_id int4 NOT NULL,
    ethnicity_source_concept_id int4 NULL,
    description varchar(255) NULL,
    CONSTRAINT ethnicity_lookup_pkey PRIMARY KEY (source_ethnicity)
);

-- Populate gender_lookup if empty
INSERT INTO staging.gender_lookup (source_gender, gender_concept_id, gender_source_concept_id, description)
SELECT * FROM (
    VALUES 
        ('M', 8507, 0, 'Male'),
        ('F', 8532, 0, 'Female'),
        ('MALE', 8507, 0, 'Male'),
        ('FEMALE', 8532, 0, 'Female'),
        ('UNKNOWN', 0, 0, 'Unknown')
) AS v(source_gender, gender_concept_id, gender_source_concept_id, description)
WHERE NOT EXISTS (SELECT 1 FROM staging.gender_lookup);

-- Populate race_lookup if empty
INSERT INTO staging.race_lookup (source_race, race_concept_id, race_source_concept_id, description)
SELECT * FROM (
    VALUES 
        ('white', 8527, 0, 'White'),
        ('black', 8516, 0, 'Black or African American'),
        ('asian', 8515, 0, 'Asian'),
        ('native', 8557, 0, 'American Indian or Alaska Native'),
        ('other', 0, 0, 'Other Race'),
        ('hawaiian', 8557, 0, 'Native Hawaiian or Other Pacific Islander')
) AS v(source_race, race_concept_id, race_source_concept_id, description)
WHERE NOT EXISTS (SELECT 1 FROM staging.race_lookup);

-- Populate ethnicity_lookup if empty
INSERT INTO staging.ethnicity_lookup (source_ethnicity, ethnicity_concept_id, ethnicity_source_concept_id, description)
SELECT * FROM (
    VALUES 
        ('hispanic', 38003563, 0, 'Hispanic'),
        ('nonhispanic', 38003564, 0, 'Not Hispanic')
) AS v(source_ethnicity, ethnicity_concept_id, ethnicity_source_concept_id, description)
WHERE NOT EXISTS (SELECT 1 FROM staging.ethnicity_lookup);
EOF

  if [ $? -ne 0 ]; then
    handle_error "Creating lookup tables" $?
  fi
  
  log "${GREEN}Lookup tables created successfully${NC}"
}

# Function to create ETL progress tracking table
create_etl_progress_table() {
  log "${BLUE}Creating ETL progress tracking table...${NC}"
  
  # Execute SQL to create progress tracking table
  $PSQL_CMD <<EOF
-- Create etl_progress table if it doesn't exist
CREATE TABLE IF NOT EXISTS staging.etl_progress (
    step_name varchar(100) PRIMARY KEY,
    status varchar(20),
    started_at timestamp DEFAULT CURRENT_TIMESTAMP,
    completed_at timestamp,
    rows_processed bigint DEFAULT 0,
    error_message text
);

-- Create sql_etl_progress table if it doesn't exist
CREATE TABLE IF NOT EXISTS staging.sql_etl_progress (
    table_name text NOT NULL,
    max_id_processed int8 DEFAULT 0 NULL,
    offset_processed int8 DEFAULT 0 NULL,
    total_processed int8 DEFAULT 0 NULL,
    last_updated timestamp DEFAULT CURRENT_TIMESTAMP NULL,
    status text NULL,
    CONSTRAINT sql_etl_progress_pkey PRIMARY KEY (table_name)
);

-- Create log_progress function if it doesn't exist
CREATE OR REPLACE FUNCTION staging.log_progress(step character varying, status character varying, rows_count bigint DEFAULT NULL::bigint, error text DEFAULT NULL::text)
 RETURNS void
 LANGUAGE plpgsql
AS \$function\$
BEGIN
    IF status = 'start' THEN
        INSERT INTO staging.etl_progress (step_name, status)
        VALUES (step, 'in_progress')
        ON CONFLICT (step_name) 
        DO UPDATE SET started_at = CURRENT_TIMESTAMP, status = 'in_progress', rows_processed = 0, error_message = NULL;
    ELSIF status = 'complete' THEN
        UPDATE staging.etl_progress 
        SET completed_at = CURRENT_TIMESTAMP, 
            status = 'completed', 
            rows_processed = COALESCE(rows_count, rows_processed)
        WHERE step_name = step;
    ELSIF status = 'error' THEN
        UPDATE staging.etl_progress 
        SET status = 'error', 
            error_message = error
        WHERE step_name = step;
    END IF;
END;
\$function\$;

-- Create report_progress function if it doesn't exist
CREATE OR REPLACE FUNCTION public.report_progress(step_name text, current_count bigint, total bigint) RETURNS void AS \$\$
BEGIN
    RAISE NOTICE 'progress: %/%  %', current_count, total, step_name;
END;
\$\$ LANGUAGE plpgsql;
EOF

  if [ $? -ne 0 ]; then
    handle_error "Creating ETL progress tracking table" $?
  fi
  
  log "${GREEN}ETL progress tracking table created successfully${NC}"
}

# Print header
log "${BOLD}=====================================================================${NC}"
log "${BOLD}                SYNTHEA TO OMOP ETL PIPELINE (SIMPLIFIED)            ${NC}"
log "${BOLD}=====================================================================${NC}"
log "Starting ETL process with the following settings:"
log "  Database: ${BLUE}$DB_NAME${NC} on ${BLUE}$DB_HOST:$DB_PORT${NC}"
log "  Synthea data directory: ${BLUE}$SYNTHEA_DATA_DIR${NC}"
log "  Force restart: ${YELLOW}$FORCE_RESTART${NC}"
log "  Force load: ${YELLOW}$FORCE_LOAD${NC}"
log "  Disable progress bars: ${YELLOW}$DISABLE_PROGRESS_BARS${NC}"
log "  Verbose output: ${YELLOW}$VERBOSE${NC}"
log "${BOLD}=====================================================================${NC}"
echo ""

# 1. Preprocess Synthea CSV files
if is_step_completed "preprocessing" && [ "$FORCE_RESTART" = false ]; then
  log "${BOLD}STEP 1: [ALREADY COMPLETED]${NC} Preprocessing Synthea CSV files"
  echo ""
else
  log "${BOLD}STEP 1: Preprocessing Synthea CSV files${NC}"
  log "${BLUE}-----------------------------------------------------------------------${NC}"
  
  # Check if Synthea data directory exists
  if [ ! -d "$SYNTHEA_DATA_DIR" ]; then
    log "${RED}ERROR: Synthea data directory not found: $SYNTHEA_DATA_DIR${NC}"
    exit 1
  fi
  
  # Create processed directory if it doesn't exist
  mkdir -p "$PROCESSED_DIR"
  
  log "Processing CSV files from: ${YELLOW}$SYNTHEA_DATA_DIR${NC}"
  log "Saving processed files to: ${YELLOW}$PROCESSED_DIR${NC}"
  START_TIME=$(date +%s)
  
  # Execute preprocessing script
  PREPROCESS_CMD="$PROJECT_ROOT/scripts/preprocess_synthea_csv.sh \"$SYNTHEA_DATA_DIR\" \"$PROCESSED_DIR\""
  
  if [ "$DISABLE_PROGRESS_BARS" = true ]; then
    PREPROCESS_CMD="$PREPROCESS_CMD --no-progress-bar"
  fi
  
  log "Running: ${YELLOW}$PREPROCESS_CMD${NC}"
  
  # Execute preprocessing with progress monitoring
  if ! monitor_process_output "$PREPROCESS_CMD" "Preprocessing" 100; then
    handle_error "Preprocessing" $?
  fi
  
  END_TIME=$(date +%s)
  DURATION=$((END_TIME - START_TIME))
  
  log "${GREEN}Preprocessing completed in $(format_time $DURATION)${NC}"
  
  # Mark step as completed
  mark_step_completed "preprocessing" "$DURATION"
  
  echo ""
fi

# 2. Load processed data into staging schema
if is_step_completed "staging_load" && [ "$FORCE_RESTART" = false ]; then
  log "${BOLD}STEP 2: [ALREADY COMPLETED]${NC} Loading processed data into staging schema"
  echo ""
else
  log "${BOLD}STEP 2: Loading processed data into staging schema with efficient batch processing${NC}"
  log "${BLUE}-----------------------------------------------------------------------${NC}"
  
  # Ensure processed directory exists
  if [ ! -d "$PROCESSED_DIR" ]; then
    log "${RED}ERROR: Processed directory not found: $PROCESSED_DIR${NC}"
    log "Did preprocessing step complete successfully?"
    exit 1
  fi
  
  # Pass progress bar option through to load script with explicit path setting
  # Use the efficient staging loader script for better batch processing and progress tracking
  PROCESSED_DIR_ABS="$(readlink -f "$PROCESSED_DIR")"
  LOAD_CMD="cd $PROJECT_ROOT && DB_PASSWORD=$DB_PASSWORD SYNTHEA_DATA_DIR=\"$PROCESSED_DIR_ABS\" DB_SCHEMA=staging ./scripts/load_staging_efficient.sh"
  if [ "$FORCE_LOAD" = true ]; then
    LOAD_CMD="$LOAD_CMD --force"
  fi
  
  if [ "$DISABLE_PROGRESS_BARS" = true ]; then
    LOAD_CMD="$LOAD_CMD --no-progress-bar"
  fi
  
  log "Running: ${YELLOW}$LOAD_CMD${NC}"
  START_TIME=$(date +%s)
  
  # Execute database load with progress monitoring
  # The load_staging_efficient.sh script has built-in progress tracking
  if ! monitor_process_output "$LOAD_CMD" "Loading_to_Staging" 100; then
    handle_error "Database loading" $?
  fi
  
  # Check for reported failures in the output
  if grep -q "ERROR:" "$TEMP_DIR/Loading_to_Staging_progress.log"; then
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

# 3. Create and populate mapping tables
if is_step_completed "mapping_tables" && [ "$FORCE_RESTART" = false ]; then
  log "${BOLD}STEP 3: [ALREADY COMPLETED]${NC} Creating and populating mapping tables"
  echo ""
else
  log "${BOLD}STEP 3: Creating and populating mapping tables${NC}"
  log "${BLUE}-----------------------------------------------------------------------${NC}"
  
  START_TIME=$(date +%s)
  
  # Create schema if it doesn't exist
  $PSQL_CMD -c "CREATE SCHEMA IF NOT EXISTS staging;"
  
  # Create ETL progress tracking table
  create_etl_progress_table
  
  # Create lookup tables
  create_lookup_tables
  
  # Create mapping tables
  create_mapping_tables
  
  # Populate mapping tables
  populate_mapping_tables
  
  END_TIME=$(date +%s)
  DURATION=$((END_TIME - START_TIME))
  
  log "${GREEN}Mapping tables created and populated in $(format_time $DURATION)${NC}"
  
  # Mark step as completed
  mark_step_completed "mapping_tables" "$DURATION"
  
  echo ""
fi

# 4. Map and transform data to OMOP CDM format
if is_step_completed "omop_transform" && [ "$FORCE_RESTART" = false ]; then
  log "${BOLD}STEP 4: [ALREADY COMPLETED]${NC} Transforming data to OMOP CDM format"
  echo ""
else
  log "${BOLD}STEP 4: Transforming data to OMOP CDM format${NC}"
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
for step in "preprocessing" "staging_load" "mapping_tables" "omop_transform"; do
  step_duration=$(grep -o "\"$step\":[^}]*\"duration_seconds\": [0-9]*" "$CHECKPOINT_FILE" | grep -o "[0-9]*" | tail -1)
  if [[ -n "$step_duration" ]]; then
    total_duration=$((total_duration + step_duration))
  fi
done

# Final summary
log "${BOLD}=====================================================================${NC}"
log "${BOLD}                      PIPELINE COMPLETE                              ${NC}"
log "${BOLD}=====================================================================${NC}"
log "Synthea data has been successfully processed through the simplified ETL pipeline:"

# Check which steps were completed (either previously or in this run)
if is_step_completed "preprocessing"; then
  log "  ${GREEN}1. Preprocessed to fix CSV formatting issues${NC}"
fi
if is_step_completed "staging_load"; then
  log "  ${GREEN}2. Loaded into staging database tables${NC}"
fi
if is_step_completed "mapping_tables"; then
  log "  ${GREEN}3. Created and populated mapping tables${NC}"
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
