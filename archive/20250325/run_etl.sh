#!/bin/bash
# Script to run the entire ETL process

# Set database connection parameters
DB_HOST="localhost"
DB_USER="postgres"
DB_NAME="ohdsi"
DB_PASSWORD="acumenus"

# SQL script path
SQL_FILE="sql/etl/run_all_etl.sql"

# Display header
echo "===== Starting Synthea to OMOP ETL Process ====="
echo "Date: $(date)"
echo ""

# Execute the SQL script with timing
echo "Executing ETL process..."
start_time=$(date +%s)
PGPASSWORD=$DB_PASSWORD psql -h $DB_HOST -U $DB_USER -d $DB_NAME -f $SQL_FILE
end_time=$(date +%s)

# Calculate duration
duration=$((end_time - start_time))
hours=$((duration / 3600))
minutes=$(( (duration % 3600) / 60 ))
seconds=$((duration % 60))

# Display completion message
echo ""
echo "===== ETL Process Completed ====="
echo "Total duration: ${hours}h ${minutes}m ${seconds}s"
echo "Date: $(date)"

# Display ETL progress log
echo ""
echo "===== ETL Progress Log ====="
PGPASSWORD=$DB_PASSWORD psql -h $DB_HOST -U $DB_USER -d $DB_NAME -c "
SELECT 
    step_name, 
    started_at, 
    completed_at, 
    status, 
    rows_processed, 
    error_message
FROM 
    staging.etl_progress
ORDER BY 
    started_at;"

# Display record counts
echo ""
echo "===== Record Counts ====="
PGPASSWORD=$DB_PASSWORD psql -h $DB_HOST -U $DB_USER -d $DB_NAME -c "
SELECT 'person' AS table_name, COUNT(*) AS record_count FROM omop.person
UNION ALL
SELECT 'observation_period' AS table_name, COUNT(*) AS record_count FROM omop.observation_period
UNION ALL
SELECT 'visit_occurrence' AS table_name, COUNT(*) AS record_count FROM omop.visit_occurrence
UNION ALL
SELECT 'condition_occurrence' AS table_name, COUNT(*) AS record_count FROM omop.condition_occurrence
UNION ALL
SELECT 'drug_exposure' AS table_name, COUNT(*) AS record_count FROM omop.drug_exposure
UNION ALL
SELECT 'procedure_occurrence' AS table_name, COUNT(*) AS record_count FROM omop.procedure_occurrence
UNION ALL
SELECT 'measurement' AS table_name, COUNT(*) AS record_count FROM omop.measurement
UNION ALL
SELECT 'observation' AS table_name, COUNT(*) AS record_count FROM omop.observation
UNION ALL
SELECT 'death' AS table_name, COUNT(*) AS record_count FROM omop.death
UNION ALL
SELECT 'cost' AS table_name, COUNT(*) AS record_count FROM omop.cost
ORDER BY table_name;"
