#!/bin/bash
# Script to reset OMOP tables for a fresh ETL run

# Set database connection parameters
DB_HOST="localhost"
DB_USER="postgres"
DB_NAME="ohdsi"
DB_PASSWORD="acumenus"

# Display header
echo "===== OMOP Tables Reset Process ====="
echo "Date: $(date)"
echo ""

# Confirm with the user
read -p "This will delete all data in the OMOP tables. Are you sure you want to continue? (y/n) " -n 1 -r
echo
if [[ ! $REPLY =~ ^[Yy]$ ]]
then
    echo "Operation cancelled."
    exit 1
fi

# Truncate OMOP tables
echo "Truncating OMOP tables..."
PGPASSWORD=$DB_PASSWORD psql -h $DB_HOST -U $DB_USER -d $DB_NAME << EOF
-- Disable foreign key constraints
SET session_replication_role = 'replica';

-- Truncate OMOP tables
TRUNCATE TABLE omop.person CASCADE;
TRUNCATE TABLE omop.observation_period CASCADE;
TRUNCATE TABLE omop.visit_occurrence CASCADE;
TRUNCATE TABLE omop.visit_detail CASCADE;
TRUNCATE TABLE omop.condition_occurrence CASCADE;
TRUNCATE TABLE omop.drug_exposure CASCADE;
TRUNCATE TABLE omop.procedure_occurrence CASCADE;
TRUNCATE TABLE omop.device_exposure CASCADE;
TRUNCATE TABLE omop.measurement CASCADE;
TRUNCATE TABLE omop.observation CASCADE;
TRUNCATE TABLE omop.death CASCADE;
TRUNCATE TABLE omop.note CASCADE;
TRUNCATE TABLE omop.note_nlp CASCADE;
TRUNCATE TABLE omop.specimen CASCADE;
TRUNCATE TABLE omop.fact_relationship CASCADE;
TRUNCATE TABLE omop.location CASCADE;
TRUNCATE TABLE omop.care_site CASCADE;
TRUNCATE TABLE omop.provider CASCADE;
TRUNCATE TABLE omop.payer_plan_period CASCADE;
TRUNCATE TABLE omop.cost CASCADE;
TRUNCATE TABLE omop.drug_era CASCADE;
TRUNCATE TABLE omop.dose_era CASCADE;
TRUNCATE TABLE omop.condition_era CASCADE;
TRUNCATE TABLE omop.episode CASCADE;
TRUNCATE TABLE omop.episode_event CASCADE;

-- Reset sequences
ALTER SEQUENCE IF EXISTS staging.observation_seq RESTART WITH 1;

-- Clear ETL progress log
TRUNCATE TABLE staging.etl_progress;

-- Re-enable foreign key constraints
SET session_replication_role = 'origin';

-- Analyze tables
ANALYZE omop.person;
ANALYZE omop.observation_period;
ANALYZE omop.visit_occurrence;
ANALYZE omop.condition_occurrence;
ANALYZE omop.drug_exposure;
ANALYZE omop.procedure_occurrence;
ANALYZE omop.measurement;
ANALYZE omop.observation;
ANALYZE omop.death;
ANALYZE omop.cost;
EOF

# Display completion message
echo ""
echo "===== OMOP Tables Reset Completed ====="
echo "All OMOP tables have been truncated and are ready for a fresh ETL run."
echo "Date: $(date)"
