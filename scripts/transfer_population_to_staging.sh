#!/bin/bash
# transfer_population_to_staging.sh
# Transfers data from population schema to staging schema using batch processing
# This script is designed to be run after Step 3 (typing) and before Step 4 (OMOP ETL)

set -e

# Load environment variables
if [ -f .env ]; then
    source .env
fi

# Default database connection parameters
DB_HOST=${DB_HOST:-localhost}
DB_PORT=${DB_PORT:-5432}
DB_NAME=${DB_NAME:-synthea}
DB_USER=${DB_USER:-postgres}
DB_PASSWORD=${DB_PASSWORD:-postgres}

# Function to display usage information
usage() {
    echo "Usage: $0 [options]"
    echo "Options:"
    echo "  -h, --host HOST       Database host (default: $DB_HOST)"
    echo "  -p, --port PORT       Database port (default: $DB_PORT)"
    echo "  -d, --dbname DBNAME   Database name (default: $DB_NAME)"
    echo "  -u, --user USER       Database user (default: $DB_USER)"
    echo "  --password PASSWORD   Database password (default: $DB_PASSWORD)"
    echo "  --help                Display this help message"
    exit 1
}

# Parse command line arguments
while [[ $# -gt 0 ]]; do
    key="$1"
    case $key in
        -h|--host)
            DB_HOST="$2"
            shift 2
            ;;
        -p|--port)
            DB_PORT="$2"
            shift 2
            ;;
        -d|--dbname)
            DB_NAME="$2"
            shift 2
            ;;
        -u|--user)
            DB_USER="$2"
            shift 2
            ;;
        --password)
            DB_PASSWORD="$2"
            shift 2
            ;;
        --help)
            usage
            ;;
        *)
            echo "Unknown option: $1"
            usage
            ;;
    esac
done

# Export database connection parameters for psql
export PGHOST=$DB_HOST
export PGPORT=$DB_PORT
export PGDATABASE=$DB_NAME
export PGUSER=$DB_USER
export PGPASSWORD=$DB_PASSWORD

echo "Starting transfer from population schema to staging schema..."
echo "Database: $DB_NAME on $DB_HOST:$DB_PORT"

# Create staging schema if it doesn't exist
psql -c "CREATE SCHEMA IF NOT EXISTS staging;"

# Create etl_progress table if it doesn't exist
psql -c "
CREATE TABLE IF NOT EXISTS staging.etl_progress (
    step_name varchar(100) PRIMARY KEY,
    status varchar(20),
    started_at timestamp DEFAULT CURRENT_TIMESTAMP,
    completed_at timestamp,
    rows_processed bigint DEFAULT 0,
    error_message text
);"

# Record start time
start_time=$(date +%s)

# Log start of transfer
psql -c "
INSERT INTO staging.etl_progress (step_name, status)
VALUES ('population_to_staging_transfer', 'in_progress')
ON CONFLICT (step_name) 
DO UPDATE SET started_at = CURRENT_TIMESTAMP, status = 'in_progress', rows_processed = 0, error_message = NULL;"

# Execute the transfer SQL script
echo "Executing transfer SQL script..."
psql -f "$(dirname "$0")/../sql/etl/transfer_population_to_staging.sql"

# Check if the script executed successfully
if [ $? -eq 0 ]; then
    # Record end time and calculate duration
    end_time=$(date +%s)
    duration=$((end_time - start_time))
    
    # Format duration as hours:minutes:seconds
    hours=$((duration / 3600))
    minutes=$(( (duration % 3600) / 60 ))
    seconds=$((duration % 60))
    
    # Log completion
    psql -c "
    UPDATE staging.etl_progress 
    SET completed_at = CURRENT_TIMESTAMP, 
        status = 'completed',
        rows_processed = (
            SELECT SUM(n_live_tup) 
            FROM pg_stat_user_tables 
            WHERE schemaname = 'staging' AND relname LIKE '%_raw'
        )
    WHERE step_name = 'population_to_staging_transfer';"
    
    echo "Transfer completed successfully in ${hours}h ${minutes}m ${seconds}s"
else
    # Log error
    psql -c "
    UPDATE staging.etl_progress 
    SET status = 'error', 
        error_message = 'Script execution failed'
    WHERE step_name = 'population_to_staging_transfer';"
    
    echo "Transfer failed. Check the logs for details."
    exit 1
fi

# Display table counts
echo "Table counts in staging schema:"
psql -c "
SELECT relname AS table_name, n_live_tup AS row_count
FROM pg_stat_user_tables
WHERE schemaname = 'staging' AND relname LIKE '%_raw'
ORDER BY n_live_tup DESC;"

echo "Transfer from population to staging schema completed."
