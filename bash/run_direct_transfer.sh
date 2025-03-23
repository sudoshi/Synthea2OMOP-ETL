#!/bin/bash
# Script to execute the direct transfer SQL

# Set database connection parameters
DB_HOST="localhost"
DB_USER="postgres"
DB_NAME="ohdsi"
DB_PASSWORD="acumenus"

# SQL script path
SQL_FILE="sql/etl/direct_transfer.sql"

# Display header
echo "===== Starting Direct Transfer Process ====="
echo "Date: $(date)"
echo ""

# Execute the SQL script with timing
echo "Executing direct transfer SQL..."
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
echo "===== Transfer Process Completed ====="
echo "Total duration: ${hours}h ${minutes}m ${seconds}s"
echo "Date: $(date)"
