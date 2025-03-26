#!/bin/bash
# Script to execute PostgreSQL database maintenance operations
# Created: 2025-03-17

# Set database connection parameters
DB_HOST=${DB_HOST:-localhost}
DB_PORT=${DB_PORT:-5432}
DB_NAME=${DB_NAME:-ohdsi}
DB_USER=${DB_USER:-postgres}
DB_PASSWORD=${DB_PASSWORD:-acumenus}

# Create connection string
CONN_STRING="postgresql://${DB_USER}:${DB_PASSWORD}@${DB_HOST}:${DB_PORT}/${DB_NAME}"

# Function to execute a SQL script
execute_script() {
    echo "Executing $1..."
    psql "${CONN_STRING}" -f "$1"
    echo "Completed $1"
    echo "----------------------------------------"
}

# Function to pause execution
pause() {
    read -p "Press Enter to continue..."
}

# Display header
echo "=========================================="
echo "PostgreSQL Database Maintenance Operations"
echo "=========================================="
echo "Database: ${DB_NAME} on ${DB_HOST}:${DB_PORT}"
echo "User: ${DB_USER}"
echo "Time: $(date)"
echo "=========================================="

# Step 1: Terminate idle transactions
echo "Step 1: Terminate idle transactions"
execute_script "sql/db_maintenance/terminate_idle_transactions.sql"
pause

# Step 2: Resolve blocking chain
echo "Step 2: Resolve blocking chain"
execute_script "sql/db_maintenance/resolve_blocking_chain.sql"
echo "Review the output above and edit the script to uncomment the appropriate commands to terminate blocking processes."
pause

# Step 3: Cancel ETL process if needed
echo "Step 3: Cancel ETL process"
execute_script "sql/db_maintenance/cancel_etl_process.sql"
echo "Review the output above and edit the script to uncomment the appropriate commands to cancel or terminate the ETL process if needed."
pause

# Step 4: Monitor recovery
echo "Step 4: Monitor recovery"
execute_script "sql/db_maintenance/monitor_recovery.sql"
echo "Review the output above to check if the system is recovering."

# Step 5: Optimize PostgreSQL configuration (optional)
echo "Step 5: Optimize PostgreSQL configuration"
echo "This step will modify PostgreSQL configuration parameters to improve performance."
echo "Note: Some settings may require a server restart to take effect."
read -p "Do you want to optimize PostgreSQL configuration? (y/n): " optimize_config
if [ "$optimize_config" = "y" ]; then
    execute_script "sql/db_maintenance/optimize_postgres_config.sql"
    echo "PostgreSQL configuration has been optimized."
    echo "You may need to restart the PostgreSQL server for some settings to take effect."
else
    echo "Skipping PostgreSQL configuration optimization."
fi

# Final message
echo "=========================================="
echo "Database maintenance operations completed."
echo "You may need to run the monitor_recovery.sql script multiple times to track progress."
echo "=========================================="
