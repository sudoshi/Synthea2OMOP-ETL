#!/usr/bin/env bash
#
# load_synthea_staging.sh
#
# A script to drop any existing table, recreate it with TEXT columns
# from the CSV header, and bulk load via \copy in a single-line command.

set -euo pipefail

##############################################################################
# 1) CONFIGURATION
##############################################################################
DB_HOST="192.168.1.155"       # Adjust if needed
DB_PORT="5432"                # Default Postgres port
DB_NAME="synthea"             # Target database
DB_USER="postgres"            # Postgres user
DB_SCHEMA="population"        # Schema to store staging tables

# Hard-coded password:
export PGPASSWORD="acumenus"

# Directory of this script (and where CSVs are expected to be)
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

echo "Loading CSV files from directory: $SCRIPT_DIR"
echo "Using database: $DB_NAME at host: $DB_HOST (schema: $DB_SCHEMA)"

# Ensure the schema exists
psql -h "$DB_HOST" -p "$DB_PORT" -U "$DB_USER" -d "$DB_NAME" \
     -c "CREATE SCHEMA IF NOT EXISTS $DB_SCHEMA;"

##############################################################################
# 2) PROCESS EACH *.csv FILE
##############################################################################
for csv_file in *.csv; do

  # If no CSVs are found, the glob remains '*.csv'
  if [[ "$csv_file" == '*.csv' ]]; then
    echo "No CSV files found in $SCRIPT_DIR"
    break
  fi

  # Derive table name from file name (strip .csv)
  table_name="${csv_file%.csv}"

  echo ""
  echo "--------------------------------------------------------------"
  echo "Processing file: $csv_file"
  echo "Will drop and recreate table: $DB_SCHEMA.$table_name"

  # 1) Drop the table if it exists
  psql -h "$DB_HOST" -p "$DB_PORT" -U "$DB_USER" -d "$DB_NAME" \
       -c "DROP TABLE IF EXISTS \"$DB_SCHEMA\".\"$table_name\" CASCADE;"

  # 2) Read the first (header) line of the CSV
  header_line="$(head -n 1 "$csv_file")"

  # 3) Split header on commas into an array of column names
  IFS=',' read -r -a columns <<< "$header_line"

  # 4) Build a CREATE TABLE statement with all TEXT columns
  create_sql="CREATE TABLE \"$DB_SCHEMA\".\"$table_name\" ("
  for col in "${columns[@]}"; do
    # Remove any surrounding quotes
    trimmed_col="$(echo "$col" | sed -E 's/^\"|\"$//g')"
    # Escape internal double-quotes
    escaped_col="${trimmed_col//\"/\"\"}"
    # Quote the identifier
    create_sql+="\"$escaped_col\" TEXT, "
  done
  # Remove trailing comma, close parenthesis
  create_sql="${create_sql%, } );"

  # 5) Create the table
  psql -h "$DB_HOST" -p "$DB_PORT" -U "$DB_USER" -d "$DB_NAME" \
       -c "$create_sql"

  # 6) Load the data using a single-line \copy command
  echo "Loading data into $DB_SCHEMA.$table_name ..."
  psql -h "$DB_HOST" -p "$DB_PORT" -U "$DB_USER" -d "$DB_NAME" \
       -c "\copy \"$DB_SCHEMA\".\"$table_name\" FROM '${SCRIPT_DIR}/${csv_file}' CSV HEADER DELIMITER ',' QUOTE '\"' ESCAPE '\"';"

  echo "Finished loading $csv_file -> $DB_SCHEMA.$table_name"
done

echo ""
echo "All CSV files have been processed!"
echo "Data is stored in staging tables under schema: $DB_SCHEMA"
echo "All columns are TEXT. Transform/cast as needed."
