#!/usr/bin/env bash
#
# load_omop_vocab.sh
#
# A script to load OMOP vocabulary files into a PostgreSQL database.
# This script:
#   1. Checks if the database and schema exist, creates them if they don't
#   2. Processes vocabulary files using clean_vocab.py
#   3. Drops circular foreign key constraints
#   4. Loads each vocabulary file with automatic delimiter detection
#   5. Re-adds the constraints
#   6. Verifies the load by checking row counts
#
# Requirements:
#   - `pv` installed (for progress bars)
#   - `psql` installed (PostgreSQL client)
#   - Python 3 (for vocabulary file cleaning)
#   - Vocabulary files in the vocabulary directory

set -euo pipefail

###############################################################################
# 0) CHECK REQUIREMENTS
###############################################################################
# Check if pv is installed
if ! command -v pv &> /dev/null; then
  echo "Error: 'pv' command is required but not installed."
  echo "Please install it using your package manager:"
  echo "  Ubuntu/Debian: sudo apt-get install pv"
  echo "  CentOS/RHEL: sudo yum install pv"
  echo "  macOS: brew install pv"
  exit 1
fi

# Check if psql is installed
if ! command -v psql &> /dev/null; then
  echo "Error: 'psql' command is required but not installed."
  echo "Please install PostgreSQL client tools."
  exit 1
fi

###############################################################################
# 1) CONFIGURATION
###############################################################################
DB_HOST="localhost"
DB_PORT="5432"
DB_NAME="ohdsi"
DB_USER="postgres"
export PGPASSWORD="acumenus"

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# Path to the vocabulary files
VOCAB_DIR="/home/acumenus/GitHub/Synthea2OMOP-ETL/vocabulary"
# Path to the processed vocabulary files (will be created if it doesn't exist)
PROCESSED_VOCAB_DIR="/home/acumenus/GitHub/Synthea2OMOP-ETL/vocabulary_processed"

# True if your tab-delimited files have a header row
WITH_HEADER=true

###############################################################################
# 2) CHECK DATABASE AND SCHEMA
###############################################################################
echo "Checking if database $DB_NAME exists..."
if ! psql -h "$DB_HOST" -p "$DB_PORT" -U "$DB_USER" -lqt | cut -d \| -f 1 | grep -qw "$DB_NAME"; then
  echo "Database $DB_NAME does not exist. Creating it..."
  psql -h "$DB_HOST" -p "$DB_PORT" -U "$DB_USER" -c "CREATE DATABASE $DB_NAME;"
  echo "Database $DB_NAME created successfully."
fi

echo "Checking if schema 'omop' exists in database $DB_NAME..."
if ! psql -h "$DB_HOST" -p "$DB_PORT" -U "$DB_USER" -d "$DB_NAME" -c "SELECT schema_name FROM information_schema.schemata WHERE schema_name = 'omop'" | grep -q omop; then
  echo "Schema 'omop' does not exist in database $DB_NAME. Creating it..."
  psql -h "$DB_HOST" -p "$DB_PORT" -U "$DB_USER" -d "$DB_NAME" -c "CREATE SCHEMA omop;"
  echo "Schema 'omop' created successfully."
  
  echo "Creating OMOP CDM tables in schema 'omop'..."
  if [[ -f "$SCRIPT_DIR/../sql/omop_ddl/OMOPCDM_postgresql_5.4_ddl.sql" ]]; then
    psql -h "$DB_HOST" -p "$DB_PORT" -U "$DB_USER" -d "$DB_NAME" -f "$SCRIPT_DIR/../sql/omop_ddl/OMOPCDM_postgresql_5.4_ddl.sql"
    echo "OMOP CDM tables created successfully."
  else
    echo "WARNING: OMOP CDM DDL file not found at $SCRIPT_DIR/../sql/omop_ddl/OMOPCDM_postgresql_5.4_ddl.sql"
    echo "You may need to create the OMOP CDM tables manually."
  fi
fi

###############################################################################
# 3) PREPARE VOCABULARY FILES
###############################################################################
echo "Creating processed vocabulary directory if it doesn't exist..."
mkdir -p "$PROCESSED_VOCAB_DIR"

echo "Checking if clean_vocab.py script exists..."
if [[ -f "$VOCAB_DIR/clean_vocab.py" ]]; then
  echo "Running clean_vocab.py to prepare vocabulary files..."
  python3 "$VOCAB_DIR/clean_vocab.py" "$VOCAB_DIR" "$PROCESSED_VOCAB_DIR"
  echo "Vocabulary files have been processed and saved to $PROCESSED_VOCAB_DIR"
else
  echo "WARNING: clean_vocab.py script not found in $VOCAB_DIR."
  echo "Will attempt to use original vocabulary files."
fi

###############################################################################
# 4) TRUNCATE (OPTIONAL)
###############################################################################
echo "Truncating OMOP vocabulary + dependent tables in one statement..."
psql -h "$DB_HOST" -p "$DB_PORT" -U "$DB_USER" -d "$DB_NAME" <<EOF
TRUNCATE TABLE
  omop.concept,
  omop.vocabulary,
  omop.domain,
  omop.concept_class,
  omop.relationship,
  omop.concept_relationship,
  omop.concept_synonym,
  omop.concept_ancestor,
  omop.drug_strength,
  omop.source_to_concept_map
CASCADE;
EOF

###############################################################################
# 5) DROP CIRCULAR FKs
###############################################################################
echo "Dropping domain->concept circular foreign keys..."

psql -h "$DB_HOST" -p "$DB_PORT" -U "$DB_USER" -d "$DB_NAME" <<EOF
ALTER TABLE omop.domain  DROP CONSTRAINT IF EXISTS fpk_domain_domain_concept_id;
ALTER TABLE omop.concept DROP CONSTRAINT IF EXISTS fpk_concept_domain_id;
EOF

###############################################################################
# 6) HELPER FUNCTION: LOAD WITH TAB DELIMITER + PROGRESS BAR
###############################################################################
load_tab_vocab_file() {
  local table_name="$1"
  local file_name="$2"
  local processed_file_path="$PROCESSED_VOCAB_DIR/$file_name"
  local original_file_path="$VOCAB_DIR/$file_name"
  local file_path=""

  # Check if the file exists in the processed directory
  if [[ -f "$processed_file_path" ]]; then
    file_path="$processed_file_path"
    echo "Using processed file: $processed_file_path"
  # If not, check if it exists in the original directory
  elif [[ -f "$original_file_path" ]]; then
    file_path="$original_file_path"
    echo "Using original file: $original_file_path"
  else
    echo "WARNING: $file_name not found in either $PROCESSED_VOCAB_DIR or $VOCAB_DIR. Skipping $table_name."
    return
  fi

  echo ""
  echo "Loading $file_name into $table_name with progress bar..."

  # Special handling for files that might have long text values
  if [[ "$file_name" == "CONCEPT.csv" || "$file_name" == "CONCEPT_SYNONYM.csv" ]]; then
    echo "Using special handling for $file_name..."
    
    # Create a temporary SQL file for loading the file
    local temp_sql_file=$(mktemp)
    
    # Determine the table column to modify based on the file
    local column_name=""
    if [[ "$file_name" == "CONCEPT.csv" ]]; then
      column_name="concept_name"
    elif [[ "$file_name" == "CONCEPT_SYNONYM.csv" ]]; then
      column_name="concept_synonym_name"
    fi
    
    cat > "$temp_sql_file" << EOF
-- Temporarily alter the column to TEXT type to handle any length
ALTER TABLE $table_name ALTER COLUMN $column_name TYPE TEXT;

-- Load the data
\\copy $table_name FROM '$file_path' WITH (FORMAT csv, DELIMITER E'\\t', QUOTE '"', ESCAPE '\\', NULL '', HEADER);

-- Convert back to varchar(2000) with truncation if needed
ALTER TABLE $table_name ALTER COLUMN $column_name TYPE varchar(2000) USING substring($column_name, 1, 2000);
EOF
    
    # Execute the SQL file
    psql -h "$DB_HOST" -p "$DB_PORT" -U "$DB_USER" -d "$DB_NAME" -f "$temp_sql_file"
    
    # Clean up
    rm "$temp_sql_file"
  else
    # Standard handling for other files
    # We'll use `FORMAT csv` but specify tab as the delimiter. This is fine in Postgres.
    # Check if the file is tab-delimited or comma-delimited
    local first_line=$(head -n 1 "$file_path")
    local delimiter="E'\t'"
    
    if [[ "$first_line" == *","* && "$first_line" != *"$(echo -e '\t')"* ]]; then
      echo "Detected comma-delimited file. Using comma as delimiter."
      delimiter="','"
    else
      echo "Detected tab-delimited file. Using tab as delimiter."
    fi
    
    local copy_opts="FORMAT csv, DELIMITER $delimiter, QUOTE '\"', ESCAPE '\\', NULL ''"
    if [ "$WITH_HEADER" = true ]; then
      copy_opts="$copy_opts, HEADER"
    fi

    # Use pv for progress, pipe into \copy
    pv "$file_path" | \
    psql -h "$DB_HOST" -p "$DB_PORT" -U "$DB_USER" -d "$DB_NAME" \
         -c "\copy $table_name FROM STDIN ($copy_opts);"
  fi
}

###############################################################################
# 7) LOAD EACH VOCAB FILE
###############################################################################
load_tab_vocab_file "omop.domain"               "DOMAIN.csv"
load_tab_vocab_file "omop.vocabulary"           "VOCABULARY.csv"
load_tab_vocab_file "omop.concept_class"        "CONCEPT_CLASS.csv"
load_tab_vocab_file "omop.relationship"         "RELATIONSHIP.csv"
load_tab_vocab_file "omop.concept"              "CONCEPT.csv"
load_tab_vocab_file "omop.concept_relationship" "CONCEPT_RELATIONSHIP.csv"
load_tab_vocab_file "omop.concept_synonym"      "CONCEPT_SYNONYM.csv"
load_tab_vocab_file "omop.concept_ancestor"     "CONCEPT_ANCESTOR.csv"
load_tab_vocab_file "omop.drug_strength"        "DRUG_STRENGTH.csv"
load_tab_vocab_file "omop.source_to_concept_map" "SOURCE_TO_CONCEPT_MAP.csv"

###############################################################################
# 8) RE-ADD FKs
###############################################################################
echo ""
echo "Recreating domain->concept foreign keys..."

psql -h "$DB_HOST" -p "$DB_PORT" -U "$DB_USER" -d "$DB_NAME" <<EOF

-- domain.domain_concept_id -> concept.concept_id
ALTER TABLE omop.domain
  ADD CONSTRAINT fpk_domain_domain_concept_id
  FOREIGN KEY (domain_concept_id)
  REFERENCES omop.concept(concept_id);

-- concept.domain_id -> domain.domain_id
ALTER TABLE omop.concept
  ADD CONSTRAINT fpk_concept_domain_id
  FOREIGN KEY (domain_id)
  REFERENCES omop.domain(domain_id);
EOF

echo ""
echo "Vocabulary load complete! The circular FKs have been re-added successfully."

###############################################################################
# 9) VERIFY LOAD
###############################################################################
echo ""
echo "Verifying vocabulary load by checking row counts..."

psql -h "$DB_HOST" -p "$DB_PORT" -U "$DB_USER" -d "$DB_NAME" <<EOF
SELECT 'concept' as table_name, COUNT(*) as row_count FROM omop.concept
UNION ALL
SELECT 'vocabulary', COUNT(*) FROM omop.vocabulary
UNION ALL
SELECT 'domain', COUNT(*) FROM omop.domain
UNION ALL
SELECT 'concept_class', COUNT(*) FROM omop.concept_class
UNION ALL
SELECT 'relationship', COUNT(*) FROM omop.relationship
UNION ALL
SELECT 'concept_relationship', COUNT(*) FROM omop.concept_relationship
UNION ALL
SELECT 'concept_synonym', COUNT(*) FROM omop.concept_synonym
UNION ALL
SELECT 'concept_ancestor', COUNT(*) FROM omop.concept_ancestor
UNION ALL
SELECT 'drug_strength', COUNT(*) FROM omop.drug_strength
UNION ALL
SELECT 'source_to_concept_map', COUNT(*) FROM omop.source_to_concept_map
ORDER BY table_name;
EOF

echo ""
echo "Vocabulary load and verification complete!"
