#!/usr/bin/env bash
#
# load_omop_vocab_tab.sh
#
# A script to load OMOP vocab files that are definitely *tab-delimited*.
# Drops the circular domain->concept FKs, truncates, loads each file with tab,
# then re-adds the constraints. Shows progress bars via `pv`.
#
# Requirements:
#   - `pv` installed
#   - `psql` installed
#   - Vocab files in same directory, each tab-delimited with a header row

set -euo pipefail

###############################################################################
# 1) CONFIGURATION
###############################################################################
DB_HOST="localhost"
DB_PORT="5432"
DB_NAME="ohdsi"
DB_USER="postgres"
export PGPASSWORD="acumenus"

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
VOCAB_DIR="$SCRIPT_DIR"

# True if your tab-delimited files have a header row
WITH_HEADER=true

###############################################################################
# 2) TRUNCATE (OPTIONAL)
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
# 3) DROP CIRCULAR FKs
###############################################################################
echo "Dropping domain->concept circular foreign keys..."

psql -h "$DB_HOST" -p "$DB_PORT" -U "$DB_USER" -d "$DB_NAME" <<EOF
ALTER TABLE omop.domain  DROP CONSTRAINT IF EXISTS fpk_domain_domain_concept_id;
ALTER TABLE omop.concept DROP CONSTRAINT IF EXISTS fpk_concept_domain_id;
EOF

###############################################################################
# 4) HELPER FUNCTION: LOAD WITH TAB DELIMITER + PROGRESS BAR
###############################################################################
load_tab_vocab_file() {
  local table_name="$1"
  local file_name="$2"
  local file_path="$VOCAB_DIR/$file_name"

  if [[ ! -f "$file_path" ]]; then
    echo "WARNING: $file_name not found in $VOCAB_DIR. Skipping $table_name."
    return
  fi

  echo ""
  echo "Loading $file_name into $table_name (tab-delimited) with progress bar..."

  # We'll use `FORMAT csv` but specify tab as the delimiter. This is fine in Postgres.
local copy_opts="FORMAT csv, DELIMITER E'\t', QUOTE '\"', ESCAPE '\\'"
  if [ "$WITH_HEADER" = true ]; then
    copy_opts="$copy_opts, HEADER"
  fi

  # Use pv for progress, pipe into \copy
  pv "$file_path" | \
  psql -h "$DB_HOST" -p "$DB_PORT" -U "$DB_USER" -d "$DB_NAME" \
       -c "\copy $table_name FROM STDIN ($copy_opts);"
}

###############################################################################
# 5) LOAD EACH VOCAB FILE
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
# 6) RE-ADD FKs
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
