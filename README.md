# OMOP ETL Project for Synthea Data

Welcome to our **OMOP ETL** repository! This project demonstrates **end-to-end extraction, transformation, and loading** of synthetic healthcare data from [Synthea™](https://github.com/synthetichealth/synthea) into the **OMOP Common Data Model (CDM)** (version 5.4). 

## Overview

The [Synthea™ Patient Generator](https://github.com/synthetichealth/synthea) is an open-source synthetic patient generator that models the medical history of artificial patients, created by The MITRE Corporation. This project shows how to transform Synthea's output into the OMOP Common Data Model format.

## ETL Process Overview

Our ETL process follows these main steps:

1. **Initialize OMOP Schema**
   - Run the OMOP CDM DDL scripts to create the core tables
   - Set up the vocabulary tables structure

2. **Load OMOP Vocabulary**
   - Use `load_omop_vocab_tab.sh` to load vocabulary files
   - Process CONCEPT, VOCABULARY, DOMAIN, and other vocabulary tables
   - Handle circular foreign key dependencies

3. **Generate Synthea Data**
   - Use Synthea to generate synthetic patient data
   - Output includes CSV files for patients, encounters, conditions, etc.

4. **Load Raw Synthea Data**
   - Use `load_synthea_staging.sh` to load Synthea CSVs
   - Creates tables in the `population` schema
   - All columns initially loaded as TEXT type

5. **Type Conversion**
   - Convert raw TEXT columns to appropriate data types
   - Handle UUIDs, timestamps, numerics, and enums
   - Validate data during conversion
   - Store typed data in `population.*_typed` tables

6. **Staging for OMOP**
   - Create staging schema and mapping tables
   - Set up ID generation sequences
   - Prepare lookup tables for standard concepts

7. **OMOP ETL**
   - Transform typed Synthea data into OMOP format
   - Generate surrogate keys
   - Map source codes to standard concepts
   - Load each OMOP domain table

## Project Structure

```
├── sql/
│   ├── omop_ddl/              # OMOP CDM table definitions
│   ├── vocabulary_load/       # Scripts for loading OMOP vocabulary
│   ├── synthea_typing/        # Convert raw Synthea data to proper types
│   ├── staging/              # Create staging schema and mapping tables
│   └── etl/                  # OMOP domain-specific ETL scripts
├── scripts/
│   ├── load_omop_vocab_tab.sh # Vocabulary loading script
│   └── load_synthea_staging.sh # Raw Synthea data loading script
└── README.md
```

## Detailed ETL Steps

### 1. Initialize OMOP Schema

First, create the OMOP CDM tables using the standard DDL scripts. This sets up the target structure for our ETL process.

### 2. Load OMOP Vocabulary

Before loading the vocabulary files, we need to handle a common preprocessing issue: the presence of problematic double quotes in the CSV files that can interfere with the import process. We provide a preprocessing script `remove_vocab_quotes.sh` to handle this:

```bash
#!/bin/bash

# Color definitions for better visibility
GREEN='\033[0;32m'
BLUE='\033[0;34m'
RED='\033[0;31m'
NC='\033[0m' # No Color

# Check if pv is installed
if ! command -v pv &> /dev/null; then
    echo -e "${RED}Error: 'pv' is not installed. Please install it first.${NC}"
    exit 1
fi

# Process files with progress tracking
for file in *.csv; do
    echo -e "\n${BLUE}Processing: $file${NC}"
    size=$(wc -c < "$file")
    cat "$file" | pv -s "$size" | tr -d '"' > "$temp_file"
    mv "$temp_file" "$file"
done
```

This preprocessing script:
- Checks for the required `pv` utility for progress tracking
- Shows all CSV files that will be processed
- Requests confirmation before proceeding
- Removes problematic double quotes
- Provides visual progress feedback
- Maintains the original file names

After preprocessing, the `load_omop_vocab_tab.sh` script handles loading the vocabulary files:
- Temporarily drops circular foreign keys
- Loads each vocabulary file using COPY
- Shows progress with `pv`
- Restores foreign key constraints

### 3. Load Raw Synthea Data

The `load_synthea_staging.sh` script:
- Creates tables based on CSV headers
- Loads all columns as TEXT initially
- Uses COPY for efficient bulk loading
- Places tables in the `population` schema

### 4. Type Conversion

SQL scripts in `sql/synthea_typing/` handle converting raw TEXT data to proper types:
- UUIDs for IDs
- TIMESTAMP for dates
- NUMERIC for measurements
- Custom ENUMs for coded values
- Validation during conversion

### 5. Staging Schema

Create staging schema with:
- ID mapping tables
- Code lookup tables
- Sequences for surrogate keys
- Intermediate tables for complex transformations

### 6. OMOP Domain Loading

Transform and load each OMOP domain:
- person
- observation_period
- visit_occurrence
- condition_occurrence
- drug_exposure
- measurement
- observation
- procedure_occurrence
- device_exposure
- death
- cost / payer information

## Code Example: Loading Raw Synthea Data

The `load_synthea_staging.sh` script demonstrates our approach to initial data loading:

```bash
# Configuration
DB_HOST="192.168.1.155"
DB_PORT="5432"
DB_NAME="synthea"
DB_USER="postgres"
DB_SCHEMA="population"

# Process each CSV file
for csv_file in *.csv; do
  # Create table with TEXT columns from CSV header
  header_line="$(head -n 1 "$csv_file")"
  # ... create table logic ...
  
  # Load data using COPY
  psql -c "\copy \"$DB_SCHEMA\".\"$table_name\" 
          FROM '${csv_file}' 
          CSV HEADER 
          DELIMITER ',' 
          QUOTE '\"' 
          ESCAPE '\"';"
done
```

## References

- [Synthea GitHub](https://github.com/synthetichealth/synthea)
- [OMOP CDM Documentation](https://ohdsi.github.io/CommonDataModel/)
- [OHDSI Documentation](https://ohdsi.github.io/TheBookOfOhdsi/)

## Contributing

We welcome contributions! Please feel free to submit issues or pull requests if you have improvements or questions about the ETL process.
