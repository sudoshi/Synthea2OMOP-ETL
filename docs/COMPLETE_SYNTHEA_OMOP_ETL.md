# Complete Synthea to OMOP ETL Pipeline

## Overview

This document explains the complete ETL process for converting Synthea output files into OMOP CDM format. The process consists of four major steps:

1. **Preprocessing** - Fix malformed CSV files
2. **Staging Load** - Load data into PostgreSQL staging tables (as TEXT)
3. **Type Transformation** - Convert TEXT fields to properly typed tables
4. **OMOP Mapping** - Transform typed data to OMOP CDM format

## The Problem

Synthea generates data with three key challenges:

1. **Malformed CSV files**: Headers have commas, but data rows don't have proper delimiters
2. **Data Type Ambiguity**: All data is initially stored as TEXT
3. **Schema Mismatch**: Synthea's schema doesn't match OMOP CDM requirements

## The Complete Pipeline Solution

We've developed a comprehensive pipeline that addresses all these challenges:

```bash
# Run the complete pipeline
bash/run_complete_synthea_etl.sh

# With custom options
bash/run_complete_synthea_etl.sh --input-dir ./synthea-output \
                                --processed-dir ./synthea-processed \
                                --force-load \
                                --debug
```

## Pipeline Steps Explained

### 1. Preprocessing

The preprocessing script (`python/preprocess_synthea_csv.py`) fixes the malformed CSV files:

- Reads the header row to identify columns
- Processes data rows to separate concatenated values
- Uses various heuristics to identify field boundaries
- Writes properly formatted CSV files to the output directory

### 2. Staging Load

The staging load script (`scripts/load_synthea_staging.sh`) loads the preprocessed CSV files into the database:

- Creates staging tables with TEXT columns
- Uses PostgreSQL's COPY command for efficient bulk loading
- Provides progress indicators and error handling

### 3. Type Transformation

The type transformation script (`sql/synthea_typing/synthea-typedtables-transformation.sql`) converts the staging TEXT data to properly typed tables:

- Creates typed tables with appropriate PostgreSQL data types
- Uses CASE expressions to safely convert TEXT to proper types
- Handles UUIDs, timestamps, numerics, and custom enum types
- Moves data to a "population" schema with typed tables

This critical step ensures data integrity before mapping to OMOP CDM by:
- Validating data types (e.g., dates, numbers)
- Ensuring referential integrity through UUID conversions
- Applying domain-specific constraints (e.g., enum types)

### 4. OMOP Mapping

The OMOP ETL scripts transform the typed data to OMOP CDM format:

- Maps source concepts to standard concepts
- Restructures data to conform to OMOP CDM tables
- Applies business rules for proper OMOP representation
- Creates relationships between OMOP entities

## Usage Options

The complete pipeline script supports various options:

- `--input-dir DIR`: Directory with Synthea output files (default: ./synthea-output)
- `--processed-dir DIR`: Directory for preprocessed files (default: ./synthea-processed)
- `--overwrite-processed`: Overwrite existing preprocessed files
- `--debug`: Enable debug logging for more detailed output
- `--force-load`: Force overwrite of existing database tables
- `--max-workers N`: Maximum number of parallel workers (default: 4)

### Skip Options

You can skip specific steps if needed:

- `--skip-preprocessing`: Skip the CSV preprocessing step
- `--skip-staging-load`: Skip loading data into staging tables
- `--skip-typing`: Skip transforming staging to typed tables
- `--skip-omop-transform`: Skip transforming typed data to OMOP CDM

## Troubleshooting

### Common Issues

1. **Missing Database Schemas**: 
   - Ensure you have created the required schemas: staging, population, omop

2. **Permission Issues**:
   - Make sure your database user has the necessary permissions

3. **Type Conversion Errors**:
   - Check the typing log if the type transformation step fails
   - May be due to unexpected data formats in the source files

### Debug Mode

Enable debug mode for more detailed logging:

```bash
bash/run_complete_synthea_etl.sh --debug
```

## Performance Considerations

For large Synthea datasets:

- The preprocessing step adds overhead but is necessary for data integrity
- Consider using a machine with more memory and CPU cores
- Progress logging shows updates during processing
- The typing transformation can be time-consuming for large datasets

## Next Steps

After running the complete pipeline:

1. Verify the data in the OMOP schema
2. Run Achilles for data quality assessment
3. Use the OMOP CDM data with OHDSI tools
