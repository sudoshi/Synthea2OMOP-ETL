# Optimized Synthea to OMOP ETL

This document describes the optimized ETL (Extract, Transform, Load) process for converting Synthea synthetic patient data directly to OMOP CDM format.

## Overview

The optimized ETL process provides a direct, efficient pathway from Synthea CSV files to OMOP CDM tables, bypassing the need for intermediate staging tables. This approach offers several advantages:

1. **Reduced Processing Time**: Direct transformation eliminates multiple data copies between staging tables
2. **Lower Storage Requirements**: No need for duplicate data in staging tables
3. **Parallel Processing**: Independent ETL steps run concurrently
4. **Optimized SQL Operations**: Bulk loading and efficient SQL transformations
5. **Simplified Workflow**: Fewer steps and dependencies in the ETL pipeline

## Prerequisites

- Python 3.6+
- PostgreSQL database with OMOP CDM 5.4 schema
- Synthea-generated CSV files
- Required Python packages: `psycopg2`, `psycopg2-binary`

## Files

- `optimized_synthea_to_omop.py`: Main Python script implementing the optimized ETL process
- `run_optimized_import.sh`: Shell script to run the ETL process with configurable parameters

## Usage

```bash
./run_optimized_import.sh [options]
```

### Options

- `--synthea-dir <path>`: Directory containing Synthea output files (default: ./synthea-output)
- `--max-workers <num>`: Maximum number of parallel workers (default: 4)
- `--skip-optimization`: Skip PostgreSQL optimization
- `--skip-validation`: Skip validation steps
- `--debug`: Enable debug logging

### Example

```bash
./run_optimized_import.sh --synthea-dir ./my-synthea-data --max-workers 8 --debug
```

## ETL Process

The optimized ETL process follows these steps:

1. **Initialize Database Connection**: Establishes a connection pool to the PostgreSQL database
2. **Optimize PostgreSQL Configuration**: Sets optimal PostgreSQL parameters for ETL operations
3. **Identify CSV Files**: Scans the Synthea directory for required CSV files
4. **Reset OMOP Tables**: Truncates existing OMOP tables to prepare for new data
5. **Run Parallel ETL**:
   - Process patients (sequential, as other steps depend on it)
   - Process encounters (sequential, as clinical data depends on it)
   - Process clinical data in parallel (conditions, medications, procedures, observations)
   - Create observation periods
   - Map concepts
   - Analyze tables
   - Validate data
6. **Log Completion**: Records ETL completion status

## Data Flow

```
Synthea CSV Files → Temporary Tables → OMOP CDM Tables
```

For each data type:
1. CSV header analysis to determine column types
2. Creation of temporary table with appropriate column types
3. Bulk loading of CSV data using PostgreSQL COPY command
4. Transformation and insertion into OMOP tables
5. Validation and logging

## Performance Considerations

- **Parallel Processing**: The script uses Python's `concurrent.futures` to run independent ETL steps in parallel
- **Connection Pooling**: Database connections are managed through a connection pool
- **Bulk Loading**: CSV data is loaded using PostgreSQL's COPY command for maximum efficiency
- **PostgreSQL Optimization**: Database parameters are tuned for ETL performance
- **Memory Management**: Temporary tables are used to minimize memory usage

## Troubleshooting

- Check the log file in the `logs` directory for detailed error messages
- Ensure the database connection parameters are correct in the `.env` file
- Verify that the Synthea CSV files have the expected format and column names
- If encountering memory issues, reduce the `max-workers` parameter

## Customization

The ETL process can be customized by modifying the following:

- Database connection parameters in the `.env` file
- PostgreSQL optimization parameters in the `optimize_postgres_config` function
- Concept mapping logic in the `map_concepts` function
- Validation criteria in the `validate_data` function
