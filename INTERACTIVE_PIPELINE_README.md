# Interactive Synthea to OMOP ETL Pipeline

This interactive pipeline provides a user-friendly way to convert Synthea synthetic patient data to the OMOP Common Data Model (CDM) format. It guides users through the entire process with clear prompts, validation checks, and detailed progress tracking.

## Features

- **Interactive Mode**: Guides users through the ETL process with clear prompts and instructions
- **Environment Validation**: Checks database connection, schemas, tables, vocabulary files, and Synthea files
- **Checkpoint System**: Remembers completed steps and allows resuming from where you left off
- **Detailed Progress Tracking**: Shows real-time progress with row counts and completion percentages
- **Comprehensive Error Handling**: Provides clear error messages with suggestions for fixing issues
- **Validation Reports**: Validates data before and after each step with detailed reports
- **Colorful Output**: Uses color-coded output for better readability
- **Automatic Dependency Management**: Detects missing Python packages and offers to install them

## Prerequisites

- Python 3.6+
- PostgreSQL database
- OMOP CDM vocabulary files
- Synthea output files

### Dependencies

The script will automatically check for required and optional dependencies and offer to install them if they're missing:

- **Required Dependencies**:
  - `psycopg2` for PostgreSQL database access

- **Optional Dependencies**:
  - `colorama` for colored output
  - `tqdm` for progress bars

## Usage

### Basic Usage

```bash
./interactive_unified_pipeline.py
```

This will run the pipeline in interactive mode, guiding you through each step.

### Command-line Options

```bash
./interactive_unified_pipeline.py --help
```

#### Interactive Mode Options

- `--interactive`: Run in interactive mode with user prompts (default: True)
- `--non-interactive`: Run in non-interactive mode

#### Database Initialization Options

- `--skip-init`: Skip database initialization
- `--drop-existing`: Drop existing schemas before initialization

#### Vocabulary Options

- `--skip-vocab`: Skip vocabulary loading
- `--vocab-dir DIR`: Directory containing vocabulary files (default: ./vocabulary)

#### ETL Options

- `--skip-etl`: Skip ETL process
- `--synthea-dir DIR`: Directory containing Synthea output files (default: ./synthea-output)
- `--max-workers N`: Maximum number of parallel workers for ETL (default: 4)
- `--skip-optimization`: Skip PostgreSQL optimization
- `--skip-validation`: Skip validation steps

#### Resume Options

- `--resume`: Resume from last checkpoint
- `--force`: Force execution even if validation fails

#### General Options

- `--debug`: Enable debug logging
- `--track-progress`: Enable progress tracking for ETL process
- `--monitor`: Launch progress monitoring in a separate terminal

## Step-by-Step Guide

### 1. Environment Validation

The pipeline first validates your environment:

- Checks database connection and credentials
- Verifies required schemas and tables
- Validates vocabulary files
- Checks Synthea output files

If any issues are found, the pipeline will provide guidance on how to fix them.

### 2. Database Initialization

Initializes the database with the OMOP CDM schema:

- Creates required schemas (omop, staging, vocabulary)
- Creates OMOP tables
- Sets up constraints and indices

### 3. Vocabulary Loading

Loads vocabulary data into the database:

- Validates vocabulary files
- Loads concept data
- Loads relationships and mappings
- Verifies loaded data

### 4. ETL Process

Transforms Synthea data to OMOP format:

- Processes patients to person table
- Processes encounters to visit_occurrence table
- Processes conditions to condition_occurrence table
- Processes medications to drug_exposure table
- Processes procedures to procedure_occurrence table
- Processes observations to measurement and observation tables
- Creates observation periods
- Maps source concepts to standard concepts

### 5. Summary Report

Provides a detailed summary of the ETL process:

- Counts of records in each table
- Comparison of source and destination counts
- Transformation statistics
- Execution time

## Troubleshooting

### Database Connection Issues

If you encounter database connection issues:

1. Verify PostgreSQL is running
2. Check your database credentials
3. Ensure the database exists
4. Check network connectivity

### Missing Vocabulary Files

If vocabulary files are missing:

1. Visit [Athena](https://athena.ohdsi.org/) to download vocabulary files
2. Extract the files to your vocabulary directory
3. Ensure file permissions allow reading

### ETL Process Failures

If the ETL process fails:

1. Check the error log for details
2. Verify Synthea files are valid
3. Ensure database has sufficient space
4. Try running with `--debug` for more detailed logs

## Examples

### Basic Interactive Run

```bash
./interactive_unified_pipeline.py
```

### Resume a Previous Run

```bash
./interactive_unified_pipeline.py --resume
```

### Skip Database Initialization

```bash
./interactive_unified_pipeline.py --skip-init
```

### Non-interactive Run with Custom Directories

```bash
./interactive_unified_pipeline.py --non-interactive --vocab-dir /path/to/vocabulary --synthea-dir /path/to/synthea-output
```

### Run with Progress Monitoring

```bash
./interactive_unified_pipeline.py --track-progress --monitor
```
