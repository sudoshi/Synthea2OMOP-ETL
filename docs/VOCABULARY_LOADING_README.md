# OMOP CDM and Vocabulary Loading Process

This document describes the optimized process for initializing the OMOP Common Data Model (CDM) database and loading vocabulary files.

## Overview

The OMOP CDM and vocabulary loading process consists of several steps:

1. **Database Initialization**: Create schemas and tables for the OMOP CDM
2. **Vocabulary Processing**: Process vocabulary files to ensure compatibility with PostgreSQL
3. **Vocabulary Loading**: Load processed vocabulary files into the database
4. **ETL Process**: Transform Synthea data to OMOP CDM format

This project provides optimized scripts for each step, as well as a unified pipeline that combines all steps into a single process.

## Scripts

### 1. `init_database_with_vocab.py`

This script initializes the database with schemas, tables, and vocabulary. It combines the functionality of the original `init_database.py` script with vocabulary processing and loading.

```bash
./init_database_with_vocab.py [options]
```

Options:
- `--skip-init`: Skip database initialization
- `--skip-vocab`: Skip vocabulary loading
- `--vocab-dir`: Directory containing vocabulary files (default: ./vocabulary)
- `--processed-vocab-dir`: Directory for processed vocabulary files (default: ./vocabulary_processed)
- `--max-workers`: Maximum number of parallel workers (default: 4)
- `--batch-size`: Batch size for processing large files (default: 1000000)
- `--debug`: Enable debug logging

### 2. `run_init_with_vocab.sh`

This is a shell script wrapper for the `init_database_with_vocab.py` script, providing a more user-friendly interface.

```bash
./run_init_with_vocab.sh [options]
```

Options:
- `-v, --vocab-dir <directory>`: Directory containing vocabulary files (default: ./vocabulary)
- `-p, --processed-vocab-dir <directory>`: Directory for processed vocabulary files (default: ./vocabulary_processed)
- `-w, --max-workers <number>`: Maximum number of parallel workers (default: 4)
- `-b, --batch-size <number>`: Batch size for processing large files (default: 1000000)
- `-i, --skip-init`: Skip database initialization
- `-s, --skip-vocab`: Skip vocabulary loading
- `-d, --debug`: Enable debug logging
- `-h, --help`: Display help message

### 3. `run_unified_pipeline.py`

This script provides a unified pipeline that combines database initialization, vocabulary loading, and ETL process into a single command.

```bash
./run_unified_pipeline.py [options]
```

Options:
- `--skip-init`: Skip database initialization
- `--skip-vocab`: Skip vocabulary loading
- `--skip-etl`: Skip ETL process
- `--vocab-dir`: Directory containing vocabulary files (default: ./vocabulary)
- `--processed-vocab-dir`: Directory for processed vocabulary files (default: ./vocabulary_processed)
- `--synthea-dir`: Directory containing Synthea output files (default: ./synthea-output)
- `--max-workers`: Maximum number of parallel workers (default: 4)
- `--batch-size`: Batch size for processing large files (default: 1000000)
- `--debug`: Enable debug logging

### 4. `run_unified_pipeline.sh`

This is a shell script wrapper for the `run_unified_pipeline.py` script, providing a more user-friendly interface.

```bash
./run_unified_pipeline.sh [options]
```

Options:
- `-v, --vocab-dir <directory>`: Directory containing vocabulary files (default: ./vocabulary)
- `-p, --processed-vocab-dir <directory>`: Directory for processed vocabulary files (default: ./vocabulary_processed)
- `-s, --synthea-dir <directory>`: Directory containing Synthea output files (default: ./synthea-output)
- `-w, --max-workers <number>`: Maximum number of parallel workers (default: 4)
- `-b, --batch-size <number>`: Batch size for processing large files (default: 1000000)
- `-i, --skip-init`: Skip database initialization
- `-v, --skip-vocab`: Skip vocabulary loading
- `-e, --skip-etl`: Skip ETL process
- `-d, --debug`: Enable debug logging
- `-h, --help`: Display help message

## Workflow

### Standard Workflow

The standard workflow for initializing the database, loading vocabulary, and running the ETL process is:

1. **Prepare Vocabulary Files**: Place OMOP vocabulary files in the `vocabulary` directory
2. **Run Unified Pipeline**: Execute the unified pipeline script
   ```bash
   ./run_unified_pipeline.sh
   ```
3. **Check Results**: Verify that the process completed successfully and check the record counts

### Advanced Workflow

For more control over the process, you can run each step separately:

1. **Initialize Database and Load Vocabulary**:
   ```bash
   ./run_init_with_vocab.sh
   ```

2. **Run ETL Process**:
   ```bash
   ./run_optimized_import.sh
   ```

## Optimizations

The optimized process includes several improvements over the original process:

### 1. Parallel Processing

The vocabulary processing and ETL steps use parallel processing to improve performance. You can control the number of parallel workers with the `--max-workers` option.

### 2. Batch Processing

Large vocabulary files are processed in batches to reduce memory usage. You can control the batch size with the `--batch-size` option.

### 3. Direct Loading

The vocabulary loading process uses PostgreSQL's COPY command for bulk loading, which is much faster than row-by-row insertion.

### 4. Special Handling

Special handling is implemented for tables with potentially long text values, such as `concept` and `concept_synonym`.

### 5. Dependency Checks

The unified pipeline includes dependency checks to ensure that vocabulary tables exist and contain required concept IDs before running the ETL process.

## Performance

The optimized process provides significant performance improvements over the original process:

- **Vocabulary Processing**: Up to 5x faster with parallel processing and batch processing
- **Vocabulary Loading**: Up to 10x faster with direct loading using COPY command
- **ETL Process**: Up to 3x faster with parallel processing and optimized SQL

## Troubleshooting

### Common Issues

1. **Missing Vocabulary Files**:
   - Ensure that vocabulary files are placed in the `vocabulary` directory
   - Check file permissions

2. **Database Connection Errors**:
   - Verify database connection parameters in `.env` file
   - Ensure PostgreSQL is running

3. **Memory Issues**:
   - Reduce the number of parallel workers with `--max-workers`
   - Reduce the batch size with `--batch-size`

### Checking Logs

Detailed logs are available in the `logs` directory. Each run creates a new log file with a timestamp:

- `logs/init_database_with_vocab_YYYYMMDD_HHMMSS.log`
- `logs/unified_pipeline_YYYYMMDD_HHMMSS.log`

## Configuration

You can configure the process using environment variables in the `.env` file:

```
DB_HOST=localhost
DB_PORT=5432
DB_NAME=ohdsi
DB_USER=postgres
DB_PASSWORD=acumenus
VOCAB_DIR=/path/to/vocabulary
PROCESSED_VOCAB_DIR=/path/to/vocabulary_processed
SYNTHEA_DIR=/path/to/synthea-output
MAX_WORKERS=4
BATCH_SIZE=1000000
```

Alternatively, you can specify these parameters on the command line.
