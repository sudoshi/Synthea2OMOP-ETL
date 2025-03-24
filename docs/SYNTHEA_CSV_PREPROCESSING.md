# Synthea CSV Preprocessing Guide

## Overview

This document explains how to use the Synthea CSV preprocessing tools to fix formatting issues in the Synthea output files before loading them into the database.

## Problem: Malformed CSV Files

The Synthea data generator creates CSV files with an unusual format:
- Headers have proper comma separators
- Data rows have values concatenated together without proper delimiters

This makes it difficult to load the data directly into PostgreSQL using standard methods. For example, if you try to use PostgreSQL's `COPY` command, you'll get errors about malformed CSV input.

## Solution

We've implemented a two-step solution:

1. **Preprocessing**: Fix the CSV files by properly separating the fields in each row
2. **Loading**: Load the preprocessed files into the staging schema

## Using the Pipeline

### Complete Pipeline (Recommended)

The simplest way to process Synthea data is to use the complete pipeline script, which handles both preprocessing and loading:

```bash
# Basic usage
bash/run_synthea_data_pipeline.sh

# With options
bash/run_synthea_data_pipeline.sh --input-dir ./synthea-output \
                                 --processed-dir ./synthea-processed \
                                 --overwrite-processed \
                                 --force-load \
                                 --debug
```

### Options

- `--input-dir DIR`: Directory containing Synthea output files (default: ./synthea-output)
- `--processed-dir DIR`: Directory for preprocessed files (default: ./synthea-processed)
- `--overwrite-processed`: Overwrite existing preprocessed files
- `--debug`: Enable debug logging for more detailed output
- `--force-load`: Force overwrite of existing database tables
- `--max-workers N`: Maximum number of parallel workers (default: 4)

### Manual Steps

If you prefer to run the steps separately:

#### 1. Preprocess the CSV files

```bash
python python/preprocess_synthea_csv.py --input-dir ./synthea-output \
                                      --output-dir ./synthea-processed \
                                      --overwrite
```

#### 2. Load the preprocessed files into the database

```bash
SYNTHEA_DATA_DIR=./synthea-processed scripts/load_synthea_staging.sh --force
```

## How the Preprocessing Works

The preprocessing script (`python/preprocess_synthea_csv.py`) performs the following steps:

1. Reads the header row to identify column names
2. Processes each data row to separate the concatenated values
3. Uses various heuristics to identify field boundaries:
   - Looking for common patterns like UUIDs, dates, etc.
   - Using field length estimation based on the number of columns
   - Handling partial comma separation in hybrid format rows
4. Writes properly formatted CSV files to the output directory

## Troubleshooting

### Common Issues

1. **Preprocessing fails**: Check the logs in the `logs` directory for details. Common causes include:
   - Permission issues with input/output directories
   - Unexpected formatting in the source files

2. **Database loading fails**: Check the logs for details. Common causes include:
   - Database connection issues
   - Schema not existing
   - Permission issues

### Debug Mode

Enable debug mode for more detailed logging:

```bash
bash/run_synthea_data_pipeline.sh --debug
```

## Implementation Details

### Preprocessing Algorithm

The preprocessing uses several strategies to handle the malformed CSV files:

1. If a row has the right number of commas, it's treated as properly formatted
2. For rows with no or few commas, it tries:
   - Identifying the first field as a UUID (common in Synthea data)
   - Dividing the remaining text based on expected field lengths
3. For partially formatted rows with some commas, it handles the properly separated parts and divides the remaining concatenated section

### Performance Considerations

For large Synthea datasets:
- The preprocessing step adds overhead to the loading process
- Consider using a machine with more memory and CPU cores
- Progress logging shows updates every 10,000 rows processed
- Multiple files are processed sequentially to avoid memory issues

## Future Improvements

Potential improvements to consider:
- Parallel processing of multiple files
- More advanced field pattern recognition for better accuracy
- Integration with OMOP ETL workflows
