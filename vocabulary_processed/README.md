# OMOP Vocabulary Processing and Loading

This directory contains scripts for processing and loading OMOP vocabulary files into a PostgreSQL database. These scripts handle common issues with vocabulary files, such as improper delimiters, long text values, and other formatting problems.

## Overview

The process consists of two main steps:

1. **Processing the vocabulary files** using `clean_vocab.py`
2. **Loading the processed files** into the database using `load_omop_vocab.sh`

## Prerequisites

- Python 3.6+
- PostgreSQL client tools (`psql`)
- `pv` command-line utility for progress bars
- OMOP vocabulary files (CSV format)

## Scripts

### 1. `clean_vocab.py`

This script processes the raw vocabulary files to prepare them for loading into a PostgreSQL database.

#### Features:

- Fixes header lines to ensure proper tab delimiters
- Handles special characters and escaping
- Truncates long text values that would exceed PostgreSQL column limits
- Ensures consistent line endings and field separators
- Preserves original files and creates processed copies

#### Usage:

```bash
python clean_vocab.py <input_dir> <output_dir>
```

Where:
- `<input_dir>` is the directory containing the original vocabulary files
- `<output_dir>` is the directory where the processed files will be saved

Example:
```bash
python clean_vocab.py /path/to/vocabulary /path/to/vocabulary_processed
```

### 2. `load_omop_vocab.sh`

This script loads the processed vocabulary files into a PostgreSQL database.

#### Features:

- Automatically detects file delimiters
- Special handling for files with potentially long text values
- Temporarily alters column types to handle long values
- Truncates values that exceed column limits
- Handles circular foreign key constraints
- Provides progress bars for large files
- Verifies the load by checking row counts

#### Usage:

```bash
bash load_omop_vocab.sh
```

The script uses configuration variables defined at the top of the file:
- `DB_HOST`: PostgreSQL host (default: "localhost")
- `DB_PORT`: PostgreSQL port (default: "5432")
- `DB_NAME`: Database name (default: "ohdsi")
- `DB_USER`: Database user (default: "postgres")
- `PGPASSWORD`: Database password (set as environment variable)
- `VOCAB_DIR`: Path to the original vocabulary files
- `PROCESSED_VOCAB_DIR`: Path to the processed vocabulary files

## Common Issues and Solutions

### Long Text Values

Some vocabulary files (particularly CONCEPT.csv and CONCEPT_SYNONYM.csv) may contain text values that exceed the 2000-character limit of the `varchar(2000)` columns in the OMOP CDM schema. The scripts handle this by:

1. In `clean_vocab.py`: Identifying and truncating values that exceed 2000 characters
2. In `load_omop_vocab.sh`: Temporarily altering column types to TEXT, loading the data, then converting back to varchar(2000) with truncation

### Improper Delimiters

Vocabulary files may have inconsistent delimiters or missing tab separators. The scripts handle this by:

1. In `clean_vocab.py`: Fixing header lines and ensuring proper tab delimiters between fields
2. In `load_omop_vocab.sh`: Automatically detecting the delimiter used in each file

### Circular Foreign Key Constraints

The OMOP schema has circular foreign key constraints between the `domain` and `concept` tables. The scripts handle this by:

1. Dropping these constraints before loading the data
2. Re-adding them after all tables have been loaded

## Workflow

1. Place your vocabulary files in the `vocabulary` directory
2. Run `clean_vocab.py` to process the files
3. Run `load_omop_vocab.sh` to load the processed files into the database
4. Verify the load by checking the row counts displayed at the end

## Troubleshooting

### Error: "value too long for type character varying(2000)"

If you encounter this error, it means there are values in the vocabulary files that exceed the 2000-character limit. Make sure you're using the latest version of both scripts, which include handling for this issue.

### Error: "invalid input syntax for type integer"

This error typically occurs when there are header issues or improper delimiters in the vocabulary files. The `clean_vocab.py` script should fix these issues, but you may need to manually inspect the problematic file.

### Error: "there is no unique constraint matching given keys for referenced table"

This error occurs when trying to add foreign key constraints, but the referenced columns don't have unique constraints. This is usually not critical for the vocabulary loading process and can be ignored.
