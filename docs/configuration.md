# Configuration System Documentation

This document explains the enhanced configuration system for the Synthea2OMOP-ETL project.

## Overview

The configuration system provides a flexible way to configure the ETL process across different environments without modifying the code. It consists of:

1. **Environment Variables** (`.env` file): For environment-specific settings like database credentials
2. **Configuration File** (`config.json`): For project-wide settings and mappings
3. **Python Configuration Loader**: A utility to access configuration values programmatically
4. **Command-line Arguments**: For runtime overrides in the ETL process

## Environment Variables (`.env` file)

The `.env` file contains environment-specific configuration values, such as database connection details. These values can vary between development, testing, and production environments.

### Example `.env` file:

```
# Database Connection Settings
DB_HOST=localhost
DB_PORT=5432
DB_NAME=synthea
DB_USER=postgres
DB_PASSWORD=acumenus

# Schema Names
OMOP_SCHEMA=omop
STAGING_SCHEMA=staging
POPULATION_SCHEMA=population

# File Paths
VOCAB_DIR=/path/to/vocabulary/files
SYNTHEA_DATA_DIR=/path/to/synthea/output

# Processing Options
WITH_HEADER=true
PARALLEL_JOBS=4
```

### Available Environment Variables

| Variable | Description | Default Value |
|----------|-------------|---------------|
| `DB_HOST` | Database host | `localhost` |
| `DB_PORT` | Database port | `5432` |
| `DB_NAME` | Database name | `synthea` |
| `DB_USER` | Database user | `postgres` |
| `DB_PASSWORD` | Database password | `acumenus` |
| `OMOP_SCHEMA` | OMOP schema name | `omop` |
| `STAGING_SCHEMA` | Staging schema name | `staging` |
| `POPULATION_SCHEMA` | Population schema name | `population` |
| `VOCAB_DIR` | Directory containing OMOP vocabulary files | `./vocabulary` |
| `SYNTHEA_DATA_DIR` | Directory containing Synthea CSV files | `./synthea_data` |
| `WITH_HEADER` | Whether CSV files have headers | `true` |
| `PARALLEL_JOBS` | Number of parallel jobs for processing | `4` |

## Configuration File (`config.json`)

The `config.json` file contains project-wide settings that are less likely to change between environments, such as concept mappings and ETL parameters.

### Example `config.json` file:

```json
{
  "project": {
    "name": "Synthea2OMOP-ETL",
    "version": "1.0.0",
    "description": "ETL pipeline for converting Synthea data to OMOP CDM"
  },
  "database": {
    "connection_timeout": 30,
    "max_connections": 20,
    "enable_ssl": false
  },
  "etl": {
    "batch_size": 10000,
    "enable_logging": true,
    "log_level": "INFO",
    "truncate_target_tables": true
  },
  "mapping": {
    "gender": {
      "M": 8507,
      "F": 8532
    },
    "race": {
      "white": 8527,
      "black": 8516,
      "asian": 8515,
      "hawaiian": 8557
    },
    "ethnicity": {
      "hispanic": 38003563,
      "nonhispanic": 38003564
    },
    "visit_type": {
      "inpatient": 9201,
      "outpatient": 9202,
      "emergency": 9203,
      "other": 44818517
    }
  },
  "vocabulary": {
    "version": "v5.0 20-MAY-23",
    "files": [
      "CONCEPT.csv",
      "VOCABULARY.csv",
      "DOMAIN.csv",
      "CONCEPT_CLASS.csv",
      "RELATIONSHIP.csv",
      "CONCEPT_RELATIONSHIP.csv",
      "CONCEPT_SYNONYM.csv",
      "CONCEPT_ANCESTOR.csv",
      "DRUG_STRENGTH.csv",
      "SOURCE_TO_CONCEPT_MAP.csv"
    ]
  }
}
```

## Python Configuration Loader

The Python configuration loader (`utils/config_loader.py`) provides a programmatic way to access configuration values from both the `.env` file and the `config.json` file.

### Example Usage:

```python
from utils.config_loader import config

# Get database configuration
db_config = config.get_db_config()
print(f"Connecting to database {db_config['dbname']} on {db_config['host']}:{db_config['port']}")

# Get schema names
schema_names = config.get_schema_names()
print(f"OMOP schema: {schema_names['omop']}")

# Get a specific environment variable
vocab_dir = config.get_env('VOCAB_DIR')
print(f"Vocabulary directory: {vocab_dir}")

# Get a specific configuration value using dot notation
batch_size = config.get_config('etl.batch_size')
print(f"ETL batch size: {batch_size}")

# Get a concept ID from the mapping configuration
gender_concept_id = config.get_concept_id('gender', 'M')
print(f"Gender concept ID for 'M': {gender_concept_id}")
```

### Available Methods:

| Method | Description |
|--------|-------------|
| `get_env(key, default=None)` | Get a value from environment variables |
| `get_config(path, default=None)` | Get a value from the config.json using dot notation |
| `get_db_config()` | Get database configuration as a dictionary |
| `get_schema_names()` | Get schema names as a dictionary |
| `get_file_paths()` | Get file paths as a dictionary |
| `get_processing_options()` | Get processing options as a dictionary |
| `get_concept_id(category, code)` | Get a concept ID from the mapping configuration |

## Command-line Arguments

The main ETL script (`run_etl.py`) supports command-line arguments to override configuration values and control the ETL process.

### Available Arguments:

| Argument | Description |
|----------|-------------|
| `--skip-schema` | Skip creating OMOP schema |
| `--skip-vocab` | Skip loading OMOP vocabulary |
| `--skip-synthea` | Skip loading Synthea data |
| `--skip-typing` | Skip converting Synthea data types |
| `--skip-staging` | Skip creating staging schema |
| `--skip-etl` | Skip running ETL process |
| `--skip-validation` | Skip validating ETL results |

### Example Usage:

```bash
# Run the full ETL process
python run_etl.py

# Skip loading vocabulary and run from Synthea data loading
python run_etl.py --skip-schema --skip-vocab

# Only run the ETL process (skip all previous steps)
python run_etl.py --skip-schema --skip-vocab --skip-synthea --skip-typing --skip-staging
```

## Shell Script Integration

The shell scripts have been updated to use the configuration system. For example, the `load_synthea_staging.sh` script now reads configuration values from the `.env` file:

```bash
# Load environment variables from .env file if it exists
if [ -f "$PROJECT_ROOT/.env" ]; then
    echo "Loading configuration from $PROJECT_ROOT/.env"
    set -a  # automatically export all variables
    source "$PROJECT_ROOT/.env"
    set +a
else
    echo "Warning: .env file not found in $PROJECT_ROOT"
    echo "Using default configuration values"
fi

# Use environment variables with defaults
DB_HOST="${DB_HOST:-localhost}"
DB_PORT="${DB_PORT:-5432}"
DB_NAME="${DB_NAME:-synthea}"
DB_USER="${DB_USER:-postgres}"
DB_PASSWORD="${DB_PASSWORD:-acumenus}"
DB_SCHEMA="${POPULATION_SCHEMA:-population}"
SYNTHEA_DATA_DIR="${SYNTHEA_DATA_DIR:-$PROJECT_ROOT/synthea_data}"
```

## Project Initialization

The `init_project.py` script initializes the project structure and creates the necessary configuration files:

```bash
# Initialize the project
python init_project.py
```

This script:
1. Creates necessary directories (vocabulary, synthea_data, logs, etc.)
2. Copies `.env.example` to `.env` if it doesn't exist
3. Creates `requirements.txt` if it doesn't exist
4. Creates `README.md` if it doesn't exist

## Best Practices

1. **Never commit `.env` files to version control**. They contain sensitive information like database passwords.
2. Always provide a `.env.example` file with placeholder values as a template.
3. Use environment variables for values that change between environments.
4. Use the configuration file for values that are consistent across environments but might need to be adjusted.
5. Use command-line arguments for one-time overrides or to control the ETL process flow.
6. When adding new configuration options, update the documentation.
