# Enhanced Synthea2OMOP-ETL

An enhanced ETL pipeline for converting Synthea synthetic healthcare data to the OMOP Common Data Model (version 5.4).

## Overview

The [Synthea™ Patient Generator](https://github.com/synthetichealth/synthea) is an open-source synthetic patient generator that models the medical history of artificial patients, created by The MITRE Corporation. This project demonstrates how to transform Synthea's output into the OMOP Common Data Model format with a robust, configurable ETL pipeline.

## Key Features

- **Flexible Configuration System**: Environment-specific settings in `.env` files and project-wide settings in `config.json`
- **Comprehensive Logging**: Detailed logs for each ETL step with timestamps and error tracking
- **Modular Architecture**: Clear separation of concerns with distinct modules for each ETL phase
- **Error Handling**: Robust error handling and recovery mechanisms
- **Testing Framework**: Unit tests to ensure code quality and reliability
- **Documentation**: Detailed documentation for all components and processes

## ETL Process Overview

Our ETL process follows these main steps:

1. **Initialize OMOP Schema**
   - Run the OMOP CDM DDL scripts to create the core tables
   - Set up the vocabulary tables structure

2. **Load OMOP Vocabulary**
   - Use `load_omop_vocab.sh` to load vocabulary files
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

## Setup

1. Clone this repository
2. Run `python init_project.py` to initialize the project structure
3. Copy your Synthea CSV files to the `synthea_data` directory
4. Copy your OMOP vocabulary files to the `vocabulary` directory
5. Edit the `.env` file with your database connection details
6. Install dependencies: `pip install -r requirements.txt`
7. Run the ETL process: `python run_etl.py`

## Configuration

- `.env`: Environment-specific configuration (database credentials, etc.)
- `config.json`: Project-wide settings and mappings

See [Configuration Documentation](docs/configuration.md) for details.

## Project Structure

```
├── sql/                      # SQL scripts
│   ├── omop_ddl/             # OMOP CDM table definitions
│   ├── synthea_typing/       # Convert raw Synthea data to proper types
│   ├── staging/              # Create staging schema and mapping tables
│   └── etl/                  # OMOP domain-specific ETL scripts
├── scripts/                  # Shell scripts
│   ├── load_omop_vocab.sh    # Vocabulary loading script
│   └── load_synthea_staging.sh # Raw Synthea data loading script
├── utils/                    # Python utility modules
│   ├── __init__.py
│   └── config_loader.py      # Configuration loading module
├── tests/                    # Test suite
│   ├── __init__.py
│   └── test_config_loader.py # Tests for configuration loader
├── docs/                     # Documentation
│   └── configuration.md      # Configuration documentation
├── .env.example              # Example environment variables
├── config.json               # Project configuration
├── init_project.py           # Project initialization script
├── run_etl.py                # Main ETL runner
├── run_tests.py              # Test runner
├── requirements.txt          # Python dependencies
└── README.md                 # This file
```

## Running the ETL Process

The main entry point for running the ETL process is `run_etl.py`. You can run the entire process or skip specific steps:

```bash
# Run the full ETL process
python run_etl.py

# Skip loading vocabulary and run from Synthea data loading
python run_etl.py --skip-schema --skip-vocab

# Only run the ETL process (skip all previous steps)
python run_etl.py --skip-schema --skip-vocab --skip-synthea --skip-typing --skip-staging
```

## Running Tests

To run the test suite:

```bash
python run_tests.py
```

## References

- [Synthea GitHub](https://github.com/synthetichealth/synthea)
- [OMOP CDM Documentation](https://ohdsi.github.io/CommonDataModel/)
- [OHDSI Documentation](https://ohdsi.github.io/TheBookOfOhdsi/)
- [Python-dotenv Documentation](https://github.com/theskumar/python-dotenv)

## Contributing

We welcome contributions! Please feel free to submit issues or pull requests if you have improvements or questions about the ETL process.
