# Synthea to OMOP ETL Pipeline

This directory contains a modular ETL (Extract, Transform, Load) pipeline for converting Synthea synthetic health data into the OMOP Common Data Model format.

## Directory Structure

```
etl_pipeline/
├── etl_setup.py                # Shared utilities for DB connections, logging, checkpointing
├── etl_main.py                 # Main orchestration script
├── etl_patients.py             # Process patients into person table
├── etl_encounters.py           # Process encounters into visit_occurrence table
├── etl_conditions.py           # Process conditions into condition_occurrence table
├── etl_procedures.py           # Process procedures into procedure_occurrence table
├── etl_medications.py          # Process medications into drug_exposure table
├── etl_observations.py         # Process observations into measurement and observation tables
├── etl_observation_periods.py  # Create observation_period records
├── etl_concept_mapping.py      # Map source concepts to standard concepts
├── etl_analyze.py              # Analyze tables for query optimization
├── etl_validation.py           # Validate ETL results
└── README.md                   # This file
```

## Key Features

- **Modular Design**: Each ETL step is implemented as a separate Python module
- **Progress Tracking**: Detailed progress tracking with database-backed status updates
- **Checkpointing**: Automatic checkpointing to resume interrupted ETL processes
- **Error Handling**: Comprehensive error handling and reporting
- **Visual Feedback**: Progress bars and colored console output
- **Batch Processing**: Efficient batch processing of large datasets

## Running the ETL Pipeline

To run the complete ETL pipeline:

```bash
python etl_pipeline/etl_main.py --data-dir /path/to/synthea/output --debug
```

To force reprocessing of all steps:

```bash
python etl_pipeline/etl_main.py --data-dir /path/to/synthea/output --force
```

To run specific ETL steps:

```bash
python etl_pipeline/etl_main.py --data-dir /path/to/synthea/output --steps process_patients process_encounters
```

## Individual ETL Modules

Each ETL module can also be run independently for testing or targeted processing:

```bash
# Process patients
python etl_pipeline/etl_patients.py --patients-csv /path/to/synthea/output/patients.csv

# Process encounters
python etl_pipeline/etl_encounters.py --encounters-csv /path/to/synthea/output/encounters.csv

# Process conditions
python etl_pipeline/etl_conditions.py --conditions-csv /path/to/synthea/output/conditions.csv

# Process procedures
python etl_pipeline/etl_procedures.py --procedures-csv /path/to/synthea/output/procedures.csv

# Process medications
python etl_pipeline/etl_medications.py --medications-csv /path/to/synthea/output/medications.csv

# Process observations
python etl_pipeline/etl_observations.py --observations-csv /path/to/synthea/output/observations.csv

# Create observation periods
python etl_pipeline/etl_observation_periods.py

# Map source to standard concepts
python etl_pipeline/etl_concept_mapping.py

# Analyze tables
python etl_pipeline/etl_analyze.py

# Validate ETL results
python etl_pipeline/etl_validation.py
```

## Implementation Notes

### UUID to Integer ID Mapping

The ETL process handles Synthea's UUID format identifiers by mapping them to sequential integer IDs required by OMOP:

- `staging.person_map` maps patient UUIDs to person_id integers
- `staging.visit_map` maps encounter UUIDs to visit_occurrence_id integers

This approach allows for consistent referential integrity while preserving the original identifiers.

### Progress Tracking

The ETL process uses two complementary progress tracking mechanisms:

1. **Checkpoint File**: A JSON file (.synthea_etl_checkpoint.json) that records completed steps
2. **Database Table**: A `staging.etl_progress` table that tracks detailed progress of each step

This dual approach provides both persistence across runs and real-time status updates during execution.
