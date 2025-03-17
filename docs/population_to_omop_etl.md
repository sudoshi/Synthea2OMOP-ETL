# Population Schema to OMOP CDM ETL Process

This document describes the ETL (Extract, Transform, Load) process for converting data from the population schema to the OMOP Common Data Model (CDM).

## Overview

The ETL process transforms data from the population schema to the OMOP CDM schema. The population schema contains patient data in a format that is not standardized, while the OMOP CDM schema is a standardized format for healthcare data.

## Prerequisites

- PostgreSQL database
- Python 3.6 or higher
- Required Python packages (see requirements.txt)

## Database Initialization

Before running the ETL process, you need to initialize the database with the required schemas and tables:

```bash
# Initialize the database with schemas and tables
./init_database.py
```

This script will:
1. Create the required schemas (population, staging, omop)
2. Create the OMOP CDM tables
3. Create the staging tables

## ETL Process

The ETL process consists of the following steps:

1. **Preparation**
   - Create ETL log directory
   - Backup current OMOP schema data (if needed)
   - Verify database connection settings
   - Repopulate staging tables from population schema
   - Add indexes to staging and population tables

2. **Concept Mapping**
   - Identify required concept mappings for conditions (SNOMED-CT)
   - Identify required concept mappings for medications (RxNorm)
   - Identify required concept mappings for procedures (SNOMED-CT)
   - Identify required concept mappings for observations (LOINC)
   - Populate local_to_omop_concept_map table

3. **ETL Execution**
   - Create observation period data
   - Transform visit data (visit_occurrence)
   - Transform condition data (condition_occurrence)
   - Transform medication data (drug_exposure)
   - Transform procedure data (procedure_occurrence)
   - Transform observation data (observation, measurement)
   - Transform death data (death)
   - Transform cost data (cost)

4. **Validation**
   - Verify record counts in OMOP tables
   - Check referential integrity
   - Validate concept mappings
   - Check for unmapped source codes
   - Verify date ranges and demographics

5. **Performance Optimization**
   - Create additional indexes if needed
   - Analyze tables for query optimization
   - Document performance metrics

## Running the ETL Process

### Pre-flight Tests

Before running the ETL process, it's recommended to run the pre-flight tests to ensure that all prerequisites are met:

```bash
./test_etl_preflight.py
```

The pre-flight tests check for:
- Database connectivity
- Schema existence (population, staging, omop)
- Table existence in all schemas
- Data presence in population tables
- Column existence in OMOP tables
- SQL script existence
- Database permissions
- Available disk space

### Running the ETL

To run the ETL process, use one of the following commands:

```bash
# Run ETL process only
./run_etl_population_to_omop.py

# Run pre-flight tests first, then ETL process if tests pass
./run_etl_with_preflight.py
```

These scripts will execute all the ETL steps in sequence and log the results to the logs directory.

## ETL Scripts

The ETL process is implemented using a series of SQL scripts:

- `sql/etl/add_population_indexes.sql`: Add indexes to population schema tables
- `sql/etl/add_staging_indexes.sql`: Add indexes to staging tables
- `sql/etl/populate_concept_map_v2.sql`: Populate the concept map table
- `sql/etl/create_observation_period_v2.sql`: Create observation period data
- `sql/etl/transform_visit_occurrence_v2.sql`: Transform visit data
- `sql/etl/transform_condition_occurrence.sql`: Transform condition data
- `sql/etl/transform_drug_exposure.sql`: Transform medication data
- `sql/etl/transform_procedure_occurrence.sql`: Transform procedure data
- `sql/etl/transform_observation_measurement.sql`: Transform observation data
- `sql/etl/transform_death.sql`: Transform death data
- `sql/etl/transform_cost.sql`: Transform cost data
- `sql/etl/run_all_etl.sql`: Run all ETL steps in sequence

## Concept Mapping

The ETL process maps source codes to OMOP concepts using the following vocabularies:

- Conditions: SNOMED-CT
- Medications: RxNorm
- Procedures: SNOMED-CT
- Observations: LOINC

The concept mappings are stored in the `staging.local_to_omop_concept_map` table.

## Data Volumes

The ETL process transforms the following data volumes:

- Patients: 888,463
- Encounters: 67,018,822
- Conditions: 37,624,858
- Medications: 64,535,201
- Procedures: 172,916,715
- Observations: 896,011,741

## Issues and Challenges

During the ETL process, the following issues and challenges were encountered:

1. **Duplicate encounter IDs**: Some encounter IDs appeared multiple times in the encounters_raw table. This was handled by using DISTINCT ON in the SQL queries.

2. **Mismatched column names**: The column names in the OMOP CDM schema did not always match the expected names. For example, the column name in the visit_occurrence table is "discharged_to_concept_id", not "discharge_to_concept_id".

3. **Performance issues**: The ETL process involved processing large volumes of data, which could be slow without proper indexing. This was addressed by adding indexes to the staging and population tables.

## Performance Metrics

The ETL process was optimized for performance by:

- Adding indexes to staging and population tables
- Using DISTINCT ON to handle duplicate records
- Analyzing tables for query optimization

## Conclusion

The ETL process successfully transformed data from the population schema to the OMOP CDM schema. The resulting OMOP CDM data can be used for standardized analytics and research.
