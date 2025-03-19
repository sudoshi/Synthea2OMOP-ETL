# Synthea to OMOP ETL Process

This document describes the ETL (Extract, Transform, Load) process for converting Synthea synthetic patient data to the OMOP Common Data Model (CDM).

## Overview

The ETL process consists of several steps that transform Synthea data into the OMOP CDM format. The main script that orchestrates the entire process is `sql/etl/run_all_etl.sql`.

## ETL Steps

1. **Add missing indexes** - Adds indexes to improve join performance
2. **Transform person data** - Transforms Synthea patient data to OMOP person table
3. **Add population and staging indexes** - Adds indexes to population and staging tables
4. **Populate concept map** - Maps Synthea concepts to OMOP concepts
5. **Create observation period data** - Creates observation periods for patients
6. **Transform visit data** - Transforms Synthea encounters to OMOP visit_occurrence
7. **Transform condition data** - Transforms Synthea conditions to OMOP condition_occurrence
8. **Transform medication data** - Transforms Synthea medications to OMOP drug_exposure
9. **Transform procedure data** - Transforms Synthea procedures to OMOP procedure_occurrence
10. **Transform measurement and observation data** - Transforms Synthea observations to OMOP measurement
11. **Transfer non-numeric measurements to observation** - Transfers non-numeric values from measurement to observation
12. **Transform death data** - Transforms Synthea death data to OMOP death
13. **Transform cost data** - Transforms Synthea cost data to OMOP cost
14. **Verify record counts** - Counts records in all OMOP tables
15. **Check for unmapped source codes** - Identifies unmapped source codes
16. **Verify date ranges** - Verifies date ranges in observation_period
17. **Verify demographics** - Verifies demographics in person table
18. **Analyze tables for query optimization** - Analyzes tables for better query performance
19. **Check ETL progress log** - Checks the ETL progress log

## Measurement and Observation Handling

One of the key challenges in the ETL process was properly separating measurements and observations according to OMOP CDM guidelines:

- **Measurements**: Should contain quantitative (numeric) values like lab results and vital signs
- **Observations**: Should contain qualitative information, survey responses, and other non-numeric clinical facts

### Approaches Explored

We explored several approaches to handle this challenge:

1. **Initial SQL Script** (`sql/etl/transfer_non_numeric_to_observation.sql`)
   - Used a batched approach with temporary tables
   - Processed data in batches of 1 million records
   - Encountered performance issues with large datasets

2. **Python Implementation** (`transfer_non_numeric.py`)
   - Processed data in batches of 100,000 records
   - Provided detailed progress reporting
   - Slow performance (87 records/second)

3. **Optimized SQL Implementation** (`sql/etl/optimized_transfer_non_numeric.sql`)
   - Created specialized indexes
   - Used temporary tables for better performance
   - Still encountered issues with very large datasets

4. **Python with Progress Tracking** (`transfer_with_progress.py`)
   - Provided real-time progress tracking
   - Processed data in batches by person_id
   - Better error handling but still slow

5. **Direct SQL Approach** (`sql/etl/direct_transfer.sql`) - **FINAL SOLUTION**
   - Uses a single SQL statement for the transfer
   - Correct column mapping between measurement and observation
   - Best performance for large datasets

### Final Solution

The final solution uses the direct SQL approach (`sql/etl/direct_transfer.sql`), which:

1. Identifies non-numeric measurements where `value_as_number IS NULL`
2. Transfers these records to the observation table with appropriate field mappings
3. Logs progress in the ETL progress table

This approach provides the best balance of performance, simplicity, and correctness.

## Running the ETL Process

To run the entire ETL process:

```bash
psql -h localhost -U postgres -d ohdsi -f sql/etl/run_all_etl.sql
```

To run just the non-numeric transfer step:

```bash
./run_direct_transfer.sh
```

## Monitoring and Debugging

The ETL process logs progress in the `staging.etl_progress` table, which can be queried to check the status of each step:

```sql
SELECT 
    step_name, 
    started_at, 
    completed_at, 
    status, 
    rows_processed, 
    error_message
FROM 
    staging.etl_progress
ORDER BY 
    started_at;
```

## Performance Considerations

- The ETL process is designed to handle large datasets efficiently
- Indexes are added to improve join performance
- Batched processing is used for memory-intensive operations
- Tables are analyzed for better query optimization

## Future Improvements

1. **Direct Placement**: Update the initial transformation to directly place records in the correct table
2. **Validation Checks**: Implement validation checks to ensure the separation is correct
3. **Incremental Processing**: Implement incremental processing for new data
4. **Further Performance Tuning**: Fine-tune PostgreSQL parameters for even better performance
