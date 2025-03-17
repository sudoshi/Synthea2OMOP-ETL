-- Run all ETL steps in sequence with optimizations for performance

-- Step 0: Add missing indexes to improve join performance
\echo 'Step 0: Adding missing indexes for better performance...'
\i sql/etl/add_missing_indexes.sql

-- Step 1: Add indexes to population and staging tables
\echo 'Step 1: Adding indexes to population and staging tables...'
\i sql/etl/add_population_indexes.sql
\i sql/etl/add_staging_indexes.sql

-- Step 2: Populate concept map (using batched version for better performance)
\echo 'Step 2: Populating concept map (batched version)...'
\i sql/etl/populate_concept_map_v2_batched.sql

-- Step 3: Create observation period data
\echo 'Step 3: Creating observation period data...'
\i sql/etl/create_observation_period_v2.sql

-- Step 4: Transform visit data
\echo 'Step 4: Transforming visit data...'
\i sql/etl/transform_visit_occurrence_v2.sql

-- Step 5: Transform condition data
\echo 'Step 5: Transforming condition data...'
\i sql/etl/transform_condition_occurrence.sql

-- Step 6: Transform medication data
\echo 'Step 6: Transforming medication data...'
\i sql/etl/transform_drug_exposure.sql

-- Step 7: Transform procedure data
\echo 'Step 7: Transforming procedure data...'
\i sql/etl/transform_procedure_occurrence.sql

-- Step 8: Transform observation data
\echo 'Step 8: Transforming observation data...'
\i sql/etl/transform_observation_measurement.sql

-- Step 9: Transform death data
\echo 'Step 9: Transforming death data...'
\i sql/etl/transform_death.sql

-- Step 10: Transform cost data
\echo 'Step 10: Transforming cost data...'
\i sql/etl/transform_cost.sql

-- Step 11: Verify record counts
\echo 'Step 11: Verifying record counts...'
SELECT 'person' AS table_name, COUNT(*) AS record_count FROM omop.person
UNION ALL
SELECT 'observation_period' AS table_name, COUNT(*) AS record_count FROM omop.observation_period
UNION ALL
SELECT 'visit_occurrence' AS table_name, COUNT(*) AS record_count FROM omop.visit_occurrence
UNION ALL
SELECT 'condition_occurrence' AS table_name, COUNT(*) AS record_count FROM omop.condition_occurrence
UNION ALL
SELECT 'drug_exposure' AS table_name, COUNT(*) AS record_count FROM omop.drug_exposure
UNION ALL
SELECT 'procedure_occurrence' AS table_name, COUNT(*) AS record_count FROM omop.procedure_occurrence
UNION ALL
SELECT 'measurement' AS table_name, COUNT(*) AS record_count FROM omop.measurement
UNION ALL
SELECT 'observation' AS table_name, COUNT(*) AS record_count FROM omop.observation
UNION ALL
SELECT 'death' AS table_name, COUNT(*) AS record_count FROM omop.death
UNION ALL
SELECT 'cost' AS table_name, COUNT(*) AS record_count FROM omop.cost
ORDER BY table_name;

-- Step 12: Check for unmapped source codes
\echo 'Step 12: Checking for unmapped source codes...'
SELECT 
    source_vocabulary, 
    domain_id, 
    COUNT(*) AS mapping_count,
    SUM(CASE WHEN target_concept_id = 0 THEN 1 ELSE 0 END) AS unmapped_count,
    ROUND(100.0 * SUM(CASE WHEN target_concept_id = 0 THEN 1 ELSE 0 END) / COUNT(*), 2) AS unmapped_percentage
FROM 
    staging.local_to_omop_concept_map
GROUP BY 
    source_vocabulary, domain_id
ORDER BY 
    source_vocabulary, domain_id;

-- Step 13: Verify date ranges
\echo 'Step 13: Verifying date ranges...'
SELECT 
    MIN(observation_period_start_date) AS min_start_date,
    MAX(observation_period_end_date) AS max_end_date
FROM 
    omop.observation_period;

-- Step 14: Verify demographics
\echo 'Step 14: Verifying demographics...'
SELECT 
    gender_concept_id,
    COUNT(*) AS person_count
FROM 
    omop.person
GROUP BY 
    gender_concept_id
ORDER BY 
    gender_concept_id;

SELECT 
    race_concept_id,
    COUNT(*) AS person_count
FROM 
    omop.person
GROUP BY 
    race_concept_id
ORDER BY 
    race_concept_id;

SELECT 
    ethnicity_concept_id,
    COUNT(*) AS person_count
FROM 
    omop.person
GROUP BY 
    ethnicity_concept_id
ORDER BY 
    ethnicity_concept_id;

-- Step 15: Analyze tables for query optimization
\echo 'Step 15: Analyzing tables for query optimization...'
ANALYZE omop.person;
ANALYZE omop.observation_period;
ANALYZE omop.visit_occurrence;
ANALYZE omop.condition_occurrence;
ANALYZE omop.drug_exposure;
ANALYZE omop.procedure_occurrence;
ANALYZE omop.measurement;
ANALYZE omop.observation;
ANALYZE omop.death;
ANALYZE omop.cost;

-- Step 16: Check ETL progress log
\echo 'Step 16: Checking ETL progress log...'
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

\echo 'ETL process completed successfully!'
