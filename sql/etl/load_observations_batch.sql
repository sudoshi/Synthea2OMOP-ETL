-- Batched SQL approach to load observations and measurements from staging to OMOP
-- This provides better performance tracking and feedback during execution
\timing on

\echo 'STARTING OBSERVATIONS ETL PROCESS (BATCHED)'
\echo '------------------------------------------'

-- Create sequences if they don't exist
CREATE SEQUENCE IF NOT EXISTS staging.observation_seq START 1 INCREMENT 1;
CREATE SEQUENCE IF NOT EXISTS staging.measurement_seq START 1 INCREMENT 1;

-- Create checkpoint table if it doesn't exist
CREATE TABLE IF NOT EXISTS staging.sql_etl_progress (
    table_name TEXT PRIMARY KEY,
    max_id_processed BIGINT DEFAULT 0,
    offset_processed BIGINT DEFAULT 0,
    total_processed BIGINT DEFAULT 0,
    last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    status TEXT
);

-- Initialize checkpoint records if they don't exist
INSERT INTO staging.sql_etl_progress (table_name, status)
VALUES 
    ('measurement', 'pending'), 
    ('observation', 'pending')
ON CONFLICT (table_name) DO NOTHING;

-- Create temp table for batching
CREATE TEMP TABLE IF NOT EXISTS observations_to_process (
    id BIGINT,
    batch_num INT,
    row_type TEXT,
    processed BOOLEAN DEFAULT FALSE
);

-- Measure performance
\echo 'SETUP: Creating temporary indexes for better performance...'
CREATE INDEX IF NOT EXISTS tmp_obs_pat_idx ON staging.observations_raw(patient_id);
CREATE INDEX IF NOT EXISTS tmp_obs_enc_idx ON staging.observations_raw(encounter_id);
CREATE INDEX IF NOT EXISTS tmp_obs_code_idx ON staging.observations_raw(code);
CREATE INDEX IF NOT EXISTS tmp_obs_val_idx ON staging.observations_raw(value_as_string);

-- Get the current count
\echo 'CURRENT COUNTS'
\echo '----------------'
\echo 'Current measurements:'
SELECT COUNT(*) FROM omop.measurement;
\echo 'Current observations:'
SELECT COUNT(*) FROM omop.observation;

-- PREPARE MEASUREMENT BATCHES
\echo 'PHASE 1: Preparing measurement batches...'

-- Get the starting offset for measurements
SELECT offset_processed INTO TEMP offset_var 
FROM staging.sql_etl_progress 
WHERE table_name = 'measurement';

\echo 'Starting from measurement offset:'
TABLE offset_var;

-- Insert the IDs for Measurements (numeric values) into our processing table
\echo 'Finding measurement records to process...'
INSERT INTO observations_to_process (id, batch_num, row_type)
SELECT 
    o.id,
    (ROW_NUMBER() OVER (ORDER BY o.id) - 1) / 1000000 + 1 AS batch_num,
    'measurement' AS row_type
FROM 
    staging.observations_raw o
JOIN 
    staging.person_map pm ON pm.source_patient_id = o.patient_id
LEFT JOIN 
    staging.visit_map vm ON vm.source_visit_id = o.encounter_id
WHERE
    (o.observation_type = 'numeric' OR o.value_as_string ~ '^[-]?[0-9]+([.][0-9]+)?([eE][-+]?[0-9]+)?$')
    AND NOT EXISTS (
        SELECT 1 FROM omop.measurement m
        JOIN staging.person_map pm2 ON pm2.person_id = m.person_id
        LEFT JOIN staging.visit_map vm2 ON vm2.visit_occurrence_id = m.visit_occurrence_id
        WHERE pm2.source_patient_id = o.patient_id
        AND (vm2.source_visit_id = o.encounter_id OR (o.encounter_id IS NULL AND m.visit_occurrence_id IS NULL))
        AND m.measurement_source_value = o.code
        AND m.measurement_date = o.timestamp::date
        AND m.value_source_value = o.value_as_string
    )
    AND o.id > (SELECT offset_processed FROM staging.sql_etl_progress WHERE table_name = 'measurement')
ORDER BY o.id
LIMIT 10000000; -- Process up to 10M records at a time to avoid memory issues

\echo 'Measurement batches prepared:'
SELECT 
    batch_num, 
    COUNT(*) AS records, 
    MIN(id) AS min_id, 
    MAX(id) AS max_id
FROM 
    observations_to_process 
WHERE 
    row_type = 'measurement'
GROUP BY 
    batch_num
ORDER BY 
    batch_num;

-- PROCESS MEASUREMENT BATCHES
\echo 'PHASE 2: Processing measurement batches...'

-- Process each batch for better progress tracking
DO $$
DECLARE
    batch_num INT;
    batch_min_id BIGINT;
    batch_max_id BIGINT;
    batch_count INT;
    batch_inserted INT;
    total_inserted INT := 0;
    measurement_offset BIGINT;
BEGIN
    -- Get current offset
    SELECT offset_processed INTO measurement_offset
    FROM staging.sql_etl_progress 
    WHERE table_name = 'measurement';
    
    -- Loop through each batch
    FOR batch_num, batch_count, batch_min_id, batch_max_id IN 
        SELECT 
            batch_num, 
            COUNT(*), 
            MIN(id), 
            MAX(id)
        FROM 
            observations_to_process 
        WHERE 
            row_type = 'measurement'
            AND NOT processed
        GROUP BY 
            batch_num
        ORDER BY 
            batch_num
    LOOP
        RAISE NOTICE 'Processing measurement batch %: % records (IDs % to %)', 
                     batch_num, batch_count, batch_min_id, batch_max_id;
        
        -- Process this batch
        WITH inserted AS (
            INSERT INTO omop.measurement (
                measurement_id,
                person_id,
                measurement_concept_id,
                measurement_date,
                measurement_datetime,
                measurement_time,
                measurement_type_concept_id,
                operator_concept_id,
                value_as_number,
                value_as_concept_id,
                unit_concept_id,
                range_low,
                range_high,
                provider_id,
                visit_occurrence_id,
                visit_detail_id,
                measurement_source_value,
                measurement_source_concept_id,
                unit_source_value,
                value_source_value
            )
            SELECT
                NEXTVAL('staging.measurement_seq') AS measurement_id,
                pm.person_id,
                0 AS measurement_concept_id,
                CAST(o.timestamp AS DATE) AS measurement_date,
                o.timestamp AS measurement_datetime,
                NULL AS measurement_time,
                32817 AS measurement_type_concept_id, -- "EHR"
                0 AS operator_concept_id,
                CASE 
                    WHEN o.value_as_string ~ '^[-]?[0-9]+([.][0-9]+)?([eE][-+]?[0-9]+)?$' THEN o.value_as_string::numeric
                    ELSE NULL
                END AS value_as_number,
                0 AS value_as_concept_id,
                0 AS unit_concept_id,
                NULL AS range_low,
                NULL AS range_high,
                NULL AS provider_id,
                vm.visit_occurrence_id,
                NULL AS visit_detail_id,
                o.code AS measurement_source_value,
                0 AS measurement_source_concept_id,
                NULL AS unit_source_value,
                o.value_as_string AS value_source_value
            FROM 
                staging.observations_raw o
            JOIN 
                staging.person_map pm ON pm.source_patient_id = o.patient_id
            LEFT JOIN 
                staging.visit_map vm ON vm.source_visit_id = o.encounter_id
            WHERE
                (o.observation_type = 'numeric' OR o.value_as_string ~ '^[-]?[0-9]+([.][0-9]+)?([eE][-+]?[0-9]+)?$')
                AND o.id BETWEEN batch_min_id AND batch_max_id
                AND NOT EXISTS (
                    SELECT 1 FROM omop.measurement m
                    JOIN staging.person_map pm2 ON pm2.person_id = m.person_id
                    LEFT JOIN staging.visit_map vm2 ON vm2.visit_occurrence_id = m.visit_occurrence_id
                    WHERE pm2.source_patient_id = o.patient_id
                    AND (vm2.source_visit_id = o.encounter_id OR (o.encounter_id IS NULL AND m.visit_occurrence_id IS NULL))
                    AND m.measurement_source_value = o.code
                    AND m.measurement_date = o.timestamp::date
                    AND m.value_source_value = o.value_as_string
                )
            RETURNING 1
        )
        SELECT COUNT(*) INTO batch_inserted FROM inserted;
        
        -- Mark as processed
        UPDATE observations_to_process 
        SET processed = TRUE 
        WHERE row_type = 'measurement' AND batch_num = batch_num;
        
        -- Update the checkpoint table with the maximum ID processed
        UPDATE staging.sql_etl_progress 
        SET 
            max_id_processed = batch_max_id,
            offset_processed = batch_max_id,
            total_processed = total_processed + batch_inserted,
            last_updated = CURRENT_TIMESTAMP,
            status = 'processing'
        WHERE table_name = 'measurement';

        -- Update our total
        total_inserted := total_inserted + batch_inserted;
        
        -- Log progress
        RAISE NOTICE 'Inserted % measurements for batch %. Total: % (max ID: %)', 
                     batch_inserted, batch_num, total_inserted, batch_max_id;
    END LOOP;
    
    -- Mark as completed if we processed everything
    UPDATE staging.sql_etl_progress 
    SET status = 'completed'
    WHERE table_name = 'measurement';
    
    RAISE NOTICE 'Total measurements inserted: %', total_inserted;
END $$;

-- Report on measurement processing
\echo 'MEASUREMENT PROCESSING RESULTS:'
\echo '--------------------------'
\echo 'Measurements progress:'
SELECT * FROM staging.sql_etl_progress WHERE table_name = 'measurement';
\echo 'Current measurement count:'
SELECT COUNT(*) FROM omop.measurement;

-- PREPARE OBSERVATION BATCHES
\echo 'PHASE 3: Preparing observation batches...'

-- Get the starting offset for observations
SELECT offset_processed INTO TEMP obs_offset_var 
FROM staging.sql_etl_progress 
WHERE table_name = 'observation';

\echo 'Starting from observation offset:'
TABLE obs_offset_var;

-- Clear the temp table
TRUNCATE observations_to_process;

-- Insert the IDs for Observations (non-numeric values) into our processing table
\echo 'Finding observation records to process...'
INSERT INTO observations_to_process (id, batch_num, row_type)
SELECT 
    o.id,
    (ROW_NUMBER() OVER (ORDER BY o.id) - 1) / 1000000 + 1 AS batch_num,
    'observation' AS row_type
FROM 
    staging.observations_raw o
JOIN 
    staging.person_map pm ON pm.source_patient_id = o.patient_id
LEFT JOIN 
    staging.visit_map vm ON vm.source_visit_id = o.encounter_id
WHERE
    NOT (o.observation_type = 'numeric' OR o.value_as_string ~ '^[-]?[0-9]+([.][0-9]+)?([eE][-+]?[0-9]+)?$')
    AND NOT EXISTS (
        SELECT 1 FROM omop.observation obs
        JOIN staging.person_map pm2 ON pm2.person_id = obs.person_id
        LEFT JOIN staging.visit_map vm2 ON vm2.visit_occurrence_id = obs.visit_occurrence_id
        WHERE pm2.source_patient_id = o.patient_id
        AND (vm2.source_visit_id = o.encounter_id OR (o.encounter_id IS NULL AND obs.visit_occurrence_id IS NULL))
        AND obs.observation_source_value = o.code
        AND obs.observation_date = o.timestamp::date
        AND obs.value_source_value = o.value_as_string
    )
    AND o.id > (SELECT offset_processed FROM staging.sql_etl_progress WHERE table_name = 'observation')
ORDER BY o.id
LIMIT 10000000; -- Process up to 10M records at a time to avoid memory issues

\echo 'Observation batches prepared:'
SELECT 
    batch_num, 
    COUNT(*) AS records, 
    MIN(id) AS min_id, 
    MAX(id) AS max_id
FROM 
    observations_to_process 
WHERE 
    row_type = 'observation'
GROUP BY 
    batch_num
ORDER BY 
    batch_num;

-- PROCESS OBSERVATION BATCHES
\echo 'PHASE 4: Processing observation batches...'

-- Process each batch for better progress tracking
DO $$
DECLARE
    batch_num INT;
    batch_min_id BIGINT;
    batch_max_id BIGINT;
    batch_count INT;
    batch_inserted INT;
    total_inserted INT := 0;
    observation_offset BIGINT;
BEGIN
    -- Get current offset
    SELECT offset_processed INTO observation_offset
    FROM staging.sql_etl_progress 
    WHERE table_name = 'observation';
    
    -- Loop through each batch
    FOR batch_num, batch_count, batch_min_id, batch_max_id IN 
        SELECT 
            batch_num, 
            COUNT(*), 
            MIN(id), 
            MAX(id)
        FROM 
            observations_to_process 
        WHERE 
            row_type = 'observation'
            AND NOT processed
        GROUP BY 
            batch_num
        ORDER BY 
            batch_num
    LOOP
        RAISE NOTICE 'Processing observation batch %: % records (IDs % to %)', 
                     batch_num, batch_count, batch_min_id, batch_max_id;
        
        -- Process this batch
        WITH inserted AS (
            INSERT INTO omop.observation (
                observation_id,
                person_id,
                observation_concept_id,
                observation_date,
                observation_datetime,
                observation_type_concept_id,
                value_as_number,
                value_as_string,
                value_as_concept_id,
                qualifier_concept_id,
                unit_concept_id,
                provider_id,
                visit_occurrence_id,
                visit_detail_id,
                observation_source_value,
                observation_source_concept_id,
                unit_source_value,
                qualifier_source_value,
                value_source_value,
                observation_event_id,
                obs_event_field_concept_id
            )
            SELECT
                NEXTVAL('staging.observation_seq') AS observation_id,
                pm.person_id,
                0 AS observation_concept_id,
                CAST(o.timestamp AS DATE) AS observation_date,
                o.timestamp AS observation_datetime,
                32817 AS observation_type_concept_id, -- "EHR"
                NULL AS value_as_number,
                o.value_as_string AS value_as_string,
                0 AS value_as_concept_id,
                0 AS qualifier_concept_id,
                0 AS unit_concept_id,
                NULL AS provider_id,
                vm.visit_occurrence_id,
                NULL AS visit_detail_id,
                o.code AS observation_source_value,
                0 AS observation_source_concept_id,
                NULL AS unit_source_value,
                NULL AS qualifier_source_value,
                o.value_as_string AS value_source_value,
                NULL AS observation_event_id,
                NULL AS obs_event_field_concept_id
            FROM 
                staging.observations_raw o
            JOIN 
                staging.person_map pm ON pm.source_patient_id = o.patient_id
            LEFT JOIN 
                staging.visit_map vm ON vm.source_visit_id = o.encounter_id
            WHERE
                NOT (o.observation_type = 'numeric' OR o.value_as_string ~ '^[-]?[0-9]+([.][0-9]+)?([eE][-+]?[0-9]+)?$')
                AND o.id BETWEEN batch_min_id AND batch_max_id
                AND NOT EXISTS (
                    SELECT 1 FROM omop.observation obs
                    JOIN staging.person_map pm2 ON pm2.person_id = obs.person_id
                    LEFT JOIN staging.visit_map vm2 ON vm2.visit_occurrence_id = obs.visit_occurrence_id
                    WHERE pm2.source_patient_id = o.patient_id
                    AND (vm2.source_visit_id = o.encounter_id OR (o.encounter_id IS NULL AND obs.visit_occurrence_id IS NULL))
                    AND obs.observation_source_value = o.code
                    AND obs.observation_date = o.timestamp::date
                    AND obs.value_source_value = o.value_as_string
                )
            RETURNING 1
        )
        SELECT COUNT(*) INTO batch_inserted FROM inserted;
        
        -- Mark as processed
        UPDATE observations_to_process 
        SET processed = TRUE 
        WHERE row_type = 'observation' AND batch_num = batch_num;
        
        -- Update the checkpoint table with the maximum ID processed
        UPDATE staging.sql_etl_progress 
        SET 
            max_id_processed = batch_max_id,
            offset_processed = batch_max_id,
            total_processed = total_processed + batch_inserted,
            last_updated = CURRENT_TIMESTAMP,
            status = 'processing'
        WHERE table_name = 'observation';

        -- Update our total
        total_inserted := total_inserted + batch_inserted;
        
        -- Log progress
        RAISE NOTICE 'Inserted % observations for batch %. Total: % (max ID: %)', 
                     batch_inserted, batch_num, total_inserted, batch_max_id;
    END LOOP;
    
    -- Mark as completed if we processed everything
    UPDATE staging.sql_etl_progress 
    SET status = 'completed'
    WHERE table_name = 'observation';
    
    RAISE NOTICE 'Total observations inserted: %', total_inserted;
END $$;

-- Report on observation processing
\echo 'OBSERVATION PROCESSING RESULTS:'
\echo '--------------------------'
\echo 'Observations progress:'
SELECT * FROM staging.sql_etl_progress WHERE table_name = 'observation';
\echo 'Current observation count:'
SELECT COUNT(*) FROM omop.observation;

-- Check if all batches are processed
\echo 'UNPROCESSED BATCHES (IF ANY):'
SELECT * FROM observations_to_process WHERE NOT processed;

-- Drop temp objects
DROP TABLE IF EXISTS observations_to_process;

-- Drop temporary indexes
\echo 'Dropping temporary indexes...'
DROP INDEX IF EXISTS tmp_obs_pat_idx;
DROP INDEX IF EXISTS tmp_obs_enc_idx;
DROP INDEX IF EXISTS tmp_obs_code_idx;
DROP INDEX IF EXISTS tmp_obs_val_idx;

-- Final counts
\echo 'FINAL RESULTS:'
\echo '--------------'
\echo 'Final measurement count:'
SELECT COUNT(*) FROM omop.measurement;

\echo 'Final observation count:'
SELECT COUNT(*) FROM omop.observation;

\echo 'Total records in OMOP tables:'
SELECT (SELECT COUNT(*) FROM omop.measurement) + (SELECT COUNT(*) FROM omop.observation) AS total_records;

\echo 'ETL process completed!'
