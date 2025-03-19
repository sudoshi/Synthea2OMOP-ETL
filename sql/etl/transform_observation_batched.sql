-- Transform observation data from staging tables to OMOP schema using batched processing
-- This script focuses only on the observation table, not measurements

-- Ensure the observation_id sequence exists
CREATE SEQUENCE IF NOT EXISTS staging.observation_seq START 1 INCREMENT 1;

-- Log the start of the process
SELECT staging.log_progress('Transform Observations - Batched', 'start');

-- Set batch size (can be adjusted based on available memory)
\set batch_size 1000000

-- Create a temporary table to track batches
DROP TABLE IF EXISTS staging.observation_batch_tracker;
CREATE TABLE staging.observation_batch_tracker (
    batch_id SERIAL PRIMARY KEY,
    min_id BIGINT,
    max_id BIGINT,
    processed BOOLEAN DEFAULT FALSE,
    started_at TIMESTAMP,
    completed_at TIMESTAMP,
    record_count BIGINT DEFAULT 0
);

-- Determine the ID ranges for batching
-- First, find the min and max IDs in the source table
DO $$
DECLARE
    min_source_id BIGINT;
    max_source_id BIGINT;
    batch_count INTEGER;
    current_min_id BIGINT;
    i INTEGER;
    batch_size INTEGER := :batch_size;
BEGIN
    -- Get min and max IDs
    SELECT MIN(id), MAX(id) INTO min_source_id, max_source_id 
    FROM staging.observations_raw
    WHERE observation_type IS NULL OR observation_type NOT IN ('vital-signs');
    
    -- Calculate number of batches
    batch_count := CEIL((max_source_id - min_source_id + 1)::FLOAT / batch_size);
    
    RAISE NOTICE 'Processing observations from ID % to % in % batches of size %', 
                 min_source_id, max_source_id, batch_count, batch_size;
    
    -- Create batch records
    current_min_id := min_source_id;
    FOR i IN 1..batch_count LOOP
        INSERT INTO staging.observation_batch_tracker (min_id, max_id)
        VALUES (
            current_min_id,
            LEAST(current_min_id + batch_size - 1, max_source_id)
        );
        current_min_id := current_min_id + batch_size;
    END LOOP;
    
    RAISE NOTICE 'Created % batch tracking records', batch_count;
END $$;

-- Process each batch
DO $$
DECLARE
    batch RECORD;
    batch_count INTEGER;
    processed_count INTEGER := 0;
    error_count INTEGER := 0;
    start_time TIMESTAMP;
    end_time TIMESTAMP;
    duration INTERVAL;
    records_inserted BIGINT;
BEGIN
    SELECT COUNT(*) INTO batch_count FROM staging.observation_batch_tracker;
    RAISE NOTICE 'Starting to process % batches', batch_count;
    
    FOR batch IN SELECT * FROM staging.observation_batch_tracker ORDER BY batch_id LOOP
        BEGIN
            start_time := clock_timestamp();
            RAISE NOTICE 'Processing batch % (IDs % to %)', 
                         batch.batch_id, batch.min_id, batch.max_id;
            
            -- Mark batch as started
            UPDATE staging.observation_batch_tracker 
            SET started_at = start_time, processed = FALSE 
            WHERE batch_id = batch.batch_id;
            
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
                    CASE 
                        WHEN o.value_as_string ~ '^[0-9]+(\.[0-9]+)?$' THEN o.value_as_string::numeric
                        ELSE NULL
                    END AS value_as_number,
                    CASE 
                        WHEN o.value_as_string ~ '^[0-9]+(\.[0-9]+)?$' THEN NULL
                        ELSE SUBSTRING(o.value_as_string, 1, 60)
                    END AS value_as_string,
                    NULL AS value_as_concept_id,
                    NULL AS qualifier_concept_id,
                    NULL AS unit_concept_id,
                    NULL AS provider_id,
                    vm.visit_occurrence_id,
                    NULL AS visit_detail_id,
                    o.code AS observation_source_value,
                    0 AS observation_source_concept_id,
                    NULL AS unit_source_value,
                    NULL AS qualifier_source_value,
                    SUBSTRING(o.value_as_string, 1, 50) AS value_source_value,
                    NULL AS observation_event_id,
                    NULL AS obs_event_field_concept_id
                FROM 
                    staging.observations_raw o
                JOIN 
                    staging.person_map pm ON pm.source_patient_id = o.patient_id
                JOIN 
                    staging.visit_map vm ON vm.source_visit_id = o.encounter_id
                WHERE
                    (o.observation_type IS NULL OR observation_type NOT IN ('vital-signs'))
                    AND o.id BETWEEN batch.min_id AND batch.max_id
                RETURNING 1
            )
            SELECT COUNT(*) INTO records_inserted FROM inserted;
            
            -- Mark batch as completed
            end_time := clock_timestamp();
            duration := end_time - start_time;
            
            UPDATE staging.observation_batch_tracker 
            SET processed = TRUE, 
                completed_at = end_time,
                record_count = records_inserted
            WHERE batch_id = batch.batch_id;
            
            RAISE NOTICE 'Batch % completed: % records inserted in %', 
                         batch.batch_id, records_inserted, duration;
            
            processed_count := processed_count + 1;
            
            -- Commit this batch
            COMMIT;
            
        EXCEPTION WHEN OTHERS THEN
            -- Roll back this batch
            ROLLBACK;
            
            RAISE NOTICE 'Error processing batch %: %', batch.batch_id, SQLERRM;
            
            -- Log the error
            UPDATE staging.observation_batch_tracker 
            SET processed = FALSE
            WHERE batch_id = batch.batch_id;
            
            error_count := error_count + 1;
        END;
    END LOOP;
    
    RAISE NOTICE 'Batch processing complete: % batches processed, % errors', 
                 processed_count, error_count;
END $$;

-- Log the results
SELECT 
    'Observation (Batched)' AS table_name,
    COUNT(*) AS record_count
FROM 
    omop.observation;

-- Update the progress log
SELECT staging.log_progress(
    'Transform Observations - Batched', 
    'complete', 
    (SELECT COUNT(*) FROM omop.observation)
);

-- Log batch statistics
SELECT 
    COUNT(*) AS total_batches,
    COUNT(*) FILTER (WHERE processed) AS completed_batches,
    COUNT(*) FILTER (WHERE NOT processed) AS failed_batches,
    SUM(record_count) AS total_records_inserted,
    MIN(completed_at - started_at) AS min_duration,
    MAX(completed_at - started_at) AS max_duration,
    AVG(completed_at - started_at) AS avg_duration
FROM 
    staging.observation_batch_tracker
WHERE 
    started_at IS NOT NULL;
