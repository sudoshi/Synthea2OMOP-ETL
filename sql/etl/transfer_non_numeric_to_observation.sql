-- Transfer non-numeric measurements to the observation table using batched processing
-- This script identifies qualitative (non-numeric) values in the measurement table
-- and moves them to the observation table where they belong according to OMOP CDM guidelines

-- Log the start of the process
SELECT staging.log_progress('Transfer Non-numeric to Observation', 'start');

-- Create observation_id sequence if it doesn't exist
CREATE SEQUENCE IF NOT EXISTS staging.observation_seq START 1 INCREMENT 1;

-- Count non-numeric measurements before transfer
SELECT COUNT(*) AS non_numeric_measurement_count
FROM omop.measurement
WHERE value_as_number IS NULL
  AND value_source_value !~ E'^[0-9]+(\\.[0-9]+)?$';

-- Get the minimum and maximum measurement_id for non-numeric measurements
SELECT MIN(measurement_id) AS min_id, MAX(measurement_id) AS max_id
FROM omop.measurement
WHERE value_as_number IS NULL
  AND value_source_value !~ E'^[0-9]+(\\.[0-9]+)?$';

-- Set batch size
\set batch_size 1000000

-- Create a temporary table to store batch information
CREATE TEMP TABLE temp_batch_info (
    batch_id SERIAL PRIMARY KEY,
    min_id BIGINT,
    max_id BIGINT,
    processed BOOLEAN DEFAULT FALSE,
    record_count BIGINT DEFAULT 0
);

-- Generate batch ranges
INSERT INTO temp_batch_info (min_id, max_id)
SELECT 
    min_id + (batch_num * :batch_size) AS min_id,
    LEAST(min_id + ((batch_num + 1) * :batch_size) - 1, max_id) AS max_id
FROM 
    (SELECT MIN(measurement_id) AS min_id, MAX(measurement_id) AS max_id
     FROM omop.measurement
     WHERE value_as_number IS NULL
       AND value_source_value !~ E'^[0-9]+(\\.[0-9]+)?$') AS id_range,
    generate_series(0, CEIL((max_id - min_id + 1)::FLOAT / :batch_size)::INTEGER - 1) AS batch_num;

-- Display batch information
SELECT COUNT(*) AS batch_count FROM temp_batch_info;

-- Create a function to process a single batch
CREATE OR REPLACE FUNCTION process_batch(p_batch_id INTEGER, p_min_id BIGINT, p_max_id BIGINT) RETURNS INTEGER AS $$
DECLARE
    v_inserted_count INTEGER;
BEGIN
    -- Insert records from this batch into observation table
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
            person_id,
            measurement_concept_id AS observation_concept_id,
            measurement_date AS observation_date,
            measurement_datetime AS observation_datetime,
            measurement_type_concept_id AS observation_type_concept_id,
            NULL AS value_as_number,
            value_source_value AS value_as_string,
            value_as_concept_id,
            operator_concept_id AS qualifier_concept_id,
            unit_concept_id,
            provider_id,
            visit_occurrence_id,
            visit_detail_id,
            measurement_source_value AS observation_source_value,
            measurement_source_concept_id AS observation_source_concept_id,
            unit_source_value,
            NULL AS qualifier_source_value,
            value_source_value,
            NULL AS observation_event_id,
            NULL AS obs_event_field_concept_id
        FROM
            omop.measurement
        WHERE
            value_as_number IS NULL
            AND value_source_value !~ E'^[0-9]+(\\.[0-9]+)?$'
            AND measurement_id BETWEEN p_min_id AND p_max_id
        RETURNING 1
    )
    SELECT COUNT(*) INTO v_inserted_count FROM inserted;
    
    -- Update batch information
    UPDATE temp_batch_info 
    SET processed = TRUE, 
        record_count = v_inserted_count
    WHERE batch_id = p_batch_id;
    
    RETURN v_inserted_count;
END;
$$ LANGUAGE plpgsql;

-- Process each batch
SELECT 
    'Processing batch ' || batch_id || ' of ' || (SELECT COUNT(*) FROM temp_batch_info) AS batch_info,
    min_id,
    max_id,
    process_batch(batch_id, min_id, max_id) AS records_inserted,
    (SELECT SUM(record_count) FROM temp_batch_info WHERE processed) AS total_processed,
    (SELECT 100.0 * SUM(record_count) / 
        (SELECT COUNT(*) FROM omop.measurement 
         WHERE value_as_number IS NULL AND value_source_value !~ E'^[0-9]+(\\.[0-9]+)?$')
     FROM temp_batch_info WHERE processed) AS progress_percent
FROM 
    temp_batch_info
ORDER BY 
    batch_id;

-- Update the ETL progress log with the final count
SELECT staging.log_progress(
    'Transfer Non-numeric to Observation', 
    'complete', 
    (SELECT COUNT(*) FROM omop.observation)
);

-- Display batch statistics
SELECT 
    COUNT(*) AS total_batches,
    COUNT(*) FILTER (WHERE processed) AS completed_batches,
    SUM(record_count) AS total_records_processed
FROM 
    temp_batch_info;

-- Optionally, delete the transferred records from the measurement table
-- Uncomment the following lines if you want to remove these records from the measurement table
/*
DELETE FROM omop.measurement
WHERE value_as_number IS NULL
  AND value_source_value !~ E'^[0-9]+(\\.[0-9]+)?$';
*/

-- Display final counts
SELECT 'Measurement' AS table_name, COUNT(*) AS record_count FROM omop.measurement
UNION ALL
SELECT 'Observation' AS table_name, COUNT(*) AS record_count FROM omop.observation
ORDER BY table_name;

-- Clean up
DROP FUNCTION IF EXISTS process_batch(INTEGER, BIGINT, BIGINT);
DROP TABLE IF EXISTS temp_batch_info;
