-- Direct transfer of non-numeric measurements to observation table
-- This script uses a single transaction for better performance and simplicity

-- Enable timing to measure performance
\timing on

-- Log the start of the process
SELECT staging.log_progress('Direct Transfer Non-numeric to Observation', 'start');

-- Create observation_id sequence if it doesn't exist
CREATE SEQUENCE IF NOT EXISTS staging.observation_seq START 1 INCREMENT 1;

-- Count records before transfer
\echo 'Counting records before transfer...'
SELECT 'Measurement' AS table_name, COUNT(*) AS total_count,
       COUNT(*) FILTER (WHERE value_as_number IS NULL) AS non_numeric_count
FROM omop.measurement;

SELECT 'Observation' AS table_name, COUNT(*) AS record_count
FROM omop.observation;

-- Start the transfer
\echo 'Starting transfer of non-numeric measurements to observation table...'

-- Insert non-numeric measurements into observation table
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
    NEXTVAL('staging.observation_seq'),
    person_id,
    measurement_concept_id,
    measurement_date,
    measurement_datetime,
    measurement_type_concept_id,
    NULL,
    value_source_value,
    value_as_concept_id,
    operator_concept_id,
    unit_concept_id,
    provider_id,
    visit_occurrence_id,
    visit_detail_id,
    measurement_source_value,
    measurement_source_concept_id,
    unit_source_value,
    NULL,
    value_source_value,
    measurement_event_id,
    meas_event_field_concept_id
FROM
    omop.measurement
WHERE
    value_as_number IS NULL;

-- Get the number of records inserted
\echo 'Transfer completed. Getting record counts...'
SELECT 'Records transferred' AS description, COUNT(*) AS count
FROM omop.observation
WHERE observation_id >= (SELECT last_value FROM staging.observation_seq) - COUNT(*);

-- Count records after transfer
SELECT 'Measurement' AS table_name, COUNT(*) AS total_count,
       COUNT(*) FILTER (WHERE value_as_number IS NULL) AS non_numeric_count
FROM omop.measurement;

SELECT 'Observation' AS table_name, COUNT(*) AS record_count
FROM omop.observation;

-- Log completion
SELECT staging.log_progress(
    'Direct Transfer Non-numeric to Observation', 
    'complete', 
    (SELECT COUNT(*) FROM omop.observation)
);

\echo 'Transfer process completed successfully!'
