-- Direct SQL approach to load observations and measurements from staging to OMOP
-- This avoids Python overhead and provides maximum performance
\echo 'Starting direct SQL ETL for observations/measurements...'

-- Create sequences if they don't exist
CREATE SEQUENCE IF NOT EXISTS staging.observation_seq START 1 INCREMENT 1;
CREATE SEQUENCE IF NOT EXISTS staging.measurement_seq START 1 INCREMENT 1;

-- Count current state
\echo 'Current measurements:'
SELECT COUNT(*) FROM omop.measurement;

\echo 'Current observations:'
SELECT COUNT(*) FROM omop.observation;

-- Create temporary index to improve performance
\echo 'Creating temporary indexes for better performance...'
CREATE INDEX IF NOT EXISTS tmp_obs_pat_idx ON staging.observations_raw(patient_id);
CREATE INDEX IF NOT EXISTS tmp_obs_enc_idx ON staging.observations_raw(encounter_id);
CREATE INDEX IF NOT EXISTS tmp_obs_code_idx ON staging.observations_raw(code);

-- First: Process numeric values into measurement table
\echo 'Loading numeric observations into measurement table...'
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
    AND NOT EXISTS (
        SELECT 1 FROM omop.measurement m
        JOIN staging.person_map pm2 ON pm2.person_id = m.person_id
        LEFT JOIN staging.visit_map vm2 ON vm2.visit_occurrence_id = m.visit_occurrence_id
        WHERE pm2.source_patient_id = o.patient_id
        AND (vm2.source_visit_id = o.encounter_id OR (o.encounter_id IS NULL AND m.visit_occurrence_id IS NULL))
        AND m.measurement_source_value = o.code
        AND m.measurement_date = o.timestamp::date
        AND m.value_source_value = o.value_as_string
    );

\echo 'Measurements inserted:'
SELECT COUNT(*) FROM omop.measurement;

-- Second: Process non-numeric values into observation table
\echo 'Loading non-numeric observations into observation table...'
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
    AND NOT EXISTS (
        SELECT 1 FROM omop.observation obs
        JOIN staging.person_map pm2 ON pm2.person_id = obs.person_id
        LEFT JOIN staging.visit_map vm2 ON vm2.visit_occurrence_id = obs.visit_occurrence_id
        WHERE pm2.source_patient_id = o.patient_id
        AND (vm2.source_visit_id = o.encounter_id OR (o.encounter_id IS NULL AND obs.visit_occurrence_id IS NULL))
        AND obs.observation_source_value = o.code
        AND obs.observation_date = o.timestamp::date
        AND obs.value_source_value = o.value_as_string
    );

\echo 'Observations inserted:'
SELECT COUNT(*) FROM omop.observation;

-- Drop temporary indexes
\echo 'Dropping temporary indexes...'
DROP INDEX IF EXISTS tmp_obs_pat_idx;
DROP INDEX IF EXISTS tmp_obs_enc_idx;
DROP INDEX IF EXISTS tmp_obs_code_idx;

-- Final counts
\echo 'Final measurement count:'
SELECT COUNT(*) FROM omop.measurement;

\echo 'Final observation count:'
SELECT COUNT(*) FROM omop.observation;

\echo 'Total records in OMOP tables:'
SELECT (SELECT COUNT(*) FROM omop.measurement) + (SELECT COUNT(*) FROM omop.observation) AS total_records;

\echo 'ETL process completed!'
