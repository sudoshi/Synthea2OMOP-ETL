-- Transform observation data from staging tables to OMOP schema

-- Create sequences for observation_id and measurement_id if they don't exist
CREATE SEQUENCE IF NOT EXISTS staging.observation_seq START 1 INCREMENT 1;
CREATE SEQUENCE IF NOT EXISTS staging.measurement_seq START 1 INCREMENT 1;

-- Insert into measurement table for vital signs
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
    COALESCE(cm.target_concept_id, 0) AS measurement_concept_id,
    CAST(o.timestamp AS DATE) AS measurement_date,
    o.timestamp AS measurement_datetime,
    NULL AS measurement_time,
    32817 AS measurement_type_concept_id, -- "EHR"
    NULL AS operator_concept_id,
    CASE 
        WHEN o.value_as_string ~ '^[0-9]+(\.[0-9]+)?$' THEN o.value_as_string::numeric
        ELSE NULL
    END AS value_as_number,
    NULL AS value_as_concept_id,
    NULL AS unit_concept_id,
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
JOIN 
    staging.visit_map vm ON vm.source_visit_id = o.encounter_id
LEFT JOIN 
    staging.local_to_omop_concept_map cm ON cm.source_code = o.code AND cm.source_vocabulary = 'LOINC' AND cm.domain_id = 'Measurement';

-- Insert into observation table for non-vital signs
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
    COALESCE(cm.target_concept_id, 0) AS observation_concept_id,
    CAST(o.timestamp AS DATE) AS observation_date,
    o.timestamp AS observation_datetime,
    32817 AS observation_type_concept_id, -- "EHR"
    CASE 
        WHEN o.value_as_string ~ '^[0-9]+(\.[0-9]+)?$' THEN o.value_as_string::numeric
        ELSE NULL
    END AS value_as_number,
    CASE 
        WHEN o.value_as_string ~ '^[0-9]+(\.[0-9]+)?$' THEN NULL
        ELSE o.value_as_string
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
    o.value_as_string AS value_source_value,
    NULL AS observation_event_id,
    NULL AS obs_event_field_concept_id
FROM 
    staging.observations_raw o
JOIN 
    staging.person_map pm ON pm.source_patient_id = o.patient_id
JOIN 
    staging.visit_map vm ON vm.source_visit_id = o.encounter_id
LEFT JOIN 
    staging.local_to_omop_concept_map cm ON cm.source_code = o.code AND cm.source_vocabulary = 'LOINC-OBS' AND cm.domain_id = 'Observation';

-- Log the results
SELECT 
    'Measurement' AS table_name,
    COUNT(*) AS record_count
FROM 
    omop.measurement
UNION ALL
SELECT 
    'Observation' AS table_name,
    COUNT(*) AS record_count
FROM 
    omop.observation;
