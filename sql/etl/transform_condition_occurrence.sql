-- Transform condition data from staging tables to OMOP schema

-- Create a sequence for condition_occurrence_id if it doesn't exist
CREATE SEQUENCE IF NOT EXISTS staging.condition_occurrence_seq START 1 INCREMENT 1;

-- Insert into condition_occurrence table
INSERT INTO omop.condition_occurrence (
    condition_occurrence_id,
    person_id,
    condition_concept_id,
    condition_start_date,
    condition_start_datetime,
    condition_end_date,
    condition_end_datetime,
    condition_type_concept_id,
    condition_status_concept_id,
    stop_reason,
    provider_id,
    visit_occurrence_id,
    visit_detail_id,
    condition_source_value,
    condition_source_concept_id,
    condition_status_source_value
)
SELECT
    NEXTVAL('staging.condition_occurrence_seq') AS condition_occurrence_id,
    pm.person_id,
    COALESCE(cm.target_concept_id, 0) AS condition_concept_id,
    CAST(c.start_date AS DATE) AS condition_start_date,
    c.start_datetime AS condition_start_datetime,
    CAST(c.stop_date AS DATE) AS condition_end_date,
    c.stop_datetime AS condition_end_datetime,
    32817 AS condition_type_concept_id, -- "EHR"
    NULL AS condition_status_concept_id,
    NULL AS stop_reason,
    NULL AS provider_id,
    vm.visit_occurrence_id,
    NULL AS visit_detail_id,
    c.code AS condition_source_value,
    0 AS condition_source_concept_id,
    NULL AS condition_status_source_value
FROM 
    staging.conditions_raw c
JOIN 
    staging.person_map pm ON pm.source_patient_id = c.patient_id
JOIN 
    staging.visit_map vm ON vm.source_visit_id = c.encounter_id
LEFT JOIN 
    staging.local_to_omop_concept_map cm ON cm.source_code = c.code AND cm.source_vocabulary = 'SNOMED-CT' AND cm.domain_id = 'Condition';

-- Log the results
SELECT 
    'Condition Occurrence' AS table_name,
    COUNT(*) AS record_count
FROM 
    omop.condition_occurrence;
