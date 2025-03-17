-- Transform procedure data from staging tables to OMOP schema

-- Create a sequence for procedure_occurrence_id if it doesn't exist
CREATE SEQUENCE IF NOT EXISTS staging.procedure_occurrence_seq START 1 INCREMENT 1;

-- Insert into procedure_occurrence table
INSERT INTO omop.procedure_occurrence (
    procedure_occurrence_id,
    person_id,
    procedure_concept_id,
    procedure_date,
    procedure_datetime,
    procedure_end_date,
    procedure_end_datetime,
    procedure_type_concept_id,
    modifier_concept_id,
    quantity,
    provider_id,
    visit_occurrence_id,
    visit_detail_id,
    procedure_source_value,
    procedure_source_concept_id,
    modifier_source_value
)
SELECT
    NEXTVAL('staging.procedure_occurrence_seq') AS procedure_occurrence_id,
    pm.person_id,
    COALESCE(cm.target_concept_id, 0) AS procedure_concept_id,
    CAST(p.start_time AS DATE) AS procedure_date,
    p.start_time AS procedure_datetime,
    CAST(p.stop_time AS DATE) AS procedure_end_date,
    p.stop_time AS procedure_end_datetime,
    32817 AS procedure_type_concept_id, -- "EHR"
    NULL AS modifier_concept_id,
    NULL AS quantity,
    NULL AS provider_id,
    vm.visit_occurrence_id,
    NULL AS visit_detail_id,
    p.code AS procedure_source_value,
    0 AS procedure_source_concept_id,
    NULL AS modifier_source_value
FROM 
    staging.procedures_raw p
JOIN 
    staging.person_map pm ON pm.source_patient_id = p.patient_id
JOIN 
    staging.visit_map vm ON vm.source_visit_id = p.encounter_id
LEFT JOIN 
    staging.local_to_omop_concept_map cm ON cm.source_code = p.code AND cm.source_vocabulary = 'SNOMED-CT' AND cm.domain_id = 'Procedure';

-- Log the results
SELECT 
    'Procedure Occurrence' AS table_name,
    COUNT(*) AS record_count
FROM 
    omop.procedure_occurrence;
