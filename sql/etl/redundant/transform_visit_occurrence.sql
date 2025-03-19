-- Transform visit data from population schema to OMOP schema

-- Create a sequence for visit_occurrence_id if it doesn't exist
CREATE SEQUENCE IF NOT EXISTS staging.visit_occurrence_seq START 1 INCREMENT 1;

-- Insert into visit_occurrence table
INSERT INTO omop.visit_occurrence (
    visit_occurrence_id,
    person_id,
    visit_concept_id,
    visit_start_date,
    visit_start_datetime,
    visit_end_date,
    visit_end_datetime,
    visit_type_concept_id,
    provider_id,
    care_site_id,
    visit_source_value,
    visit_source_concept_id,
    admitted_from_concept_id,
    admitted_from_source_value,
    discharge_to_concept_id,
    discharge_to_source_value,
    preceding_visit_occurrence_id
)
SELECT
    vm.visit_occurrence_id,
    pm.person_id,
    CASE 
        WHEN e.encounter_class ILIKE '%inpatient%'  THEN 9201  -- Inpatient Visit
        WHEN e.encounter_class ILIKE '%outpatient%' THEN 9202  -- Outpatient Visit
        WHEN e.encounter_class ILIKE '%emergency%' OR e.encounter_class ILIKE '%er%' THEN 9203  -- Emergency Room Visit
        WHEN e.encounter_class ILIKE '%ambulatory%' THEN 9202  -- Outpatient Visit
        WHEN e.encounter_class ILIKE '%wellness%' THEN 9202    -- Outpatient Visit
        WHEN e.encounter_class ILIKE '%home%' THEN 581476      -- Home Visit
        WHEN e.encounter_class ILIKE '%hospice%' THEN 42898160 -- Hospice Visit
        WHEN e.encounter_class ILIKE '%office%' THEN 9202      -- Outpatient Visit
        WHEN e.encounter_class ILIKE '%virtual%' THEN 5083     -- Virtual Visit
        ELSE 0                                                 -- No matching concept
    END AS visit_concept_id,
    
    CAST(e.start_time AS DATE) AS visit_start_date,
    e.start_time AS visit_start_datetime,
    CAST(e.stop_time AS DATE) AS visit_end_date,
    e.stop_time AS visit_end_datetime,
    
    44818518 AS visit_type_concept_id, -- "Visit derived from EHR"
    
    NULL AS provider_id,      -- Could join to provider_map if available
    NULL AS care_site_id,     -- Could join to care_site_map if available
    
    e.encounter_class AS visit_source_value,
    0 AS visit_source_concept_id,
    NULL AS admitted_from_concept_id,
    NULL AS admitted_from_source_value,
    NULL AS discharge_to_concept_id,
    NULL AS discharge_to_source_value,
    NULL AS preceding_visit_occurrence_id
FROM 
    population.encounters_typed e
JOIN 
    staging.person_map pm ON pm.source_patient_id = e.patient::text
LEFT JOIN 
    staging.visit_map vm ON vm.source_visit_id = e.encounter_id::text
WHERE 
    vm.visit_occurrence_id IS NOT NULL
    AND vm.visit_occurrence_id NOT IN (
        SELECT visit_occurrence_id
        FROM omop.visit_occurrence
    );

-- Log the results
SELECT 
    'Visit Occurrence' AS table_name,
    COUNT(*) AS record_count
FROM 
    omop.visit_occurrence;
