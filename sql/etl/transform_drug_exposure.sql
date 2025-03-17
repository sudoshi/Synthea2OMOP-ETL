-- Transform medication data from staging tables to OMOP schema

-- Create a sequence for drug_exposure_id if it doesn't exist
CREATE SEQUENCE IF NOT EXISTS staging.drug_exposure_seq START 1 INCREMENT 1;

-- Insert into drug_exposure table
INSERT INTO omop.drug_exposure (
    drug_exposure_id,
    person_id,
    drug_concept_id,
    drug_exposure_start_date,
    drug_exposure_start_datetime,
    drug_exposure_end_date,
    drug_exposure_end_datetime,
    verbatim_end_date,
    drug_type_concept_id,
    stop_reason,
    refills,
    quantity,
    days_supply,
    sig,
    route_concept_id,
    lot_number,
    provider_id,
    visit_occurrence_id,
    visit_detail_id,
    drug_source_value,
    drug_source_concept_id,
    route_source_value,
    dose_unit_source_value
)
SELECT
    NEXTVAL('staging.drug_exposure_seq') AS drug_exposure_id,
    pm.person_id,
    COALESCE(cm.target_concept_id, 0) AS drug_concept_id,
    CAST(m.start_timestamp AS DATE) AS drug_exposure_start_date,
    m.start_timestamp AS drug_exposure_start_datetime,
    CAST(m.stop_timestamp AS DATE) AS drug_exposure_end_date,
    m.stop_timestamp AS drug_exposure_end_datetime,
    NULL AS verbatim_end_date,
    32817 AS drug_type_concept_id, -- "EHR"
    NULL AS stop_reason,
    m.dispenses AS refills,
    NULL AS quantity,
    NULL AS days_supply,
    NULL AS sig,
    NULL AS route_concept_id,
    NULL AS lot_number,
    NULL AS provider_id,
    vm.visit_occurrence_id,
    NULL AS visit_detail_id,
    m.code AS drug_source_value,
    0 AS drug_source_concept_id,
    NULL AS route_source_value,
    NULL AS dose_unit_source_value
FROM 
    staging.medications_raw m
JOIN 
    staging.person_map pm ON pm.source_patient_id = m.patient_id
JOIN 
    staging.visit_map vm ON vm.source_visit_id = m.encounter_id
LEFT JOIN 
    staging.local_to_omop_concept_map cm ON cm.source_code = m.code AND cm.source_vocabulary = 'RxNorm' AND cm.domain_id = 'Drug';

-- Log the results
SELECT 
    'Drug Exposure' AS table_name,
    COUNT(*) AS record_count
FROM 
    omop.drug_exposure;
