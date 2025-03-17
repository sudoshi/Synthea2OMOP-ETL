-- Transform cost data from staging tables to OMOP schema

-- Create a sequence for cost_id if it doesn't exist
CREATE SEQUENCE IF NOT EXISTS staging.cost_seq START 1 INCREMENT 1;

-- Insert into cost table for visit costs
INSERT INTO omop.cost (
    cost_id,
    cost_event_id,
    cost_domain_id,
    cost_type_concept_id,
    currency_concept_id,
    total_charge,
    total_cost,
    total_paid,
    paid_by_payer,
    paid_by_patient,
    paid_patient_copay,
    paid_patient_coinsurance,
    paid_patient_deductible,
    paid_by_primary,
    paid_ingredient_cost,
    paid_dispensing_fee,
    payer_plan_period_id,
    amount_allowed,
    revenue_code_concept_id,
    revenue_code_source_value,
    drg_concept_id,
    drg_source_value
)
SELECT
    NEXTVAL('staging.cost_seq') AS cost_id,
    vo.visit_occurrence_id AS cost_event_id,
    'Visit' AS cost_domain_id,
    5031 AS cost_type_concept_id, -- "Calculated"
    44818668 AS currency_concept_id, -- "USD"
    e.total_claim_cost AS total_charge,
    e.base_encounter_cost AS total_cost,
    e.payer_coverage AS total_paid,
    e.payer_coverage AS paid_by_payer,
    e.total_claim_cost - e.payer_coverage AS paid_by_patient,
    NULL AS paid_patient_copay,
    NULL AS paid_patient_coinsurance,
    NULL AS paid_patient_deductible,
    NULL AS paid_by_primary,
    NULL AS paid_ingredient_cost,
    NULL AS paid_dispensing_fee,
    NULL AS payer_plan_period_id,
    NULL AS amount_allowed,
    NULL AS revenue_code_concept_id,
    NULL AS revenue_code_source_value,
    NULL AS drg_concept_id,
    NULL AS drg_source_value
FROM 
    staging.encounters_raw e
JOIN 
    staging.visit_map vm ON vm.source_visit_id = e.id
JOIN 
    omop.visit_occurrence vo ON vo.visit_occurrence_id = vm.visit_occurrence_id
WHERE 
    e.base_encounter_cost IS NOT NULL OR e.total_claim_cost IS NOT NULL OR e.payer_coverage IS NOT NULL;

-- Insert into cost table for medication costs
INSERT INTO omop.cost (
    cost_id,
    cost_event_id,
    cost_domain_id,
    cost_type_concept_id,
    currency_concept_id,
    total_charge,
    total_cost,
    total_paid,
    paid_by_payer,
    paid_by_patient,
    paid_patient_copay,
    paid_patient_coinsurance,
    paid_patient_deductible,
    paid_by_primary,
    paid_ingredient_cost,
    paid_dispensing_fee,
    payer_plan_period_id,
    amount_allowed,
    revenue_code_concept_id,
    revenue_code_source_value,
    drg_concept_id,
    drg_source_value
)
SELECT
    NEXTVAL('staging.cost_seq') AS cost_id,
    de.drug_exposure_id AS cost_event_id,
    'Drug' AS cost_domain_id,
    5031 AS cost_type_concept_id, -- "Calculated"
    44818668 AS currency_concept_id, -- "USD"
    m.total_cost AS total_charge,
    m.base_cost AS total_cost,
    m.payer_coverage AS total_paid,
    m.payer_coverage AS paid_by_payer,
    m.total_cost - m.payer_coverage AS paid_by_patient,
    NULL AS paid_patient_copay,
    NULL AS paid_patient_coinsurance,
    NULL AS paid_patient_deductible,
    NULL AS paid_by_primary,
    NULL AS paid_ingredient_cost,
    NULL AS paid_dispensing_fee,
    NULL AS payer_plan_period_id,
    NULL AS amount_allowed,
    NULL AS revenue_code_concept_id,
    NULL AS revenue_code_source_value,
    NULL AS drg_concept_id,
    NULL AS drg_source_value
FROM 
    staging.medications_raw m
JOIN 
    staging.person_map pm ON pm.source_patient_id = m.patient_id
JOIN 
    staging.visit_map vm ON vm.source_visit_id = m.encounter_id
JOIN 
    omop.drug_exposure de ON de.person_id = pm.person_id AND de.visit_occurrence_id = vm.visit_occurrence_id
WHERE 
    m.base_cost IS NOT NULL OR m.total_cost IS NOT NULL OR m.payer_coverage IS NOT NULL;

-- Insert into cost table for procedure costs
INSERT INTO omop.cost (
    cost_id,
    cost_event_id,
    cost_domain_id,
    cost_type_concept_id,
    currency_concept_id,
    total_charge,
    total_cost,
    total_paid,
    paid_by_payer,
    paid_by_patient,
    paid_patient_copay,
    paid_patient_coinsurance,
    paid_patient_deductible,
    paid_by_primary,
    paid_ingredient_cost,
    paid_dispensing_fee,
    payer_plan_period_id,
    amount_allowed,
    revenue_code_concept_id,
    revenue_code_source_value,
    drg_concept_id,
    drg_source_value
)
SELECT
    NEXTVAL('staging.cost_seq') AS cost_id,
    po.procedure_occurrence_id AS cost_event_id,
    'Procedure' AS cost_domain_id,
    5031 AS cost_type_concept_id, -- "Calculated"
    44818668 AS currency_concept_id, -- "USD"
    p.base_cost AS total_charge,
    p.base_cost AS total_cost,
    NULL AS total_paid,
    NULL AS paid_by_payer,
    NULL AS paid_by_patient,
    NULL AS paid_patient_copay,
    NULL AS paid_patient_coinsurance,
    NULL AS paid_patient_deductible,
    NULL AS paid_by_primary,
    NULL AS paid_ingredient_cost,
    NULL AS paid_dispensing_fee,
    NULL AS payer_plan_period_id,
    NULL AS amount_allowed,
    NULL AS revenue_code_concept_id,
    NULL AS revenue_code_source_value,
    NULL AS drg_concept_id,
    NULL AS drg_source_value
FROM 
    staging.procedures_raw p
JOIN 
    staging.person_map pm ON pm.source_patient_id = p.patient_id
JOIN 
    staging.visit_map vm ON vm.source_visit_id = p.encounter_id
JOIN 
    omop.procedure_occurrence po ON po.person_id = pm.person_id AND po.visit_occurrence_id = vm.visit_occurrence_id
WHERE 
    p.base_cost IS NOT NULL;

-- Log the results
SELECT 
    'Cost' AS table_name,
    COUNT(*) AS record_count
FROM 
    omop.cost;
