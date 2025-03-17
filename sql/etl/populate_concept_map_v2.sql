-- Populate the local_to_omop_concept_map table with mappings from source codes to OMOP concepts

-- Clear existing mappings
TRUNCATE TABLE staging.local_to_omop_concept_map;

-- Conditions (SNOMED-CT)
INSERT INTO staging.local_to_omop_concept_map (
    source_code,
    source_vocabulary,
    source_description,
    domain_id,
    target_concept_id,
    target_vocabulary_id,
    valid_start_date,
    valid_end_date
)
SELECT DISTINCT ON (c.code)
    c.code AS source_code,
    'SNOMED-CT' AS source_vocabulary,
    c.description AS source_description,
    'Condition' AS domain_id,
    concept.concept_id AS target_concept_id,
    concept.vocabulary_id AS target_vocabulary_id,
    concept.valid_start_date,
    concept.valid_end_date
FROM 
    population.conditions_typed c
JOIN 
    omop.concept concept ON concept.concept_code = c.code
WHERE 
    concept.vocabulary_id = 'SNOMED'
    AND concept.domain_id = 'Condition'
    AND concept.invalid_reason IS NULL;

-- Medications (RxNorm)
INSERT INTO staging.local_to_omop_concept_map (
    source_code,
    source_vocabulary,
    source_description,
    domain_id,
    target_concept_id,
    target_vocabulary_id,
    valid_start_date,
    valid_end_date
)
SELECT DISTINCT ON (m.code)
    m.code AS source_code,
    'RxNorm' AS source_vocabulary,
    m.description AS source_description,
    'Drug' AS domain_id,
    concept.concept_id AS target_concept_id,
    concept.vocabulary_id AS target_vocabulary_id,
    concept.valid_start_date,
    concept.valid_end_date
FROM 
    population.medications_typed m
JOIN 
    omop.concept concept ON concept.concept_code = m.code
WHERE 
    concept.vocabulary_id = 'RxNorm'
    AND concept.domain_id = 'Drug'
    AND concept.invalid_reason IS NULL;

-- Procedures (SNOMED-CT)
INSERT INTO staging.local_to_omop_concept_map (
    source_code,
    source_vocabulary,
    source_description,
    domain_id,
    target_concept_id,
    target_vocabulary_id,
    valid_start_date,
    valid_end_date
)
SELECT DISTINCT ON (p.code)
    p.code AS source_code,
    'SNOMED-CT' AS source_vocabulary,
    p.description AS source_description,
    'Procedure' AS domain_id,
    concept.concept_id AS target_concept_id,
    concept.vocabulary_id AS target_vocabulary_id,
    concept.valid_start_date,
    concept.valid_end_date
FROM 
    population.procedures_typed p
JOIN 
    omop.concept concept ON concept.concept_code = p.code
WHERE 
    concept.vocabulary_id = 'SNOMED'
    AND concept.domain_id = 'Procedure'
    AND concept.invalid_reason IS NULL;

-- Observations - Measurement (LOINC)
INSERT INTO staging.local_to_omop_concept_map (
    source_code,
    source_vocabulary,
    source_description,
    domain_id,
    target_concept_id,
    target_vocabulary_id,
    valid_start_date,
    valid_end_date
)
SELECT DISTINCT ON (o.code)
    o.code AS source_code,
    'LOINC' AS source_vocabulary,
    o.description AS source_description,
    'Measurement' AS domain_id,
    concept.concept_id AS target_concept_id,
    concept.vocabulary_id AS target_vocabulary_id,
    concept.valid_start_date,
    concept.valid_end_date
FROM 
    population.observations_typed o
JOIN 
    omop.concept concept ON concept.concept_code = o.code
WHERE 
    concept.vocabulary_id = 'LOINC'
    AND concept.invalid_reason IS NULL
    AND o.category = 'vital-signs';

-- Observations - Observation (LOINC)
INSERT INTO staging.local_to_omop_concept_map (
    source_code,
    source_vocabulary,
    source_description,
    domain_id,
    target_concept_id,
    target_vocabulary_id,
    valid_start_date,
    valid_end_date
)
SELECT DISTINCT ON (o.code)
    o.code AS source_code,
    'LOINC-OBS' AS source_vocabulary,  -- Use a different source_vocabulary to avoid PK conflict
    o.description AS source_description,
    'Observation' AS domain_id,
    concept.concept_id AS target_concept_id,
    concept.vocabulary_id AS target_vocabulary_id,
    concept.valid_start_date,
    concept.valid_end_date
FROM 
    population.observations_typed o
JOIN 
    omop.concept concept ON concept.concept_code = o.code
WHERE 
    concept.vocabulary_id = 'LOINC'
    AND concept.invalid_reason IS NULL
    AND (o.category IS NULL OR o.category != 'vital-signs');

-- Handle unmapped codes
-- For any codes that don't have direct mappings, we'll map them to the "No matching concept" concept (0)

-- Unmapped conditions
INSERT INTO staging.local_to_omop_concept_map (
    source_code,
    source_vocabulary,
    source_description,
    domain_id,
    target_concept_id,
    target_vocabulary_id,
    valid_start_date,
    valid_end_date
)
SELECT DISTINCT ON (c.code)
    c.code AS source_code,
    'SNOMED-CT' AS source_vocabulary,
    c.description AS source_description,
    'Condition' AS domain_id,
    0 AS target_concept_id,
    'SNOMED' AS target_vocabulary_id,
    '1970-01-01'::date AS valid_start_date,
    '2099-12-31'::date AS valid_end_date
FROM 
    population.conditions_typed c
LEFT JOIN 
    omop.concept concept ON concept.concept_code = c.code AND concept.vocabulary_id = 'SNOMED'
WHERE 
    concept.concept_id IS NULL;

-- Unmapped medications
INSERT INTO staging.local_to_omop_concept_map (
    source_code,
    source_vocabulary,
    source_description,
    domain_id,
    target_concept_id,
    target_vocabulary_id,
    valid_start_date,
    valid_end_date
)
SELECT DISTINCT ON (m.code)
    m.code AS source_code,
    'RxNorm' AS source_vocabulary,
    m.description AS source_description,
    'Drug' AS domain_id,
    0 AS target_concept_id,
    'RxNorm' AS target_vocabulary_id,
    '1970-01-01'::date AS valid_start_date,
    '2099-12-31'::date AS valid_end_date
FROM 
    population.medications_typed m
LEFT JOIN 
    omop.concept concept ON concept.concept_code = m.code AND concept.vocabulary_id = 'RxNorm'
WHERE 
    concept.concept_id IS NULL;

-- Unmapped procedures
INSERT INTO staging.local_to_omop_concept_map (
    source_code,
    source_vocabulary,
    source_description,
    domain_id,
    target_concept_id,
    target_vocabulary_id,
    valid_start_date,
    valid_end_date
)
SELECT DISTINCT ON (p.code)
    p.code AS source_code,
    'SNOMED-CT' AS source_vocabulary,
    p.description AS source_description,
    'Procedure' AS domain_id,
    0 AS target_concept_id,
    'SNOMED' AS target_vocabulary_id,
    '1970-01-01'::date AS valid_start_date,
    '2099-12-31'::date AS valid_end_date
FROM 
    population.procedures_typed p
LEFT JOIN 
    omop.concept concept ON concept.concept_code = p.code AND concept.vocabulary_id = 'SNOMED'
WHERE 
    concept.concept_id IS NULL;

-- Unmapped observations - Measurement
INSERT INTO staging.local_to_omop_concept_map (
    source_code,
    source_vocabulary,
    source_description,
    domain_id,
    target_concept_id,
    target_vocabulary_id,
    valid_start_date,
    valid_end_date
)
SELECT DISTINCT ON (o.code)
    o.code AS source_code,
    'LOINC' AS source_vocabulary,
    o.description AS source_description,
    'Measurement' AS domain_id,
    0 AS target_concept_id,
    'LOINC' AS target_vocabulary_id,
    '1970-01-01'::date AS valid_start_date,
    '2099-12-31'::date AS valid_end_date
FROM 
    population.observations_typed o
LEFT JOIN 
    omop.concept concept ON concept.concept_code = o.code AND concept.vocabulary_id = 'LOINC'
WHERE 
    concept.concept_id IS NULL
    AND o.category = 'vital-signs';

-- Unmapped observations - Observation
INSERT INTO staging.local_to_omop_concept_map (
    source_code,
    source_vocabulary,
    source_description,
    domain_id,
    target_concept_id,
    target_vocabulary_id,
    valid_start_date,
    valid_end_date
)
SELECT DISTINCT ON (o.code)
    o.code AS source_code,
    'LOINC-OBS' AS source_vocabulary,  -- Use a different source_vocabulary to avoid PK conflict
    o.description AS source_description,
    'Observation' AS domain_id,
    0 AS target_concept_id,
    'LOINC' AS target_vocabulary_id,
    '1970-01-01'::date AS valid_start_date,
    '2099-12-31'::date AS valid_end_date
FROM 
    population.observations_typed o
LEFT JOIN 
    omop.concept concept ON concept.concept_code = o.code AND concept.vocabulary_id = 'LOINC'
WHERE 
    concept.concept_id IS NULL
    AND (o.category IS NULL OR o.category != 'vital-signs');

-- Log the results
SELECT 
    source_vocabulary, 
    domain_id, 
    COUNT(*) AS mapping_count,
    SUM(CASE WHEN target_concept_id = 0 THEN 1 ELSE 0 END) AS unmapped_count
FROM 
    staging.local_to_omop_concept_map
GROUP BY 
    source_vocabulary, domain_id
ORDER BY 
    source_vocabulary, domain_id;
