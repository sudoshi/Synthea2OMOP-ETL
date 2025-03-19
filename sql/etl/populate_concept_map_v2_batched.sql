-- Populate the local_to_omop_concept_map table with mappings from source codes to OMOP concepts
-- This version processes large tables in batches to improve performance

-- Clear existing mappings
TRUNCATE TABLE staging.local_to_omop_concept_map;

-- Create a temporary table to track progress
DROP TABLE IF EXISTS staging.etl_progress;
CREATE TABLE staging.etl_progress (
    step_name VARCHAR(100) PRIMARY KEY,
    started_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    completed_at TIMESTAMP,
    status VARCHAR(20) DEFAULT 'in_progress',
    rows_processed BIGINT DEFAULT 0,
    error_message TEXT
);

-- Function to log progress
CREATE OR REPLACE FUNCTION staging.log_progress(step VARCHAR, status VARCHAR, rows_count BIGINT DEFAULT NULL, error TEXT DEFAULT NULL)
RETURNS VOID AS $$
BEGIN
    IF status = 'start' THEN
        INSERT INTO staging.etl_progress (step_name, status)
        VALUES (step, 'in_progress')
        ON CONFLICT (step_name) 
        DO UPDATE SET started_at = CURRENT_TIMESTAMP, status = 'in_progress', rows_processed = 0, error_message = NULL;
    ELSIF status = 'complete' THEN
        UPDATE staging.etl_progress 
        SET completed_at = CURRENT_TIMESTAMP, 
            status = 'completed', 
            rows_processed = COALESCE(rows_count, rows_processed)
        WHERE step_name = step;
    ELSIF status = 'error' THEN
        UPDATE staging.etl_progress 
        SET status = 'error', 
            error_message = error
        WHERE step_name = step;
    END IF;
END;
$$ LANGUAGE plpgsql;

-- Conditions (SNOMED-CT)
SELECT staging.log_progress('Conditions (SNOMED-CT)', 'start');

BEGIN;
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

SELECT staging.log_progress('Conditions (SNOMED-CT)', 'complete', (SELECT COUNT(*) FROM staging.local_to_omop_concept_map WHERE source_vocabulary = 'SNOMED-CT' AND domain_id = 'Condition'));
COMMIT;

-- Medications (RxNorm)
SELECT staging.log_progress('Medications (RxNorm)', 'start');

BEGIN;
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

SELECT staging.log_progress('Medications (RxNorm)', 'complete', (SELECT COUNT(*) FROM staging.local_to_omop_concept_map WHERE source_vocabulary = 'RxNorm' AND domain_id = 'Drug'));
COMMIT;

-- Procedures (SNOMED-CT)
SELECT staging.log_progress('Procedures (SNOMED-CT)', 'start');

BEGIN;
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

SELECT staging.log_progress('Procedures (SNOMED-CT)', 'complete', (SELECT COUNT(*) FROM staging.local_to_omop_concept_map WHERE source_vocabulary = 'SNOMED-CT' AND domain_id = 'Procedure'));
COMMIT;

-- Observations - Measurement (LOINC) - Process in batches
SELECT staging.log_progress('Observations - Measurement (LOINC)', 'start');

-- Create a table to store distinct LOINC codes for vital signs
DROP TABLE IF EXISTS staging.temp_loinc_vital_signs;
CREATE TABLE staging.temp_loinc_vital_signs AS
SELECT DISTINCT code
FROM population.observations_typed
WHERE category = 'vital-signs'
AND code IN (
    SELECT concept_code 
    FROM omop.concept 
    WHERE vocabulary_id = 'LOINC' 
    AND invalid_reason IS NULL
);

-- Get the count of distinct codes
DO $$
DECLARE
    total_codes INTEGER;
    batch_size INTEGER := 1000;
    batch_count INTEGER;
    i INTEGER;
BEGIN
    SELECT COUNT(*) INTO total_codes FROM staging.temp_loinc_vital_signs;
    batch_count := CEIL(total_codes::FLOAT / batch_size);
    
    RAISE NOTICE 'Processing % distinct LOINC codes in % batches', total_codes, batch_count;
    
    -- Process each batch
    FOR i IN 0..(batch_count-1) LOOP
        RAISE NOTICE 'Processing batch % of %', i+1, batch_count;
        
        BEGIN
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
            JOIN 
                (SELECT code FROM staging.temp_loinc_vital_signs 
                 ORDER BY code 
                 LIMIT batch_size OFFSET (i * batch_size)) batch_codes
                ON o.code = batch_codes.code
            WHERE 
                concept.vocabulary_id = 'LOINC'
                AND concept.invalid_reason IS NULL
                AND o.category = 'vital-signs'
            LIMIT 1000000; -- Safety limit
            
            COMMIT;
            
        EXCEPTION WHEN OTHERS THEN
            ROLLBACK;
            RAISE NOTICE 'Error in batch %: %', i+1, SQLERRM;
            PERFORM staging.log_progress('Observations - Measurement (LOINC) - Batch ' || (i+1), 'error', NULL, SQLERRM);
        END;
    END LOOP;
END $$;

SELECT staging.log_progress('Observations - Measurement (LOINC)', 'complete', (SELECT COUNT(*) FROM staging.local_to_omop_concept_map WHERE source_vocabulary = 'LOINC' AND domain_id = 'Measurement'));

-- Observations - Observation (LOINC) - Process in batches
SELECT staging.log_progress('Observations - Observation (LOINC)', 'start');

-- Create a table to store distinct LOINC codes for non-vital signs
DROP TABLE IF EXISTS staging.temp_loinc_non_vital_signs;
CREATE TABLE staging.temp_loinc_non_vital_signs AS
SELECT DISTINCT code
FROM population.observations_typed
WHERE (category IS NULL OR category != 'vital-signs')
AND code IN (
    SELECT concept_code 
    FROM omop.concept 
    WHERE vocabulary_id = 'LOINC' 
    AND invalid_reason IS NULL
);

-- Get the count of distinct codes
DO $$
DECLARE
    total_codes INTEGER;
    batch_size INTEGER := 1000;
    batch_count INTEGER;
    i INTEGER;
BEGIN
    SELECT COUNT(*) INTO total_codes FROM staging.temp_loinc_non_vital_signs;
    batch_count := CEIL(total_codes::FLOAT / batch_size);
    
    RAISE NOTICE 'Processing % distinct LOINC codes in % batches', total_codes, batch_count;
    
    -- Process each batch
    FOR i IN 0..(batch_count-1) LOOP
        RAISE NOTICE 'Processing batch % of %', i+1, batch_count;
        
        BEGIN
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
            JOIN 
                (SELECT code FROM staging.temp_loinc_non_vital_signs 
                 ORDER BY code 
                 LIMIT batch_size OFFSET (i * batch_size)) batch_codes
                ON o.code = batch_codes.code
            WHERE 
                concept.vocabulary_id = 'LOINC'
                AND concept.invalid_reason IS NULL
                AND (o.category IS NULL OR o.category != 'vital-signs')
            LIMIT 1000000; -- Safety limit
            
            COMMIT;
            
        EXCEPTION WHEN OTHERS THEN
            ROLLBACK;
            RAISE NOTICE 'Error in batch %: %', i+1, SQLERRM;
            PERFORM staging.log_progress('Observations - Observation (LOINC) - Batch ' || (i+1), 'error', NULL, SQLERRM);
        END;
    END LOOP;
END $$;

SELECT staging.log_progress('Observations - Observation (LOINC)', 'complete', (SELECT COUNT(*) FROM staging.local_to_omop_concept_map WHERE source_vocabulary = 'LOINC-OBS' AND domain_id = 'Observation'));

-- Handle unmapped codes
-- For any codes that don't have direct mappings, we'll map them to the "No matching concept" concept (0)

-- Unmapped conditions
SELECT staging.log_progress('Unmapped conditions', 'start');

BEGIN;
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

SELECT staging.log_progress('Unmapped conditions', 'complete', (SELECT COUNT(*) FROM staging.local_to_omop_concept_map WHERE source_vocabulary = 'SNOMED-CT' AND domain_id = 'Condition' AND target_concept_id = 0));
COMMIT;

-- Unmapped medications
SELECT staging.log_progress('Unmapped medications', 'start');

BEGIN;
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

SELECT staging.log_progress('Unmapped medications', 'complete', (SELECT COUNT(*) FROM staging.local_to_omop_concept_map WHERE source_vocabulary = 'RxNorm' AND domain_id = 'Drug' AND target_concept_id = 0));
COMMIT;

-- Unmapped procedures
SELECT staging.log_progress('Unmapped procedures', 'start');

BEGIN;
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

SELECT staging.log_progress('Unmapped procedures', 'complete', (SELECT COUNT(*) FROM staging.local_to_omop_concept_map WHERE source_vocabulary = 'SNOMED-CT' AND domain_id = 'Procedure' AND target_concept_id = 0));
COMMIT;

-- Unmapped observations - Measurement - Process in batches
SELECT staging.log_progress('Unmapped observations - Measurement', 'start');

-- Create a table to store distinct unmapped LOINC codes for vital signs
DROP TABLE IF EXISTS staging.temp_unmapped_loinc_vital_signs;
CREATE TABLE staging.temp_unmapped_loinc_vital_signs AS
SELECT DISTINCT code
FROM population.observations_typed
WHERE category = 'vital-signs'
AND code NOT IN (
    SELECT concept_code 
    FROM omop.concept 
    WHERE vocabulary_id = 'LOINC'
);

-- Get the count of distinct codes
DO $$
DECLARE
    total_codes INTEGER;
    batch_size INTEGER := 1000;
    batch_count INTEGER;
    i INTEGER;
BEGIN
    SELECT COUNT(*) INTO total_codes FROM staging.temp_unmapped_loinc_vital_signs;
    batch_count := CEIL(total_codes::FLOAT / batch_size);
    
    RAISE NOTICE 'Processing % distinct unmapped LOINC codes in % batches', total_codes, batch_count;
    
    -- Process each batch
    FOR i IN 0..(batch_count-1) LOOP
        RAISE NOTICE 'Processing batch % of %', i+1, batch_count;
        
        BEGIN
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
            JOIN 
                (SELECT code FROM staging.temp_unmapped_loinc_vital_signs 
                 ORDER BY code 
                 LIMIT batch_size OFFSET (i * batch_size)) batch_codes
                ON o.code = batch_codes.code
            WHERE 
                o.category = 'vital-signs'
            LIMIT 1000000; -- Safety limit
            
            COMMIT;
            
        EXCEPTION WHEN OTHERS THEN
            ROLLBACK;
            RAISE NOTICE 'Error in batch %: %', i+1, SQLERRM;
            PERFORM staging.log_progress('Unmapped observations - Measurement - Batch ' || (i+1), 'error', NULL, SQLERRM);
        END;
    END LOOP;
END $$;

SELECT staging.log_progress('Unmapped observations - Measurement', 'complete', (SELECT COUNT(*) FROM staging.local_to_omop_concept_map WHERE source_vocabulary = 'LOINC' AND domain_id = 'Measurement' AND target_concept_id = 0));

-- Unmapped observations - Observation - Process in batches
SELECT staging.log_progress('Unmapped observations - Observation', 'start');

-- Create a table to store distinct unmapped LOINC codes for non-vital signs
DROP TABLE IF EXISTS staging.temp_unmapped_loinc_non_vital_signs;
CREATE TABLE staging.temp_unmapped_loinc_non_vital_signs AS
SELECT DISTINCT code
FROM population.observations_typed
WHERE (category IS NULL OR category != 'vital-signs')
AND code NOT IN (
    SELECT concept_code 
    FROM omop.concept 
    WHERE vocabulary_id = 'LOINC'
);

-- Get the count of distinct codes
DO $$
DECLARE
    total_codes INTEGER;
    batch_size INTEGER := 1000;
    batch_count INTEGER;
    i INTEGER;
BEGIN
    SELECT COUNT(*) INTO total_codes FROM staging.temp_unmapped_loinc_non_vital_signs;
    batch_count := CEIL(total_codes::FLOAT / batch_size);
    
    RAISE NOTICE 'Processing % distinct unmapped LOINC codes in % batches', total_codes, batch_count;
    
    -- Process each batch
    FOR i IN 0..(batch_count-1) LOOP
        RAISE NOTICE 'Processing batch % of %', i+1, batch_count;
        
        BEGIN
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
            JOIN 
                (SELECT code FROM staging.temp_unmapped_loinc_non_vital_signs 
                 ORDER BY code 
                 LIMIT batch_size OFFSET (i * batch_size)) batch_codes
                ON o.code = batch_codes.code
            WHERE 
                (o.category IS NULL OR o.category != 'vital-signs')
            LIMIT 1000000; -- Safety limit
            
            COMMIT;
            
        EXCEPTION WHEN OTHERS THEN
            ROLLBACK;
            RAISE NOTICE 'Error in batch %: %', i+1, SQLERRM;
            PERFORM staging.log_progress('Unmapped observations - Observation - Batch ' || (i+1), 'error', NULL, SQLERRM);
        END;
    END LOOP;
END $$;

SELECT staging.log_progress('Unmapped observations - Observation', 'complete', (SELECT COUNT(*) FROM staging.local_to_omop_concept_map WHERE source_vocabulary = 'LOINC-OBS' AND domain_id = 'Observation' AND target_concept_id = 0));

-- Log the results
SELECT 
    source_vocabulary, 
    domain_id, 
    COUNT(*) AS mapping_count,
    SUM(CASE WHEN target_concept_id = 0 THEN 1 ELSE 0 END) AS unmapped_count,
    ROUND(100.0 * SUM(CASE WHEN target_concept_id = 0 THEN 1 ELSE 0 END) / COUNT(*), 2) AS unmapped_percentage
FROM 
    staging.local_to_omop_concept_map
GROUP BY 
    source_vocabulary, domain_id
ORDER BY 
    source_vocabulary, domain_id;

-- Log the ETL progress
SELECT 
    step_name, 
    started_at, 
    completed_at, 
    status, 
    rows_processed, 
    error_message
FROM 
    staging.etl_progress
ORDER BY 
    started_at;

-- Clean up temporary tables
DROP TABLE IF EXISTS staging.temp_loinc_vital_signs;
DROP TABLE IF EXISTS staging.temp_loinc_non_vital_signs;
DROP TABLE IF EXISTS staging.temp_unmapped_loinc_vital_signs;
DROP TABLE IF EXISTS staging.temp_unmapped_loinc_non_vital_signs;
