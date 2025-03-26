-- transfer_population_to_staging.sql
-- Efficiently transfers data from population_typed tables to staging tables
-- Uses batch processing for large tables to prevent memory issues

\set QUIET on
\set ON_ERROR_STOP true

-- Function to report progress to client
CREATE OR REPLACE FUNCTION public.report_progress(step_name text, current_count bigint, total bigint) RETURNS void AS $$
BEGIN
    RAISE NOTICE 'progress: %/%  %', current_count, total, step_name;
END;
$$ LANGUAGE plpgsql;

-- Create ETL progress tracking table if it doesn't exist
CREATE TABLE IF NOT EXISTS staging.etl_progress (
    step_name varchar(100) PRIMARY KEY,
    status varchar(20),
    started_at timestamp DEFAULT CURRENT_TIMESTAMP,
    completed_at timestamp,
    rows_processed bigint DEFAULT 0,
    error_message text
);

\set QUIET off
\echo 'Starting population to staging transfer with batch processing...'
\echo 'progress: 0/19 Starting population to staging transfer'

BEGIN;

-- Optimize PostgreSQL settings for bulk operations
SET work_mem = '256MB';
SET maintenance_work_mem = '1GB';
SET synchronous_commit = 'off';
SET constraint_exclusion = 'on';
SET temp_buffers = '256MB';

-- 1) PATIENTS
\echo 'progress: 1/19 Transferring patients data'
TRUNCATE TABLE staging.patients_raw;

INSERT INTO staging.patients_raw (
    id, birthdate, deathdate, race, ethnicity, gender, 
    first_name, last_name, address, city, state, zip, county,
    latitude, longitude, income, healthcare_expenses, healthcare_coverage
)
SELECT 
    patient::text AS id,
    birthdate::date,
    deathdate::date,
    race,
    ethnicity,
    gender,
    first,
    last,
    address,
    city,
    state,
    zip,
    county,
    latitude,
    longitude,
    income,
    healthcare_expenses,
    healthcare_coverage
FROM population.patients_typed;

SELECT report_progress('Patients', COUNT(*), COUNT(*)) FROM staging.patients_raw;

-- 2) ENCOUNTERS
\echo 'progress: 2/19 Transferring encounters data'
TRUNCATE TABLE staging.encounters_raw;

INSERT INTO staging.encounters_raw (
    id, patient_id, start_timestamp, stop_timestamp, encounter_class,
    code, description, base_encounter_cost, total_claim_cost, payer_coverage,
    reason_code, reason_description, organization_id, provider_id
)
SELECT 
    encounter_id::text AS id,
    patient::text AS patient_id,
    start_time AS start_timestamp,
    stop_time AS stop_timestamp,
    encounter_class,
    code,
    description,
    base_encounter_cost,
    total_claim_cost,
    payer_coverage,
    reason_code,
    reason_description,
    organization_id::text,
    provider_id::text
FROM population.encounters_typed;

SELECT report_progress('Encounters', COUNT(*), COUNT(*)) FROM staging.encounters_raw;

-- 3) CONDITIONS
\echo 'progress: 3/19 Transferring conditions data'
TRUNCATE TABLE staging.conditions_raw;

-- For large tables, use batch processing
DO $$
DECLARE
    batch_size INT := 1000000; -- Adjust based on available memory
    total_rows BIGINT;
    processed_rows BIGINT := 0;
    batch_number INT := 0;
BEGIN
    -- Get total count
    SELECT COUNT(*) INTO total_rows FROM population.conditions_typed;
    
    RAISE NOTICE 'Total conditions to process: %', total_rows;
    
    -- Process in batches
    WHILE processed_rows < total_rows LOOP
        batch_number := batch_number + 1;
        
        RAISE NOTICE 'Processing conditions batch % (% to %)', 
            batch_number, processed_rows, LEAST(processed_rows + batch_size, total_rows);
        
        INSERT INTO staging.conditions_raw (
            id, patient_id, encounter_id, code, system, description,
            start_date, start_datetime, stop_date, stop_datetime, type, category
        )
        SELECT 
            condition_id::text AS id,
            patient::text AS patient_id,
            encounter::text AS encounter_id,
            code,
            system,
            description,
            start_time::date AS start_date,
            start_time AS start_datetime,
            stop_time::date AS stop_date,
            stop_time AS stop_datetime,
            condition_type AS type,
            category
        FROM population.conditions_typed
        ORDER BY condition_id
        LIMIT batch_size
        OFFSET processed_rows;
        
        processed_rows := processed_rows + batch_size;
        
        -- Report progress
        PERFORM report_progress('Conditions', LEAST(processed_rows, total_rows), total_rows);
        
        -- Commit each batch to prevent transaction log buildup
        COMMIT;
        BEGIN;
    END LOOP;
END $$;

-- 4) MEDICATIONS
\echo 'progress: 4/19 Transferring medications data'
TRUNCATE TABLE staging.medications_raw;

-- For large tables, use batch processing
DO $$
DECLARE
    batch_size INT := 1000000; -- Adjust based on available memory
    total_rows BIGINT;
    processed_rows BIGINT := 0;
    batch_number INT := 0;
BEGIN
    -- Get total count
    SELECT COUNT(*) INTO total_rows FROM population.medications_typed;
    
    RAISE NOTICE 'Total medications to process: %', total_rows;
    
    -- Process in batches
    WHILE processed_rows < total_rows LOOP
        batch_number := batch_number + 1;
        
        RAISE NOTICE 'Processing medications batch % (% to %)', 
            batch_number, processed_rows, LEAST(processed_rows + batch_size, total_rows);
        
        INSERT INTO staging.medications_raw (
            id, patient_id, encounter_id, code, description,
            start_timestamp, stop_timestamp, base_cost, payer_coverage,
            dispenses, total_cost, reason_code, reason_description
        )
        SELECT 
            medication_id::text AS id,
            patient::text AS patient_id,
            encounter::text AS encounter_id,
            code,
            description,
            start_time AS start_timestamp,
            stop_time AS stop_timestamp,
            base_cost,
            payer_coverage,
            dispenses,
            total_cost,
            reason_code,
            reason_description
        FROM population.medications_typed
        ORDER BY medication_id
        LIMIT batch_size
        OFFSET processed_rows;
        
        processed_rows := processed_rows + batch_size;
        
        -- Report progress
        PERFORM report_progress('Medications', LEAST(processed_rows, total_rows), total_rows);
        
        -- Commit each batch to prevent transaction log buildup
        COMMIT;
        BEGIN;
    END LOOP;
END $$;

-- 5) PROCEDURES
\echo 'progress: 5/19 Transferring procedures data'
TRUNCATE TABLE staging.procedures_raw;

-- For large tables, use batch processing
DO $$
DECLARE
    batch_size INT := 1000000; -- Adjust based on available memory
    total_rows BIGINT;
    processed_rows BIGINT := 0;
    batch_number INT := 0;
BEGIN
    -- Get total count
    SELECT COUNT(*) INTO total_rows FROM population.procedures_typed;
    
    RAISE NOTICE 'Total procedures to process: %', total_rows;
    
    -- Process in batches
    WHILE processed_rows < total_rows LOOP
        batch_number := batch_number + 1;
        
        RAISE NOTICE 'Processing procedures batch % (% to %)', 
            batch_number, processed_rows, LEAST(processed_rows + batch_size, total_rows);
        
        INSERT INTO staging.procedures_raw (
            id, patient_id, encounter_id, code, description,
            base_cost, timestamp, reason_code, reason_description
        )
        SELECT 
            procedure_id::text AS id,
            patient::text AS patient_id,
            encounter::text AS encounter_id,
            code,
            description,
            base_cost,
            start_time AS timestamp,
            reason_code,
            reason_description
        FROM population.procedures_typed
        ORDER BY procedure_id
        LIMIT batch_size
        OFFSET processed_rows;
        
        processed_rows := processed_rows + batch_size;
        
        -- Report progress
        PERFORM report_progress('Procedures', LEAST(processed_rows, total_rows), total_rows);
        
        -- Commit each batch to prevent transaction log buildup
        COMMIT;
        BEGIN;
    END LOOP;
END $$;

-- 6) OBSERVATIONS
\echo 'progress: 6/19 Transferring observations data'
TRUNCATE TABLE staging.observations_raw;

-- For large tables, use batch processing
DO $$
DECLARE
    batch_size INT := 1000000; -- Adjust based on available memory
    total_rows BIGINT;
    processed_rows BIGINT := 0;
    batch_number INT := 0;
BEGIN
    -- Get total count
    SELECT COUNT(*) INTO total_rows FROM population.observations_typed;
    
    RAISE NOTICE 'Total observations to process: %', total_rows;
    
    -- Process in batches
    WHILE processed_rows < total_rows LOOP
        batch_number := batch_number + 1;
        
        RAISE NOTICE 'Processing observations batch % (% to %)', 
            batch_number, processed_rows, LEAST(processed_rows + batch_size, total_rows);
        
        INSERT INTO staging.observations_raw (
            id, patient_id, encounter_id, observation_type, code, description,
            value_as_string, timestamp
        )
        SELECT 
            observation_id::text AS id,
            patient::text AS patient_id,
            encounter::text AS encounter_id,
            category AS observation_type,
            code,
            description,
            value,
            date AS timestamp
        FROM population.observations_typed
        ORDER BY observation_id
        LIMIT batch_size
        OFFSET processed_rows;
        
        processed_rows := processed_rows + batch_size;
        
        -- Report progress
        PERFORM report_progress('Observations', LEAST(processed_rows, total_rows), total_rows);
        
        -- Commit each batch to prevent transaction log buildup
        COMMIT;
        BEGIN;
    END LOOP;
END $$;

-- 7) ALLERGIES
\echo 'progress: 7/19 Transferring allergies data'
TRUNCATE TABLE staging.allergies_raw;

INSERT INTO staging.allergies_raw (
    patient_id, encounter_id, code, system, description,
    type, category, reaction1_code, reaction1_desc, severity1,
    reaction2_code, reaction2_desc, severity2, start_date, stop_date
)
SELECT 
    patient::text AS patient_id,
    encounter::text AS encounter_id,
    code,
    system,
    description,
    allergy_type::text AS type,
    allergy_category::text AS category,
    reaction1 AS reaction1_code,
    description1 AS reaction1_desc,
    severity1::text,
    reaction2 AS reaction2_code,
    description2 AS reaction2_desc,
    severity2::text,
    start_time::date AS start_date,
    stop_time::date AS stop_date
FROM population.allergies_typed;

SELECT report_progress('Allergies', COUNT(*), COUNT(*)) FROM staging.allergies_raw;

-- 8) CAREPLANS
\echo 'progress: 8/19 Transferring careplans data'
TRUNCATE TABLE staging.careplans_raw;

INSERT INTO staging.careplans_raw (
    id, patient_id, encounter_id, start_date, stop_date,
    code, description, reason_code, reason_description
)
SELECT 
    careplan_id::text AS id,
    patient::text AS patient_id,
    encounter::text AS encounter_id,
    start_time::date AS start_date,
    stop_time::date AS stop_date,
    code,
    description,
    reasoncode AS reason_code,
    reasondescription AS reason_description
FROM population.careplans_typed;

SELECT report_progress('Careplans', COUNT(*), COUNT(*)) FROM staging.careplans_raw;

-- 9) DEVICES
\echo 'progress: 9/19 Transferring devices data'
TRUNCATE TABLE staging.devices_raw;

INSERT INTO staging.devices_raw (
    id, patient_id, encounter_id, code, description,
    udi, start_timestamp, stop_timestamp
)
SELECT 
    device_id::text AS id,
    patient::text AS patient_id,
    encounter::text AS encounter_id,
    code,
    description,
    udi,
    start_time AS start_timestamp,
    stop_time AS stop_timestamp
FROM population.devices_typed;

SELECT report_progress('Devices', COUNT(*), COUNT(*)) FROM staging.devices_raw;

-- 10) IMAGING_STUDIES
\echo 'progress: 10/19 Transferring imaging studies data'
TRUNCATE TABLE staging.imaging_studies_raw;

INSERT INTO staging.imaging_studies_raw (
    id, patient_id, encounter_id, date, series_uid,
    body_site_code, body_site_description, modality_code,
    modality_description, sop_code, sop_description
)
SELECT 
    imaging_study_id::text AS id,
    patient::text AS patient_id,
    encounter::text AS encounter_id,
    date,
    series_uid,
    body_site_code,
    body_site_description,
    modality_code,
    modality_description,
    sop_code,
    sop_description
FROM population.imaging_studies_typed;

SELECT report_progress('Imaging Studies', COUNT(*), COUNT(*)) FROM staging.imaging_studies_raw;

-- 11) IMMUNIZATIONS
\echo 'progress: 11/19 Transferring immunizations data'
TRUNCATE TABLE staging.immunizations_raw;

INSERT INTO staging.immunizations_raw (
    id, patient_id, encounter_id, code, description,
    date, base_cost
)
SELECT 
    immunization_id::text AS id,
    patient::text AS patient_id,
    encounter::text AS encounter_id,
    code,
    description,
    date,
    cost AS base_cost
FROM population.immunizations_typed;

SELECT report_progress('Immunizations', COUNT(*), COUNT(*)) FROM staging.immunizations_raw;

-- 12) ORGANIZATIONS
\echo 'progress: 12/19 Transferring organizations data'
TRUNCATE TABLE staging.organizations_raw;

INSERT INTO staging.organizations_raw (
    id, name, address, city, state, zip, phone, revenue, utilization
)
SELECT 
    organization_id::text AS id,
    name,
    address,
    city,
    state,
    zip,
    phone,
    revenue,
    utilization
FROM population.organizations_typed;

SELECT report_progress('Organizations', COUNT(*), COUNT(*)) FROM staging.organizations_raw;

-- 13) PATIENT_EXPENSES
\echo 'progress: 13/19 Transferring patient expenses data'
TRUNCATE TABLE staging.patient_expenses_raw;

INSERT INTO staging.patient_expenses_raw (
    patient_id, year_date, payer_id, healthcare_expenses, insurance_costs, covered_costs
)
SELECT 
    patient_id::text,
    make_date(year, 1, 1) AS year_date,
    payer_id::text,
    healthcare_expenses,
    NULL AS insurance_costs,
    NULL AS covered_costs
FROM population.patient_expenses_typed;

SELECT report_progress('Patient Expenses', COUNT(*), COUNT(*)) FROM staging.patient_expenses_raw;

-- 14) PAYER_TRANSITIONS
\echo 'progress: 14/19 Transferring payer transitions data'
TRUNCATE TABLE staging.payer_transitions_raw;

INSERT INTO staging.payer_transitions_raw (
    patient_id, member_id, start_date, end_date, payer_id, ownership
)
SELECT 
    patient::text AS patient_id,
    memberid::text AS member_id,
    start_date,
    end_date,
    payer::text AS payer_id,
    ownership
FROM population.payer_transitions_typed;

SELECT report_progress('Payer Transitions', COUNT(*), COUNT(*)) FROM staging.payer_transitions_raw;

-- 15) PAYERS
\echo 'progress: 15/19 Transferring payers data'
TRUNCATE TABLE staging.payers_raw;

INSERT INTO staging.payers_raw (
    id, name, address, city, state_headquartered, zip, phone, amount_covered, amount_uncovered, revenue, covered_encounters, uncovered_encounters, covered_medications, uncovered_medications, covered_procedures, uncovered_procedures, covered_immunizations, uncovered_immunizations, unique_customers, qols_avg, member_months
)
SELECT 
    payer_id::text AS id,
    name,
    address,
    city,
    state_headquartered,
    zip,
    phone,
    amount_covered,
    amount_uncovered,
    revenue,
    covered_encounters,
    uncovered_encounters,
    covered_medications,
    uncovered_medications,
    covered_procedures,
    uncovered_procedures,
    covered_immunizations,
    uncovered_immunizations,
    unique_customers,
    qols_avg,
    member_months
FROM population.payers_typed;

SELECT report_progress('Payers', COUNT(*), COUNT(*)) FROM staging.payers_raw;

-- 16) PROVIDERS
\echo 'progress: 16/19 Transferring providers data'
TRUNCATE TABLE staging.providers_raw;

INSERT INTO staging.providers_raw (
    id, organization_id, name, gender, speciality, address, city, state, zip, utilization
)
SELECT 
    provider_id::text AS id,
    organization::text AS organization_id,
    name,
    gender,
    speciality,
    address,
    city,
    state,
    zip,
    utilization
FROM population.providers_typed;

SELECT report_progress('Providers', COUNT(*), COUNT(*)) FROM staging.providers_raw;

-- 17) SUPPLIES
\echo 'progress: 17/19 Transferring supplies data'
TRUNCATE TABLE staging.supplies_raw;

INSERT INTO staging.supplies_raw (
    id, patient_id, encounter_id, code, description, quantity, supply_cost
)
SELECT 
    supply_id::text AS id,
    patient::text AS patient_id,
    encounter::text AS encounter_id,
    code,
    description,
    quantity,
    supply_cost
FROM population.supplies_typed;

SELECT report_progress('Supplies', COUNT(*), COUNT(*)) FROM staging.supplies_raw;

-- 18) CLAIMS
\echo 'progress: 18/19 Transferring claims data'
TRUNCATE TABLE staging.claims_raw;

INSERT INTO staging.claims_raw (
    id, patient_id, provider_id, payer_id, department_id,
    diagnosis1, diagnosis2, status1, status2,
    outstanding1, outstanding2, service_date
)
SELECT 
    claim_id::text AS id,
    patient_id::text,
    provider_id::text,
    primary_insurance_id::text AS payer_id,
    department_id,
    diagnosis1,
    diagnosis2,
    status1::text,
    status2::text,
    outstanding1,
    outstanding2,
    service_date
FROM population.claims_typed;

SELECT report_progress('Claims', COUNT(*), COUNT(*)) FROM staging.claims_raw;

-- 19) CLAIMS_TRANSACTIONS
\echo 'progress: 19/19 Transferring claims transactions data'
TRUNCATE TABLE staging.claims_transactions_raw;

-- For large tables, use batch processing
DO $$
DECLARE
    batch_size INT := 1000000; -- Adjust based on available memory
    total_rows BIGINT;
    processed_rows BIGINT := 0;
    batch_number INT := 0;
BEGIN
    -- Get total count
    SELECT COUNT(*) INTO total_rows FROM population.claims_transactions_typed;
    
    RAISE NOTICE 'Total claims transactions to process: %', total_rows;
    
    -- Process in batches
    WHILE processed_rows < total_rows LOOP
        batch_number := batch_number + 1;
        
        RAISE NOTICE 'Processing claims transactions batch % (% to %)', 
            batch_number, processed_rows, LEAST(processed_rows + batch_size, total_rows);
        
        INSERT INTO staging.claims_transactions_raw (
            id, claim_id, patient_id, type, amount, payment_method,
            from_date, to_date, procedure_code, diagnosis_ref1, units, provider_id
        )
        SELECT 
            id::text,
            claim_id::text,
            patient_id::text,
            type,
            amount,
            method AS payment_method,
            from_date,
            to_date,
            procedure_code,
            diagnosis1 AS diagnosis_ref1,
            units,
            provider_id::text
        FROM population.claims_transactions_typed
        ORDER BY id
        LIMIT batch_size
        OFFSET processed_rows;
        
        processed_rows := processed_rows + batch_size;
        
        -- Report progress
        PERFORM report_progress('Claims Transactions', LEAST(processed_rows, total_rows), total_rows);
        
        -- Commit each batch to prevent transaction log buildup
        COMMIT;
        BEGIN;
    END LOOP;
END $$;

-- Analyze tables for better query performance
ANALYZE staging.patients_raw;
ANALYZE staging.encounters_raw;
ANALYZE staging.conditions_raw;
ANALYZE staging.medications_raw;
ANALYZE staging.procedures_raw;
ANALYZE staging.observations_raw;
ANALYZE staging.allergies_raw;
ANALYZE staging.careplans_raw;
ANALYZE staging.devices_raw;
ANALYZE staging.imaging_studies_raw;
ANALYZE staging.immunizations_raw;
ANALYZE staging.organizations_raw;
ANALYZE staging.patient_expenses_raw;
ANALYZE staging.payer_transitions_raw;
ANALYZE staging.payers_raw;
ANALYZE staging.providers_raw;
ANALYZE staging.supplies_raw;
ANALYZE staging.claims_raw;
ANALYZE staging.claims_transactions_raw;

\echo 'progress: 19/19 Population to staging transfer complete'

COMMIT;

\echo 'Population to staging transfer completed successfully.'
