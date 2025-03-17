-- Repopulate staging tables from population schema tables

-- First, drop existing staging tables
DROP TABLE IF EXISTS staging.patients_raw CASCADE;
DROP TABLE IF EXISTS staging.encounters_raw CASCADE;
DROP TABLE IF EXISTS staging.conditions_raw CASCADE;
DROP TABLE IF EXISTS staging.medications_raw CASCADE;
DROP TABLE IF EXISTS staging.procedures_raw CASCADE;
DROP TABLE IF EXISTS staging.observations_raw CASCADE;
DROP TABLE IF EXISTS staging.immunizations_raw CASCADE;
DROP TABLE IF EXISTS staging.allergies_raw CASCADE;
DROP TABLE IF EXISTS staging.careplans_raw CASCADE;
DROP TABLE IF EXISTS staging.devices_raw CASCADE;
DROP TABLE IF EXISTS staging.imaging_studies_raw CASCADE;
DROP TABLE IF EXISTS staging.supplies_raw CASCADE;
DROP TABLE IF EXISTS staging.patient_expenses_raw CASCADE;

-- Recreate staging tables from population schema tables
CREATE TABLE staging.patients_raw AS
SELECT 
    patient_id::text AS id,
    birthdate,
    deathdate,
    ssn,
    drivers,
    passport,
    prefix::text,
    first_name,
    middle_name,
    last_name,
    suffix,
    maiden,
    marital_status::text,
    race::text,
    ethnicity::text,
    gender::text,
    birthplace,
    address,
    city,
    state,
    county,
    fips,
    zip,
    lat,
    lon,
    healthcare_expenses,
    healthcare_coverage,
    income,
    CURRENT_TIMESTAMP AS created_at,
    CURRENT_TIMESTAMP AS updated_at
FROM 
    population.patients_typed;

CREATE TABLE staging.encounters_raw AS
SELECT 
    encounter_id::text AS id,
    patient::text AS patient_id,
    start_time,
    stop_time,
    encounter_class,
    code,
    description,
    base_encounter_cost,
    total_claim_cost,
    payer_coverage,
    reasoncode,
    reasondescription,
    organization::text AS organization_id,
    provider::text AS provider_id,
    NULL AS care_site_id,
    CURRENT_TIMESTAMP AS created_at,
    CURRENT_TIMESTAMP AS updated_at
FROM 
    population.encounters_typed;

CREATE TABLE staging.conditions_raw AS
SELECT 
    NULL AS id,
    patient::text AS patient_id,
    encounter::text AS encounter_id,
    code,
    coding_sys::text AS system,
    description,
    start_time::date AS start_date,
    start_time AS start_datetime,
    stop_time::date AS stop_date,
    stop_time AS stop_datetime,
    NULL AS type,
    NULL AS category,
    CURRENT_TIMESTAMP AS created_at,
    CURRENT_TIMESTAMP AS updated_at
FROM 
    population.conditions_typed;

CREATE TABLE staging.medications_raw AS
SELECT 
    NULL AS id,
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
    reasoncode,
    reasondescription,
    CURRENT_TIMESTAMP AS created_at,
    CURRENT_TIMESTAMP AS updated_at
FROM 
    population.medications_typed;

CREATE TABLE staging.procedures_raw AS
SELECT 
    NULL AS id,
    patient::text AS patient_id,
    encounter::text AS encounter_id,
    code,
    coding_sys::text AS system,
    description,
    start_time,
    stop_time,
    base_cost,
    reasoncode,
    reasondesc AS reasondescription,
    CURRENT_TIMESTAMP AS created_at,
    CURRENT_TIMESTAMP AS updated_at
FROM 
    population.procedures_typed;

CREATE TABLE staging.observations_raw AS
SELECT 
    NULL AS id,
    patient::text AS patient_id,
    encounter::text AS encounter_id,
    category::text AS observation_type,
    code,
    description,
    value AS value_as_string,
    date_time AS timestamp,
    CURRENT_TIMESTAMP AS created_at,
    CURRENT_TIMESTAMP AS updated_at
FROM 
    population.observations_typed;

-- Recreate person_map table
TRUNCATE TABLE staging.person_map;
ALTER SEQUENCE staging.person_seq RESTART WITH 1;

INSERT INTO staging.person_map (source_patient_id, person_id, created_at, updated_at)
SELECT 
    id,
    NEXTVAL('staging.person_seq'),
    CURRENT_TIMESTAMP,
    CURRENT_TIMESTAMP
FROM 
    staging.patients_raw;

-- Recreate visit_map table
TRUNCATE TABLE staging.visit_map;
ALTER SEQUENCE staging.visit_seq RESTART WITH 1;

INSERT INTO staging.visit_map (source_visit_id, visit_occurrence_id, person_id, created_at, updated_at)
SELECT 
    e.id,
    NEXTVAL('staging.visit_seq'),
    pm.person_id,
    CURRENT_TIMESTAMP,
    CURRENT_TIMESTAMP
FROM 
    staging.encounters_raw e
JOIN 
    staging.person_map pm ON pm.source_patient_id = e.patient_id;

-- Log the results
SELECT 'patients_raw' AS table_name, COUNT(*) AS record_count FROM staging.patients_raw
UNION ALL
SELECT 'encounters_raw' AS table_name, COUNT(*) AS record_count FROM staging.encounters_raw
UNION ALL
SELECT 'conditions_raw' AS table_name, COUNT(*) AS record_count FROM staging.conditions_raw
UNION ALL
SELECT 'medications_raw' AS table_name, COUNT(*) AS record_count FROM staging.medications_raw
UNION ALL
SELECT 'procedures_raw' AS table_name, COUNT(*) AS record_count FROM staging.procedures_raw
UNION ALL
SELECT 'observations_raw' AS table_name, COUNT(*) AS record_count FROM staging.observations_raw
UNION ALL
SELECT 'person_map' AS table_name, COUNT(*) AS record_count FROM staging.person_map
UNION ALL
SELECT 'visit_map' AS table_name, COUNT(*) AS record_count FROM staging.visit_map;
