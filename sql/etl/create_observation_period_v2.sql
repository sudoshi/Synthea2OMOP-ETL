-- Create observation period data for each person

-- Create a sequence for observation_period_id if it doesn't exist
CREATE SEQUENCE IF NOT EXISTS staging.observation_period_seq START 1 INCREMENT 1;

-- First, calculate the earliest and latest dates for each person
DROP TABLE IF EXISTS staging.obs_period_calc;

CREATE TABLE staging.obs_period_calc AS
WITH all_dates AS (
    -- Encounters
    SELECT 
        patient_id AS person_source_id,
        start_time AS event_date
    FROM 
        staging.encounters_raw
    UNION ALL
    -- Conditions
    SELECT 
        patient_id AS person_source_id,
        start_datetime AS event_date
    FROM 
        staging.conditions_raw
    UNION ALL
    -- Medications
    SELECT 
        patient_id AS person_source_id,
        start_timestamp AS event_date
    FROM 
        staging.medications_raw
    UNION ALL
    -- Procedures
    SELECT 
        patient_id AS person_source_id,
        start_time AS event_date
    FROM 
        staging.procedures_raw
    UNION ALL
    -- Observations
    SELECT 
        patient_id AS person_source_id,
        timestamp AS event_date
    FROM 
        staging.observations_raw
)
SELECT 
    pm.person_id,
    MIN(ad.event_date) AS earliest_date,
    MAX(ad.event_date) AS latest_date
FROM 
    all_dates ad
JOIN 
    staging.person_map pm ON pm.source_patient_id = ad.person_source_id
GROUP BY 
    pm.person_id;

-- Now insert into the observation_period table
INSERT INTO omop.observation_period (
    observation_period_id,
    person_id,
    observation_period_start_date,
    observation_period_end_date,
    period_type_concept_id
)
SELECT
    NEXTVAL('staging.observation_period_seq') AS observation_period_id,
    calc.person_id,
    CAST(calc.earliest_date AS date) AS observation_period_start_date,
    CAST(calc.latest_date AS date) AS observation_period_end_date,
    44814724 AS period_type_concept_id  -- "EHR record"
FROM 
    staging.obs_period_calc calc
WHERE 
    calc.person_id NOT IN (
        SELECT person_id 
        FROM omop.observation_period
    );

-- Log the results
SELECT 
    'Observation Period' AS table_name,
    COUNT(*) AS record_count
FROM 
    omop.observation_period;
