-- Create observation period data for each person

-- Create a sequence for observation_period_id if it doesn't exist
CREATE SEQUENCE IF NOT EXISTS staging.observation_period_seq START 1 INCREMENT 1;

-- First, calculate the earliest and latest dates for each person
DROP TABLE IF EXISTS staging.obs_period_calc;

CREATE TABLE staging.obs_period_calc AS
WITH all_dates AS (
    -- Encounters
    SELECT 
        patient::text AS person_source_id,
        start_time AS event_date
    FROM 
        population.encounters_typed
    UNION ALL
    -- Conditions
    SELECT 
        patient::text AS person_source_id,
        start_time AS event_date
    FROM 
        population.conditions_typed
    UNION ALL
    -- Medications
    SELECT 
        patient::text AS person_source_id,
        start_time AS event_date
    FROM 
        population.medications_typed
    UNION ALL
    -- Procedures
    SELECT 
        patient::text AS person_source_id,
        start_time AS event_date
    FROM 
        population.procedures_typed
    UNION ALL
    -- Observations
    SELECT 
        patient::text AS person_source_id,
        date_time AS event_date
    FROM 
        population.observations_typed
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
