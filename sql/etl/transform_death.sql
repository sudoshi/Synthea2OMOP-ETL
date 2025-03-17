-- Transform death data from staging tables to OMOP schema

-- Insert into death table
INSERT INTO omop.death (
    person_id,
    death_date,
    death_datetime,
    death_type_concept_id,
    cause_concept_id,
    cause_source_value,
    cause_source_concept_id
)
SELECT
    pm.person_id,
    p.deathdate AS death_date,
    p.deathdate AS death_datetime,
    32817 AS death_type_concept_id, -- "EHR"
    0 AS cause_concept_id,
    NULL AS cause_source_value,
    0 AS cause_source_concept_id
FROM 
    staging.patients_raw p
JOIN 
    staging.person_map pm ON pm.source_patient_id = p.id
WHERE 
    p.deathdate IS NOT NULL
    AND pm.person_id NOT IN (
        SELECT person_id
        FROM omop.death
    );

-- Log the results
SELECT 
    'Death' AS table_name,
    COUNT(*) AS record_count
FROM 
    omop.death;
