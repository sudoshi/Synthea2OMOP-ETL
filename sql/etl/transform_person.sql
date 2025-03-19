-- Transform person data from staging tables to OMOP schema

INSERT INTO omop.person (
    person_id,
    gender_concept_id,
    year_of_birth,
    month_of_birth,
    day_of_birth,
    race_concept_id,
    ethnicity_concept_id,
    location_id,
    provider_id,
    care_site_id,
    person_source_value,
    gender_source_value,
    race_source_value,
    ethnicity_source_value,
    gender_source_concept_id,
    race_source_concept_id,
    ethnicity_source_concept_id,
    birth_datetime
)
SELECT
    pm.person_id,
    COALESCE(gl.gender_concept_id, 0)            AS gender_concept_id,
    EXTRACT(YEAR  FROM p.birthdate)::INT         AS year_of_birth,
    EXTRACT(MONTH FROM p.birthdate)::INT         AS month_of_birth,
    EXTRACT(DAY   FROM p.birthdate)::INT         AS day_of_birth,
    COALESCE(rl.race_concept_id, 0)              AS race_concept_id,
    COALESCE(el.ethnicity_concept_id, 0)         AS ethnicity_concept_id,
    NULL                                         AS location_id,     -- set if you use omop.LOCATION
    NULL                                         AS provider_id,     -- set if you track default PCP
    NULL                                         AS care_site_id,    -- set if you link to care sites
    p.id                                         AS person_source_value,
    p.gender                                     AS gender_source_value,
    p.race                                       AS race_source_value,
    p.ethnicity                                  AS ethnicity_source_value,
    COALESCE(gl.gender_source_concept_id, 0)     AS gender_source_concept_id,
    COALESCE(rl.race_source_concept_id, 0)       AS race_source_concept_id,
    COALESCE(el.ethnicity_source_concept_id, 0)  AS ethnicity_source_concept_id,
    CAST(p.birthdate AS timestamp)               AS birth_datetime
FROM staging.patients_raw       p
JOIN staging.person_map         pm ON pm.source_patient_id = p.id
LEFT JOIN staging.gender_lookup    gl ON p.gender    = gl.source_gender
LEFT JOIN staging.race_lookup      rl ON p.race      = rl.source_race
LEFT JOIN staging.ethnicity_lookup el ON p.ethnicity = el.source_ethnicity
WHERE pm.person_id NOT IN (SELECT person_id FROM omop.person);

-- Log the results
SELECT 
    'Person' AS table_name,
    COUNT(*) AS record_count
FROM 
    omop.person;
