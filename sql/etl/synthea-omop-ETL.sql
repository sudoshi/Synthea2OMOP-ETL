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

-- (Assuming you built staging.obs_period_calc 
--  with earliest & latest dates per person)

CREATE SEQUENCE IF NOT EXISTS staging.observation_period_seq START 1 INCREMENT 1;

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
    CAST(calc.latest_date   AS date) AS observation_period_end_date,
    44814724                 AS period_type_concept_id  -- "EHR record"
FROM staging.obs_period_calc calc
WHERE calc.person_id NOT IN (
    SELECT person_id 
    FROM omop.observation_period
);

INSERT INTO omop.visit_occurrence (
    visit_occurrence_id,
    person_id,
    visit_concept_id,
    visit_start_date,
    visit_start_datetime,
    visit_end_date,
    visit_end_datetime,
    visit_type_concept_id,
    provider_id,
    care_site_id,
    visit_source_value,
    visit_source_concept_id,
    preceding_visit_occurrence_id
)
SELECT
    vm.visit_occurrence_id,
    vm.person_id,

    CASE 
        WHEN e.encounter_class ILIKE '%inpatient%'  THEN 9201  -- Inpatient
        WHEN e.encounter_class ILIKE '%outpatient%' THEN 9202  -- Outpatient
        WHEN e.encounter_class ILIKE '%er%'         THEN 9203  -- Emergency
        ELSE 44818517  -- "Other/Unknown"
    END AS visit_concept_id,
    
    CAST(e.start_timestamp AS DATE) AS visit_start_date,
    e.start_timestamp              AS visit_start_datetime,
    CAST(e.stop_timestamp  AS DATE) AS visit_end_date,
    e.stop_timestamp               AS visit_end_datetime,
    
    44818518 AS visit_type_concept_id, -- "Visit derived from EHR"

    NULL AS provider_id,      -- join provider_map if you have it
    NULL AS care_site_id,     -- join care_site_map if you have it

    COALESCE(e.code, e.encounter_class) AS visit_source_value,
    NULL                                AS visit_source_concept_id,
    NULL AS preceding_visit_occurrence_id
FROM staging.encounters_raw e
JOIN staging.visit_map vm
    ON e.id = vm.source_visit_id
WHERE vm.visit_occurrence_id NOT IN (
    SELECT visit_occurrence_id
    FROM omop.visit_occurrence
);
CREATE SEQUENCE IF NOT EXISTS staging.condition_occurrence_seq START 1 INCREMENT 1;

INSERT INTO omop.condition_occurrence (
    condition_occurrence_id,
    person_id,
    condition_concept_id,
    condition_start_date,
    condition_start_datetime,
    condition_end_date,
    condition_end_datetime,
    condition_type_concept_id,
    stop_reason,
    provider_id,
    visit_occurrence_id,
    condition_source_value,
    condition_source_concept_id
)
SELECT
    NEXTVAL('staging.condition_occurrence_seq') AS condition_occurrence_id,
    
    pm.person_id,
    
    -- Real code mapping would use a local_to_omop_concept_map or detailed CASE statements:
    CASE 
       WHEN c.code ILIKE 'E11%' THEN 201826 -- Example: T2 Diabetes in SNOMED
       ELSE 0
    END AS condition_concept_id,

    COALESCE(c.start_date, CAST(c.start_datetime AS DATE)) AS condition_start_date,
    COALESCE(c.start_datetime, CAST(c.start_date AS TIMESTAMP)) AS condition_start_datetime,
    
    c.stop_date,
    c.stop_datetime,
    
    32020 AS condition_type_concept_id,  -- "EHR problem list entry"
    
    NULL AS stop_reason,
    
    NULL AS provider_id,
    vm.visit_occurrence_id,
    
    CONCAT(c.system, ': ', c.code) AS condition_source_value,
    NULL AS condition_source_concept_id
FROM staging.conditions_raw c
JOIN staging.person_map pm ON pm.source_patient_id = c.patient_id
LEFT JOIN staging.visit_map  vm ON vm.source_visit_id = c.encounter_id
WHERE NOT EXISTS (
    SELECT 1
    FROM omop.condition_occurrence co
    WHERE co.person_id = pm.person_id
      AND co.condition_start_date = COALESCE(c.start_date, CAST(c.start_datetime AS DATE))
      AND co.condition_source_value = CONCAT(c.system, ': ', c.code)
);

CREATE SEQUENCE IF NOT EXISTS staging.drug_exposure_seq START 1 INCREMENT 1;

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
    drug_source_value,
    drug_source_concept_id,
    route_source_value,
    dose_unit_source_value
)
SELECT
    NEXTVAL('staging.drug_exposure_seq') AS drug_exposure_id,
    pm.person_id,

    CASE 
      WHEN m.code ILIKE '12345%' THEN 19019066 -- Example: RxNorm concept
      ELSE 0
    END AS drug_concept_id,

    CAST(m.start_timestamp AS DATE) AS drug_exposure_start_date,
    m.start_timestamp               AS drug_exposure_start_datetime,
    
    CAST(m.stop_timestamp AS DATE)  AS drug_exposure_end_date,
    m.stop_timestamp                AS drug_exposure_end_datetime,
    
    NULL AS verbatim_end_date,
    38000177 AS drug_type_concept_id,  -- "Prescription written" or your choice
    NULL AS stop_reason,
    NULL AS refills,
    NULL AS quantity,
    NULL AS days_supply,
    NULL AS sig,
    NULL AS route_concept_id,
    NULL AS lot_number,
    
    NULL AS provider_id,
    vm.visit_occurrence_id,
    
    m.code AS drug_source_value,
    NULL AS drug_source_concept_id,
    NULL AS route_source_value,
    NULL AS dose_unit_source_value
FROM staging.medications_raw m
JOIN staging.person_map pm ON pm.source_patient_id = m.patient_id
LEFT JOIN staging.visit_map vm ON vm.source_visit_id = m.encounter_id
WHERE NOT EXISTS (
    SELECT 1
    FROM omop.drug_exposure de
    WHERE de.person_id = pm.person_id
      AND de.drug_exposure_start_datetime = m.start_timestamp
      AND de.drug_source_value = m.code
);

CREATE SEQUENCE IF NOT EXISTS staging.device_exposure_seq START 1 INCREMENT 1;

INSERT INTO omop.device_exposure (
    device_exposure_id,
    person_id,
    device_concept_id,
    device_exposure_start_date,
    device_exposure_start_datetime,
    device_exposure_end_date,
    device_exposure_end_datetime,
    device_type_concept_id,
    unique_device_id,
    quantity,
    provider_id,
    visit_occurrence_id,
    device_source_value,
    device_source_concept_id,
    unit_concept_id,
    unit_source_value,
    unit_source_concept_id
)
SELECT
    NEXTVAL('staging.device_exposure_seq'),
    pm.person_id,

    CASE 
      WHEN d.code ILIKE 'DEVICE123%' THEN 4263759  -- Example OMOP concept
      ELSE 0
    END AS device_concept_id,

    CAST(d.start_timestamp AS DATE) AS device_exposure_start_date,
    d.start_timestamp               AS device_exposure_start_datetime,
    CAST(d.stop_timestamp AS DATE)  AS device_exposure_end_date,
    d.stop_timestamp                AS device_exposure_end_datetime,

    44818707 AS device_type_concept_id,  -- "Device Recorded from EHR"

    d.udi AS unique_device_id,

    NULL AS quantity,
    NULL AS provider_id,
    vm.visit_occurrence_id,
    d.code AS device_source_value,
    NULL AS device_source_concept_id,
    NULL AS unit_concept_id,
    NULL AS unit_source_value,
    NULL AS unit_source_concept_id
FROM staging.devices_raw d
JOIN staging.person_map pm ON pm.source_patient_id = d.patient_id
LEFT JOIN staging.visit_map vm ON vm.source_visit_id = d.encounter_id
WHERE NOT EXISTS (
    SELECT 1 
    FROM omop.device_exposure de
    WHERE de.person_id = pm.person_id
      AND de.device_exposure_start_datetime = d.start_timestamp
      AND de.device_source_value = d.code
);
CREATE SEQUENCE IF NOT EXISTS staging.measurement_seq START 1 INCREMENT 1;

-- Example of filtering to "vital-signs" or "laboratory" and numeric values
INSERT INTO omop.measurement (
    measurement_id,
    person_id,
    measurement_concept_id,
    measurement_date,
    measurement_datetime,
    measurement_type_concept_id,
    operator_concept_id,
    value_as_number,
    value_as_concept_id,
    unit_concept_id,
    range_low,
    range_high,
    provider_id,
    visit_occurrence_id,
    measurement_source_value,
    measurement_source_concept_id,
    unit_source_value,
    unit_source_concept_id,
    measurement_event_id,
    meas_event_field_concept_id
)
SELECT
    NEXTVAL('staging.measurement_seq'),
    pm.person_id,

    CASE
      WHEN o.code = '8302-2' THEN 3036277  -- e.g. LOINC concept for height
      WHEN o.code = '29463-7' THEN 3025315 -- LOINC for weight
      ELSE 0
    END AS measurement_concept_id,

    CAST(o.timestamp AS DATE)  AS measurement_date,
    o.timestamp                AS measurement_datetime,

    CASE 
      WHEN o.category ILIKE '%laboratory%' THEN 32817     -- "Lab result"
      WHEN o.category ILIKE '%vital-signs%' THEN 44818702 -- "From physical measurement"
      ELSE 0
    END AS measurement_type_concept_id,

    NULL AS operator_concept_id,
    o.value  AS value_as_number,   -- numeric
    NULL AS value_as_concept_id,

    CASE
      WHEN o.units = 'mmHg'  THEN 8876   -- UCUM concept
      WHEN o.units = 'kg'    THEN 8840
      ELSE 0
    END AS unit_concept_id,

    NULL AS range_low,
    NULL AS range_high,

    NULL AS provider_id,
    vm.visit_occurrence_id,
    o.code AS measurement_source_value,
    NULL   AS measurement_source_concept_id,
    o.units AS unit_source_value,
    NULL   AS unit_source_concept_id,
    NULL   AS measurement_event_id,
    NULL   AS meas_event_field_concept_id
FROM staging.observations_raw o
JOIN staging.person_map pm ON pm.source_patient_id = o.patient_id
LEFT JOIN staging.visit_map vm ON vm.source_visit_id = o.encounter_id

-- Only numeric rows that should be measurements
WHERE o.category IN ('vital-signs','laboratory')
  AND o.value_as_string ~ '^-?\d+(\.\d+)?$'
  -- Avoid duplicates
  AND NOT EXISTS (
     SELECT 1 
     FROM omop.measurement m2
     WHERE m2.person_id = pm.person_id
       AND m2.measurement_datetime = o.timestamp
       AND m2.measurement_source_value = o.code
  );

CREATE SEQUENCE IF NOT EXISTS staging.observation_seq START 1 INCREMENT 1;

INSERT INTO omop.observation (
    observation_id,
    person_id,
    observation_concept_id,
    observation_date,
    observation_datetime,
    observation_type_concept_id,
    value_as_number,
    value_as_string,
    value_as_concept_id,
    qualifier_concept_id,
    unit_concept_id,
    provider_id,
    visit_occurrence_id,
    observation_source_value,
    observation_source_concept_id,
    unit_source_value,
    qualifier_source_value,
    value_source_value
)
SELECT
    NEXTVAL('staging.observation_seq'),
    pm.person_id,
    0 AS observation_concept_id,
    CAST(o.timestamp AS DATE) AS observation_date,
    o.timestamp               AS observation_datetime,
    38000280 AS observation_type_concept_id, -- "Observation recorded from EHR"

    NULL AS value_as_number,
    o.value_as_string AS value_as_string,
    NULL AS value_as_concept_id,
    NULL AS qualifier_concept_id,
    NULL AS unit_concept_id,
    NULL AS provider_id,
    vm.visit_occurrence_id,
    o.code AS observation_source_value,
    NULL AS observation_source_concept_id,
    NULL AS unit_source_value,
    NULL AS qualifier_source_value,
    o.value_as_string AS value_source_value
FROM staging.observations_raw o
JOIN staging.person_map pm ON pm.source_patient_id = o.patient_id
LEFT JOIN staging.visit_map vm ON vm.source_visit_id = o.encounter_id

-- Only non-numeric or categories not in (lab, vital-signs)
WHERE NOT (o.category IN ('vital-signs','laboratory') AND o.value_as_string ~ '^-?\d+(\.\d+)?$')
  AND NOT EXISTS (
    SELECT 1 
    FROM omop.observation obs
    WHERE obs.person_id = pm.person_id
      AND obs.observation_datetime = o.timestamp
      AND obs.observation_source_value = o.code
  );

CREATE SEQUENCE IF NOT EXISTS staging.drug_exposure_seq START 1 INCREMENT 1;

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
    drug_source_value,
    drug_source_concept_id,
    route_source_value,
    dose_unit_source_value
)
SELECT
    NEXTVAL('staging.drug_exposure_seq'),
    pm.person_id,
    
    CASE 
      WHEN i.code ILIKE '99999%' THEN 123456  -- Example concept ID for a vaccine
      ELSE 0
    END AS drug_concept_id,

    CAST(i.date AS DATE) AS drug_exposure_start_date,
    i.date               AS drug_exposure_start_datetime,

    CAST(i.date AS DATE) AS drug_exposure_end_date,
    i.date               AS drug_exposure_end_datetime,

    NULL AS verbatim_end_date,
    38000177 AS drug_type_concept_id, -- "Prescription written", or 38000175 "Physician admin"
    NULL AS stop_reason,
    NULL AS refills,
    NULL AS quantity,
    NULL AS days_supply,
    NULL AS sig,
    NULL AS route_concept_id,
    NULL AS lot_number,

    NULL AS provider_id,
    vm.visit_occurrence_id,

    i.code AS drug_source_value,
    NULL AS drug_source_concept_id,
    NULL AS route_source_value,
    NULL AS dose_unit_source_value
FROM staging.immunizations_raw i
JOIN staging.person_map pm ON pm.source_patient_id = i.patient_id
LEFT JOIN staging.visit_map vm ON vm.source_visit_id = i.encounter_id
WHERE NOT EXISTS (
    SELECT 1 
    FROM omop.drug_exposure de
    WHERE de.person_id = pm.person_id
      AND de.drug_exposure_start_datetime = i.date
      AND de.drug_source_value = i.code
);

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
    CAST(p.deathdate AS DATE)   AS death_date,
    CAST(p.deathdate AS TIMESTAMP) AS death_datetime,
    38003565 AS death_type_concept_id,  -- "EHR reported death"
    NULL AS cause_concept_id,          -- Map if you have cause_of_death code
    NULL AS cause_source_value,
    NULL AS cause_source_concept_id
FROM staging.patients_raw p
JOIN staging.person_map pm ON pm.source_patient_id = p.id
WHERE p.deathdate IS NOT NULL
  AND pm.person_id NOT IN (
    SELECT person_id 
    FROM omop.death
  );

CREATE SEQUENCE IF NOT EXISTS staging.payer_plan_period_seq START 1 INCREMENT 1;

INSERT INTO omop.payer_plan_period (
    payer_plan_period_id,
    person_id,
    payer_plan_period_start_date,
    payer_plan_period_end_date,
    payer_concept_id,
    payer_source_value,
    plan_concept_id,
    plan_source_value,
    sponsor_concept_id,
    sponsor_source_value,
    family_source_value,
    stop_reason_concept_id,
    stop_reason_source_value
)
SELECT
    NEXTVAL('staging.payer_plan_period_seq'),
    pm.person_id,

    CAST(pe.year_date AS DATE) AS payer_plan_period_start_date,
    (CAST(pe.year_date AS DATE) + INTERVAL '1 year - 1 day') AS payer_plan_period_end_date,

    0 AS payer_concept_id,
    pe.payer_id AS payer_source_value,
    
    0 AS plan_concept_id,
    NULL AS plan_source_value,
    NULL AS sponsor_concept_id,
    NULL AS sponsor_source_value,
    NULL AS family_source_value,
    NULL AS stop_reason_concept_id,
    NULL AS stop_reason_source_value

FROM staging.patient_expenses_raw pe
JOIN staging.person_map pm ON pm.source_patient_id = pe.patient_id
WHERE NOT EXISTS (
  SELECT 1
  FROM omop.payer_plan_period ppp
  WHERE ppp.person_id = pm.person_id
    AND ppp.payer_plan_period_start_date = CAST(pe.year_date AS DATE)
);


