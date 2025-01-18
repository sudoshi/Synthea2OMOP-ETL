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
    COALESCE(gl.gender_concept_id, 0)       AS gender_concept_id,
    EXTRACT(YEAR  FROM p.birthdate)::INT    AS year_of_birth,
    EXTRACT(MONTH FROM p.birthdate)::INT    AS month_of_birth,
    EXTRACT(DAY   FROM p.birthdate)::INT    AS day_of_birth,
    COALESCE(rl.race_concept_id, 0)         AS race_concept_id,
    COALESCE(el.ethnicity_concept_id, 0)    AS ethnicity_concept_id,
    /* If you have a separate location mapping, you could set it here. Else NULL: */
    NULL                                    AS location_id,
    /* If each patient has a PCP or assigned provider, join and set provider_id. Else NULL: */
    NULL                                    AS provider_id,
    /* If relevant, map to a care_site_id. Else NULL: */
    NULL                                    AS care_site_id,
    /* Original patient ID from your system: */
    p.id                                    AS person_source_value,
    /* Keep original strings for gender/race/ethnicity in source_value columns: */
    p.gender                                AS gender_source_value,
    p.race                                  AS race_source_value,
    p.ethnicity                             AS ethnicity_source_value,
    /* If you have local concept IDs for these, store them. Else, use the mapped lookup or set 0: */
    COALESCE(gl.gender_source_concept_id, 0)   AS gender_source_concept_id,
    COALESCE(rl.race_source_concept_id, 0)     AS race_source_concept_id,
    COALESCE(el.ethnicity_source_concept_id, 0)AS ethnicity_source_concept_id,
    /* If you'd like, store the full birth timestamp. Otherwise you can omit this column. */
    CAST(p.birthdate AS TIMESTAMP) AS birth_datetime
FROM staging.patients_raw p
JOIN staging.person_map pm
    ON pm.source_patient_id = p.id
LEFT JOIN staging.gender_lookup    gl ON p.gender    = gl.source_gender
LEFT JOIN staging.race_lookup      rl ON p.race      = rl.source_race
LEFT JOIN staging.ethnicity_lookup el ON p.ethnicity = el.source_ethnicity

/* Optionally exclude rows already loaded into person if you want to avoid duplicates */
WHERE pm.person_id NOT IN (
    SELECT person_id 
    FROM omop.person
);

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
    -- Coalesce earliest_date in case it's missing, or set a default
    CAST(calc.earliest_date AS DATE) AS observation_period_start_date,
    -- If the latest_date is null, you might default to earliest_date or 'today'
    CAST(calc.latest_date AS DATE) AS observation_period_end_date,
    44814724 AS period_type_concept_id    -- "EHR record" concept
FROM staging.obs_period_calc calc

-- If you've already inserted some rows, avoid duplicating
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
    
    -- Map local encounter_class to OMOP concept IDs
    CASE 
        WHEN e.encounter_class ILIKE '%inpatient%'  THEN 9201  -- Inpatient
        WHEN e.encounter_class ILIKE '%outpatient%' THEN 9202  -- Outpatient
        WHEN e.encounter_class ILIKE '%er%'         THEN 9203  -- ED
        ELSE 44818517  -- "Other/Unknown" or a default concept
    END AS visit_concept_id,
    
    CAST(e.start_timestamp AS DATE)      AS visit_start_date,
    e.start_timestamp                    AS visit_start_datetime,
    CAST(e.stop_timestamp AS DATE)       AS visit_end_date,
    e.stop_timestamp                     AS visit_end_datetime,
    
    44818518                             AS visit_type_concept_id, -- "Visit derived from EHR"
    
    -- If you have mapped your providers, you can join staging.provider_map. Otherwise NULL:
    NULL AS provider_id,
    
    -- If you have mapped your care sites, you can join staging.care_site_map. Otherwise NULL:
    NULL AS care_site_id,
    
    -- You might store the original encounter "code" or "encounter_class" here:
    COALESCE(e.code, e.encounter_class)  AS visit_source_value,
    
    -- If you have a concept ID specifically for the source, or use local_to_omop_concept_map:
    NULL AS visit_source_concept_id,
    
    -- If your data tracks a preceding encounter, you could map that. For now, set NULL:
    NULL AS preceding_visit_occurrence_id

FROM staging.encounters_raw e
JOIN staging.visit_map vm
    ON e.id = vm.source_visit_id

-- Optionally exclude if already loaded:
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
    
    -- Map or default. For demonstration, we do a basic CASE 
    -- or rely on local_to_omop_concept_map joined if you have codes
    CASE 
       WHEN c.code ILIKE 'E11%' THEN 201826  -- e.g. type 2 diabetes 
       ELSE 0                               -- unknown concept
    END AS condition_concept_id,
    
    -- Start date/time
    COALESCE(c.start_date, CAST(c.start_datetime AS DATE))     AS condition_start_date,
    COALESCE(c.start_datetime, CAST(c.start_date AS TIMESTAMP)) AS condition_start_datetime,
    
    -- End date/time: if your data doesn't have a stop date, set null
    c.stop_date        AS condition_end_date,
    c.stop_datetime    AS condition_end_datetime,
    
    -- Condition type. For example:
    32020  AS condition_type_concept_id,  -- "EHR problem list entry"
    
    -- If your data has a reason for ending/stopping the condition
    NULL   AS stop_reason,
    
    -- If you track diagnosing provider, you can map them. For now, NULL:
    NULL   AS provider_id,
    
    -- Link to a visit if you have it
    vm.visit_occurrence_id,
    
    -- Keep your original code or description
    CONCAT(c.system, ': ', c.code) AS condition_source_value,
    
    -- If you can map to a non-standard concept, put it here. Otherwise set 0 or NULL
    NULL   AS condition_source_concept_id

FROM staging.conditions_raw c
JOIN staging.person_map pm 
    ON pm.source_patient_id = c.patient_id
LEFT JOIN staging.visit_map vm
    ON vm.source_visit_id = c.encounter_id  -- if your conditions reference an encounter

-- Omit rows already inserted, if re-running
WHERE NOT EXISTS (
    SELECT 1
    FROM omop.condition_occurrence oc
    WHERE oc.person_id = pm.person_id
      AND oc.condition_start_date = COALESCE(c.start_date, CAST(c.start_datetime AS DATE))
      AND oc.condition_source_value = CONCAT(c.system, ': ', c.code)
      /* or oc.condition_occurrence_id = something if you prefer a direct check */
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
    
    -- Map from your medication code to a standard OMOP concept
    -- For demonstration, we do a simple CASE or set 0 if unknown.
    /* 
       If you have a local_to_omop_concept_map, do something like:
         LEFT JOIN staging.local_to_omop_concept_map map
           ON map.source_code = m.code
           AND map.source_vocabulary = 'NDC'
         Then use map.target_concept_id
    */
    CASE 
      WHEN m.code ILIKE '12345%' THEN 19019066  -- Example: an RxNorm concept for a certain med
      ELSE 0  -- Unknown concept
    END AS drug_concept_id,

    -- Start date/time
    CAST(m.start_timestamp AS DATE) AS drug_exposure_start_date,
    m.start_timestamp               AS drug_exposure_start_datetime,

    -- End date/time
    CAST(m.stop_timestamp AS DATE)  AS drug_exposure_end_date,
    m.stop_timestamp                AS drug_exposure_end_datetime,

    -- verbatim_end_date is often used if you have a separate "intended end" date
    NULL AS verbatim_end_date,

    -- drug_type_concept_id indicates the origin (e.g. 38000177 = "Prescription written")
    38000177 AS drug_type_concept_id,

    -- If you store a reason medication was stopped
    NULL AS stop_reason,

    -- Some sites track refills, quantity, days_supply, etc.
    NULL AS refills,
    NULL AS quantity,
    NULL AS days_supply,

    -- If you have a "sig" or instructions field
    NULL AS sig,

    -- route_concept_id: if you have route data (e.g. "PO", "IV"), map to a standard concept.
    NULL AS route_concept_id,

    -- lot_number if relevant
    NULL AS lot_number,

    -- If you track prescriber or ordering provider, link to staging.provider_map
    NULL AS provider_id,

    -- Link to an encounter/visit if relevant
    vm.visit_occurrence_id,

    -- Original code or text
    m.code AS drug_source_value,

    -- If you can identify a non-standard concept for the source code
    NULL AS drug_source_concept_id,

    -- If you stored route as text, put it here
    NULL AS route_source_value,

    -- If you have a dose unit from the source
    NULL AS dose_unit_source_value

FROM staging.medications_raw m
JOIN staging.person_map pm
    ON pm.source_patient_id = m.patient_id
LEFT JOIN staging.visit_map vm
    ON vm.source_visit_id = m.encounter_id

-- If re-running, avoid duplicates by checking if we already inserted
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
    NEXTVAL('staging.device_exposure_seq') AS device_exposure_id,
    
    pm.person_id,
    
    -- Map local device code to standard OMOP concept_id
    CASE 
      WHEN d.code ILIKE 'DEVICE123%' THEN 4263759  -- Example concept for a certain device
      ELSE 0                                      -- Unknown
    END AS device_concept_id,
    
    CAST(d.start_timestamp AS DATE) AS device_exposure_start_date,
    d.start_timestamp                AS device_exposure_start_datetime,
    CAST(d.stop_timestamp AS DATE)   AS device_exposure_end_date,
    d.stop_timestamp                 AS device_exposure_end_datetime,
    
    -- e.g. 44818707 = “Device Recorded from EHR”
    44818707 AS device_type_concept_id,
    
    -- If you store UDI or other unique ID
    d.udi AS unique_device_id,
    
    -- If you track how many devices were used (some sites do)
    NULL AS quantity,
    
    -- If you track which provider placed/used the device, join provider_map
    NULL AS provider_id,
    
    -- Link device usage to the visit if known
    vm.visit_occurrence_id,
    
    -- Keep your original device code or label
    d.code AS device_source_value,
    
    -- If you have a non-standard concept for the source
    NULL AS device_source_concept_id,
    
    -- If device measurement units are relevant, map them
    NULL AS unit_concept_id,
    NULL AS unit_source_value,
    NULL AS unit_source_concept_id

FROM staging.devices_raw d
JOIN staging.person_map pm
    ON pm.source_patient_id = d.patient_id
LEFT JOIN staging.visit_map vm
    ON vm.source_visit_id = d.encounter_id

-- Avoid duplicates if re-running
WHERE NOT EXISTS (
    SELECT 1 
    FROM omop.device_exposure de
    WHERE de.person_id = pm.person_id
      AND de.device_exposure_start_datetime = d.start_timestamp
      AND de.device_source_value = d.code
);

DROP TABLE IF EXISTS staging.measurement_candidates;
CREATE TEMP TABLE staging.measurement_candidates AS
SELECT *
FROM staging.observations_raw
WHERE category IN ('laboratory','vital-signs')
  AND value IS NOT NULL;

CREATE SEQUENCE IF NOT EXISTS staging.measurement_seq START 1 INCREMENT 1;

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
    NEXTVAL('staging.measurement_seq') AS measurement_id,

    pm.person_id,

    -- Map your code to an OMOP measurement concept (e.g., LOINC).
    CASE
      WHEN m.code ILIKE 'LAB123%' THEN 1234567  -- example concept_id for a lab test
      ELSE 0                                   -- unknown or unmapped
    END AS measurement_concept_id,

    CAST(m.timestamp AS DATE) AS measurement_date,
    m.timestamp             AS measurement_datetime,

    -- Indicate whether this is a lab result or a vital sign:
    CASE
      WHEN m.category = 'laboratory'   THEN 32817    -- "Lab result"
      WHEN m.category = 'vital-signs'  THEN 44818702 -- "From physical measurement"
      ELSE 0
    END AS measurement_type_concept_id,

    -- If you have an operator (like < or >), store a concept. Otherwise NULL:
    NULL AS operator_concept_id,

    -- The numeric value from your source
    m.value AS value_as_number,

    -- If it's a coded result (like "Positive"), you might map to concept_id. Here, numeric => NULL
    NULL AS value_as_concept_id,

    -- Map units to a UCUM concept if possible
    CASE
      WHEN m.units = 'mmHg' THEN 8876    -- example UCUM concept_id
      WHEN m.units = 'mg/dL' THEN 8840   -- example UCUM concept_id
      ELSE 0
    END AS unit_concept_id,

    -- range_low, range_high if known (many labs have reference ranges)
    NULL AS range_low,
    NULL AS range_high,

    -- If you track the provider who took the measurement
    NULL AS provider_id,

    -- Link to a visit if relevant
    vm.visit_occurrence_id,

    -- Store original code or text
    m.code AS measurement_source_value,

    -- Non-standard concept for the measurement, if you have one
    NULL AS measurement_source_concept_id,

    -- Keep your original unit text
    m.units AS unit_source_value,

    -- If you have a local concept for the unit
    NULL AS unit_source_concept_id,

    -- measurement_event_id, meas_event_field_concept_id are advanced, typically NULL
    NULL AS measurement_event_id,
    NULL AS meas_event_field_concept_id

FROM staging.measurement_candidates m
JOIN staging.person_map pm
    ON pm.source_patient_id = m.patient_id
LEFT JOIN staging.visit_map vm
    ON vm.source_visit_id = m.encounter_id

WHERE NOT EXISTS (
  SELECT 1
  FROM omop.measurement mm
  WHERE mm.person_id = pm.person_id
    AND mm.measurement_datetime = m.timestamp
    AND mm.measurement_source_value = m.code
);

DROP TABLE IF EXISTS staging.observation_candidates;
CREATE TEMP TABLE staging.observation_candidates AS
SELECT o.*
FROM staging.observations_raw o
LEFT JOIN staging.measurement_candidates mc
  ON o.id = mc.id  -- if the same row was used in measurement
WHERE mc.id IS NULL;  -- anything not used in measurement, or

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
    NEXTVAL('staging.observation_seq') AS observation_id,

    pm.person_id,

    -- If you can map your code to a standard concept, do so; else fallback to 0
    0 AS observation_concept_id,

    CAST(o.timestamp AS DATE) AS observation_date,
    o.timestamp               AS observation_datetime,

    -- e.g. 38000280 = "Observation recorded from EHR"
    38000280 AS observation_type_concept_id,

    -- If there's a numeric value, we usually treat it as a measurement, so for observation set to NULL
    NULL AS value_as_number,

    -- If there's a free-text or descriptive value, store it here
    o.description AS value_as_string,

    -- If there's a coded result, you can store it in value_as_concept_id. We'll set NULL
    NULL AS value_as_concept_id,

    -- If there's a qualifier concept, store it here. We'll set NULL
    NULL AS qualifier_concept_id,

    -- If you do have a unit, but consider it more of an observation. Usually we'd do measurement. So set NULL
    NULL AS unit_concept_id,

    -- If a provider is relevant
    NULL AS provider_id,

    -- Link to the visit if known
    vm.visit_occurrence_id,

    -- Original code
    o.code AS observation_source_value,

    -- If you have a local concept ID
    NULL AS observation_source_concept_id,

    -- If you do have a textual unit
    NULL AS unit_source_value,

    -- If there's a textual qualifier (like "history of" or "familial")
    NULL AS qualifier_source_value,

    -- If there's a textual representation of the value
    o.description AS value_source_value

FROM staging.observation_candidates o
JOIN staging.person_map pm
    ON pm.source_patient_id = o.patient_id
LEFT JOIN staging.visit_map vm
    ON vm.source_visit_id = o.encounter_id

WHERE NOT EXISTS (
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
    NEXTVAL('staging.drug_exposure_seq') AS drug_exposure_id,

    pm.person_id,
    
    -- Map your immunization code to an RxNorm concept if possible
    CASE 
      WHEN i.code ILIKE '99999%' THEN 123456  -- example concept ID for a particular vaccine
      ELSE 0                                 -- fallback if unknown
    END AS drug_concept_id,

    CAST(i.date AS DATE) AS drug_exposure_start_date,
    i.date AS drug_exposure_start_datetime,

    -- Usually immunizations are a single-day event, so end date can match start
    CAST(i.date AS DATE) AS drug_exposure_end_date,
    i.date AS drug_exposure_end_datetime,

    NULL AS verbatim_end_date,

    38000177 AS drug_type_concept_id,  -- "Prescription written" or 38000175 "Inpatient administration", 
                                       -- or 38000176 "Physician administered drug (identified as procedure)"
    
    NULL AS stop_reason,
    NULL AS refills,
    NULL AS quantity,
    NULL AS days_supply,
    NULL AS sig,
    NULL AS route_concept_id,  -- if you have route data, set it
    NULL AS lot_number,
    
    -- Link to provider if known
    NULL AS provider_id,

    -- Link to the encounter if known
    vm.visit_occurrence_id,

    -- Original code or text
    i.code AS drug_source_value,

    NULL AS drug_source_concept_id,
    NULL AS route_source_value,
    NULL AS dose_unit_source_value

FROM staging.immunizations_raw i
JOIN staging.person_map pm
    ON pm.source_patient_id = i.patient_id
LEFT JOIN staging.visit_map vm
    ON vm.source_visit_id = i.encounter_id

WHERE NOT EXISTS (
    SELECT 1 
    FROM omop.drug_exposure de
    WHERE de.person_id = pm.person_id
      AND de.drug_exposure_start_datetime = i.date
      AND de.drug_source_value = i.code
);

CREATE SEQUENCE IF NOT EXISTS staging.note_seq START 1 INCREMENT 1;

INSERT INTO omop.note (
    note_id,
    person_id,
    note_date,
    note_datetime,
    note_type_concept_id,
    note_class_concept_id,
    note_title,
    note_text,
    encoding_concept_id,
    language_concept_id,
    provider_id,
    visit_occurrence_id,
    note_source_value,
    note_event_id,
    note_event_field_concept_id
)
SELECT
    NEXTVAL('staging.note_seq') AS note_id,
    pm.person_id,

    CAST(n.note_date AS DATE) AS note_date,
    n.note_date              AS note_datetime,

    /* 
       If you have a standard concept for note_type, e.g. 44814645 = "Clinical note" 
       or 32869 = "Progress note," etc.
    */
    44814645 AS note_type_concept_id,

    -- If you classify note_class, e.g. 40771346 = "Clinical"
    40771346 AS note_class_concept_id,

    /* Title if you have one, else NULL */
    NULL AS note_title,

    /* The large note text */
    n.note_text,

    -- 6502 = "UTF-8 encoding" if you want 
    6502 AS encoding_concept_id,

    -- 4180186 = "English" if you know the language 
    4180186 AS language_concept_id,

    -- If you map the provider
    NULL AS provider_id,

    vm.visit_occurrence_id,

    /* store your local note source or type? */
    n.note_type AS note_source_value,

    -- note_event_id, note_event_field_concept_id are advanced usage; set NULL 
    NULL AS note_event_id,
    NULL AS note_event_field_concept_id

FROM staging.notes_raw n
JOIN staging.person_map pm
    ON pm.source_patient_id = n.patient_id
LEFT JOIN staging.visit_map vm
    ON vm.source_visit_id = n.encounter_id

WHERE NOT EXISTS (
    SELECT 1
    FROM omop.note no
    WHERE no.person_id = pm.person_id
      AND no.note_date = CAST(n.note_date AS DATE)
      AND no.note_text = n.note_text
);

CREATE SEQUENCE IF NOT EXISTS staging.episode_seq START 1 INCREMENT 1;

INSERT INTO @cdmDatabaseSchema.episode (
    episode_id,
    person_id,
    episode_concept_id,
    episode_start_date,
    episode_start_datetime,
    episode_end_date,
    episode_end_datetime,
    episode_parent_id,
    episode_number,
    episode_object_concept_id,
    episode_type_concept_id,
    episode_source_value,
    episode_source_concept_id
)
SELECT
    NEXTVAL('staging.episode_seq') AS episode_id,
    pm.person_id,

    /* 
       You might choose a SNOMED concept for "Care plan" or "Counseling for management of ...". 
       If unknown, set 0. 
    */
    0 AS episode_concept_id,

    c.start_date AS episode_start_date,
    CAST(c.start_date AS TIMESTAMP) AS episode_start_datetime,

    c.stop_date AS episode_end_date,
    CAST(c.stop_date AS TIMESTAMP) AS episode_end_datetime,

    NULL AS episode_parent_id,
    NULL AS episode_number,

    -- e.g. 0 or 32550 = "Object type for episode" 
    0 AS episode_object_concept_id,

    -- e.g. 32549 = "Episode derived from EHR"
    32549 AS episode_type_concept_id,

    c.code AS episode_source_value,
    NULL AS episode_source_concept_id

FROM staging.careplans_raw c
JOIN staging.person_map pm
    ON pm.source_patient_id = c.patient_id

WHERE NOT EXISTS (
    SELECT 1
    FROM @cdmDatabaseSchema.episode e
    WHERE e.person_id = pm.person_id
      AND e.episode_start_date = c.start_date
      AND e.episode_source_value = c.code
);

INSERT INTO @cdmDatabaseSchema.death (
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
    
    -- The date of death
    CAST(p.deathdate AS DATE) AS death_date,
    
    -- If your deathdate field is only a date, we can cast to midnight or store as is
    CAST(p.deathdate AS TIMESTAMP) AS death_datetime,
    
    -- e.g. 38003565 = "EHR reported death"
    38003565 AS death_type_concept_id,
    
    -- If you have a coded cause of death, map it here. Otherwise set 0 or NULL
    CASE 
      WHEN p.cause_code ILIKE 'I21%' THEN 443392   -- Example SNOMED concept for myocardial infarction
      ELSE 0                                      -- Fallback if unknown
    END AS cause_concept_id,
    
    -- Original cause code or description
    p.cause_code AS cause_source_value,
    
    -- If you have a local cause concept, you can store it here; else NULL
    NULL AS cause_source_concept_id

FROM staging.patients_raw p
JOIN staging.person_map pm
    ON pm.source_patient_id = p.id
-- Only insert for those with a deathdate
WHERE p.deathdate IS NOT NULL

-- Avoid duplicates if re-running
AND pm.person_id NOT IN (
    SELECT person_id 
    FROM @cdmDatabaseSchema.death
);

CREATE SEQUENCE IF NOT EXISTS staging.payer_plan_period_seq START 1 INCREMENT 1;

INSERT INTO @cdmDatabaseSchema.payer_plan_period (
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
    NEXTVAL('staging.payer_plan_period_seq') AS payer_plan_period_id,

    pm.person_id,

    -- If your data only has annual coverage, pick e.g. Jan 1 as start, Dec 31 as end
    -- or use actual coverage_start, coverage_end if you have them
    CAST(pe.year_date AS DATE)                    AS payer_plan_period_start_date,
    (CAST(pe.year_date AS DATE) + INTERVAL '1 year - 1 day')  AS payer_plan_period_end_date,
    
    -- If you can map the payer to a concept ID, do so. Otherwise set 0 or NULL
    0 AS payer_concept_id,
    
    -- Original payer code or name
    pe.payer_id AS payer_source_value,

    -- If you track a plan concept
    0 AS plan_concept_id,
    NULL AS plan_source_value,

    -- If you track a sponsor concept
    NULL AS sponsor_concept_id,
    NULL AS sponsor_source_value,

    -- If you track family coverage
    NULL AS family_source_value,

    -- If coverage ended for a reason
    NULL AS stop_reason_concept_id,
    NULL AS stop_reason_source_value

FROM staging.patient_expenses_raw pe
JOIN staging.person_map pm
    ON pm.source_patient_id = pe.patient_id

WHERE NOT EXISTS (
  SELECT 1
  FROM @cdmDatabaseSchema.payer_plan_period ppp
  WHERE ppp.person_id = pm.person_id
    AND ppp.payer_plan_period_start_date = CAST(pe.year_date AS DATE)
);

CREATE SEQUENCE IF NOT EXISTS staging.cost_seq START 1 INCREMENT 1;

INSERT INTO @cdmDatabaseSchema.cost (
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

    -- The OMOP ID for the event. We'll do a CASE for demonstration
    CASE 
       WHEN c.event_type = 'DRUG' THEN dm.drug_exposure_id
       WHEN c.event_type = 'PROCEDURE' THEN pm.procedure_occurrence_id
       WHEN c.event_type = 'VISIT' THEN vm.visit_occurrence_id
       ELSE NULL
    END AS cost_event_id,

    -- Must match the above domain: 'Drug', 'Procedure', 'Visit'
    CASE
       WHEN c.event_type = 'DRUG' THEN 'Drug'
       WHEN c.event_type = 'PROCEDURE' THEN 'Procedure'
       WHEN c.event_type = 'VISIT' THEN 'Visit'
       ELSE 'Observation'
    END AS cost_domain_id,

    -- cost_type_concept_id, e.g. 5032 = "Primary"
    5032 AS cost_type_concept_id,

    -- 44818550 = "US Dollar" or if you have a concept ID for currency
    44818550 AS currency_concept_id,

    -- total_charge, total_cost, total_paid, etc. from your claims data
    c.total_charge,
    c.total_cost,
    c.total_paid,

    c.paid_by_payer,
    c.paid_by_patient,

    c.paid_patient_copay,
    c.paid_patient_coinsurance,
    c.paid_patient_deductible,
    c.paid_by_primary,

    c.paid_ingredient_cost,
    c.paid_dispensing_fee,

    -- If you mapped coverage to payer_plan_period above, link it by matching coverage date to claim date
    ppp.payer_plan_period_id,

    c.amount_allowed,
    NULL AS revenue_code_concept_id,
    NULL AS revenue_code_source_value,
    NULL AS drg_concept_id,
    NULL AS drg_source_value

FROM staging.claims_transactions_raw c

LEFT JOIN staging.drug_map dm
    ON dm.source_drug_id = c.event_id
    AND c.event_type = 'DRUG'

LEFT JOIN staging.procedure_map pm
    ON pm.source_procedure_id = c.event_id
    AND c.event_type = 'PROCEDURE'

LEFT JOIN staging.visit_map vm
    ON vm.source_visit_id = c.event_id
    AND c.event_type = 'VISIT'

LEFT JOIN staging.patient_expenses_raw pe
    ON pe.patient_id = c.patient_id
    -- match coverage year or date with claim date if needed

LEFT JOIN @cdmDatabaseSchema.payer_plan_period ppp
    ON ppp.person_id = (SELECT person_id FROM staging.person_map WHERE source_patient_id = c.patient_id)
    -- some logic to match coverage date range to c.from_date, c.to_date, etc.

WHERE c.total_cost IS NOT NULL
  AND NOT EXISTS (
    SELECT 1
    FROM @cdmDatabaseSchema.cost co
    WHERE co.cost_domain_id = CASE WHEN c.event_type = 'DRUG' THEN 'Drug'
                                   WHEN c.event_type = 'PROCEDURE' THEN 'Procedure'
                                   WHEN c.event_type = 'VISIT' THEN 'Visit'
                                   ELSE 'Observation' END
      AND co.cost_event_id = CASE WHEN c.event_type = 'DRUG' THEN dm.drug_exposure_id
                                  WHEN c.event_type = 'PROCEDURE' THEN pm.procedure_occurrence_id
                                  WHEN c.event_type = 'VISIT' THEN vm.visit_occurrence_id
                                  ELSE NULL END
  );

  INSERT INTO @cdmDatabaseSchema.fact_relationship (
      domain_concept_id_1,
      fact_id_1,
      domain_concept_id_2,
      fact_id_2,
      relationship_concept_id
  )
  SELECT
      10,  -- domain_concept_id for 'Procedure Occurrence' (check concept ID from the domain table)
      pm.procedure_occurrence_id,
      9,   -- domain_concept_id for 'Condition Occurrence'
      cm.condition_occurrence_id,
      45877994  -- some concept ID for "Procedure has indication Condition" or a relevant relationship
  FROM ... 
  WHERE ... 

#You need to know the domain concept IDs from the OMOP vocabulary reference for each table:
#Condition Occurrence = 9
#Procedure Occurrence = 10
#Drug Exposure = 13
#Visit Occurrence = 8
#etc.
#Then pick a relationship_concept_id that expresses how they’re linked. OMOP doesn’t define many standard relationships out of the box, so you might define your own.

