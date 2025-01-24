-- =====================================================================
-- CREATE STAGING SCHEMA AND SEQUENCES
-- =====================================================================
CREATE SCHEMA IF NOT EXISTS staging;

-- Optionally set default search_path to staging
-- SET search_path TO staging;

-- Example: Sequences for assigning integer IDs in the staging process
CREATE SEQUENCE IF NOT EXISTS staging.person_seq START 1 INCREMENT 1;
CREATE SEQUENCE IF NOT EXISTS staging.visit_seq START 1 INCREMENT 1;
CREATE SEQUENCE IF NOT EXISTS staging.provider_seq START 1 INCREMENT 1;
CREATE SEQUENCE IF NOT EXISTS staging.care_site_seq START 1 INCREMENT 1;

-- =====================================================================
-- MAPPING TABLES FOR OMOP INTEGER KEYS
-- =====================================================================

-- 1) Person Mapping
CREATE TABLE IF NOT EXISTS staging.person_map (
  source_patient_id    TEXT PRIMARY KEY,  -- Could be UUID or any unique string
  person_id            INTEGER NOT NULL UNIQUE,
  created_at           TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
  updated_at           TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- 2) Visit/Encounter Mapping
CREATE TABLE IF NOT EXISTS staging.visit_map (
  source_visit_id      TEXT PRIMARY KEY,
  visit_occurrence_id  INTEGER NOT NULL UNIQUE,
  person_id            INTEGER NOT NULL,  -- Optionally store the mapped person_id too
  created_at           TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
  updated_at           TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- 3) Provider Mapping (if you have distinct providers)
CREATE TABLE IF NOT EXISTS staging.provider_map (
  source_provider_id   TEXT PRIMARY KEY,
  provider_id          INTEGER NOT NULL UNIQUE,
  created_at           TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- 4) Care Site Mapping (if you have distinct facilities)
CREATE TABLE IF NOT EXISTS staging.care_site_map (
  source_care_site_id  TEXT PRIMARY KEY,
  care_site_id         INTEGER NOT NULL UNIQUE,
  created_at           TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- =====================================================================
-- LOOKUP TABLES FOR RACE, ETHNICITY, GENDER
-- =====================================================================

-- For mapping your local race values to OMOP race_concept_id
CREATE TABLE IF NOT EXISTS staging.race_lookup (
  source_race              VARCHAR(50) PRIMARY KEY,
  race_concept_id          INTEGER NOT NULL,
  race_source_concept_id   INTEGER,
  description              VARCHAR(255)
);

-- For mapping your local ethnicity values to OMOP ethnicity_concept_id
CREATE TABLE IF NOT EXISTS staging.ethnicity_lookup (
  source_ethnicity             VARCHAR(50) PRIMARY KEY,
  ethnicity_concept_id         INTEGER NOT NULL,
  ethnicity_source_concept_id  INTEGER,
  description                  VARCHAR(255)
);

-- For mapping your local gender values (e.g. 'M','F') to OMOP concept IDs
CREATE TABLE IF NOT EXISTS staging.gender_lookup (
  source_gender              VARCHAR(10) PRIMARY KEY,
  gender_concept_id          INTEGER NOT NULL,
  gender_source_concept_id   INTEGER,
  description                VARCHAR(255)
);

-- =====================================================================
-- LOCAL-TO-OMOP CONCEPT MAP
-- =====================================================================

CREATE TABLE IF NOT EXISTS staging.local_to_omop_concept_map (
  source_code          VARCHAR(50) NOT NULL,
  source_vocabulary    VARCHAR(50) NOT NULL,  -- e.g. 'ICD10','CPT','RXNORM','LOCAL'
  source_description   VARCHAR(255),
  domain_id           VARCHAR(20),           -- e.g. 'Condition','Drug','Measurement'
  target_concept_id    INTEGER,              -- The standard OMOP concept_id
  target_vocabulary_id VARCHAR(20),
  valid_start_date     DATE,
  valid_end_date       DATE,
  invalid_reason       VARCHAR(1),
  created_at           TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (source_code, source_vocabulary)
);

-- =====================================================================
-- STAGING "RAW" TABLES FOR EACH SOURCE DOMAIN
-- (Adapt columns to match your real source data)
-- =====================================================================

-- 1) patients_raw (source from your "patients" table)
CREATE TABLE IF NOT EXISTS staging.patients_raw (
  id               TEXT,     -- Original patient UUID or ID
  birthdate        DATE,
  deathdate        DATE,
  race             VARCHAR(50),
  ethnicity        VARCHAR(50),
  gender           VARCHAR(10),
  first_name       VARCHAR(100),
  last_name        VARCHAR(100),
  address          TEXT,
  city             VARCHAR(100),
  state            VARCHAR(50),
  zip              VARCHAR(10),
  county           VARCHAR(100),
  latitude         NUMERIC(17,14),
  longitude        NUMERIC(17,14),
  income           NUMERIC(10,2),
  healthcare_expenses NUMERIC(10,2),
  healthcare_coverage NUMERIC(10,2),
  created_at       TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  updated_at       TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- 2) encounters_raw
CREATE TABLE IF NOT EXISTS staging.encounters_raw (
  id                    TEXT,  -- Original encounter UUID
  patient_id            TEXT,  -- Link to patients_raw.id
  start_timestamp       TIMESTAMP,
  stop_timestamp        TIMESTAMP,
  encounter_class       VARCHAR(50),
  code                  VARCHAR(20),
  description           TEXT,
  base_encounter_cost   DECIMAL(10,2),
  total_claim_cost      DECIMAL(10,2),
  payer_coverage        DECIMAL(10,2),
  reason_code           VARCHAR(20),
  reason_description    TEXT,
  organization_id       TEXT,
  provider_id           TEXT,
  care_site_id          TEXT,
  created_at            TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  updated_at            TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- 3) conditions_raw
CREATE TABLE IF NOT EXISTS staging.conditions_raw (
  id                    TEXT,
  patient_id            TEXT,
  encounter_id          TEXT,
  code                  VARCHAR(20),
  system                VARCHAR(50),
  description           TEXT,
  start_date            DATE,
  start_datetime        TIMESTAMP,
  stop_date             DATE,
  stop_datetime         TIMESTAMP,
  type                  VARCHAR(20),
  category              VARCHAR(20),
  created_at            TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  updated_at            TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- 4) medications_raw
CREATE TABLE IF NOT EXISTS staging.medications_raw (
  id                 TEXT,
  patient_id         TEXT,
  encounter_id       TEXT,
  code               VARCHAR(20),
  description        TEXT,
  start_timestamp    TIMESTAMP,
  stop_timestamp     TIMESTAMP,
  base_cost          DECIMAL(10,2),
  payer_coverage     DECIMAL(10,2),
  dispenses          INTEGER,
  total_cost         DECIMAL(10,2),
  reason_code        VARCHAR(20),
  reason_description TEXT,
  created_at         TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  updated_at         TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- 5) devices_raw
CREATE TABLE IF NOT EXISTS staging.devices_raw (
  id                 TEXT,
  patient_id         TEXT,
  encounter_id       TEXT,
  code               VARCHAR(20),
  description        TEXT,
  udi                TEXT,
  start_timestamp    TIMESTAMP,
  stop_timestamp     TIMESTAMP,
  created_at         TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  updated_at         TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- 6) measurements_raw (for labs, vitals)
CREATE TABLE IF NOT EXISTS staging.measurements_raw (
  id                 TEXT,
  patient_id         TEXT,
  encounter_id       TEXT,
  category           VARCHAR(50),   -- e.g. 'laboratory' or 'vital-signs'
  code               VARCHAR(20),
  description        TEXT,
  value              NUMERIC,
  units              VARCHAR(20),
  timestamp          TIMESTAMP,
  created_at         TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  updated_at         TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- 7) observations_raw (non-numeric or social observations)
CREATE TABLE IF NOT EXISTS staging.observations_raw (
  id                 TEXT,
  patient_id         TEXT,
  encounter_id       TEXT,
  observation_type   VARCHAR(50),   -- e.g. 'social-history'
  code               VARCHAR(20),
  description        TEXT,
  value_as_string    TEXT,
  timestamp          TIMESTAMP,
  created_at         TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  updated_at         TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- 8) immunizations_raw
CREATE TABLE IF NOT EXISTS staging.immunizations_raw (
  id                 TEXT,
  patient_id         TEXT,
  encounter_id       TEXT,
  code               VARCHAR(20),
  description        TEXT,
  date               TIMESTAMP,
  base_cost          DECIMAL(10,2),
  created_at         TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  updated_at         TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- 9) allergies_raw
CREATE TABLE IF NOT EXISTS staging.allergies_raw (
  id                 TEXT,
  patient_id         TEXT,
  encounter_id       TEXT,
  code               VARCHAR(20),
  system             VARCHAR(50),
  description        TEXT,
  type               VARCHAR(20),
  category           VARCHAR(20),
  reaction1_code     VARCHAR(20),
  reaction1_desc     TEXT,
  severity1          VARCHAR(10),
  reaction2_code     VARCHAR(20),
  reaction2_desc     TEXT,
  severity2          VARCHAR(10),
  start_date         DATE,
  stop_date          DATE,
  created_at         TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  updated_at         TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- 10) careplans_raw
CREATE TABLE IF NOT EXISTS staging.careplans_raw (
  id                 TEXT,
  patient_id         TEXT,
  encounter_id       TEXT,
  start_date         DATE,
  stop_date          DATE,
  code               VARCHAR(20),
  description        TEXT,
  reason_code        VARCHAR(20),
  reason_description TEXT,
  created_at         TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  updated_at         TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- 11) claims_raw
CREATE TABLE IF NOT EXISTS staging.claims_raw (
  id                 TEXT,
  patient_id         TEXT,
  provider_id        TEXT,
  payer_id           TEXT,
  department_id      TEXT,
  diagnosis1         VARCHAR(20),
  diagnosis2         VARCHAR(20),
  status1            VARCHAR(20),
  status2            VARCHAR(20),
  outstanding1       DECIMAL(10,2),
  outstanding2       DECIMAL(10,2),
  service_date       TIMESTAMP,
  created_at         TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  updated_at         TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- 12) claims_transactions_raw
CREATE TABLE IF NOT EXISTS staging.claims_transactions_raw (
  id                 TEXT,
  claim_id           TEXT,
  patient_id         TEXT,
  type               VARCHAR(20),
  amount             DECIMAL(10,2),
  payment_method     VARCHAR(20),
  from_date          TIMESTAMP,
  to_date            TIMESTAMP,
  procedure_code     VARCHAR(20),
  diagnosis_ref1     VARCHAR(20),
  units              INTEGER,
  provider_id        TEXT,
  created_at         TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  updated_at         TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- 13) patient_expenses_raw (if you have that table)
CREATE TABLE IF NOT EXISTS staging.patient_expenses_raw (
  patient_id         TEXT,
  year_date          TIMESTAMP,
  payer_id           TEXT,
  healthcare_expenses  DECIMAL(12,2),
  insurance_costs      DECIMAL(12,2),
  covered_costs        DECIMAL(12,2),
  created_at         TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  updated_at         TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- You could add others (notes_raw, caretaker_raw, etc.) as needed.

-- =====================================================================
-- END OF STAGING DDL
-- =====================================================================
