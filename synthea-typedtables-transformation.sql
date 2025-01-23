BEGIN;

-------------------------------------------------------------------------------
-- 1) ALLERGIES
-------------------------------------------------------------------------------
DROP TABLE IF EXISTS population.allergies_typed CASCADE;

CREATE TABLE population.allergies_typed (
  start_time       TIMESTAMP,
  stop_time        TIMESTAMP,
  patient          UUID,
  encounter        UUID,
  code             TEXT,
  system           TEXT,
  description      TEXT,
  allergy_type     population.allergy_type,     -- 'allergy','intolerance'
  allergy_category population.allergy_category, -- 'environment','medication','food'
  reaction1        TEXT,
  description1     TEXT,
  severity1        population.reaction_severity,  -- 'MILD','MODERATE','SEVERE'
  reaction2        TEXT,
  description2     TEXT,
  severity2        population.reaction_severity
);

INSERT INTO population.allergies_typed (
  start_time, stop_time, patient, encounter,
  code, system, description,
  allergy_type, allergy_category,
  reaction1, description1, severity1,
  reaction2, description2, severity2
)
SELECT
  CASE WHEN "START" ~ '^\d{4}-\d{2}-\d{2}' THEN "START"::timestamp ELSE NULL END AS start_time,
  CASE WHEN "STOP"  ~ '^\d{4}-\d{2}-\d{2}' THEN "STOP"::timestamp  ELSE NULL END AS stop_time,
  CASE WHEN "PATIENT"   ~ '^[0-9a-fA-F-]{36}$' THEN "PATIENT"::uuid   ELSE NULL END AS patient,
  CASE WHEN "ENCOUNTER" ~ '^[0-9a-fA-F-]{36}$' THEN "ENCOUNTER"::uuid ELSE NULL END AS encounter,
  "CODE"       AS code,
  "SYSTEM"     AS system,
  "DESCRIPTION" AS description,
  CASE WHEN "TYPE" IN ('allergy','intolerance')
       THEN "TYPE"::population.allergy_type
       ELSE NULL
  END AS allergy_type,
  CASE WHEN "CATEGORY" IN ('environment','medication','food')
       THEN "CATEGORY"::population.allergy_category
       ELSE NULL
  END AS allergy_category,
  "REACTION1"    AS reaction1,
  "DESCRIPTION1" AS description1,
  CASE WHEN "SEVERITY1" IN ('MILD','MODERATE','SEVERE')
       THEN "SEVERITY1"::population.reaction_severity
       ELSE NULL
  END AS severity1,
  "REACTION2"    AS reaction2,
  "DESCRIPTION2" AS description2,
  CASE WHEN "SEVERITY2" IN ('MILD','MODERATE','SEVERE')
       THEN "SEVERITY2"::population.reaction_severity
       ELSE NULL
  END AS severity2
FROM population.allergies;


-------------------------------------------------------------------------------
-- 2) CAREPLANS
-------------------------------------------------------------------------------
DROP TABLE IF EXISTS population.careplans_typed CASCADE;

CREATE TABLE population.careplans_typed (
  careplan_id      UUID,
  start_time       TIMESTAMP,
  stop_time        TIMESTAMP,
  patient          UUID,
  encounter        UUID,
  code             TEXT,
  description      TEXT,
  reasoncode       TEXT,
  reasondescription TEXT
);

INSERT INTO population.careplans_typed (
  careplan_id, start_time, stop_time, patient, encounter,
  code, description, reasoncode, reasondescription
)
SELECT
  CASE WHEN "Id"       ~ '^[0-9a-fA-F-]{36}$' THEN "Id"::uuid       ELSE NULL END,
  CASE WHEN "START"    ~ '^\d{4}-\d{2}-\d{2}' THEN "START"::timestamp ELSE NULL END,
  CASE WHEN "STOP"     ~ '^\d{4}-\d{2}-\d{2}' THEN "STOP"::timestamp  ELSE NULL END,
  CASE WHEN "PATIENT"  ~ '^[0-9a-fA-F-]{36}$' THEN "PATIENT"::uuid    ELSE NULL END,
  CASE WHEN "ENCOUNTER"~ '^[0-9a-fA-F-]{36}$' THEN "ENCOUNTER"::uuid  ELSE NULL END,
  "CODE",
  "DESCRIPTION",
  "REASONCODE",
  "REASONDESCRIPTION"
FROM population.careplans;


-------------------------------------------------------------------------------
-- 3) CLAIMS
-------------------------------------------------------------------------------
DROP TABLE IF EXISTS population.claims_typed CASCADE;

CREATE TABLE population.claims_typed (
  claim_id                 UUID,
  patient_id               UUID,
  provider_id              UUID,
  primary_insurance_id     UUID,
  secondary_insurance_id   UUID,
  department_id            TEXT,
  patient_department_id    TEXT,
  diagnosis1               TEXT,
  diagnosis2               TEXT,
  diagnosis3               TEXT,
  diagnosis4               TEXT,
  diagnosis5               TEXT,
  diagnosis6               TEXT,
  diagnosis7               TEXT,
  diagnosis8               TEXT,
  referring_provider_id    UUID,
  appointment_id           UUID,
  current_illness_date     TIMESTAMP,
  service_date             TIMESTAMP,
  supervising_provider_id  UUID,
  status1                  population.claim_status,
  status2                  population.claim_status,
  statusp                  population.claim_status,
  outstanding1             NUMERIC,
  outstanding2             NUMERIC,
  outstandingp             NUMERIC,
  last_billed_date1        TIMESTAMP,
  last_billed_date2        TIMESTAMP,
  last_billed_datep        TIMESTAMP,
  healthcare_claimtypeid1  TEXT,
  healthcare_claimtypeid2  TEXT
);

INSERT INTO population.claims_typed (
  claim_id,
  patient_id, provider_id,
  primary_insurance_id, secondary_insurance_id,
  department_id, patient_department_id,
  diagnosis1, diagnosis2, diagnosis3, diagnosis4, diagnosis5,
  diagnosis6, diagnosis7, diagnosis8,
  referring_provider_id,
  appointment_id,
  current_illness_date,
  service_date,
  supervising_provider_id,
  status1, status2, statusp,
  outstanding1, outstanding2, outstandingp,
  last_billed_date1, last_billed_date2, last_billed_datep,
  healthcare_claimtypeid1, healthcare_claimtypeid2
)
SELECT
  CASE WHEN "Id"             ~ '^[0-9a-fA-F-]{36}$' THEN "Id"::uuid ELSE NULL END,
  CASE WHEN "PATIENTID"      ~ '^[0-9a-fA-F-]{36}$' THEN "PATIENTID"::uuid ELSE NULL END,
  CASE WHEN "PROVIDERID"     ~ '^[0-9a-fA-F-]{36}$' THEN "PROVIDERID"::uuid ELSE NULL END,
  CASE WHEN "PRIMARYPATIENTINSURANCEID"   ~ '^[0-9a-fA-F-]{36}$' THEN "PRIMARYPATIENTINSURANCEID"::uuid ELSE NULL END,
  CASE WHEN "SECONDARYPATIENTINSURANCEID" ~ '^[0-9a-fA-F-]{36}$' THEN "SECONDARYPATIENTINSURANCEID"::uuid ELSE NULL END,
  "DEPARTMENTID",
  "PATIENTDEPARTMENTID",
  "DIAGNOSIS1",
  "DIAGNOSIS2",
  "DIAGNOSIS3",
  "DIAGNOSIS4",
  "DIAGNOSIS5",
  "DIAGNOSIS6",
  "DIAGNOSIS7",
  "DIAGNOSIS8",
  CASE WHEN "REFERRINGPROVIDERID" ~ '^[0-9a-fA-F-]{36}$' THEN "REFERRINGPROVIDERID"::uuid ELSE NULL END,
  CASE WHEN "APPOINTMENTID"       ~ '^[0-9a-fA-F-]{36}$' THEN "APPOINTMENTID"::uuid       ELSE NULL END,
  CASE WHEN "CURRENTILLNESSDATE" ~ '^\d{4}-\d{2}-\d{2}' THEN "CURRENTILLNESSDATE"::timestamp ELSE NULL END,
  CASE WHEN "SERVICEDATE"        ~ '^\d{4}-\d{2}-\d{2}' THEN "SERVICEDATE"::timestamp      ELSE NULL END,
  CASE WHEN "SUPERVISINGPROVIDERID" ~ '^[0-9a-fA-F-]{36}$' THEN "SUPERVISINGPROVIDERID"::uuid ELSE NULL END,

  CASE WHEN "STATUS1" IN ('OPEN','CLOSED','PENDING','DENIED') THEN "STATUS1"::population.claim_status ELSE NULL END,
  CASE WHEN "STATUS2" IN ('OPEN','CLOSED','PENDING','DENIED') THEN "STATUS2"::population.claim_status ELSE NULL END,
  CASE WHEN "STATUSP" IN ('OPEN','CLOSED','PENDING','DENIED') THEN "STATUSP"::population.claim_status ELSE NULL END,

  CASE WHEN "OUTSTANDING1" ~ '^\d+(\.\d+)?$' THEN "OUTSTANDING1"::numeric ELSE NULL END,
  CASE WHEN "OUTSTANDING2" ~ '^\d+(\.\d+)?$' THEN "OUTSTANDING2"::numeric ELSE NULL END,
  CASE WHEN "OUTSTANDINGP" ~ '^\d+(\.\d+)?$' THEN "OUTSTANDINGP"::numeric ELSE NULL END,

  CASE WHEN "LASTBILLEDDATE1" ~ '^\d{4}-\d{2}-\d{2}' THEN "LASTBILLEDDATE1"::timestamp ELSE NULL END,
  CASE WHEN "LASTBILLEDDATE2" ~ '^\d{4}-\d{2}-\d{2}' THEN "LASTBILLEDDATE2"::timestamp ELSE NULL END,
  CASE WHEN "LASTBILLEDDATEP" ~ '^\d{4}-\d{2}-\d{2}' THEN "LASTBILLEDDATEP"::timestamp ELSE NULL END,

  "HEALTHCARECLAIMTYPEID1",
  "HEALTHCARECLAIMTYPEID2"
FROM population.claims;


-------------------------------------------------------------------------------
-- 4) CLAIMS_TRANSACTIONS
-------------------------------------------------------------------------------
DROP TABLE IF EXISTS population.claims_transactions_typed CASCADE;

CREATE TABLE population.claims_transactions_typed (
  id               UUID,
  claim_id         UUID,
  charge_id        UUID,
  patient_id       UUID,
  transaction_type population.transaction_type,  -- 'CHARGE','PAYMENT','TRANSFERIN','TRANSFEROUT'
  amount           NUMERIC,
  payment_method   population.payment_method,    -- 'CASH','CHECK','CC','ECHECK'
  from_date        TIMESTAMP,
  to_date          TIMESTAMP,
  place_of_service TEXT,
  procedure_code   TEXT,
  modifier1        TEXT,
  modifier2        TEXT,
  diagnosisref1    TEXT,
  diagnosisref2    TEXT,
  diagnosisref3    TEXT,
  diagnosisref4    TEXT,
  units            NUMERIC,
  department_id    TEXT,
  notes            TEXT,
  unit_amount      NUMERIC,
  transferout_id   UUID,
  transfer_type    population.transfer_type,     -- '1','p'
  payments         NUMERIC,
  adjustments      NUMERIC,
  transfers        NUMERIC,
  outstanding      NUMERIC,
  appointment_id   UUID,
  linenote         TEXT,
  patientinsuranceid UUID,
  feescheduleid    TEXT,
  provider_id      UUID,
  supervisingproviderid UUID
);

INSERT INTO population.claims_transactions_typed (
  id, claim_id, charge_id, patient_id, transaction_type, amount,
  payment_method, from_date, to_date, place_of_service, procedure_code,
  modifier1, modifier2, diagnosisref1, diagnosisref2, diagnosisref3, diagnosisref4,
  units, department_id, notes, unit_amount, transferout_id, transfer_type,
  payments, adjustments, transfers, outstanding, appointment_id, linenote,
  patientinsuranceid, feescheduleid, provider_id, supervisingproviderid
)
SELECT
  CASE WHEN "ID"       ~ '^[0-9a-fA-F-]{36}$' THEN "ID"::uuid ELSE NULL END,
  CASE WHEN "CLAIMID"  ~ '^[0-9a-fA-F-]{36}$' THEN "CLAIMID"::uuid ELSE NULL END,
  CASE WHEN "CHARGEID" ~ '^[0-9a-fA-F-]{36}$' THEN "CHARGEID"::uuid ELSE NULL END,
  CASE WHEN "PATIENTID"~ '^[0-9a-fA-F-]{36}$' THEN "PATIENTID"::uuid ELSE NULL END,

  CASE WHEN "TYPE" IN ('CHARGE','PAYMENT','TRANSFERIN','TRANSFEROUT')
       THEN "TYPE"::population.transaction_type
       ELSE NULL
  END,

  CASE WHEN "AMOUNT" ~ '^\d+(\.\d+)?$' THEN "AMOUNT"::numeric ELSE NULL END,

  CASE WHEN "METHOD" IN ('CASH','CHECK','CC','ECHECK')
       THEN "METHOD"::population.payment_method
       ELSE NULL
  END,

  CASE WHEN "FROMDATE" ~ '^\d{4}-\d{2}-\d{2}' THEN "FROMDATE"::timestamp ELSE NULL END,
  CASE WHEN "TODATE"   ~ '^\d{4}-\d{2}-\d{2}' THEN "TODATE"::timestamp   ELSE NULL END,

  "PLACEOFSERVICE",
  "PROCEDURECODE",
  "MODIFIER1",
  "MODIFIER2",
  "DIAGNOSISREF1",
  "DIAGNOSISREF2",
  "DIAGNOSISREF3",
  "DIAGNOSISREF4",

  CASE WHEN "UNITS" ~ '^\d+(\.\d+)?$' THEN "UNITS"::numeric ELSE NULL END,
  "DEPARTMENTID",
  "NOTES",
  CASE WHEN "UNITAMOUNT" ~ '^\d+(\.\d+)?$' THEN "UNITAMOUNT"::numeric ELSE NULL END,
  CASE WHEN "TRANSFEROUTID" ~ '^[0-9a-fA-F-]{36}$' THEN "TRANSFEROUTID"::uuid ELSE NULL END,

  CASE WHEN "TRANSFERTYPE" IN ('1','p') THEN "TRANSFERTYPE"::population.transfer_type ELSE NULL END,

  CASE WHEN "PAYMENTS"    ~ '^\d+(\.\d+)?$' THEN "PAYMENTS"::numeric    ELSE NULL END,
  CASE WHEN "ADJUSTMENTS" ~ '^\d+(\.\d+)?$' THEN "ADJUSTMENTS"::numeric ELSE NULL END,
  CASE WHEN "TRANSFERS"   ~ '^\d+(\.\d+)?$' THEN "TRANSFERS"::numeric   ELSE NULL END,
  CASE WHEN "OUTSTANDING" ~ '^\d+(\.\d+)?$' THEN "OUTSTANDING"::numeric ELSE NULL END,
  CASE WHEN "APPOINTMENTID" ~ '^[0-9a-fA-F-]{36}$' THEN "APPOINTMENTID"::uuid ELSE NULL END,
  "LINENOTE",
  CASE WHEN "PATIENTINSURANCEID" ~ '^[0-9a-fA-F-]{36}$' THEN "PATIENTINSURANCEID"::uuid ELSE NULL END,
  "FEESCHEDULEID",
  CASE WHEN "PROVIDERID" ~ '^[0-9a-fA-F-]{36}$' THEN "PROVIDERID"::uuid ELSE NULL END,
  CASE WHEN "SUPERVISINGPROVIDERID" ~ '^[0-9a-fA-F-]{36}$' THEN "SUPERVISINGPROVIDERID"::uuid ELSE NULL END
FROM population.claims_transactions;

-------------------------------------------------------------------------------
-- 5) CONDITIONS
-------------------------------------------------------------------------------
DROP TABLE IF EXISTS population.conditions_typed CASCADE;

CREATE TABLE population.conditions_typed (
  start_time  TIMESTAMP,
  stop_time   TIMESTAMP,
  patient     UUID,
  encounter   UUID,
  coding_sys  population.condition_coding_system, -- 'SNOMED-CT','ICD-10','ICD-9'
  code        TEXT,
  description TEXT
);

INSERT INTO population.conditions_typed (
  start_time, stop_time, patient, encounter,
  coding_sys, code, description
)
SELECT
  CASE WHEN "START" ~ '^\d{4}-\d{2}-\d{2}' THEN "START"::timestamp ELSE NULL END,
  CASE WHEN "STOP"  ~ '^\d{4}-\d{2}-\d{2}' THEN "STOP"::timestamp  ELSE NULL END,
  CASE WHEN "PATIENT"   ~ '^[0-9a-fA-F-]{36}$' THEN "PATIENT"::uuid   ELSE NULL END,
  CASE WHEN "ENCOUNTER" ~ '^[0-9a-fA-F-]{36}$' THEN "ENCOUNTER"::uuid ELSE NULL END,
  CASE WHEN "SYSTEM" IN ('SNOMED-CT','ICD-10','ICD-9') THEN "SYSTEM"::population.condition_coding_system ELSE NULL END,
  "CODE",
  "DESCRIPTION"
FROM population.conditions;

-------------------------------------------------------------------------------
-- 6) DEVICES
-------------------------------------------------------------------------------
DROP TABLE IF EXISTS population.devices_typed CASCADE;

CREATE TABLE population.devices_typed (
  start_time   TIMESTAMP,
  stop_time    TIMESTAMP,
  patient      UUID,
  encounter    UUID,
  code         TEXT,
  description  TEXT,
  udi          TEXT
);

INSERT INTO population.devices_typed (
  start_time, stop_time, patient, encounter,
  code, description, udi
)
SELECT
  CASE WHEN "START" ~ '^\d{4}-\d{2}-\d{2}' THEN "START"::timestamp ELSE NULL END,
  CASE WHEN "STOP"  ~ '^\d{4}-\d{2}-\d{2}' THEN "STOP"::timestamp  ELSE NULL END,
  CASE WHEN "PATIENT"   ~ '^[0-9a-fA-F-]{36}$' THEN "PATIENT"::uuid   ELSE NULL END,
  CASE WHEN "ENCOUNTER" ~ '^[0-9a-fA-F-]{36}$' THEN "ENCOUNTER"::uuid ELSE NULL END,
  "CODE",
  "DESCRIPTION",
  "UDI"
FROM population.devices;

-------------------------------------------------------------------------------
-- 7) ENCOUNTERS
-------------------------------------------------------------------------------
DROP TABLE IF EXISTS population.encounters_typed CASCADE;

CREATE TABLE population.encounters_typed (
  encounter_id      UUID,
  start_time        TIMESTAMP,
  stop_time         TIMESTAMP,
  patient           UUID,
  organization      UUID,
  provider          UUID,
  payer             UUID,
  encounter_class   TEXT,  -- could be an enum if known
  code              TEXT,
  description       TEXT,
  base_encounter_cost NUMERIC,
  total_claim_cost  NUMERIC,
  payer_coverage    NUMERIC,
  reasoncode        TEXT,
  reasondescription TEXT
);

INSERT INTO population.encounters_typed (
  encounter_id, start_time, stop_time, patient,
  organization, provider, payer,
  encounter_class, code, description,
  base_encounter_cost, total_claim_cost, payer_coverage,
  reasoncode, reasondescription
)
SELECT
  CASE WHEN "Id" ~ '^[0-9a-fA-F-]{36}$' THEN "Id"::uuid ELSE NULL END,
  CASE WHEN "START" ~ '^\d{4}-\d{2}-\d{2}' THEN "START"::timestamp ELSE NULL END,
  CASE WHEN "STOP"  ~ '^\d{4}-\d{2}-\d{2}' THEN "STOP"::timestamp  ELSE NULL END,
  CASE WHEN "PATIENT"      ~ '^[0-9a-fA-F-]{36}$' THEN "PATIENT"::uuid      ELSE NULL END,
  CASE WHEN "ORGANIZATION" ~ '^[0-9a-fA-F-]{36}$' THEN "ORGANIZATION"::uuid ELSE NULL END,
  CASE WHEN "PROVIDER"     ~ '^[0-9a-fA-F-]{36}$' THEN "PROVIDER"::uuid     ELSE NULL END,
  CASE WHEN "PAYER"        ~ '^[0-9a-fA-F-]{36}$' THEN "PAYER"::uuid        ELSE NULL END,

  "ENCOUNTERCLASS",
  "CODE",
  "DESCRIPTION",

  CASE WHEN "BASE_ENCOUNTER_COST" ~ '^\d+(\.\d+)?$' THEN "BASE_ENCOUNTER_COST"::numeric ELSE NULL END,
  CASE WHEN "TOTAL_CLAIM_COST"    ~ '^\d+(\.\d+)?$' THEN "TOTAL_CLAIM_COST"::numeric    ELSE NULL END,
  CASE WHEN "PAYER_COVERAGE"      ~ '^\d+(\.\d+)?$' THEN "PAYER_COVERAGE"::numeric      ELSE NULL END,
  "REASONCODE",
  "REASONDESCRIPTION"
FROM population.encounters;

-------------------------------------------------------------------------------
-- 8) IMAGING_STUDIES
-------------------------------------------------------------------------------
DROP TABLE IF EXISTS population.imaging_studies_typed CASCADE;

CREATE TABLE population.imaging_studies_typed (
  imaging_id         UUID,
  date_time          TIMESTAMP,
  patient            UUID,
  encounter          UUID,
  series_uid         TEXT,
  bodysite_code      TEXT,
  bodysite_desc      TEXT,
  modality_code      population.imaging_modality,  -- e.g. 'DX','CR','US'
  modality_desc      TEXT,
  instance_uid       TEXT,
  sop_code           TEXT,
  sop_description    TEXT,
  procedure_code     TEXT
);

INSERT INTO population.imaging_studies_typed (
  imaging_id, date_time, patient, encounter,
  series_uid, bodysite_code, bodysite_desc,
  modality_code, modality_desc,
  instance_uid, sop_code, sop_description,
  procedure_code
)
SELECT
  CASE WHEN "Id" ~ '^[0-9a-fA-F-]{36}$' THEN "Id"::uuid ELSE NULL END,
  CASE WHEN "DATE" ~ '^\d{4}-\d{2}-\d{2}' THEN "DATE"::timestamp ELSE NULL END,
  CASE WHEN "PATIENT"   ~ '^[0-9a-fA-F-]{36}$' THEN "PATIENT"::uuid   ELSE NULL END,
  CASE WHEN "ENCOUNTER" ~ '^[0-9a-fA-F-]{36}$' THEN "ENCOUNTER"::uuid ELSE NULL END,
  "SERIES_UID",
  "BODYSITE_CODE",
  "BODYSITE_DESCRIPTION",
  CASE WHEN "MODALITY_CODE" IN ('DX','CR','US')
       THEN "MODALITY_CODE"::population.imaging_modality
       ELSE NULL
  END,
  "MODALITY_DESCRIPTION",
  "INSTANCE_UID",
  "SOP_CODE",
  "SOP_DESCRIPTION",
  "PROCEDURE_CODE"
FROM population.imaging_studies;

-------------------------------------------------------------------------------
-- 9) IMMUNIZATIONS
-------------------------------------------------------------------------------
DROP TABLE IF EXISTS population.immunizations_typed CASCADE;

CREATE TABLE population.immunizations_typed (
  date_time    TIMESTAMP,
  patient      UUID,
  encounter    UUID,
  code         population.immunization_code /* e.g. '08','10','20','49','119','133','140' */,
  description  TEXT,
  base_cost    NUMERIC
);

INSERT INTO population.immunizations_typed (
  date_time, patient, encounter,
  code, description, base_cost
)
SELECT
  CASE WHEN "DATE" ~ '^\d{4}-\d{2}-\d{2}' THEN "DATE"::timestamp ELSE NULL END,
  CASE WHEN "PATIENT"   ~ '^[0-9a-fA-F-]{36}$' THEN "PATIENT"::uuid   ELSE NULL END,
  CASE WHEN "ENCOUNTER" ~ '^[0-9a-fA-F-]{36}$' THEN "ENCOUNTER"::uuid ELSE NULL END,
  CASE WHEN "CODE" IN ('08','10','20','49','119','133','140')
       THEN "CODE"::population.immunization_code
       ELSE NULL
  END,
  "DESCRIPTION",
  CASE WHEN "BASE_COST" ~ '^\d+(\.\d+)?$' THEN "BASE_COST"::numeric ELSE NULL END
FROM population.immunizations;


-------------------------------------------------------------------------------
-- 10) MEDICATIONS
-------------------------------------------------------------------------------
DROP TABLE IF EXISTS population.medications_typed CASCADE;

CREATE TABLE population.medications_typed (
  start_time      TIMESTAMP,
  stop_time       TIMESTAMP,
  patient         UUID,
  payer           UUID,
  encounter       UUID,
  code            TEXT,
  description     TEXT,
  base_cost       NUMERIC,
  payer_coverage  NUMERIC,
  dispenses       INT,
  total_cost      NUMERIC,
  reasoncode      TEXT,
  reasondescription TEXT
);

INSERT INTO population.medications_typed (
  start_time, stop_time, patient, payer, encounter,
  code, description, base_cost, payer_coverage,
  dispenses, total_cost, reasoncode, reasondescription
)
SELECT
  CASE WHEN "START" ~ '^\d{4}-\d{2}-\d{2}' THEN "START"::timestamp ELSE NULL END,
  CASE WHEN "STOP"  ~ '^\d{4}-\d{2}-\d{2}' THEN "STOP"::timestamp  ELSE NULL END,
  CASE WHEN "PATIENT"   ~ '^[0-9a-fA-F-]{36}$' THEN "PATIENT"::uuid   ELSE NULL END,
  CASE WHEN "PAYER"     ~ '^[0-9a-fA-F-]{36}$' THEN "PAYER"::uuid     ELSE NULL END,
  CASE WHEN "ENCOUNTER" ~ '^[0-9a-fA-F-]{36}$' THEN "ENCOUNTER"::uuid ELSE NULL END,
  "CODE",
  "DESCRIPTION",
  CASE WHEN "BASE_COST"      ~ '^\d+(\.\d+)?$' THEN "BASE_COST"::numeric      ELSE NULL END,
  CASE WHEN "PAYER_COVERAGE" ~ '^\d+(\.\d+)?$' THEN "PAYER_COVERAGE"::numeric ELSE NULL END,
  CASE WHEN "DISPENSES"      ~ '^\d+$' THEN "DISPENSES"::int ELSE NULL END,
  CASE WHEN "TOTALCOST"      ~ '^\d+(\.\d+)?$' THEN "TOTALCOST"::numeric      ELSE NULL END,
  "REASONCODE",
  "REASONDESCRIPTION"
FROM population.medications;

-------------------------------------------------------------------------------
-- 11) OBSERVATIONS
-------------------------------------------------------------------------------
DROP TABLE IF EXISTS population.observations_typed CASCADE;

CREATE TABLE population.observations_typed (
  date_time       TIMESTAMP,
  patient         UUID,
  encounter       UUID,
  category        population.observation_category /* e.g. 'vital-signs' */,
  code            TEXT,
  description     TEXT,
  value           TEXT,
  units           population.observation_unit     /* e.g. 'cm','kg','%','mm[Hg]','/min','{score}' */,
  observation_type population.observation_type     /* e.g. 'numeric' */
  /* etc., depending on your real needs */
);

INSERT INTO population.observations_typed (
  date_time, patient, encounter, category,
  code, description, value, units, observation_type
)
SELECT
  CASE WHEN "DATE" ~ '^\d{4}-\d{2}-\d{2}' THEN "DATE"::timestamp ELSE NULL END,
  CASE WHEN "PATIENT"   ~ '^[0-9a-fA-F-]{36}$' THEN "PATIENT"::uuid   ELSE NULL END,
  CASE WHEN "ENCOUNTER" ~ '^[0-9a-fA-F-]{36}$' THEN "ENCOUNTER"::uuid ELSE NULL END,
  CASE WHEN "CATEGORY" IN ('vital-signs')
       THEN "CATEGORY"::population.observation_category
       ELSE NULL
  END,
  "CODE",
  "DESCRIPTION",
  "VALUE",
  CASE WHEN "UNITS" IN ('cm','kg','%','mm[Hg]','/min','{score}')
       THEN "UNITS"::population.observation_unit
       ELSE NULL
  END,
  CASE WHEN "TYPE" IN ('numeric')
       THEN "TYPE"::population.observation_type
       ELSE NULL
  END
FROM population.observations;


-------------------------------------------------------------------------------
-- 12) ORGANIZATIONS
-------------------------------------------------------------------------------
DROP TABLE IF EXISTS population.organizations_typed CASCADE;

CREATE TABLE population.organizations_typed (
  organization_id UUID,
  name            TEXT,
  address         TEXT,
  city            TEXT,
  state           TEXT,
  zip             TEXT,
  lat             NUMERIC,
  lon             NUMERIC,
  phone           TEXT,
  revenue         NUMERIC,
  utilization     NUMERIC
);

INSERT INTO population.organizations_typed (
  organization_id, name, address, city, state, zip,
  lat, lon, phone, revenue, utilization
)
SELECT
  CASE WHEN "Id" ~ '^[0-9a-fA-F-]{36}$' THEN "Id"::uuid ELSE NULL END,
  "NAME",
  "ADDRESS",
  "CITY",
  "STATE",
  "ZIP",
  CASE WHEN "LAT" ~ '^-?\d+(\.\d+)?$' THEN "LAT"::numeric ELSE NULL END,
  CASE WHEN "LON" ~ '^-?\d+(\.\d+)?$' THEN "LON"::numeric ELSE NULL END,
  "PHONE",
  CASE WHEN "REVENUE"     ~ '^\d+(\.\d+)?$' THEN "REVENUE"::numeric     ELSE NULL END,
  CASE WHEN "UTILIZATION" ~ '^\d+(\.\d+)?$' THEN "UTILIZATION"::numeric ELSE NULL END
FROM population.organizations;

-------------------------------------------------------------------------------
-- 13) PATIENT_EXPENSES
-------------------------------------------------------------------------------
DROP TABLE IF EXISTS population.patient_expenses_typed CASCADE;

CREATE TABLE population.patient_expenses_typed (
  patient_id          UUID,
  expense_year        INT,
  payer_id            UUID,
  healthcare_expenses NUMERIC,
  insurance_costs     NUMERIC,
  covered_costs       NUMERIC
);

INSERT INTO population.patient_expenses_typed (
  patient_id, expense_year, payer_id,
  healthcare_expenses, insurance_costs, covered_costs
)
SELECT
  CASE WHEN "PATIENT_ID" ~ '^[0-9a-fA-F-]{36}$' THEN "PATIENT_ID"::uuid ELSE NULL END,
  CASE WHEN "YEAR" ~ '^\d+$' THEN "YEAR"::int ELSE NULL END,
  CASE WHEN "PAYER_ID" ~ '^[0-9a-fA-F-]{36}$' THEN "PAYER_ID"::uuid ELSE NULL END,
  CASE WHEN "HEALTHCARE_EXPENSES" ~ '^\d+(\.\d+)?$' THEN "HEALTHCARE_EXPENSES"::numeric ELSE NULL END,
  CASE WHEN "INSURANCE_COSTS"     ~ '^\d+(\.\d+)?$' THEN "INSURANCE_COSTS"::numeric     ELSE NULL END,
  CASE WHEN "COVERED_COSTS"       ~ '^\d+(\.\d+)?$' THEN "COVERED_COSTS"::numeric       ELSE NULL END
FROM population.patient_expenses;

-------------------------------------------------------------------------------
-- 14) PATIENTS
-------------------------------------------------------------------------------
DROP TABLE IF EXISTS population.patients_typed CASCADE;

CREATE TABLE population.patients_typed (
  patient_id           UUID,
  birthdate            DATE,
  deathdate            DATE,
  ssn                  TEXT,
  drivers              TEXT,
  passport             TEXT,
  prefix               population.name_prefix,   -- 'Mr.','Mrs.','Ms.'
  first_name           TEXT,
  middle_name          TEXT,
  last_name            TEXT,
  suffix               TEXT,
  maiden               TEXT,
  marital_status       population.marital_status, -- 'M','S','D','W'
  race                 population.race_type,      -- 'white','black','asian','hawaiian'
  ethnicity            population.ethnicity_type, -- 'hispanic','nonhispanic'
  gender               population.gender_type,    -- 'M','F'
  birthplace           TEXT,
  address              TEXT,
  city                 TEXT,
  state                TEXT,
  county               TEXT,
  fips                 TEXT,
  zip                  TEXT,
  lat                  NUMERIC,
  lon                  NUMERIC,
  healthcare_expenses  NUMERIC,
  healthcare_coverage  NUMERIC,
  income               NUMERIC
);

INSERT INTO population.patients_typed (
  patient_id, birthdate, deathdate, ssn, drivers, passport,
  prefix, first_name, middle_name, last_name, suffix, maiden,
  marital_status, race, ethnicity, gender,
  birthplace, address, city, state, county, fips, zip,
  lat, lon, healthcare_expenses, healthcare_coverage,
  income
)
SELECT
  CASE WHEN "Id" ~ '^[0-9a-fA-F-]{36}$' THEN "Id"::uuid ELSE NULL END,
  CASE WHEN "BIRTHDATE" ~ '^\d{4}-\d{2}-\d{2}' THEN "BIRTHDATE"::date ELSE NULL END,
  CASE WHEN "DEATHDATE" ~ '^\d{4}-\d{2}-\d{2}' THEN "DEATHDATE"::date ELSE NULL END,
  "SSN",
  "DRIVERS",
  "PASSPORT",
  CASE WHEN "PREFIX" IN ('Mr.','Mrs.','Ms.') THEN "PREFIX"::population.name_prefix ELSE NULL END,
  "FIRST",
  "MIDDLE",
  "LAST",
  "SUFFIX",
  "MAIDEN",
  CASE WHEN "MARITAL" IN ('M','S','D','W') THEN "MARITAL"::population.marital_status ELSE NULL END,
  CASE WHEN "RACE" IN ('white','black','asian','hawaiian') THEN "RACE"::population.race_type ELSE NULL END,
  CASE WHEN "ETHNICITY" IN ('hispanic','nonhispanic') THEN "ETHNICITY"::population.ethnicity_type ELSE NULL END,
  CASE WHEN "GENDER" IN ('M','F') THEN "GENDER"::population.gender_type ELSE NULL END,
  "BIRTHPLACE",
  "ADDRESS",
  "CITY",
  "STATE",
  "COUNTY",
  "FIPS",
  "ZIP",
  CASE WHEN "LAT" ~ '^-?\d+(\.\d+)?$' THEN "LAT"::numeric ELSE NULL END,
  CASE WHEN "LON" ~ '^-?\d+(\.\d+)?$' THEN "LON"::numeric ELSE NULL END,
  CASE WHEN "HEALTHCARE_EXPENSES" ~ '^\d+(\.\d+)?$' THEN "HEALTHCARE_EXPENSES"::numeric ELSE NULL END,
  CASE WHEN "HEALTHCARE_COVERAGE" ~ '^\d+(\.\d+)?$' THEN "HEALTHCARE_COVERAGE"::numeric ELSE NULL END,
  CASE WHEN "INCOME" ~ '^\d+(\.\d+)?$' THEN "INCOME"::numeric ELSE NULL END
FROM population.patients;

-------------------------------------------------------------------------------
-- 15) PAYER_TRANSITIONS
-------------------------------------------------------------------------------
DROP TABLE IF EXISTS population.payer_transitions_typed CASCADE;

CREATE TABLE population.payer_transitions_typed (
  patient        UUID,
  member_id      UUID,
  start_date     TIMESTAMP,
  end_date       TIMESTAMP,
  payer          UUID,
  secondary_payer UUID,
  plan_ownership population.plan_ownership_type,  -- 'Self','Spouse','Guardian'
  owner_name     TEXT
);

INSERT INTO population.payer_transitions_typed (
  patient, member_id, start_date, end_date,
  payer, secondary_payer, plan_ownership, owner_name
)
SELECT
  CASE WHEN "PATIENT"  ~ '^[0-9a-fA-F-]{36}$' THEN "PATIENT"::uuid  ELSE NULL END,
  CASE WHEN "MEMBERID" ~ '^[0-9a-fA-F-]{36}$' THEN "MEMBERID"::uuid ELSE NULL END,
  CASE WHEN "START_DATE" ~ '^\d{4}-\d{2}-\d{2}' THEN "START_DATE"::timestamp ELSE NULL END,
  CASE WHEN "END_DATE"   ~ '^\d{4}-\d{2}-\d{2}' THEN "END_DATE"::timestamp   ELSE NULL END,
  CASE WHEN "PAYER"          ~ '^[0-9a-fA-F-]{36}$' THEN "PAYER"::uuid          ELSE NULL END,
  CASE WHEN "SECONDARY_PAYER"~ '^[0-9a-fA-F-]{36}$' THEN "SECONDARY_PAYER"::uuid ELSE NULL END,
  CASE WHEN "PLAN_OWNERSHIP" IN ('Self','Spouse','Guardian')
       THEN "PLAN_OWNERSHIP"::population.plan_ownership_type
       ELSE NULL
  END,
  "OWNER_NAME"
FROM population.payer_transitions;

-------------------------------------------------------------------------------
-- 16) PAYERS
-------------------------------------------------------------------------------
DROP TABLE IF EXISTS population.payers_typed CASCADE;

CREATE TABLE population.payers_typed (
  payer_id           UUID,
  name               TEXT,
  ownership          population.payer_ownership_type, -- 'GOVERNMENT','PRIVATE','NO_INSURANCE'
  address            TEXT,
  city               TEXT,
  state_headquartered TEXT,
  zip                TEXT,
  phone              TEXT,
  amount_covered     NUMERIC,
  amount_uncovered   NUMERIC,
  revenue            NUMERIC,
  covered_encounters   NUMERIC,
  uncovered_encounters NUMERIC,
  covered_medications  NUMERIC,
  uncovered_medications NUMERIC,
  covered_procedures   NUMERIC,
  uncovered_procedures NUMERIC,
  covered_immunizations   NUMERIC,
  uncovered_immunizations NUMERIC,
  unique_customers    INT,
  qols_avg            NUMERIC,
  member_months       INT
);

INSERT INTO population.payers_typed (
  payer_id, name, ownership, address, city, state_headquartered, zip, phone,
  amount_covered, amount_uncovered, revenue,
  covered_encounters, uncovered_encounters,
  covered_medications, uncovered_medications,
  covered_procedures, uncovered_procedures,
  covered_immunizations, uncovered_immunizations,
  unique_customers, qols_avg, member_months
)
SELECT
  CASE WHEN "Id"   ~ '^[0-9a-fA-F-]{36}$' THEN "Id"::uuid ELSE NULL END,
  "NAME",
  CASE WHEN "OWNERSHIP" IN ('GOVERNMENT','PRIVATE','NO_INSURANCE')
       THEN "OWNERSHIP"::population.payer_ownership_type
       ELSE NULL
  END,
  "ADDRESS",
  "CITY",
  "STATE_HEADQUARTERED",
  "ZIP",
  "PHONE",
  CASE WHEN "AMOUNT_COVERED"   ~ '^\d+(\.\d+)?$' THEN "AMOUNT_COVERED"::numeric   ELSE NULL END,
  CASE WHEN "AMOUNT_UNCOVERED" ~ '^\d+(\.\d+)?$' THEN "AMOUNT_UNCOVERED"::numeric ELSE NULL END,
  CASE WHEN "REVENUE" ~ '^\d+(\.\d+)?$' THEN "REVENUE"::numeric ELSE NULL END,
  CASE WHEN "COVERED_ENCOUNTERS"   ~ '^\d+(\.\d+)?$' THEN "COVERED_ENCOUNTERS"::numeric   ELSE NULL END,
  CASE WHEN "UNCOVERED_ENCOUNTERS" ~ '^\d+(\.\d+)?$' THEN "UNCOVERED_ENCOUNTERS"::numeric ELSE NULL END,
  CASE WHEN "COVERED_MEDICATIONS"  ~ '^\d+(\.\d+)?$' THEN "COVERED_MEDICATIONS"::numeric  ELSE NULL END,
  CASE WHEN "UNCOVERED_MEDICATIONS"~ '^\d+(\.\d+)?$' THEN "UNCOVERED_MEDICATIONS"::numeric ELSE NULL END,
  CASE WHEN "COVERED_PROCEDURES"   ~ '^\d+(\.\d+)?$' THEN "COVERED_PROCEDURES"::numeric   ELSE NULL END,
  CASE WHEN "UNCOVERED_PROCEDURES" ~ '^\d+(\.\d+)?$' THEN "UNCOVERED_PROCEDURES"::numeric ELSE NULL END,
  CASE WHEN "COVERED_IMMUNIZATIONS"   ~ '^\d+(\.\d+)?$' THEN "COVERED_IMMUNIZATIONS"::numeric   ELSE NULL END,
  CASE WHEN "UNCOVERED_IMMUNIZATIONS" ~ '^\d+(\.\d+)?$' THEN "UNCOVERED_IMMUNIZATIONS"::numeric ELSE NULL END,
  CASE WHEN "UNIQUE_CUSTOMERS" ~ '^\d+$' THEN "UNIQUE_CUSTOMERS"::int ELSE NULL END,
  CASE WHEN "QOLS_AVG"        ~ '^\d+(\.\d+)?$' THEN "QOLS_AVG"::numeric        ELSE NULL END,
  CASE WHEN "MEMBER_MONTHS"   ~ '^\d+$' THEN "MEMBER_MONTHS"::int   ELSE NULL END
FROM population.payers;

-------------------------------------------------------------------------------
-- 17) PROCEDURES
-------------------------------------------------------------------------------
DROP TABLE IF EXISTS population.procedures_typed CASCADE;

CREATE TABLE population.procedures_typed (
  start_time     TIMESTAMP,
  stop_time      TIMESTAMP,
  patient        UUID,
  encounter      UUID,
  coding_sys     population.procedure_coding_system, -- 'SNOMED-CT','CPT','HCPCS','ICD-10-PCS'
  code           TEXT,
  description    TEXT,
  base_cost      NUMERIC,
  reasoncode     TEXT,
  reasondesc     TEXT
);

INSERT INTO population.procedures_typed (
  start_time, stop_time, patient, encounter,
  coding_sys, code, description, base_cost,
  reasoncode, reasondesc
)
SELECT
  CASE WHEN "START" ~ '^\d{4}-\d{2}-\d{2}' THEN "START"::timestamp ELSE NULL END,
  CASE WHEN "STOP"  ~ '^\d{4}-\d{2}-\d{2}' THEN "STOP"::timestamp  ELSE NULL END,
  CASE WHEN "PATIENT"   ~ '^[0-9a-fA-F-]{36}$' THEN "PATIENT"::uuid   ELSE NULL END,
  CASE WHEN "ENCOUNTER" ~ '^[0-9a-fA-F-]{36}$' THEN "ENCOUNTER"::uuid ELSE NULL END,
  CASE WHEN "SYSTEM" IN ('SNOMED-CT','CPT','HCPCS','ICD-10-PCS')
       THEN "SYSTEM"::population.procedure_coding_system
       ELSE NULL
  END,
  "CODE",
  "DESCRIPTION",
  CASE WHEN "BASE_COST" ~ '^\d+(\.\d+)?$' THEN "BASE_COST"::numeric ELSE NULL END,
  "REASONCODE",
  "REASONDESCRIPTION"
FROM population."procedures";

-------------------------------------------------------------------------------
-- 18) PROVIDERS
-------------------------------------------------------------------------------
DROP TABLE IF EXISTS population.providers_typed CASCADE;

CREATE TABLE population.providers_typed (
  provider_id    UUID,
  organization   UUID,
  name           TEXT,
  gender         population.gender_type,  -- 'M','F'
  speciality     TEXT,
  address        TEXT,
  city           TEXT,
  state          TEXT,
  zip            TEXT,
  lat            NUMERIC,
  lon            NUMERIC,
  encounters     NUMERIC,
  procedures     NUMERIC
);

INSERT INTO population.providers_typed (
  provider_id, organization, name, gender, speciality,
  address, city, state, zip, lat, lon, encounters, procedures
)
SELECT
  CASE WHEN "Id" ~ '^[0-9a-fA-F-]{36}$' THEN "Id"::uuid ELSE NULL END,
  CASE WHEN "ORGANIZATION" ~ '^[0-9a-fA-F-]{36}$' THEN "ORGANIZATION"::uuid ELSE NULL END,
  "NAME",
  CASE WHEN "GENDER" IN ('M','F') THEN "GENDER"::population.gender_type ELSE NULL END,
  "SPECIALITY",
  "ADDRESS",
  "CITY",
  "STATE",
  "ZIP",
  CASE WHEN "LAT" ~ '^-?\d+(\.\d+)?$' THEN "LAT"::numeric ELSE NULL END,
  CASE WHEN "LON" ~ '^-?\d+(\.\d+)?$' THEN "LON"::numeric ELSE NULL END,
  CASE WHEN "ENCOUNTERS" ~ '^\d+(\.\d+)?$' THEN "ENCOUNTERS"::numeric ELSE NULL END,
  CASE WHEN "PROCEDURES" ~ '^\d+(\.\d+)?$' THEN "PROCEDURES"::numeric ELSE NULL END
FROM population.providers;

-------------------------------------------------------------------------------
-- 19) SUPPLIES
-------------------------------------------------------------------------------
DROP TABLE IF EXISTS population.supplies_typed CASCADE;

CREATE TABLE population.supplies_typed (
  date_time   TIMESTAMP,
  patient     UUID,
  encounter   UUID,
  code        TEXT,
  description TEXT,
  quantity    NUMERIC
);

INSERT INTO population.supplies_typed (
  date_time, patient, encounter,
  code, description, quantity
)
SELECT
  CASE WHEN "DATE" ~ '^\d{4}-\d{2}-\d{2}' THEN "DATE"::timestamp ELSE NULL END,
  CASE WHEN "PATIENT"   ~ '^[0-9a-fA-F-]{36}$' THEN "PATIENT"::uuid   ELSE NULL END,
  CASE WHEN "ENCOUNTER" ~ '^[0-9a-fA-F-]{36}$' THEN "ENCOUNTER"::uuid ELSE NULL END,
  "CODE",
  "DESCRIPTION",
  CASE WHEN "QUANTITY" ~ '^\d+(\.\d+)?$' THEN "QUANTITY"::numeric ELSE NULL END
FROM population.supplies;


COMMIT;

-- Truncate all non-typed tables in the population schema
TRUNCATE TABLE population.allergies CASCADE;
TRUNCATE TABLE population.careplans CASCADE;
TRUNCATE TABLE population.claims CASCADE;
TRUNCATE TABLE population.claims_transactions CASCADE;
TRUNCATE TABLE population.conditions CASCADE;
TRUNCATE TABLE population.devices CASCADE;
TRUNCATE TABLE population.encounters CASCADE;
TRUNCATE TABLE population.imaging_studies CASCADE;
TRUNCATE TABLE population.immunizations CASCADE;
TRUNCATE TABLE population.medications CASCADE;
TRUNCATE TABLE population.observations CASCADE;
TRUNCATE TABLE population.organizations CASCADE;
TRUNCATE TABLE population.patient_expenses CASCADE;
TRUNCATE TABLE population.patients CASCADE;
TRUNCATE TABLE population.payer_transitions CASCADE;
TRUNCATE TABLE population.payers CASCADE;
TRUNCATE TABLE population.procedures CASCADE;
TRUNCATE TABLE population.providers CASCADE;
TRUNCATE TABLE population.supplies CASCADE;
