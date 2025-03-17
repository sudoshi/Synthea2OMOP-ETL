-- Add indexes to staging tables to speed up ETL operations

-- patients_raw table
CREATE INDEX IF NOT EXISTS idx_patients_raw_id ON staging.patients_raw (id);

-- encounters_raw table
CREATE INDEX IF NOT EXISTS idx_encounters_raw_id ON staging.encounters_raw (id);
CREATE INDEX IF NOT EXISTS idx_encounters_raw_patient_id ON staging.encounters_raw (patient_id);

-- conditions_raw table
CREATE INDEX IF NOT EXISTS idx_conditions_raw_patient_id ON staging.conditions_raw (patient_id);
CREATE INDEX IF NOT EXISTS idx_conditions_raw_encounter_id ON staging.conditions_raw (encounter_id);

-- medications_raw table
CREATE INDEX IF NOT EXISTS idx_medications_raw_patient_id ON staging.medications_raw (patient_id);
CREATE INDEX IF NOT EXISTS idx_medications_raw_encounter_id ON staging.medications_raw (encounter_id);

-- procedures_raw table
CREATE INDEX IF NOT EXISTS idx_procedures_raw_patient_id ON staging.procedures_raw (patient_id);
CREATE INDEX IF NOT EXISTS idx_procedures_raw_encounter_id ON staging.procedures_raw (encounter_id);

-- observations_raw table
CREATE INDEX IF NOT EXISTS idx_observations_raw_patient_id ON staging.observations_raw (patient_id);
CREATE INDEX IF NOT EXISTS idx_observations_raw_encounter_id ON staging.observations_raw (encounter_id);

-- person_map table
CREATE INDEX IF NOT EXISTS idx_person_map_person_id ON staging.person_map (person_id);

-- visit_map table
CREATE INDEX IF NOT EXISTS idx_visit_map_person_id ON staging.visit_map (person_id);
CREATE INDEX IF NOT EXISTS idx_visit_map_visit_occurrence_id ON staging.visit_map (visit_occurrence_id);

-- Log the results
SELECT 'Indexes created successfully' AS message;
