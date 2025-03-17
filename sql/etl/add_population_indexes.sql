-- Add indexes to population schema tables to speed up ETL operations

-- patients_typed table
CREATE INDEX IF NOT EXISTS idx_patients_typed_patient_id ON population.patients_typed (patient_id);

-- encounters_typed table
CREATE INDEX IF NOT EXISTS idx_encounters_typed_encounter_id ON population.encounters_typed (encounter_id);
CREATE INDEX IF NOT EXISTS idx_encounters_typed_patient ON population.encounters_typed (patient);

-- conditions_typed table
CREATE INDEX IF NOT EXISTS idx_conditions_typed_patient ON population.conditions_typed (patient);
CREATE INDEX IF NOT EXISTS idx_conditions_typed_encounter ON population.conditions_typed (encounter);

-- medications_typed table
CREATE INDEX IF NOT EXISTS idx_medications_typed_patient ON population.medications_typed (patient);
CREATE INDEX IF NOT EXISTS idx_medications_typed_encounter ON population.medications_typed (encounter);

-- procedures_typed table
CREATE INDEX IF NOT EXISTS idx_procedures_typed_patient ON population.procedures_typed (patient);
CREATE INDEX IF NOT EXISTS idx_procedures_typed_encounter ON population.procedures_typed (encounter);

-- observations_typed table
CREATE INDEX IF NOT EXISTS idx_observations_typed_patient ON population.observations_typed (patient);
CREATE INDEX IF NOT EXISTS idx_observations_typed_encounter ON population.observations_typed (encounter);

-- Log the results
SELECT 'Population schema indexes created successfully' AS message;
