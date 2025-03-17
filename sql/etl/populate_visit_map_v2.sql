-- Populate the visit_map table from the encounters_raw table

-- Truncate the visit_map table
TRUNCATE TABLE staging.visit_map;
ALTER SEQUENCE staging.visit_seq RESTART WITH 1;

-- Create a temporary table with distinct encounter_id values
DROP TABLE IF EXISTS staging.temp_encounters;
CREATE TABLE staging.temp_encounters AS
SELECT DISTINCT ON (id) 
    id,
    patient_id
FROM 
    staging.encounters_raw;

-- Insert into visit_map table using the temporary table
INSERT INTO staging.visit_map (source_visit_id, visit_occurrence_id, person_id, created_at, updated_at)
SELECT 
    te.id,
    NEXTVAL('staging.visit_seq'),
    pm.person_id,
    CURRENT_TIMESTAMP,
    CURRENT_TIMESTAMP
FROM 
    staging.temp_encounters te
JOIN 
    staging.person_map pm ON pm.source_patient_id = te.patient_id;

-- Drop the temporary table
DROP TABLE staging.temp_encounters;

-- Log the results
SELECT 'visit_map' AS table_name, COUNT(*) AS record_count FROM staging.visit_map;
