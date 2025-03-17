-- Populate the visit_map table from the encounters_raw table

-- Truncate the visit_map table
TRUNCATE TABLE staging.visit_map;
ALTER SEQUENCE staging.visit_seq RESTART WITH 1;

-- Insert into visit_map table, handling duplicate encounter_id values
INSERT INTO staging.visit_map (source_visit_id, visit_occurrence_id, person_id, created_at, updated_at)
SELECT 
    e.id,
    NEXTVAL('staging.visit_seq'),
    pm.person_id,
    CURRENT_TIMESTAMP,
    CURRENT_TIMESTAMP
FROM 
    staging.encounters_raw e
JOIN 
    staging.person_map pm ON pm.source_patient_id = e.patient_id
ON CONFLICT (source_visit_id) DO NOTHING;

-- Log the results
SELECT 'visit_map' AS table_name, COUNT(*) AS record_count FROM staging.visit_map;
