-- Script to modify ETL process to use batching and more frequent commits
-- Created: 2025-03-17

-- This script provides examples of how to modify ETL SQL to use batching
-- These patterns should be applied to the actual ETL scripts in sql/etl/

-- Example 1: Batched INSERT with explicit transaction control
-- Replace large INSERT statements with batched versions like this:

/*
-- Original version (single large transaction)
INSERT INTO omop.condition_occurrence (...)
SELECT ...
FROM population.conditions_typed
WHERE ...;

-- Batched version with explicit commits
DO $$
DECLARE
    batch_size INT := 10000;
    max_id INT;
    current_id INT := 0;
    affected_rows INT;
BEGIN
    -- Get the maximum ID to process
    SELECT MAX(id) INTO max_id FROM population.conditions_typed;
    
    RAISE NOTICE 'Processing % records in batches of %', max_id, batch_size;
    
    -- Process in batches
    WHILE current_id < max_id LOOP
        -- Begin a transaction for this batch
        BEGIN
            -- Insert the current batch
            INSERT INTO omop.condition_occurrence (...)
            SELECT ...
            FROM population.conditions_typed
            WHERE id > current_id AND id <= current_id + batch_size
            AND ...;
            
            GET DIAGNOSTICS affected_rows = ROW_COUNT;
            RAISE NOTICE 'Processed batch starting at ID %: % rows', current_id, affected_rows;
            
            -- Move to the next batch
            current_id := current_id + batch_size;
            
            -- Commit this batch
            COMMIT;
        EXCEPTION WHEN OTHERS THEN
            -- Roll back on error
            ROLLBACK;
            RAISE NOTICE 'Error processing batch starting at ID %: %', current_id, SQLERRM;
            -- Skip to the next batch on error
            current_id := current_id + batch_size;
        END;
    END LOOP;
    
    RAISE NOTICE 'Processing completed';
END $$;
*/

-- Example 2: Batched UPDATE with explicit transaction control

/*
-- Original version (single large transaction)
UPDATE omop.visit_occurrence
SET ...
FROM population.encounters_typed
WHERE ...;

-- Batched version with explicit commits
DO $$
DECLARE
    batch_size INT := 10000;
    max_id INT;
    current_id INT := 0;
    affected_rows INT;
BEGIN
    -- Get the maximum ID to process
    SELECT MAX(visit_occurrence_id) INTO max_id FROM omop.visit_occurrence;
    
    RAISE NOTICE 'Processing % records in batches of %', max_id, batch_size;
    
    -- Process in batches
    WHILE current_id < max_id LOOP
        -- Begin a transaction for this batch
        BEGIN
            -- Update the current batch
            UPDATE omop.visit_occurrence
            SET ...
            WHERE visit_occurrence_id > current_id AND visit_occurrence_id <= current_id + batch_size
            AND ...;
            
            GET DIAGNOSTICS affected_rows = ROW_COUNT;
            RAISE NOTICE 'Processed batch starting at ID %: % rows', current_id, affected_rows;
            
            -- Move to the next batch
            current_id := current_id + batch_size;
            
            -- Commit this batch
            COMMIT;
        EXCEPTION WHEN OTHERS THEN
            -- Roll back on error
            ROLLBACK;
            RAISE NOTICE 'Error processing batch starting at ID %: %', current_id, SQLERRM;
            -- Skip to the next batch on error
            current_id := current_id + batch_size;
        END;
    END LOOP;
    
    RAISE NOTICE 'Processing completed';
END $$;
*/

-- Example 3: Batched DELETE with explicit transaction control

/*
-- Original version (single large transaction)
DELETE FROM staging.temp_table
WHERE ...;

-- Batched version with explicit commits
DO $$
DECLARE
    batch_size INT := 10000;
    affected_rows INT;
    total_rows INT := 1; -- Start with a non-zero value
BEGIN
    RAISE NOTICE 'Deleting records in batches of %', batch_size;
    
    -- Process in batches until no more rows are affected
    WHILE total_rows > 0 LOOP
        -- Begin a transaction for this batch
        BEGIN
            -- Delete a batch of records
            DELETE FROM staging.temp_table
            WHERE ctid IN (
                SELECT ctid
                FROM staging.temp_table
                WHERE ...
                LIMIT batch_size
            );
            
            GET DIAGNOSTICS affected_rows = ROW_COUNT;
            total_rows := affected_rows;
            
            RAISE NOTICE 'Deleted batch: % rows', affected_rows;
            
            -- Commit this batch
            COMMIT;
        EXCEPTION WHEN OTHERS THEN
            -- Roll back on error
            ROLLBACK;
            RAISE NOTICE 'Error deleting batch: %', SQLERRM;
            -- Exit the loop on error
            total_rows := 0;
        END;
    END LOOP;
    
    RAISE NOTICE 'Deletion completed';
END $$;
*/

-- Example 4: Creating indexes with lower maintenance_work_mem and in non-blocking mode

/*
-- Original version
CREATE INDEX idx_condition_occurrence_person_id ON omop.condition_occurrence(person_id);

-- Improved version
-- First, set maintenance_work_mem for this session only
SET maintenance_work_mem = '128MB';

-- Create index concurrently to avoid blocking
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_condition_occurrence_person_id ON omop.condition_occurrence(person_id);

-- Reset maintenance_work_mem to default
RESET maintenance_work_mem;
*/

-- Example 5: Staggered execution of heavy operations

/*
-- Instead of running all transformations in parallel, sequence them:

-- Step 1: Transform visit data
\echo 'Step 1: Transforming visit data...'
\i sql/etl/transform_visit_occurrence_v2.sql

-- Wait for system to stabilize
SELECT pg_sleep(10);

-- Step 2: Transform condition data
\echo 'Step 2: Transforming condition data...'
\i sql/etl/transform_condition_occurrence.sql

-- Wait for system to stabilize
SELECT pg_sleep(10);

-- And so on...
*/

-- Note: These are example patterns. The actual implementation would require
-- modifying the specific ETL scripts in the sql/etl/ directory.
