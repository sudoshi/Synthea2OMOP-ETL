-- =====================================================
-- MIGRATE NUMERIC OBSERVATIONS TO MEASUREMENTS TABLE
-- =====================================================
-- Features:
--  - Batch processing for large datasets
--  - Transaction safety
--  - Detailed error logging
--  - Verification before deletion
--  - Configurable parameters
-- =====================================================

-- Configuration Parameters
\set deletion_enabled true   -- Set to 'true' to enable deletion after verification, 'false' to keep original records
\set verification_enabled true  -- Verify data before deletion
\set batch_size 100000      -- Number of records per batch
\set debug_output true      -- Show detailed progress information

\echo 'STARTING NUMERIC OBSERVATIONS MIGRATION'
\echo 'Settings: deletion_enabled=' :deletion_enabled ', verification_enabled=' :verification_enabled ', batch_size=' :batch_size

-- Enable timing for performance analysis
\timing on

-- =====================================================
-- SETUP: Create required tables and infrastructure
-- =====================================================

-- Create tables for tracking migration progress and errors
CREATE TABLE IF NOT EXISTS staging.observation_migration_errors (
    id text,
    patient_id text,
    value_as_string text,
    error_message text,
    attempted_at timestamp DEFAULT CURRENT_TIMESTAMP,
    batch_id integer
);

CREATE TABLE IF NOT EXISTS staging.observation_migration_stats (
    migration_id serial PRIMARY KEY,
    started_at timestamp DEFAULT CURRENT_TIMESTAMP,
    completed_at timestamp,
    records_processed bigint DEFAULT 0,
    records_migrated bigint DEFAULT 0,
    records_failed bigint DEFAULT 0,
    records_deleted bigint DEFAULT 0,
    batches_total integer DEFAULT 0,
    batches_completed integer DEFAULT 0,
    deletion_enabled boolean,
    status text DEFAULT 'in_progress'
);

CREATE TABLE IF NOT EXISTS staging.observation_migration_batches (
    batch_id serial PRIMARY KEY,
    min_id text,
    max_id text,
    record_count integer,
    processed boolean DEFAULT false,
    verified boolean DEFAULT false,
    deleted boolean DEFAULT false,
    started_at timestamp,
    completed_at timestamp,
    verification_at timestamp,
    deletion_at timestamp,
    processed_count integer DEFAULT 0,
    verified_count integer DEFAULT 0,
    deleted_count integer DEFAULT 0,
    error_count integer DEFAULT 0
);

-- =====================================================
-- STEP 1: Count records and prepare batches
-- =====================================================

DO $$
DECLARE
    v_migration_id INTEGER;
    v_total_count BIGINT;
    v_batch_count INTEGER;
    v_deletion_enabled BOOLEAN := :deletion_enabled;
    v_verification_enabled BOOLEAN := :verification_enabled;
    v_batch_size INTEGER := :batch_size;
    v_debug_output BOOLEAN := :debug_output;
BEGIN
    -- Start a new migration
    INSERT INTO staging.observation_migration_stats (
        started_at, 
        status, 
        deletion_enabled
    )
    VALUES (
        CURRENT_TIMESTAMP, 
        'preparing',
        v_deletion_enabled
    )
    RETURNING migration_id INTO v_migration_id;
    
    IF v_debug_output THEN
        RAISE NOTICE 'Migration #% started with settings: deletion_enabled=%, verification_enabled=%, batch_size=%', 
                     v_migration_id, v_deletion_enabled, v_verification_enabled, v_batch_size;
    END IF;
    
    -- Count total eligible records
    SELECT COUNT(*) INTO v_total_count
    FROM staging.observations_raw
    WHERE observation_type = 'numeric';
    
    RAISE NOTICE 'Found % numeric observation records to process', v_total_count;
    
    -- Prepare batches
    INSERT INTO staging.observation_migration_batches (
        min_id, max_id, record_count
    )
    SELECT
        min(id) AS min_id,
        max(id) AS max_id,
        count(*) AS record_count
    FROM (
        SELECT 
            id,
            ntile(CEILING(COUNT(*) OVER () / v_batch_size::float)) OVER (ORDER BY id) AS batch_num
        FROM 
            staging.observations_raw
        WHERE 
            observation_type = 'numeric'
    ) AS batched_data
    GROUP BY batch_num
    ORDER BY batch_num;
    
    -- Get batch count
    GET DIAGNOSTICS v_batch_count = ROW_COUNT;
    
    -- Update stats with batch information
    UPDATE staging.observation_migration_stats
    SET 
        records_processed = v_total_count,
        batches_total = v_batch_count,
        status = 'batched'
    WHERE 
        migration_id = v_migration_id;
    
    RAISE NOTICE 'Prepared % batches for processing (batch size: %)', v_batch_count, v_batch_size;
END $$;

-- =====================================================
-- STEP 2: Process each batch with verification and optional deletion
-- =====================================================

DO $$
DECLARE
    v_batch RECORD;
    v_migration_id INTEGER;
    v_processed INTEGER;
    v_migrated INTEGER;
    v_failed INTEGER;
    v_verified INTEGER;
    v_deleted INTEGER;
    v_start_time TIMESTAMP;
    v_batch_duration INTERVAL;
    v_deletion_enabled BOOLEAN;
    v_verification_enabled BOOLEAN := :verification_enabled;
    v_debug_output BOOLEAN := :debug_output;
    v_rate NUMERIC;
BEGIN
    -- Get the current migration ID and settings
    SELECT 
        migration_id, 
        deletion_enabled INTO v_migration_id, v_deletion_enabled
    FROM staging.observation_migration_stats
    WHERE status = 'batched'
    ORDER BY migration_id DESC
    LIMIT 1;
    
    -- Update status
    UPDATE staging.observation_migration_stats
    SET status = 'processing'
    WHERE migration_id = v_migration_id;
    
    RAISE NOTICE '==========================================';
    RAISE NOTICE 'Starting batch processing on migration #%', v_migration_id;
    RAISE NOTICE '==========================================';
    
    -- Process each batch
    FOR v_batch IN 
        SELECT * FROM staging.observation_migration_batches
        WHERE NOT processed
        ORDER BY batch_id
    LOOP
        -- Mark batch as started
        v_start_time := CURRENT_TIMESTAMP;
        UPDATE staging.observation_migration_batches
        SET started_at = v_start_time
        WHERE batch_id = v_batch.batch_id;
        
        RAISE NOTICE 'Processing batch % of % (records: %, IDs % to %)', 
            v_batch.batch_id, 
            (SELECT batches_total FROM staging.observation_migration_stats WHERE migration_id = v_migration_id),
            v_batch.record_count, 
            v_batch.min_id, 
            v_batch.max_id;
        
        -- Start a transaction for this batch migration
        BEGIN
            -- Migrate valid numeric records
            WITH source_data AS (
                SELECT 
                    id, 
                    patient_id, 
                    encounter_id, 
                    code, 
                    description,
                    value_as_string,
                    "timestamp"
                FROM 
                    staging.observations_raw
                WHERE 
                    observation_type = 'numeric'
                    AND value_as_string ~ '^[-]?[0-9]+([.][0-9]+)?([eE][-+]?[0-9]+)?$'
                    AND id >= v_batch.min_id
                    AND id <= v_batch.max_id
            )
            INSERT INTO staging.measurements_raw (
                id, patient_id, encounter_id, category, code, 
                description, value, units, "timestamp"
            )
            SELECT 
                id, 
                patient_id, 
                encounter_id, 
                'numeric' AS category,  
                code, 
                description,
                value_as_string::numeric AS value,
                '' AS units,
                "timestamp"
            FROM source_data;
            
            -- Track successfully migrated records
            GET DIAGNOSTICS v_migrated = ROW_COUNT;
            
            -- Log records with invalid numeric values
            INSERT INTO staging.observation_migration_errors (
                id, patient_id, value_as_string, error_message, batch_id
            )
            SELECT 
                id, 
                patient_id, 
                value_as_string,
                'Cannot convert value to numeric: ' || value_as_string,
                v_batch.batch_id
            FROM 
                staging.observations_raw
            WHERE 
                observation_type = 'numeric'
                AND value_as_string !~ '^[-]?[0-9]+([.][0-9]+)?([eE][-+]?[0-9]+)?$'
                AND id >= v_batch.min_id
                AND id <= v_batch.max_id;
            
            -- Track error records
            GET DIAGNOSTICS v_failed = ROW_COUNT;
            
            -- Calculate processed count
            v_processed := v_migrated + v_failed;
            
            -- Mark batch as migrated
            UPDATE staging.observation_migration_batches
            SET 
                processed = TRUE,
                completed_at = CURRENT_TIMESTAMP,
                processed_count = v_migrated,
                error_count = v_failed
            WHERE 
                batch_id = v_batch.batch_id;
            
            -- Update overall statistics
            UPDATE staging.observation_migration_stats
            SET 
                records_migrated = records_migrated + v_migrated,
                records_failed = records_failed + v_failed,
                batches_completed = batches_completed + 1
            WHERE 
                migration_id = v_migration_id;
            
            -- Commit the migration
            COMMIT;
            
            -- Calculate migration rate (records per second)
            v_batch_duration := CURRENT_TIMESTAMP - v_start_time;
            IF EXTRACT(EPOCH FROM v_batch_duration) > 0 THEN
                v_rate := v_processed / EXTRACT(EPOCH FROM v_batch_duration);
            ELSE
                v_rate := 0;
            END IF;
            
            RAISE NOTICE 'Migrated % records (% valid, % errors) in %s (%.2f records/sec)', 
                v_processed, v_migrated, v_failed, 
                EXTRACT(EPOCH FROM v_batch_duration)::integer,
                v_rate;
            
            -- VERIFICATION STEP (separate transaction)
            IF v_verification_enabled THEN
                BEGIN
                    IF v_debug_output THEN
                        RAISE NOTICE 'Verifying batch %...', v_batch.batch_id;
                    END IF;
                    
                    -- Count number of records successfully verified
                    SELECT COUNT(*) INTO v_verified
                    FROM staging.measurements_raw m
                    JOIN staging.observations_raw o ON 
                        m.id = o.id AND
                        m.patient_id = o.patient_id AND
                        m.code = o.code AND
                        m.value::text = o.value_as_string
                    WHERE 
                        o.observation_type = 'numeric' AND
                        o.id >= v_batch.min_id AND
                        o.id <= v_batch.max_id;
                    
                    -- Mark batch as verified if all expected records are found
                    UPDATE staging.observation_migration_batches
                    SET 
                        verified = (v_verified = v_migrated),
                        verification_at = CURRENT_TIMESTAMP,
                        verified_count = v_verified
                    WHERE 
                        batch_id = v_batch.batch_id;
                    
                    IF v_debug_output THEN
                        RAISE NOTICE 'Batch % verification: % of % records verified', 
                            v_batch.batch_id, v_verified, v_migrated;
                    END IF;
                    
                    -- DELETION STEP (only if verification passed and deletion is enabled)
                    IF v_deletion_enabled AND v_verified = v_migrated AND v_migrated > 0 THEN
                        RAISE NOTICE 'Deleting % verified records from source table...', v_migrated;
                        
                        DELETE FROM staging.observations_raw
                        WHERE 
                            observation_type = 'numeric' AND
                            value_as_string ~ '^[-]?[0-9]+([.][0-9]+)?([eE][-+]?[0-9]+)?$' AND
                            id >= v_batch.min_id AND
                            id <= v_batch.max_id;
                        
                        -- Track deleted records
                        GET DIAGNOSTICS v_deleted = ROW_COUNT;
                        
                        -- Update batch deletion stats
                        UPDATE staging.observation_migration_batches
                        SET 
                            deleted = TRUE,
                            deletion_at = CURRENT_TIMESTAMP,
                            deleted_count = v_deleted
                        WHERE 
                            batch_id = v_batch.batch_id;
                        
                        -- Update overall deletion stats
                        UPDATE staging.observation_migration_stats
                        SET records_deleted = records_deleted + v_deleted
                        WHERE migration_id = v_migration_id;
                        
                        RAISE NOTICE 'Deleted % records from observations_raw table', v_deleted;
                    ELSIF v_deletion_enabled AND v_verified < v_migrated THEN
                        RAISE WARNING 'Verification failed for batch %. Deletion skipped.', v_batch.batch_id;
                    END IF;
                    
                    -- Commit verification and deletion
                    COMMIT;
                EXCEPTION WHEN OTHERS THEN
                    -- Log verification/deletion error
                    RAISE WARNING 'Error during verification/deletion of batch %: %', v_batch.batch_id, SQLERRM;
                    ROLLBACK;
                END;
            END IF;
            
            -- Progress update
            RAISE NOTICE 'Progress: % of % batches completed (%.1f%%)', 
                (SELECT batches_completed FROM staging.observation_migration_stats WHERE migration_id = v_migration_id),
                (SELECT batches_total FROM staging.observation_migration_stats WHERE migration_id = v_migration_id),
                (100.0 * (SELECT batches_completed FROM staging.observation_migration_stats WHERE migration_id = v_migration_id) / 
                 NULLIF((SELECT batches_total FROM staging.observation_migration_stats WHERE migration_id = v_migration_id), 0));
                
        EXCEPTION WHEN OTHERS THEN
            -- Log the migration error
            RAISE WARNING 'Error processing batch %: %', v_batch.batch_id, SQLERRM;
            
            -- Rollback the migration
            ROLLBACK;
            
            -- Mark batch as errored but not processed
            UPDATE staging.observation_migration_batches
            SET 
                processed = FALSE,
                completed_at = CURRENT_TIMESTAMP,
                error_count = v_batch.record_count
            WHERE 
                batch_id = v_batch.batch_id;
        END;
    END LOOP;
    
    -- Mark migration as completed
    UPDATE staging.observation_migration_stats
    SET 
        completed_at = CURRENT_TIMESTAMP,
        status = CASE 
            WHEN batches_completed = batches_total THEN 'completed'
            ELSE 'partially_completed'
        END
    WHERE 
        migration_id = v_migration_id;
    
    RAISE NOTICE '==========================================';
    RAISE NOTICE 'Migration #% completed', v_migration_id;
    RAISE NOTICE '==========================================';
END $$;

-- =====================================================
-- STEP 3: Show comprehensive migration results
-- =====================================================

\echo '=== MIGRATION SUMMARY ==='
SELECT 
    migration_id,
    deletion_enabled,
    started_at,
    completed_at,
    completed_at - started_at AS duration,
    records_processed,
    records_migrated,
    records_failed,
    records_deleted,
    ROUND((records_migrated::numeric / NULLIF(records_processed, 0)) * 100, 2) AS pct_migrated,
    ROUND((records_deleted::numeric / NULLIF(records_migrated, 0)) * 100, 2) AS pct_deleted,
    batches_total,
    batches_completed,
    status
FROM 
    staging.observation_migration_stats
ORDER BY 
    migration_id DESC
LIMIT 1;

\echo '=== BATCH PROCESSING DETAILS ==='
SELECT 
    batch_id,
    record_count,
    processed_count,
    verified_count,
    deleted_count,
    error_count,
    processed,
    verified,
    deleted,
    started_at,
    completed_at,
    verification_at,
    deletion_at,
    completed_at - started_at AS processing_duration,
    CASE WHEN verification_at IS NOT NULL THEN
        verification_at - completed_at
    END AS verification_duration,
    CASE WHEN deletion_at IS NOT NULL AND verification_at IS NOT NULL THEN
        deletion_at - verification_at
    END AS deletion_duration
FROM 
    staging.observation_migration_batches
ORDER BY 
    batch_id;

\echo '=== ERROR SAMPLES ==='
SELECT * FROM staging.observation_migration_errors
ORDER BY attempted_at DESC
LIMIT 10;

\echo '=== VERIFICATION CHECK ==='
\echo 'Records that failed to migrate (should be zero):'
SELECT COUNT(*) AS missing_records
FROM staging.observations_raw o
LEFT JOIN staging.measurements_raw m ON o.id = m.id
WHERE 
    o.observation_type = 'numeric'
    AND o.value_as_string ~ '^[-]?[0-9]+([.][0-9]+)?([eE][-+]?[0-9]+)?$'
    AND m.id IS NULL;

\echo '=== DATA SAMPLES ==='
\echo 'Sample records from measurements_raw:'
SELECT * FROM staging.measurements_raw
ORDER BY created_at DESC
LIMIT 5;

\echo 'MIGRATION PROCESS COMPLETE'
