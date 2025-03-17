-- Script to optimize PostgreSQL configuration for better performance
-- Created: 2025-03-17

-- Memory-related parameters
-- Reduce work_mem to a more reasonable value (from 64MB to 16MB)
ALTER SYSTEM SET work_mem = '16MB';

-- Adjust maintenance_work_mem for index creation (from 256MB to 128MB)
ALTER SYSTEM SET maintenance_work_mem = '128MB';

-- Reduce shared_buffers (from 1GB to 512MB)
ALTER SYSTEM SET shared_buffers = '512MB';

-- Set effective_cache_size appropriately (from 8GB to 4GB)
ALTER SYSTEM SET effective_cache_size = '4GB';

-- Parallelism settings
-- Reduce max_parallel_workers (from 16 to 4)
ALTER SYSTEM SET max_parallel_workers = '4';

-- Adjust max_parallel_workers_per_gather (from 8 to 2)
ALTER SYSTEM SET max_parallel_workers_per_gather = '2';

-- Set max_worker_processes appropriately (from 16 to 8)
ALTER SYSTEM SET max_worker_processes = '8';

-- Checkpoint and WAL settings
-- Increase checkpoint_timeout (from 15min to 10min)
ALTER SYSTEM SET checkpoint_timeout = '10min';

-- Adjust max_wal_size (from 32GB to 16GB)
ALTER SYSTEM SET max_wal_size = '16GB';

-- Set synchronous_commit to off during ETL for better performance
ALTER SYSTEM SET synchronous_commit = 'off';

-- Reload the configuration
SELECT pg_reload_conf();

-- Verify the changes
SELECT name, setting, unit 
FROM pg_settings 
WHERE name IN (
    'work_mem', 
    'maintenance_work_mem', 
    'shared_buffers', 
    'effective_cache_size', 
    'max_parallel_workers', 
    'max_parallel_workers_per_gather', 
    'max_worker_processes', 
    'checkpoint_timeout', 
    'max_wal_size', 
    'synchronous_commit'
);

-- Note: Some settings may require a server restart to take effect
SELECT 'PostgreSQL configuration optimized successfully. Some settings may require a server restart.' AS message;
