-- Optimize PostgreSQL configuration for ETL performance

-- Memory-related parameters
-- Increase work_mem for complex query operations (from 4MB to 64MB)
ALTER SYSTEM SET work_mem = '64MB';

-- Adjust maintenance_work_mem for index creation (from 64MB to 256MB)
ALTER SYSTEM SET maintenance_work_mem = '256MB';

-- Optimize shared_buffers (from 128MB to 1GB)
ALTER SYSTEM SET shared_buffers = '1GB';

-- Set effective_cache_size appropriately (from 4GB to 8GB)
ALTER SYSTEM SET effective_cache_size = '8GB';

-- Parallelism settings
-- Increase max_parallel_workers (from 8 to 16)
ALTER SYSTEM SET max_parallel_workers = '16';

-- Adjust max_parallel_workers_per_gather (from 2 to 8)
ALTER SYSTEM SET max_parallel_workers_per_gather = '8';

-- Set max_worker_processes appropriately (from 8 to 16)
ALTER SYSTEM SET max_worker_processes = '16';

-- Checkpoint and WAL settings
-- Increase checkpoint_timeout (from 5min to 15min)
ALTER SYSTEM SET checkpoint_timeout = '15min';

-- Adjust max_wal_size (from 16GB to 32GB)
ALTER SYSTEM SET max_wal_size = '32GB';

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
