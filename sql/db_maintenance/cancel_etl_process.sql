-- Script to identify and cancel long-running ETL processes
-- Created: 2025-03-17

-- Identify the main ETL process and its children
SELECT pid, 
       usename,
       state,
       query_start,
       now() - query_start AS duration, 
       query
FROM pg_stat_activity
WHERE query LIKE '%run_all_etl_optimized%'
   OR pid = 338160
ORDER BY duration DESC;

-- First try to cancel the query (sends SIGINT)
-- Uncomment after confirming the PID
-- SELECT pg_cancel_backend(338160);

-- Wait 10 seconds to see if it terminates gracefully
-- If cancellation doesn't work, terminate the connection (sends SIGTERM)
-- Uncomment after trying pg_cancel_backend first
-- SELECT pg_terminate_backend(338160);

-- Verify it's gone
SELECT pid, state, query
FROM pg_stat_activity
WHERE pid = 338160;

-- Check for any child processes that might still be running
SELECT pid, 
       usename,
       state,
       query_start,
       now() - query_start AS duration, 
       query
FROM pg_stat_activity
WHERE query LIKE '%run_all_etl%'
ORDER BY duration DESC;

-- Uncomment to terminate any remaining child processes
-- SELECT pg_terminate_backend(child_pid);
