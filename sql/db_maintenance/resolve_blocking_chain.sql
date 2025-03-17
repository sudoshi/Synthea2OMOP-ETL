-- Script to identify and resolve blocking chains in PostgreSQL
-- Created: 2025-03-17

-- Find blocking/blocked processes
SELECT blocked_locks.pid AS blocked_pid,
       blocked_activity.usename AS blocked_user,
       blocking_locks.pid AS blocking_pid,
       blocking_activity.usename AS blocking_user,
       blocked_activity.query AS blocked_statement,
       blocking_activity.query AS blocking_statement
FROM pg_catalog.pg_locks blocked_locks
JOIN pg_catalog.pg_stat_activity blocked_activity ON blocked_locks.pid = blocked_activity.pid
JOIN pg_catalog.pg_locks blocking_locks 
    ON blocking_locks.locktype = blocked_locks.locktype
    AND blocking_locks.database IS NOT DISTINCT FROM blocked_locks.database
    AND blocking_locks.relation IS NOT DISTINCT FROM blocked_locks.relation
    AND blocking_locks.page IS NOT DISTINCT FROM blocked_locks.page
    AND blocking_locks.tuple IS NOT DISTINCT FROM blocked_locks.tuple
    AND blocking_locks.virtualxid IS NOT DISTINCT FROM blocked_locks.virtualxid
    AND blocking_locks.transactionid IS NOT DISTINCT FROM blocked_locks.transactionid
    AND blocking_locks.classid IS NOT DISTINCT FROM blocked_locks.classid
    AND blocking_locks.objid IS NOT DISTINCT FROM blocked_locks.objid
    AND blocking_locks.objsubid IS NOT DISTINCT FROM blocked_locks.objsubid
    AND blocking_locks.pid != blocked_locks.pid
JOIN pg_catalog.pg_stat_activity blocking_activity ON blocking_locks.pid = blocking_activity.pid
WHERE NOT blocked_locks.granted;

-- After reviewing the output, uncomment and modify the following commands to terminate specific blocking processes
-- Replace blocking_pid_number with the actual PID from the query above

-- SELECT pg_terminate_backend(blocking_pid_number);

-- Check for processes in specific waiting states (DROP TABLE, TRUNCATE TABLE)
SELECT pid, 
       usename, 
       state, 
       wait_event_type, 
       wait_event, 
       query
FROM pg_stat_activity
WHERE state LIKE '%waiting%'
  AND (query LIKE '%DROP TABLE%' OR query LIKE '%TRUNCATE TABLE%');

-- Uncomment to terminate these specific processes if needed
-- SELECT pg_terminate_backend(pid_from_above_query);
