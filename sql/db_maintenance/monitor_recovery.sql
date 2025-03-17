-- Script to monitor PostgreSQL system recovery after intervention
-- Created: 2025-03-17

-- Check for remaining active queries
SELECT pid, 
       usename,
       state, 
       wait_event_type,
       wait_event,
       now() - query_start AS duration, 
       query
FROM pg_stat_activity
WHERE state != 'idle'
  AND query NOT LIKE '%pg_stat_activity%'
ORDER BY duration DESC;

-- Check for any remaining locks
SELECT relation::regclass, 
       mode, 
       count(*) as lock_count
FROM pg_locks l
JOIN pg_stat_activity s ON l.pid = s.pid
WHERE relation IS NOT NULL
GROUP BY relation, mode
ORDER BY count(*) DESC;

-- Check for any remaining blocked processes
SELECT count(*) AS blocked_queries
FROM pg_stat_activity
WHERE wait_event_type = 'Lock'
  AND state = 'active';

-- Check for processes in specific states
SELECT state, count(*) 
FROM pg_stat_activity 
GROUP BY state 
ORDER BY count(*) DESC;

-- Check for long-running queries (over 5 minutes)
SELECT pid, 
       usename,
       state,
       now() - query_start AS duration, 
       query
FROM pg_stat_activity
WHERE state = 'active'
  AND now() - query_start > interval '5 minutes'
ORDER BY duration DESC;

-- Check for idle in transaction sessions
SELECT pid, 
       usename,
       state,
       now() - query_start AS duration, 
       query
FROM pg_stat_activity
WHERE state = 'idle in transaction'
ORDER BY duration DESC;

-- Check PostgreSQL statistics
SELECT * FROM pg_stat_database 
WHERE datname = current_database();
