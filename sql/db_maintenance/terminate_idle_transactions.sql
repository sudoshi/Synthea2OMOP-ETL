-- Script to identify and terminate idle transactions
-- Created: 2025-03-17

-- First check what locks these idle transactions are holding
SELECT relation::regclass, mode, pid, granted
FROM pg_locks l
JOIN pg_stat_activity s ON l.pid = s.pid
WHERE s.pid IN (332059, 370788);

-- Then terminate these sessions
SELECT pg_terminate_backend(332059);
SELECT pg_terminate_backend(370788);

-- Verify they're gone
SELECT pid, state, query, now() - query_start AS duration
FROM pg_stat_activity
WHERE pid IN (332059, 370788);
