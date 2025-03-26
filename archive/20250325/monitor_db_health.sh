#!/bin/bash
# Script to monitor PostgreSQL database health
# Created: 2025-03-17

# Set database connection parameters
DB_HOST=${DB_HOST:-localhost}
DB_PORT=${DB_PORT:-5432}
DB_NAME=${DB_NAME:-ohdsi}
DB_USER=${DB_USER:-postgres}
DB_PASSWORD=${DB_PASSWORD:-acumenus}

# Create connection string
CONN_STRING="postgresql://${DB_USER}:${DB_PASSWORD}@${DB_HOST}:${DB_PORT}/${DB_NAME}"

# Output directory for reports
REPORT_DIR="db_health_reports"
mkdir -p "$REPORT_DIR"

# Generate timestamp for report files
TIMESTAMP=$(date +"%Y%m%d_%H%M%S")
REPORT_FILE="${REPORT_DIR}/db_health_report_${TIMESTAMP}.txt"

# Function to execute a SQL query and append results to the report
execute_query() {
    local query_name="$1"
    local query="$2"
    
    echo "=== $query_name ===" >> "$REPORT_FILE"
    echo "Query executed at: $(date)" >> "$REPORT_FILE"
    echo "" >> "$REPORT_FILE"
    
    psql "${CONN_STRING}" -c "$query" >> "$REPORT_FILE" 2>&1
    
    echo "" >> "$REPORT_FILE"
    echo "----------------------------------------" >> "$REPORT_FILE"
}

# Start the report
echo "PostgreSQL Database Health Report" > "$REPORT_FILE"
echo "Generated: $(date)" >> "$REPORT_FILE"
echo "Database: ${DB_NAME} on ${DB_HOST}:${DB_PORT}" >> "$REPORT_FILE"
echo "----------------------------------------" >> "$REPORT_FILE"
echo "" >> "$REPORT_FILE"

# Check for long-running queries (over 5 minutes)
execute_query "Long-Running Queries" "
SELECT pid, 
       usename,
       application_name,
       client_addr,
       state,
       now() - query_start AS duration, 
       query
FROM pg_stat_activity
WHERE state = 'active'
  AND now() - query_start > interval '5 minutes'
ORDER BY duration DESC;
"

# Check for idle transactions (over 10 minutes)
execute_query "Idle Transactions" "
SELECT pid, 
       usename,
       application_name,
       client_addr,
       state,
       now() - state_change AS idle_duration,
       now() - query_start AS query_duration,
       query
FROM pg_stat_activity
WHERE state = 'idle in transaction'
  AND now() - state_change > interval '10 minutes'
ORDER BY idle_duration DESC;
"

# Check for blocked processes
execute_query "Blocked Processes" "
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
"

# Check for lock contention
execute_query "Lock Contention" "
SELECT relation::regclass, 
       mode, 
       count(*) as lock_count
FROM pg_locks l
JOIN pg_stat_activity s ON l.pid = s.pid
WHERE relation IS NOT NULL
GROUP BY relation, mode
HAVING count(*) > 3
ORDER BY count(*) DESC;
"

# Check for table bloat
execute_query "Table Bloat" "
SELECT
  current_database(), schemaname, tablename, reltuples::bigint, relpages::bigint, otta,
  ROUND(CASE WHEN otta=0 THEN 0.0 ELSE sml.relpages/otta::numeric END,1) AS tbloat,
  CASE WHEN relpages < otta THEN 0 ELSE relpages::bigint - otta END AS wastedpages,
  CASE WHEN relpages < otta THEN 0 ELSE bs*(sml.relpages-otta)::bigint END AS wastedbytes,
  CASE WHEN relpages < otta THEN '0 bytes'::text ELSE pg_size_pretty((bs*(relpages-otta))::bigint) END AS wastedsize,
  iname, ituples::bigint, ipages::bigint, iotta,
  ROUND(CASE WHEN iotta=0 OR ipages=0 THEN 0.0 ELSE ipages/iotta::numeric END,1) AS ibloat,
  CASE WHEN ipages < iotta THEN 0 ELSE ipages::bigint - iotta END AS wastedipages,
  CASE WHEN ipages < iotta THEN 0 ELSE bs*(ipages-iotta) END AS wastedibytes,
  CASE WHEN ipages < iotta THEN '0 bytes' ELSE pg_size_pretty((bs*(ipages-iotta))::bigint) END AS wastedisize
FROM (
  SELECT
    schemaname, tablename, cc.reltuples, cc.relpages, bs,
    CEIL((cc.reltuples*((datahdr+ma-
      (CASE WHEN datahdr%ma=0 THEN ma ELSE datahdr%ma END))+nullhdr2+4))/(bs-20::float)) AS otta,
    COALESCE(c2.relname,'?') AS iname, COALESCE(c2.reltuples,0) AS ituples, COALESCE(c2.relpages,0) AS ipages,
    COALESCE(CEIL((c2.reltuples*(datahdr-12))/(bs-20::float)),0) AS iotta -- very rough approximation, assumes all cols
  FROM (
    SELECT
      ma,bs,schemaname,tablename,
      (datawidth+(hdr+ma-(case when hdr%ma=0 THEN ma ELSE hdr%ma END)))::numeric AS datahdr,
      (maxfracsum*(nullhdr+ma-(case when nullhdr%ma=0 THEN ma ELSE nullhdr%ma END))) AS nullhdr2
    FROM (
      SELECT
        schemaname, tablename, hdr, ma, bs,
        SUM((1-null_frac)*avg_width) AS datawidth,
        MAX(null_frac) AS maxfracsum,
        hdr+(
          SELECT 1+count(*)/8
          FROM pg_stats s2
          WHERE null_frac<>0 AND s2.schemaname = s.schemaname AND s2.tablename = s.tablename
        ) AS nullhdr
      FROM pg_stats s, (
        SELECT
          (SELECT current_setting('block_size')::numeric) AS bs,
          CASE WHEN substring(v,12,3) IN ('8.0','8.1','8.2') THEN 27 ELSE 23 END AS hdr,
          CASE WHEN v ~ 'mingw32' THEN 8 ELSE 4 END AS ma
        FROM (SELECT version() AS v) AS foo
      ) AS constants
      GROUP BY 1,2,3,4,5
    ) AS foo
  ) AS rs
  JOIN pg_class cc ON cc.relname = rs.tablename
  JOIN pg_namespace nn ON cc.relnamespace = nn.oid AND nn.nspname = rs.schemaname AND nn.nspname <> 'information_schema'
  LEFT JOIN pg_index i ON indrelid = cc.oid
  LEFT JOIN pg_class c2 ON c2.oid = i.indexrelid
) AS sml
WHERE sml.relpages - otta > 128
  AND sml.tablename NOT LIKE 'pg_%'
  AND sml.schemaname IN ('omop', 'staging', 'population')
ORDER BY wastedbytes DESC
LIMIT 20;
"

# Check for index usage
execute_query "Index Usage" "
SELECT
    schemaname || '.' || relname AS table,
    indexrelname AS index,
    pg_size_pretty(pg_relation_size(i.indexrelid)) AS index_size,
    idx_scan AS index_scans,
    idx_tup_read AS tuples_read,
    idx_tup_fetch AS tuples_fetched
FROM pg_stat_user_indexes ui
JOIN pg_index i ON ui.indexrelid = i.indexrelid
WHERE NOT indisunique AND idx_scan < 50
  AND pg_relation_size(i.indexrelid) > 5 * 8192
  AND schemaname IN ('omop', 'staging', 'population')
ORDER BY pg_relation_size(i.indexrelid) DESC;
"

# Check for database statistics
execute_query "Database Statistics" "
SELECT * FROM pg_stat_database 
WHERE datname = current_database();
"

# Check for system resource usage
execute_query "System Resource Usage" "
SELECT * FROM pg_stat_bgwriter;
"

# Check for checkpoint statistics
execute_query "Checkpoint Statistics" "
SELECT 
    checkpoints_timed,
    checkpoints_req,
    checkpoint_write_time / 1000 AS checkpoint_write_time_seconds,
    checkpoint_sync_time / 1000 AS checkpoint_sync_time_seconds,
    buffers_checkpoint,
    buffers_clean,
    buffers_backend,
    buffers_backend_fsync,
    buffers_alloc
FROM pg_stat_bgwriter;
"

# Finalize the report
echo "Report completed at: $(date)" >> "$REPORT_FILE"
echo "Full report saved to: $REPORT_FILE"

# Check for critical issues and provide a summary
LONG_RUNNING_COUNT=$(grep -A 1 "=== Long-Running Queries ===" "$REPORT_FILE" | grep -c "duration")
IDLE_TRANS_COUNT=$(grep -A 1 "=== Idle Transactions ===" "$REPORT_FILE" | grep -c "idle_duration")
BLOCKED_COUNT=$(grep -A 1 "=== Blocked Processes ===" "$REPORT_FILE" | grep -c "blocked_pid")

echo ""
echo "Database Health Summary:"
echo "------------------------"
echo "Long-running queries: $LONG_RUNNING_COUNT"
echo "Idle transactions: $IDLE_TRANS_COUNT"
echo "Blocked processes: $BLOCKED_COUNT"
echo ""

if [ $LONG_RUNNING_COUNT -gt 0 ] || [ $IDLE_TRANS_COUNT -gt 0 ] || [ $BLOCKED_COUNT -gt 0 ]; then
    echo "ATTENTION: Potential issues detected. Review the full report for details."
    
    # Provide recommendations
    echo ""
    echo "Recommendations:"
    
    if [ $IDLE_TRANS_COUNT -gt 0 ]; then
        echo "- Terminate idle transactions using: ./run_db_maintenance.sh"
    fi
    
    if [ $BLOCKED_COUNT -gt 0 ]; then
        echo "- Resolve blocking chains using: ./run_db_maintenance.sh"
    fi
    
    if [ $LONG_RUNNING_COUNT -gt 0 ]; then
        echo "- Investigate and optimize long-running queries"
    fi
else
    echo "No critical issues detected."
fi

echo ""
echo "For detailed information, see the full report: $REPORT_FILE"
