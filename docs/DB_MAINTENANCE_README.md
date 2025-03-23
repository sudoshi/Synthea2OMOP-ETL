# PostgreSQL Database Maintenance Tools

This directory contains tools for diagnosing and resolving PostgreSQL database performance issues, particularly during ETL processes.

## Overview

The maintenance tools consist of SQL scripts and a shell script to execute them in sequence. These tools are designed to address common PostgreSQL performance issues such as:

- Blocked processes and lock contention
- Idle transactions holding locks
- Resource overcommitment (CPU, memory, I/O)
- Excessive parallelism
- Long-running queries

## Available Scripts

### SQL Scripts

1. **terminate_idle_transactions.sql**
   - Identifies and terminates idle transactions that may be holding locks
   - Located in `sql/db_maintenance/terminate_idle_transactions.sql`

2. **resolve_blocking_chain.sql**
   - Identifies blocking chains where processes are waiting for resources held by other processes
   - Provides commands to terminate blocking processes
   - Located in `sql/db_maintenance/resolve_blocking_chain.sql`

3. **cancel_etl_process.sql**
   - Identifies and cancels long-running ETL processes
   - Provides options to cancel (SIGINT) or terminate (SIGTERM) processes
   - Located in `sql/db_maintenance/cancel_etl_process.sql`

4. **monitor_recovery.sql**
   - Monitors system recovery after intervention
   - Checks for remaining active queries, locks, and blocked processes
   - Located in `sql/db_maintenance/monitor_recovery.sql`

5. **optimize_postgres_config.sql**
   - Optimizes PostgreSQL configuration for better performance
   - Adjusts memory, parallelism, and I/O settings
   - Located in `sql/db_maintenance/optimize_postgres_config.sql`

6. **etl_batch_processing.sql**
   - Provides examples for modifying ETL scripts to use batching and frequent commits
   - Includes patterns for batched INSERT, UPDATE, and DELETE operations
   - Shows how to create indexes in non-blocking mode
   - Located in `sql/db_maintenance/etl_batch_processing.sql`

### Shell Scripts

- **run_db_maintenance.sh**
  - Executes the SQL scripts in sequence
  - Provides interactive prompts to review results and continue
  - Allows optional PostgreSQL configuration optimization

- **monitor_db_health.sh**
  - Proactively monitors database health
  - Generates detailed reports on long-running queries, idle transactions, and blocked processes
  - Identifies table bloat and unused indexes
  - Provides recommendations based on detected issues

## Usage

### Prerequisites

- PostgreSQL client (psql) installed
- Database connection parameters set as environment variables or defaults in the script

### Running the Maintenance Tools

1. Make the shell script executable:
   ```bash
   chmod +x run_db_maintenance.sh
   ```

2. Run the script:
   ```bash
   ./run_db_maintenance.sh
   ```

3. Follow the interactive prompts to execute each step and review results.

### Environment Variables

You can customize the database connection parameters by setting the following environment variables:

- `DB_HOST`: Database host (default: localhost)
- `DB_PORT`: Database port (default: 5432)
- `DB_NAME`: Database name (default: ohdsi)
- `DB_USER`: Database user (default: postgres)
- `DB_PASSWORD`: Database password (default: acumenus)

Example:
```bash
DB_HOST=db.example.com DB_NAME=mydb ./run_db_maintenance.sh
```

## Maintenance Steps

The maintenance process follows these steps:

1. **Terminate Idle Transactions**
   - Identifies and terminates idle transactions that may be holding locks

2. **Resolve Blocking Chain**
   - Identifies blocking chains and provides options to terminate blocking processes

3. **Cancel ETL Process**
   - Identifies and cancels long-running ETL processes if needed

4. **Monitor Recovery**
   - Checks if the system is recovering after the interventions

5. **Optimize PostgreSQL Configuration (Optional)**
   - Adjusts PostgreSQL configuration parameters for better performance

## Best Practices

1. **Review Before Terminating**
   - Always review the output of diagnostic queries before terminating processes
   - Understand which processes are critical before terminating them

2. **Start with Least Disruptive Actions**
   - Begin with terminating idle transactions
   - Then address blocking chains
   - Only cancel active ETL processes if necessary

3. **Monitor Recovery**
   - Run the monitoring script multiple times to track progress
   - Ensure the system is stabilizing before ending the maintenance session

4. **Configuration Changes**
   - Some configuration changes require a server restart to take effect
   - Consider scheduling a maintenance window for configuration changes

## Troubleshooting

If you encounter issues:

1. Check database connection parameters
2. Ensure you have sufficient privileges to execute the commands
3. Review PostgreSQL logs for additional error information
4. For persistent issues, consider restarting the PostgreSQL server

## Maintenance Schedule

Consider implementing a regular maintenance schedule:

1. Daily: Run monitoring scripts to detect potential issues
2. Weekly: Check for idle transactions and long-running queries
3. Monthly: Review and optimize PostgreSQL configuration

### Scheduling with Cron

You can schedule the monitoring script to run automatically using cron:

```bash
# Edit crontab
crontab -e

# Add one of the following entries:

# Run every hour
0 * * * * /path/to/monitor_db_health.sh

# Run three times a day (8 AM, 2 PM, 8 PM)
0 8,14,20 * * * /path/to/monitor_db_health.sh

# Run daily at midnight
0 0 * * * /path/to/monitor_db_health.sh
```

For critical production environments, consider setting up email notifications:

```bash
# Run daily and email the report
0 0 * * * /path/to/monitor_db_health.sh | mail -s "Database Health Report" admin@example.com
```

## Additional Resources

- [PostgreSQL Documentation](https://www.postgresql.org/docs/)
- [PostgreSQL Performance Tuning](https://www.postgresql.org/docs/current/performance-tips.html)
- [PostgreSQL Lock Monitoring](https://www.postgresql.org/docs/current/monitoring-locks.html)
