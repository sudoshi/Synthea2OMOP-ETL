#!/usr/bin/env python3
"""
Enhanced ETL Progress Monitor

This script monitors the progress of the ETL process, providing detailed information
about each step, estimated time remaining, and system resource usage.
"""

import argparse
import datetime
import os
import sys
import time
import psycopg2
import psutil
from tabulate import tabulate
from colorama import init, Fore, Style

# Initialize colorama
init()

# Configuration
DEFAULT_INTERVAL = 30  # seconds
DEFAULT_COUNT = None  # run indefinitely by default

def get_db_connection(config=None):
    """Create a database connection using environment variables or config."""
    if config is None:
        # Default connection parameters
        db_params = {
            'host': os.environ.get('DB_HOST', 'localhost'),
            'port': os.environ.get('DB_PORT', '5432'),
            'database': os.environ.get('DB_NAME', 'ohdsi'),
            'user': os.environ.get('DB_USER', 'postgres'),
            'password': os.environ.get('DB_PASSWORD', 'acumenus')
        }
    else:
        db_params = config
    
    return psycopg2.connect(**db_params)

def get_active_query(conn):
    """Get the currently running query."""
    cursor = conn.cursor()
    try:
        cursor.execute("""
            SELECT pid, query, state, query_start, now() - query_start as duration
            FROM pg_stat_activity
            WHERE state = 'active'
              AND query NOT LIKE '%pg_stat_activity%'
              AND query NOT LIKE '%pg_locks%'
            ORDER BY duration DESC
            LIMIT 1
        """)
        result = cursor.fetchone()
        if result:
            return {
                'pid': result[0],
                'query': result[1],
                'state': result[2],
                'start_time': result[3],
                'duration': result[4]
            }
        return None
    finally:
        cursor.close()

def get_table_counts(conn):
    """Get row counts for source and target tables."""
    cursor = conn.cursor()
    try:
        # Source tables
        source_tables = [
            'patients_typed',
            'encounters_typed',
            'conditions_typed',
            'medications_typed',
            'procedures_typed',
            'observations_typed'
        ]
        
        # Target tables
        target_tables = [
            'person',
            'visit_occurrence',
            'condition_occurrence',
            'drug_exposure',
            'procedure_occurrence',
            'measurement',
            'observation'
        ]
        
        # Get counts for source tables
        source_counts = {}
        for table in source_tables:
            try:
                cursor.execute(f'SELECT COUNT(*) FROM population.{table}')
                count = cursor.fetchone()[0]
                source_counts[table] = count
            except Exception as e:
                source_counts[table] = f"Error: {str(e)}"
        
        # Get counts for target tables
        target_counts = {}
        for table in target_tables:
            try:
                cursor.execute(f'SELECT COUNT(*) FROM omop.{table}')
                count = cursor.fetchone()[0]
                target_counts[table] = count
            except Exception as e:
                target_counts[table] = f"Error: {str(e)}"
        
        return source_counts, target_counts
    finally:
        cursor.close()

def get_etl_progress(conn):
    """Get ETL progress from the etl_progress table."""
    cursor = conn.cursor()
    try:
        try:
            cursor.execute("""
                SELECT step_name, started_at, completed_at, status, rows_processed, error_message
                FROM staging.etl_progress
                ORDER BY started_at
            """)
            return cursor.fetchall()
        except Exception:
            # Table might not exist yet
            return []
    finally:
        cursor.close()

def get_system_resources():
    """Get system resource usage."""
    cpu_percent = psutil.cpu_percent(interval=0.1)
    memory = psutil.virtual_memory()
    disk = psutil.disk_usage('/')
    
    return {
        'cpu_percent': cpu_percent,
        'memory_percent': memory.percent,
        'memory_used': memory.used / (1024 * 1024 * 1024),  # GB
        'memory_total': memory.total / (1024 * 1024 * 1024),  # GB
        'disk_percent': disk.percent,
        'disk_used': disk.used / (1024 * 1024 * 1024),  # GB
        'disk_total': disk.total / (1024 * 1024 * 1024)  # GB
    }

def calculate_progress(source_counts, target_counts):
    """Calculate progress percentages."""
    progress = {}
    
    # Map source tables to target tables
    table_mapping = {
        'patients_typed': 'person',
        'encounters_typed': 'visit_occurrence',
        'conditions_typed': 'condition_occurrence',
        'medications_typed': 'drug_exposure',
        'procedures_typed': 'procedure_occurrence',
        'observations_typed': ['measurement', 'observation']
    }
    
    # Calculate progress for each mapping
    for source_table, target_table in table_mapping.items():
        if isinstance(target_table, list):
            # Handle multiple target tables (like observations)
            source_count = source_counts.get(source_table, 0)
            if isinstance(source_count, str) or source_count == 0:
                progress[source_table] = {t: 0 for t in target_table}
                continue
                
            for t in target_table:
                target_count = target_counts.get(t, 0)
                if isinstance(target_count, str) or source_count == 0:
                    progress[source_table] = {t: 0}
                    continue
                
                # For observations, we can't directly compare counts
                # since one observation might map to either measurement or observation
                progress.setdefault(source_table, {})[t] = "-"
        else:
            source_count = source_counts.get(source_table, 0)
            target_count = target_counts.get(target_table, 0)
            
            if isinstance(source_count, str) or isinstance(target_count, str) or source_count == 0:
                progress[source_table] = 0
                continue
                
            progress[source_table] = min(100, (target_count / source_count) * 100)
    
    # Calculate overall progress (average of all progress values)
    progress_values = []
    for p in progress.values():
        if isinstance(p, dict):
            # Skip observation tables in overall calculation
            continue
        elif isinstance(p, (int, float)) and not isinstance(p, bool):
            progress_values.append(p)
    
    if progress_values:
        overall_progress = sum(progress_values) / len(progress_values)
    else:
        overall_progress = 0
    
    return progress, overall_progress

def estimate_time_remaining(overall_progress, elapsed_time):
    """Estimate time remaining based on progress and elapsed time."""
    if overall_progress <= 0:
        return "Unknown"
    
    # Calculate estimated total time
    estimated_total_time = elapsed_time * (100 / overall_progress)
    
    # Calculate remaining time
    remaining_time = estimated_total_time - elapsed_time
    
    # Format remaining time
    if remaining_time < 60:
        return f"{int(remaining_time)} seconds"
    elif remaining_time < 3600:
        return f"{int(remaining_time / 60)} minutes"
    else:
        hours = int(remaining_time / 3600)
        minutes = int((remaining_time % 3600) / 60)
        return f"{hours} hours, {minutes} minutes"

def format_query(query, max_length=80):
    """Format a query for display."""
    if not query:
        return "No active query"
    
    # Simplify the query for display
    lines = query.strip().split('\n')
    formatted_lines = []
    
    for line in lines:
        line = line.strip()
        if not line:
            continue
        
        # Truncate long lines
        if len(line) > max_length:
            formatted_lines.append(line[:max_length] + "...")
        else:
            formatted_lines.append(line)
    
    # Limit the number of lines
    if len(formatted_lines) > 10:
        return "\n".join(formatted_lines[:5]) + "\n...\n" + "\n".join(formatted_lines[-5:])
    
    return "\n".join(formatted_lines)

def display_progress(start_time, conn):
    """Display ETL progress information."""
    # Get current time
    current_time = datetime.datetime.now()
    elapsed_time = (current_time - start_time).total_seconds()
    
    # Get active query
    active_query = get_active_query(conn)
    
    # Get table counts
    source_counts, target_counts = get_table_counts(conn)
    
    # Calculate progress
    progress, overall_progress = calculate_progress(source_counts, target_counts)
    
    # Estimate time remaining
    time_remaining = estimate_time_remaining(overall_progress, elapsed_time)
    
    # Get ETL progress from the etl_progress table
    etl_progress = get_etl_progress(conn)
    
    # Get system resources
    resources = get_system_resources()
    
    # Clear screen
    os.system('cls' if os.name == 'nt' else 'clear')
    
    # Display header
    print(f"{Fore.CYAN}ETL Progress Monitor - {current_time.strftime('%Y-%m-%d %H:%M:%S')}{Style.RESET_ALL}")
    print("-" * 80)
    
    # Display active query
    if active_query:
        print(f"{Fore.GREEN}Current Query (PID: {active_query['pid']} State: {active_query['state']}):{Style.RESET_ALL}")
        print(f"Started at: {active_query['start_time']}")
        print(f"Duration: {active_query['duration']}")
        print("-" * 80)
        print(format_query(active_query['query']))
        print("-" * 80)
    else:
        print(f"{Fore.YELLOW}No active ETL query running{Style.RESET_ALL}")
        print("-" * 80)
    
    # Display table counts and progress
    table_data = []
    for source_table in sorted(source_counts.keys()):
        source_count = source_counts[source_table]
        
        if source_table == 'observations_typed':
            # Handle observations separately (maps to both measurement and observation)
            measurement_count = target_counts.get('measurement', 0)
            observation_count = target_counts.get('observation', 0)
            
            table_data.append([
                source_table, 
                f"{source_count:,}" if isinstance(source_count, int) else source_count,
                'measurement',
                f"{measurement_count:,}" if isinstance(measurement_count, int) else measurement_count,
                progress.get(source_table, {}).get('measurement', 0)
            ])
            
            table_data.append([
                "", 
                "",
                'observation',
                f"{observation_count:,}" if isinstance(observation_count, int) else observation_count,
                progress.get(source_table, {}).get('observation', 0)
            ])
        else:
            # Map source table to target table
            target_table = {
                'patients_typed': 'person',
                'encounters_typed': 'visit_occurrence',
                'conditions_typed': 'condition_occurrence',
                'medications_typed': 'drug_exposure',
                'procedures_typed': 'procedure_occurrence'
            }.get(source_table)
            
            target_count = target_counts.get(target_table, 0)
            progress_value = progress.get(source_table, 0)
            
            table_data.append([
                source_table, 
                f"{source_count:,}" if isinstance(source_count, int) else source_count,
                target_table,
                f"{target_count:,}" if isinstance(target_count, int) else target_count,
                f"{progress_value:.2f}%" if isinstance(progress_value, (int, float)) and not isinstance(progress_value, bool) else progress_value
            ])
    
    print(tabulate(
        table_data,
        headers=["Source Table", "Count", "Target Table", "Count", "Progress"],
        tablefmt="grid"
    ))
    print("-" * 80)
    
    # Display overall progress
    print(f"{Fore.GREEN}Overall Progress: {overall_progress:.2f}%{Style.RESET_ALL}")
    print(f"{Fore.GREEN}Elapsed Time: {int(elapsed_time // 3600)}h {int((elapsed_time % 3600) // 60)}m {int(elapsed_time % 60)}s{Style.RESET_ALL}")
    print(f"{Fore.GREEN}Estimated Time Remaining: {time_remaining}{Style.RESET_ALL}")
    print("-" * 80)
    
    # Display system resources
    print(f"{Fore.CYAN}System Resources:{Style.RESET_ALL}")
    print(f"CPU Usage: {resources['cpu_percent']:.1f}%")
    print(f"Memory Usage: {resources['memory_used']:.1f}GB / {resources['memory_total']:.1f}GB ({resources['memory_percent']:.1f}%)")
    print(f"Disk Usage: {resources['disk_used']:.1f}GB / {resources['disk_total']:.1f}GB ({resources['disk_percent']:.1f}%)")
    print("-" * 80)
    
    # Display ETL progress from the etl_progress table
    if etl_progress:
        print(f"{Fore.CYAN}ETL Steps Progress:{Style.RESET_ALL}")
        etl_data = []
        for step in etl_progress:
            step_name, started_at, completed_at, status, rows_processed, error_message = step
            
            # Format duration
            if completed_at:
                duration = (completed_at - started_at).total_seconds()
                duration_str = f"{int(duration // 60)}m {int(duration % 60)}s"
            else:
                duration_str = "-"
            
            # Format status with color
            if status == 'completed':
                status_str = f"{Fore.GREEN}Completed{Style.RESET_ALL}"
            elif status == 'in_progress':
                status_str = f"{Fore.YELLOW}In Progress{Style.RESET_ALL}"
            elif status == 'error':
                status_str = f"{Fore.RED}Error{Style.RESET_ALL}"
            else:
                status_str = status
            
            etl_data.append([
                step_name,
                started_at.strftime('%H:%M:%S') if started_at else "-",
                completed_at.strftime('%H:%M:%S') if completed_at else "-",
                duration_str,
                status_str,
                f"{rows_processed:,}" if rows_processed else "-",
                error_message or "-"
            ])
        
        print(tabulate(
            etl_data,
            headers=["Step", "Started", "Completed", "Duration", "Status", "Rows", "Error"],
            tablefmt="grid"
        ))
        print("-" * 80)

def main():
    """Main function."""
    parser = argparse.ArgumentParser(description="Monitor ETL progress")
    parser.add_argument("--interval", type=int, default=DEFAULT_INTERVAL,
                        help=f"Update interval in seconds (default: {DEFAULT_INTERVAL})")
    parser.add_argument("--count", type=int, default=DEFAULT_COUNT,
                        help="Number of updates to display (default: run indefinitely)")
    parser.add_argument("--host", default=os.environ.get('DB_HOST', 'localhost'),
                        help="Database host (default: from DB_HOST env var or 'localhost')")
    parser.add_argument("--port", default=os.environ.get('DB_PORT', '5432'),
                        help="Database port (default: from DB_PORT env var or '5432')")
    parser.add_argument("--dbname", default=os.environ.get('DB_NAME', 'ohdsi'),
                        help="Database name (default: from DB_NAME env var or 'ohdsi')")
    parser.add_argument("--user", default=os.environ.get('DB_USER', 'postgres'),
                        help="Database user (default: from DB_USER env var or 'postgres')")
    parser.add_argument("--password", default=os.environ.get('DB_PASSWORD', 'acumenus'),
                        help="Database password (default: from DB_PASSWORD env var)")
    
    args = parser.parse_args()
    
    # Database connection parameters
    db_params = {
        'host': args.host,
        'port': args.port,
        'database': args.dbname,
        'user': args.user,
        'password': args.password
    }
    
    try:
        # Connect to the database
        conn = get_db_connection(db_params)
        
        # Record start time
        start_time = datetime.datetime.now()
        
        # Display progress
        count = 0
        while args.count is None or count < args.count:
            display_progress(start_time, conn)
            
            count += 1
            if args.count is None or count < args.count:
                print(f"\nNext update in {args.interval} seconds. Press Ctrl+C to exit.")
                time.sleep(args.interval)
        
        # Close the connection
        conn.close()
        
    except KeyboardInterrupt:
        print("\nMonitoring stopped by user")
        sys.exit(0)
    except Exception as e:
        print(f"Error: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()
