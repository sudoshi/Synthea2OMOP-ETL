#!/usr/bin/env python3
"""
Simple ETL Progress Monitor

This script provides a simplified view of the ETL progress.
"""

import os
import sys
import time
import argparse
import datetime
import psycopg2
import psutil

def get_db_connection():
    """Create a database connection using environment variables."""
    db_params = {
        'host': os.environ.get('DB_HOST', 'localhost'),
        'port': os.environ.get('DB_PORT', '5432'),
        'database': os.environ.get('DB_NAME', 'ohdsi'),
        'user': os.environ.get('DB_USER', 'postgres'),
        'password': os.environ.get('DB_PASSWORD', 'acumenus')
    }
    
    return psycopg2.connect(**db_params)

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
                source_counts[table] = 0
        
        # Get counts for target tables
        target_counts = {}
        for table in target_tables:
            try:
                cursor.execute(f'SELECT COUNT(*) FROM omop.{table}')
                count = cursor.fetchone()[0]
                target_counts[table] = count
            except Exception as e:
                target_counts[table] = 0
        
        return source_counts, target_counts
    finally:
        cursor.close()

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
            if source_count == 0:
                progress[source_table] = {t: 0 for t in target_table}
                continue
                
            for t in target_table:
                target_count = target_counts.get(t, 0)
                if source_count == 0:
                    progress[source_table] = {t: 0}
                    continue
                
                # For observations, we can't directly compare counts
                # since one observation might map to either measurement or observation
                progress.setdefault(source_table, {})[t] = "-"
        else:
            source_count = source_counts.get(source_table, 0)
            target_count = target_counts.get(target_table, 0)
            
            if source_count == 0:
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
    
    # Clear screen
    os.system('cls' if os.name == 'nt' else 'clear')
    
    # Display header
    print(f"ETL Progress Monitor - {current_time.strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 80)
    
    # Display active query
    if active_query:
        print(f"Current Query (PID: {active_query['pid']} State: {active_query['state']}):")
        print(f"Started at: {active_query['start_time']}")
        print(f"Duration: {active_query['duration']}")
        print("-" * 80)
        print(format_query(active_query['query']))
        print("-" * 80)
    else:
        print("No active ETL query running")
        print("-" * 80)
    
    # Display overall progress
    print(f"Overall Progress: {overall_progress:.2f}%")
    print(f"Elapsed Time: {int(elapsed_time // 3600)}h {int((elapsed_time % 3600) // 60)}m {int(elapsed_time % 60)}s")
    
    # Estimate time remaining
    if overall_progress > 0:
        total_time = elapsed_time * (100 / overall_progress)
        remaining_time = total_time - elapsed_time
        print(f"Estimated Time Remaining: {int(remaining_time // 3600)}h {int((remaining_time % 3600) // 60)}m {int(remaining_time % 60)}s")
    else:
        print("Estimated Time Remaining: Unknown")
    
    print("-" * 80)
    
    # Display table progress
    print("Table Progress:")
    print(f"{'Source Table':<20} {'Source Count':<15} {'Target Table':<20} {'Target Count':<15} {'Progress':<10}")
    print("-" * 80)
    
    # Person
    print(f"{'patients_typed':<20} {source_counts.get('patients_typed', 0):<15,} {'person':<20} {target_counts.get('person', 0):<15,} {progress.get('patients_typed', 0):.2f}%")
    
    # Visit Occurrence
    print(f"{'encounters_typed':<20} {source_counts.get('encounters_typed', 0):<15,} {'visit_occurrence':<20} {target_counts.get('visit_occurrence', 0):<15,} {progress.get('encounters_typed', 0):.2f}%")
    
    # Condition Occurrence
    print(f"{'conditions_typed':<20} {source_counts.get('conditions_typed', 0):<15,} {'condition_occurrence':<20} {target_counts.get('condition_occurrence', 0):<15,} {progress.get('conditions_typed', 0):.2f}%")
    
    # Drug Exposure
    print(f"{'medications_typed':<20} {source_counts.get('medications_typed', 0):<15,} {'drug_exposure':<20} {target_counts.get('drug_exposure', 0):<15,} {progress.get('medications_typed', 0):.2f}%")
    
    # Procedure Occurrence
    print(f"{'procedures_typed':<20} {source_counts.get('procedures_typed', 0):<15,} {'procedure_occurrence':<20} {target_counts.get('procedure_occurrence', 0):<15,} {progress.get('procedures_typed', 0):.2f}%")
    
    # Measurement
    print(f"{'observations_typed':<20} {source_counts.get('observations_typed', 0):<15,} {'measurement':<20} {target_counts.get('measurement', 0):<15,} -")
    
    # Observation
    print(f"{'':<20} {'':<15} {'observation':<20} {target_counts.get('observation', 0):<15,} -")
    
    print("-" * 80)
    
    # Display system resources
    cpu_percent = psutil.cpu_percent(interval=0.1)
    memory = psutil.virtual_memory()
    disk = psutil.disk_usage('/')
    
    print("System Resources:")
    print(f"CPU Usage: {cpu_percent:.1f}%")
    print(f"Memory Usage: {memory.used / (1024 * 1024 * 1024):.1f}GB / {memory.total / (1024 * 1024 * 1024):.1f}GB ({memory.percent:.1f}%)")
    print(f"Disk Usage: {disk.used / (1024 * 1024 * 1024):.1f}GB / {disk.total / (1024 * 1024 * 1024):.1f}GB ({disk.percent:.1f}%)")
    print("-" * 80)

def main():
    """Main function."""
    parser = argparse.ArgumentParser(description="Monitor ETL progress")
    parser.add_argument("--interval", type=int, default=10,
                        help="Update interval in seconds (default: 10)")
    parser.add_argument("--count", type=int, default=None,
                        help="Number of updates to display (default: run indefinitely)")
    
    args = parser.parse_args()
    
    try:
        # Connect to the database
        conn = get_db_connection()
        
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
