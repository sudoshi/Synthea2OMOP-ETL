#!/usr/bin/env python3
"""
etl_progress_tracking.py

A module for tracking and reporting ETL progress.
"""

import os
import sys
import time
import logging
import argparse
import psycopg2
from psycopg2 import pool
from tqdm import tqdm
from datetime import datetime

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class ETLProgressTracker:
    """Track and report progress of ETL operations."""
    
    def __init__(self, db_config):
        """Initialize the progress tracker with database connection."""
        self.db_config = db_config
        self.conn = None
        self.initialize_connection()
        self.ensure_progress_table()
        
    def initialize_connection(self):
        """Initialize database connection."""
        try:
            self.conn = psycopg2.connect(**self.db_config)
            self.conn.autocommit = True
            logger.debug("Database connection initialized")
        except Exception as e:
            logger.error(f"Failed to initialize database connection: {e}")
            sys.exit(1)
    
    def ensure_progress_table(self):
        """Ensure the etl_progress table exists."""
        try:
            with self.conn.cursor() as cursor:
                # Check the existing table schema
                cursor.execute("""
                SELECT EXISTS (
                    SELECT FROM information_schema.tables 
                    WHERE table_schema = 'staging'
                    AND table_name = 'etl_progress'
                );
                """)
                table_exists = cursor.fetchone()[0]
                
                if not table_exists:
                    # Create schema if it doesn't exist
                    cursor.execute("CREATE SCHEMA IF NOT EXISTS staging;")
                    
                    # Create the progress table with the expected schema to match the existing system
                    cursor.execute("""
                    CREATE TABLE IF NOT EXISTS staging.etl_progress (
                        step_name VARCHAR(100) NOT NULL PRIMARY KEY,
                        started_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        completed_at TIMESTAMP,
                        status VARCHAR(20) DEFAULT 'in_progress',
                        rows_processed BIGINT DEFAULT 0,
                        total_rows BIGINT DEFAULT 0,
                        percentage_complete NUMERIC(5,2) DEFAULT 0,
                        error_message TEXT
                    );
                    """)
                    logger.debug("ETL progress table created with compatibility schema")
                logger.debug("Using existing ETL progress table")
                logger.debug("ETL progress table created/verified")
        except Exception as e:
            logger.error(f"Failed to create progress table: {e}")
            sys.exit(1)
    
    def start_step(self, process_name, step_name, total_items=None, message=None):
        """Register the start of an ETL step."""
        try:
            with self.conn.cursor() as cursor:
                # Combine process_name and step_name since the existing table only has step_name
                combined_step = f"{process_name}_{step_name}"
                
                # Use the total_items if provided
                total_rows = total_items if total_items is not None else 0
                
                cursor.execute("""
                INSERT INTO staging.etl_progress 
                    (step_name, status, rows_processed, total_rows, percentage_complete, error_message)
                VALUES 
                    (%s, 'in_progress', 0, %s, 0, %s)
                ON CONFLICT (step_name) 
                DO UPDATE SET 
                    status = 'in_progress',
                    started_at = CURRENT_TIMESTAMP,
                    completed_at = NULL,
                    rows_processed = 0,
                    total_rows = EXCLUDED.total_rows,
                    percentage_complete = 0,
                    error_message = EXCLUDED.error_message
                """, (combined_step, total_rows, message))
                logger.info(f"Started ETL step: {combined_step} with target of {total_rows} rows")
        except Exception as e:
            logger.error(f"Failed to register step start: {e}")
    
    def update_progress(self, process_name, step_name, processed_items, message=None, total_items=None):
        """Update the progress of an ETL step."""
        try:
            with self.conn.cursor() as cursor:
                # Combine process_name and step_name since the existing table only has step_name
                combined_step = f"{process_name}_{step_name}"
                
                # If total_items is provided, update it as well
                if total_items is not None:
                    # Calculate percentage
                    percentage = round((processed_items / total_items) * 100, 2) if total_items > 0 else 0
                    
                    # Update progress with total and percentage
                    cursor.execute("""
                    UPDATE staging.etl_progress 
                    SET 
                        rows_processed = %s,
                        total_rows = %s,
                        percentage_complete = %s,
                        error_message = COALESCE(%s, error_message)
                    WHERE 
                        step_name = %s
                    """, (processed_items, total_items, percentage, message, combined_step))
                else:
                    # Get current total_rows
                    cursor.execute("""
                    SELECT total_rows FROM staging.etl_progress 
                    WHERE step_name = %s
                    """, (combined_step,))
                    result = cursor.fetchone()
                    
                    if result and result[0] > 0:
                        total_rows = result[0]
                        percentage = round((processed_items / total_rows) * 100, 2) if total_rows > 0 else 0
                    else:
                        # If no total or total is 0, use a percentage based on processed items
                        # This is just an estimate for display
                        percentage = min(processed_items / 100, 99.9) if processed_items < 100 else 99.9
                    
                    # Update progress with calculated percentage
                    cursor.execute("""
                    UPDATE staging.etl_progress 
                    SET 
                        rows_processed = %s,
                        percentage_complete = %s,
                        error_message = COALESCE(%s, error_message)
                    WHERE 
                        step_name = %s
                    """, (processed_items, percentage, message, combined_step))
                
                rows_affected = cursor.rowcount
                if rows_affected == 0:
                    logger.error(f"Step not found: {combined_step}")
                    return
                    
                # Get the current percentage for logging
                cursor.execute("""
                SELECT percentage_complete FROM staging.etl_progress 
                WHERE step_name = %s
                """, (combined_step,))
                current_percentage = cursor.fetchone()[0]
                
                logger.debug(f"Updated progress: {combined_step}: {processed_items} rows processed ({current_percentage:.2f}%)")
        except Exception as e:
            logger.error(f"Failed to update progress: {e}")
    
    def complete_step(self, process_name, step_name, success=True, message=None):
        """Mark an ETL step as complete."""
        status = 'completed' if success else 'failed'
        try:
            with self.conn.cursor() as cursor:
                # Combine process_name and step_name since the existing table only has step_name
                combined_step = f"{process_name}_{step_name}"
                
                # Update progress to complete
                cursor.execute("""
                UPDATE staging.etl_progress 
                SET 
                    status = %s,
                    completed_at = CURRENT_TIMESTAMP,
                    error_message = COALESCE(%s, error_message)
                WHERE 
                    step_name = %s
                """, (status, message, combined_step))
                
                rows_affected = cursor.rowcount
                if rows_affected == 0:
                    logger.error(f"Step not found: {combined_step}")
                    return
                    
                logger.info(f"Completed ETL step: {combined_step} with status: {status}")
        except Exception as e:
            logger.error(f"Failed to mark step as complete: {e}")

    def display_progress(self, process_name=None):
        """Display the current ETL progress."""
        try:
            # First check if the table exists
            with self.conn.cursor() as cursor:
                cursor.execute("""
                SELECT EXISTS (
                    SELECT FROM information_schema.tables 
                    WHERE table_schema = 'staging'
                    AND table_name = 'etl_progress'
                );
                """)
                table_exists = cursor.fetchone()[0]
                
                if not table_exists:
                    print("\nNo ETL progress data found. Table staging.etl_progress does not exist.")
                    print("The ETL process has not started tracking progress yet.")
                    return
                
                # Check if the percentage_complete column exists
                cursor.execute("""
                SELECT EXISTS (
                    SELECT FROM information_schema.columns
                    WHERE table_schema = 'staging'
                    AND table_name = 'etl_progress'
                    AND column_name = 'percentage_complete'
                );
                """)
                percentage_column_exists = cursor.fetchone()[0]
                
                # Query using the appropriate table structure
                if percentage_column_exists:
                    query = """
                    SELECT 
                        step_name, 
                        started_at,
                        completed_at,
                        status,
                        rows_processed,
                        total_rows,
                        percentage_complete,
                        error_message
                    FROM staging.etl_progress
                    """
                else:
                    query = """
                    SELECT 
                        step_name, 
                        started_at,
                        completed_at,
                        status,
                        rows_processed,
                        0 as total_rows,
                        0 as percentage_complete,
                        error_message
                    FROM staging.etl_progress
                    """
                
                # Filter by process name if provided by extracting from combined step name
                if process_name:
                    query += " WHERE step_name LIKE %s"
                    cursor.execute(query, (f"{process_name}_%",))
                else:
                    cursor.execute(query)
                
                results = cursor.fetchall()
                
                if not results:
                    print("No ETL progress data found.")
                    return
                
                # Calculate overall progress (excluding steps with 0 rows processed)
                active_steps = [r for r in results if not (r[3] == 'completed' and r[4] == 0)]
                total_active_steps = len(active_steps)
                completed_active_steps = sum(1 for r in active_steps if r[3] == 'completed')
                
                # Total count including inactive steps
                total_steps = len(results)
                completed_steps = sum(1 for r in results if r[3] == 'completed')
                
                # Calculate percentages
                if total_active_steps > 0:
                    active_completion_ratio = completed_active_steps / total_active_steps
                    overall_percentage = active_completion_ratio * 100
                else:
                    overall_percentage = 0
                
                # Number of steps with no data
                no_data_steps = sum(1 for r in results if r[3] == 'completed' and r[4] == 0)
                
                print("\n" + "="*80)
                print(f"ETL PROGRESS SUMMARY - Overall: {overall_percentage:.2f}% ({completed_active_steps}/{total_active_steps} active steps)")
                if no_data_steps > 0:
                    print(f"Note: {no_data_steps} steps are marked as complete but processed no data")
                print("="*80)
                
                for r in results:
                    if len(r) == 8:  # If we're using the updated schema with percentage
                        step_name, start_time, end_time, status, rows_processed, total_rows, percentage_complete, error_msg = r
                    else:  # Backward compatibility
                        step_name, start_time, end_time, status, rows_processed, error_msg = r
                        total_rows = 0
                        percentage_complete = 0
                    
                    # Try to extract process name and step from combined name
                    parts = step_name.split('_', 1)
                    process = parts[0] if len(parts) > 1 else ''
                    step = parts[1] if len(parts) > 1 else step_name
                    
                    # Calculate duration
                    now = datetime.now()
                    start = start_time.replace(tzinfo=None) if start_time else now
                    duration = (end_time.replace(tzinfo=None) - start).total_seconds() if end_time else (now - start).total_seconds()
                    hours, remainder = divmod(int(duration), 3600)
                    minutes, seconds = divmod(remainder, 60)
                    
                    # Format status with color
                    status_str = status.upper()
                    if status == 'in_progress':
                        status_str = "\033[33mIN PROGRESS\033[0m"  # Yellow
                    elif status == 'completed':
                        if rows_processed == 0:
                            status_str = "\033[33mCOMPLETED (NO DATA)\033[0m"  # Yellow warning
                        else:
                            status_str = "\033[32mCOMPLETED\033[0m"  # Green
                    elif status == 'failed':
                        status_str = "\033[31mFAILED\033[0m"  # Red
                    
                    # Use stored percentage if available, otherwise estimate
                    if percentage_complete > 0:
                        percentage = percentage_complete
                    else:
                        # If we have total_rows, use that for percentage
                        if total_rows > 0:
                            percentage = min((rows_processed / total_rows) * 100, 99.9) if status != 'completed' else 100.0
                        else:
                            # Fallback to estimations
                            if status == 'completed':
                                percentage = 100.0
                            elif status == 'failed':
                                percentage = rows_processed / 100  # Arbitrary estimate
                            else:  # in_progress
                                # Rough progress estimation based on time and rows
                                time_based = min((duration / 3600), 1.0) * 100  # Assuming 1 hour is complete
                                row_based = min(rows_processed / 1000000, 1.0) * 100  # Assuming 1M rows is complete
                                percentage = max(time_based, row_based)
                                percentage = min(percentage, 99.0)  # Cap at 99% if not completed
                    
                    print(f"\nStep: {step_name}")
                    if process:
                        print(f"Process: {process}")
                    print(f"Status: {status_str}")
                    
                    # Display row counts with percentages
                    if total_rows > 0:
                        print(f"Progress: {rows_processed:,}/{total_rows:,} rows ({percentage:.2f}%)")
                    else:
                        print(f"Rows processed: {rows_processed:,} ({percentage:.2f}%)")
                        
                    print(f"Duration: {hours:02d}:{minutes:02d}:{seconds:02d}")
                    if error_msg:
                        print(f"Message: {error_msg}")
                    
                    # Print progress bar
                    bar_length = 50
                    filled_length = int(percentage / 100 * bar_length)
                    bar = '█' * filled_length + '░' * (bar_length - filled_length)
                    print(f"[{bar}] {percentage:.2f}%")
                
                print("\n" + "="*80)
                
        except Exception as e:
            logger.error(f"Failed to display progress: {e}")

def monitor_etl_progress(db_config, process_name=None, interval=5, continuous=False):
    """Monitor ETL progress in real-time."""
    tracker = ETLProgressTracker(db_config)
    
    try:
        while True:
            os.system('clear')  # Clear screen on each update
            tracker.display_progress(process_name)
            
            if not continuous:
                break
                
            print(f"\nUpdating in {interval} seconds... Press Ctrl+C to exit.")
            time.sleep(interval)
    except KeyboardInterrupt:
        print("\nExiting progress monitor.")

def main():
    parser = argparse.ArgumentParser(description='Monitor ETL progress')
    parser.add_argument('--host', default='localhost', help='Database hostname')
    parser.add_argument('--port', default='5432', help='Database port')
    parser.add_argument('--dbname', default='ohdsi', help='Database name')
    parser.add_argument('--user', default='postgres', help='Database username')
    parser.add_argument('--password', default='acumenus', help='Database password')
    parser.add_argument('--process', help='Filter by process name')
    parser.add_argument('--interval', type=int, default=5, help='Refresh interval in seconds')
    parser.add_argument('--continuous', action='store_true', help='Continuously monitor progress')
    
    args = parser.parse_args()
    
    db_config = {
        'host': args.host,
        'port': args.port,
        'dbname': args.dbname,
        'user': args.user,
        'password': args.password
    }
    
    monitor_etl_progress(db_config, args.process, args.interval, args.continuous)

if __name__ == "__main__":
    main()
