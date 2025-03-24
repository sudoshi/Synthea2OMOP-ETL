#!/usr/bin/env python3
"""
check_etl_progress.py - View the current ETL progress
"""

import os
import sys
import logging
from dotenv import load_dotenv
from tabulate import tabulate
from datetime import datetime

# Add the parent directory to sys.path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

# Import ETL setup utilities
from etl_pipeline.etl_setup import ETLProgressTracker, init_logging, db_config

def format_datetime(dt):
    """Format datetime for display"""
    if not dt:
        return "N/A"
    return dt.strftime("%Y-%m-%d %H:%M:%S")

def format_percentage(pct):
    """Format percentage for display"""
    if pct is None:
        return "0.00%"
    return f"{pct:.2f}%"

def main():
    """Main function to check ETL progress"""
    # Initialize logging
    init_logging()
    
    # Load environment variables
    load_dotenv()
    
    # Update db_config from environment variables
    db_config.update({
        'host': os.getenv('DB_HOST', 'localhost'),
        'port': os.getenv('DB_PORT', '5432'),
        'database': os.getenv('DB_NAME', 'ohdsi'),
        'user': os.getenv('DB_USER', 'postgres'),
        'password': os.getenv('DB_PASSWORD', 'acumenus')
    })
    
    # Initialize progress tracker
    tracker = ETLProgressTracker(db_config)
    
    # Option to rebuild the table
    if len(sys.argv) > 1 and sys.argv[1] == '--rebuild':
        tracker.rebuild_progress_table()
        print("ETL progress table has been rebuilt.")
    
    # Get all progress
    progress_data = tracker.get_all_progress()
    
    if not progress_data:
        print("No ETL progress data found.")
        return
    
    # Prepare data for tabulate
    table_data = []
    for item in progress_data:
        table_data.append([
            f"{item['process_name']}/{item['step_name']}",
            item['status'],
            f"{item['rows_processed']:,}/{item['total_rows']:,}" if item['total_rows'] else f"{item['rows_processed']:,}",
            format_percentage(item['percentage_complete']),
            format_datetime(item['started_at']),
            format_datetime(item['completed_at']),
            item['error_message'] or ""
        ])
    
    # Print the table
    headers = ["Step", "Status", "Progress", "Percent", "Started", "Completed", "Message"]
    print(tabulate(table_data, headers=headers, tablefmt="grid"))

if __name__ == "__main__":
    main()
