#!/usr/bin/env python3
"""
add_etl_progress.py - Manually add an ETL progress entry
"""

import os
import sys
import logging
import argparse
from dotenv import load_dotenv

# Add the parent directory to sys.path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

# Import ETL setup utilities
from etl_pipeline.etl_setup import ETLProgressTracker, init_logging, db_config

def main():
    """Main function to add ETL progress entry"""
    parser = argparse.ArgumentParser(description='Add ETL progress entry')
    parser.add_argument('--process', required=True, help='Process name')
    parser.add_argument('--step', required=True, help='Step name')
    parser.add_argument('--status', choices=['in_progress', 'completed', 'failed'], default='in_progress', help='Status')
    parser.add_argument('--processed', type=int, default=0, help='Number of processed items')
    parser.add_argument('--total', type=int, default=0, help='Total number of items')
    parser.add_argument('--message', help='Message or error message')
    args = parser.parse_args()
    
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
    
    # Start the step
    tracker.start_step(args.process, args.step, args.total, args.message)
    
    # Update progress if needed
    if args.processed > 0:
        tracker.update_progress(args.process, args.step, args.processed, args.total, args.message)
    
    # Complete the step if needed
    if args.status != 'in_progress':
        success = args.status == 'completed'
        tracker.complete_step(args.process, args.step, success, args.message)
    
    print(f"Added ETL progress entry: {args.process}/{args.step} ({args.status})")

if __name__ == "__main__":
    main()
