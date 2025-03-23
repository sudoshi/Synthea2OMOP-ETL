#!/usr/bin/env python3
"""
Synthea2OMOP-ETL Diagnostic and Fix Utility

This script helps diagnose and fix issues with the ETL process:
1. Checks the database status
2. Verifies ETL progress tracking
3. Provides options to reset/restart the ETL process
4. Fixes common issues with checkpoints and progress tracking
"""

import os
import sys
import psycopg2
import json
import argparse
import subprocess
from pathlib import Path

# Add project root to sys.path for imports
PROJECT_ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

# Try to import from config_loader if available
try:
    from utils.config_loader import ConfigLoader
    config_loader_available = True
except ImportError:
    config_loader_available = False

# Constants and default values
CHECKPOINT_FILE = PROJECT_ROOT / ".synthea_etl_checkpoint.json"
DEFAULT_CONFIG = {
    'host': 'localhost',
    'port': '5432',
    'database': 'ohdsi',
    'user': 'postgres',
    'password': 'postgres'
}

def get_db_config():
    """Get database configuration from environment or config."""
    config = DEFAULT_CONFIG.copy()
    
    if config_loader_available:
        cl = ConfigLoader()
        config = {
            'host': cl.get_env('DB_HOST', config['host']),
            'port': cl.get_env('DB_PORT', config['port']),
            'database': cl.get_env('DB_NAME', config['database']),
            'user': cl.get_env('DB_USER', config['user']),
            'password': cl.get_env('DB_PASSWORD', config['password'])
        }
    else:
        # Fall back to environment variables
        config = {
            'host': os.environ.get('DB_HOST', config['host']),
            'port': os.environ.get('DB_PORT', config['port']),
            'database': os.environ.get('DB_NAME', config['database']),
            'user': os.environ.get('DB_USER', config['user']),
            'password': os.environ.get('DB_PASSWORD', config['password'])
        }
    
    return config

def get_db_connection():
    """Create and return a database connection."""
    config = get_db_config()
    
    try:
        conn = psycopg2.connect(
            host=config['host'],
            port=config['port'],
            database=config['database'],
            user=config['user'],
            password=config['password']
        )
        conn.autocommit = True
        return conn
    except Exception as e:
        print(f"Error connecting to database: {e}")
        return None

def check_db_status():
    """Check if the database is in recovery mode."""
    conn = get_db_connection()
    if not conn:
        return False, "Could not connect to database"
    
    try:
        with conn.cursor() as cursor:
            cursor.execute("SELECT pg_is_in_recovery()")
            in_recovery = cursor.fetchone()[0]
            
            if in_recovery:
                return False, "Database is in recovery mode"
            
            cursor.execute("SELECT datname, usename, state FROM pg_stat_activity")
            activities = cursor.fetchall()
            
            print("\nDatabase Activity:")
            for activity in activities:
                print(f"  - Database: {activity[0]}, User: {activity[1]}, State: {activity[2]}")
            
            return True, "Database is operational"
    except Exception as e:
        return False, f"Error checking database status: {e}"
    finally:
        conn.close()

def check_etl_progress():
    """Check the ETL progress tracking table."""
    conn = get_db_connection()
    if not conn:
        return False, "Could not connect to database"
    
    try:
        with conn.cursor() as cursor:
            # First check if the table exists
            cursor.execute("""
            SELECT EXISTS (
                SELECT FROM information_schema.tables 
                WHERE table_schema = 'staging' 
                AND table_name = 'etl_progress'
            )
            """)
            table_exists = cursor.fetchone()[0]
            
            if not table_exists:
                return False, "ETL progress tracking table does not exist"
            
            # Get ETL progress
            cursor.execute("""
            SELECT step_name, status, rows_processed, error_message, 
                   started_at, completed_at
            FROM staging.etl_progress
            ORDER BY started_at
            """)
            progress = cursor.fetchall()
            
            if not progress:
                return False, "No ETL progress data found in the tracking table"
            
            print("\nETL Progress:")
            print("{:<30} {:<15} {:<10} {:<20} {:<20}".format(
                "Step", "Status", "Rows", "Started", "Completed"))
            print("-" * 100)
            
            for step in progress:
                print("{:<30} {:<15} {:<10} {:<20} {:<20}".format(
                    step[0], step[1], step[2] or 0, 
                    str(step[4] or "N/A"), str(step[5] or "N/A")))
            
            return True, f"Found {len(progress)} ETL progress records"
    except Exception as e:
        return False, f"Error checking ETL progress: {e}"
    finally:
        conn.close()

def check_checkpoint_file():
    """Check the ETL checkpoint file."""
    if not CHECKPOINT_FILE.exists():
        return False, "Checkpoint file does not exist"
    
    try:
        with open(CHECKPOINT_FILE, 'r') as f:
            checkpoint = json.load(f)
        
        completed_steps = checkpoint.get('completed_steps', [])
        last_updated = checkpoint.get('last_updated', None)
        stats = checkpoint.get('stats', {})
        
        print("\nCheckpoint Information:")
        print(f"Last Updated: {last_updated}")
        print(f"Completed Steps: {len(completed_steps)}")
        
        for step in completed_steps:
            stats_for_step = stats.get(step, {})
            count = stats_for_step.get('count', 'N/A')
            print(f"  - {step}: {count} records")
        
        return True, f"Checkpoint file is valid with {len(completed_steps)} completed steps"
    except Exception as e:
        return False, f"Error reading checkpoint file: {e}"

def reset_checkpoint():
    """Delete the checkpoint file to force reprocessing."""
    if CHECKPOINT_FILE.exists():
        try:
            os.remove(CHECKPOINT_FILE)
            return True, "Checkpoint file has been removed"
        except Exception as e:
            return False, f"Error removing checkpoint file: {e}"
    else:
        return True, "Checkpoint file does not exist, nothing to remove"

def reset_etl_progress():
    """Reset the ETL progress tracking table."""
    conn = get_db_connection()
    if not conn:
        return False, "Could not connect to database"
    
    try:
        with conn.cursor() as cursor:
            cursor.execute("TRUNCATE TABLE staging.etl_progress")
            return True, "ETL progress tracking table has been reset"
    except Exception as e:
        return False, f"Error resetting ETL progress: {e}"
    finally:
        conn.close()

def kill_running_queries():
    """Kill any running ETL queries."""
    conn = get_db_connection()
    if not conn:
        return False, "Could not connect to database"
    
    try:
        with conn.cursor() as cursor:
            # Find any long-running queries associated with our process
            cursor.execute("""
            SELECT pid, now() - query_start as duration, query 
            FROM pg_stat_activity 
            WHERE state = 'active' 
              AND now() - query_start > interval '1 minute'
              AND query ILIKE '%temp_%' OR query ILIKE '%staging.%' OR query ILIKE '%omop.%'
            """)
            
            long_queries = cursor.fetchall()
            
            if not long_queries:
                return True, "No long-running ETL queries found"
            
            print("\nLong Running Queries:")
            for query in long_queries:
                pid, duration, sql = query
                print(f"  - PID: {pid}, Duration: {duration}")
                print(f"    Query: {sql[:100]}...")
                
                # Optional: Kill the query
                if input(f"Kill query with PID {pid}? (y/n): ").lower() == 'y':
                    cursor.execute(f"SELECT pg_terminate_backend({pid})")
                    print(f"  ✓ Terminated query with PID {pid}")
            
            return True, f"Found {len(long_queries)} long-running queries"
    except Exception as e:
        return False, f"Error managing running queries: {e}"
    finally:
        conn.close()

def rebuild_progress_table():
    """Recreate the ETL progress tracking table."""
    conn = get_db_connection()
    if not conn:
        return False, "Could not connect to database"
    
    try:
        with conn.cursor() as cursor:
            # First check if the table exists and drop it
            cursor.execute("""
            DROP TABLE IF EXISTS staging.etl_progress;
            """)
            
            # Create the table
            cursor.execute("""
            CREATE TABLE IF NOT EXISTS staging.etl_progress (
                id SERIAL PRIMARY KEY,
                step_name VARCHAR(255) NOT NULL,
                status VARCHAR(50) DEFAULT 'pending',
                rows_processed INTEGER DEFAULT 0,
                error_message TEXT,
                started_at TIMESTAMP DEFAULT NULL,
                completed_at TIMESTAMP DEFAULT NULL
            );
            """)
            
            # Create index on step_name
            cursor.execute("""
            CREATE INDEX IF NOT EXISTS idx_etl_progress_step_name ON staging.etl_progress (step_name);
            """)
            
            return True, "ETL progress tracking table has been rebuilt"
    except Exception as e:
        return False, f"Error rebuilding progress table: {e}"
    finally:
        conn.close()

def main():
    """Main function."""
    parser = argparse.ArgumentParser(description="Diagnose and fix Synthea2OMOP ETL issues")
    
    parser.add_argument("--check-db", action="store_true", help="Check database status")
    parser.add_argument("--check-progress", action="store_true", help="Check ETL progress")
    parser.add_argument("--check-checkpoint", action="store_true", help="Check checkpoint file")
    parser.add_argument("--reset-checkpoint", action="store_true", help="Reset checkpoint file")
    parser.add_argument("--reset-progress", action="store_true", help="Reset ETL progress tracking")
    parser.add_argument("--kill-queries", action="store_true", help="Kill running ETL queries")
    parser.add_argument("--rebuild-progress", action="store_true", help="Rebuild progress tracking table")
    parser.add_argument("--full-reset", action="store_true", help="Perform full ETL reset (checkpoint + progress)")
    
    args = parser.parse_args()
    
    # If no arguments, show all information
    if not any(vars(args).values()):
        args.check_db = True
        args.check_progress = True
        args.check_checkpoint = True
    
    # Perform requested operations
    if args.check_db:
        status, message = check_db_status()
        print(f"\nDatabase Status: {'✅' if status else '❌'} {message}")
    
    if args.check_progress:
        status, message = check_etl_progress()
        print(f"\nETL Progress Status: {'✅' if status else '❌'} {message}")
    
    if args.check_checkpoint:
        status, message = check_checkpoint_file()
        print(f"\nCheckpoint Status: {'✅' if status else '❌'} {message}")
    
    if args.reset_checkpoint:
        status, message = reset_checkpoint()
        print(f"\nReset Checkpoint: {'✅' if status else '❌'} {message}")
    
    if args.reset_progress:
        status, message = reset_etl_progress()
        print(f"\nReset ETL Progress: {'✅' if status else '❌'} {message}")
    
    if args.kill_queries:
        status, message = kill_running_queries()
        print(f"\nKill Queries: {'✅' if status else '❌'} {message}")
    
    if args.rebuild_progress:
        status, message = rebuild_progress_table()
        print(f"\nRebuild Progress Table: {'✅' if status else '❌'} {message}")
    
    if args.full_reset:
        print("\nPerforming full ETL reset...")
        
        status1, message1 = reset_checkpoint()
        print(f"Reset Checkpoint: {'✅' if status1 else '❌'} {message1}")
        
        status2, message2 = reset_etl_progress()
        print(f"Reset ETL Progress: {'✅' if status2 else '❌'} {message2}")
        
        status3, message3 = kill_running_queries()
        print(f"Kill Queries: {'✅' if status3 else '❌'} {message3}")
        
        status4, message4 = rebuild_progress_table()
        print(f"Rebuild Progress Table: {'✅' if status4 else '❌'} {message4}")
        
        print(f"\nFull ETL Reset: {'✅' if all([status1, status2, status3, status4]) else '❌'} Complete")

if __name__ == "__main__":
    main()
