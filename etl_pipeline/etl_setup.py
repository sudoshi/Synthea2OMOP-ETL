#!/usr/bin/env python3
"""
etl_setup.py - Shared utilities for database connections, logging, checkpointing, etc.
"""

import os
import sys
import json
import logging
import time
import psycopg2
from psycopg2 import pool
from datetime import datetime
from typing import Any, Dict, Optional, Tuple, List
from pathlib import Path

# Try to import optional dependencies
try:
    from tqdm import tqdm
    tqdm_available = True
except ImportError:
    tqdm_available = False

try:
    from colorama import init, Fore, Style
    init()  # Initialize colorama
    colorama_available = True
except ImportError:
    colorama_available = False

# GLOBALS
connection_pool: Optional[pool.ThreadedConnectionPool] = None
CHECKPOINT_FILE = Path(".synthea_etl_checkpoint.json")

# Default config (override as needed)
db_config = {
    'host': 'localhost',
    'port': '5432',
    'database': 'ohdsi',
    'user': 'postgres',
    'password': 'acumenus'
}

# Configure logging
def init_logging(debug: bool=False) -> None:
    """Initialize logging, optionally with debug level."""
    level = logging.DEBUG if debug else logging.INFO
    logging.basicConfig(
        level=level,
        format="%(asctime)s [%(levelname)s] %(message)s",
        handlers=[logging.StreamHandler(sys.stdout)]
    )
    logging.info("Logging initialized.")

# Database connection handling
def init_db_connection_pool(
    host: str = db_config['host'],
    port: str = db_config['port'],
    database: str = db_config['database'],
    user: str = db_config['user'],
    password: str = db_config['password'],
    minconn: int = 1,
    maxconn: int = 10
) -> None:
    """Create a global connection pool for Postgres."""
    global connection_pool
    
    # Update the global db_config with the provided parameters
    db_config['host'] = host
    db_config['port'] = port
    db_config['database'] = database
    db_config['user'] = user
    db_config['password'] = password
    
    # Check if pool is already initialized
    if connection_pool is not None:
        logging.info("Connection pool already initialized, reusing existing pool")
        return
        
    logging.info("Initializing database connection pool...")
    try:
        connection_pool = pool.ThreadedConnectionPool(
            minconn=minconn,
            maxconn=maxconn,
            host=host,
            port=port,
            database=database,
            user=user,
            password=password
        )
        logging.info(f"Database connection pool initialized: {host}:{port}/{database}")
        
        # Test the connection to ensure it's working
        conn = connection_pool.getconn()
        if conn:
            connection_pool.putconn(conn)
            logging.debug("Connection pool verified with test connection")
    except Exception as e:
        logging.error(f"Failed to initialize DB connection pool: {e}")
        sys.exit(1)

def get_connection() -> psycopg2.extensions.connection:
    """Get a database connection from the pool."""
    global connection_pool
    if not connection_pool:
        raise RuntimeError("Connection pool not initialized. Call init_db_connection_pool first.")
    return connection_pool.getconn()

def release_connection(conn: psycopg2.extensions.connection) -> None:
    """Release a connection back to the pool."""
    global connection_pool
    if connection_pool:
        connection_pool.putconn(conn)

def execute_query(query: str, params: Tuple[Any, ...] = (), fetch: bool=False) -> Any:
    """Helper to execute a query within a borrowed connection."""
    global connection_pool, db_config
    conn = None
    
    # Try to use the connection pool
    try:
        # If connection pool is not initialized, try to create a direct connection
        if connection_pool is None:
            logging.warning("Connection pool not initialized, creating direct connection")
            try:
                # Create a direct connection using db_config
                conn = psycopg2.connect(
                    host=db_config['host'],
                    port=db_config['port'],
                    database=db_config['database'],
                    user=db_config['user'],
                    password=db_config['password']
                )
                conn.autocommit = True
                
                with conn.cursor() as cur:
                    cur.execute(query, params)
                    if fetch:
                        result = cur.fetchall()
                        # Handle both scalar results and array results
                        if result:
                            if isinstance(result[0], (int, float)):
                                # Result is already a scalar
                                return result[0]
                            elif hasattr(result[0], '__getitem__'):
                                # Result is a tuple/list
                                return result[0][0] if len(result[0]) > 0 else 0
                        return 0
                    return True
            except Exception as e:
                if conn:
                    conn.rollback()
                logging.error(f"Error executing query with direct connection: {e}")
                logging.debug(f"Query was: {query}")
                if fetch:
                    return 0  # Default value for count queries
                return False
            finally:
                if conn:
                    conn.close()
        else:
            # Use the connection pool as normal
            try:
                conn = get_connection()
                with conn.cursor() as cur:
                    cur.execute(query, params)
                    if fetch:
                        result = cur.fetchall()
                        conn.commit()
                        # Handle both scalar results and array results
                        if result:
                            if isinstance(result[0], (int, float)):
                                # Result is already a scalar
                                return result[0]
                            elif hasattr(result[0], '__getitem__'):
                                # Result is a tuple/list
                                return result[0][0] if len(result[0]) > 0 else 0
                        return 0
                    conn.commit()
                    return True
            except Exception as e:
                if conn:
                    conn.rollback()
                logging.error(f"Error executing query: {e}")
                logging.debug(f"Query was: {query}")
                if fetch:
                    return 0  # Default value for count queries
                return False
            finally:
                if conn:
                    release_connection(conn)
    except Exception as e:
        logging.error(f"Unexpected error in execute_query: {e}")
        if fetch:
            return 0  # Default value for count queries
        return False

# Checkpoint handling
def load_checkpoint() -> Dict[str, Any]:
    """Load checkpoint from file, or return empty structure."""
    if CHECKPOINT_FILE.exists():
        try:
            with open(CHECKPOINT_FILE, 'r') as f:
                return json.load(f)
        except Exception as e:
            logging.warning(f"Failed to load checkpoint file: {e}")
    return {"completed_steps": [], "stats": {}, "last_updated": None}

def save_checkpoint(checkpoint: Dict[str, Any]) -> None:
    """Save checkpoint to file."""
    checkpoint["last_updated"] = datetime.now().isoformat()
    try:
        with open(CHECKPOINT_FILE, "w") as f:
            json.dump(checkpoint, f, indent=2)
    except Exception as e:
        logging.warning(f"Failed to save checkpoint: {e}")

def mark_step_completed(step_name: str, stats: Dict[str, Any] = None) -> None:
    """Mark a step as completed in the checkpoint."""
    cp = load_checkpoint()
    if step_name not in cp["completed_steps"]:
        cp["completed_steps"].append(step_name)
    if stats:
        if "stats" not in cp:
            cp["stats"] = {}
        cp["stats"][step_name] = stats
    save_checkpoint(cp)
    logging.debug(f"Step completed: {step_name}")

def is_step_completed(step_name: str, force_reprocess: bool = False) -> bool:
    """
    Check if step is completed in the checkpoint.
    
    First checks the checkpoint file, then validates against the database 
    to handle cases where data exists but checkpoint is out of sync.
    """
    # First check if force reprocessing is enabled
    if force_reprocess:
        return False
        
    # Check checkpoint file first
    cp = load_checkpoint()
    if step_name in cp["completed_steps"]:
        return True
        
    # If not in checkpoint, verify against database state
    # This handles cases where data was loaded but checkpoint wasn't updated
    try:
        if step_name == "process_patients":
            count = execute_query("SELECT COUNT(*) FROM omop.person", fetch=True)
            return count > 0
        elif step_name == "process_encounters":
            count = execute_query("SELECT COUNT(*) FROM omop.visit_occurrence", fetch=True)
            return count > 0
        elif step_name == "process_conditions":
            count = execute_query("SELECT COUNT(*) FROM omop.condition_occurrence", fetch=True)
            return count > 0
        elif step_name == "process_medications":
            count = execute_query("SELECT COUNT(*) FROM omop.drug_exposure", fetch=True)
            return count > 0
        elif step_name == "process_procedures":
            count = execute_query("SELECT COUNT(*) FROM omop.procedure_occurrence", fetch=True)
            return count > 0
        elif step_name == "process_observations":
            count1 = execute_query("SELECT COUNT(*) FROM omop.measurement", fetch=True)
            count2 = execute_query("SELECT COUNT(*) FROM omop.observation", fetch=True)
            return count1 > 0 or count2 > 0
        elif step_name == "create_observation_periods":
            count = execute_query("SELECT COUNT(*) FROM omop.observation_period", fetch=True)
            return count > 0
        elif step_name == "map_source_to_standard_concepts":
            # This is more complex - check if concept mapping has been applied
            # For simplicity, assume its state follows the other steps
            return False
    except Exception as e:
        logging.warning(f"Error verifying step completion state in database: {e}")
        
    # Default to checkpoint file state
    return False

# CSV loading utilities
def count_csv_rows(csv_path: str) -> int:
    """Count the number of rows in a CSV file (excluding header).
    Uses wc -l for efficiency with large files.
    """
    import subprocess
    import os
    
    if not os.path.exists(csv_path):
        raise FileNotFoundError(f"CSV file not found: {csv_path}")
        
    try:
        # Use wc -l for much faster line counting
        result = subprocess.run(['wc', '-l', csv_path], capture_output=True, text=True, check=True)
        # wc -l counts include header, so subtract 1
        return int(result.stdout.strip().split()[0]) - 1
    except (subprocess.SubprocessError, ValueError, IndexError) as e:
        logging.warning(f"Error using wc -l to count rows, falling back to Python method: {e}")
        # Fall back to Python method if wc fails
        with open(csv_path, 'r') as f:
            # Skip header
            next(f, None)
            return sum(1 for _ in f)

def load_csv_to_temp_table(csv_path: str, temp_table: str, batch_size: int = 1000) -> int:
    """
    Load CSV data into a temporary table in batches with progress reporting.
    Returns the number of rows loaded.
    """
    # Count rows for progress tracking
    total_rows = count_csv_rows(csv_path)
    logging.info(f"Found {total_rows:,} rows in {csv_path}")
    
    # Create progress bar if tqdm is available
    if tqdm_available:
        progress_bar = tqdm(total=total_rows, desc=f"Loading {temp_table}", unit="rows")
    else:
        progress_bar = None
    
    # Process in batches
    rows_loaded = 0
    conn = get_connection()
    try:
        with conn.cursor() as cursor:
            # First, determine columns from CSV header
            with open(csv_path, 'r', newline='') as f:
                header = next(f).strip().split(',')
                
            # Create temp table with appropriate columns
            columns = [f'"{col}" TEXT' for col in header]
            create_sql = f"""
            CREATE TEMP TABLE IF NOT EXISTS {temp_table} (
                {', '.join(columns)}
            );
            """
            cursor.execute(create_sql)
            
            # Prepare COPY statement
            copy_sql = f"""
            COPY {temp_table} FROM STDIN WITH (FORMAT CSV, HEADER);
            """
            
            # Use COPY for efficient loading
            with open(csv_path, 'r') as f:
                cursor.copy_expert(copy_sql, f)
            
            # Get row count
            cursor.execute(f"SELECT COUNT(*) FROM {temp_table}")
            rows_loaded = cursor.fetchone()[0]
            
            conn.commit()
            
            if progress_bar:
                progress_bar.update(rows_loaded)
                progress_bar.close()
            
            logging.info(f"Loaded {rows_loaded:,} rows into {temp_table}")
            
    except Exception as e:
        conn.rollback()
        logging.error(f"Error loading CSV to temp table: {e}")
        raise
    finally:
        release_connection(conn)
        
    return rows_loaded

# Progress bar utilities
def create_progress_bar(total: int, description: str = "Processing") -> Optional[Any]:
    """Create a progress bar if tqdm is available."""
    if tqdm_available:
        return tqdm(total=total, desc=description, unit="rows")
    return None

def update_progress_bar(progress_bar: Optional[Any], increment: int = 1) -> None:
    """Update a progress bar if it exists."""
    if progress_bar and tqdm_available:
        progress_bar.update(increment)

def close_progress_bar(progress_bar: Optional[Any]) -> None:
    """Close a progress bar if it exists."""
    if progress_bar and tqdm_available:
        progress_bar.close()

# Console output formatting
class ColoredFormatter:
    """Format console output with colors if colorama is available."""
    
    @staticmethod
    def info(message: str) -> str:
        """Format an info message."""
        if colorama_available:
            return f"{Fore.BLUE}{message}{Style.RESET_ALL}"
        return message
    
    @staticmethod
    def success(message: str) -> str:
        """Format a success message."""
        if colorama_available:
            return f"{Fore.GREEN}{message}{Style.RESET_ALL}"
        return message
    
    @staticmethod
    def warning(message: str) -> str:
        """Format a warning message."""
        if colorama_available:
            return f"{Fore.YELLOW}{message}{Style.RESET_ALL}"
        return message
    
    @staticmethod
    def error(message: str) -> str:
        """Format an error message."""
        if colorama_available:
            return f"{Fore.RED}{message}{Style.RESET_ALL}"
        return message

# ETL Progress Tracking
class ETLProgressTracker:
    """Track and report progress of ETL operations."""
    
    def __init__(self, db_config=None):
        """Initialize the progress tracker with database connection."""
        self.db_config = db_config or db_config
        self.conn = None
        self.initialize_connection()
        self.ensure_progress_table()
        
    def initialize_connection(self):
        """Initialize database connection using the connection pool."""
        try:
            # Try to use the connection pool first
            try:
                self.conn = get_connection()
                self.conn.autocommit = True
                logging.debug("ETL Progress Tracker: Database connection initialized from pool")
            except Exception as pool_error:
                # Fall back to direct connection if pool is not available
                logging.warning(f"Connection pool not initialized, creating direct connection: {pool_error}")
                self.conn = psycopg2.connect(**self.db_config)
                self.conn.autocommit = True
                logging.debug("ETL Progress Tracker: Direct database connection initialized")
        except Exception as e:
            logging.error(f"ETL Progress Tracker: Failed to initialize database connection: {e}")
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
                    
                    # Create the progress table with the schema that matches the existing database
                    cursor.execute("""
                    CREATE TABLE IF NOT EXISTS staging.etl_progress (
                        id SERIAL PRIMARY KEY,
                        process_name VARCHAR(50) NOT NULL,
                        step_name VARCHAR(100) NOT NULL,
                        status VARCHAR(20) DEFAULT 'in_progress',
                        rows_processed BIGINT DEFAULT 0,
                        total_rows BIGINT DEFAULT 0,
                        percentage_complete NUMERIC(5,2) DEFAULT 0,
                        error_message TEXT,
                        started_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        completed_at TIMESTAMP,
                        UNIQUE(process_name, step_name)
                    );
                    """)
                    logging.debug("ETL progress table created with compatibility schema")
                logging.debug("ETL progress table created/verified")
        except Exception as e:
            logging.error(f"Failed to create progress table: {e}")
            sys.exit(1)
    
    def start_step(self, process_name, step_name, total_items=None, message=None):
        """Register the start of an ETL step."""
        try:
            with self.conn.cursor() as cursor:
                # Use the total_items if provided
                total_rows = total_items if total_items is not None else 0
                
                cursor.execute("""
                INSERT INTO staging.etl_progress 
                    (process_name, step_name, status, rows_processed, total_rows, percentage_complete, error_message, started_at)
                VALUES 
                    (%s, %s, 'in_progress', 0, %s, 0, %s, CURRENT_TIMESTAMP)
                ON CONFLICT (process_name, step_name) 
                DO UPDATE SET 
                    status = 'in_progress',
                    started_at = CURRENT_TIMESTAMP,
                    rows_processed = 0,
                    total_rows = %s,
                    percentage_complete = 0,
                    error_message = COALESCE(%s, staging.etl_progress.error_message)
                """, (process_name, step_name, total_rows, message, total_rows, message))
                logging.debug(f"Started ETL step: {process_name}/{step_name}")
        except Exception as e:
            logging.error(f"Failed to start ETL step: {e}")
    
    def update_progress(self, process_name, step_name, processed_items, total_items=None, message=None):
        """Update the progress of an ETL step."""
        try:
            with self.conn.cursor() as cursor:
                # Calculate percentage complete if total_items is provided
                percentage = 0
                if total_items and total_items > 0:
                    percentage = round((processed_items / total_items) * 100, 2)
                
                cursor.execute("""
                UPDATE staging.etl_progress 
                SET 
                    rows_processed = %s,
                    total_rows = COALESCE(%s, total_rows),
                    percentage_complete = %s,
                    error_message = COALESCE(%s, error_message)
                WHERE 
                    process_name = %s AND step_name = %s
                """, (processed_items, total_items, percentage, message, process_name, step_name))
                logging.debug(f"Updated ETL step progress: {process_name}/{step_name} - {processed_items} items processed")
        except Exception as e:
            logging.error(f"Failed to update ETL step progress: {e}")
    
    def complete_step(self, process_name, step_name, success=True, message=None):
        """Mark an ETL step as completed."""
        try:
            with self.conn.cursor() as cursor:
                status = "completed" if success else "failed"
                
                cursor.execute("""
                UPDATE staging.etl_progress 
                SET 
                    status = %s,
                    completed_at = CURRENT_TIMESTAMP,
                    error_message = COALESCE(%s, error_message)
                WHERE 
                    process_name = %s AND step_name = %s
                """, (status, message, process_name, step_name))
                logging.debug(f"Completed ETL step: {process_name}/{step_name} with status {status}")
        except Exception as e:
            logging.error(f"Failed to complete ETL step: {e}")
    
    def get_step_progress(self, process_name, step_name):
        """Get the current progress of an ETL step."""
        try:
            with self.conn.cursor() as cursor:
                cursor.execute("""
                SELECT 
                    rows_processed, 
                    total_rows, 
                    percentage_complete, 
                    status,
                    started_at,
                    completed_at,
                    error_message
                FROM staging.etl_progress 
                WHERE process_name = %s AND step_name = %s
                """, (process_name, step_name))
                result = cursor.fetchone()
                if result:
                    return {
                        "rows_processed": result[0],
                        "total_rows": result[1],
                        "percentage_complete": result[2],
                        "status": result[3],
                        "started_at": result[4],
                        "completed_at": result[5],
                        "error_message": result[6]
                    }
                return None
        except Exception as e:
            logging.error(f"Failed to get ETL step progress: {e}")
            return None
    
    def rebuild_progress_table(self):
        """Rebuild the progress tracking table."""
        try:
            with self.conn.cursor() as cursor:
                cursor.execute("DROP TABLE IF EXISTS staging.etl_progress")
                self.ensure_progress_table()
                logging.info("ETL progress tracking table has been rebuilt")
                return True
        except Exception as e:
            logging.error(f"Failed to rebuild progress table: {e}")
            return False
            
    def get_all_progress(self):
        """Get progress for all ETL steps."""
        try:
            with self.conn.cursor() as cursor:
                cursor.execute("""
                SELECT 
                    process_name,
                    step_name,
                    status,
                    rows_processed, 
                    total_rows, 
                    percentage_complete,
                    started_at,
                    completed_at,
                    error_message
                FROM staging.etl_progress 
                ORDER BY started_at DESC
                """)
                results = cursor.fetchall()
                if results:
                    return [
                        {
                            "process_name": row[0],
                            "step_name": row[1],
                            "status": row[2],
                            "rows_processed": row[3],
                            "total_rows": row[4],
                            "percentage_complete": row[5],
                            "started_at": row[6],
                            "completed_at": row[7],
                            "error_message": row[8]
                        } for row in results
                    ]
                return []
        except Exception as e:
            logging.error(f"Failed to get all ETL progress: {e}")
            return []
