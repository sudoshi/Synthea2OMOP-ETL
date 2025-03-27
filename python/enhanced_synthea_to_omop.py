#!/usr/bin/env python3
"""
enhanced_synthea_to_omop.py - Enhanced ETL process for Synthea to OMOP CDM

This script provides a robust, resumable ETL process with:
1. Direct loading from Synthea CSV files to OMOP tables
2. Comprehensive progress tracking and reporting
3. Detailed validation at each step
4. Error handling and recovery
5. Performance optimizations

Usage Example:
  ./enhanced_synthea_to_omop.py \
    --synthea-dir /path/to/synthea/output \
    --reset-tables \
    --track-progress

Optional arguments and details can be seen via --help
"""

import argparse
import csv
import logging
import os
import sys
import time
import psycopg2
import concurrent.futures  # Not currently used, but available if you want to parallelize steps
from datetime import datetime
from pathlib import Path
import json
from typing import Dict, List, Any, Tuple, Optional
from psycopg2 import pool

# Define project root for proper path references
PROJECT_ROOT = Path(__file__).parent.parent

# Try to import optional dependencies
try:
    import pandas as pd
    pandas_available = True
except ImportError:
    pandas_available = False

try:
    from tqdm import tqdm
    tqdm_available = True
except ImportError:
    tqdm_available = False

try:
    from colorama import init, Fore, Style
    init()  # Initialize colorama for colored console output
    colorama_available = True
except ImportError:
    colorama_available = False

# Try to import ETL progress tracker (if you have a separate module)
try:
    # Now looking in the same directory as this script
    from .etl_progress_tracking import ETLProgressTracker
    progress_tracker_available = True
except ImportError:
    # Fallback to absolute import in case script is run directly
    try:
        from etl_progress_tracking import ETLProgressTracker
        progress_tracker_available = True
    except ImportError:
        progress_tracker_available = False

# Try to import config loader (if you have a separate module for config)
try:
    # Try relative import from project root
    sys.path.insert(0, str(PROJECT_ROOT))
    from utils.config_loader import ConfigLoader
    config_loader_available = True
except ImportError:
    config_loader_available = False

# ---------------------------
# Global/Module-Level Setup
# ---------------------------

# Set up logging
log_dir = PROJECT_ROOT / "logs"
os.makedirs(log_dir, exist_ok=True)
log_file = log_dir / f"synthea_etl_{time.strftime('%Y%m%d_%H%M%S')}.log"
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler(log_file),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

# Constants and defaults
CHECKPOINT_FILE = PROJECT_ROOT / ".synthea_etl_checkpoint.json"

REQUIRED_SYNTHEA_FILES = [
    "patients.csv",
    "encounters.csv",
    "conditions.csv",
    "observations.csv",
    "procedures.csv",
    "medications.csv"
]

# Default Database configuration (can be overridden by env vars or config loader)
db_config = {
    'host': 'localhost',
    'port': '5432',
    'database': 'ohdsi',
    'user': 'postgres',
    'password': 'acumenus'
}

# Global variables for connections and trackers
connection_pool: Optional[pool.ThreadedConnectionPool] = None
config: Dict[str, str] = {}
progress_tracker: Optional["ETLProgressTracker"] = None  # type: ignore

# ---------------------------
# Colored Console Helper
# ---------------------------

class ColoredFormatter:
    """Helper class for colored console output."""
    
    @staticmethod
    def info(message: str) -> str:
        if colorama_available:
            return f"{Fore.CYAN}{message}{Style.RESET_ALL}"
        return message
    
    @staticmethod
    def success(message: str) -> str:
        if colorama_available:
            return f"{Fore.GREEN}{message}{Style.RESET_ALL}"
        return message
    
    @staticmethod
    def warning(message: str) -> str:
        if colorama_available:
            return f"{Fore.YELLOW}{message}{Style.RESET_ALL}"
        return message
    
    @staticmethod
    def error(message: str) -> str:
        if colorama_available:
            return f"{Fore.RED}{message}{Style.RESET_ALL}"
        return message
    
    @staticmethod
    def highlight(message: str) -> str:
        if colorama_available:
            return f"{Fore.WHITE}{Fore.BLUE}{message}{Style.RESET_ALL}"
        return message

# ---------------------------
# Argument Parsing
# ---------------------------

def parse_arguments() -> argparse.Namespace:
    """Parse command line arguments for the ETL."""
    parser = argparse.ArgumentParser(description='Enhanced Synthea to OMOP ETL Process')
    
    default_synthea_dir = str(PROJECT_ROOT / 'synthea-output')
    parser.add_argument('--synthea-dir', type=str, default=default_synthea_dir,
                        help=f'Directory containing Synthea output files (default: {default_synthea_dir})')
    parser.add_argument('--resume', action='store_true',
                        help='Resume from last checkpoint')
    parser.add_argument('--force', action='store_true',
                        help='Force execution even if validation fails')
    parser.add_argument('--force-reprocess', action='store_true',
                        help='Force reprocessing of all steps by clearing checkpoint')
    parser.add_argument('--debug', action='store_true',
                        help='Enable debug logging')
    parser.add_argument('--batch-size', type=int, default=50000,
                        help='Batch size for loading data (default: 50000)')
    parser.add_argument('--skip-validation', action='store_true',
                        help='Skip validation steps')
    parser.add_argument('--max-workers', type=int, default=4,
                        help='Maximum number of parallel workers (default: 4) - Not currently used in code')
    parser.add_argument('--track-progress', action='store_true',
                        help='Enable progress tracking (requires etl_progress_tracking module)')
    parser.add_argument('--reset-tables', action='store_true',
                        help='Reset OMOP tables before loading (truncate and remove old data)')
    parser.add_argument('--skip-concept-mapping', action='store_true',
                        help='Skip concept mapping step')
    parser.add_argument('--direct-import-observations', action='store_true',
                        help='Directly import observations.csv to omop.observation table without processing')
    
    return parser.parse_args()

def setup_logging(debug: bool=False) -> None:
    """Set logging level based on 'debug' flag."""
    if debug:
        logging.getLogger().setLevel(logging.DEBUG)
        logger.debug("Debug logging enabled")
    else:
        logging.getLogger().setLevel(logging.INFO)

# ---------------------------
# Checkpoint Handling
# ---------------------------

def load_checkpoint() -> Dict[str, Any]:
    """Load checkpoint data from file."""
    # Check if we're forcing reprocessing via command line arg
    if 'args' in globals() and hasattr(args, 'force_reprocess') and args.force_reprocess:
        logger.info("Force reprocessing requested - ignoring checkpoint file")
        return {
            'completed_steps': [],
            'last_updated': None,
            'stats': {}
        }
        
    if CHECKPOINT_FILE.exists():
        try:
            with open(CHECKPOINT_FILE, 'r') as f:
                return json.load(f)
        except Exception as e:
            logger.warning(f"Failed to load checkpoint file: {e}")
    return {
        'completed_steps': [],
        'last_updated': None,
        'stats': {}
    }

def save_checkpoint(checkpoint_data: Dict[str, Any]) -> None:
    """Save checkpoint data to file."""
    checkpoint_data['last_updated'] = datetime.now().isoformat()
    try:
        with open(CHECKPOINT_FILE, 'w') as f:
            json.dump(checkpoint_data, f, indent=2)
    except Exception as e:
        logger.warning(f"Failed to save checkpoint file: {e}")

def mark_step_completed(step_name: str, stats: Optional[Dict[str, Any]]=None) -> None:
    """Mark a step as completed in the checkpoint file."""
    checkpoint = load_checkpoint()
    if step_name not in checkpoint['completed_steps']:
        checkpoint['completed_steps'].append(step_name)
    
    if stats:
        if 'stats' not in checkpoint:
            checkpoint['stats'] = {}
        checkpoint['stats'][step_name] = stats
    
    save_checkpoint(checkpoint)
    logger.debug(f"Marked step '{step_name}' as completed")

def is_step_completed(step_name: str) -> bool:
    """Check if a step is already completed.
    
    First checks the checkpoint file, then validates against the database 
    to handle cases where data exists but checkpoint is out of sync.
    """
    # First check if force reprocessing is enabled
    if 'args' in globals() and hasattr(args, 'force_reprocess') and args.force_reprocess:
        return False
        
    # Check checkpoint file first
    checkpoint = load_checkpoint()
    if step_name in checkpoint['completed_steps']:
        return True
        
    # If not in checkpoint, verify against database state
    # This handles cases where data was loaded but checkpoint wasn't updated
    try:
        if step_name == "process_patients":
            count = execute_query("SELECT COUNT(*) FROM omop.person", fetch=True)[0][0]
            return count > 0
        elif step_name == "process_encounters":
            count = execute_query("SELECT COUNT(*) FROM omop.visit_occurrence", fetch=True)[0][0]
            return count > 0
        elif step_name == "process_conditions":
            count = execute_query("SELECT COUNT(*) FROM omop.condition_occurrence", fetch=True)[0][0]
            return count > 0
        elif step_name == "process_medications":
            count = execute_query("SELECT COUNT(*) FROM omop.drug_exposure", fetch=True)[0][0]
            return count > 0
        elif step_name == "process_procedures":
            count = execute_query("SELECT COUNT(*) FROM omop.procedure_occurrence", fetch=True)[0][0]
            return count > 0
        elif step_name == "process_observations":
            count1 = execute_query("SELECT COUNT(*) FROM omop.measurement", fetch=True)[0][0]
            count2 = execute_query("SELECT COUNT(*) FROM omop.observation", fetch=True)[0][0]
            return count1 > 0 or count2 > 0
        elif step_name == "create_observation_periods":
            count = execute_query("SELECT COUNT(*) FROM omop.observation_period", fetch=True)[0][0]
            return count > 0
        elif step_name == "map_source_to_standard_concepts":
            # This is more complex - check if concept mapping has been applied
            # For simplicity, assume its state follows the other steps
            return False
    except Exception as e:
        logger.warning(f"Error verifying step completion state in database: {e}")
        
    # Default to checkpoint file state
    return False

# ---------------------------
# Database Connection Handling
# ---------------------------

def initialize_database_connection(args: argparse.Namespace) -> bool:
    """
    Initialize the database connection pool.
    Optionally create a progress tracker if requested.
    """
    global connection_pool, config, progress_tracker
    
    try:
        # If you have a config loader, you can load from that or from env:
        if config_loader_available:
            # Load config via your custom loader
            cl = ConfigLoader()
            config = {
                'host': cl.get_env('DB_HOST', db_config['host']),
                'port': cl.get_env('DB_PORT', db_config['port']),
                'database': cl.get_env('DB_NAME', db_config['database']),
                'user': cl.get_env('DB_USER', db_config['user']),
                'password': cl.get_env('DB_PASSWORD', db_config['password'])
            }
        else:
            # Otherwise, use environment vars or fallback to db_config
            config = {
                'host': os.environ.get('DB_HOST', db_config['host']),
                'port': os.environ.get('DB_PORT', db_config['port']),
                'database': os.environ.get('DB_NAME', db_config['database']),
                'user': os.environ.get('DB_USER', db_config['user']),
                'password': os.environ.get('DB_PASSWORD', db_config['password'])
            }
        
        # Create connection pool
        connection_pool = pool.ThreadedConnectionPool(
            minconn=1,
            maxconn=20,  # Adjust maxconn based on your concurrency needs
            **config
        )
        
        # Initialize progress tracker if requested and available
        if progress_tracker_available and args.track_progress:
            progress_tracker = ETLProgressTracker({
                'host': config['host'],
                'port': config['port'],
                'dbname': config['database'],
                'user': config['user'],
                'password': config['password']
            })
            logger.info("ETL progress tracking initialized")
        
        logger.info(f"Database connection pool initialized: "
                    f"{config['host']}:{config['port']}/{config['database']}")
        return True
    except Exception as e:
        logger.error(f"Failed to initialize database connection: {e}")
        return False

def get_connection() -> psycopg2.extensions.connection:
    """Get a connection from the pool."""
    global connection_pool
    try:
        return connection_pool.getconn()
    except Exception as e:
        logger.error(f"Failed to get database connection: {e}")
        raise

def release_connection(conn: psycopg2.extensions.connection) -> None:
    """Release a connection back to the pool."""
    global connection_pool
    try:
        connection_pool.putconn(conn)
    except Exception as e:
        logger.error(f"Failed to release database connection: {e}")

def execute_query(query: str, params: Optional[Tuple[Any, ...]]=None,
                  fetch: bool=False, conn: Optional[psycopg2.extensions.connection]=None) -> Any:
    """Execute a SQL query with optional parameters and optional fetch."""
    close_conn = False
    try:
        if conn is None:
            conn = get_connection()
            close_conn = True
        
        with conn.cursor() as cursor:
            cursor.execute(query, params)
            if fetch:
                results = cursor.fetchall()
                conn.commit()
                return results
            conn.commit()
            return True
    except Exception as e:
        if conn:
            conn.rollback()
        logger.error(f"Query execution failed: {e}")
        logger.debug(f"Failed query: {query}")
        if params:
            logger.debug(f"Parameters: {params}")
        raise
    finally:
        if close_conn and conn:
            release_connection(conn)

# ---------------------------
# Validation of Synthea Files
# ---------------------------

def validate_synthea_files(synthea_dir: str) -> Tuple[bool, Dict[str, Dict[str, Any]]]:
    """
    Validate whether required Synthea files exist and have correct format.
    Returns a tuple (is_valid, file_stats).
    """
    print(ColoredFormatter.info(f"\n🔍 Validating Synthea files in {synthea_dir}..."))
    
    if not os.path.isdir(synthea_dir):
        print(ColoredFormatter.error(f"❌ Synthea directory not found: {synthea_dir}"))
        return (False, {})
    
    missing_files = []
    file_stats: Dict[str, Dict[str, Any]] = {}
    
    for file in REQUIRED_SYNTHEA_FILES:
        file_path = os.path.join(synthea_dir, file)
        if not os.path.exists(file_path):
            missing_files.append(file)
            continue
        
        file_size = os.path.getsize(file_path)
        if file_size > 1024 * 1024:
            size_str = f"{file_size / (1024 * 1024):.2f} MB"
        else:
            size_str = f"{file_size / 1024:.2f} KB"
        
        # Count rows (excluding header)
        row_count = 0
        try:
            with open(file_path, 'r') as f:
                next(f)  # skip header
                for _ in f:
                    row_count += 1
        except Exception as e:
            logger.error(f"Error counting rows in {file_path}: {e}")
        
        file_stats[file] = {
            "size": size_str,
            "row_count": row_count
        }
        print(f"  - {file}: {size_str}, {row_count:,} rows")
    
    if missing_files:
        print(ColoredFormatter.warning(f"⚠️ Missing required Synthea files: {', '.join(missing_files)}"))
        return (False, file_stats)
    else:
        total_rows = sum(item["row_count"] for item in file_stats.values())
        print(ColoredFormatter.success(f"✅ All required Synthea files exist. Total: {total_rows:,} rows"))
        return (True, file_stats)

# ---------------------------
# DB Schema/Lookup Setup
# ---------------------------

def ensure_schemas_exist() -> bool:
    """Ensure OMOP and staging schemas and tables exist."""
    step_name = "ensure_schemas_exist"
    if is_step_completed(step_name):
        print(ColoredFormatter.info("✅ Schemas were previously created. Skipping."))
        return True
    
    print(ColoredFormatter.info("\n🔍 Ensuring required schemas exist..."))
    try:
        # Create main schemas
        execute_query("CREATE SCHEMA IF NOT EXISTS omop;")
        execute_query("CREATE SCHEMA IF NOT EXISTS staging;")
        
        # Create progress tracking table
        execute_query("""
        CREATE TABLE IF NOT EXISTS staging.etl_progress (
            step_name VARCHAR(100) NOT NULL PRIMARY KEY,
            started_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            completed_at TIMESTAMP,
            status VARCHAR(20) DEFAULT 'in_progress',
            rows_processed BIGINT DEFAULT 0,
            error_message TEXT
        );
        """)
        
        # Mapping tables
        execute_query("""
        CREATE TABLE IF NOT EXISTS staging.person_map (
            source_patient_id TEXT PRIMARY KEY,
            person_id INTEGER NOT NULL UNIQUE,
            created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
        );
        """)
        
        execute_query("""
        CREATE TABLE IF NOT EXISTS staging.visit_map (
            source_visit_id TEXT PRIMARY KEY,
            visit_occurrence_id INTEGER NOT NULL UNIQUE,
            person_id INTEGER,
            created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
        );
        """)
        
        execute_query("""
        CREATE TABLE IF NOT EXISTS staging.local_to_omop_concept_map (
            source_code VARCHAR(50) NOT NULL,
            source_vocabulary VARCHAR(50) NOT NULL,
            source_description VARCHAR(255),
            domain_id VARCHAR(20),
            target_concept_id INTEGER,
            target_vocabulary_id VARCHAR(20),
            valid_start_date DATE,
            valid_end_date DATE,
            invalid_reason VARCHAR(1),
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (source_code, source_vocabulary)
        );
        """)
        
        # Lookup tables
        execute_query("""
        CREATE TABLE IF NOT EXISTS staging.gender_lookup (
            source_gender VARCHAR(10) PRIMARY KEY,
            gender_concept_id INTEGER NOT NULL,
            gender_source_concept_id INTEGER,
            description VARCHAR(255)
        );
        """)
        
        execute_query("""
        CREATE TABLE IF NOT EXISTS staging.race_lookup (
            source_race VARCHAR(50) PRIMARY KEY,
            race_concept_id INTEGER NOT NULL,
            race_source_concept_id INTEGER,
            description VARCHAR(255)
        );
        """)
        
        execute_query("""
        CREATE TABLE IF NOT EXISTS staging.ethnicity_lookup (
            source_ethnicity VARCHAR(50) PRIMARY KEY,
            ethnicity_concept_id INTEGER NOT NULL,
            ethnicity_source_concept_id INTEGER,
            description VARCHAR(255)
        );
        """)
        
        # Create sequences if they don't exist
        execute_query("CREATE SEQUENCE IF NOT EXISTS staging.person_seq START 1 INCREMENT 1;")
        execute_query("CREATE SEQUENCE IF NOT EXISTS staging.visit_occurrence_seq START 1 INCREMENT 1;")
        execute_query("CREATE SEQUENCE IF NOT EXISTS staging.condition_occurrence_seq START 1 INCREMENT 1;")
        execute_query("CREATE SEQUENCE IF NOT EXISTS staging.drug_exposure_seq START 1 INCREMENT 1;")
        execute_query("CREATE SEQUENCE IF NOT EXISTS staging.procedure_occurrence_seq START 1 INCREMENT 1;")
        execute_query("CREATE SEQUENCE IF NOT EXISTS staging.measurement_seq START 1 INCREMENT 1;")
        execute_query("CREATE SEQUENCE IF NOT EXISTS staging.observation_seq START 1 INCREMENT 1;")
        execute_query("CREATE SEQUENCE IF NOT EXISTS staging.observation_period_seq START 1 INCREMENT 1;")
        
        print(ColoredFormatter.success("✅ Required schemas and tables created successfully"))
        mark_step_completed(step_name)
        return True
    except Exception as e:
        logger.error(f"Error ensuring schemas exist: {e}")
        print(ColoredFormatter.error(f"❌ Error ensuring schemas exist: {e}"))
        return False

def populate_lookup_tables() -> bool:
    """Populate gender, race, and ethnicity lookup tables."""
    step_name = "populate_lookup_tables"
    if is_step_completed(step_name):
        print(ColoredFormatter.info("✅ Lookup tables were previously populated. Skipping."))
        return True
    
    print(ColoredFormatter.info("\n🔍 Populating lookup tables..."))
    
    try:
        # Gender lookup
        execute_query("""
        INSERT INTO staging.gender_lookup (source_gender, gender_concept_id, gender_source_concept_id, description)
        VALUES
            ('M', 8507, 0, 'Male'),
            ('F', 8532, 0, 'Female'),
            ('MALE', 8507, 0, 'Male'),
            ('FEMALE', 8532, 0, 'Female'),
            ('male', 8507, 0, 'Male'),
            ('female', 8532, 0, 'Female'),
            ('m', 8507, 0, 'Male'),
            ('f', 8532, 0, 'Female')
        ON CONFLICT (source_gender) DO NOTHING;
        """)
        
        # Race lookup
        execute_query("""
        INSERT INTO staging.race_lookup (source_race, race_concept_id, race_source_concept_id, description)
        VALUES
            ('white', 8527, 0, 'White'),
            ('black', 8516, 0, 'Black or African American'),
            ('asian', 8515, 0, 'Asian'),
            ('native', 8657, 0, 'American Indian or Alaska Native'),
            ('other', 8522, 0, 'Other Race'),
            ('WHITE', 8527, 0, 'White'),
            ('BLACK', 8516, 0, 'Black or African American'),
            ('ASIAN', 8515, 0, 'Asian'),
            ('NATIVE', 8657, 0, 'American Indian or Alaska Native'),
            ('OTHER', 8522, 0, 'Other Race')
        ON CONFLICT (source_race) DO NOTHING;
        """)
        
        # Ethnicity lookup
        execute_query("""
        INSERT INTO staging.ethnicity_lookup (source_ethnicity, ethnicity_concept_id, ethnicity_source_concept_id, description)
        VALUES
            ('hispanic', 38003563, 0, 'Hispanic'),
            ('nonhispanic', 38003564, 0, 'Not Hispanic'),
            ('HISPANIC', 38003563, 0, 'Hispanic'),
            ('NONHISPANIC', 38003564, 0, 'Not Hispanic')
        ON CONFLICT (source_ethnicity) DO NOTHING;
        """)
        
        print(ColoredFormatter.success("✅ Lookup tables populated successfully"))
        mark_step_completed(step_name)
        return True
    except Exception as e:
        logger.error(f"Error populating lookup tables: {e}")
        print(ColoredFormatter.error(f"❌ Error populating lookup tables: {e}"))
        return False

def reset_omop_tables() -> bool:
    """Reset OMOP tables by truncating them and clearing any staging maps."""
    print(ColoredFormatter.info("\n🔍 Resetting OMOP tables..."))
    try:
        # Truncate tables in correct dependency order
        execute_query("""
        TRUNCATE TABLE omop.observation CASCADE;
        TRUNCATE TABLE omop.measurement CASCADE;
        TRUNCATE TABLE omop.procedure_occurrence CASCADE;
        TRUNCATE TABLE omop.drug_exposure CASCADE;
        TRUNCATE TABLE omop.condition_occurrence CASCADE;
        TRUNCATE TABLE omop.visit_occurrence CASCADE;
        TRUNCATE TABLE omop.observation_period CASCADE;
        TRUNCATE TABLE omop.person CASCADE;
        """)
        
        # Reset sequences
        execute_query("ALTER SEQUENCE staging.person_seq RESTART WITH 1;")
        execute_query("ALTER SEQUENCE staging.visit_occurrence_seq RESTART WITH 1;")
        execute_query("ALTER SEQUENCE staging.condition_occurrence_seq RESTART WITH 1;")
        execute_query("ALTER SEQUENCE staging.drug_exposure_seq RESTART WITH 1;")
        execute_query("ALTER SEQUENCE staging.procedure_occurrence_seq RESTART WITH 1;")
        execute_query("ALTER SEQUENCE staging.measurement_seq RESTART WITH 1;")
        execute_query("ALTER SEQUENCE staging.observation_seq RESTART WITH 1;")
        execute_query("ALTER SEQUENCE staging.observation_period_seq RESTART WITH 1;")
        
        # Truncate mapping tables
        execute_query("TRUNCATE TABLE staging.person_map CASCADE;")
        execute_query("TRUNCATE TABLE staging.visit_map CASCADE;")
        
        # Reset ETL progress tracking table
        execute_query("""
        DO $$
        BEGIN
            IF EXISTS (SELECT FROM information_schema.tables 
                       WHERE table_schema = 'staging' 
                       AND table_name = 'etl_progress') THEN
                TRUNCATE TABLE staging.etl_progress CASCADE;
            END IF;
        END $$;
        """)
        
        # Remove checkpoint file to start fresh
        if os.path.exists(CHECKPOINT_FILE):
            os.remove(CHECKPOINT_FILE)
        
        print(ColoredFormatter.success("✅ OMOP tables reset successfully"))
        return True
    except Exception as e:
        logger.error(f"Error resetting OMOP tables: {e}")
        print(ColoredFormatter.error(f"❌ Error resetting OMOP tables: {e}"))
        return False

# ---------------------------
# CSV Loading to Temp Tables
# ---------------------------

def load_csv_to_temp_table(csv_file: str, table_name: str) -> int:
    """
    Load a CSV file into a temporary table via COPY.
    Returns the number of rows loaded.
    """
    try:
        # Read the header
        with open(csv_file, 'r') as f:
            reader = csv.reader(f)
            header = next(reader)
        
        # Create temp table
        cols = ", ".join([f"\"{col}\" TEXT" for col in header])
        create_sql = f"CREATE TEMPORARY TABLE {table_name} ({cols})"
        execute_query(create_sql)
        
        # COPY data
        conn = get_connection()
        row_count = 0
        try:
            with conn.cursor() as cursor, open(csv_file, 'r') as f_in:
                # Skip header line again
                next(f_in)
                cursor.copy_expert(f"COPY {table_name} FROM STDIN WITH CSV", f_in)
            conn.commit()
            
            # Count rows in temp table
            with conn.cursor() as cursor:
                cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
                row_count = cursor.fetchone()[0]
            
            logger.info(f"Loaded {row_count} rows into {table_name} from {os.path.basename(csv_file)}")
        finally:
            release_connection(conn)
        return row_count
    
    except Exception as e:
        logger.error(f"Error loading CSV to temp table: {e}")
        raise

# ---------------------------
# ETL Steps
# ---------------------------

def process_patients(patients_csv: str) -> bool:
    """Process Synthea patients into OMOP person table."""
    step_name = "process_patients"
    if is_step_completed(step_name):
        print(ColoredFormatter.info("✅ Patients were previously processed. Skipping."))
        return True
    
    print(ColoredFormatter.info("\n🔍 Processing patients data..."))
    
    try:
        # First, get a count of total rows in the CSV (excluding header)
        total_rows = 0
        with open(patients_csv, 'r') as f:
            # Skip header
            next(f)
            # Count remaining lines
            for _ in f:
                total_rows += 1
        
        # Start tracking this step with the total row count
        if progress_tracker and progress_tracker_available:
            progress_tracker.start_step("ETL", step_name, total_items=total_rows, 
                                      message=f"Starting patient processing for {total_rows} records")
    
        # Load CSV to temp table
        temp_table = "temp_patients"
        row_count = load_csv_to_temp_table(patients_csv, temp_table)
        
        # Update progress tracker after loading data (10% complete)
        if progress_tracker and progress_tracker_available:
            progress_tracker.update_progress("ETL", step_name, int(total_rows * 0.1), total_items=total_rows,
                                           message=f"Loaded {row_count} patient records from CSV")
        
        # Create person_map entries
        execute_query(f"""
        INSERT INTO staging.person_map (source_patient_id, person_id)
        SELECT p."Id" AS source_patient_id,
               nextval('staging.person_seq') AS person_id
        FROM {temp_table} p
        WHERE p."Id" NOT IN (SELECT source_patient_id FROM staging.person_map)
        ON CONFLICT (source_patient_id) DO NOTHING;
        """)
        
        # Update progress with mapping completion
        mapping_count = execute_query("SELECT COUNT(*) FROM staging.person_map", fetch=True)[0][0]
        if progress_tracker and progress_tracker_available:
            progress_tracker.update_progress("ETL", step_name, mapping_count, 
                                           f"Created person mapping for {mapping_count} patients")
        
        # Insert into OMOP person - Using WITH clause to deduplicate lookups
        execute_query(f"""
        WITH 
        -- Create deduplicated gender lookup (first match only)
        gender_lookup_dedup AS (
            SELECT DISTINCT ON (LOWER(p."GENDER")) 
                p."Id", gl.gender_concept_id, gl.gender_source_concept_id
            FROM {temp_table} p
            LEFT JOIN staging.gender_lookup gl ON LOWER(p."GENDER") = LOWER(gl.source_gender)
            ORDER BY LOWER(p."GENDER"), gl.source_gender  -- Prefer exact case match
        ),
        -- Create deduplicated race lookup (first match only)
        race_lookup_dedup AS (
            SELECT DISTINCT ON (LOWER(p."RACE")) 
                p."Id", rl.race_concept_id, rl.race_source_concept_id
            FROM {temp_table} p
            LEFT JOIN staging.race_lookup rl ON LOWER(p."RACE") = LOWER(rl.source_race)
            ORDER BY LOWER(p."RACE"), rl.source_race  -- Prefer exact case match
        ),
        -- Create deduplicated ethnicity lookup (first match only)
        ethnicity_lookup_dedup AS (
            SELECT DISTINCT ON (LOWER(p."ETHNICITY")) 
                p."Id", el.ethnicity_concept_id, el.ethnicity_source_concept_id
            FROM {temp_table} p
            LEFT JOIN staging.ethnicity_lookup el ON LOWER(p."ETHNICITY") = LOWER(el.source_ethnicity)
            ORDER BY LOWER(p."ETHNICITY"), el.source_ethnicity  -- Prefer exact case match
        )
        
        -- Main insert with deduplicated lookups
        INSERT INTO omop.person (
            person_id,
            gender_concept_id,
            year_of_birth,
            month_of_birth,
            day_of_birth,
            birth_datetime,
            race_concept_id,
            ethnicity_concept_id,
            location_id,
            provider_id,
            care_site_id,
            person_source_value,
            gender_source_value,
            race_source_value,
            ethnicity_source_value,
            gender_source_concept_id,
            race_source_concept_id,
            ethnicity_source_concept_id
        )
        SELECT
            pm.person_id,
            COALESCE(g.gender_concept_id, 0) AS gender_concept_id,
            EXTRACT(YEAR FROM p."BIRTHDATE"::date) AS year_of_birth,
            EXTRACT(MONTH FROM p."BIRTHDATE"::date) AS month_of_birth,
            EXTRACT(DAY FROM p."BIRTHDATE"::date) AS day_of_birth,
            p."BIRTHDATE"::timestamp AS birth_datetime,
            COALESCE(r.race_concept_id, 0) AS race_concept_id,
            COALESCE(e.ethnicity_concept_id, 0) AS ethnicity_concept_id,
            NULL AS location_id,
            NULL AS provider_id,
            NULL AS care_site_id,
            p."Id" AS person_source_value,
            p."GENDER" AS gender_source_value,
            p."RACE"   AS race_source_value,
            p."ETHNICITY" AS ethnicity_source_value,
            COALESCE(g.gender_source_concept_id, 0),
            COALESCE(r.race_source_concept_id, 0),
            COALESCE(e.ethnicity_source_concept_id, 0)
        FROM {temp_table} p
        JOIN staging.person_map pm ON pm.source_patient_id = p."Id"
        LEFT JOIN gender_lookup_dedup g ON g."Id" = p."Id"
        LEFT JOIN race_lookup_dedup r ON r."Id" = p."Id"
        LEFT JOIN ethnicity_lookup_dedup e ON e."Id" = p."Id"
        WHERE pm.person_id NOT IN (SELECT person_id FROM omop.person);
        """)
        
        # Count final
        person_count = execute_query("SELECT COUNT(*) FROM omop.person", fetch=True)[0][0]
        print(ColoredFormatter.success(f"✅ Successfully processed {person_count} patients"))
        
        # Mark completion in checkpoint system
        mark_step_completed(step_name, {"count": person_count})
        
        # Update ETL progress tracker with completion status
        if progress_tracker and progress_tracker_available:
            progress_tracker.complete_step("ETL", step_name, True, 
                                         f"Successfully processed {person_count} patients")
        
        return True
    except Exception as e:
        error_msg = f"Error processing patients: {e}"
        logger.error(error_msg)
        print(ColoredFormatter.error(f"❌ {error_msg}"))
        
        # Update ETL progress tracker with error - capture current progress
        if progress_tracker and progress_tracker_available:
            # Try to get current progress
            try:
                conn = get_connection()
                with conn.cursor() as cursor:
                    cursor.execute("""
                    SELECT rows_processed, total_rows FROM staging.etl_progress 
                    WHERE step_name = %s
                    """, (f"ETL_{step_name}",))
                    result = cursor.fetchone()
                    if result:
                        rows_processed, total_rows = result
                        progress_tracker.update_progress("ETL", step_name, rows_processed, 
                                                       total_items=total_rows, message=error_msg)
                release_connection(conn)
            except Exception as inner_e:
                logger.error(f"Error getting progress for failed step: {inner_e}")
                
            progress_tracker.complete_step("ETL", step_name, False, error_msg)
            
        return False

def process_encounters(encounters_csv: str) -> bool:
    """Process Synthea encounters into OMOP visit_occurrence table."""
    step_name = "process_encounters"
    if is_step_completed(step_name):
        print(ColoredFormatter.info("✅ Encounters were previously processed. Skipping."))
        return True
    
    print(ColoredFormatter.info("\n🔍 Processing encounters data..."))
    
    # Initialize progress bar
    bar_length = 50
    bar = '░' * bar_length  # Empty bar
    print(f"\r[{bar}] 0% - Starting encounter data processing")
    
    # Start tracking this step if progress tracking is enabled
    if progress_tracker and progress_tracker_available:
        progress_tracker.start_step("ETL", step_name, message="Starting encounter processing")
    
    try:
        temp_table = "temp_encounters"
        row_count = load_csv_to_temp_table(encounters_csv, temp_table)
        
        # Update progress tracker and display progress after loading data (25%)
        filled_length = int(25 / 100 * bar_length)
        bar = '█' * filled_length + '░' * (bar_length - filled_length)
        print(f"\r[{bar}] 25% - Loaded {row_count:,} encounter records from CSV")
        
        # Update progress tracker after loading data
        if progress_tracker and progress_tracker_available:
            progress_tracker.update_progress("ETL", step_name, row_count * 0.25, total_items=row_count,
                                           message=f"Loaded {row_count} encounter records from CSV")
        
        # Create visit_map - critical step for mapping UUID encounter IDs to sequential integers
        execute_query(f"""
        INSERT INTO staging.visit_map (source_visit_id, visit_occurrence_id, person_id)
        SELECT e."Id",
               nextval('staging.visit_occurrence_seq'),
               pm.person_id
        FROM {temp_table} e
        JOIN staging.person_map pm ON pm.source_patient_id = e."PATIENT"
        WHERE e."Id" NOT IN (SELECT source_visit_id FROM staging.visit_map)
        ON CONFLICT (source_visit_id) DO NOTHING;
        """)
        
        # Update progress to 50% after creating visit mapping
        filled_length = int(50 / 100 * bar_length)
        bar = '█' * filled_length + '░' * (bar_length - filled_length)
        
        # Update progress with mapping completion
        mapping_count = execute_query("SELECT COUNT(*) FROM staging.visit_map", fetch=True)[0][0]
        print(f"\r[{bar}] 50% - Created visit mapping for {mapping_count:,} encounters (UUID to integer)")
        
        if progress_tracker and progress_tracker_available:
            progress_tracker.update_progress("ETL", step_name, row_count * 0.5, total_items=row_count,
                                           message=f"Created visit mapping for {mapping_count} encounters (UUID to integer)")
        
        # Update progress to 75% before inserting into visit_occurrence
        filled_length = int(75 / 100 * bar_length)
        bar = '█' * filled_length + '░' * (bar_length - filled_length)
        print(f"\r[{bar}] 75% - Inserting visit records into OMOP tables")
        
        # Update progress tracker
        if progress_tracker and progress_tracker_available:
            progress_tracker.update_progress("ETL", step_name, row_count * 0.75, total_items=row_count,
                                           message=f"Inserting visit records into OMOP tables")
        
        # Insert into visit_occurrence
        execute_query(f"""
        INSERT INTO omop.visit_occurrence (
            visit_occurrence_id,
            person_id,
            visit_concept_id,
            visit_start_date,
            visit_start_datetime,
            visit_end_date,
            visit_end_datetime,
            visit_type_concept_id,
            provider_id,
            care_site_id,
            visit_source_value,
            visit_source_concept_id,
            admitted_from_concept_id,
            admitted_from_source_value,
            discharged_to_concept_id,
            discharged_to_source_value,
            preceding_visit_occurrence_id
        )
        SELECT
            vm.visit_occurrence_id,
            vm.person_id,
            CASE
              WHEN LOWER(e."ENCOUNTERCLASS") = 'ambulatory' THEN 9202
              WHEN LOWER(e."ENCOUNTERCLASS") = 'emergency' THEN 9203
              WHEN LOWER(e."ENCOUNTERCLASS") = 'inpatient' THEN 9201
              WHEN LOWER(e."ENCOUNTERCLASS") = 'wellness' THEN 9202
              WHEN LOWER(e."ENCOUNTERCLASS") = 'urgentcare' THEN 9203
              WHEN LOWER(e."ENCOUNTERCLASS") = 'outpatient' THEN 9202
              ELSE 0
            END AS visit_concept_id,
            e."START"::date,
            e."START"::timestamp,
            e."STOP"::date,
            e."STOP"::timestamp,
            32817,  # EHR
            NULL,
            NULL,
            e."Id",
            0,
            0,
            NULL,
            0,
            NULL,
            NULL
        FROM {temp_table} e
        JOIN staging.visit_map vm ON vm.source_visit_id = e."Id"
        WHERE vm.visit_occurrence_id NOT IN (SELECT visit_occurrence_id FROM omop.visit_occurrence);
        """)
        
        visit_count = execute_query("SELECT COUNT(*) FROM omop.visit_occurrence", fetch=True)[0][0]
        print(ColoredFormatter.success(f"✅ Successfully processed {visit_count} encounters"))
        
        # Mark completion in checkpoint system
        mark_step_completed(step_name, {"count": visit_count})
        
        # Update ETL progress tracker with completion status
        if progress_tracker and progress_tracker_available:
            progress_tracker.complete_step("ETL", step_name, True, 
                                         f"Successfully processed {visit_count} encounters")
        
        return True
    except Exception as e:
        error_msg = f"Error processing encounters: {e}"
        logger.error(error_msg)
        print(ColoredFormatter.error(f"❌ {error_msg}"))
        
        # Update ETL progress tracker with error - capture current progress
        if progress_tracker and progress_tracker_available:
            # Try to get current progress
            try:
                conn = get_connection()
                with conn.cursor() as cursor:
                    cursor.execute("""
                    SELECT rows_processed, total_rows FROM staging.etl_progress 
                    WHERE step_name = %s
                    """, (f"ETL_{step_name}",))
                    result = cursor.fetchone()
                    if result:
                        rows_processed, total_rows = result
                        progress_tracker.update_progress("ETL", step_name, rows_processed, 
                                                       total_items=total_rows, message=error_msg)
                release_connection(conn)
            except Exception as inner_e:
                logger.error(f"Error getting progress for failed step: {inner_e}")
                
            progress_tracker.complete_step("ETL", step_name, False, error_msg)
            
        return False

def process_conditions(conditions_csv: str) -> bool:
    """Process Synthea conditions into OMOP condition_occurrence."""
    step_name = "process_conditions"
    if is_step_completed(step_name):
        print(ColoredFormatter.info("✅ Conditions were previously processed. Skipping."))
        return True
    
    print(ColoredFormatter.info("\n🔍 Processing conditions data..."))
    
    # Start tracking this step if progress tracking is enabled
    if progress_tracker and progress_tracker_available:
        progress_tracker.start_step("ETL", step_name, message="Starting conditions processing")
    
    try:
        temp_table = "temp_conditions"
        row_count = load_csv_to_temp_table(conditions_csv, temp_table)
        
        # Update progress tracker after loading data
        if progress_tracker and progress_tracker_available:
            progress_tracker.update_progress("ETL", step_name, 0, 
                                           f"Loaded {row_count} condition records from CSV")
        
        # Insert condition_occurrence
        execute_query(f"""
        INSERT INTO omop.condition_occurrence (
            condition_occurrence_id,
            person_id,
            condition_concept_id,
            condition_start_date,
            condition_start_datetime,
            condition_end_date,
            condition_end_datetime,
            condition_type_concept_id,
            condition_status_concept_id,
            stop_reason,
            provider_id,
            visit_occurrence_id,
            visit_detail_id,
            condition_source_value,
            condition_source_concept_id,
            condition_status_source_value
        )
        SELECT
            nextval('staging.condition_occurrence_seq'),
            pm.person_id,
            0,
            c."START"::date,
            c."START"::timestamp,
            c."STOP"::date,
            c."STOP"::timestamp,
            32817, -- EHR
            0,
            NULL,
            NULL,
            vm.visit_occurrence_id,
            NULL,
            c."CODE",
            0,
            NULL
        FROM {temp_table} c
        JOIN staging.person_map pm ON pm.source_patient_id = c."PATIENT"
        JOIN staging.visit_map vm ON vm.source_visit_id = c."ENCOUNTER"
        WHERE NOT EXISTS (
            SELECT 1 FROM omop.condition_occurrence co
            WHERE co.person_id = pm.person_id
              AND co.visit_occurrence_id = vm.visit_occurrence_id
              AND co.condition_source_value = c."CODE"
        );
        """)
        
        condition_count = execute_query("SELECT COUNT(*) FROM omop.condition_occurrence", fetch=True)[0][0]
        print(ColoredFormatter.success(f"✅ Successfully processed {condition_count} conditions"))
        
        # Mark completion in checkpoint system
        mark_step_completed(step_name, {"count": condition_count})
        
        # Update ETL progress tracker with completion status
        if progress_tracker and progress_tracker_available:
            progress_tracker.complete_step("ETL", step_name, True, 
                                         f"Successfully processed {condition_count} conditions")
        
        return True
    except Exception as e:
        error_msg = f"Error processing conditions: {e}"
        logger.error(error_msg)
        print(ColoredFormatter.error(f"❌ {error_msg}"))
        
        # Update ETL progress tracker with error - capture current progress
        if progress_tracker and progress_tracker_available:
            # Try to get current progress
            try:
                conn = get_connection()
                with conn.cursor() as cursor:
                    cursor.execute("""
                    SELECT rows_processed, total_rows FROM staging.etl_progress 
                    WHERE step_name = %s
                    """, (f"ETL_{step_name}",))
                    result = cursor.fetchone()
                    if result:
                        rows_processed, total_rows = result
                        progress_tracker.update_progress("ETL", step_name, rows_processed, 
                                                       total_items=total_rows, message=error_msg)
                release_connection(conn)
            except Exception as inner_e:
                logger.error(f"Error getting progress for failed step: {inner_e}")
                
            progress_tracker.complete_step("ETL", step_name, False, error_msg)
            
        return False

def process_medications(medications_csv: str) -> bool:
    """Process Synthea medications into OMOP drug_exposure."""
    step_name = "process_medications"
    if is_step_completed(step_name):
        print(ColoredFormatter.info("✅ Medications were previously processed. Skipping."))
        return True
    
    print(ColoredFormatter.info("\n🔍 Processing medications data..."))
    
    # Start tracking this step if progress tracking is enabled
    if progress_tracker and progress_tracker_available:
        progress_tracker.start_step("ETL", step_name, message="Starting medications processing")
        
    # Display initial progress bar
    bar_length = 50
    bar = '░' * bar_length  # Empty bar
    print(f"\r[{bar}] 0% - Starting medication data processing")
    
    try:
        temp_table = "temp_medications"
        row_count = load_csv_to_temp_table(medications_csv, temp_table)
        
        # Update progress tracker after loading data
        if progress_tracker and progress_tracker_available:
            progress_tracker.update_progress("ETL", step_name, row_count * 0.5, total_items=row_count,
                                           message=f"Loaded {row_count} medication records from CSV")
        
        # Display progress bar at 50%
        filled_length = int(50 / 100 * bar_length)
        bar = '█' * filled_length + '░' * (bar_length - filled_length)
        print(f"\r[{bar}] 50% - Loaded {row_count:,} medication records from CSV")
        
        # Insert drug_exposure
        execute_query(f"""
        INSERT INTO omop.drug_exposure (
            drug_exposure_id,
            person_id,
            drug_concept_id,
            drug_exposure_start_date,
            drug_exposure_start_datetime,
            drug_exposure_end_date,
            drug_exposure_end_datetime,
            verbatim_end_date,
            drug_type_concept_id,
            stop_reason,
            refills,
            quantity,
            days_supply,
            sig,
            route_concept_id,
            lot_number,
            provider_id,
            visit_occurrence_id,
            visit_detail_id,
            drug_source_value,
            drug_source_concept_id,
            route_source_value,
            dose_unit_source_value
        )
        SELECT
            nextval('staging.drug_exposure_seq'),
            pm.person_id,
            0,
            m."START"::date,
            m."START"::timestamp,
            -- Handle NULL STOP dates by using START date plus 30 days as a default end date
            COALESCE(m."STOP"::date, (m."START"::date + INTERVAL '30 days')::date),
            COALESCE(m."STOP"::timestamp, (m."START"::timestamp + INTERVAL '30 days')),
            COALESCE(m."STOP"::date, (m."START"::date + INTERVAL '30 days')::date),
            32817, -- EHR
            NULL,
            0,
            CASE
              WHEN m."DISPENSES" ~ '^[0-9]+(\\.[0-9]+)?$' THEN m."DISPENSES"::numeric
              ELSE NULL
            END,
            NULL,
            NULL,
            0,
            NULL,
            NULL,
            vm.visit_occurrence_id,
            NULL,
            m."CODE",
            0,
            NULL,
            NULL
        FROM {temp_table} m
        JOIN staging.person_map pm ON pm.source_patient_id = m."PATIENT"
        JOIN staging.visit_map vm ON vm.source_visit_id = m."ENCOUNTER"
        WHERE NOT EXISTS (
            SELECT 1 FROM omop.drug_exposure de
            WHERE de.person_id = pm.person_id
              AND de.visit_occurrence_id = vm.visit_occurrence_id
              AND de.drug_source_value = m."CODE"
        );
        """)
        
        drug_count = execute_query("SELECT COUNT(*) FROM omop.drug_exposure", fetch=True)[0][0]
        
        # Update progress tracker with completion
        if progress_tracker and progress_tracker_available:
            progress_tracker.update_progress("ETL", step_name, row_count, total_items=row_count,
                                           message=f"Processed {drug_count} medications")
        
        # Display completed progress bar
        bar = '█' * bar_length
        print(f"\r[{bar}] 100% - Processed {drug_count:,} medications")
        print(ColoredFormatter.success(f"✅ Successfully processed {drug_count:,} medications of {row_count:,} records"))
        
        # Mark completion in checkpoint system
        mark_step_completed(step_name, {"count": drug_count})
        
        # Update ETL progress tracker with completion status
        if progress_tracker and progress_tracker_available:
            progress_tracker.complete_step("ETL", step_name, True, 
                                         f"Successfully processed {drug_count} medications")
        
        return True
    except Exception as e:
        error_msg = f"Error processing medications: {e}"
        logger.error(error_msg)
        print(ColoredFormatter.error(f"❌ {error_msg}"))
        
        # Update ETL progress tracker with error - capture current progress
        if progress_tracker and progress_tracker_available:
            # Try to get current progress
            try:
                conn = get_connection()
                with conn.cursor() as cursor:
                    cursor.execute("""
                    SELECT rows_processed, total_rows FROM staging.etl_progress 
                    WHERE step_name = %s
                    """, (f"ETL_{step_name}",))
                    result = cursor.fetchone()
                    if result:
                        rows_processed, total_rows = result
                        progress_tracker.update_progress("ETL", step_name, rows_processed, 
                                                       total_items=total_rows, message=error_msg)
                release_connection(conn)
            except Exception as inner_e:
                logger.error(f"Error getting progress for failed step: {inner_e}")
                
            progress_tracker.complete_step("ETL", step_name, False, error_msg)
            
        return False

def process_procedures(procedures_csv: str) -> bool:
    """Process Synthea procedures into OMOP procedure_occurrence."""
    step_name = "process_procedures"
    if is_step_completed(step_name):
        print(ColoredFormatter.info("✅ Procedures were previously processed. Skipping."))
        return True
    
    print(ColoredFormatter.info("\n🔍 Processing procedures data..."))
    
    # Start tracking this step if progress tracking is enabled
    if progress_tracker and progress_tracker_available:
        progress_tracker.start_step("ETL", step_name, message="Starting procedures processing")
    
    try:
        temp_table = "temp_procedures"
        row_count = load_csv_to_temp_table(procedures_csv, temp_table)
        
        # Update progress tracker after loading data
        if progress_tracker and progress_tracker_available:
            progress_tracker.update_progress("ETL", step_name, 0, 
                                           f"Loaded {row_count} procedure records from CSV")
        
        # Insert procedure_occurrence
        execute_query(f"""
        INSERT INTO omop.procedure_occurrence (
            procedure_occurrence_id,
            person_id,
            procedure_concept_id,
            procedure_date,
            procedure_datetime,
            procedure_type_concept_id,
            modifier_concept_id,
            quantity,
            provider_id,
            visit_occurrence_id,
            visit_detail_id,
            procedure_source_value,
            procedure_source_concept_id,
            modifier_source_value
        )
        SELECT
            nextval('staging.procedure_occurrence_seq'),
            pm.person_id,
            0,
            p."START"::date,
            p."START"::timestamp,
            32817, -- EHR
            0,
            NULL,
            NULL,
            vm.visit_occurrence_id,
            NULL,
            p."CODE",
            0,
            NULL
        FROM {temp_table} p
        JOIN staging.person_map pm ON pm.source_patient_id = p."PATIENT"
        JOIN staging.visit_map vm ON vm.source_visit_id = p."ENCOUNTER"
        WHERE NOT EXISTS (
            SELECT 1 FROM omop.procedure_occurrence po
            WHERE po.person_id = pm.person_id
              AND po.visit_occurrence_id = vm.visit_occurrence_id
              AND po.procedure_source_value = p."CODE"
        );
        """)
        
        procedure_count = execute_query("SELECT COUNT(*) FROM omop.procedure_occurrence", fetch=True)[0][0]
        print(ColoredFormatter.success(f"✅ Successfully processed {procedure_count} procedures"))
        
        # Mark completion in checkpoint system
        mark_step_completed(step_name, {"count": procedure_count})
        
        # Update ETL progress tracker with completion status
        if progress_tracker and progress_tracker_available:
            progress_tracker.complete_step("ETL", step_name, True, 
                                         f"Successfully processed {procedure_count} procedures")
        
        return True
    except Exception as e:
        error_msg = f"Error processing procedures: {e}"
        logger.error(error_msg)
        print(ColoredFormatter.error(f"❌ {error_msg}"))
        
        # Update ETL progress tracker with error - capture current progress
        if progress_tracker and progress_tracker_available:
            # Try to get current progress
            try:
                conn = get_connection()
                with conn.cursor() as cursor:
                    cursor.execute("""
                    SELECT rows_processed, total_rows FROM staging.etl_progress 
                    WHERE step_name = %s
                    """, (f"ETL_{step_name}",))
                    result = cursor.fetchone()
                    if result:
                        rows_processed, total_rows = result
                        progress_tracker.update_progress("ETL", step_name, rows_processed, 
                                                       total_items=total_rows, message=error_msg)
                release_connection(conn)
            except Exception as inner_e:
                logger.error(f"Error getting progress for failed step: {inner_e}")
                
            progress_tracker.complete_step("ETL", step_name, False, error_msg)
            
        return False

def count_csv_rows(csv_file):
    """
    Count the number of rows in a CSV file (excluding header row).
    
    Args:
        csv_file: Path to the CSV file
        
    Returns:
        int: Number of rows in the CSV file (excluding header)
    """
    with open(csv_file, 'r') as f:
        # Skip header
        next(f)
        # Count remaining lines
        count = sum(1 for _ in f)
    return count


def direct_import_observations_to_omop(observations_csv: str, batch_size: int = 50000, min_batch_size: int = 10000, max_batch_size: int = 200000) -> bool:
    """
    Directly import observations from CSV to OMOP observation table using batch processing.
    This is an optimized method for handling very large observation files.
    
    Args:
        observations_csv: Path to the observations CSV file
        batch_size: Initial batch size for processing (will be adjusted adaptively)
        min_batch_size: Minimum batch size for adaptive sizing
        max_batch_size: Maximum batch size for adaptive sizing
    """
    import psutil  # For memory monitoring
    step_name = "direct_import_observations"
    
    if is_step_completed(step_name):
        print(ColoredFormatter.info("✅ Observations were previously directly imported. Skipping."))
        return True
    
    if not os.path.exists(observations_csv):
        logger.error(f"Observations file not found: {observations_csv}")
        return False
    
    logger.info(f"Starting direct import of observations from {observations_csv}")
    print(ColoredFormatter.info("\n🔍 Directly importing observations to OMOP..."))
    
    try:
        # Get a dedicated connection for this entire process to ensure temp tables persist
        conn = get_connection()
        
        # Performance tracking variables
        start_time = time.time()
        process = psutil.Process(os.getpid())
        initial_memory = process.memory_info().rss / 1024 / 1024  # MB
        processing_rates = []
        current_batch_size = batch_size
        
        # Get total row count for progress tracking
        total_rows = count_csv_rows(observations_csv)
        logger.info(f"Found {total_rows:,} observations to process")
        
        # Ensure staging schema exists and create etl_progress table if it doesn't exist
        with conn.cursor() as cur:
            # Create the staging.etl_progress table if it doesn't exist
            cur.execute("""
            CREATE SCHEMA IF NOT EXISTS staging;
            
            CREATE TABLE IF NOT EXISTS staging.etl_progress (
                id SERIAL PRIMARY KEY,
                process_type VARCHAR(50),
                step_name VARCHAR(100),
                start_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                end_time TIMESTAMP,
                current_progress BIGINT DEFAULT 0,
                total_items BIGINT,
                status VARCHAR(20) DEFAULT 'in_progress',
                message TEXT,
                UNIQUE(process_type, step_name)
            );
            
            -- Ensure person_map and visit_map tables exist with proper constraints
            CREATE TABLE IF NOT EXISTS staging.person_map (
                id SERIAL PRIMARY KEY,
                source_patient_id TEXT UNIQUE,
                person_id INTEGER
            );
            
            CREATE TABLE IF NOT EXISTS staging.visit_map (
                id SERIAL PRIMARY KEY,
                source_visit_id TEXT UNIQUE,
                visit_occurrence_id INTEGER,
                person_id INTEGER
            );
            """)
            conn.commit()
    
        # Start tracking this step if progress tracking is enabled
        if progress_tracker and progress_tracker_available:
            try:
                progress_tracker.start_step("ETL", step_name, total_items=total_rows, 
                                         message=f"Starting direct observations import for {total_rows} records")
            except Exception as e:
                logger.warning(f"Failed to start progress tracking: {e}")
        
        # Create a temporary table for staging the observations and ensure sequences exist
        with conn.cursor() as cur:
            cur.execute("""
            -- Create sequences if they don't exist
            CREATE SEQUENCE IF NOT EXISTS staging.person_seq;
            CREATE SEQUENCE IF NOT EXISTS staging.visit_occurrence_seq;
            CREATE SEQUENCE IF NOT EXISTS staging.observation_seq;
            
            -- Create temporary table for staging
            DROP TABLE IF EXISTS temp_direct_observations;
            CREATE TEMPORARY TABLE temp_direct_observations (
                id SERIAL PRIMARY KEY,
                date TIMESTAMP,
                patient TEXT,
                encounter TEXT,
                code TEXT,
                description TEXT,
                value TEXT,
                units TEXT,
                type TEXT
            );
            """)
            conn.commit()
        
        # Initialize progress tracking variables
        processed_rows = 0
        rows_inserted = 0
        last_batch_time = start_time
        last_log_time = start_time
        adaptive_batch_size = batch_size
        
        # Setup progress bar
        if tqdm_available:
            progress_bar = tqdm(total=total_rows, desc="Importing Observations", unit="rows")
        else:
            print(f"Starting import of {total_rows:,} observations...")
        
        # Process the CSV in batches using cursor-based pagination
        with open(observations_csv, 'r') as f:
            reader = csv.DictReader(f)
            batch = []
            
            for row in reader:
                batch.append((row.get('DATE', ''), 
                             row.get('PATIENT', ''), 
                             row.get('ENCOUNTER', ''), 
                             row.get('CODE', ''), 
                             row.get('DESCRIPTION', ''), 
                             row.get('VALUE', ''), 
                             row.get('UNITS', ''), 
                             row.get('TYPE', '')))
                
                if len(batch) >= adaptive_batch_size:
                    # Record batch start time for performance tracking
                    batch_start_time = time.time()
                    
                    # Insert the batch into temp table
                    with conn.cursor() as cur:
                        args_str = ','.join(cur.mogrify("(%s::timestamp, %s, %s, %s, %s, %s, %s, %s)", row).decode('utf-8') for row in batch)
                        cur.execute(f"""
                        INSERT INTO temp_direct_observations (date, patient, encounter, code, description, value, units, type)
                        VALUES {args_str}
                        """)
                    
                    # Process the batch from temp table to OMOP
                    with conn.cursor() as cur:
                        try:
                            cur.execute("""
                            -- First, ensure person_map and visit_map have entries for our data
                            INSERT INTO staging.person_map (source_patient_id, person_id)
                            SELECT DISTINCT o.patient, 
                                   COALESCE((SELECT person_id FROM staging.person_map WHERE source_patient_id = o.patient), 
                                           nextval('staging.person_seq'))
                            FROM temp_direct_observations o
                            WHERE NOT EXISTS (SELECT 1 FROM staging.person_map pm WHERE pm.source_patient_id = o.patient)
                            ON CONFLICT (source_patient_id) DO NOTHING;
                            
                            -- First get person_id for each patient
                            WITH patient_ids AS (
                                INSERT INTO staging.person_map (source_patient_id, person_id)
                                SELECT DISTINCT o.patient, 
                                       COALESCE((SELECT person_id FROM staging.person_map WHERE source_patient_id = o.patient), 
                                               nextval('staging.person_seq'))
                                FROM temp_direct_observations o
                                WHERE NOT EXISTS (SELECT 1 FROM staging.person_map pm WHERE pm.source_patient_id = o.patient)
                                ON CONFLICT (source_patient_id) DO NOTHING
                                RETURNING source_patient_id, person_id
                            )
                            -- Then insert into visit_map with person_id
                            INSERT INTO staging.visit_map (source_visit_id, visit_occurrence_id, person_id)
                            SELECT DISTINCT o.encounter, 
                                   COALESCE((SELECT visit_occurrence_id FROM staging.visit_map WHERE source_visit_id = o.encounter), 
                                           nextval('staging.visit_occurrence_seq')),
                                   COALESCE((SELECT person_id FROM staging.person_map WHERE source_patient_id = o.patient),
                                           (SELECT person_id FROM patient_ids WHERE source_patient_id = o.patient))
                            FROM temp_direct_observations o
                            WHERE NOT EXISTS (SELECT 1 FROM staging.visit_map vm WHERE vm.source_visit_id = o.encounter)
                            ON CONFLICT (source_visit_id) DO NOTHING;
                            
                            -- Now insert the observations
                            INSERT INTO omop.observation (
                                observation_id,
                                person_id,
                                observation_concept_id,
                                observation_date,
                                observation_datetime,
                                observation_type_concept_id,
                                value_as_number,
                                value_as_string,
                                value_as_concept_id,
                                qualifier_concept_id,
                                unit_concept_id,
                                provider_id,
                                visit_occurrence_id,
                                visit_detail_id,
                                observation_source_value,
                                observation_source_concept_id,
                                unit_source_value,
                                qualifier_source_value,
                                value_source_value
                            )
                            SELECT
                                nextval('staging.observation_seq'),
                                pm.person_id,
                                0,
                                o.date::date,
                                o.date::timestamp,
                                32817, -- EHR
                                CASE WHEN o.value ~ '^[0-9]+(\.[0-9]+)?$' THEN o.value::numeric ELSE NULL END,
                                o.value,
                                0,
                                0,
                                0,
                                NULL,
                                vm.visit_occurrence_id,
                                NULL,
                                o.code,
                                0,
                                o.units,
                                NULL,
                                o.value
                            FROM temp_direct_observations o
                            JOIN staging.person_map pm ON pm.source_patient_id = o.patient
                            JOIN staging.visit_map vm ON vm.source_visit_id = o.encounter
                            WHERE NOT EXISTS (
                                SELECT 1 FROM omop.observation obs
                                WHERE obs.person_id = pm.person_id
                                  AND obs.visit_occurrence_id = vm.visit_occurrence_id
                                  AND obs.observation_source_value = o.code
                                  AND obs.value_source_value = o.value
                            )
                            """)
                            rows_inserted += cur.rowcount
                            logger.info(f"Inserted {cur.rowcount} rows into omop.observation (total: {rows_inserted:,})")
                        except Exception as e:
                            logger.error(f"Error inserting into omop.observation: {e}")
                            conn.rollback()
                            # Continue processing despite errors
                            continue
                    
                    # Commit the transaction
                    conn.commit()
                    
                    # Clear the temp table for the next batch
                    with conn.cursor() as cur:
                        cur.execute("TRUNCATE TABLE temp_direct_observations")
                    conn.commit()
                    
                    # Update progress tracking
                    processed_rows += len(batch)
                    if tqdm_available:
                        progress_bar.update(len(batch))
                    
                    # Calculate processing rate and adjust batch size adaptively
                    current_time = time.time()
                    batch_time = current_time - last_batch_time
                    rows_per_second = len(batch) / batch_time if batch_time > 0 else 0
                    
                    # Adjust batch size based on performance
                    if batch_time > 10:  # If batch took too long, reduce size
                        adaptive_batch_size = max(1000, int(adaptive_batch_size * 0.8))
                    elif batch_time < 2 and rows_per_second > 0:  # If batch was fast, increase size
                        adaptive_batch_size = min(500000, int(adaptive_batch_size * 1.2))
                    
                    # Log progress with detailed rate and memory information
                    elapsed = current_time - start_time
                    percent_complete = (processed_rows / total_rows) * 100 if total_rows > 0 else 0
                    eta = ((total_rows - processed_rows) / rows_per_second) if rows_per_second > 0 else 0
                    current_memory = process.memory_info().rss / 1024 / 1024  # MB
                    memory_change = current_memory - initial_memory
                    
                    logger.info(f"Processed {processed_rows:,}/{total_rows:,} rows ({percent_complete:.1f}%) | "
                               f"Rate: {rows_per_second:.1f} rows/sec | Batch size: {adaptive_batch_size:,} | "
                               f"ETA: {eta/60:.1f} minutes | Memory: {int(current_memory)} MB ({int(memory_change):+d} MB)")
                    
                    # Update progress and performance metrics
                    batch_time = time.time() - batch_start_time
                    current_rate = len(batch) / batch_time if batch_time > 0 else 0
                    processing_rates.append(current_rate)
                    
                    # Calculate average rate from the last 5 batches
                    recent_rate = sum(processing_rates[-5:]) / min(len(processing_rates), 5) if processing_rates else 0
                    
                    # Calculate ETA
                    remaining_rows = total_rows - processed_rows
                    eta_seconds = remaining_rows / recent_rate if recent_rate > 0 else 0
                    eta_str = str(datetime.timedelta(seconds=int(eta_seconds))) if eta_seconds > 0 else "unknown"
                    
                    # Monitor memory usage
                    current_memory = process.memory_info().rss / 1024 / 1024  # MB
                    memory_change = current_memory - initial_memory
                    
                    # Adaptive batch sizing
                    if batch_time > 5 and adaptive_batch_size > min_batch_size:
                        # If batch is taking too long, reduce size
                        adaptive_batch_size = max(min_batch_size, int(adaptive_batch_size * 0.8))
                        logger.info(f"Reducing batch size to {adaptive_batch_size} due to slow processing")
                    elif batch_time < 1 and current_memory < 1024 and adaptive_batch_size < max_batch_size:
                        # If processing is fast and memory usage is reasonable, increase batch size
                        adaptive_batch_size = min(max_batch_size, int(adaptive_batch_size * 1.2))
                        logger.info(f"Increasing batch size to {adaptive_batch_size} for better throughput")
                    
                    # Update progress tracker
                    if progress_tracker and progress_tracker_available:
                        try:
                            progress_message = (f"Imported {processed_rows:,} of {total_rows:,} observations | "
                                              f"Rate: {int(current_rate):,} rows/s | "
                                              f"Avg Rate: {int(recent_rate):,} rows/s | "
                                              f"ETA: {eta_str} | "
                                              f"Memory: {int(current_memory)} MB ({int(memory_change):+d} MB)")
                            progress_tracker.update_progress("ETL", step_name, processed_rows, total_items=total_rows,
                                                         message=progress_message)
                        except Exception as e:
                            # Just log the error but continue processing
                            logger.error(f"Failed to update progress: {e}")
                    
                    # Reset for next batch
                    batch = []
                    last_batch_time = current_time
            
            # Process any remaining rows
            if batch:
                # Insert the batch into temp table
                with conn.cursor() as cur:
                    args_str = ','.join(cur.mogrify("(%s::timestamp, %s, %s, %s, %s, %s, %s, %s)", row).decode('utf-8') for row in batch)
                    cur.execute(f"""
                    INSERT INTO temp_direct_observations (date, patient, encounter, code, description, value, units, type)
                    VALUES {args_str}
                    """)
                
                # Process the batch from temp table to OMOP
                with conn.cursor() as cur:
                    try:
                        cur.execute("""
                        -- First, ensure person_map and visit_map have entries for our data
                        INSERT INTO staging.person_map (source_patient_id, person_id)
                        SELECT DISTINCT o.patient, 
                               COALESCE((SELECT person_id FROM staging.person_map WHERE source_patient_id = o.patient), 
                                       nextval('staging.person_seq'))
                        FROM temp_direct_observations o
                        WHERE NOT EXISTS (SELECT 1 FROM staging.person_map pm WHERE pm.source_patient_id = o.patient)
                        ON CONFLICT (source_patient_id) DO NOTHING;
                        
                        -- First get person_id for each patient
                        WITH patient_ids AS (
                            INSERT INTO staging.person_map (source_patient_id, person_id)
                            SELECT DISTINCT o.patient, 
                                   COALESCE((SELECT person_id FROM staging.person_map WHERE source_patient_id = o.patient), 
                                           nextval('staging.person_seq'))
                            FROM temp_direct_observations o
                            WHERE NOT EXISTS (SELECT 1 FROM staging.person_map pm WHERE pm.source_patient_id = o.patient)
                            ON CONFLICT (source_patient_id) DO NOTHING
                            RETURNING source_patient_id, person_id
                        )
                        -- Then insert into visit_map with person_id
                        INSERT INTO staging.visit_map (source_visit_id, visit_occurrence_id, person_id)
                        SELECT DISTINCT o.encounter, 
                               COALESCE((SELECT visit_occurrence_id FROM staging.visit_map WHERE source_visit_id = o.encounter), 
                                       nextval('staging.visit_occurrence_seq')),
                               COALESCE((SELECT person_id FROM staging.person_map WHERE source_patient_id = o.patient),
                                       (SELECT person_id FROM patient_ids WHERE source_patient_id = o.patient))
                        FROM temp_direct_observations o
                        WHERE NOT EXISTS (SELECT 1 FROM staging.visit_map vm WHERE vm.source_visit_id = o.encounter)
                        ON CONFLICT (source_visit_id) DO NOTHING;
                        
                        -- Now insert the observations
                        INSERT INTO omop.observation (
                            observation_id,
                            person_id,
                            observation_concept_id,
                            observation_date,
                            observation_datetime,
                            observation_type_concept_id,
                            value_as_number,
                            value_as_string,
                            value_as_concept_id,
                            qualifier_concept_id,
                            unit_concept_id,
                            provider_id,
                            visit_occurrence_id,
                            visit_detail_id,
                            observation_source_value,
                            observation_source_concept_id,
                            unit_source_value,
                            qualifier_source_value,
                            value_source_value
                        )
                        SELECT
                            nextval('staging.observation_seq'),
                            pm.person_id,
                            0,
                            o.date::date,
                            o.date::timestamp,
                            32817, -- EHR
                            CASE WHEN o.value ~ '^[0-9]+(\.[0-9]+)?$' THEN o.value::numeric ELSE NULL END,
                            o.value,
                            0,
                            0,
                            0,
                            NULL,
                            vm.visit_occurrence_id,
                            NULL,
                            o.code,
                            0,
                            o.units,
                            NULL,
                            o.value
                        FROM temp_direct_observations o
                        JOIN staging.person_map pm ON pm.source_patient_id = o.patient
                        JOIN staging.visit_map vm ON vm.source_visit_id = o.encounter
                        WHERE NOT EXISTS (
                            SELECT 1 FROM omop.observation obs
                            WHERE obs.person_id = pm.person_id
                              AND obs.visit_occurrence_id = vm.visit_occurrence_id
                              AND obs.observation_source_value = o.code
                              AND obs.value_source_value = o.value
                        )
                        """)
                        rows_inserted += cur.rowcount
                        logger.info(f"Inserted {cur.rowcount} rows into omop.observation (total: {rows_inserted:,})")
                    except Exception as e:
                        logger.error(f"Error inserting into omop.observation: {e}")
                        conn.rollback()
                
                # Commit the transaction
                conn.commit()
                
                # Update progress tracking
                processed_rows += len(batch)
                if tqdm_available:
                    progress_bar.update(len(batch))
        
        if tqdm_available:
            progress_bar.close()
            
        # Calculate final performance metrics
        total_time = time.time() - start_time
        final_memory = process.memory_info().rss / 1024 / 1024  # MB
        memory_change = final_memory - initial_memory
        
        # Mark step as complete
        if progress_tracker and progress_tracker_available:
            try:
                progress_tracker.complete_step("ETL", step_name, 
                                          message=f"Completed direct import of {processed_rows:,} observations")
            except Exception as e:
                logger.error(f"Failed to complete progress tracking: {e}")
        
        # Log completion with detailed performance metrics
        logger.info(f"Completed direct import of {processed_rows:,} observations in {total_time:.1f} seconds")
        logger.info(f"Successfully inserted {rows_inserted:,} observations into OMOP")
        logger.info(f"Overall processing rate: {processed_rows/total_time if total_time > 0 else 0:,.1f} rows/second")
        logger.info(f"Memory usage: {int(final_memory)} MB (change: {int(memory_change):+d} MB)")
        
        # Mark step as completed in the database
        mark_step_completed(step_name)
        
        print(ColoredFormatter.success(f"✅ Completed direct import of {processed_rows:,} observations ({rows_inserted:,} inserted)"))
    
    except Exception as e:
        logger.error(f"Error directly importing observations: {e}")
        print(ColoredFormatter.error(f"❌ {e}"))
        return False

def process_observations(observations_csv: str) -> bool:
    """Process Synthea observations into OMOP measurement or observation tables."""
    step_name = "process_observations"
    if is_step_completed(step_name):
        print(ColoredFormatter.info("✅ Observations were previously processed. Skipping."))
        return True
    
    print(ColoredFormatter.info("\n🔍 Processing observations data..."))
    
    try:
        # First, get a count of total rows in the CSV (excluding header)
        total_rows = 0
        with open(observations_csv, 'r') as f:
            # Skip header
            next(f)
            # Count remaining lines
            for _ in f:
                total_rows += 1
        
        # Start tracking this step with the total row count
        if progress_tracker and progress_tracker_available:
            progress_tracker.start_step("ETL", step_name, total_items=total_rows, 
                                      message=f"Starting observations processing for {total_rows} records")
            
        # Display initial progress bar
        bar_length = 50
        bar = '░' * bar_length  # Empty bar
        print(f"\r[{bar}] 0% - Starting observations processing")
    
        temp_table = "temp_observations"
        row_count = load_csv_to_temp_table(observations_csv, temp_table)
        
        # Update progress tracker after loading data (10% complete)
        if progress_tracker and progress_tracker_available:
            progress_tracker.update_progress("ETL", step_name, int(total_rows * 0.1), total_items=total_rows,
                                           message=f"Loaded {row_count} observation records from CSV")
        
        # Display progress bar at 10%
        filled_length = int(10 / 100 * bar_length)
        bar = '█' * filled_length + '░' * (bar_length - filled_length)
        print(f"\r[{bar}] 10% - Loaded observation records")
        
        # Insert numeric as measurement
        execute_query(f"""
        INSERT INTO omop.measurement (
            measurement_id,
            person_id,
            measurement_concept_id,
            measurement_date,
            measurement_datetime,
            measurement_type_concept_id,
            operator_concept_id,
            value_as_number,
            value_as_concept_id,
            unit_concept_id,
            provider_id,
            visit_occurrence_id,
            visit_detail_id,
            measurement_source_value,
            measurement_source_concept_id,
            unit_source_value,
            value_source_value
        )
        SELECT
            nextval('staging.measurement_seq'),
            pm.person_id,
            0,
            o."DATE"::date,
            o."DATE"::timestamp,
            32817, -- EHR
            0,
            CASE
              WHEN o."VALUE" ~ '^[0-9]+(\\.[0-9]+)?$' THEN o."VALUE"::numeric
              ELSE NULL
            END,
            0,
            0,
            NULL,
            vm.visit_occurrence_id,
            NULL,
            o."CODE",
            0,
            o."UNITS",
            o."VALUE"
        FROM {temp_table} o
        JOIN staging.person_map pm ON pm.source_patient_id = o."PATIENT"
        JOIN staging.visit_map vm ON vm.source_visit_id = o."ENCOUNTER"
        WHERE o."VALUE" ~ '^[0-9]+(\\.[0-9]+)?$'
          AND NOT EXISTS (
            SELECT 1 FROM omop.measurement m
            WHERE m.person_id = pm.person_id
              AND m.visit_occurrence_id = vm.visit_occurrence_id
              AND m.measurement_source_value = o."CODE"
              AND m.value_source_value = o."VALUE"
          );
        """)
        
        measurement_count = execute_query("SELECT COUNT(*) FROM omop.measurement", fetch=True)[0][0]
        
        # Update progress to 50% after processing measurements
        if progress_tracker and progress_tracker_available:
            progress_tracker.update_progress("ETL", step_name, int(total_rows * 0.5), total_items=total_rows,
                                          message=f"Processed {measurement_count} measurements")
        
        # Display progress bar at 50%
        filled_length = int(50 / 100 * bar_length)
        bar = '█' * filled_length + '░' * (bar_length - filled_length)
        print(f"\r[{bar}] 50% - Processed measurements")
        
        # Insert non-numeric as observation
        execute_query(f"""
        INSERT INTO omop.observation (
            observation_id,
            person_id,
            observation_concept_id,
            observation_date,
            observation_datetime,
            observation_type_concept_id,
            value_as_number,
            value_as_string,
            value_as_concept_id,
            qualifier_concept_id,
            unit_concept_id,
            provider_id,
            visit_occurrence_id,
            visit_detail_id,
            observation_source_value,
            observation_source_concept_id,
            unit_source_value,
            qualifier_source_value,
            value_source_value
        )
        SELECT
            nextval('staging.observation_seq'),
            pm.person_id,
            0,
            o."DATE"::date,
            o."DATE"::timestamp,
            32817,
            NULL,
            o."VALUE",
            0,
            0,
            0,
            NULL,
            vm.visit_occurrence_id,
            NULL,
            o."CODE",
            0,
            o."UNITS",
            NULL,
            o."VALUE"
        FROM {temp_table} o
        JOIN staging.person_map pm ON pm.source_patient_id = o."PATIENT"
        JOIN staging.visit_map vm ON vm.source_visit_id = o."ENCOUNTER"
        WHERE NOT (o."VALUE" ~ '^[0-9]+(\\.[0-9]+)?$')
          AND NOT EXISTS (
            SELECT 1 FROM omop.observation obs
            WHERE obs.person_id = pm.person_id
              AND obs.visit_occurrence_id = vm.visit_occurrence_id
              AND obs.observation_source_value = o."CODE"
              AND obs.value_source_value = o."VALUE"
          );
        """)
        
        observation_count = execute_query("SELECT COUNT(*) FROM omop.observation", fetch=True)[0][0]
        
        # Update progress to 90% after processing observations
        if progress_tracker and progress_tracker_available:
            progress_tracker.update_progress("ETL", step_name, int(total_rows * 0.9), total_items=total_rows,
                                          message=f"Processed {observation_count} observations")
        
        # Display progress bar at 90%
        filled_length = int(90 / 100 * bar_length)
        bar = '█' * filled_length + '░' * (bar_length - filled_length)
        print(f"\r[{bar}] 90% - Processed observations")
        
        # Create a success message with both measurements and observations counts
        success_msg = f"Successfully processed {measurement_count} measurements and {observation_count} observations"
        
        # Display completed progress bar
        bar = '█' * bar_length
        print(f"\r[{bar}] 100% - Completed observation processing")
        print(ColoredFormatter.success(f"✅ {success_msg}"))
        
        # Mark completion in checkpoint system
        mark_step_completed(step_name, {
            "measurement_count": measurement_count,
            "observation_count": observation_count
        })
        
        # Update ETL progress tracker with completion status
        if progress_tracker and progress_tracker_available:
            progress_tracker.complete_step("ETL", step_name, True, success_msg)
        
        return True
    except Exception as e:
        error_msg = f"Error processing observations: {e}"
        logger.error(error_msg)
        print(ColoredFormatter.error(f"❌ {error_msg}"))
        
        # Update ETL progress tracker with error - capture current progress
        if progress_tracker and progress_tracker_available:
            # Try to get current progress
            try:
                conn = get_connection()
                with conn.cursor() as cursor:
                    cursor.execute("""
                    SELECT rows_processed, total_rows FROM staging.etl_progress 
                    WHERE step_name = %s
                    """, (f"ETL_{step_name}",))
                    result = cursor.fetchone()
                    if result:
                        rows_processed, total_rows = result
                        progress_tracker.update_progress("ETL", step_name, rows_processed, 
                                                       total_items=total_rows, message=error_msg)
                release_connection(conn)
            except Exception as inner_e:
                logger.error(f"Error getting progress for failed step: {inner_e}")
                
            progress_tracker.complete_step("ETL", step_name, False, error_msg)
            
        return False

def create_observation_periods() -> bool:
    """Create observation periods for each person, from min to max event dates."""
    step_name = "create_observation_periods"
    if is_step_completed(step_name):
        print(ColoredFormatter.info("✅ Observation periods were previously created. Skipping."))
        return True
    
    print(ColoredFormatter.info("\n🔍 Creating observation periods..."))
    
    try:    
        # First, get a count of the persons for whom we need to create observation periods
        total_persons = execute_query("SELECT COUNT(*) FROM omop.person", fetch=True)[0][0]
        
        # Start tracking this step with the total person count
        if progress_tracker and progress_tracker_available:
            progress_tracker.start_step("ETL", step_name, total_items=total_persons, 
                                       message=f"Creating observation periods for {total_persons} persons")
        
        # Display initial progress bar
        bar_length = 50
        bar = '░' * bar_length  # Empty bar
        print(f"\r[{bar}] 0% - Starting observation period creation")
    
        # Update progress to 25% - collecting data
        if progress_tracker and progress_tracker_available:
            progress_tracker.update_progress("ETL", step_name, int(total_persons * 0.25), 
                                           total_items=total_persons,
                                           message="Collecting data for observation periods")
        
        # Display progress bar at 25%
        filled_length = int(25 / 100 * bar_length)
        bar = '█' * filled_length + '░' * (bar_length - filled_length)
        print(f"\r[{bar}] 25% - Collecting event data")
        
        # Execute the main query
        execute_query("""
        INSERT INTO omop.observation_period (
            observation_period_id,
            person_id,
            observation_period_start_date,
            observation_period_end_date,
            period_type_concept_id
        )
        SELECT
            nextval('staging.observation_period_seq'),
            person_id,
            MIN(observation_start_date),
            MAX(observation_end_date),
            32817 -- EHR
        FROM (
            -- UNION of event ranges
            SELECT person_id, visit_start_date AS observation_start_date, visit_end_date AS observation_end_date
            FROM omop.visit_occurrence
            UNION ALL
            SELECT person_id, condition_start_date, COALESCE(condition_end_date, condition_start_date)
            FROM omop.condition_occurrence
            UNION ALL
            SELECT person_id, drug_exposure_start_date, COALESCE(drug_exposure_end_date, drug_exposure_start_date)
            FROM omop.drug_exposure
            UNION ALL
            SELECT person_id, procedure_date, procedure_date
            FROM omop.procedure_occurrence
            UNION ALL
            SELECT person_id, measurement_date, measurement_date
            FROM omop.measurement
            UNION ALL
            SELECT person_id, observation_date, observation_date
            FROM omop.observation
        ) all_events
        GROUP BY person_id
        ON CONFLICT DO NOTHING;
        """)
        
        period_count = execute_query("SELECT COUNT(*) FROM omop.observation_period", fetch=True)[0][0]
        
        # Update progress to 90% after creating periods
        if progress_tracker and progress_tracker_available:
            progress_tracker.update_progress("ETL", step_name, int(total_persons * 0.9), 
                                           total_items=total_persons,
                                           message=f"Created {period_count} observation periods")
        
        # Display progress bar at 90%
        filled_length = int(90 / 100 * bar_length)
        bar = '█' * filled_length + '░' * (bar_length - filled_length)
        print(f"\r[{bar}] 90% - Created observation periods")
        
        success_msg = f"Successfully created {period_count} observation periods"
        
        # Display completed progress bar
        bar = '█' * bar_length
        print(f"\r[{bar}] 100% - Completed observation period creation")
        print(ColoredFormatter.success(f"✅ {success_msg}"))
        
        # Mark completion in checkpoint system
        mark_step_completed(step_name, {"count": period_count})
        
        # Update ETL progress tracker with completion status
        if progress_tracker and progress_tracker_available:
            progress_tracker.complete_step("ETL", step_name, True, success_msg)
        
        return True
    except Exception as e:
        error_msg = f"Error creating observation periods: {e}"
        logger.error(error_msg)
        print(ColoredFormatter.error(f"❌ {error_msg}"))
        
        # Update ETL progress tracker with error - capture current progress
        if progress_tracker and progress_tracker_available:
            # Try to get current progress
            try:
                conn = get_connection()
                with conn.cursor() as cursor:
                    cursor.execute("""
                    SELECT rows_processed, total_rows FROM staging.etl_progress 
                    WHERE step_name = %s
                    """, (f"ETL_{step_name}",))
                    result = cursor.fetchone()
                    if result:
                        rows_processed, total_rows = result
                        progress_tracker.update_progress("ETL", step_name, rows_processed, 
                                                       total_items=total_rows, message=error_msg)
                release_connection(conn)
            except Exception as inner_e:
                logger.error(f"Error getting progress for failed step: {inner_e}")
                
            progress_tracker.complete_step("ETL", step_name, False, error_msg)
            
        return False

def map_source_to_standard_concepts() -> bool:
    """Map local source codes in condition, drug, procedure, measurement, observation to standard concepts."""
    step_name = "map_source_to_standard_concepts"
    if is_step_completed(step_name):
        print(ColoredFormatter.info("✅ Concept mapping was previously completed. Skipping."))
        return True
    
    print(ColoredFormatter.info("\n🔍 Mapping source codes to standard concepts..."))
    
    try:
        # First, get counts of records in all tables to be mapped
        table_counts = execute_query("""
        SELECT 
            (SELECT COUNT(*) FROM omop.condition_occurrence) as condition_count,
            (SELECT COUNT(*) FROM omop.drug_exposure) as drug_count,
            (SELECT COUNT(*) FROM omop.procedure_occurrence) as procedure_count,
            (SELECT COUNT(*) FROM omop.measurement) as measurement_count,
            (SELECT COUNT(*) FROM omop.observation) as observation_count
        """, fetch=True)[0]
        
        # Calculate total records to process
        total_records = sum(table_counts)
        
        # Initialize progress tracking
        if progress_tracker and progress_tracker_available:
            progress_tracker.start_step("ETL", step_name, total_items=total_records,
                                      message=f"Starting concept mapping for {total_records} records")
        
        # Display initial progress bar
        bar_length = 50
        bar = '░' * bar_length  # Empty bar
        print(f"\r[{bar}] 0% - Starting concept mapping")
        
        # Keep track of progress
        records_processed = 0
    
        # Condition mapping
        condition_count = table_counts[0]
        execute_query("""
        UPDATE omop.condition_occurrence co
        SET condition_concept_id = COALESCE(scm.target_concept_id, 0)
        FROM staging.local_to_omop_concept_map scm
        WHERE co.condition_source_value = scm.source_code
          AND scm.source_vocabulary = 'SNOMED'
          AND scm.domain_id = 'Condition';
        """)
        
        # Update progress after condition mapping
        records_processed += condition_count
        progress_pct = int((records_processed / total_records) * 100) if total_records > 0 else 0
        
        if progress_tracker and progress_tracker_available:
            progress_tracker.update_progress("ETL", step_name, records_processed, total_items=total_records,
                                           message=f"Mapped conditions ({progress_pct}% complete)")
        
        # Display progress bar
        filled_length = int(progress_pct / 100 * bar_length)
        bar = '█' * filled_length + '░' * (bar_length - filled_length)
        print(f"\r[{bar}] {progress_pct}% - Mapped conditions")
        
        # Drug mapping
        drug_count = table_counts[1]
        execute_query("""
        UPDATE omop.drug_exposure de
        SET drug_concept_id = COALESCE(scm.target_concept_id, 0)
        FROM staging.local_to_omop_concept_map scm
        WHERE de.drug_source_value = scm.source_code
          AND scm.source_vocabulary = 'RxNorm'
          AND scm.domain_id = 'Drug';
        """)
        
        # Update progress after drug mapping
        records_processed += drug_count
        progress_pct = int((records_processed / total_records) * 100) if total_records > 0 else 0
        
        if progress_tracker and progress_tracker_available:
            progress_tracker.update_progress("ETL", step_name, records_processed, total_items=total_records,
                                           message=f"Mapped drugs ({progress_pct}% complete)")
        
        # Display progress bar
        filled_length = int(progress_pct / 100 * bar_length)
        bar = '█' * filled_length + '░' * (bar_length - filled_length)
        print(f"\r[{bar}] {progress_pct}% - Mapped drugs")
        
        # Procedure mapping
        procedure_count = table_counts[2]
        execute_query("""
        UPDATE omop.procedure_occurrence po
        SET procedure_concept_id = COALESCE(scm.target_concept_id, 0)
        FROM staging.local_to_omop_concept_map scm
        WHERE po.procedure_source_value = scm.source_code
          AND scm.source_vocabulary = 'SNOMED'
          AND scm.domain_id = 'Procedure';
        """)
        
        # Update progress after procedure mapping
        records_processed += procedure_count
        progress_pct = int((records_processed / total_records) * 100) if total_records > 0 else 0
        
        if progress_tracker and progress_tracker_available:
            progress_tracker.update_progress("ETL", step_name, records_processed, total_items=total_records,
                                           message=f"Mapped procedures ({progress_pct}% complete)")
        
        # Display progress bar
        filled_length = int(progress_pct / 100 * bar_length)
        bar = '█' * filled_length + '░' * (bar_length - filled_length)
        print(f"\r[{bar}] {progress_pct}% - Mapped procedures")
        
        # Measurement mapping
        measurement_count = table_counts[3]
        execute_query("""
        UPDATE omop.measurement m
        SET measurement_concept_id = COALESCE(scm.target_concept_id, 0)
        FROM staging.local_to_omop_concept_map scm
        WHERE m.measurement_source_value = scm.source_code
          AND scm.source_vocabulary = 'LOINC'
          AND scm.domain_id = 'Measurement';
        """)
        
        # Update progress after measurement mapping
        records_processed += measurement_count
        progress_pct = int((records_processed / total_records) * 100) if total_records > 0 else 0
        
        if progress_tracker and progress_tracker_available:
            progress_tracker.update_progress("ETL", step_name, records_processed, total_items=total_records,
                                           message=f"Mapped measurements ({progress_pct}% complete)")
        
        # Display progress bar
        filled_length = int(progress_pct / 100 * bar_length)
        bar = '█' * filled_length + '░' * (bar_length - filled_length)
        print(f"\r[{bar}] {progress_pct}% - Mapped measurements")
        
        # Observation mapping
        observation_count = table_counts[4]
        execute_query("""
        UPDATE omop.observation o
        SET observation_concept_id = COALESCE(scm.target_concept_id, 0)
        FROM staging.local_to_omop_concept_map scm
        WHERE o.observation_source_value = scm.source_code
          AND scm.source_vocabulary = 'LOINC'
          AND scm.domain_id = 'Observation';
        """)

        # Update progress after observation mapping
        records_processed += observation_count
        progress_pct = int((records_processed / total_records) * 100) if total_records > 0 else 0
        
        if progress_tracker and progress_tracker_available:
            progress_tracker.update_progress("ETL", step_name, records_processed, total_items=total_records,
                                           message=f"Mapped observations ({progress_pct}% complete)")
        
        # Display progress bar
        filled_length = int(progress_pct / 100 * bar_length)
        bar = '█' * filled_length + '░' * (bar_length - filled_length)
        print(f"\r[{bar}] {progress_pct}% - Mapped observations")
        
        # Gather simple stats
        mapping_stats = execute_query("""
        SELECT 
            'condition_occurrence' AS table_name,
            COUNT(*) AS total_count,
            SUM(CASE WHEN condition_concept_id = 0 THEN 1 ELSE 0 END) AS unmapped_count,
            ROUND(100.0 * SUM(CASE WHEN condition_concept_id = 0 THEN 1 ELSE 0 END) / NULLIF(COUNT(*), 0), 2) AS unmapped_percentage
        FROM omop.condition_occurrence
        UNION ALL
        SELECT 
            'drug_exposure' AS table_name,
            COUNT(*) AS total_count,
            SUM(CASE WHEN drug_concept_id = 0 THEN 1 ELSE 0 END) AS unmapped_count,
            ROUND(100.0 * SUM(CASE WHEN drug_concept_id = 0 THEN 1 ELSE 0 END) / NULLIF(COUNT(*), 0), 2) AS unmapped_percentage
        FROM omop.drug_exposure
        UNION ALL
        SELECT 
            'procedure_occurrence' AS table_name,
            COUNT(*),
            SUM(CASE WHEN procedure_concept_id = 0 THEN 1 ELSE 0 END),
            ROUND(100.0 * SUM(CASE WHEN procedure_concept_id = 0 THEN 1 ELSE 0 END) / NULLIF(COUNT(*), 0), 2)
        FROM omop.procedure_occurrence
        UNION ALL
        SELECT 
            'measurement' AS table_name,
            COUNT(*),
            SUM(CASE WHEN measurement_concept_id = 0 THEN 1 ELSE 0 END),
            ROUND(100.0 * SUM(CASE WHEN measurement_concept_id = 0 THEN 1 ELSE 0 END) / NULLIF(COUNT(*), 0), 2)
        FROM omop.measurement
        UNION ALL
        SELECT 
            'observation' AS table_name,
            COUNT(*),
            SUM(CASE WHEN observation_concept_id = 0 THEN 1 ELSE 0 END),
            ROUND(100.0 * SUM(CASE WHEN observation_concept_id = 0 THEN 1 ELSE 0 END) / NULLIF(COUNT(*), 0), 2)
        FROM omop.observation
        """, fetch=True)
        
        # Compile mapping statistics
        mapping_summary = "\nConcept Mapping Statistics:\n"
        
        # Update progress to 95% during statistics calculation
        if progress_tracker and progress_tracker_available:
            progress_tracker.update_progress("ETL", step_name, int(total_records * 0.95), total_items=total_records,
                                           message="Completed concept mapping updates, computing statistics")
        
        # Display progress bar at 95%
        filled_length = int(95 / 100 * bar_length)
        bar = '█' * filled_length + '░' * (bar_length - filled_length)
        print(f"\r[{bar}] 95% - Computing mapping statistics")
        
        print("\nConcept Mapping Statistics:")
        for row in mapping_stats:
            table_name, total_count, unmapped_count, unmapped_pct = row
            stat_line = f"  - {table_name}: {unmapped_count}/{total_count} unmapped ({unmapped_pct}%)"
            mapping_summary += stat_line + "\n"
            print(stat_line)
        
        success_msg = "Successfully mapped source codes to standard concepts"
        
        # Display completed progress bar
        bar = '█' * bar_length
        print(f"\r[{bar}] 100% - Completed concept mapping")
        print(ColoredFormatter.success(f"✅ {success_msg}"))
        
        # Mark completion in checkpoint system
        mark_step_completed(step_name, {"mapping_stats": mapping_stats})
        
        # Update ETL progress tracker with completion status
        if progress_tracker and progress_tracker_available:
            progress_tracker.complete_step("ETL", step_name, True, 
                                         f"{success_msg}\n{mapping_summary}")
        
        return True
    except Exception as e:
        error_msg = f"Error mapping source to standard concepts: {e}"
        logger.error(error_msg)
        print(ColoredFormatter.error(f"❌ {error_msg}"))
        
        # Update ETL progress tracker with error - capture current progress
        if progress_tracker and progress_tracker_available:
            # Try to get current progress
            try:
                conn = get_connection()
                with conn.cursor() as cursor:
                    cursor.execute("""
                    SELECT rows_processed, total_rows FROM staging.etl_progress 
                    WHERE step_name = %s
                    """, (f"ETL_{step_name}",))
                    result = cursor.fetchone()
                    if result:
                        rows_processed, total_rows = result
                        progress_tracker.update_progress("ETL", step_name, rows_processed, 
                                                       total_items=total_rows, message=error_msg)
                release_connection(conn)
            except Exception as inner_e:
                logger.error(f"Error getting progress for failed step: {inner_e}")
                
            progress_tracker.complete_step("ETL", step_name, False, error_msg)
            
        return False

def analyze_tables() -> bool:
    """Run ANALYZE on primary OMOP tables for query optimization."""
    step_name = "analyze_tables"
    if is_step_completed(step_name):
        print(ColoredFormatter.info("✅ Tables were previously analyzed. Skipping."))
        return True
    
    print(ColoredFormatter.info("\n🔍 Analyzing tables for query optimization..."))
    
    try:
        tables = [
            'person', 'observation_period', 'visit_occurrence', 
            'condition_occurrence', 'drug_exposure', 'procedure_occurrence',
            'measurement', 'observation'
        ]
        
        total_tables = len(tables)
        
        # Start tracking this step with the total number of tables
        if progress_tracker and progress_tracker_available:
            progress_tracker.start_step("ETL", step_name, total_items=total_tables,
                                      message=f"Starting table analysis for {total_tables} tables")
        
        # Display initial progress bar
        bar_length = 50
        bar = '░' * bar_length  # Empty bar
        print(f"\r[{bar}] 0% - Starting table analysis")
    
        for i, t in enumerate(tables):
            table_msg = f"Analyzing omop.{t}..."
            print(f"  - {table_msg}")
            execute_query(f"ANALYZE omop.{t}")
            
            # Update progress with each table analyzed
            if progress_tracker and progress_tracker_available:
                progress_tracker.update_progress("ETL", step_name, i + 1, total_items=total_tables,
                                              message=f"Analyzed {i+1}/{total_tables} tables: {t}")
                
            # Display progress bar
            progress_pct = int(((i + 1) / total_tables) * 100)
            filled_length = int(progress_pct / 100 * bar_length)
            bar = '█' * filled_length + '░' * (bar_length - filled_length)
            print(f"\r[{bar}] {progress_pct}% - Analyzed {i+1}/{total_tables} tables")
        
        success_msg = "Successfully analyzed all OMOP tables for query optimization"
        
        # Display completed progress bar
        bar = '█' * bar_length
        print(f"\r[{bar}] 100% - Completed table analysis")
        print(ColoredFormatter.success(f"✅ {success_msg}"))
        
        # Mark completion in checkpoint system
        mark_step_completed(step_name)
        
        # Update ETL progress tracker with completion status
        if progress_tracker and progress_tracker_available:
            progress_tracker.complete_step("ETL", step_name, True, success_msg)
            
        return True
    except Exception as e:
        error_msg = f"Error analyzing tables: {e}"
        logger.error(error_msg)
        print(ColoredFormatter.error(f"❌ {error_msg}"))
        
        # Update ETL progress tracker with error - capture current progress
        if progress_tracker and progress_tracker_available:
            # Try to get current progress
            try:
                conn = get_connection()
                with conn.cursor() as cursor:
                    cursor.execute("""
                    SELECT rows_processed, total_rows FROM staging.etl_progress 
                    WHERE step_name = %s
                    """, (f"ETL_{step_name}",))
                    result = cursor.fetchone()
                    if result:
                        rows_processed, total_rows = result
                        progress_tracker.update_progress("ETL", step_name, rows_processed, 
                                                       total_items=total_rows, message=error_msg)
                release_connection(conn)
            except Exception as inner_e:
                logger.error(f"Error getting progress for failed step: {inner_e}")
                
            progress_tracker.complete_step("ETL", step_name, False, error_msg)
            
        return False

def validate_etl_results(args: argparse.Namespace) -> bool:
    """
    Validate ETL results unless --skip-validation is set.
    Checks record counts, date ranges, demographics, referential integrity.
    """
    step_name = "validate_etl_results"
    if args.skip_validation:
        print(ColoredFormatter.info("✅ Validation skipped as requested."))
        return True
    
    if is_step_completed(step_name):
        print(ColoredFormatter.info("✅ ETL results were previously validated. Skipping."))
        return True
    
    print(ColoredFormatter.info("\n🔍 Validating ETL results..."))
    
    # Define validation tasks
    validation_tasks = [
        "Record counts",
        "Date ranges", 
        "Gender distribution",
        "Referential integrity"
    ]
    total_tasks = len(validation_tasks)
    
    # Start tracking this step with the total number of validation tasks
    if progress_tracker and progress_tracker_available:
        progress_tracker.start_step("ETL", step_name, total_items=total_tasks,
                                  message=f"Starting ETL validation with {total_tasks} validation checks")
    
    # Display initial progress bar
    bar_length = 50
    bar = '░' * bar_length  # Empty bar
    print(f"\r[{bar}] 0% - Starting validation process")
    
    try:
        # Task 1: Gather record counts
        current_task = 1
        print(f"\nValidation Task {current_task}/{total_tasks}: {validation_tasks[current_task-1]}")
        
        # Update progress tracker
        if progress_tracker and progress_tracker_available:
            progress_tracker.update_progress("ETL", step_name, current_task-1, total_items=total_tasks,
                                            message=f"Starting {validation_tasks[current_task-1]} check")
        
        # Display progress bar
        progress_pct = int(((current_task-0.5) / total_tasks) * 100)
        filled_length = int(progress_pct / 100 * bar_length)
        bar = '█' * filled_length + '░' * (bar_length - filled_length)
        print(f"\r[{bar}] {progress_pct}% - Checking record counts")
        
        record_counts = execute_query("""
        SELECT 'person' AS table_name, COUNT(*) AS record_count FROM omop.person
        UNION ALL
        SELECT 'observation_period', COUNT(*) FROM omop.observation_period
        UNION ALL
        SELECT 'visit_occurrence', COUNT(*) FROM omop.visit_occurrence
        UNION ALL
        SELECT 'condition_occurrence', COUNT(*) FROM omop.condition_occurrence
        UNION ALL
        SELECT 'drug_exposure', COUNT(*) FROM omop.drug_exposure
        UNION ALL
        SELECT 'procedure_occurrence', COUNT(*) FROM omop.procedure_occurrence
        UNION ALL
        SELECT 'measurement', COUNT(*) FROM omop.measurement
        UNION ALL
        SELECT 'observation', COUNT(*) FROM omop.observation
        ORDER BY table_name
        """, fetch=True)
        
        print("\nRecord Counts:")
        for row in record_counts:
            table_name, count = row
            print(f"  - {table_name}: {count:,} records")
        
        # Update progress for completed task
        if progress_tracker and progress_tracker_available:
            progress_tracker.update_progress("ETL", step_name, current_task, total_items=total_tasks,
                                            message=f"Completed {validation_tasks[current_task-1]} check")
        
        # Display progress bar
        progress_pct = int((current_task / total_tasks) * 100)
        filled_length = int(progress_pct / 100 * bar_length)
        bar = '█' * filled_length + '░' * (bar_length - filled_length)
        print(f"\r[{bar}] {progress_pct}% - Completed record counts check")
        
        # Task 2: Check date ranges
        current_task = 2
        print(f"\nValidation Task {current_task}/{total_tasks}: {validation_tasks[current_task-1]}")
        
        # Update progress tracker
        if progress_tracker and progress_tracker_available:
            progress_tracker.update_progress("ETL", step_name, current_task-0.5, total_items=total_tasks,
                                            message=f"Starting {validation_tasks[current_task-1]} check")
        
        # Display progress bar
        progress_pct = int(((current_task-0.5) / total_tasks) * 100)
        filled_length = int(progress_pct / 100 * bar_length)
        bar = '█' * filled_length + '░' * (bar_length - filled_length)
        print(f"\r[{bar}] {progress_pct}% - Checking date ranges")
        
        date_ranges = execute_query("""
        SELECT 
            MIN(observation_period_start_date),
            MAX(observation_period_end_date)
        FROM omop.observation_period
        """, fetch=True)[0]
        min_date, max_date = date_ranges
        print(f"\nDate Range in observation_period: {min_date} to {max_date}")
        
        # Update progress for completed task
        if progress_tracker and progress_tracker_available:
            progress_tracker.update_progress("ETL", step_name, current_task, total_items=total_tasks,
                                            message=f"Completed {validation_tasks[current_task-1]} check")
        
        # Display progress bar
        progress_pct = int((current_task / total_tasks) * 100)
        filled_length = int(progress_pct / 100 * bar_length)
        bar = '█' * filled_length + '░' * (bar_length - filled_length)
        print(f"\r[{bar}] {progress_pct}% - Completed date ranges check")
        
        # Task 3: Check gender distribution
        current_task = 3
        print(f"\nValidation Task {current_task}/{total_tasks}: {validation_tasks[current_task-1]}")
        
        # Update progress tracker
        if progress_tracker and progress_tracker_available:
            progress_tracker.update_progress("ETL", step_name, current_task-0.5, total_items=total_tasks,
                                            message=f"Starting {validation_tasks[current_task-1]} check")
        
        # Display progress bar
        progress_pct = int(((current_task-0.5) / total_tasks) * 100)
        filled_length = int(progress_pct / 100 * bar_length)
        bar = '█' * filled_length + '░' * (bar_length - filled_length)
        print(f"\r[{bar}] {progress_pct}% - Checking gender distribution")
        
        gender_counts = execute_query("""
        SELECT gender_concept_id, COUNT(*) 
        FROM omop.person
        GROUP BY gender_concept_id
        ORDER BY gender_concept_id
        """, fetch=True)
        print("\nGender Distribution:")
        for gender_id, count in gender_counts:
            if gender_id == 8507:
                gender_name = "Male"
            elif gender_id == 8532:
                gender_name = "Female"
            else:
                gender_name = "Unknown"
            print(f"  - {gender_name} (concept_id: {gender_id}): {count:,} persons")
        
        # Update progress for completed task
        if progress_tracker and progress_tracker_available:
            progress_tracker.update_progress("ETL", step_name, current_task, total_items=total_tasks,
                                            message=f"Completed {validation_tasks[current_task-1]} check")
        
        # Display progress bar
        progress_pct = int((current_task / total_tasks) * 100)
        filled_length = int(progress_pct / 100 * bar_length)
        bar = '█' * filled_length + '░' * (bar_length - filled_length)
        print(f"\r[{bar}] {progress_pct}% - Completed gender distribution check")
        
        # Task 4: Check referential integrity for a few major tables
        current_task = 4
        print(f"\nValidation Task {current_task}/{total_tasks}: {validation_tasks[current_task-1]}")
        
        # Update progress tracker
        if progress_tracker and progress_tracker_available:
            progress_tracker.update_progress("ETL", step_name, current_task-0.5, total_items=total_tasks,
                                            message=f"Starting {validation_tasks[current_task-1]} check")
        
        # Display progress bar
        progress_pct = int(((current_task-0.5) / total_tasks) * 100)
        filled_length = int(progress_pct / 100 * bar_length)
        bar = '█' * filled_length + '░' * (bar_length - filled_length)
        print(f"\r[{bar}] {progress_pct}% - Checking referential integrity")
        ref_integrity = execute_query("""
        SELECT 
            'visit_occurrence' AS table_name,
            COUNT(*) AS total_count,
            COALESCE(SUM(CASE WHEN p.person_id IS NULL THEN 1 ELSE 0 END), 0) AS orphaned_count
        FROM omop.visit_occurrence vo
        LEFT JOIN omop.person p ON vo.person_id = p.person_id
        UNION ALL
        SELECT 
            'condition_occurrence',
            COUNT(*),
            COALESCE(SUM(CASE WHEN p.person_id IS NULL THEN 1 ELSE 0 END), 0)
        FROM omop.condition_occurrence co
        LEFT JOIN omop.person p ON co.person_id = p.person_id
        UNION ALL
        SELECT 
            'drug_exposure',
            COUNT(*),
            COALESCE(SUM(CASE WHEN p.person_id IS NULL THEN 1 ELSE 0 END), 0)
        FROM omop.drug_exposure de
        LEFT JOIN omop.person p ON de.person_id = p.person_id
        """, fetch=True)
        
        # Compile validation results summary
        validation_summary = "\nValidation Results:\n"
        validation_summary += "\nReferential Integrity Check:\n"
        
        print("\nReferential Integrity Check:")
        for (table_name, total_count, orphaned_count) in ref_integrity:
            # Use a safe comparison with default of 0 for orphaned_count if it's None
            if (orphaned_count or 0) > 0:
                integrity_message = f"  - {table_name}: {orphaned_count}/{total_count} orphaned records"
                validation_summary += integrity_message + " (WARNING)\n"
                print(ColoredFormatter.warning(integrity_message))
            else:
                integrity_message = f"  - {table_name}: No orphaned records"
                validation_summary += integrity_message + "\n"
                print(integrity_message)
        
        # Update progress for completed task
        if progress_tracker and progress_tracker_available:
            progress_tracker.update_progress("ETL", step_name, current_task, total_items=total_tasks,
                                            message=f"Completed {validation_tasks[current_task-1]} check")
        
        # Display completed progress bar
        bar = '█' * bar_length
        print(f"\r[{bar}] 100% - Completed all validation checks")
        
        success_msg = "ETL validation completed successfully"
        print(ColoredFormatter.success(f"✅ {success_msg}"))
        
        # Mark completion in checkpoint system
        mark_step_completed(step_name, {"record_counts": record_counts})
        
        # Update ETL progress tracker with completion status
        if progress_tracker and progress_tracker_available:
            progress_tracker.complete_step("ETL", step_name, True, 
                                         f"{success_msg}\n{validation_summary}")
            
        return True
    except Exception as e:
        error_msg = f"Error validating ETL results: {e}"
        logger.error(error_msg)
        print(ColoredFormatter.error(f"❌ {error_msg}"))
        
        # Update ETL progress tracker with error - capture current progress
        if progress_tracker and progress_tracker_available:
            # Try to get current progress
            try:
                conn = get_connection()
                with conn.cursor() as cursor:
                    cursor.execute("""
                    SELECT rows_processed, total_rows FROM staging.etl_progress 
                    WHERE step_name = %s
                    """, (f"ETL_{step_name}",))
                    result = cursor.fetchone()
                    if result:
                        rows_processed, total_rows = result
                        progress_tracker.update_progress("ETL", step_name, rows_processed, 
                                                       total_items=total_rows, message=error_msg)
                release_connection(conn)
            except Exception as inner_e:
                logger.error(f"Error getting progress for failed step: {inner_e}")
                
            progress_tracker.complete_step("ETL", step_name, False, error_msg)
            
        return False

# ---------------------------
# Interactive Menu
# ---------------------------

def display_interactive_menu() -> Dict[str, bool]:
    """
    Display an interactive menu to allow users to select which data domains to process.
    Returns a dictionary with the user's selections.
    """
    print(ColoredFormatter.highlight("\n=== Synthea to OMOP ETL Interactive Menu ==="))
    print("Select which data domains you would like to process:")
    
    # Default all options to True
    selections = {
        "patients": True,
        "encounters": True,
        "conditions": True,
        "medications": True,
        "procedures": True,
        "observations": True,
        "direct_import_observations": False,  # Default to standard processing
        "observation_periods": True,
        "concept_mapping": True,
        "analyze_tables": True,
        "validate_results": True
    }
    
    # Define the menu options with descriptions
    menu_options = [
        ("patients", "Process patients.csv to OMOP person table"),
        ("encounters", "Process encounters.csv to OMOP visit_occurrence table"),
        ("conditions", "Process conditions.csv to OMOP condition_occurrence table"),
        ("medications", "Process medications.csv to OMOP drug_exposure table"),
        ("procedures", "Process procedures.csv to OMOP procedure_occurrence table"),
        ("observations", "Process observations.csv to OMOP measurement and observation tables"),
        ("direct_import_observations", "Directly import observations.csv to OMOP observation table (faster for large files)"),
        ("observation_periods", "Create observation_period records"),
        ("concept_mapping", "Map source concepts to standard concepts"),
        ("analyze_tables", "Run ANALYZE on tables for query optimization"),
        ("validate_results", "Validate ETL results")
    ]
    
    # Check if we're in an interactive terminal
    if sys.stdout.isatty():
        try:
            # For each option, ask the user if they want to process it
            for key, description in menu_options:
                # Special handling for direct import observations
                if key == "direct_import_observations" and not selections["observations"]:
                    continue  # Skip this option if observations processing is disabled
                
                # For direct import, phrase the question differently
                if key == "direct_import_observations":
                    print(f"\n{ColoredFormatter.info('Observations Processing Method:')}")
                    print("1. Standard processing (split into measurement and observation tables)")
                    print("2. Direct import (faster for large files, all go to observation table)")
                    choice = input("Select method [1/2]: ").strip()
                    selections["direct_import_observations"] = (choice == "2")
                    # If direct import is selected, we still need observations processing enabled
                    if selections["direct_import_observations"]:
                        selections["observations"] = True
                else:
                    default = "Y" if selections[key] else "N"
                    choice = input(f"{description} [Y/n]: " if default == "Y" else f"{description} [y/N]: ").strip().upper()
                    if choice == "":
                        choice = default
                    selections[key] = (choice == "Y")
            
            # If the user disabled observations, make sure direct_import_observations is also disabled
            if not selections["observations"]:
                selections["direct_import_observations"] = False
                
        except (KeyboardInterrupt, EOFError):
            print("\nInteractive selection cancelled. Using default settings (process all domains).")
            return {key: True for key in selections}
    else:
        print(ColoredFormatter.warning("Non-interactive terminal detected. Using command line arguments only."))
    
    # Display summary of selections
    print(ColoredFormatter.highlight("\nYou have selected to process:"))
    for key, description in menu_options:
        if key == "direct_import_observations":
            if selections[key]:
                print(f"✅ Observations: Direct import method")
            elif selections["observations"]:
                print(f"✅ Observations: Standard processing method")
        elif selections[key]:
            print(f"✅ {description}")
        else:
            print(f"❌ {description}")
    
    # Ask for confirmation
    if sys.stdout.isatty():
        try:
            confirm = input(f"\n{ColoredFormatter.info('Proceed with these selections? [Y/n]:')} ").strip().upper()
            if confirm == "N":
                print("Cancelled. Exiting.")
                sys.exit(0)
        except (KeyboardInterrupt, EOFError):
            print("\nCancelled. Exiting.")
            sys.exit(0)
    
    return selections

# ---------------------------
# Main Orchestration
# ---------------------------

def main():
    """Main ETL orchestration function."""
    # Parse command line arguments
    args = parse_arguments()
    
    # Print a welcome message with information about progress bars
    print(ColoredFormatter.info("Starting Enhanced Synthea to OMOP ETL Process"))
    print(ColoredFormatter.info("Progress tracking: Progress bars will be displayed during processing steps"))
    
    if args.track_progress and progress_tracker_available:
        print(ColoredFormatter.info("ETL Progress Tracking enabled: Progress will be stored in the database"))
    
    # If force_reprocess is enabled, delete the checkpoint file
    if args.force_reprocess and CHECKPOINT_FILE.exists():
        logger.info(f"Removing checkpoint file to force reprocessing of all steps")
        print(ColoredFormatter.info("\n🔄 Force reprocessing enabled: All steps will be executed regardless of previous runs"))
        try:
            os.remove(CHECKPOINT_FILE)
            print(ColoredFormatter.info("✅ Checkpoint file removed, all steps will be reprocessed"))
        except Exception as e:
            logger.error(f"Failed to remove checkpoint file: {e}")
    
    # Set up logging as requested
    setup_logging(args.debug)
    
    # Display interactive menu if not in non-interactive mode
    interactive_selections = display_interactive_menu()
    
    # Override command line arguments with interactive selections
    if args.direct_import_observations:
        # If command line flag is set, respect it unless explicitly disabled in interactive menu
        interactive_selections["direct_import_observations"] = True
    
    # Validate that Synthea files exist (unless forcibly ignoring)
    valid_files, file_stats = validate_synthea_files(args.synthea_dir)
    if not valid_files and not args.force:
        logger.error("Required Synthea files missing; use --force to ignore validation or provide the files.")
        sys.exit(1)
    
    # Initialize DB connection
    if not initialize_database_connection(args):
        sys.exit("Failed to initialize database connection; cannot proceed with ETL.")
    
    # Optionally reset tables
    if args.reset_tables:
        reset_omop_tables()
    
    # Ensure schemas & populate lookups
    ensure_schemas_exist()
    populate_lookup_tables()
    
    # Process each domain in sequence based on interactive selections
    patients_csv = os.path.join(args.synthea_dir, "patients.csv")
    encounters_csv = os.path.join(args.synthea_dir, "encounters.csv")
    conditions_csv = os.path.join(args.synthea_dir, "conditions.csv")
    medications_csv = os.path.join(args.synthea_dir, "medications.csv")
    procedures_csv = os.path.join(args.synthea_dir, "procedures.csv")
    observations_csv = os.path.join(args.synthea_dir, "observations.csv")
    
    if interactive_selections["patients"]:
        process_patients(patients_csv)
    else:
        print(ColoredFormatter.info("Skipping patients processing as per user selection"))
    
    if interactive_selections["encounters"]:
        process_encounters(encounters_csv)
    else:
        print(ColoredFormatter.info("Skipping encounters processing as per user selection"))
    
    if interactive_selections["conditions"]:
        process_conditions(conditions_csv)
    else:
        print(ColoredFormatter.info("Skipping conditions processing as per user selection"))
    
    if interactive_selections["medications"]:
        process_medications(medications_csv)
    else:
        print(ColoredFormatter.info("Skipping medications processing as per user selection"))
    
    if interactive_selections["procedures"]:
        process_procedures(procedures_csv)
    else:
        print(ColoredFormatter.info("Skipping procedures processing as per user selection"))
    
    # For observations, either use direct import or standard processing based on selection
    if interactive_selections["observations"]:
        if interactive_selections["direct_import_observations"]:
            logger.info("Using direct import for observations.csv to omop.observation table")
            direct_import_observations_to_omop(observations_csv, batch_size=args.batch_size)
        else:
            process_observations(observations_csv)
    else:
        print(ColoredFormatter.info("Skipping observations processing as per user selection"))
    
    # Create observation periods if selected
    if interactive_selections["observation_periods"]:
        create_observation_periods()
    else:
        print(ColoredFormatter.info("Skipping observation period creation as per user selection"))
    
    # Map concepts unless user skips
    if interactive_selections["concept_mapping"] and not args.skip_concept_mapping:
        map_source_to_standard_concepts()
    else:
        print(ColoredFormatter.info("Skipping concept mapping as per user selection"))
    
    # Analyze tables if selected
    if interactive_selections["analyze_tables"]:
        analyze_tables()
    else:
        print(ColoredFormatter.info("Skipping table analysis as per user selection"))
    
    # Validate results if selected
    if interactive_selections["validate_results"]:
        validate_etl_results(args)
    else:
        print(ColoredFormatter.info("Skipping ETL validation as per user selection"))
    
    print(ColoredFormatter.success("\n🎉 ETL process completed successfully!\n"))

if __name__ == "__main__":
    main()
