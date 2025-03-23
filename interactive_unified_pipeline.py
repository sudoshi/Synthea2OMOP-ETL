#!/usr/bin/env python3
"""
interactive_unified_pipeline.py - Interactive Unified pipeline for Synthea to OMOP ETL

This script provides an interactive unified pipeline that:
1. Validates the environment and prerequisites
2. Initializes the database with OMOP CDM schema
3. Loads vocabulary data
4. Runs the optimized Synthea to OMOP ETL process

It combines multiple steps into a single, streamlined, interactive workflow
with detailed progress tracking and error recovery.
"""

import argparse
import logging
import os
import subprocess
import sys
import time
import json
import psycopg2
import shutil
import csv
from typing import List, Dict, Any, Optional, Tuple
from datetime import datetime, timedelta
from pathlib import Path

# Initialize package availability flags
tqdm_available = False
colorama_available = False
progress_tracker_available = False

def check_and_install_dependencies(interactive=True):
    """Check for required and optional dependencies and offer to install them."""
    global tqdm_available, colorama_available, progress_tracker_available
    global tqdm, Fore, Back, Style, init, ETLProgressTracker
    
    missing_required = []
    missing_optional = []
    
    # Check required packages
    required_packages = ['psycopg2']
    for package in required_packages:
        try:
            __import__(package)
        except ImportError:
            missing_required.append(package)
    
    # Check optional packages
    optional_packages = {
        'colorama': 'Colored console output',
        'tqdm': 'Progress bars for long-running operations',
        'etl_progress_tracking': 'ETL progress tracking (local module)'
    }
    
    for package, description in optional_packages.items():
        if package != 'etl_progress_tracking':  # Skip local modules for installation
            try:
                __import__(package)
            except ImportError:
                missing_optional.append((package, description))
    
    # Try to import the ETL progress tracker (local module)
    try:
        from etl_progress_tracking import ETLProgressTracker
        progress_tracker_available = True
    except ImportError:
        if 'etl_progress_tracking' not in [p[0] for p in missing_optional]:
            missing_optional.append(('etl_progress_tracking', 'ETL progress tracking (local module)'))
    
    # If any packages are missing and we're in interactive mode, prompt for installation
    if interactive and (missing_required or missing_optional):
        print("\nMissing Python packages detected:")
        
        if missing_required:
            print("\nRequired packages (script will not run without these):")
            for pkg in missing_required:
                print(f"  - {pkg}")
        
        if missing_optional:
            print("\nOptional packages (enhance functionality):")
            for pkg, desc in missing_optional:
                print(f"  - {pkg}: {desc}")
        
        if missing_required or [p for p in missing_optional if p[0] != 'etl_progress_tracking']:
            install = input("\nWould you like to install the missing packages now? (yes/no) [yes]: ").strip().lower() or "yes"
            
            if install == "yes":
                try:
                    import subprocess
                    
                    # Only install non-local packages
                    packages_to_install = missing_required + [p[0] for p in missing_optional if p[0] != 'etl_progress_tracking']
                    
                    if packages_to_install:
                        print(f"\nInstalling packages: {', '.join(packages_to_install)}")
                        
                        subprocess.check_call([sys.executable, "-m", "pip", "install"] + packages_to_install)
                        print("Package installation completed successfully.")
                        
                        # Now try to import the packages again
                        if 'colorama' in packages_to_install:
                            try:
                                from colorama import init, Fore, Back, Style
                                init()  # Initialize colorama
                                colorama_available = True
                                print("✅ Colorama imported successfully")
                            except ImportError:
                                print("⚠️ Failed to import colorama even after installation")
                        
                        if 'tqdm' in packages_to_install:
                            try:
                                from tqdm import tqdm
                                tqdm_available = True
                                print("✅ tqdm imported successfully")
                            except ImportError:
                                print("⚠️ Failed to import tqdm even after installation")
                except Exception as e:
                    print(f"Error installing packages: {e}")
                    print("Please install the packages manually using:")
                    print(f"pip install {' '.join(missing_required + [p[0] for p in missing_optional if p[0] != 'etl_progress_tracking'])}")
                    
                    if missing_required:
                        print("\nRequired packages are missing. Exiting.")
                        sys.exit(1)
            elif missing_required:
                print("\nRequired packages are missing. Exiting.")
                sys.exit(1)
    
    # Import packages if they're available (even if we didn't need to install them)
    if not tqdm_available:
        try:
            from tqdm import tqdm
            tqdm_available = True
        except ImportError:
            pass
    
    if not colorama_available:
        try:
            from colorama import init, Fore, Back, Style
            init()  # Initialize colorama
            colorama_available = True
        except ImportError:
            pass
    
    if not progress_tracker_available:
        try:
            from etl_progress_tracking import ETLProgressTracker
            progress_tracker_available = True
        except ImportError:
            pass

# Set up logging
log_dir = "logs"
os.makedirs(log_dir, exist_ok=True)
log_file = os.path.join(log_dir, f"interactive_pipeline_{time.strftime('%Y%m%d_%H%M%S')}.log")
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler(log_file),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

# Constants
CHECKPOINT_FILE = ".pipeline_checkpoint.json"
ERROR_LOG_FILE = os.path.join(log_dir, f"error_log_{time.strftime('%Y%m%d_%H%M%S')}.log")

# Global variables
db_config = {
    'host': 'localhost',
    'port': '5432',
    'database': 'ohdsi',
    'user': 'postgres',
    'password': 'acumenus'
}

class ColoredFormatter:
    """Helper class for colored console output."""
    
    @staticmethod
    def info(message):
        """Format an info message."""
        if colorama_available:
            return f"{Fore.CYAN}{message}{Style.RESET_ALL}"
        return message
    
    @staticmethod
    def success(message):
        """Format a success message."""
        if colorama_available:
            return f"{Fore.GREEN}{message}{Style.RESET_ALL}"
        return message
    
    @staticmethod
    def warning(message):
        """Format a warning message."""
        if colorama_available:
            return f"{Fore.YELLOW}{message}{Style.RESET_ALL}"
        return message
    
    @staticmethod
    def error(message):
        """Format an error message."""
        if colorama_available:
            return f"{Fore.RED}{message}{Style.RESET_ALL}"
        return message
    
    @staticmethod
    def highlight(message):
        """Format a highlighted message."""
        if colorama_available:
            return f"{Fore.WHITE}{Back.BLUE}{message}{Style.RESET_ALL}"
        return message
    
    @staticmethod
    def prompt(message):
        """Format a prompt message."""
        if colorama_available:
            return f"{Fore.MAGENTA}{message}{Style.RESET_ALL}"
        return message

def parse_arguments():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(description='Interactive Unified Synthea to OMOP ETL Pipeline')
    
    # Interactive mode
    parser.add_argument('--interactive', action='store_true', default=True,
                        help='Run in interactive mode with user prompts (default: True)')
    parser.add_argument('--non-interactive', action='store_true',
                        help='Run in non-interactive mode (overrides --interactive)')
    
    # Database initialization options
    parser.add_argument('--skip-init', action='store_true',
                        help='Skip database initialization')
    parser.add_argument('--drop-existing', action='store_true',
                        help='Drop existing schemas before initialization')
    
    # Vocabulary options
    parser.add_argument('--skip-vocab', action='store_true',
                        help='Skip vocabulary loading')
    parser.add_argument('--vocab-dir', type=str, default='./vocabulary',
                        help='Directory containing vocabulary files (default: ./vocabulary)')
    
    # ETL options
    parser.add_argument('--skip-etl', action='store_true',
                        help='Skip ETL process')
    parser.add_argument('--synthea-dir', type=str, default='./synthea-output',
                        help='Directory containing Synthea output files (default: ./synthea-output)')
    parser.add_argument('--max-workers', type=int, default=4,
                        help='Maximum number of parallel workers for ETL (default: 4)')
    parser.add_argument('--skip-optimization', action='store_true',
                        help='Skip PostgreSQL optimization')
    parser.add_argument('--skip-validation', action='store_true',
                        help='Skip validation steps')
    
    # Resume options
    parser.add_argument('--resume', action='store_true',
                        help='Resume from last checkpoint')
    parser.add_argument('--force', action='store_true',
                        help='Force execution even if validation fails')
    
    # General options
    parser.add_argument('--debug', action='store_true',
                        help='Enable debug logging')
    parser.add_argument('--track-progress', action='store_true',
                        help='Enable progress tracking for ETL process')
    parser.add_argument('--monitor', action='store_true',
                        help='Launch progress monitoring in a separate terminal (requires --track-progress)')
    
    args = parser.parse_args()
    
    # Override interactive mode if non-interactive is specified
    if args.non_interactive:
        args.interactive = False
    
    return args

def setup_logging(debug=False):
    """Set up logging with appropriate level."""
    if debug:
        logging.getLogger().setLevel(logging.DEBUG)
        logger.debug("Debug logging enabled")
    else:
        logging.getLogger().setLevel(logging.INFO)

def print_banner():
    """Print a welcome banner for the interactive pipeline."""
    banner = """
    ╔════════════════════════════════════════════════════════════════╗
    ║                                                                ║
    ║  Interactive Synthea to OMOP ETL Pipeline                      ║
    ║                                                                ║
    ║  This tool will guide you through the process of:              ║
    ║    1. Validating your environment                              ║
    ║    2. Initializing the OMOP database                           ║
    ║    3. Loading vocabulary data                                  ║
    ║    4. Running the ETL process                                  ║
    ║                                                                ║
    ╚════════════════════════════════════════════════════════════════╝
    """
    print(ColoredFormatter.highlight(banner))

def prompt_user(message, options=None, default=None):
    """
    Prompt the user for input with optional choices.
    
    Args:
        message: The prompt message
        options: Optional list of choices
        default: Default option if user presses Enter
        
    Returns:
        User's response
    """
    if options:
        option_str = "/".join(options)
        if default:
            option_str = option_str.replace(default, f"[{default}]")
        prompt_text = f"{message} ({option_str}): "
    else:
        prompt_text = f"{message}: "
        
    if default:
        prompt_text = prompt_text.replace(f": ", f" (default: {default}): ")
    
    print(ColoredFormatter.prompt(prompt_text), end="")
    response = input().strip()
    
    if not response and default:
        return default
    
    if options and response not in options and response:
        print(ColoredFormatter.warning(f"Invalid option. Please choose from: {', '.join(options)}"))
        return prompt_user(message, options, default)
    
    return response

def load_checkpoint():
    """Load checkpoint data from file."""
    if os.path.exists(CHECKPOINT_FILE):
        try:
            with open(CHECKPOINT_FILE, 'r') as f:
                return json.load(f)
        except Exception as e:
            logger.warning(f"Failed to load checkpoint file: {e}")
    return {
        'completed_steps': [],
        'last_updated': None,
        'environment': {},
        'stats': {}
    }

def save_checkpoint(checkpoint_data):
    """Save checkpoint data to file."""
    checkpoint_data['last_updated'] = datetime.now().isoformat()
    try:
        with open(CHECKPOINT_FILE, 'w') as f:
            json.dump(checkpoint_data, f, indent=2)
    except Exception as e:
        logger.warning(f"Failed to save checkpoint file: {e}")

def mark_step_completed(step_name, stats=None):
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

def is_step_completed(step_name):
    """Check if a step is already completed."""
    checkpoint = load_checkpoint()
    return step_name in checkpoint['completed_steps']

def run_command(command: List[str], description: str, show_output=True) -> bool:
    """Run a command and log the output."""
    logger.info(f"Running {description}...")
    logger.debug(f"Command: {' '.join(command)}")
    
    if show_output:
        print(ColoredFormatter.info(f"\n▶ Running {description}..."))
    
    try:
        process = subprocess.Popen(
            command,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            universal_newlines=True
        )
        
        # Process output in real-time
        for line in process.stdout:
            line = line.strip()
            logger.info(line)
            if show_output:
                print(line)
        
        # Wait for process to complete
        process.wait()
        
        # Check return code
        if process.returncode != 0:
            logger.error(f"{description} failed with return code {process.returncode}")
            # Get error output
            stderr = process.stderr.read()
            logger.error(f"Error output: {stderr}")
            if show_output:
                print(ColoredFormatter.error(f"\n❌ {description} failed with return code {process.returncode}"))
                print(ColoredFormatter.error(f"Error output: {stderr}"))
            return False
        
        logger.info(f"{description} completed successfully")
        if show_output:
            print(ColoredFormatter.success(f"\n✅ {description} completed successfully"))
        return True
    except Exception as e:
        logger.error(f"Error running {description}: {e}")
        if show_output:
            print(ColoredFormatter.error(f"\n❌ Error running {description}: {e}"))
        return False

def validate_database_connection():
    """Validate database connection and credentials."""
    print(ColoredFormatter.info("\n🔍 Validating database connection..."))
    
    # Get database connection parameters from environment or config
    global db_config
    db_config = {
        'host': os.environ.get('DB_HOST', db_config['host']),
        'port': os.environ.get('DB_PORT', db_config['port']),
        'database': os.environ.get('DB_NAME', db_config['database']),
        'user': os.environ.get('DB_USER', db_config['user']),
        'password': os.environ.get('DB_PASSWORD', db_config['password'])
    }
    
    # Try to connect to the database
    try:
        print(f"Connecting to PostgreSQL at {db_config['host']}:{db_config['port']} as {db_config['user']}...")
        conn = psycopg2.connect(
            host=db_config['host'],
            port=db_config['port'],
            database=db_config['database'],
            user=db_config['user'],
            password=db_config['password']
        )
        conn.close()
        print(ColoredFormatter.success("✅ Database connection successful!"))
        return True
    except psycopg2.OperationalError as e:
        print(ColoredFormatter.error(f"❌ Database connection failed: {e}"))
        
        # Check if database exists
        try:
            # Try connecting to postgres database to check if server is running
            temp_config = db_config.copy()
            temp_config['database'] = 'postgres'
            conn = psycopg2.connect(**temp_config)
            conn.close()
            
            print(ColoredFormatter.warning(f"⚠️ PostgreSQL server is running, but database '{db_config['database']}' may not exist."))
            create_db = prompt_user("Would you like to create the database?", ["yes", "no"], "yes")
            
            if create_db.lower() == "yes":
                try:
                    conn = psycopg2.connect(**temp_config)
                    conn.autocommit = True
                    with conn.cursor() as cursor:
                        cursor.execute(f"CREATE DATABASE {db_config['database']}")
                    conn.close()
                    print(ColoredFormatter.success(f"✅ Database '{db_config['database']}' created successfully!"))
                    return True
                except Exception as create_error:
                    print(ColoredFormatter.error(f"❌ Failed to create database: {create_error}"))
        except:
            print(ColoredFormatter.error("❌ PostgreSQL server may not be running or accessible."))
        
        # Prompt for new connection details
        update_connection = prompt_user("Would you like to update the connection details?", ["yes", "no"], "yes")
        if update_connection.lower() == "yes":
            db_config['host'] = prompt_user("Enter database host", default=db_config['host'])
            db_config['port'] = prompt_user("Enter database port", default=db_config['port'])
            db_config['database'] = prompt_user("Enter database name", default=db_config['database'])
            db_config['user'] = prompt_user("Enter database user", default=db_config['user'])
            db_config['password'] = prompt_user("Enter database password", default=db_config['password'])
            
            # Save to environment variables for child processes
            os.environ['DB_HOST'] = db_config['host']
            os.environ['DB_PORT'] = db_config['port']
            os.environ['DB_NAME'] = db_config['database']
            os.environ['DB_USER'] = db_config['user']
            os.environ['DB_PASSWORD'] = db_config['password']
            
            # Try again with new settings
            return validate_database_connection()
        
        return False

def validate_schemas_and_tables():
    """Validate if required schemas and tables exist in the database."""
    print(ColoredFormatter.info("\n🔍 Validating database schemas and tables..."))
    
    try:
        conn = psycopg2.connect(**db_config)
        with conn.cursor() as cursor:
            # Check for required schemas
            cursor.execute("""
            SELECT schema_name 
            FROM information_schema.schemata 
            WHERE schema_name IN ('omop', 'staging', 'vocabulary');
            """)
            existing_schemas = [row[0] for row in cursor.fetchall()]
            
            missing_schemas = []
            for schema in ['omop', 'staging', 'vocabulary']:
                if schema not in existing_schemas:
                    missing_schemas.append(schema)
            
            if missing_schemas:
                print(ColoredFormatter.warning(f"⚠️ Missing schemas: {', '.join(missing_schemas)}"))
                print("These schemas will be created during database initialization.")
            else:
                print(ColoredFormatter.success("✅ All required schemas exist."))
            
            # If omop schema exists, check for required tables
            if 'omop' in existing_schemas:
                cursor.execute("""
                SELECT table_name 
                FROM information_schema.tables 
                WHERE table_schema = 'omop' 
                AND table_type = 'BASE TABLE';
                """)
                existing_tables = [row[0] for row in cursor.fetchall()]
                
                required_tables = [
                    'person', 'observation_period', 'visit_occurrence', 
                    'condition_occurrence', 'drug_exposure', 'procedure_occurrence',
                    'measurement', 'observation', 'concept'
                ]
                
                missing_tables = []
                for table in required_tables:
                    if table not in existing_tables:
                        missing_tables.append(table)
                
                if missing_tables:
                    print(ColoredFormatter.warning(f"⚠️ Missing tables in omop schema: {', '.join(missing_tables)}"))
                    print("These tables will be created during database initialization.")
                else:
                    print(ColoredFormatter.success("✅ All required OMOP tables exist."))
                
                # Check if concept table has data
                if 'concept' in existing_tables:
                    cursor.execute("SELECT COUNT(*) FROM omop.concept")
                    concept_count = cursor.fetchone()[0]
                    if concept_count == 0:
                        print(ColoredFormatter.warning("⚠️ The concept table exists but contains no data."))
                        print("Vocabulary data will need to be loaded.")
                    else:
                        print(ColoredFormatter.success(f"✅ Concept table contains {concept_count:,} concepts."))
        
        conn.close()
        return True
    except Exception as e:
        print(ColoredFormatter.error(f"❌ Error validating schemas and tables: {e}"))
        return False

def validate_vocabulary_files(vocab_dir):
    """Validate if required vocabulary files exist and have the correct format."""
    print(ColoredFormatter.info(f"\n🔍 Validating vocabulary files in {vocab_dir}..."))
    
    # Check if vocabulary directory exists
    if not os.path.isdir(vocab_dir):
        print(ColoredFormatter.error(f"❌ Vocabulary directory not found: {vocab_dir}"))
        
        create_dir = prompt_user("Would you like to create this directory?", ["yes", "no"], "yes")
        if create_dir.lower() == "yes":
            try:
                os.makedirs(vocab_dir, exist_ok=True)
                print(ColoredFormatter.success(f"✅ Created directory: {vocab_dir}"))
            except Exception as e:
                print(ColoredFormatter.error(f"❌ Failed to create directory: {e}"))
                return False
        else:
            return False
    
    # Required vocabulary files
    required_files = [
        "CONCEPT.csv", 
        "CONCEPT_RELATIONSHIP.csv", 
        "VOCABULARY.csv",
        "DOMAIN.csv",
        "CONCEPT_CLASS.csv",
        "RELATIONSHIP.csv"
    ]
    
    # Check for required files
    missing_files = []
    for file in required_files:
        file_path = os.path.join(vocab_dir, file)
        if not os.path.exists(file_path):
            missing_files.append(file)
    
    if missing_files:
        print(ColoredFormatter.warning(f"⚠️ Missing required vocabulary files: {', '.join(missing_files)}"))
        print("These files are needed for proper OMOP CDM functionality.")
        
        download_vocab = prompt_user("Would you like information on how to obtain these files?", ["yes", "no"], "yes")
        if download_vocab.lower() == "yes":
            print(ColoredFormatter.info("\nTo obtain OMOP vocabulary files:"))
            print("1. Visit Athena: https://athena.ohdsi.org/")
            print("2. Create an account and log in")
            print("3. Download the vocabulary files")
            print("4. Extract the files to your vocabulary directory")
            print(f"5. Place the files in: {vocab_dir}")
            print("\nNote: Some vocabularies require a license (e.g., SNOMED CT, RxNorm).")
            print("You may need to provide a UMLS API key for certain vocabularies.")
        
        proceed = prompt_user("Would you like to proceed anyway?", ["yes", "no"], "no")
        if proceed.lower() != "yes":
            return False
    else:
        print(ColoredFormatter.success("✅ All required vocabulary files exist."))
        
        # Validate file formats
        valid_formats = True
        for file in required_files:
            file_path = os.path.join(vocab_dir, file)
            try:
                # First try to detect if the file is tab-delimited
                with open(file_path, 'r') as f:
                    first_line = f.readline().strip()
                    if '\t' in first_line:
                        delimiter = '\t'
                    else:
                        delimiter = ','
                
                # Now read with the detected delimiter
                with open(file_path, 'r') as f:
                    reader = csv.reader(f, delimiter=delimiter)
                    header = next(reader)
                    # Convert header to lowercase for case-insensitive comparison
                    header_lower = [h.lower() for h in header]
                    
                    if file == "CONCEPT.csv" and "concept_id" not in header_lower:
                        print(ColoredFormatter.warning(f"⚠️ {file} may have an invalid format. Could not find 'concept_id' in header."))
                        valid_formats = False
                    elif file == "CONCEPT.csv":
                        print(f"  - {file}: Format detected as {delimiter}-delimited, found concept_id column")
            except Exception as e:
                print(ColoredFormatter.warning(f"⚠️ Could not validate {file}: {e}"))
                valid_formats = False
        
        if valid_formats:
            print(ColoredFormatter.success("✅ All vocabulary files have valid formats."))
    
    return True

def validate_synthea_files(synthea_dir):
    """Validate if required Synthea output files exist and have the correct format."""
    print(ColoredFormatter.info(f"\n🔍 Validating Synthea output files in {synthea_dir}..."))
    
    # Check if Synthea directory exists
    if not os.path.isdir(synthea_dir):
        print(ColoredFormatter.error(f"❌ Synthea output directory not found: {synthea_dir}"))
        
        create_dir = prompt_user("Would you like to create this directory?", ["yes", "no"], "yes")
        if create_dir.lower() == "yes":
            try:
                os.makedirs(synthea_dir, exist_ok=True)
                print(ColoredFormatter.success(f"✅ Created directory: {synthea_dir}"))
            except Exception as e:
                print(ColoredFormatter.error(f"❌ Failed to create directory: {e}"))
                return False
        else:
            return False
    
    # Required Synthea files
    required_files = [
        "patients.csv", 
        "encounters.csv", 
        "conditions.csv", 
        "observations.csv", 
        "procedures.csv", 
        "medications.csv"
    ]
    
    # Check for required files
    missing_files = []
    for file in required_files:
        file_path = os.path.join(synthea_dir, file)
        if not os.path.exists(file_path):
            missing_files.append(file)
    
    if missing_files:
        print(ColoredFormatter.warning(f"⚠️ Missing Synthea files: {', '.join(missing_files)}"))
        
        generate_synthea = prompt_user("Would you like information on how to generate Synthea data?", ["yes", "no"], "yes")
        if generate_synthea.lower() == "yes":
            print(ColoredFormatter.info("\nTo generate Synthea data:"))
            print("1. Clone the Synthea repository: git clone https://github.com/synthetichealth/synthea.git")
            print("2. Build Synthea: ./gradlew build")
            print("3. Run Synthea to generate data: ./run_synthea -p 100")
            print("   (This generates data for 100 patients)")
            print(f"4. Copy the output files to: {synthea_dir}")
        
        proceed = prompt_user("Would you like to proceed anyway?", ["yes", "no"], "no")
        if proceed.lower() != "yes":
            return False
    else:
        print(ColoredFormatter.success("✅ All required Synthea files exist."))
        
        # Count rows in each file
        print("\nSynthea data summary:")
        total_rows = 0
        for file in required_files:
            file_path = os.path.join(synthea_dir, file)
            try:
                with open(file_path, 'r') as f:
                    row_count = sum(1 for _ in f) - 1  # Subtract 1 for header
                print(f"  - {file}: {row_count:,} rows")
                total_rows += row_count
            except Exception as e:
                print(ColoredFormatter.warning(f"  - {file}: Could not count rows - {e}"))
        
        print(ColoredFormatter.success(f"\n✅ Total: {total_rows:,} rows across all files"))
    
    return True

def initialize_database(drop_existing=False, interactive=True) -> bool:
    """Initialize the database with OMOP CDM schema."""
    step_name = "initialize_database"
    
    # Check if this step is already completed
    if is_step_completed(step_name) and not drop_existing:
        if interactive:
            print(ColoredFormatter.info("\n🔍 Database initialization was previously completed."))
            rerun = prompt_user("Would you like to re-run database initialization?", ["yes", "no"], "no")
            if rerun.lower() != "yes":
                print(ColoredFormatter.success("✅ Skipping database initialization."))
                return True
        else:
            logger.info("Database initialization was previously completed. Skipping.")
            return True
    
    logger.info("Initializing database with OMOP CDM schema")
    
    if interactive:
        print(ColoredFormatter.info("\n🔍 Initializing database with OMOP CDM schema..."))
        
        if drop_existing:
            confirm = prompt_user(
                "⚠️ This will DROP existing schemas and ALL DATA. Are you sure?", 
                ["yes", "no"], 
                "no"
            )
            if confirm.lower() != "yes":
                print(ColoredFormatter.warning("❌ Database initialization cancelled."))
                return False
    
    command = ["python3", "init_database.py"]
    if drop_existing:
        command.append("--drop-existing")
    
    success = run_command(command, "database initialization")
    
    if success:
        mark_step_completed(step_name)
    
    return success

def load_vocabulary(vocab_dir: str, interactive=True) -> bool:
    """Load vocabulary data into the database using the enhanced vocabulary loader."""
    step_name = "load_vocabulary"
    
    # Check if this step is already completed
    if is_step_completed(step_name):
        if interactive:
            print(ColoredFormatter.info("\n🔍 Vocabulary loading was previously completed."))
            rerun = prompt_user("Would you like to re-run vocabulary loading?", ["yes", "no"], "no")
            if rerun.lower() != "yes":
                print(ColoredFormatter.success("✅ Skipping vocabulary loading."))
                return True
        else:
            logger.info("Vocabulary loading was previously completed. Skipping.")
            return True
    
    logger.info(f"Loading vocabulary data from {vocab_dir}")
    
    if interactive:
        print(ColoredFormatter.info(f"\n🔍 Loading vocabulary data from {vocab_dir}..."))
        print(ColoredFormatter.info("Using enhanced vocabulary loader with real-time progress tracking."))
        print(ColoredFormatter.warning("⚠️ This process may take a long time depending on the size of your vocabulary files."))
    
    # Build command for enhanced vocabulary loader
    command = ["./enhanced_vocabulary_loader.py", "--vocab-dir", vocab_dir]
    
    if not interactive:
        command.append("--non-interactive")
    
    # Run enhanced vocabulary loader
    success = run_command(command, "enhanced vocabulary loading")
    
    if success:
        # Verify vocabulary was loaded
        try:
            conn = psycopg2.connect(**db_config)
            with conn.cursor() as cursor:
                cursor.execute("SELECT COUNT(*) FROM omop.concept")
                concept_count = cursor.fetchone()[0]
                
                # Get counts for other tables
                cursor.execute("SELECT COUNT(*) FROM omop.concept_relationship")
                relationship_count = cursor.fetchone()[0]
                
                cursor.execute("SELECT COUNT(*) FROM omop.vocabulary")
                vocabulary_count = cursor.fetchone()[0]
                
                if concept_count > 0:
                    logger.info(f"Successfully loaded {concept_count:,} concepts")
                    if interactive:
                        print(ColoredFormatter.success(f"\n✅ Vocabulary loading completed successfully:"))
                        print(f"  - Concepts: {concept_count:,}")
                        print(f"  - Relationships: {relationship_count:,}")
                        print(f"  - Vocabularies: {vocabulary_count:,}")
                else:
                    logger.warning("Vocabulary loading completed but no concepts were found")
                    if interactive:
                        print(ColoredFormatter.warning("⚠️ Vocabulary loading completed but no concepts were found"))
            conn.close()
        except Exception as e:
            logger.error(f"Error verifying vocabulary loading: {e}")
            if interactive:
                print(ColoredFormatter.error(f"❌ Error verifying vocabulary loading: {e}"))
        
        # Save statistics to checkpoint
        mark_step_completed(step_name, {
            "concept_count": concept_count,
            "relationship_count": relationship_count,
            "vocabulary_count": vocabulary_count
        })
    
    return success

def run_etl(synthea_dir: str, max_workers: int, skip_optimization: bool, skip_validation: bool, debug: bool, interactive=True) -> bool:
    """Run the optimized Synthea to OMOP ETL process."""
    step_name = "run_etl"
    
    # Check if this step is already completed
    if is_step_completed(step_name):
        if interactive:
            print(ColoredFormatter.info("\n🔍 ETL process was previously completed."))
            rerun = prompt_user("Would you like to re-run the ETL process?", ["yes", "no"], "no")
            if rerun.lower() != "yes":
                print(ColoredFormatter.success("✅ Skipping ETL process."))
                return True
        else:
            logger.info("ETL process was previously completed. Skipping.")
            return True
    
    logger.info(f"Running optimized ETL process with data from {synthea_dir}")
    
    if interactive:
        print(ColoredFormatter.info(f"\n🔍 Running optimized ETL process with data from {synthea_dir}..."))
        print(ColoredFormatter.warning("⚠️ This process may take a long time depending on the size of your dataset."))
        
        # Validate Synthea files
        if not validate_synthea_files(synthea_dir):
            proceed = prompt_user("Synthea file validation failed. Would you like to proceed anyway?", ["yes", "no"], "no")
            if proceed.lower() != "yes":
                return False
        
        # Confirm ETL settings
        print("\nETL Configuration:")
        print(f"  - Max workers: {max_workers}")
        print(f"  - Skip optimization: {skip_optimization}")
        print(f"  - Skip validation: {skip_validation}")
        print(f"  - Debug mode: {debug}")
        
        confirm = prompt_user("Would you like to proceed with these settings?", ["yes", "no"], "yes")
        if confirm.lower() != "yes":
            print(ColoredFormatter.warning("❌ ETL process cancelled."))
            return False
    
    # Check if Synthea directory exists
    if not os.path.isdir(synthea_dir):
        logger.error(f"Synthea directory not found: {synthea_dir}")
        if interactive:
            print(ColoredFormatter.error(f"❌ Synthea directory not found: {synthea_dir}"))
        return False
    
    # Build command
    command = ["./run_optimized_import.sh", "--synthea-dir", synthea_dir, "--max-workers", str(max_workers)]
    
    if skip_optimization:
        command.append("--skip-optimization")
    
    if skip_validation:
        command.append("--skip-validation")
    
    if debug:
        command.append("--debug")
    
    success = run_command(command, "optimized ETL process")
    
    if success:
        # Verify ETL results
        try:
            conn = psycopg2.connect(**db_config)
            with conn.cursor() as cursor:
                # Get counts from OMOP tables
                tables = [
                    'person', 'observation_period', 'visit_occurrence', 
                    'condition_occurrence', 'drug_exposure', 'procedure_occurrence',
                    'measurement', 'observation'
                ]
                
                if interactive:
                    print("\nETL Results:")
                
                table_counts = {}
                for table in tables:
                    cursor.execute(f"SELECT COUNT(*) FROM omop.{table}")
                    count = cursor.fetchone()[0]
                    table_counts[table] = count
                    if interactive:
                        print(f"  - {table}: {count:,} rows")
                
                # Get source counts
                source_counts = {}
                source_files = {
                    'patients.csv': 'person',
                    'encounters.csv': 'visit_occurrence',
                    'conditions.csv': 'condition_occurrence',
                    'medications.csv': 'drug_exposure',
                    'procedures.csv': 'procedure_occurrence',
                    'observations.csv': 'measurement+observation'
                }
                
                if interactive:
                    print("\nSource to Destination Comparison:")
                
                for source_file, dest_table in source_files.items():
                    file_path = os.path.join(synthea_dir, source_file)
                    if os.path.exists(file_path):
                        with open(file_path, 'r') as f:
                            source_count = sum(1 for _ in f) - 1  # Subtract 1 for header
                        source_counts[source_file] = source_count
                        
                        if dest_table == 'measurement+observation':
                            dest_count = table_counts['measurement'] + table_counts['observation']
                            if interactive:
                                print(f"  - {source_file} ({source_count:,}) → measurement ({table_counts['measurement']:,}) + observation ({table_counts['observation']:,})")
                        else:
                            dest_count = table_counts[dest_table]
                            if interactive:
                                print(f"  - {source_file} ({source_count:,}) → {dest_table} ({dest_count:,})")
            
            conn.close()
            
            # Save statistics to checkpoint
            mark_step_completed(step_name, {
                "table_counts": table_counts,
                "source_counts": source_counts
            })
            
            if interactive:
                print(ColoredFormatter.success("\n✅ ETL process completed successfully!"))
        except Exception as e:
            logger.error(f"Error verifying ETL results: {e}")
            if interactive:
                print(ColoredFormatter.error(f"⚠️ ETL process completed but verification failed: {e}"))
    
    return success

def launch_progress_monitor(interactive=True):
    """Launch the progress monitor in a separate terminal."""
    if not os.path.exists("monitor_etl_progress.sh"):
        logger.warning("Progress monitor script not found: monitor_etl_progress.sh")
        if interactive:
            print(ColoredFormatter.warning("⚠️ Progress monitor script not found: monitor_etl_progress.sh"))
        return False
    
    logger.info("Launching progress monitor in a separate terminal")
    
    if interactive:
        print(ColoredFormatter.info("\n🔍 Launching progress monitor..."))
    
    # Try different terminal emulators
    terminal_found = False
    
    for terminal, command in [
        ("gnome-terminal", ["gnome-terminal", "--", "bash", "-c", "./monitor_etl_progress.sh; echo 'Press Enter to close'; read"]),
        ("xterm", ["xterm", "-e", "./monitor_etl_progress.sh; echo 'Press Enter to close'; read"]),
        ("konsole", ["konsole", "--new-tab", "-e", "./monitor_etl_progress.sh; echo 'Press Enter to close'; read"])
    ]:
        try:
            if shutil.which(terminal):
                subprocess.Popen(command)
                terminal_found = True
                logger.info(f"Launched progress monitor using {terminal}")
                if interactive:
                    print(ColoredFormatter.success(f"✅ Launched progress monitor using {terminal}"))
                break
        except Exception as e:
            logger.warning(f"Failed to launch {terminal}: {e}")
    
    if not terminal_found:
        logger.warning("Could not find a suitable terminal emulator")
        if interactive:
            print(ColoredFormatter.warning("⚠️ Could not find a suitable terminal emulator."))
            print("Please run ./monitor_etl_progress.sh manually in another terminal.")
    
    return terminal_found

def display_summary(checkpoint_data, interactive=True):
    """Display a summary of the pipeline execution."""
    if not checkpoint_data.get('completed_steps'):
        logger.info("No steps have been completed yet")
        if interactive:
            print(ColoredFormatter.info("\nNo steps have been completed yet."))
        return
    
    logger.info("Displaying pipeline execution summary")
    
    if interactive:
        print(ColoredFormatter.highlight("\n╔════════════════════════════════════════════════════════════════╗"))
        print(ColoredFormatter.highlight("║                    Pipeline Execution Summary                   ║"))
        print(ColoredFormatter.highlight("╚════════════════════════════════════════════════════════════════╝"))
        
        print("\nCompleted Steps:")
        for step in checkpoint_data.get('completed_steps', []):
            print(ColoredFormatter.success(f"  ✅ {step}"))
        
        # Display statistics if available
        if 'stats' in checkpoint_data:
            stats = checkpoint_data['stats']
            
            if 'load_vocabulary' in stats:
                vocab_stats = stats['load_vocabulary']
                print("\nVocabulary Statistics:")
                print(f"  - Concepts loaded: {vocab_stats.get('concept_count', 'unknown'):,}")
            
            if 'run_etl' in stats:
                etl_stats = stats['run_etl']
                
                if 'table_counts' in etl_stats:
                    print("\nOMOP Table Counts:")
                    for table, count in etl_stats['table_counts'].items():
                        print(f"  - {table}: {int(count):,} rows")
                
                if 'source_counts' in etl_stats and 'table_counts' in etl_stats:
                    print("\nData Transformation Summary:")
                    source_counts = etl_stats['source_counts']
                    table_counts = etl_stats['table_counts']
                    
                    source_total = sum(int(count) for count in source_counts.values())
                    dest_total = sum(int(count) for count in table_counts.values())
                    
                    print(f"  - Total source rows: {source_total:,}")
                    print(f"  - Total destination rows: {dest_total:,}")
                    
                    if source_total > 0:
                        ratio = dest_total / source_total
                        print(f"  - Transformation ratio: {ratio:.2f}x")
        
        # Display last updated time
        if checkpoint_data.get('last_updated'):
            try:
                last_updated = datetime.fromisoformat(checkpoint_data['last_updated'])
                print(f"\nLast updated: {last_updated.strftime('%Y-%m-%d %H:%M:%S')}")
            except:
                pass
    
    else:
        logger.info(f"Completed steps: {', '.join(checkpoint_data.get('completed_steps', []))}")
        
        if 'stats' in checkpoint_data and 'run_etl' in checkpoint_data['stats']:
            etl_stats = checkpoint_data['stats']['run_etl']
            if 'table_counts' in etl_stats:
                for table, count in etl_stats['table_counts'].items():
                    logger.info(f"{table}: {int(count):,} rows")

def main():
    """Main function to run the interactive unified pipeline."""
    start_time = time.time()
    
    # Parse command line arguments
    args = parse_arguments()
    
    # Set up logging
    setup_logging(args.debug)
    
    # Print welcome banner in interactive mode
    if args.interactive:
        print_banner()
    
    # Check and install dependencies if needed
    check_and_install_dependencies(args.interactive)
    
    logger.info("Starting interactive unified Synthea to OMOP pipeline")
    
    # Load checkpoint data
    checkpoint_data = load_checkpoint()
    
    # Check if resuming from checkpoint
    if args.resume and checkpoint_data.get('completed_steps'):
        logger.info(f"Resuming from checkpoint with completed steps: {checkpoint_data['completed_steps']}")
        if args.interactive:
            print(ColoredFormatter.info("\n🔄 Resuming from previous checkpoint..."))
            print(f"Completed steps: {', '.join(checkpoint_data['completed_steps'])}")
    
    # Validate environment in interactive mode
    if args.interactive and not args.force:
        # Validate database connection
        if not validate_database_connection():
            print(ColoredFormatter.error("\n❌ Database validation failed. Please fix the issues and try again."))
            return 1
        
        # Validate schemas and tables
        validate_schemas_and_tables()
        
        # Validate vocabulary files if not skipping vocabulary loading
        if not args.skip_vocab:
            validate_vocabulary_files(args.vocab_dir)
        
        # Validate Synthea files if not skipping ETL
        if not args.skip_etl:
            validate_synthea_files(args.synthea_dir)
    
    # Step 1: Initialize database (if not skipped)
    if not args.skip_init:
        if not initialize_database(args.drop_existing, args.interactive):
            logger.error("Database initialization failed")
            return 1
    else:
        logger.info("Skipping database initialization")
        if args.interactive:
            print(ColoredFormatter.info("\n⏩ Skipping database initialization as requested."))
    
    # Step 2: Load vocabulary (if not skipped)
    if not args.skip_vocab:
        if not load_vocabulary(args.vocab_dir, args.interactive):
            logger.error("Vocabulary loading failed")
            return 1
    else:
        logger.info("Skipping vocabulary loading")
        if args.interactive:
            print(ColoredFormatter.info("\n⏩ Skipping vocabulary loading as requested."))
    
    # Step 3: Run ETL process (if not skipped)
    if not args.skip_etl:
        # Launch progress monitor if requested
        if args.monitor and args.track_progress:
            launch_progress_monitor(args.interactive)
        
        if not run_etl(args.synthea_dir, args.max_workers, args.skip_optimization, args.skip_validation, args.debug, args.interactive):
            logger.error("ETL process failed")
            return 1
    else:
        logger.info("Skipping ETL process")
        if args.interactive:
            print(ColoredFormatter.info("\n⏩ Skipping ETL process as requested."))
    
    # Calculate total duration
    end_time = time.time()
    duration = end_time - start_time
    hours, remainder = divmod(int(duration), 3600)
    minutes, seconds = divmod(remainder, 60)
    
    logger.info(f"Interactive unified pipeline completed in {hours:02d}:{minutes:02d}:{seconds:02d}")
    
    # Display summary in interactive mode
    if args.interactive:
        # Reload checkpoint data to get the latest stats
        checkpoint_data = load_checkpoint()
        display_summary(checkpoint_data)
        
        print(ColoredFormatter.highlight(f"\n✨ Pipeline completed in {hours:02d}:{minutes:02d}:{seconds:02d}! ✨"))
        
        # Suggest next steps
        print("\nNext steps you might want to take:")
        print("  1. Explore the data using SQL queries")
        print("  2. Run Achilles for data characterization: ./run_achilles.sh")
        print("  3. Start the visualization dashboard: ./run_dashboard.sh")
    
    return 0

if __name__ == "__main__":
    try:
        sys.exit(main())
    except KeyboardInterrupt:
        print("\nPipeline interrupted by user.")
        sys.exit(130)
    except Exception as e:
        logger.error(f"Unhandled exception: {e}", exc_info=True)
        print(f"\nAn unexpected error occurred: {e}")
        sys.exit(1)
