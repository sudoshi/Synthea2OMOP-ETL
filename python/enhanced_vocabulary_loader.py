#!/usr/bin/env python3
"""
enhanced_vocabulary_loader.py - Enhanced vocabulary loading script for OMOP CDM

This script provides an improved vocabulary loading process with:
1. Real-time progress tracking
2. Detailed validation
3. Comprehensive reporting
4. Robust error handling
5. Resumable processing
"""

import argparse
import csv
import logging
import os
import sys
import time
import psycopg2
import subprocess
from datetime import datetime
from pathlib import Path
import json
import shutil

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
    init()  # Initialize colorama
    colorama_available = True
except ImportError:
    colorama_available = False

# Set up logging
log_dir = "logs"
os.makedirs(log_dir, exist_ok=True)
log_file = os.path.join(log_dir, f"vocabulary_loading_{time.strftime('%Y%m%d_%H%M%S')}.log")
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
CHECKPOINT_FILE = ".vocabulary_checkpoint.json"
REQUIRED_VOCAB_FILES = [
    "CONCEPT.csv",
    "CONCEPT_RELATIONSHIP.csv",
    "VOCABULARY.csv",
    "DOMAIN.csv",
    "CONCEPT_CLASS.csv",
    "RELATIONSHIP.csv",
    "CONCEPT_ANCESTOR.csv",
    "CONCEPT_SYNONYM.csv",
    "DRUG_STRENGTH.csv"
]

EXPECTED_ROW_COUNTS = {
    "concept": 500000,
    "concept_relationship": 1000000,
    "vocabulary": 50,
    "domain": 20,
    "concept_class": 100,
    "relationship": 20,
    "concept_ancestor": 500000,
    "concept_synonym": 300000,
    "drug_strength": 50000
}

# Database configuration
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
            return f"{Fore.WHITE}{Fore.BLUE}{message}{Style.RESET_ALL}"
        return message

def parse_arguments():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(description='Enhanced Vocabulary Loading for OMOP CDM')
    
    parser.add_argument('--vocab-dir', type=str, default='./vocabulary',
                        help='Directory containing vocabulary files (default: ./vocabulary)')
    parser.add_argument('--processed-dir', type=str, default='./vocabulary_processed',
                        help='Directory for processed vocabulary files (default: ./vocabulary_processed)')
    parser.add_argument('--resume', action='store_true',
                        help='Resume from last checkpoint')
    parser.add_argument('--force', action='store_true',
                        help='Force execution even if validation fails')
    parser.add_argument('--debug', action='store_true',
                        help='Enable debug logging')
    parser.add_argument('--batch-size', type=int, default=50000,
                        help='Batch size for loading data (default: 50000)')
    parser.add_argument('--skip-validation', action='store_true',
                        help='Skip validation steps')
    parser.add_argument('--skip-cpt4', action='store_true',
                        help='Skip CPT4 processing')
    parser.add_argument('--drop-tables', action='store_true',
                        help='Drop existing tables before creating new ones')
    parser.add_argument('--verbose', action='store_true',
                        help='Enable verbose output')
    
    return parser.parse_args()

def setup_logging(debug=False):
    """Set up logging with appropriate level."""
    if debug:
        logging.getLogger().setLevel(logging.DEBUG)
        logger.debug("Debug logging enabled")
    else:
        logging.getLogger().setLevel(logging.INFO)

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

def count_rows(file_path, delimiter=','):
    """Count the number of rows in a CSV file."""
    try:
        with open(file_path, 'r') as f:
            # Skip header
            next(f)
            count = sum(1 for _ in f)
        return count
    except Exception as e:
        logger.error(f"Error counting rows in {file_path}: {e}")
        return 0

def detect_delimiter(file_path):
    """Detect the delimiter used in a CSV file."""
    try:
        with open(file_path, 'r', encoding='utf-8', errors='replace') as f:
            first_line = f.readline().strip()
            if '\t' in first_line:
                return '\t'
            elif ',' in first_line:
                return ','
            else:
                return ','  # Default to comma if can't determine
    except Exception as e:
        logger.error(f"Error detecting delimiter in {file_path}: {e}")
        return ','

def get_file_size(file_path):
    """Get the size of a file in a human-readable format."""
    size_bytes = os.path.getsize(file_path)
    for unit in ['B', 'KB', 'MB', 'GB']:
        if size_bytes < 1024.0 or unit == 'GB':
            break
        size_bytes /= 1024.0
    return f"{size_bytes:.2f} {unit}"

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
    except Exception as e:
        print(ColoredFormatter.error(f"❌ Database connection failed: {e}"))
        return False

def validate_vocabulary_files(vocab_dir):
    """Validate if required vocabulary files exist and have the correct format."""
    print(ColoredFormatter.info(f"\n🔍 Validating vocabulary files in {vocab_dir}..."))
    
    # Check if vocabulary directory exists
    if not os.path.isdir(vocab_dir):
        print(ColoredFormatter.error(f"❌ Vocabulary directory not found: {vocab_dir}"))
        return False
    
    # Check for required files
    missing_files = []
    file_stats = {}
    
    for file in REQUIRED_VOCAB_FILES:
        file_path = os.path.join(vocab_dir, file)
        if not os.path.exists(file_path):
            missing_files.append(file)
            continue
        
        # Get file stats
        file_size = get_file_size(file_path)
        delimiter = detect_delimiter(file_path)
        row_count = count_rows(file_path, delimiter)
        
        file_stats[file] = {
            'size': file_size,
            'delimiter': delimiter,
            'row_count': row_count
        }
        
        print(f"  - {file}: {file_size}, {row_count:,} rows, {delimiter}-delimited")
    
    if missing_files:
        print(ColoredFormatter.warning(f"⚠️ Missing required vocabulary files: {', '.join(missing_files)}"))
        print("These files are needed for proper OMOP CDM functionality.")
        return False
    else:
        total_rows = sum(stats['row_count'] for stats in file_stats.values())
        print(ColoredFormatter.success(f"✅ All required vocabulary files exist. Total: {total_rows:,} rows"))
    
    return True, file_stats

def process_cpt4(vocab_dir, processed_dir, umls_api_key=None):
    """Process CPT4 codes with UMLS API key."""
    step_name = "process_cpt4"
    
    if is_step_completed(step_name):
        print(ColoredFormatter.info("✅ CPT4 processing was previously completed. Skipping."))
        return True
    
    print(ColoredFormatter.info("\n🔍 Processing CPT4 codes..."))
    
    # Check if UMLS API key is available
    if not umls_api_key:
        umls_api_key_file = "./secrets/omop_vocab/UMLS_API_KEY"
        if os.path.exists(umls_api_key_file):
            with open(umls_api_key_file, 'r') as f:
                umls_api_key = f.read().strip()
            print(ColoredFormatter.success(f"✅ Found UMLS API key in {umls_api_key_file}"))
        else:
            print(ColoredFormatter.warning("⚠️ UMLS API key not found. CPT4 processing will be skipped."))
            return True
    
    # Run CPT4 processing script
    try:
        print(ColoredFormatter.info("Starting CPT4 processing with UMLS API key..."))
        process = subprocess.Popen(
            ["./vocabulary/process_cpt4.sh", umls_api_key],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            universal_newlines=True
        )
        
        # Process output in real-time
        for line in process.stdout:
            line = line.strip()
            logger.info(line)
            print(line)
        
        # Wait for process to complete
        process.wait()
        
        if process.returncode != 0:
            stderr = process.stderr.read()
            logger.error(f"CPT4 processing failed: {stderr}")
            print(ColoredFormatter.error(f"❌ CPT4 processing failed: {stderr}"))
            return False
        
        print(ColoredFormatter.success("✅ CPT4 processing completed successfully"))
        mark_step_completed(step_name)
        return True
    except Exception as e:
        logger.error(f"Error processing CPT4 codes: {e}")
        print(ColoredFormatter.error(f"❌ Error processing CPT4 codes: {e}"))
        return False

def clean_vocabulary_files(vocab_dir, processed_dir):
    """Clean vocabulary files and prepare them for loading."""
    step_name = "clean_vocabulary_files"
    
    if is_step_completed(step_name):
        print(ColoredFormatter.info("✅ Vocabulary cleaning was previously completed. Skipping."))
        return True
    
    print(ColoredFormatter.info("\n🔍 Cleaning vocabulary files..."))
    
    # Create processed directory if it doesn't exist
    os.makedirs(processed_dir, exist_ok=True)
    
    # Execute clean_vocab.py script
    clean_vocab_path = os.path.join(vocab_dir, "clean_vocab.py")
    if os.path.exists(clean_vocab_path):
        try:
            print(ColoredFormatter.info("Running clean_vocab.py script..."))
            subprocess.run(
                [sys.executable, clean_vocab_path, vocab_dir, processed_dir],
                check=True
            )
            print(ColoredFormatter.success("✅ clean_vocab.py executed successfully"))
            
            # Copy other necessary files that might not be copied by clean_vocab.py
            for file in ['cpt.sh', 'cpt4.jar', 'readme.txt', 'clean_vocab.py', 'cpt.bat', 'process_cpt4.sh', 'load_vocabulary.sh', 'README.md']:
                source_path = os.path.join(vocab_dir, file)
                dest_path = os.path.join(processed_dir, file)
                if os.path.exists(source_path) and not os.path.exists(dest_path):
                    shutil.copy2(source_path, dest_path)
                    print(f"Copied {file}")
            
            mark_step_completed(step_name)
            return True
        except subprocess.CalledProcessError as e:
            print(ColoredFormatter.error(f"❌ Error executing clean_vocab.py: {e}"))
            print(ColoredFormatter.info("Falling back to manual processing..."))
    else:
        print(ColoredFormatter.info("clean_vocab.py not found. Using manual processing..."))
    
    # Fall back to manual processing if clean_vocab.py fails or doesn't exist
    # Get list of CSV files in vocabulary directory
    csv_files = [f for f in os.listdir(vocab_dir) if f.endswith('.csv')]
    print(f"Found {len(csv_files)} CSV files to process")
    
    # Process each file
    processed_files = []
    for file in csv_files:
        source_path = os.path.join(vocab_dir, file)
        dest_path = os.path.join(processed_dir, file)
        
        # Check if file is already processed
        if os.path.exists(dest_path):
            print(f"Skipping {file} (already processed)")
            processed_files.append(file)
            continue
        
        try:
            # Detect delimiter
            delimiter = detect_delimiter(source_path)
            
            # Read file with pandas if available
            if pandas_available:
                df = pd.read_csv(source_path, delimiter=delimiter, low_memory=False)
                df.to_csv(dest_path, index=False)
            else:
                # Manual processing if pandas is not available
                with open(source_path, 'r', encoding='utf-8', errors='replace') as infile, open(dest_path, 'w', newline='', encoding='utf-8') as outfile:
                    reader = csv.reader(infile, delimiter=delimiter)
                    writer = csv.writer(outfile)
                    for row in reader:
                        writer.writerow(row)
            
            processed_files.append(file)
        except Exception as e:
            logger.error(f"Error processing {file}: {e}")
            print(ColoredFormatter.error(f"❌ Error processing {file}: {e}"))
    
    # Copy other necessary files
    for file in ['cpt.sh', 'cpt4.jar', 'readme.txt', 'clean_vocab.py', 'cpt.bat', 'process_cpt4.sh', 'load_vocabulary.sh', 'README.md']:
        source_path = os.path.join(vocab_dir, file)
        dest_path = os.path.join(processed_dir, file)
        if os.path.exists(source_path):
            shutil.copy2(source_path, dest_path)
            print(f"Copied {file}")
    
    print(ColoredFormatter.success("✅ All vocabulary files processed successfully"))
    mark_step_completed(step_name, {"processed_files": processed_files})
    return True

def create_schemas_and_tables(drop_tables=False):
    """Create necessary schemas and tables in the database."""
    step_name = "create_schemas_and_tables"
    
    if is_step_completed(step_name) and not drop_tables:
        print(ColoredFormatter.info("✅ Schemas and tables were previously created. Skipping."))
        return True
    
    print(ColoredFormatter.info("\n🔍 Creating schemas and tables..."))
    
    try:
        conn = psycopg2.connect(**db_config)
        conn.autocommit = True
        with conn.cursor() as cursor:
            # Create schemas if they don't exist
            cursor.execute("CREATE SCHEMA IF NOT EXISTS omop;")
            cursor.execute("CREATE SCHEMA IF NOT EXISTS vocabulary;")
            
            # Drop tables if requested
            if drop_tables:
                print(ColoredFormatter.info("Dropping existing tables..."))
                cursor.execute("""
                DROP TABLE IF EXISTS 
                    omop.concept,
                    omop.vocabulary,
                    omop.domain,
                    omop.concept_class,
                    omop.relationship,
                    omop.concept_relationship,
                    omop.concept_ancestor,
                    omop.concept_synonym,
                    omop.drug_strength
                CASCADE;
                """)
            
            # Create vocabulary tables
            cursor.execute("""
            CREATE TABLE IF NOT EXISTS omop.concept (
                concept_id INTEGER PRIMARY KEY,
                concept_name VARCHAR(1000) NOT NULL,
                domain_id VARCHAR(20) NOT NULL,
                vocabulary_id VARCHAR(20) NOT NULL,
                concept_class_id VARCHAR(20) NOT NULL,
                standard_concept VARCHAR(1),
                concept_code VARCHAR(50) NOT NULL,
                valid_start_date DATE NOT NULL,
                valid_end_date DATE NOT NULL,
                invalid_reason VARCHAR(1)
            );
            """)
            
            cursor.execute("""
            CREATE TABLE IF NOT EXISTS omop.vocabulary (
                vocabulary_id VARCHAR(20) PRIMARY KEY,
                vocabulary_name VARCHAR(255) NOT NULL,
                vocabulary_reference VARCHAR(255),
                vocabulary_version VARCHAR(255),
                vocabulary_concept_id INTEGER
            );
            """)
            
            cursor.execute("""
            CREATE TABLE IF NOT EXISTS omop.domain (
                domain_id VARCHAR(20) PRIMARY KEY,
                domain_name VARCHAR(255) NOT NULL,
                domain_concept_id INTEGER
            );
            """)
            
            cursor.execute("""
            CREATE TABLE IF NOT EXISTS omop.concept_class (
                concept_class_id VARCHAR(20) PRIMARY KEY,
                concept_class_name VARCHAR(255) NOT NULL,
                concept_class_concept_id INTEGER
            );
            """)
            
            cursor.execute("""
            CREATE TABLE IF NOT EXISTS omop.relationship (
                relationship_id VARCHAR(20) PRIMARY KEY,
                relationship_name VARCHAR(255) NOT NULL,
                is_hierarchical VARCHAR(1) NOT NULL,
                defines_ancestry VARCHAR(1) NOT NULL,
                reverse_relationship_id VARCHAR(20) NOT NULL,
                relationship_concept_id INTEGER
            );
            """)
            
            cursor.execute("""
            CREATE TABLE IF NOT EXISTS omop.concept_relationship (
                concept_id_1 INTEGER NOT NULL,
                concept_id_2 INTEGER NOT NULL,
                relationship_id VARCHAR(20) NOT NULL,
                valid_start_date DATE NOT NULL,
                valid_end_date DATE NOT NULL,
                invalid_reason VARCHAR(1),
                PRIMARY KEY (concept_id_1, concept_id_2, relationship_id)
            );
            """)
            
            cursor.execute("""
            CREATE TABLE IF NOT EXISTS omop.concept_ancestor (
                ancestor_concept_id INTEGER NOT NULL,
                descendant_concept_id INTEGER NOT NULL,
                min_levels_of_separation INTEGER NOT NULL,
                max_levels_of_separation INTEGER NOT NULL,
                PRIMARY KEY (ancestor_concept_id, descendant_concept_id)
            );
            """)
            
            cursor.execute("""
            CREATE TABLE IF NOT EXISTS omop.concept_synonym (
                concept_id INTEGER NOT NULL,
                concept_synonym_name VARCHAR(1000) NOT NULL,
                language_concept_id INTEGER NOT NULL
            );
            """)
            
            cursor.execute("""
            CREATE TABLE IF NOT EXISTS omop.drug_strength (
                drug_concept_id INTEGER NOT NULL,
                ingredient_concept_id INTEGER NOT NULL,
                amount_value NUMERIC,
                amount_unit_concept_id INTEGER,
                numerator_value NUMERIC,
                numerator_unit_concept_id INTEGER,
                denominator_value NUMERIC,
                denominator_unit_concept_id INTEGER,
                box_size INTEGER,
                valid_start_date DATE NOT NULL,
                valid_end_date DATE NOT NULL,
                invalid_reason VARCHAR(1)
            );
            """)
        
        conn.close()
        print(ColoredFormatter.success("✅ Schemas and tables created successfully"))
        mark_step_completed(step_name)
        return True
    except Exception as e:
        logger.error(f"Error creating schemas and tables: {e}")
        print(ColoredFormatter.error(f"❌ Error creating schemas and tables: {e}"))
        return False

def load_vocabulary_file(file_name, processed_dir, batch_size=50000):
    """Load a vocabulary file into the database."""
    file_path = os.path.join(processed_dir, file_name)
    table_name = file_name.split('.')[0].lower()
    
    print(ColoredFormatter.info(f"\n🔍 Loading {file_name} into omop.{table_name}..."))
    
    try:
        # Detect delimiter and count rows
        delimiter = detect_delimiter(file_path)
        total_rows = count_rows(file_path, delimiter)
        
        # Connect to database
        conn = psycopg2.connect(**db_config)
        conn.autocommit = False
        
        # Check if table already has the correct number of rows
        with conn.cursor() as cursor:
            cursor.execute(f"SELECT COUNT(*) FROM omop.{table_name}")
            db_count = cursor.fetchone()[0]
            
            if db_count == total_rows:
                print(ColoredFormatter.info(f"✅ Table omop.{table_name} already has {db_count:,} rows (matches file). Skipping."))
                conn.close()
                return True, {"file": file_name, "rows": db_count}
        
        # Truncate table if it exists
        with conn.cursor() as cursor:
            cursor.execute(f"TRUNCATE TABLE omop.{table_name};")
        conn.commit()
        
        # For drug_strength table, use COPY command with NULL handling
        if table_name == 'drug_strength':
            try:
                # Create a temporary file for COPY
                temp_file = f"{file_path}.tmp"
                
                # Process the file to handle empty values
                with open(file_path, 'r', encoding='utf-8', errors='replace') as infile, open(temp_file, 'w', encoding='utf-8', newline='') as outfile:
                    reader = csv.reader(infile, delimiter=delimiter)
                    writer = csv.writer(outfile, delimiter='\t')
                    
                    # Skip header (don't write it to the temp file)
                    header = next(reader)
                    
                    # Process rows
                    for row in reader:
                        writer.writerow(row)
                
                # Use COPY command with NULL handling
                with conn.cursor() as cursor, open(temp_file, 'r', encoding='utf-8') as f:
                    cursor.copy_expert(f"COPY omop.{table_name} FROM STDIN WITH DELIMITER E'\\t' NULL ''", f)
                conn.commit()
                
                # Clean up temporary file
                os.remove(temp_file)
                
                # Verify row count
                with conn.cursor() as cursor:
                    cursor.execute(f"SELECT COUNT(*) FROM omop.{table_name}")
                    db_count = cursor.fetchone()[0]
                
                # Check if row counts match
                if db_count == total_rows:
                    print(ColoredFormatter.success(f"✅ Successfully loaded {db_count:,} rows into omop.{table_name} using COPY"))
                    conn.close()
                    return True, {"file": file_name, "rows": db_count}
                else:
                    print(ColoredFormatter.warning(f"⚠️ Row count mismatch: {total_rows:,} in file, {db_count:,} in database"))
                    conn.close()
                    return False, {"file": file_name, "rows": db_count, "expected": total_rows}
                
            except Exception as e:
                logger.error(f"Error using COPY for {file_name}: {e}")
                print(ColoredFormatter.warning(f"⚠️ COPY method failed, falling back to executemany: {e}"))
                # Fall back to executemany method below
        
        # Read file and insert data in batches
        with open(file_path, 'r', encoding='utf-8', errors='replace') as f:
            reader = csv.reader(f, delimiter=delimiter)
            header = next(reader)
            
            # Create placeholders for INSERT statement
            placeholders = ', '.join(['%s'] * len(header))
            
            # Create INSERT statement
            insert_query = f"INSERT INTO omop.{table_name} ({', '.join(header)}) VALUES ({placeholders})"
            
            # Process data in batches
            batch = []
            processed_rows = 0
            
            # Set up progress bar if tqdm is available
            if tqdm_available:
                pbar = tqdm(total=total_rows, desc=f"Loading {table_name}", unit="rows")
            
            for row in reader:
                # Process row to handle empty values for numeric fields
                processed_row = []
                for val in row:
                    if val == '':
                        processed_row.append(None)  # None becomes NULL in SQL
                    else:
                        processed_row.append(val)
                
                batch.append(processed_row)
                processed_rows += 1
                
                if len(batch) >= batch_size:
                    with conn.cursor() as cursor:
                        cursor.executemany(insert_query, batch)
                    conn.commit()
                    
                    if tqdm_available:
                        pbar.update(len(batch))
                    else:
                        print(f"Processed {processed_rows:,}/{total_rows:,} rows ({processed_rows/total_rows*100:.1f}%)")
                    
                    batch = []
            
            # Insert remaining rows
            if batch:
                with conn.cursor() as cursor:
                    cursor.executemany(insert_query, batch)
                conn.commit()
                
                if tqdm_available:
                    pbar.update(len(batch))
            
            if tqdm_available:
                pbar.close()
        
        # Verify row count
        with conn.cursor() as cursor:
            cursor.execute(f"SELECT COUNT(*) FROM omop.{table_name}")
            db_count = cursor.fetchone()[0]
        
        conn.close()
        
        # Check if row counts match
        if db_count == total_rows:
            print(ColoredFormatter.success(f"✅ Successfully loaded {db_count:,} rows into omop.{table_name}"))
            return True, {"file": file_name, "rows": db_count}
        else:
            print(ColoredFormatter.warning(f"⚠️ Row count mismatch: {total_rows:,} in file, {db_count:,} in database"))
            return False, {"file": file_name, "rows": db_count, "expected": total_rows}
    except Exception as e:
        logger.error(f"Error loading {file_name}: {e}")
        print(ColoredFormatter.error(f"❌ Error loading {file_name}: {e}"))
        return False, {"file": file_name, "error": str(e)}

def load_all_vocabulary_files(processed_dir, batch_size=50000):
    """Load all vocabulary files into the database."""
    step_name = "load_vocabulary_files"
    
    # Load checkpoint to see which files have been loaded
    checkpoint = load_checkpoint()
    loaded_files = checkpoint.get('loaded_files', [])
    
    if is_step_completed(step_name):
        print(ColoredFormatter.info("✅ Vocabulary files were previously loaded. Skipping."))
        return True, {}
    
    print(ColoredFormatter.info("\n🔍 Loading vocabulary files into database..."))
    
    # Files to load in order
    files_to_load = [
        "VOCABULARY.csv",
        "DOMAIN.csv",
        "CONCEPT_CLASS.csv",
        "RELATIONSHIP.csv",
        "CONCEPT.csv",
        "CONCEPT_RELATIONSHIP.csv",
        "CONCEPT_ANCESTOR.csv",
        "CONCEPT_SYNONYM.csv",
        "DRUG_STRENGTH.csv"
    ]
    
    # Load each file
    results = {}
    success = True
    
    for file in files_to_load:
        # Skip if file has already been loaded successfully
        if file in loaded_files:
            print(ColoredFormatter.info(f"✅ {file} was previously loaded successfully. Skipping."))
            continue
            
        file_success, file_stats = load_vocabulary_file(file, processed_dir, batch_size)
        results[file] = file_stats
        
        if file_success:
            # Add to loaded files in checkpoint
            if file not in loaded_files:
                loaded_files.append(file)
                checkpoint['loaded_files'] = loaded_files
                save_checkpoint(checkpoint)
        else:
            success = False
            break  # Stop on first failure
    
    if success:
        print(ColoredFormatter.success("\n✅ All vocabulary files loaded successfully"))
        mark_step_completed(step_name, results)
    else:
        print(ColoredFormatter.warning("\n⚠️ Some vocabulary files failed to load correctly"))
    
    return success, results

def create_indexes():
    """Create indexes on vocabulary tables."""
    step_name = "create_indexes"
    
    if is_step_completed(step_name):
        print(ColoredFormatter.info("✅ Indexes were previously created. Skipping."))
        return True
    
    print(ColoredFormatter.info("\n🔍 Creating indexes on vocabulary tables..."))
    
    try:
        conn = psycopg2.connect(**db_config)
        conn.autocommit = True
        
        with conn.cursor() as cursor:
            # Create indexes on concept table
            print("Creating indexes on concept table...")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_concept_code ON omop.concept (concept_code);")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_concept_vocab ON omop.concept (vocabulary_id);")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_concept_domain ON omop.concept (domain_id);")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_concept_class ON omop.concept (concept_class_id);")
            
            # Create indexes on concept_relationship table
            print("Creating indexes on concept_relationship table...")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_concept_rel_1 ON omop.concept_relationship (concept_id_1);")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_concept_rel_2 ON omop.concept_relationship (concept_id_2);")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_concept_rel_id ON omop.concept_relationship (relationship_id);")
            
            # Create indexes on concept_ancestor table
            print("Creating indexes on concept_ancestor table...")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_concept_ancestor_1 ON omop.concept_ancestor (ancestor_concept_id);")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_concept_ancestor_2 ON omop.concept_ancestor (descendant_concept_id);")
            
            # Create indexes on concept_synonym table
            print("Creating indexes on concept_synonym table...")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_concept_synonym ON omop.concept_synonym (concept_id);")
            
            # Create indexes on drug_strength table
            print("Creating indexes on drug_strength table...")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_drug_strength_1 ON omop.drug_strength (drug_concept_id);")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_drug_strength_2 ON omop.drug_strength (ingredient_concept_id);")
        
        conn.close()
        print(ColoredFormatter.success("✅ Indexes created successfully"))
        mark_step_completed(step_name)
        return True
    except Exception as e:
        logger.error(f"Error creating indexes: {e}")
        print(ColoredFormatter.error(f"❌ Error creating indexes: {e}"))
        return False

def validate_loaded_data():
    """Validate the loaded vocabulary data."""
    step_name = "validate_loaded_data"
    
    if is_step_completed(step_name):
        print(ColoredFormatter.info("✅ Data validation was previously completed. Skipping."))
        return True
    
    print(ColoredFormatter.info("\n🔍 Validating loaded vocabulary data..."))
    
    try:
        conn = psycopg2.connect(**db_config)
        
        with conn.cursor() as cursor:
            # Check row counts
            tables = [
                "concept",
                "concept_relationship",
                "vocabulary",
                "domain",
                "concept_class",
                "relationship",
                "concept_ancestor",
                "concept_synonym",
                "drug_strength"
            ]
            
            table_counts = {}
            for table in tables:
                cursor.execute(f"SELECT COUNT(*) FROM omop.{table}")
                count = cursor.fetchone()[0]
                table_counts[table] = count
                
                # Check if count is reasonable
                expected = EXPECTED_ROW_COUNTS.get(table, 0)
                if count < expected * 0.1:  # Less than 10% of expected
                    print(ColoredFormatter.warning(f"⚠️ Low row count in {table}: {count:,} (expected at least {expected * 0.1:,.0f})"))
                else:
                    print(f"  - {table}: {count:,} rows")
            
            # Check for key concepts
            print("\nChecking for key concepts...")
            cursor.execute("SELECT COUNT(*) FROM omop.concept WHERE vocabulary_id = 'SNOMED'")
            snomed_count = cursor.fetchone()[0]
            
            cursor.execute("SELECT COUNT(*) FROM omop.concept WHERE vocabulary_id = 'RxNorm'")
            rxnorm_count = cursor.fetchone()[0]
            
            cursor.execute("SELECT COUNT(*) FROM omop.concept WHERE vocabulary_id = 'ICD10CM'")
            icd10_count = cursor.fetchone()[0]
            
            if snomed_count == 0:
                print(ColoredFormatter.warning("⚠️ No SNOMED concepts found"))
            else:
                print(f"  - SNOMED concepts: {snomed_count:,}")
            
            if rxnorm_count == 0:
                print(ColoredFormatter.warning("⚠️ No RxNorm concepts found"))
            else:
                print(f"  - RxNorm concepts: {rxnorm_count:,}")
            
            if icd10_count == 0:
                print(ColoredFormatter.warning("⚠️ No ICD10CM concepts found"))
            else:
                print(f"  - ICD10CM concepts: {icd10_count:,}")
            
            # Check for relationships
            cursor.execute("SELECT COUNT(*) FROM omop.concept_relationship WHERE relationship_id = 'Maps to'")
            maps_to_count = cursor.fetchone()[0]
            
            if maps_to_count == 0:
                print(ColoredFormatter.warning("⚠️ No 'Maps to' relationships found"))
            else:
                print(f"  - 'Maps to' relationships: {maps_to_count:,}")
            
            # Check for standard concepts
            cursor.execute("SELECT COUNT(*) FROM omop.concept WHERE standard_concept = 'S'")
            standard_count = cursor.fetchone()[0]
            
            if standard_count == 0:
                print(ColoredFormatter.warning("⚠️ No standard concepts found"))
            else:
                print(f"  - Standard concepts: {standard_count:,}")
        
        conn.close()
        
        # Determine if validation passed
        validation_passed = True
        for table, count in table_counts.items():
            expected = EXPECTED_ROW_COUNTS.get(table, 0)
            if count < expected * 0.1:
                validation_passed = False
        
        if snomed_count == 0 or rxnorm_count == 0 or icd10_count == 0 or maps_to_count == 0 or standard_count == 0:
            validation_passed = False
        
        if validation_passed:
            print(ColoredFormatter.success("\n✅ Vocabulary data validation passed"))
            mark_step_completed(step_name, table_counts)
            return True
        else:
            print(ColoredFormatter.warning("\n⚠️ Vocabulary data validation found issues"))
            return False
    except Exception as e:
        logger.error(f"Error validating loaded data: {e}")
        print(ColoredFormatter.error(f"❌ Error validating loaded data: {e}"))
        return False

def generate_summary_report(stats):
    """Generate a summary report of the vocabulary loading process."""
    print(ColoredFormatter.highlight("\n╔════════════════════════════════════════════════════════════════╗"))
    print(ColoredFormatter.highlight("║                Vocabulary Loading Summary Report                ║"))
    print(ColoredFormatter.highlight("╚════════════════════════════════════════════════════════════════╝"))
    
    # Get table counts from database to ensure accuracy
    try:
        conn = psycopg2.connect(**db_config)
        with conn.cursor() as cursor:
            tables = [
                "vocabulary",
                "domain",
                "concept_class",
                "relationship",
                "concept",
                "concept_relationship",
                "concept_ancestor",
                "concept_synonym",
                "drug_strength"
            ]
            
            print("\nVocabulary Loading Summary:")
            for table in tables:
                cursor.execute(f"SELECT COUNT(*) FROM omop.{table}")
                count = cursor.fetchone()[0]
                status = "✓" if count > 0 else "✗"
                print(f"{status} {table}: {count:,} rows")
        conn.close()
    except Exception as e:
        logger.error(f"Error getting table counts: {e}")
        
        # Fall back to stats if database query fails
        if 'load_vocabulary_files' in stats:
            load_stats = stats['load_vocabulary_files']
            
            print("\nLoaded Tables (from stats):")
            for file, file_stats in load_stats.items():
                if 'rows' in file_stats:
                    print(f"  - {file}: {file_stats['rows']:,} rows")
        
        if 'validate_loaded_data' in stats:
            validation_stats = stats['validate_loaded_data']
            
            print("\nTable Counts (from validation):")
            for table, count in validation_stats.items():
                print(f"  - {table}: {count:,} rows")
    
    print("\nVocabulary Loading Process Completed Successfully!")

def main():
    """Main function to run the enhanced vocabulary loading process."""
    # Parse command line arguments
    args = parse_arguments()
    
    # Set up logging
    setup_logging(args.debug)
    
    # Print welcome banner
    print(ColoredFormatter.highlight("\n╔════════════════════════════════════════════════════════════════╗"))
    print(ColoredFormatter.highlight("║            Enhanced Vocabulary Loading for OMOP CDM            ║"))
    print(ColoredFormatter.highlight("╚════════════════════════════════════════════════════════════════╝"))
    
    # Load checkpoint data
    checkpoint = load_checkpoint()
    
    # Check if resuming from checkpoint
    if args.resume and checkpoint.get('completed_steps'):
        print(ColoredFormatter.info(f"\n🔄 Resuming from checkpoint with completed steps: {', '.join(checkpoint['completed_steps'])}"))
    
    # Validate database connection
    if not validate_database_connection():
        print(ColoredFormatter.error("\n❌ Database connection failed. Please fix the issues and try again."))
        return 1
    
    # Validate vocabulary files
    validation_result = validate_vocabulary_files(args.vocab_dir)
    if not validation_result and not args.force:
        print(ColoredFormatter.error("\n❌ Vocabulary file validation failed. Please fix the issues and try again."))
        return 1
    
    # Process CPT4 codes if needed
    if not args.skip_cpt4:
        if not process_cpt4(args.vocab_dir, args.processed_dir):
            print(ColoredFormatter.warning("\n⚠️ CPT4 processing failed. Continuing with other steps."))
    
    # Clean vocabulary files
    if not clean_vocabulary_files(args.vocab_dir, args.processed_dir):
        print(ColoredFormatter.error("\n❌ Vocabulary file cleaning failed. Please fix the issues and try again."))
        return 1
    
    # Create schemas and tables
    if not create_schemas_and_tables(args.drop_tables):
        print(ColoredFormatter.error("\n❌ Failed to create schemas and tables. Please fix the issues and try again."))
        return 1
    
    # Load vocabulary files
    load_result, load_stats = load_all_vocabulary_files(args.processed_dir, args.batch_size)
    if not load_result and not args.force:
        print(ColoredFormatter.error("\n❌ Vocabulary loading failed. Please fix the issues and try again."))
        return 1
    
    # Create indexes
    if not create_indexes():
        print(ColoredFormatter.warning("\n⚠️ Index creation failed. This may impact query performance."))
    
    # Validate loaded data
    if not args.skip_validation:
        if not validate_loaded_data() and not args.force:
            print(ColoredFormatter.warning("\n⚠️ Data validation found issues. Use --force to proceed anyway."))
            return 1
    
    # Generate summary report
    generate_summary_report(checkpoint.get('stats', {}))
    
    print(ColoredFormatter.success("\n✅ Vocabulary loading process completed successfully!"))
    return 0

if __name__ == "__main__":
    try:
        sys.exit(main())
    except KeyboardInterrupt:
        print("\nVocabulary loading process interrupted by user.")
        sys.exit(130)
    except Exception as e:
        logger.error(f"Unhandled exception: {e}", exc_info=True)
        print(f"\nAn unexpected error occurred: {e}")
        sys.exit(1)
