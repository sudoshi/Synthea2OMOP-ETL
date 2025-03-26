#!/usr/bin/env python3
"""
Initialize the database with the required schemas, tables, and vocabulary for the ETL process.
This unified script handles both database initialization and vocabulary loading in a single process.
"""

import os
import sys
import subprocess
import logging
import datetime
import time
import argparse
import concurrent.futures
import shutil
import re
import csv
from pathlib import Path
from tqdm import tqdm
from utils.config_loader import ConfigLoader

# Set up logging
log_dir = "logs"
os.makedirs(log_dir, exist_ok=True)
log_file = os.path.join(log_dir, f"init_database_with_vocab_{datetime.datetime.now().strftime('%Y%m%d_%H%M%S')}.log")
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler(log_file),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

def parse_arguments():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(description='Initialize database with schemas, tables, and vocabulary')
    parser.add_argument('--skip-init', action='store_true', help='Skip database initialization')
    parser.add_argument('--skip-vocab', action='store_true', help='Skip vocabulary loading')
    parser.add_argument('--vocab-dir', type=str, default='./vocabulary', help='Directory containing vocabulary files')
    parser.add_argument('--processed-vocab-dir', type=str, default='./vocabulary_processed', help='Directory for processed vocabulary files')
    parser.add_argument('--max-workers', type=int, default=4, help='Maximum number of parallel workers')
    parser.add_argument('--batch-size', type=int, default=1000000, help='Batch size for processing large files')
    parser.add_argument('--debug', action='store_true', help='Enable debug logging')
    return parser.parse_args()

def setup_logging(debug=False):
    """Set up logging with appropriate level."""
    if debug:
        logging.getLogger().setLevel(logging.DEBUG)
        logger.debug("Debug logging enabled")
    else:
        logging.getLogger().setLevel(logging.INFO)

def run_sql_script(script_path, config_loader, max_retries=3, retry_delay=5):
    """Run a SQL script using psql with retry logic."""
    connection_string = f"postgresql://{config_loader.get_env('DB_USER', 'postgres')}:{config_loader.get_env('DB_PASSWORD', '')}@{config_loader.get_env('DB_HOST', 'localhost')}:{config_loader.get_env('DB_PORT', '5432')}/{config_loader.get_env('DB_NAME', 'ohdsi')}"
    
    logger.info(f"Running SQL script: {script_path}")
    
    for attempt in range(max_retries):
        try:
            start_time = time.time()
            result = subprocess.run(
                ["psql", connection_string, "-f", script_path],
                capture_output=True,
                text=True,
                check=True
            )
            end_time = time.time()
            duration = end_time - start_time
            
            logger.info(f"SQL script completed successfully: {script_path} (Duration: {duration:.2f} seconds)")
            logger.debug(result.stdout)
            return True
        except subprocess.CalledProcessError as e:
            logger.error(f"Error running SQL script {script_path} (Attempt {attempt+1}/{max_retries}): {e}")
            logger.error(f"STDOUT: {e.stdout}")
            logger.error(f"STDERR: {e.stderr}")
            
            if attempt < max_retries - 1:
                logger.info(f"Retrying in {retry_delay} seconds...")
                time.sleep(retry_delay)
                retry_delay *= 2  # Exponential backoff
            else:
                logger.error(f"Maximum retry attempts reached for {script_path}")
                return False
    
    return False

def run_sql_command(sql_command, config_loader, max_retries=3, retry_delay=5):
    """Run a SQL command using psql with retry logic."""
    connection_string = f"postgresql://{config_loader.get_env('DB_USER', 'postgres')}:{config_loader.get_env('DB_PASSWORD', '')}@{config_loader.get_env('DB_HOST', 'localhost')}:{config_loader.get_env('DB_PORT', '5432')}/{config_loader.get_env('DB_NAME', 'ohdsi')}"
    
    logger.debug(f"Running SQL command: {sql_command[:100]}...")
    
    for attempt in range(max_retries):
        try:
            result = subprocess.run(
                ["psql", connection_string, "-c", sql_command],
                capture_output=True,
                text=True,
                check=True
            )
            
            logger.debug("SQL command completed successfully")
            return result.stdout
        except subprocess.CalledProcessError as e:
            logger.error(f"Error running SQL command (Attempt {attempt+1}/{max_retries}): {e}")
            logger.error(f"STDOUT: {e.stdout}")
            logger.error(f"STDERR: {e.stderr}")
            
            if attempt < max_retries - 1:
                logger.info(f"Retrying in {retry_delay} seconds...")
                time.sleep(retry_delay)
                retry_delay *= 2  # Exponential backoff
            else:
                logger.error("Maximum retry attempts reached for SQL command")
                return None
    
    return None

def initialize_database(config_loader):
    """Initialize the database with schemas and tables."""
    logger.info("Starting database initialization")
    
    # Create schemas
    schema_script = "sql/init/create_schemas.sql"
    if not run_sql_script(schema_script, config_loader):
        logger.error("Failed to create schemas")
        return False
    
    # Create OMOP tables
    omop_ddl_scripts = [
        "sql/omop_ddl/OMOPCDM_postgresql_5.4_ddl.sql",
        "sql/omop_ddl/OMOPCDM_postgresql_5.4_primary_keys.sql",
        "sql/omop_ddl/OMOPCDM_postgresql_5.4_constraints.sql",
        "sql/omop_ddl/OMOPCDM_postgresql_5.4_indices.sql"
    ]
    
    for script in omop_ddl_scripts:
        if not run_sql_script(script, config_loader):
            logger.error(f"Failed to run OMOP DDL script: {script}")
            return False
    
    # Create staging tables
    staging_script = "sql/staging/synthea-omop-staging.sql"
    if not run_sql_script(staging_script, config_loader):
        logger.error("Failed to create staging tables")
        return False
    
    logger.info("Database initialization completed successfully")
    return True

def fix_header(file_path):
    """
    Fix the header of a vocabulary file by adding tab separators between column names.
    
    Args:
        file_path: Path to the vocabulary file
    
    Returns:
        True if the header was fixed, False otherwise
    """
    with open(file_path, 'r', encoding='utf-8', errors='replace') as f:
        first_line = f.readline().strip()
    
    # Check if the header needs fixing (no tabs)
    if '\t' not in first_line:
        # Try to identify column names based on common OMOP vocabulary patterns
        # First try to match snake_case patterns like domain_id, domain_name, etc.
        snake_case_pattern = r'([a-z]+_[a-z_]+)'
        snake_case_columns = re.findall(snake_case_pattern, first_line)
        
        # If we found snake_case columns, use them
        if snake_case_columns:
            columns = snake_case_columns
        else:
            # Otherwise, try the original pattern for camelCase
            columns = re.findall(r'([a-z_]+)(?=[A-Z]|$)', first_line)
        
        # If we found columns with either method
        if columns:
            # Read the entire file
            with open(file_path, 'r', encoding='utf-8', errors='replace') as f:
                content = f.read()
            
            # Replace the header
            new_header = '\t'.join(columns)
            new_content = content.replace(first_line, new_header, 1)
            
            # Write the file back
            with open(file_path, 'w', encoding='utf-8') as f:
                f.write(new_content)
            
            return True
        
        # If we couldn't identify columns automatically, try a manual approach for known files
        known_headers = {
            'DOMAIN.csv': "domain_id\tdomain_name\tdomain_concept_id",
            'CONCEPT_CLASS.csv': "concept_class_id\tconcept_class_name\tconcept_class_concept_id",
            'VOCABULARY.csv': "vocabulary_id\tvocabulary_name\tvocabulary_reference\tvocabulary_version\tvocabulary_concept_id",
            'RELATIONSHIP.csv': "relationship_id\trelationship_name\tis_hierarchical\tdefines_ancestry\treverse_relationship_id\trelationship_concept_id",
            'CONCEPT.csv': "concept_id\tconcept_name\tdomain_id\tvocabulary_id\tconcept_class_id\tstandard_concept\tconcept_code\tvalid_start_date\tvalid_end_date\tinvalid_reason",
            'CONCEPT_RELATIONSHIP.csv': "concept_id_1\tconcept_id_2\trelationship_id\tvalid_start_date\tvalid_end_date\tinvalid_reason",
            'CONCEPT_SYNONYM.csv': "concept_id\tconcept_synonym_name\tlanguage_concept_id",
            'CONCEPT_ANCESTOR.csv': "ancestor_concept_id\tdescendant_concept_id\tmin_levels_of_separation\tmax_levels_of_separation",
            'DRUG_STRENGTH.csv': "drug_concept_id\tingredient_concept_id\tamount_value\tamount_unit_concept_id\tnumerator_value\tnumerator_unit_concept_id\tdenominator_value\tdenominator_unit_concept_id\tbox_size\tvalid_start_date\tvalid_end_date\tinvalid_reason"
        }
        
        filename = os.path.basename(file_path)
        if filename in known_headers:
            with open(file_path, 'r', encoding='utf-8', errors='replace') as f:
                content = f.read()
            
            # Manually set the header for the known file
            new_header = known_headers[filename]
            new_content = content.replace(first_line, new_header, 1)
            
            with open(file_path, 'w', encoding='utf-8') as f:
                f.write(new_content)
            
            return True
    
    return False

def clean_vocabulary_file(input_file, output_file, batch_size=1000000):
    """
    Clean and prepare a vocabulary CSV file for PostgreSQL import.
    
    Args:
        input_file: Path to the original vocabulary file
        output_file: Path where the cleaned file will be saved
        batch_size: Number of lines to process in each batch
    """
    logger.info(f"Processing {os.path.basename(input_file)}...")
    
    # Create a temporary copy to work with
    temp_file = f"{input_file}.temp"
    shutil.copy2(input_file, temp_file)
    
    # Fix the header if needed
    fix_header(temp_file)
    
    # Count lines for progress bar
    line_count = sum(1 for _ in open(temp_file, 'r', encoding='utf-8', errors='replace'))
    
    # Check if this is a file that might have long text values
    file_name = os.path.basename(input_file).upper()
    is_concept_file = file_name == "CONCEPT.CSV"
    is_concept_synonym_file = file_name == "CONCEPT_SYNONYM.CSV"
    
    # Get the index of text columns that might need truncation
    text_column_index = -1
    text_column_name = ""
    
    with open(temp_file, 'r', encoding='utf-8', errors='replace') as infile:
        # Read the header
        header_line = infile.readline().strip()
        
        # Ensure the header has proper tab delimiters
        if '\t' not in header_line:
            # For known files, use predefined headers
            if file_name == "CONCEPT.CSV":
                header = "concept_id\tconcept_name\tdomain_id\tvocabulary_id\tconcept_class_id\tstandard_concept\tconcept_code\tvalid_start_date\tvalid_end_date\tinvalid_reason"
            elif file_name == "CONCEPT_SYNONYM.CSV":
                header = "concept_id\tconcept_synonym_name\tlanguage_concept_id"
            else:
                # Try to split by common delimiters
                if ',' in header_line:
                    header = '\t'.join(header_line.split(','))
                else:
                    # Use the original header as a fallback
                    header = header_line
        else:
            header = header_line
        
        # Get the index of text columns that might need truncation
        if is_concept_file:
            headers = header.split('\t')
            try:
                text_column_index = headers.index('concept_name')
                text_column_name = 'concept_name'
                logger.debug(f"Found {text_column_name} at index {text_column_index}")
            except ValueError:
                logger.warning(f"Could not find {text_column_name} column in {file_name} header")
        elif is_concept_synonym_file:
            headers = header.split('\t')
            try:
                text_column_index = headers.index('concept_synonym_name')
                text_column_name = 'concept_synonym_name'
                logger.debug(f"Found {text_column_name} at index {text_column_index}")
            except ValueError:
                logger.warning(f"Could not find {text_column_name} column in {file_name} header")
    
    # Process the file in batches
    with open(temp_file, 'r', encoding='utf-8', errors='replace') as infile, \
         open(output_file, 'w', encoding='utf-8', newline='') as outfile:
        
        # Write the fixed header
        outfile.write(header + '\n')
        
        # Process each batch
        batch_count = (line_count - 1) // batch_size + 1
        for batch_num in range(batch_count):
            start_line = batch_num * batch_size + 1  # +1 to skip header
            end_line = min((batch_num + 1) * batch_size + 1, line_count)
            
            logger.debug(f"Processing batch {batch_num+1}/{batch_count} (lines {start_line}-{end_line})")
            
            # Skip to the start line
            infile.seek(0)
            infile.readline()  # Skip header
            for _ in range(start_line - 1):
                infile.readline()
            
            # Process lines in this batch
            batch_lines = []
            for _ in tqdm(range(end_line - start_line), desc=f"Batch {batch_num+1}/{batch_count}"):
                line = infile.readline()
                if not line:
                    break
                
                if not line.strip():
                    continue
                
                # Replace problematic characters
                cleaned_line = line
                
                # 1. Handle inch marks (e.g., 22")
                cleaned_line = re.sub(r'(\d+)"', r'\1 inch', cleaned_line)
                
                # 2. Replace unescaped quotes with escaped quotes
                # This regex looks for quotes that are not at the beginning or end of a field
                cleaned_line = re.sub(r'(?<!\t)"(?!\t|\n|$)', "'", cleaned_line)
                
                # 3. Remove any carriage returns or newlines within fields, but preserve the ending newline
                cleaned_line = cleaned_line.replace('\r', ' ')
                
                # 4. Ensure proper tab delimiters between fields
                # If there are no tabs in the line but there should be (based on the header)
                if '\t' not in cleaned_line and '\t' in header:
                    # Try to split the line into fields based on common patterns
                    fields = re.findall(r'([^,]+)', cleaned_line)
                    if fields:
                        cleaned_line = '\t'.join(fields)
                
                # 5. Truncate text columns that might be too long
                if text_column_index >= 0:
                    fields = cleaned_line.split('\t')
                    if len(fields) > text_column_index:
                        # Truncate text to 2000 characters if it's longer
                        if len(fields[text_column_index]) > 2000:
                            logger.debug(f"Truncating {text_column_name}: {fields[text_column_index][:50]}... (length: {len(fields[text_column_index])})")
                            fields[text_column_index] = fields[text_column_index][:1997] + "..."
                            cleaned_line = '\t'.join(fields)
                
                # Make sure the line ends with a newline
                if not cleaned_line.endswith('\n'):
                    cleaned_line = cleaned_line + '\n'
                
                batch_lines.append(cleaned_line)
            
            # Write the batch to the output file
            outfile.writelines(batch_lines)
    
    # Clean up the temporary file
    os.remove(temp_file)
    
    logger.info(f"Completed processing {os.path.basename(input_file)} -> {os.path.basename(output_file)}")
    return True

def process_vocabulary_files(input_dir, output_dir, max_workers=4, batch_size=1000000):
    """Process vocabulary files in parallel."""
    logger.info(f"Processing vocabulary files from {input_dir} to {output_dir}")
    
    # Create output directory if it doesn't exist
    os.makedirs(output_dir, exist_ok=True)
    
    # Get all CSV files
    csv_files = [f for f in os.listdir(input_dir) if f.endswith('.csv')]
    
    if not csv_files:
        logger.warning(f"No CSV files found in {input_dir}")
        return False
    
    logger.info(f"Found {len(csv_files)} CSV files to process")
    
    # Process files in parallel
    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {}
        for filename in csv_files:
            input_path = os.path.join(input_dir, filename)
            output_path = os.path.join(output_dir, filename)
            
            # Skip if the output file already exists and is newer than the input file
            if os.path.exists(output_path) and os.path.getmtime(output_path) > os.path.getmtime(input_path):
                logger.info(f"Skipping {filename} (already processed)")
                continue
            
            # Submit the task to the executor
            future = executor.submit(clean_vocabulary_file, input_path, output_path, batch_size)
            futures[future] = filename
        
        # Process results as they complete
        for future in concurrent.futures.as_completed(futures):
            filename = futures[future]
            try:
                result = future.result()
                if result:
                    logger.info(f"Successfully processed {filename}")
                else:
                    logger.error(f"Failed to process {filename}")
            except Exception as e:
                logger.error(f"Error processing {filename}: {e}")
    
    # Copy non-CSV files
    for filename in os.listdir(input_dir):
        if not filename.endswith('.csv'):
            src = os.path.join(input_dir, filename)
            dst = os.path.join(output_dir, filename)
            if os.path.isfile(src):
                shutil.copy2(src, dst)
                logger.info(f"Copied {filename}")
    
    logger.info("Vocabulary file processing completed")
    return True

def load_vocabulary_file(table_name, file_path, config_loader):
    """Load a vocabulary file into the database."""
    logger.info(f"Loading {os.path.basename(file_path)} into {table_name}")
    
    # Determine if this is a special file that needs temporary column type changes
    is_special_file = table_name in ["omop.concept", "omop.concept_synonym"]
    column_name = "concept_name" if table_name == "omop.concept" else "concept_synonym_name" if table_name == "omop.concept_synonym" else None
    
    # Check if the file exists
    if not os.path.exists(file_path):
        logger.error(f"File not found: {file_path}")
        return False
    
    try:
        # For special files, alter the column type to TEXT first
        if is_special_file:
            logger.info(f"Temporarily altering {column_name} column to TEXT type")
            alter_sql = f"ALTER TABLE {table_name} ALTER COLUMN {column_name} TYPE TEXT;"
            run_sql_command(alter_sql, config_loader)
        
        # Determine the delimiter by checking the first line
        with open(file_path, 'r', encoding='utf-8', errors='replace') as f:
            first_line = f.readline()
            delimiter = '\t' if '\t' in first_line else ','
        
        # Create a temporary SQL file for the COPY command
        temp_sql_file = f"temp_copy_{os.path.basename(file_path)}.sql"
        with open(temp_sql_file, 'w') as f:
            f.write(f"\\copy {table_name} FROM '{file_path}' WITH (FORMAT csv, DELIMITER E'\\t', QUOTE '\"', ESCAPE '\\', NULL '', HEADER);\n")
        
        # Run the COPY command
        if not run_sql_script(temp_sql_file, config_loader):
            logger.error(f"Failed to load {file_path}")
            return False
        
        # For special files, convert the column back to varchar(2000)
        if is_special_file:
            logger.info(f"Converting {column_name} back to varchar(2000)")
            alter_sql = f"ALTER TABLE {table_name} ALTER COLUMN {column_name} TYPE varchar(2000) USING substring({column_name}, 1, 2000);"
            run_sql_command(alter_sql, config_loader)
        
        # Clean up the temporary SQL file
        os.remove(temp_sql_file)
        
        # Get the count of loaded records
        count_sql = f"SELECT COUNT(*) FROM {table_name};"
        count_result = run_sql_command(count_sql, config_loader)
        if count_result:
            count = count_result.strip().split('\n')[2].strip()
            logger.info(f"Loaded {count} records into {table_name}")
        
        return True
    except Exception as e:
        logger.error(f"Error loading {file_path}: {e}")
        return False

def drop_circular_foreign_keys(config_loader):
    """Drop circular foreign key constraints."""
    logger.info("Dropping circular foreign key constraints")
    
    sql = """
    ALTER TABLE omop.domain DROP CONSTRAINT IF EXISTS fpk_domain_domain_concept_id;
    ALTER TABLE omop.concept DROP CONSTRAINT IF EXISTS fpk_concept_domain_id;
    """
    
    result = run_sql_command(sql, config_loader)
    if result is None:
        logger.error("Failed to drop circular foreign key constraints")
        return False
    
    logger.info("Circular foreign key constraints dropped successfully")
    return True

def add_circular_foreign_keys(config_loader):
    """Add circular foreign key constraints."""
    logger.info("Adding circular foreign key constraints")
    
    sql = """
    ALTER TABLE omop.domain
      ADD CONSTRAINT fpk_domain_domain_concept_id
      FOREIGN KEY (domain_concept_id)
      REFERENCES omop.concept(concept_id);

    ALTER TABLE omop.concept
      ADD CONSTRAINT fpk_concept_domain_id
      FOREIGN KEY (domain_id)
      REFERENCES omop.domain(domain_id);
    """
    
    result = run_sql_command(sql, config_loader)
    if result is None:
        logger.error("Failed to add circular foreign key constraints")
        return False
    
    logger.info("Circular foreign key constraints added successfully")
    return True

def truncate_vocabulary_tables(config_loader):
    """Truncate vocabulary tables."""
    logger.info("Truncating vocabulary tables")
    
    sql = """
    TRUNCATE TABLE
      omop.concept,
      omop.vocabulary,
      omop.domain,
      omop.concept_class,
      omop.relationship,
      omop.concept_relationship,
      omop.concept_synonym,
      omop.concept_ancestor,
      omop.drug_strength,
      omop.source_to_concept_map
    CASCADE;
    """
    
    result = run_sql_command(sql, config_loader)
    if result is None:
        logger.error("Failed to truncate vocabulary tables")
        return False
    
    logger.info("Vocabulary tables truncated successfully")
    return True

def load_vocabulary(vocab_dir, config_loader):
    """Load vocabulary files into the database."""
    logger.info(f"Loading vocabulary files from {vocab_dir}")
    
    # Truncate vocabulary tables
    if not truncate_vocabulary_tables(config_loader):
        return False
    
    # Drop circular foreign key constraints
    if not drop_circular_foreign_keys(config_loader):
        return False
    
    # Define the vocabulary files and their corresponding tables
    vocab_files = [
        ("omop.domain", "DOMAIN.csv"),
        ("omop.vocabulary", "VOCABULARY.csv"),
        ("omop.concept_class", "CONCEPT_CLASS.csv"),
        ("omop.relationship", "RELATIONSHIP.csv"),
        ("omop.concept", "CONCEPT.csv"),
        ("omop.concept_relationship", "CONCEPT_RELATIONSHIP.csv"),
        ("omop.concept_synonym", "CONCEPT_SYNONYM.csv"),
        ("omop.concept_ancestor", "CONCEPT_ANCESTOR.csv"),
        ("omop.drug_strength", "DRUG_STRENGTH.csv"),
        ("omop.source_to_concept_map", "SOURCE_TO_CONCEPT_MAP.csv")
    ]
    
    # Load each vocabulary file
    for table_name, file_name in vocab_files:
        file_path = os.path.join(vocab_dir, file_name)
        if not os.path.exists(file_path):
            logger.warning(f"File not found: {file_path}")
            continue
        
        if not load_vocabulary_file(table_name, file_path, config_loader):
            logger.error(f"Failed to load {file_name}")
            return False
    
    # Add circular foreign key constraints
    if not add_circular_foreign_keys(config_loader):
        return False
    
    # Verify the load by checking row counts
    verify_sql = """
    SELECT 'concept' as table_name, COUNT(*) as row_count FROM omop.concept
    UNION ALL
    SELECT 'vocabulary', COUNT(*) FROM omop.vocabulary
    UNION ALL
    SELECT 'domain', COUNT(*) FROM omop.domain
    UNION ALL
    SELECT 'concept_class', COUNT(*) FROM omop.concept_class
    UNION ALL
    SELECT 'relationship', COUNT(*) FROM omop.relationship
    UNION ALL
    SELECT 'concept_relationship', COUNT(*) FROM omop.concept_relationship
    UNION ALL
    SELECT 'concept_synonym', COUNT(*) FROM omop.concept_synonym
    UNION ALL
    SELECT 'concept_ancestor', COUNT(*) FROM omop.concept_ancestor
    UNION ALL
    SELECT 'drug_strength', COUNT(*) FROM omop.drug_strength
    UNION ALL
    SELECT 'source_to_concept_map', COUNT(*) FROM omop.source_to_concept_map
    ORDER BY table_name;
    """
    
    verify_result = run_sql_command(verify_sql, config_loader)
    if verify_result:
        logger.info("Vocabulary load verification:")
        logger.info(verify_result)
    
    logger.info("Vocabulary loading completed successfully")
    return True

def main():
    """Main function to initialize the database and load vocabulary."""
    # Parse command line arguments
    args = parse_arguments()
    
    # Set up logging
    setup_logging(args.debug)
    
    logger.info("Starting database initialization and vocabulary loading")
    
    # Load configuration
    try:
        config_loader = ConfigLoader()
        logger.info("Configuration loaded successfully")
    except Exception as e:
        logger.error(f"Error loading configuration: {e}")
        return 1
    
    # Initialize database if not skipped
    if not args.skip_init:
        if not initialize_database(config_loader):
            logger.error("Database initialization failed")
            return 1
    else:
        logger.info("Skipping database initialization")
    
    # Process and load vocabulary if not skipped
    if not args.skip_vocab:
        # Process vocabulary files
        if not process_vocabulary_files(args.vocab_dir, args.processed_vocab_dir, args.max_workers, args.batch_size):
            logger.error("Vocabulary processing failed")
            return 1
        
        # Load vocabulary files
        if not load_vocabulary(args.processed_vocab_dir, config_loader):
            logger.error("Vocabulary loading failed")
            return 1
    else:
        logger.info("Skipping vocabulary processing and loading")
    
    logger.info("Database initialization and vocabulary loading completed successfully")
    return 0

if __name__ == "__main__":
    sys.exit(main())
