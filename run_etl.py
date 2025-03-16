#!/usr/bin/env python3
"""
run_etl.py

Main entry point for running the Synthea to OMOP ETL process.
This script orchestrates the entire ETL pipeline using the configuration system.
"""

import os
import sys
import time
import argparse
import logging
import subprocess
from pathlib import Path
from typing import List, Optional

# Import our configuration loader
from utils.config_loader import config

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(f"logs/etl_{time.strftime('%Y%m%d_%H%M%S')}.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

def run_command(command: List[str], description: str) -> bool:
    """
    Run a shell command and log the output.
    
    Args:
        command: List of command parts
        description: Description of the command for logging
        
    Returns:
        True if the command succeeded, False otherwise
    """
    logger.info(f"Running {description}...")
    logger.debug(f"Command: {' '.join(command)}")
    
    try:
        result = subprocess.run(
            command,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            check=True
        )
        logger.info(f"{description} completed successfully")
        logger.debug(f"Output: {result.stdout}")
        return True
    except subprocess.CalledProcessError as e:
        logger.error(f"{description} failed with exit code {e.returncode}")
        logger.error(f"Error output: {e.stderr}")
        return False

def run_sql_file(sql_file: Path, description: str) -> bool:
    """
    Run a SQL file against the database.
    
    Args:
        sql_file: Path to the SQL file
        description: Description of the SQL file for logging
        
    Returns:
        True if the SQL file executed successfully, False otherwise
    """
    if not sql_file.exists():
        logger.error(f"SQL file not found: {sql_file}")
        return False
    
    db_config = config.get_db_config()
    
    command = [
        "psql",
        "-h", db_config['host'],
        "-p", db_config['port'],
        "-U", db_config['user'],
        "-d", db_config['dbname'],
        "-f", str(sql_file)
    ]
    
    return run_command(command, description)

def create_omop_schema() -> bool:
    """
    Create the OMOP schema and tables.
    
    Returns:
        True if the schema was created successfully, False otherwise
    """
    logger.info("Creating OMOP schema and tables...")
    
    project_root = Path(__file__).parent.absolute()
    omop_ddl_dir = project_root / "sql" / "omop_ddl"
    
    # Order matters for these files
    ddl_files = [
        omop_ddl_dir / "OMOPCDM_postgresql_5.4_ddl.sql",
        omop_ddl_dir / "OMOPCDM_postgresql_5.4_primary_keys.sql",
        omop_ddl_dir / "OMOPCDM_postgresql_5.4_indices.sql",
        omop_ddl_dir / "OMOPCDM_postgresql_5.4_constraints.sql"
    ]
    
    for ddl_file in ddl_files:
        if not ddl_file.exists():
            logger.error(f"DDL file not found: {ddl_file}")
            return False
        
        description = f"Executing DDL file: {ddl_file.name}"
        if not run_sql_file(ddl_file, description):
            return False
    
    logger.info("OMOP schema and tables created successfully")
    return True

def load_omop_vocabulary() -> bool:
    """
    Load the OMOP vocabulary files.
    
    Returns:
        True if the vocabulary was loaded successfully, False otherwise
    """
    logger.info("Loading OMOP vocabulary...")
    
    project_root = Path(__file__).parent.absolute()
    vocab_script = project_root / "scripts" / "load_omop_vocab.sh"
    
    if not vocab_script.exists():
        logger.error(f"Vocabulary loading script not found: {vocab_script}")
        return False
    
    # Make the script executable
    os.chmod(vocab_script, 0o755)
    
    return run_command([str(vocab_script)], "Loading OMOP vocabulary")

def load_synthea_data() -> bool:
    """
    Load the Synthea data into staging tables.
    
    Returns:
        True if the data was loaded successfully, False otherwise
    """
    logger.info("Loading Synthea data into staging tables...")
    
    project_root = Path(__file__).parent.absolute()
    synthea_script = project_root / "scripts" / "load_synthea_staging.sh"
    
    if not synthea_script.exists():
        logger.error(f"Synthea loading script not found: {synthea_script}")
        return False
    
    # Make the script executable
    os.chmod(synthea_script, 0o755)
    
    return run_command([str(synthea_script)], "Loading Synthea data")

def convert_synthea_types() -> bool:
    """
    Convert Synthea data types.
    
    Returns:
        True if the conversion was successful, False otherwise
    """
    logger.info("Converting Synthea data types...")
    
    project_root = Path(__file__).parent.absolute()
    typing_sql = project_root / "sql" / "synthea_typing" / "synthea-typedtables-transformation.sql"
    
    return run_sql_file(typing_sql, "Converting Synthea data types")

def create_staging_schema() -> bool:
    """
    Create the staging schema and tables.
    
    Returns:
        True if the staging schema was created successfully, False otherwise
    """
    logger.info("Creating staging schema and tables...")
    
    project_root = Path(__file__).parent.absolute()
    staging_sql = project_root / "sql" / "staging" / "synthea-omop-staging.sql"
    
    return run_sql_file(staging_sql, "Creating staging schema")

def run_etl_process() -> bool:
    """
    Run the ETL process to transform Synthea data to OMOP.
    
    Returns:
        True if the ETL process was successful, False otherwise
    """
    logger.info("Running ETL process...")
    
    project_root = Path(__file__).parent.absolute()
    etl_sql = project_root / "sql" / "etl" / "synthea-omop-ETL.sql"
    
    return run_sql_file(etl_sql, "Running ETL process")

def validate_etl_results() -> bool:
    """
    Validate the ETL results.
    
    Returns:
        True if the validation was successful, False otherwise
    """
    logger.info("Validating ETL results...")
    
    # Get database configuration
    db_config = config.get_db_config()
    schema_names = config.get_schema_names()
    
    # Build validation queries
    validation_queries = [
        f"SELECT COUNT(*) FROM {schema_names['omop']}.person;",
        f"SELECT COUNT(*) FROM {schema_names['omop']}.visit_occurrence;",
        f"SELECT COUNT(*) FROM {schema_names['omop']}.condition_occurrence;",
        f"SELECT COUNT(*) FROM {schema_names['omop']}.drug_exposure;",
        f"SELECT COUNT(*) FROM {schema_names['omop']}.measurement;",
        f"SELECT COUNT(*) FROM {schema_names['omop']}.observation;"
    ]
    
    # Run validation queries
    for query in validation_queries:
        command = [
            "psql",
            "-h", db_config['host'],
            "-p", db_config['port'],
            "-U", db_config['user'],
            "-d", db_config['dbname'],
            "-c", query
        ]
        
        if not run_command(command, f"Running validation query: {query}"):
            return False
    
    logger.info("ETL validation completed successfully")
    return True

def main(args: Optional[List[str]] = None) -> int:
    """
    Main entry point for the ETL process.
    
    Args:
        args: Command line arguments
        
    Returns:
        Exit code (0 for success, non-zero for failure)
    """
    parser = argparse.ArgumentParser(description="Run the Synthea to OMOP ETL process")
    parser.add_argument("--skip-schema", action="store_true", help="Skip creating OMOP schema")
    parser.add_argument("--skip-vocab", action="store_true", help="Skip loading OMOP vocabulary")
    parser.add_argument("--skip-synthea", action="store_true", help="Skip loading Synthea data")
    parser.add_argument("--skip-typing", action="store_true", help="Skip converting Synthea data types")
    parser.add_argument("--skip-staging", action="store_true", help="Skip creating staging schema")
    parser.add_argument("--skip-etl", action="store_true", help="Skip running ETL process")
    parser.add_argument("--skip-validation", action="store_true", help="Skip validating ETL results")
    
    args = parser.parse_args(args)
    
    # Create logs directory if it doesn't exist
    Path("logs").mkdir(exist_ok=True)
    
    logger.info("Starting Synthea to OMOP ETL process")
    
    # Run each step of the ETL process
    if not args.skip_schema:
        if not create_omop_schema():
            logger.error("Failed to create OMOP schema")
            return 1
    
    if not args.skip_vocab:
        if not load_omop_vocabulary():
            logger.error("Failed to load OMOP vocabulary")
            return 1
    
    if not args.skip_synthea:
        if not load_synthea_data():
            logger.error("Failed to load Synthea data")
            return 1
    
    if not args.skip_typing:
        if not convert_synthea_types():
            logger.error("Failed to convert Synthea data types")
            return 1
    
    if not args.skip_staging:
        if not create_staging_schema():
            logger.error("Failed to create staging schema")
            return 1
    
    if not args.skip_etl:
        if not run_etl_process():
            logger.error("Failed to run ETL process")
            return 1
    
    if not args.skip_validation:
        if not validate_etl_results():
            logger.error("Failed to validate ETL results")
            return 1
    
    logger.info("Synthea to OMOP ETL process completed successfully")
    return 0

if __name__ == "__main__":
    sys.exit(main())
