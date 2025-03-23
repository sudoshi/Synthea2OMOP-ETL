#!/usr/bin/env python3
"""
Initialize the database with the required schemas and tables for the ETL process.
"""

import os
import sys
import subprocess
import logging
import datetime
from utils.config_loader import ConfigLoader

# Set up logging
log_dir = "logs"
os.makedirs(log_dir, exist_ok=True)
log_file = os.path.join(log_dir, f"init_database_{datetime.datetime.now().strftime('%Y%m%d_%H%M%S')}.log")
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler(log_file),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

def run_sql_script(script_path, config_loader):
    """Run a SQL script using psql."""
    connection_string = f"postgresql://{config_loader.get_env('DB_USER', 'postgres')}:{config_loader.get_env('DB_PASSWORD', '')}@{config_loader.get_env('DB_HOST', 'localhost')}:{config_loader.get_env('DB_PORT', '5432')}/{config_loader.get_env('DB_NAME', 'ohdsi')}"
    
    logger.info(f"Running SQL script: {script_path}")
    
    try:
        result = subprocess.run(
            ["psql", connection_string, "-f", script_path],
            capture_output=True,
            text=True,
            check=True
        )
        logger.info(f"SQL script completed successfully: {script_path}")
        logger.debug(result.stdout)
        return True
    except subprocess.CalledProcessError as e:
        logger.error(f"Error running SQL script {script_path}: {e}")
        logger.error(f"STDOUT: {e.stdout}")
        logger.error(f"STDERR: {e.stderr}")
        return False

def main():
    """Main function to initialize the database."""
    logger.info("Starting database initialization")
    
    # Load configuration
    try:
        config_loader = ConfigLoader()
        logger.info("Configuration loaded successfully")
    except Exception as e:
        logger.error(f"Error loading configuration: {e}")
        return 1
    
    # Create schemas
    schema_script = "sql/init/create_schemas.sql"
    if not run_sql_script(schema_script, config_loader):
        logger.error("Failed to create schemas")
        return 1
    
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
            return 1
    
    # Create staging tables
    staging_script = "sql/staging/synthea-omop-staging.sql"
    if not run_sql_script(staging_script, config_loader):
        logger.error("Failed to create staging tables")
        return 1
    
    logger.info("Database initialization completed successfully")
    return 0

if __name__ == "__main__":
    sys.exit(main())
