#!/usr/bin/env python3
"""
Run the optimized ETL process to transform data from the population schema to the OMOP CDM schema.
This version includes performance optimizations and better error handling.
"""

import os
import sys
import subprocess
import logging
import datetime
import time
import argparse
from utils.config_loader import ConfigLoader

# Set up logging
log_dir = "logs"
os.makedirs(log_dir, exist_ok=True)
log_file = os.path.join(log_dir, f"etl_population_to_omop_optimized_{datetime.datetime.now().strftime('%Y%m%d_%H%M%S')}.log")
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler(log_file),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

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

def optimize_postgres_config(config_loader):
    """Apply PostgreSQL configuration optimizations."""
    logger.info("Applying PostgreSQL configuration optimizations")
    
    try:
        # Run the optimization script
        success = run_sql_script("sql/etl/optimize_postgres_config.sql", config_loader)
        
        if success:
            logger.info("PostgreSQL configuration optimized successfully")
            return True
        else:
            logger.warning("Failed to optimize PostgreSQL configuration. Continuing with default settings.")
            return False
    except Exception as e:
        logger.error(f"Error optimizing PostgreSQL configuration: {e}")
        logger.warning("Continuing with default PostgreSQL settings")
        return False

def main():
    """Main function to run the ETL process."""
    parser = argparse.ArgumentParser(description="Run the optimized ETL process")
    parser.add_argument("--skip-config-optimization", action="store_true", help="Skip PostgreSQL configuration optimization")
    parser.add_argument("--skip-preflight", action="store_true", help="Skip preflight tests")
    parser.add_argument("--monitor", action="store_true", help="Run the monitoring tool in a separate process")
    args = parser.parse_args()
    
    logger.info("Starting optimized ETL process: population schema to OMOP CDM")
    
    # Load configuration
    try:
        config_loader = ConfigLoader()
    except Exception as e:
        logger.error(f"Error loading configuration: {e}")
        return 1
    
    # Optimize PostgreSQL configuration if not skipped
    if not args.skip_config_optimization:
        optimize_postgres_config(config_loader)
    
    # Run preflight tests if not skipped
    if not args.skip_preflight:
        logger.info("Running preflight tests")
        preflight_result = subprocess.run(
            ["python", "test_etl_preflight.py"],
            capture_output=True,
            text=True
        )
        
        if preflight_result.returncode != 0:
            logger.error("Preflight tests failed. Check the logs for details.")
            logger.error(f"Preflight output: {preflight_result.stdout}")
            logger.error(f"Preflight errors: {preflight_result.stderr}")
            
            # Ask for confirmation to continue
            response = input("Preflight tests failed. Do you want to continue anyway? (y/n): ")
            if response.lower() != 'y':
                logger.info("ETL process aborted by user")
                return 1
            
            logger.warning("Continuing ETL process despite preflight test failures")
    
    # Start monitoring tool if requested
    if args.monitor:
        logger.info("Starting monitoring tool")
        monitor_process = subprocess.Popen(
            ["python", "monitor_etl_progress.py", "--interval", "30"],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE
        )
        logger.info("Monitoring tool started")
    
    # Run the ETL process
    start_time = time.time()
    etl_script = "sql/etl/run_all_etl_optimized.sql"
    success = run_sql_script(etl_script, config_loader)
    end_time = time.time()
    total_duration = end_time - start_time
    
    # Stop monitoring tool if it was started
    if args.monitor and 'monitor_process' in locals():
        monitor_process.terminate()
        logger.info("Monitoring tool stopped")
    
    if success:
        logger.info(f"ETL process completed successfully in {total_duration:.2f} seconds")
        return 0
    else:
        logger.error("ETL process failed")
        return 1

if __name__ == "__main__":
    sys.exit(main())
