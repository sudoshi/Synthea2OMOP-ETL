#!/usr/bin/env python3
"""
run_unified_pipeline.py - Unified pipeline for Synthea to OMOP ETL

This script provides a unified pipeline that:
1. Initializes the database with OMOP CDM schema
2. Loads vocabulary data
3. Runs the optimized Synthea to OMOP ETL process

It combines multiple steps into a single, streamlined workflow.
"""

import argparse
import logging
import os
import subprocess
import sys
import time
from typing import List, Dict, Any, Optional

# Set up logging
log_dir = "logs"
os.makedirs(log_dir, exist_ok=True)
log_file = os.path.join(log_dir, f"unified_pipeline_{time.strftime('%Y%m%d_%H%M%S')}.log")
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
    parser = argparse.ArgumentParser(description='Unified Synthea to OMOP ETL Pipeline')
    
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
    
    # General options
    parser.add_argument('--debug', action='store_true',
                        help='Enable debug logging')
    parser.add_argument('--track-progress', action='store_true',
                        help='Enable progress tracking for ETL process')
    parser.add_argument('--monitor', action='store_true',
                        help='Launch progress monitoring in a separate terminal (requires --track-progress)')
    
    return parser.parse_args()

def setup_logging(debug=False):
    """Set up logging with appropriate level."""
    if debug:
        logging.getLogger().setLevel(logging.DEBUG)
        logger.debug("Debug logging enabled")
    else:
        logging.getLogger().setLevel(logging.INFO)

def run_command(command: List[str], description: str) -> bool:
    """Run a command and log the output."""
    logger.info(f"Running {description}...")
    logger.debug(f"Command: {' '.join(command)}")
    
    try:
        process = subprocess.Popen(
            command,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            universal_newlines=True
        )
        
        # Process output in real-time
        for line in process.stdout:
            logger.info(line.strip())
        
        # Wait for process to complete
        process.wait()
        
        # Check return code
        if process.returncode != 0:
            logger.error(f"{description} failed with return code {process.returncode}")
            # Get error output
            stderr = process.stderr.read()
            logger.error(f"Error output: {stderr}")
            return False
        
        logger.info(f"{description} completed successfully")
        return True
    except Exception as e:
        logger.error(f"Error running {description}: {e}")
        return False

def initialize_database(drop_existing=False) -> bool:
    """Initialize the database with OMOP CDM schema."""
    logger.info("Initializing database with OMOP CDM schema")
    
    command = ["python3", "init_database.py"]
    if drop_existing:
        command.append("--drop-existing")
    
    return run_command(command, "database initialization")

def load_vocabulary(vocab_dir: str) -> bool:
    """Load vocabulary data into the database."""
    logger.info(f"Loading vocabulary data from {vocab_dir}")
    
    # Check if vocabulary directory exists
    if not os.path.isdir(vocab_dir):
        logger.error(f"Vocabulary directory not found: {vocab_dir}")
        return False
    
    # Check if vocabulary files exist
    required_files = ["CONCEPT.csv", "CONCEPT_RELATIONSHIP.csv", "VOCABULARY.csv"]
    missing_files = [f for f in required_files if not os.path.exists(os.path.join(vocab_dir, f))]
    if missing_files:
        logger.error(f"Missing required vocabulary files: {', '.join(missing_files)}")
        return False
    
    # Run vocabulary loading script
    command = ["bash", "vocabulary/load_vocabulary.sh", vocab_dir]
    return run_command(command, "vocabulary loading")

def run_etl(synthea_dir: str, max_workers: int, skip_optimization: bool, skip_validation: bool, debug: bool) -> bool:
    """Run the optimized Synthea to OMOP ETL process."""
    logger.info(f"Running optimized ETL process with data from {synthea_dir}")
    
    # Check if Synthea directory exists
    if not os.path.isdir(synthea_dir):
        logger.error(f"Synthea directory not found: {synthea_dir}")
        return False
    
    # Build command
    command = ["./run_optimized_import.sh", "--synthea-dir", synthea_dir, "--max-workers", str(max_workers)]
    
    if skip_optimization:
        command.append("--skip-optimization")
    
    if skip_validation:
        command.append("--skip-validation")
    
    if debug:
        command.append("--debug")
    
    return run_command(command, "optimized ETL process")

def main():
    """Main function to run the unified pipeline."""
    start_time = time.time()
    
    # Parse command line arguments
    args = parse_arguments()
    
    # Set up logging
    setup_logging(args.debug)
    
    logger.info("Starting unified Synthea to OMOP pipeline")
    
    # Step 1: Initialize database (if not skipped)
    if not args.skip_init:
        if not initialize_database(args.drop_existing):
            logger.error("Database initialization failed")
            return 1
    else:
        logger.info("Skipping database initialization")
    
    # Step 2: Load vocabulary (if not skipped)
    if not args.skip_vocab:
        if not load_vocabulary(args.vocab_dir):
            logger.error("Vocabulary loading failed")
            return 1
    else:
        logger.info("Skipping vocabulary loading")
    
    # Step 3: Run ETL process (if not skipped)
    if not args.skip_etl:
        if not run_etl(args.synthea_dir, args.max_workers, args.skip_optimization, args.skip_validation, args.debug):
            logger.error("ETL process failed")
            return 1
    else:
        logger.info("Skipping ETL process")
    
    # Calculate total duration
    end_time = time.time()
    duration = end_time - start_time
    
    logger.info(f"Unified pipeline completed successfully in {duration:.2f} seconds")
    return 0

if __name__ == "__main__":
    sys.exit(main())
