#!/usr/bin/env python3
"""
etl_main.py - Main orchestration script for the Synthea to OMOP ETL process.
This script coordinates the execution of individual ETL steps in the correct order.
"""

import os
import sys
import argparse
import logging
import time
from pathlib import Path
from typing import Dict, List, Any, Optional

# Add parent directory to path to import from etl_setup
sys.path.append(str(Path(__file__).parent.parent))
from etl_pipeline.etl_setup import (
    init_logging,
    init_db_connection_pool,
    load_checkpoint,
    save_checkpoint,
    is_step_completed,
    mark_step_completed,
    execute_query,
    ColoredFormatter,
    ETLProgressTracker,
    CHECKPOINT_FILE
)

# Import ETL step modules
from etl_pipeline.etl_patients import process_patients
from etl_pipeline.etl_encounters import process_encounters
from etl_pipeline.etl_conditions import process_conditions
from etl_pipeline.etl_procedures import process_procedures
from etl_pipeline.etl_medications import process_medications
from etl_pipeline.etl_observations import process_observations
from etl_pipeline.etl_observation_periods import create_observation_periods
from etl_pipeline.etl_concept_mapping import map_source_to_standard_concepts
from etl_pipeline.etl_validation import validate_etl_results
from etl_pipeline.etl_analyze import analyze_tables

# Define ETL steps and their dependencies
ETL_STEPS = [
    {
        "name": "process_patients",
        "description": "Process patients into person table",
        "dependencies": [],
        "csv_file": "patients.csv"
    },
    {
        "name": "process_encounters",
        "description": "Process encounters into visit_occurrence table",
        "dependencies": ["process_patients"],
        "csv_file": "encounters.csv"
    },
    {
        "name": "process_conditions",
        "description": "Process conditions into condition_occurrence table",
        "dependencies": ["process_patients", "process_encounters"],
        "csv_file": "conditions.csv"
    },
    {
        "name": "process_procedures",
        "description": "Process procedures into procedure_occurrence table",
        "dependencies": ["process_patients", "process_encounters"],
        "csv_file": "procedures.csv"
    },
    {
        "name": "process_medications",
        "description": "Process medications into drug_exposure table",
        "dependencies": ["process_patients", "process_encounters"],
        "csv_file": "medications.csv"
    },
    {
        "name": "process_observations",
        "description": "Process observations into measurement and observation tables",
        "dependencies": ["process_patients", "process_encounters"],
        "csv_file": "observations.csv"
    },
    {
        "name": "create_observation_periods",
        "description": "Create observation_period records",
        "dependencies": ["process_patients", "process_encounters", "process_conditions", 
                        "process_procedures", "process_medications", "process_observations"]
    },
    {
        "name": "map_source_to_standard_concepts",
        "description": "Map source concepts to standard concepts",
        "dependencies": [
            "process_patients", 
            "process_encounters", 
            "process_conditions", 
            "process_procedures", 
            "process_medications", 
            "process_observations"
        ]
    },
    {
        "name": "analyze_tables",
        "description": "Analyze OMOP tables for statistics",
        "dependencies": ["map_source_to_standard_concepts"]
    },
    {
        "name": "validate_etl_results",
        "description": "Validate ETL results",
        "dependencies": ["analyze_tables"]
    }
]

def clear_checkpoint_file():
    """Clear the checkpoint file to force reprocessing of all steps."""
    if CHECKPOINT_FILE.exists():
        try:
            CHECKPOINT_FILE.unlink()
            logging.info(ColoredFormatter.success("Checkpoint file cleared. All steps will be reprocessed."))
            return True
        except Exception as e:
            logging.error(f"Failed to clear checkpoint file: {e}")
            return False
    else:
        logging.info("No checkpoint file found. All steps will be processed.")
        return True

def ensure_database_setup():
    """Ensure the database is properly set up for ETL."""
    try:
        # Check if OMOP schema exists
        schema_exists = execute_query(
            "SELECT EXISTS(SELECT 1 FROM information_schema.schemata WHERE schema_name = 'omop')",
            fetch=True
        )[0][0]
        
        if not schema_exists:
            logging.error(ColoredFormatter.error(
                "❌ OMOP schema does not exist. Please run the database setup script first."
            ))
            return False
        
        # Check if staging schema exists, create if not
        staging_exists = execute_query(
            "SELECT EXISTS(SELECT 1 FROM information_schema.schemata WHERE schema_name = 'staging')",
            fetch=True
        )[0][0]
        
        if not staging_exists:
            logging.info("Creating staging schema...")
            execute_query("CREATE SCHEMA staging")
        
        # Check if key tables exist
        tables_to_check = [
            "omop.person", 
            "omop.visit_occurrence", 
            "omop.condition_occurrence",
            "omop.procedure_occurrence",
            "omop.drug_exposure",
            "omop.measurement",
            "omop.observation",
            "omop.observation_period"
        ]
        
        missing_tables = []
        for table in tables_to_check:
            schema, table_name = table.split(".")
            exists = execute_query(
                "SELECT EXISTS(SELECT 1 FROM information_schema.tables WHERE table_schema = %s AND table_name = %s)",
                (schema, table_name),
                fetch=True
            )[0][0]
            
            if not exists:
                missing_tables.append(table)
        
        if missing_tables:
            logging.error(ColoredFormatter.error(
                f"❌ The following tables are missing: {', '.join(missing_tables)}. "
                "Please run the database setup script first."
            ))
            return False
        
        logging.info(ColoredFormatter.success("✅ Database setup verified."))
        return True
        
    except Exception as e:
        logging.error(f"Error checking database setup: {e}")
        return False

def get_step_function(step_name: str):
    """Get the function to execute for a given step name."""
    # Map step names to their corresponding functions
    step_functions = {
        "process_patients": process_patients,
        "process_encounters": process_encounters,
        "process_conditions": process_conditions,
        "process_procedures": process_procedures,
        "process_medications": process_medications,
        "process_observations": process_observations,
        "create_observation_periods": create_observation_periods,
        "map_source_to_standard_concepts": map_source_to_standard_concepts,
        "validate_etl_results": validate_etl_results,
        "analyze_tables": analyze_tables
    }
    
    return step_functions.get(step_name)

def run_etl_process(data_dir: str, force_reprocess: bool = False, steps_to_run: Optional[List[str]] = None):
    """
    Run the ETL process with the specified steps.
    
    Args:
        data_dir: Directory containing Synthea CSV files
        force_reprocess: Whether to force reprocessing of all steps
        steps_to_run: List of specific steps to run, or None for all steps
    """
    # Clear checkpoint file if force_reprocess is True
    if force_reprocess:
        clear_checkpoint_file()
    
    # Ensure database is set up
    if not ensure_database_setup():
        return False
    
    # Initialize progress tracker
    progress_tracker = ETLProgressTracker()
    
    # Get list of steps to run
    steps = ETL_STEPS
    if steps_to_run:
        steps = [step for step in ETL_STEPS if step["name"] in steps_to_run]
    
    # Track overall ETL progress
    total_steps = len(steps)
    completed_steps = 0
    
    logging.info(ColoredFormatter.info(f"\n🚀 Starting ETL process with {total_steps} steps"))
    
    # Start timer for the entire process
    start_time = time.time()
    
    # Process each step
    for step in steps:
        step_name = step["name"]
        step_desc = step["description"]
        
        # Check if step should be skipped
        if is_step_completed(step_name, force_reprocess):
            logging.info(ColoredFormatter.info(f"✅ Step '{step_name}' was previously completed. Skipping."))
            completed_steps += 1
            continue
        
        # Check if dependencies are met
        dependencies_met = True
        for dep in step.get("dependencies", []):
            if not is_step_completed(dep, False):
                logging.warning(ColoredFormatter.warning(
                    f"⚠️ Skipping step '{step_name}' because dependency '{dep}' is not completed."
                ))
                dependencies_met = False
                break
        
        if not dependencies_met:
            continue
        
        # Get the function to execute this step
        step_function = get_step_function(step_name)
        
        if step_function:
            logging.info(ColoredFormatter.info(f"\n🔍 Running step {completed_steps+1}/{total_steps}: {step_desc}"))
            
            # Determine CSV file path if applicable
            csv_file = None
            if "csv_file" in step:
                csv_file = os.path.join(data_dir, step["csv_file"])
                if not os.path.exists(csv_file):
                    logging.error(ColoredFormatter.error(f"❌ CSV file not found: {csv_file}"))
                    return False
            
            # Execute the step
            try:
                if csv_file:
                    success = step_function(csv_file, force_reprocess)
                else:
                    success = step_function(force_reprocess)
                
                if success:
                    completed_steps += 1
                else:
                    logging.error(ColoredFormatter.error(f"❌ Step '{step_name}' failed."))
                    return False
                    
            except Exception as e:
                logging.error(ColoredFormatter.error(f"❌ Error executing step '{step_name}': {e}"))
                return False
        else:
            logging.warning(ColoredFormatter.warning(
                f"⚠️ Step '{step_name}' is not implemented yet. Skipping."
            ))
    
    # Calculate total time
    end_time = time.time()
    total_time = end_time - start_time
    
    # Log completion
    if completed_steps == total_steps:
        logging.info(ColoredFormatter.success(
            f"\n✅ ETL process completed successfully! {completed_steps}/{total_steps} steps completed "
            f"in {total_time:.2f} seconds."
        ))
        return True
    else:
        logging.warning(ColoredFormatter.warning(
            f"\n⚠️ ETL process partially completed. {completed_steps}/{total_steps} steps completed "
            f"in {total_time:.2f} seconds."
        ))
        return False

def main():
    """Main entry point for the ETL process."""
    parser = argparse.ArgumentParser(description="Run Synthea to OMOP ETL process")
    parser.add_argument("--data-dir", required=True, help="Directory containing Synthea CSV files")
    parser.add_argument("--force", action="store_true", help="Force reprocessing of all steps")
    parser.add_argument("--steps", nargs="+", help="Specific steps to run (default: all)")
    parser.add_argument("--debug", action="store_true", help="Enable debug logging")
    
    args = parser.parse_args()
    
    # Initialize logging and database connection
    init_logging(debug=args.debug)
    init_db_connection_pool()
    
    # Run ETL process
    success = run_etl_process(args.data_dir, args.force, args.steps)
    
    # Exit with appropriate status code
    sys.exit(0 if success else 1)

if __name__ == "__main__":
    main()
