#!/usr/bin/env python3
"""
etl_observation_periods.py - Create observation_period records in OMOP CDM.
This module creates observation periods by analyzing the earliest and latest
healthcare events for each person.
"""

import os
import logging
import time
import sys
from pathlib import Path
from typing import Dict, Any, Optional

# Add parent directory to path to import from etl_setup
sys.path.append(str(Path(__file__).parent.parent))
from etl_pipeline.etl_setup import (
    execute_query,
    mark_step_completed,
    is_step_completed,
    get_connection,
    release_connection,
    create_progress_bar,
    update_progress_bar,
    close_progress_bar,
    ColoredFormatter,
    ETLProgressTracker
)

def create_observation_periods(force_reprocess: bool = False) -> bool:
    """
    Create observation_period records for all persons in the OMOP CDM.
    
    Args:
        force_reprocess: Whether to force reprocessing even if already completed
        
    Returns:
        bool: True if processing was successful, False otherwise
    """
    step_name = "create_observation_periods"
    if is_step_completed(step_name, force_reprocess):
        logging.info(ColoredFormatter.info("✅ Observation periods were previously created. Skipping."))
        return True
    
    logging.info(ColoredFormatter.info("\n🔍 Creating observation periods..."))
    
    # Initialize progress tracker
    progress_tracker = ETLProgressTracker()
    progress_tracker.start_step("ETL", step_name, message="Starting observation period creation")
    
    # --- Pre-count persons for reference ---
    person_count_result = execute_query("SELECT COUNT(*) FROM omop.person", fetch=True)
    person_count = person_count_result[0][0] if person_count_result else 0
    logging.info(f"Found {person_count:,} persons to process.")
    
    # Update progress tracker with total rows
    progress_tracker.update_progress("ETL", step_name, 0, total_items=person_count, 
                                   message=f"Found {person_count:,} persons to process")
    
    # --- Pre-count in the DB for reference ---
    pre_count_result = execute_query("SELECT COUNT(*) FROM omop.observation_period", fetch=True)
    pre_count_db = pre_count_result[0][0] if pre_count_result else 0
    logging.info(f"Current observation_period rows (before creation): {pre_count_db:,}")
    
    start_time = time.time()

    try:
        # 1) Create sequence for observation_period_id if it doesn't exist
        execute_query("""
        CREATE SEQUENCE IF NOT EXISTS staging.observation_period_seq;
        """)
        
        # 2) Create a temporary table to store the earliest and latest dates for each person
        logging.info("Calculating observation period dates...")
        progress_tracker.update_progress("ETL", step_name, 0, total_items=person_count, 
                                      message="Calculating observation period dates")
        
        execute_query("""
        DROP TABLE IF EXISTS temp_obs_periods;
        CREATE TEMP TABLE temp_obs_periods AS
        WITH all_dates AS (
            -- Visit dates
            SELECT 
                person_id, 
                visit_start_date AS event_date
            FROM omop.visit_occurrence
            UNION
            SELECT 
                person_id, 
                visit_end_date AS event_date
            FROM omop.visit_occurrence
            WHERE visit_end_date IS NOT NULL
            
            UNION
            
            -- Condition dates
            SELECT 
                person_id, 
                condition_start_date AS event_date
            FROM omop.condition_occurrence
            UNION
            SELECT 
                person_id, 
                condition_end_date AS event_date
            FROM omop.condition_occurrence
            WHERE condition_end_date IS NOT NULL
            
            UNION
            
            -- Procedure dates
            SELECT 
                person_id, 
                procedure_date AS event_date
            FROM omop.procedure_occurrence
            
            UNION
            
            -- Drug exposure dates
            SELECT 
                person_id, 
                drug_exposure_start_date AS event_date
            FROM omop.drug_exposure
            UNION
            SELECT 
                person_id, 
                drug_exposure_end_date AS event_date
            FROM omop.drug_exposure
            WHERE drug_exposure_end_date IS NOT NULL
            
            UNION
            
            -- Measurement dates
            SELECT 
                person_id, 
                measurement_date AS event_date
            FROM omop.measurement
            
            UNION
            
            -- Observation dates
            SELECT 
                person_id, 
                observation_date AS event_date
            FROM omop.observation
        )
        SELECT 
            person_id,
            MIN(event_date) AS start_date,
            MAX(event_date) AS end_date
        FROM all_dates
        WHERE event_date IS NOT NULL
        GROUP BY person_id;
        """)
        
        # 3) Handle persons with no events - use birth date as start and current date as end
        logging.info("Handling persons with no events...")
        progress_tracker.update_progress("ETL", step_name, person_count // 3, total_items=person_count, 
                                      message="Handling persons with no events")
        
        execute_query("""
        INSERT INTO temp_obs_periods (person_id, start_date, end_date)
        SELECT 
            p.person_id,
            MAKE_DATE(p.year_of_birth, COALESCE(p.month_of_birth, 1), COALESCE(p.day_of_birth, 1)) AS start_date,
            CURRENT_DATE AS end_date
        FROM omop.person p
        WHERE NOT EXISTS (
            SELECT 1 FROM temp_obs_periods op WHERE op.person_id = p.person_id
        );
        """)
        
        # 4) Insert into observation_period
        logging.info("Creating observation_period records...")
        progress_tracker.update_progress("ETL", step_name, person_count * 2 // 3, total_items=person_count, 
                                      message="Creating observation_period records")
        
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
            op.person_id,
            op.start_date,
            op.end_date,
            32817 -- EHR
        FROM temp_obs_periods op
        WHERE NOT EXISTS (
            SELECT 1 
            FROM omop.observation_period existing
            WHERE existing.person_id = op.person_id
        );
        """)
        
        # Post-count in DB
        post_count_result = execute_query("SELECT COUNT(*) FROM omop.observation_period", fetch=True)
        post_count_db = post_count_result[0][0] if post_count_result else 0
        new_records = post_count_db - pre_count_db
        
        end_time = time.time()
        total_time = end_time - start_time
        
        logging.info(ColoredFormatter.success(
            f"✅ Successfully created {new_records:,} observation periods " +
            f"({post_count_db:,} total in database) in {total_time:.2f} sec"
        ))
        
        # Mark completion in checkpoint system
        mark_step_completed(step_name, {
            "total_persons": person_count,
            "new_observation_periods": new_records,
            "processing_time_sec": total_time
        })
        
        # Update ETL progress tracker with completion status
        progress_tracker.complete_step("ETL", step_name, True, 
                                    f"Successfully created {new_records:,} observation periods")
        
        return True
        
    except Exception as e:
        error_msg = f"Error creating observation periods: {e}"
        logging.error(ColoredFormatter.error(f"❌ {error_msg}"))
        
        # Update ETL progress tracker with error
        progress_tracker.complete_step("ETL", step_name, False, error_msg)
        
        return False

if __name__ == "__main__":
    # This allows the module to be run directly for testing
    import argparse
    
    parser = argparse.ArgumentParser(description="Create observation periods in OMOP CDM")
    parser.add_argument("--force", action="store_true", help="Force reprocessing even if already completed")
    parser.add_argument("--debug", action="store_true", help="Enable debug logging")
    
    args = parser.parse_args()
    
    from etl_setup import init_logging, init_db_connection_pool
    
    init_logging(debug=args.debug)
    init_db_connection_pool()
    
    success = create_observation_periods(args.force)
    sys.exit(0 if success else 1)
