#!/usr/bin/env python3
"""
etl_medications.py - Process Synthea 'medications.csv' data into OMOP drug_exposure.
With row-by-row or small-batch progress reporting and pre/post row counts.
"""

import os
import logging
import time
import csv
from typing import List, Dict, Any, Optional
import sys
from pathlib import Path

# Add parent directory to path to import from etl_setup
sys.path.append(str(Path(__file__).parent.parent))
from etl_pipeline.etl_setup import (
    execute_query,
    mark_step_completed,
    is_step_completed,
    get_connection,
    release_connection,
    count_csv_rows,
    create_progress_bar,
    update_progress_bar,
    close_progress_bar,
    ColoredFormatter,
    ETLProgressTracker
)

def process_medications(medications_csv: str, force_reprocess: bool = False) -> bool:
    """
    Process Synthea medications into OMOP drug_exposure with detailed progress tracking.
    
    Args:
        medications_csv: Path to the medications CSV file
        force_reprocess: Whether to force reprocessing even if already completed
        
    Returns:
        bool: True if processing was successful, False otherwise
    """
    step_name = "process_medications"
    if is_step_completed(step_name, force_reprocess):
        logging.info(ColoredFormatter.info("✅ Medications were previously processed. Skipping."))
        return True
    
    logging.info(ColoredFormatter.info("\n🔍 Processing medications data..."))
    
    # Initialize progress tracker
    progress_tracker = ETLProgressTracker()
    progress_tracker.start_step("ETL", step_name, message="Starting medications processing")
    
    # --- Pre-count rows in the CSV ---
    total_rows = count_csv_rows(medications_csv)
    logging.info(f"Found {total_rows:,} medications in CSV (excluding header).")
    
    # Update progress tracker with total rows
    progress_tracker.update_progress("ETL", step_name, 0, total_items=total_rows, 
                                   message=f"Found {total_rows:,} medications in CSV")
    
    # --- Pre-count in the DB for reference ---
    pre_count_result = execute_query("SELECT COUNT(*) FROM omop.drug_exposure", fetch=True)
    pre_count_db = pre_count_result[0][0] if pre_count_result else 0
    logging.info(f"Current drug_exposure rows (before load): {pre_count_db:,}")
    
    # We will do chunk-based loading: read CSV row by row, accumulate in a batch, then insert.
    BATCH_SIZE = 1000
    inserted_rows = 0
    start_time = time.time()

    # Create temp table for medications
    temp_table = "temp_medications"
    try:
        # 1) Create temp table
        execute_query(f"""
        DROP TABLE IF EXISTS {temp_table};
        CREATE TEMP TABLE {temp_table} (
            "START" TEXT,
            "STOP" TEXT,
            "PATIENT" TEXT,
            "ENCOUNTER" TEXT,
            "CODE" TEXT,
            "DESCRIPTION" TEXT,
            "BASE_COST" TEXT,
            "PAYER_COVERAGE" TEXT,
            "DISPENSES" TEXT,
            "TOTALCOST" TEXT,
            "REASONCODE" TEXT,
            "REASONDESCRIPTION" TEXT
        );
        """)
        
        # 2) Insert rows in small batches
        conn = get_connection()
        try:
            with conn.cursor() as cur, open(medications_csv, 'r', newline='') as f:
                reader = csv.DictReader(f)
                batch = []
                
                # Create progress bar
                progress_bar = create_progress_bar(total_rows, "Loading Medications")
                
                for row_idx, row in enumerate(reader, start=1):
                    # Convert row to tuple in same col order as temp table
                    batch.append((
                        row.get("START", ""),
                        row.get("STOP", ""),
                        row.get("PATIENT", ""),
                        row.get("ENCOUNTER", ""),
                        row.get("CODE", ""),
                        row.get("DESCRIPTION", ""),
                        row.get("BASE_COST", ""),
                        row.get("PAYER_COVERAGE", ""),
                        row.get("DISPENSES", ""),
                        row.get("TOTALCOST", ""),
                        row.get("REASONCODE", ""),
                        row.get("REASONDESCRIPTION", "")
                    ))
                    
                    # If batch is large enough, insert
                    if len(batch) >= BATCH_SIZE:
                        _insert_medication_batch(cur, batch, temp_table)
                        inserted_rows += len(batch)
                        batch.clear()
                        
                        # Update progress bar and tracker
                        update_progress_bar(progress_bar, BATCH_SIZE)
                        progress_tracker.update_progress("ETL", step_name, inserted_rows, total_items=total_rows,
                                                      message=f"Loaded {inserted_rows:,} of {total_rows:,} medications")
                
                # leftover batch
                if batch:
                    _insert_medication_batch(cur, batch, temp_table)
                    inserted_rows += len(batch)
                    update_progress_bar(progress_bar, len(batch))
                    progress_tracker.update_progress("ETL", step_name, inserted_rows, total_items=total_rows,
                                                  message=f"Loaded {inserted_rows:,} of {total_rows:,} medications")
                
                conn.commit()
                close_progress_bar(progress_bar)
                
        except Exception as e:
            conn.rollback()
            error_msg = f"Error loading medications: {e}"
            logging.error(error_msg)
            progress_tracker.complete_step("ETL", step_name, False, error_msg)
            release_connection(conn)
            return False
        finally:
            release_connection(conn)
        
        csv_load_time = time.time() - start_time
        logging.info(f"Inserted {inserted_rows:,} rows into {temp_table} in {csv_load_time:.2f} sec.")
        
        # Update progress tracker
        progress_tracker.update_progress("ETL", step_name, inserted_rows, total_items=total_rows,
                                      message=f"Creating drug_exposure records")
        
        # 3) Create sequence for drug_exposure_id if it doesn't exist
        execute_query("""
        CREATE SEQUENCE IF NOT EXISTS staging.drug_exposure_seq;
        """)
        
        # 4) Insert into omop.drug_exposure
        logging.info("Inserting into drug_exposure...")
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
            0, -- Will be mapped in concept mapping step
            m."START"::date,
            m."START"::timestamp,
            CASE WHEN m."STOP" IS NULL OR m."STOP" = '' THEN NULL ELSE m."STOP"::date END,
            CASE WHEN m."STOP" IS NULL OR m."STOP" = '' THEN NULL ELSE m."STOP"::timestamp END,
            NULL,
            32817, -- EHR
            NULL,
            CASE WHEN m."DISPENSES" IS NULL OR m."DISPENSES" = '' THEN 0 ELSE m."DISPENSES"::integer - 1 END,
            1, -- Default quantity
            CASE 
                WHEN m."STOP" IS NULL OR m."STOP" = '' THEN NULL 
                ELSE EXTRACT(DAY FROM (m."STOP"::timestamp - m."START"::timestamp))::integer
            END,
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
        LEFT JOIN staging.visit_map vm ON vm.source_visit_id = m."ENCOUNTER"
        WHERE NOT EXISTS (
            SELECT 1 
            FROM omop.drug_exposure de
            JOIN staging.person_map pm2 ON pm2.person_id = de.person_id
            LEFT JOIN staging.visit_map vm2 ON vm2.visit_occurrence_id = de.visit_occurrence_id
            WHERE pm2.source_patient_id = m."PATIENT"
            AND (vm2.source_visit_id = m."ENCOUNTER" OR (m."ENCOUNTER" IS NULL AND de.visit_occurrence_id IS NULL))
            AND de.drug_source_value = m."CODE"
            AND de.drug_exposure_start_date = m."START"::date
        );
        """)
        
        # Post-count in DB
        post_count_result = execute_query("SELECT COUNT(*) FROM omop.drug_exposure", fetch=True)
        post_count_db = post_count_result[0][0] if post_count_result else 0
        new_records = post_count_db - pre_count_db
        
        end_time = time.time()
        total_time = end_time - start_time
        
        logging.info(ColoredFormatter.success(
            f"✅ Successfully processed {new_records:,} medications " +
            f"({post_count_db:,} total in database) in {total_time:.2f} sec"
        ))
        
        # Mark completion in checkpoint system
        mark_step_completed(step_name, {
            "csv_rows": total_rows,
            "inserted_rows": inserted_rows,
            "db_new_records": new_records,
            "processing_time_sec": total_time
        })
        
        # Update ETL progress tracker with completion status
        progress_tracker.complete_step("ETL", step_name, True, 
                                    f"Successfully processed {new_records:,} medications")
        
        return True
        
    except Exception as e:
        error_msg = f"Error processing medications: {e}"
        logging.error(ColoredFormatter.error(f"❌ {error_msg}"))
        
        # Update ETL progress tracker with error
        progress_tracker.complete_step("ETL", step_name, False, error_msg)
        
        return False

def _insert_medication_batch(cur, batch, table_name: str) -> None:
    """
    Helper to do a parameterized INSERT for a batch into the temp table.
    This uses the standard psycopg2 executemany approach.
    """
    insert_sql = f"""
    INSERT INTO {table_name} ("START", "STOP", "PATIENT", "ENCOUNTER", "CODE", "DESCRIPTION", 
                            "BASE_COST", "PAYER_COVERAGE", "DISPENSES", "TOTALCOST", 
                            "REASONCODE", "REASONDESCRIPTION")
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """
    cur.executemany(insert_sql, batch)

if __name__ == "__main__":
    # This allows the module to be run directly for testing
    import argparse
    
    parser = argparse.ArgumentParser(description="Process Synthea medications into OMOP CDM")
    parser.add_argument("--medications-csv", required=True, help="Path to medications.csv file")
    parser.add_argument("--force", action="store_true", help="Force reprocessing even if already completed")
    parser.add_argument("--debug", action="store_true", help="Enable debug logging")
    
    args = parser.parse_args()
    
    from etl_setup import init_logging, init_db_connection_pool
    
    init_logging(debug=args.debug)
    init_db_connection_pool()
    
    success = process_medications(args.medications_csv, args.force)
    sys.exit(0 if success else 1)
