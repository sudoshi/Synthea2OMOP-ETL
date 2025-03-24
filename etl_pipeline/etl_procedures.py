#!/usr/bin/env python3
"""
etl_procedures.py - Process Synthea 'procedures.csv' data into OMOP procedure_occurrence.
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

def process_procedures(procedures_csv: str, force_reprocess: bool = False) -> bool:
    """
    Process Synthea procedures into OMOP procedure_occurrence with detailed progress tracking.
    
    Args:
        procedures_csv: Path to the procedures CSV file
        force_reprocess: Whether to force reprocessing even if already completed
        
    Returns:
        bool: True if processing was successful, False otherwise
    """
    step_name = "process_procedures"
    if is_step_completed(step_name, force_reprocess):
        logging.info(ColoredFormatter.info("✅ Procedures were previously processed. Skipping."))
        return True
    
    logging.info(ColoredFormatter.info("\n🔍 Processing procedures data..."))
    
    # Initialize progress tracker
    progress_tracker = ETLProgressTracker()
    progress_tracker.start_step("ETL", step_name, message="Starting procedures processing")
    
    # --- Pre-count rows in the CSV ---
    total_rows = count_csv_rows(procedures_csv)
    logging.info(f"Found {total_rows:,} procedures in CSV (excluding header).")
    
    # Update progress tracker with total rows
    progress_tracker.update_progress("ETL", step_name, 0, total_items=total_rows, 
                                   message=f"Found {total_rows:,} procedures in CSV")
    
    # --- Pre-count in the DB for reference ---
    pre_count_result = execute_query("SELECT COUNT(*) FROM omop.procedure_occurrence", fetch=True)
    pre_count_db = pre_count_result[0][0] if pre_count_result else 0
    logging.info(f"Current procedure_occurrence rows (before load): {pre_count_db:,}")
    
    # We will do chunk-based loading: read CSV row by row, accumulate in a batch, then insert.
    BATCH_SIZE = 1000
    inserted_rows = 0
    start_time = time.time()

    # Create temp table for procedures
    temp_table = "temp_procedures"
    try:
        # 1) Create temp table
        execute_query(f"""
        DROP TABLE IF EXISTS {temp_table};
        CREATE TEMP TABLE {temp_table} (
            "DATE" TEXT,
            "PATIENT" TEXT,
            "ENCOUNTER" TEXT,
            "CODE" TEXT,
            "DESCRIPTION" TEXT,
            "BASE_COST" TEXT,
            "REASONCODE" TEXT,
            "REASONDESCRIPTION" TEXT
        );
        """)
        
        # 2) Insert rows in small batches
        conn = get_connection()
        try:
            with conn.cursor() as cur, open(procedures_csv, 'r', newline='') as f:
                reader = csv.DictReader(f)
                batch = []
                
                # Create progress bar
                progress_bar = create_progress_bar(total_rows, "Loading Procedures")
                
                for row_idx, row in enumerate(reader, start=1):
                    # Convert row to tuple in same col order as temp table
                    batch.append((
                        row.get("DATE", ""),
                        row.get("PATIENT", ""),
                        row.get("ENCOUNTER", ""),
                        row.get("CODE", ""),
                        row.get("DESCRIPTION", ""),
                        row.get("BASE_COST", ""),
                        row.get("REASONCODE", ""),
                        row.get("REASONDESCRIPTION", "")
                    ))
                    
                    # If batch is large enough, insert
                    if len(batch) >= BATCH_SIZE:
                        _insert_procedure_batch(cur, batch, temp_table)
                        inserted_rows += len(batch)
                        batch.clear()
                        
                        # Update progress bar and tracker
                        update_progress_bar(progress_bar, BATCH_SIZE)
                        progress_tracker.update_progress("ETL", step_name, inserted_rows, total_items=total_rows,
                                                      message=f"Loaded {inserted_rows:,} of {total_rows:,} procedures")
                
                # leftover batch
                if batch:
                    _insert_procedure_batch(cur, batch, temp_table)
                    inserted_rows += len(batch)
                    update_progress_bar(progress_bar, len(batch))
                    progress_tracker.update_progress("ETL", step_name, inserted_rows, total_items=total_rows,
                                                  message=f"Loaded {inserted_rows:,} of {total_rows:,} procedures")
                
                conn.commit()
                close_progress_bar(progress_bar)
                
        except Exception as e:
            conn.rollback()
            error_msg = f"Error loading procedures: {e}"
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
                                      message=f"Creating procedure_occurrence records")
        
        # 3) Create sequence for procedure_occurrence_id if it doesn't exist
        execute_query("""
        CREATE SEQUENCE IF NOT EXISTS staging.procedure_occurrence_seq;
        """)
        
        # 4) Insert into omop.procedure_occurrence
        logging.info("Inserting into procedure_occurrence...")
        execute_query(f"""
        INSERT INTO omop.procedure_occurrence (
            procedure_occurrence_id,
            person_id,
            procedure_concept_id,
            procedure_date,
            procedure_datetime,
            procedure_type_concept_id,
            modifier_concept_id,
            quantity,
            provider_id,
            visit_occurrence_id,
            visit_detail_id,
            procedure_source_value,
            procedure_source_concept_id,
            modifier_source_value
        )
        SELECT
            nextval('staging.procedure_occurrence_seq'),
            pm.person_id,
            0, -- Will be mapped in concept mapping step
            p."DATE"::date,
            p."DATE"::timestamp,
            32817, -- EHR
            0,
            1,
            NULL,
            vm.visit_occurrence_id,
            NULL,
            p."CODE",
            0,
            NULL
        FROM {temp_table} p
        JOIN staging.person_map pm ON pm.source_patient_id = p."PATIENT"
        LEFT JOIN staging.visit_map vm ON vm.source_visit_id = p."ENCOUNTER"
        WHERE NOT EXISTS (
            SELECT 1 
            FROM omop.procedure_occurrence po
            JOIN staging.person_map pm2 ON pm2.person_id = po.person_id
            LEFT JOIN staging.visit_map vm2 ON vm2.visit_occurrence_id = po.visit_occurrence_id
            WHERE pm2.source_patient_id = p."PATIENT"
            AND (vm2.source_visit_id = p."ENCOUNTER" OR (p."ENCOUNTER" IS NULL AND po.visit_occurrence_id IS NULL))
            AND po.procedure_source_value = p."CODE"
            AND po.procedure_date = p."DATE"::date
        );
        """)
        
        # Post-count in DB
        post_count_result = execute_query("SELECT COUNT(*) FROM omop.procedure_occurrence", fetch=True)
        post_count_db = post_count_result[0][0] if post_count_result else 0
        new_records = post_count_db - pre_count_db
        
        end_time = time.time()
        total_time = end_time - start_time
        
        logging.info(ColoredFormatter.success(
            f"✅ Successfully processed {new_records:,} procedures " +
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
                                    f"Successfully processed {new_records:,} procedures")
        
        return True
        
    except Exception as e:
        error_msg = f"Error processing procedures: {e}"
        logging.error(ColoredFormatter.error(f"❌ {error_msg}"))
        
        # Update ETL progress tracker with error
        progress_tracker.complete_step("ETL", step_name, False, error_msg)
        
        return False

def _insert_procedure_batch(cur, batch, table_name: str) -> None:
    """
    Helper to do a parameterized INSERT for a batch into the temp table.
    This uses the standard psycopg2 executemany approach.
    """
    insert_sql = f"""
    INSERT INTO {table_name} ("DATE", "PATIENT", "ENCOUNTER", "CODE", "DESCRIPTION", 
                            "BASE_COST", "REASONCODE", "REASONDESCRIPTION")
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
    """
    cur.executemany(insert_sql, batch)

if __name__ == "__main__":
    # This allows the module to be run directly for testing
    import argparse
    
    parser = argparse.ArgumentParser(description="Process Synthea procedures into OMOP CDM")
    parser.add_argument("--procedures-csv", required=True, help="Path to procedures.csv file")
    parser.add_argument("--force", action="store_true", help="Force reprocessing even if already completed")
    parser.add_argument("--debug", action="store_true", help="Enable debug logging")
    
    args = parser.parse_args()
    
    from etl_setup import init_logging, init_db_connection_pool
    
    init_logging(debug=args.debug)
    init_db_connection_pool()
    
    success = process_procedures(args.procedures_csv, args.force)
    sys.exit(0 if success else 1)
