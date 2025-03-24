#!/usr/bin/env python3
"""
etl_encounters.py - Process Synthea 'encounters.csv' data into OMOP visit_occurrence.
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

# Import all needed modules from etl_setup at the top level
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
    ETLProgressTracker,
    db_config,
    init_db_connection_pool
)

def process_encounters(encounters_csv: str, force_reprocess: bool = False) -> bool:
    """
    Process Synthea encounters into OMOP visit_occurrence with detailed progress tracking.
    
    Args:
        encounters_csv: Path to the encounters CSV file
        force_reprocess: Whether to force reprocessing even if already completed
        
    Returns:
        bool: True if processing was successful, False otherwise
    """
    # Verify connection pool is initialized
    try:
        conn = get_connection()
        if conn:
            logging.debug("Database connection verified in process_encounters")
            release_connection(conn)
    except Exception as e:
        logging.error(f"Database connection not available in process_encounters: {e}")
        return False
        
    step_name = "process_encounters"
    if is_step_completed(step_name, force_reprocess):
        logging.info(ColoredFormatter.info("✅ Encounters were previously processed. Skipping."))
        return True
    
    logging.info(ColoredFormatter.info("\n🔍 Processing encounters data..."))
    
    # Initialize progress tracker with the global db_config
    progress_tracker = ETLProgressTracker(db_config)
    progress_tracker.start_step("ETL", step_name, message="Starting encounters processing")
    
    # --- Pre-count rows in the CSV ---
    total_rows = count_csv_rows(encounters_csv)
    logging.info(f"Found {total_rows:,} encounters in CSV (excluding header).")
    
    # Update progress tracker with total rows
    progress_tracker.update_progress("ETL", step_name, 0, total_items=total_rows, 
                                   message=f"Found {total_rows:,} encounters in CSV")
    
    # --- Pre-count in the DB for reference ---
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM omop.visit_occurrence")
            pre_count_db = cur.fetchone()[0]
        logging.info(f"Current visit_occurrence rows (before load): {pre_count_db:,}")
    finally:
        release_connection(conn)
    
    # We will do chunk-based loading: read CSV row by row, accumulate in a batch, then insert.
    BATCH_SIZE = 1000
    inserted_rows = 0
    start_time = time.time()

    # Create temp table for encounters
    temp_table = "temp_encounters"
    try:
        # Get a single connection for the entire process to ensure temp tables persist
        conn = get_connection()
        conn.autocommit = True
        
        try:
            # 1) Create temp table
            with conn.cursor() as cur:
                cur.execute(f"""
                DROP TABLE IF EXISTS {temp_table};
                CREATE TEMP TABLE {temp_table} (
                    "Id" TEXT,
                    "START" TEXT,
                    "STOP" TEXT,
                    "PATIENT" TEXT,
                    "ENCOUNTERCLASS" TEXT,
                    "CODE" TEXT,
                    "DESCRIPTION" TEXT,
                    "BASE_ENCOUNTER_COST" TEXT,
                    "TOTAL_CLAIM_COST" TEXT,
                    "PAYER_COVERAGE" TEXT,
                    "REASONCODE" TEXT,
                    "REASONDESCRIPTION" TEXT,
                    "PROVIDERID" TEXT
                );
                """)
            
            # 2) Insert rows in small batches
            with conn.cursor() as cur, open(encounters_csv, 'r', newline='') as f:
                reader = csv.DictReader(f)
                batch = []
                
                # Create progress bar
                progress_bar = create_progress_bar(total_rows, "Loading Encounters")
                
                for row_idx, row in enumerate(reader, start=1):
                    # Convert row to tuple in same col order as temp table
                    batch.append((
                        row.get("Id", ""),
                        row.get("START", ""),
                        row.get("STOP", ""),
                        row.get("PATIENT", ""),
                        row.get("ENCOUNTERCLASS", ""),
                        row.get("CODE", ""),
                        row.get("DESCRIPTION", ""),
                        row.get("BASE_ENCOUNTER_COST", ""),
                        row.get("TOTAL_CLAIM_COST", ""),
                        row.get("PAYER_COVERAGE", ""),
                        row.get("REASONCODE", ""),
                        row.get("REASONDESCRIPTION", ""),
                        row.get("PROVIDERID", "")
                    ))
                    
                    # If batch is large enough, insert
                    if len(batch) >= BATCH_SIZE:
                        _insert_encounter_batch(cur, batch, temp_table)
                        inserted_rows += len(batch)
                        batch.clear()
                        
                        # Update progress bar and tracker
                        update_progress_bar(progress_bar, BATCH_SIZE)
                        progress_tracker.update_progress("ETL", step_name, inserted_rows, total_items=total_rows,
                                                      message=f"Loaded {inserted_rows:,} of {total_rows:,} encounters")
                
                # leftover batch
                if batch:
                    _insert_encounter_batch(cur, batch, temp_table)
                    inserted_rows += len(batch)
                    update_progress_bar(progress_bar, len(batch))
                    progress_tracker.update_progress("ETL", step_name, inserted_rows, total_items=total_rows,
                                                  message=f"Loaded {inserted_rows:,} of {total_rows:,} encounters")
                
                conn.commit()
                close_progress_bar(progress_bar)
                
        except Exception as e:
            conn.rollback()
            error_msg = f"Error loading encounters: {e}"
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
                                      message=f"Creating visit_map table")
        
        # Get a new connection for the mapping and insertion steps
        conn = get_connection()
        conn.autocommit = True
        
        try:
            # 3) Create or update staging.visit_map
            # This is a critical step that maps UUID encounter IDs to sequential integer IDs
            logging.info("Creating visit_map table...")
            with conn.cursor() as cur:
                cur.execute("""
                CREATE TABLE IF NOT EXISTS staging.visit_map (
                    source_visit_id TEXT PRIMARY KEY,
                    visit_occurrence_id BIGINT NOT NULL
                );
                
                -- Create sequence if it doesn't exist
                CREATE SEQUENCE IF NOT EXISTS staging.visit_occurrence_seq;
                """)
            
            # 4) Populate visit_map with new encounters
            logging.info("Populating visit_map with new encounters...")
            with conn.cursor() as cur:
                cur.execute(f"""
                INSERT INTO staging.visit_map (source_visit_id, visit_occurrence_id)
                SELECT e."Id", nextval('staging.visit_occurrence_seq')
                FROM {temp_table} e
                WHERE NOT EXISTS (
                    SELECT 1 FROM staging.visit_map vm WHERE vm.source_visit_id = e."Id"
                );
                """)
            
            # Update progress tracker
            progress_tracker.update_progress("ETL", step_name, inserted_rows, total_items=total_rows,
                                          message=f"Inserting into visit_occurrence")
            
            # 5) Insert into visit_occurrence in batches
            logging.info("Inserting into visit_occurrence in batches...")
            
            # First, check if we need to process any records
            with conn.cursor() as cur:
                # Check if any records have already been processed
                cur.execute("SELECT COUNT(*) FROM omop.visit_occurrence")
                existing_count = cur.fetchone()[0]
                
                if existing_count >= total_rows:
                    logging.info(f"All {existing_count:,} encounters already exist in visit_occurrence. Skipping insert.")
                    return True
                
                # Count how many records we still need to process
                cur.execute(f"""
                SELECT COUNT(*) 
                FROM {temp_table} e
                JOIN staging.visit_map vm ON vm.source_visit_id = e."Id"
                JOIN staging.person_map pm ON pm.source_patient_id = e."PATIENT"
                WHERE vm.visit_occurrence_id NOT IN (SELECT visit_occurrence_id FROM omop.visit_occurrence)
                """)
                records_to_process = cur.fetchone()[0]
            
            if records_to_process == 0:
                logging.info("No new encounters to insert into visit_occurrence")
                return True
                
            logging.info(f"Found {records_to_process:,} new encounters to insert into visit_occurrence")
            
            # Create progress bar for this operation
            progress_bar = create_progress_bar(records_to_process, "Inserting into visit_occurrence")
            
            # Process in batches of 50,000 records (reduced from 100,000 to be more memory-efficient)
            VISIT_BATCH_SIZE = 50000
            processed_records = 0
            batch_number = 1
            
            # Use a cursor with server-side processing to avoid loading all IDs into memory
            with conn.cursor(name='encounter_cursor') as cur:
                # Use a server-side cursor to avoid memory issues
                cur.execute(f"""
                SELECT e."Id" 
                FROM {temp_table} e
                JOIN staging.visit_map vm ON vm.source_visit_id = e."Id"
                JOIN staging.person_map pm ON pm.source_patient_id = e."PATIENT"
                WHERE vm.visit_occurrence_id NOT IN (SELECT visit_occurrence_id FROM omop.visit_occurrence)
                """)
                
                # Process in batches using the server-side cursor
                batch_ids = []
                for row in cur:
                    batch_ids.append(row[0])
                    
                    # When we've collected a full batch, process it
                    if len(batch_ids) >= VISIT_BATCH_SIZE:
                        # Log the batch progress
                        logging.info(f"Processing batch {batch_number} ({len(batch_ids):,} records)")
                        
                        # Process this batch
                        self._process_visit_batch(conn, temp_table, batch_ids, progress_tracker, step_name, 
                                               progress_bar, processed_records, records_to_process)
                        
                        # Update counters
                        processed_records += len(batch_ids)
                        batch_number += 1
                        batch_ids = []
                        
                        # Commit after each batch
                        conn.commit()
                        
                        # Add a small delay to allow other processes to run
                        time.sleep(0.1)
                
                # Process any remaining records
                if batch_ids:
                    logging.info(f"Processing final batch {batch_number} ({len(batch_ids):,} records)")
                    self._process_visit_batch(conn, temp_table, batch_ids, progress_tracker, step_name, 
                                           progress_bar, processed_records, records_to_process)
                    processed_records += len(batch_ids)
                    conn.commit()
            
            # Close progress bar
            close_progress_bar(progress_bar)
            
            # Final count check
            with conn.cursor() as cur:
                cur.execute("SELECT COUNT(*) FROM omop.visit_occurrence")
                final_count = cur.fetchone()[0]
                logging.info(f"Final visit_occurrence count: {final_count:,} rows")
                
            return True
        finally:
            release_connection(conn)
        
        # Post-count in DB
        conn = get_connection()
        try:
            with conn.cursor() as cur:
                cur.execute("SELECT COUNT(*) FROM omop.visit_occurrence")
                post_count_db = cur.fetchone()[0]
            new_records = post_count_db - pre_count_db
        finally:
            release_connection(conn)
        
        end_time = time.time()
        total_time = end_time - start_time
        
        logging.info(ColoredFormatter.success(
            f"✅ Successfully processed {new_records:,} encounters " +
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
                                    f"Successfully processed {new_records:,} encounters")
        
        return True
        
    except Exception as e:
        error_msg = f"Error processing encounters: {e}"
        logging.error(ColoredFormatter.error(f"❌ {error_msg}"))
        
        # Update ETL progress tracker with error
        progress_tracker.complete_step("ETL", step_name, False, error_msg)
        
        return False

def _process_visit_batch(self, conn, temp_table, batch_ids, progress_tracker, step_name, 
                       progress_bar, processed_records, total_records):
    """Helper method to process a batch of visit records"""
    # Convert batch IDs to SQL string
    batch_ids_str = "','" .join(batch_ids)
    
    with conn.cursor() as cur:
        # Insert this batch
        cur.execute(f"""
        INSERT INTO omop.visit_occurrence (
            visit_occurrence_id,
            person_id,
            visit_concept_id,
            visit_start_date,
            visit_start_datetime,
            visit_end_date,
            visit_end_datetime,
            visit_type_concept_id,
            provider_id,
            care_site_id,
            visit_source_value,
            visit_source_concept_id,
            admitted_from_concept_id,
            admitted_from_source_value,
            discharged_to_concept_id,
            discharged_to_source_value
        )
        SELECT
            vm.visit_occurrence_id,
            pm.person_id,
            CASE e."ENCOUNTERCLASS"
                WHEN 'ambulatory' THEN 9202
                WHEN 'emergency' THEN 9203
                WHEN 'inpatient' THEN 9201
                WHEN 'wellness' THEN 9202
                WHEN 'urgentcare' THEN 9203
                WHEN 'outpatient' THEN 9202
                ELSE 0
            END,
            e."START"::date,
            e."START"::timestamp,
            e."STOP"::date,
            e."STOP"::timestamp,
            32817,  -- EHR
            NULL,
            NULL,
            e."Id",
            0,
            0,
            NULL,
            0,
            NULL
        FROM {temp_table} e
        JOIN staging.visit_map vm ON vm.source_visit_id = e."Id"
        JOIN staging.person_map pm ON pm.source_patient_id = e."PATIENT"
        WHERE e."Id" IN ('{batch_ids_str}')
        """)
        
        # Get number of rows inserted in this batch
        rows_inserted = cur.rowcount
        
        # Update progress
        current_processed = processed_records + rows_inserted
        update_progress_bar(progress_bar, rows_inserted)
        progress_tracker.update_progress("ETL", step_name, current_processed, total_items=total_records,
                                      message=f"Inserted {current_processed:,} of {total_records:,} encounters")

def _insert_encounter_batch(cur, batch, table_name: str) -> None:
    """
    Helper to do a parameterized INSERT for a batch into the temp table.
    This uses the standard psycopg2 executemany approach.
    """
    insert_sql = f"""
    INSERT INTO {table_name} ("Id","START","STOP","PATIENT","ENCOUNTERCLASS","CODE",
            "DESCRIPTION","BASE_ENCOUNTER_COST","TOTAL_CLAIM_COST","PAYER_COVERAGE",
            "REASONCODE","REASONDESCRIPTION","PROVIDERID")
    VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
    """
    cur.executemany(insert_sql, batch)

if __name__ == "__main__":
    # This allows the module to be run directly for testing
    import argparse
    import os
    from dotenv import load_dotenv
    
    # Load environment variables from .env file
    load_dotenv()
    
    parser = argparse.ArgumentParser(description="Process Synthea encounters into OMOP CDM")
    parser.add_argument("--encounters-csv", required=True, help="Path to encounters.csv file")
    parser.add_argument("--force", action="store_true", help="Force reprocessing even if already completed")
    parser.add_argument("--debug", action="store_true", help="Enable debug logging")
    
    args = parser.parse_args()
    
    from etl_pipeline.etl_setup import init_logging, init_db_connection_pool
    
    init_logging(debug=args.debug)
    
    # Initialize DB connection with environment variables
    db_host = os.getenv('DB_HOST', 'localhost')
    db_port = os.getenv('DB_PORT', '5432')
    db_name = os.getenv('DB_NAME', 'ohdsi')  # Use ohdsi as the database name
    db_user = os.getenv('DB_USER', 'postgres')
    db_password = os.getenv('DB_PASSWORD', 'acumenus')
    
    print(f"Connecting to database: {db_host}:{db_port}/{db_name} as {db_user}")
    
    # Update the global db_config with the environment variables
    from etl_pipeline.etl_setup import db_config
    db_config['host'] = db_host
    db_config['port'] = db_port
    db_config['database'] = db_name
    db_config['user'] = db_user
    db_config['password'] = db_password
    
    init_db_connection_pool(
        host=db_host,
        port=db_port,
        database=db_name,
        user=db_user,
        password=db_password
    )
    
    # Verify the connection pool is initialized
    from etl_pipeline.etl_setup import get_connection, release_connection
    try:
        conn = get_connection()
        if conn:
            logging.info("Database connection pool verified")
            release_connection(conn)
    except Exception as e:
        logging.error(f"Failed to verify database connection: {e}")
        sys.exit(1)
    
    success = process_encounters(args.encounters_csv, args.force)
    sys.exit(0 if success else 1)
