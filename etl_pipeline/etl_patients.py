#!/usr/bin/env python3
"""
etl_patients.py - Process Synthea 'patients.csv' data into OMOP person table.
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

def process_patients(patients_csv: str, force_reprocess: bool = False) -> bool:
    """
    Process Synthea patients into OMOP person table with detailed progress tracking.
    
    Args:
        patients_csv: Path to the patients CSV file
        force_reprocess: Whether to force reprocessing even if already completed
        
    Returns:
        bool: True if processing was successful, False otherwise
    """
    step_name = "process_patients"
    if is_step_completed(step_name, force_reprocess):
        logging.info(ColoredFormatter.info("✅ Patients were previously processed. Skipping."))
        return True
    
    logging.info(ColoredFormatter.info("\n🔍 Processing patients data..."))
    
    # Initialize progress tracker
    progress_tracker = ETLProgressTracker()
    progress_tracker.start_step("ETL", step_name, message="Starting patients processing")
    
    # --- Pre-count rows in the CSV ---
    total_rows = count_csv_rows(patients_csv)
    logging.info(f"Found {total_rows:,} patients in CSV (excluding header).")
    
    # Update progress tracker with total rows
    progress_tracker.update_progress("ETL", step_name, 0, total_items=total_rows, 
                                   message=f"Found {total_rows:,} patients in CSV")
    
    # --- Pre-count in the DB for reference ---
    pre_count_result = execute_query("SELECT COUNT(*) FROM omop.person", fetch=True)
    pre_count_db = pre_count_result[0][0] if pre_count_result else 0
    logging.info(f"Current person rows (before load): {pre_count_db:,}")
    
    # We will do chunk-based loading: read CSV row by row, accumulate in a batch, then insert.
    BATCH_SIZE = 1000
    inserted_rows = 0
    start_time = time.time()

    # Create temp table for patients
    temp_table = "temp_patients"
    try:
        # 1) Create temp table
        execute_query(f"""
        DROP TABLE IF EXISTS {temp_table};
        CREATE TEMP TABLE {temp_table} (
            "Id" TEXT,
            "BIRTHDATE" TEXT,
            "DEATHDATE" TEXT,
            "SSN" TEXT,
            "DRIVERS" TEXT,
            "PASSPORT" TEXT,
            "PREFIX" TEXT,
            "FIRST" TEXT,
            "LAST" TEXT,
            "SUFFIX" TEXT,
            "MAIDEN" TEXT,
            "MARITAL" TEXT,
            "RACE" TEXT,
            "ETHNICITY" TEXT,
            "GENDER" TEXT,
            "BIRTHPLACE" TEXT,
            "ADDRESS" TEXT,
            "CITY" TEXT,
            "STATE" TEXT,
            "COUNTY" TEXT,
            "ZIP" TEXT,
            "LAT" TEXT,
            "LON" TEXT,
            "HEALTHCARE_EXPENSES" TEXT,
            "HEALTHCARE_COVERAGE" TEXT,
            "INCOME" TEXT
        );
        """)
        
        # 2) Insert rows in small batches
        conn = get_connection()
        try:
            with conn.cursor() as cur, open(patients_csv, 'r', newline='') as f:
                reader = csv.DictReader(f)
                batch = []
                
                # Create progress bar
                progress_bar = create_progress_bar(total_rows, "Loading Patients")
                
                for row_idx, row in enumerate(reader, start=1):
                    # Convert row to tuple in same col order as temp table
                    batch.append((
                        row.get("Id", ""),
                        row.get("BIRTHDATE", ""),
                        row.get("DEATHDATE", ""),
                        row.get("SSN", ""),
                        row.get("DRIVERS", ""),
                        row.get("PASSPORT", ""),
                        row.get("PREFIX", ""),
                        row.get("FIRST", ""),
                        row.get("LAST", ""),
                        row.get("SUFFIX", ""),
                        row.get("MAIDEN", ""),
                        row.get("MARITAL", ""),
                        row.get("RACE", ""),
                        row.get("ETHNICITY", ""),
                        row.get("GENDER", ""),
                        row.get("BIRTHPLACE", ""),
                        row.get("ADDRESS", ""),
                        row.get("CITY", ""),
                        row.get("STATE", ""),
                        row.get("COUNTY", ""),
                        row.get("ZIP", ""),
                        row.get("LAT", ""),
                        row.get("LON", ""),
                        row.get("HEALTHCARE_EXPENSES", ""),
                        row.get("HEALTHCARE_COVERAGE", ""),
                        row.get("INCOME", "")
                    ))
                    
                    # If batch is large enough, insert
                    if len(batch) >= BATCH_SIZE:
                        _insert_patient_batch(cur, batch, temp_table)
                        inserted_rows += len(batch)
                        batch.clear()
                        
                        # Update progress bar and tracker
                        update_progress_bar(progress_bar, BATCH_SIZE)
                        progress_tracker.update_progress("ETL", step_name, inserted_rows, total_items=total_rows,
                                                      message=f"Loaded {inserted_rows:,} of {total_rows:,} patients")
                
                # leftover batch
                if batch:
                    _insert_patient_batch(cur, batch, temp_table)
                    inserted_rows += len(batch)
                    update_progress_bar(progress_bar, len(batch))
                    progress_tracker.update_progress("ETL", step_name, inserted_rows, total_items=total_rows,
                                                  message=f"Loaded {inserted_rows:,} of {total_rows:,} patients")
                
                conn.commit()
                close_progress_bar(progress_bar)
                
        except Exception as e:
            conn.rollback()
            error_msg = f"Error loading patients: {e}"
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
                                      message=f"Creating person_map table")
        
        # 3) Create or update staging.person_map
        # This is a critical step that maps UUID patient IDs to sequential integer IDs
        logging.info("Creating person_map table...")
        execute_query("""
        CREATE TABLE IF NOT EXISTS staging.person_map (
            source_patient_id TEXT PRIMARY KEY,
            person_id BIGINT NOT NULL
        );
        
        -- Create sequence if it doesn't exist
        CREATE SEQUENCE IF NOT EXISTS staging.person_seq;
        """)
        
        # 4) Populate person_map with new patients
        logging.info("Populating person_map with new patients...")
        execute_query(f"""
        INSERT INTO staging.person_map (source_patient_id, person_id)
        SELECT p."Id", nextval('staging.person_seq')
        FROM {temp_table} p
        WHERE NOT EXISTS (
            SELECT 1 FROM staging.person_map pm WHERE pm.source_patient_id = p."Id"
        );
        """)
        
        # Update progress tracker
        progress_tracker.update_progress("ETL", step_name, inserted_rows, total_items=total_rows,
                                      message=f"Inserting into person table")
        
        # 5) Insert into omop.person
        logging.info("Inserting into person table...")
        execute_query(f"""
        INSERT INTO omop.person (
            person_id,
            gender_concept_id,
            year_of_birth,
            month_of_birth,
            day_of_birth,
            birth_datetime,
            race_concept_id,
            ethnicity_concept_id,
            location_id,
            provider_id,
            care_site_id,
            person_source_value,
            gender_source_value,
            gender_source_concept_id,
            race_source_value,
            race_source_concept_id,
            ethnicity_source_value,
            ethnicity_source_concept_id
        )
        SELECT
            pm.person_id,
            CASE p."GENDER"
                WHEN 'M' THEN 8507
                WHEN 'F' THEN 8532
                ELSE 0
            END,
            EXTRACT(YEAR FROM p."BIRTHDATE"::date),
            EXTRACT(MONTH FROM p."BIRTHDATE"::date),
            EXTRACT(DAY FROM p."BIRTHDATE"::date),
            p."BIRTHDATE"::timestamp,
            CASE p."RACE"
                WHEN 'white' THEN 8527
                WHEN 'black' THEN 8516
                WHEN 'asian' THEN 8515
                ELSE 0
            END,
            CASE p."ETHNICITY"
                WHEN 'hispanic' THEN 38003563
                WHEN 'nonhispanic' THEN 38003564
                ELSE 0
            END,
            NULL,
            NULL,
            NULL,
            p."Id",
            p."GENDER",
            0,
            p."RACE",
            0,
            p."ETHNICITY",
            0
        FROM {temp_table} p
        JOIN staging.person_map pm ON pm.source_patient_id = p."Id"
        WHERE pm.person_id NOT IN (SELECT person_id FROM omop.person);
        """)
        
        # Post-count in DB
        post_count_result = execute_query("SELECT COUNT(*) FROM omop.person", fetch=True)
        post_count_db = post_count_result[0][0] if post_count_result else 0
        new_records = post_count_db - pre_count_db
        
        end_time = time.time()
        total_time = end_time - start_time
        
        logging.info(ColoredFormatter.success(
            f"✅ Successfully processed {new_records:,} patients " +
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
                                    f"Successfully processed {new_records:,} patients")
        
        return True
        
    except Exception as e:
        error_msg = f"Error processing patients: {e}"
        logging.error(ColoredFormatter.error(f"❌ {error_msg}"))
        
        # Update ETL progress tracker with error
        progress_tracker.complete_step("ETL", step_name, False, error_msg)
        
        return False

def _insert_patient_batch(cur, batch, table_name: str) -> None:
    """
    Helper to do a parameterized INSERT for a batch into the temp table.
    This uses the standard psycopg2 executemany approach.
    """
    insert_sql = f"""
    INSERT INTO {table_name} ("Id","BIRTHDATE","DEATHDATE","SSN","DRIVERS","PASSPORT",
            "PREFIX","FIRST","LAST","SUFFIX","MAIDEN","MARITAL","RACE","ETHNICITY",
            "GENDER","BIRTHPLACE","ADDRESS","CITY","STATE","COUNTY","ZIP","LAT","LON",
            "HEALTHCARE_EXPENSES","HEALTHCARE_COVERAGE","INCOME")
    VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
    """
    cur.executemany(insert_sql, batch)

if __name__ == "__main__":
    # This allows the module to be run directly for testing
    import argparse
    
    parser = argparse.ArgumentParser(description="Process Synthea patients into OMOP CDM")
    parser.add_argument("--patients-csv", required=True, help="Path to patients.csv file")
    parser.add_argument("--force", action="store_true", help="Force reprocessing even if already completed")
    parser.add_argument("--debug", action="store_true", help="Enable debug logging")
    
    args = parser.parse_args()
    
    from etl_setup import init_logging, init_db_connection_pool
    
    init_logging(debug=args.debug)
    init_db_connection_pool()
    
    success = process_patients(args.patients_csv, args.force)
    sys.exit(0 if success else 1)
