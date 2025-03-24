#!/usr/bin/env python3
"""
etl_observations.py - Process Synthea 'observations.csv' data into OMOP measurement and observation tables.
With row-by-row or small-batch progress reporting and pre/post row counts.
"""

import os
import logging
import time
import csv
from typing import List, Dict, Any, Optional, Set, Tuple
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

# LOINC codes that should be mapped to measurement table
# This is a subset of common lab tests - in a production system,
# this would be more comprehensive or use a different approach
MEASUREMENT_LOINC_CODES = {
    '8302-2',   # Height
    '3141-9',   # Weight
    '8867-4',   # Heart rate
    '8480-6',   # Systolic BP
    '8462-4',   # Diastolic BP
    '2093-3',   # Cholesterol
    '2571-8',   # Triglycerides
    '2085-9',   # HDL
    '2089-1',   # LDL
    '6690-2',   # WBC
    '718-7',    # Hemoglobin
    '4544-3',   # Hematocrit
    '787-2',    # MCV
    '785-6',    # MCH
    '786-4',    # MCHC
    '788-0',    # RDW
    '777-3',    # Platelets
    '789-8',    # RBC
    '2345-7',   # Glucose
    '2339-0',   # Glucose (fasting)
    '2160-0',   # Creatinine
    '3094-0',   # BUN
    '1751-7',   # Albumin
    '1920-8',   # AST
    '1742-6',   # ALT
    '1975-2',   # Bilirubin
    '2324-2',   # Gamma GT
    '2951-2',   # Sodium
    '2823-3',   # Potassium
    '2075-0',   # Chloride
    '2028-9',   # Carbon dioxide
    '17861-6',  # Calcium
    '2777-1',   # Phosphate
    '2885-2',   # Protein
    '2093-3',   # Cholesterol
    '2571-8',   # Triglycerides
    '2085-9',   # HDL
    '2089-1',   # LDL
    '6301-6',   # INR
    '5902-2',   # PT
    '5895-7',   # PTT
    '4548-4',   # HbA1c
    '2339-0',   # Glucose
    '2157-6',   # Creatinine clearance
    '33914-3',  # Estimated GFR
    '2965-2',   # Specific gravity
    '5811-5',   # pH
    '5767-9',   # Appearance
    '5778-6',   # Color
    '5804-0',   # Protein (urine)
    '5794-3',   # Ketones
    '5802-4',   # Nitrite
    '5799-9',   # Leukocytes
    '20454-5',  # Protein/creatinine ratio
    '14959-1',  # Microalbumin
    '14958-3',  # Microalbumin/creatinine ratio
    '2349-9',   # Glucose (urine)
    '2514-8',   # Ketones (urine)
    '5799-9',   # Leukocytes (urine)
    '5794-3',   # Hemoglobin (urine)
    '25428-4',  # Glucose tolerance test
    '14647-2',  # Cholesterol/HDL ratio
    '2093-3',   # Cholesterol
    '2571-8',   # Triglycerides
    '2085-9',   # HDL
    '2089-1',   # LDL
    '2500-7',   # Iron
    '2498-4',   # Iron binding capacity
    '2714-6',   # Ferritin
    '4679-7',   # Reticulocytes
    '2276-4',   # Ferritin
    '2132-9',   # CRP
    '2276-4',   # Folate
    '2284-8',   # Folate (RBC)
    '2614-6',   # TSH
    '3024-7',   # Thyroxine (T4)
    '3026-2',   # Thyroxine (T4) free
    '3050-2',   # Triiodothyronine (T3)
    '3051-0',   # Triiodothyronine (T3) free
    '2842-3',   # Prolactin
    '2119-6',   # Cortisol
    '2143-6',   # DHEA-S
    '2986-8',   # Testosterone
    '2991-8',   # Testosterone (free)
    '2243-4',   # Estradiol
    '2016-4',   # Carcinoembryonic antigen
    '2857-1',   # PSA
    '10834-0',  # Globulin
    '1798-8',   # Amylase
    '2243-4',   # Estradiol
    '2472-9',   # IgG
    '2458-8',   # IgA
    '2465-3',   # IgM
    '2639-3',   # Urate
    '2532-0',   # Lactate dehydrogenase
    '2324-2',   # Gamma glutamyl transferase
    '2777-1',   # Phosphate
    '2885-2',   # Protein (serum)
    '2951-2',   # Sodium
    '2823-3',   # Potassium
    '2075-0',   # Chloride
    '2028-9',   # Carbon dioxide
    '17861-6',  # Calcium
    '2345-7',   # Glucose
    '2160-0',   # Creatinine
    '3094-0',   # BUN
    '1751-7',   # Albumin
    '1920-8',   # AST
    '1742-6',   # ALT
    '1975-2',   # Bilirubin
    '2093-3',   # Cholesterol
    '2571-8',   # Triglycerides
    '2085-9',   # HDL
    '2089-1',   # LDL
    '2093-3',   # Cholesterol
    '2571-8',   # Triglycerides
    '2085-9',   # HDL
    '2089-1',   # LDL
    '8302-2',   # Height
    '3141-9',   # Weight
    '39156-5',  # BMI
    '8867-4',   # Heart rate
    '8480-6',   # Systolic BP
    '8462-4',   # Diastolic BP
    '2339-0',   # Glucose
    '4548-4',   # HbA1c
    '2160-0',   # Creatinine
    '3094-0',   # BUN
    '1975-2',   # Bilirubin
    '1920-8',   # AST
    '1742-6',   # ALT
    '2324-2',   # Gamma GT
    '2951-2',   # Sodium
    '2823-3',   # Potassium
    '2075-0',   # Chloride
    '2028-9',   # Carbon dioxide
    '17861-6',  # Calcium
    '2777-1',   # Phosphate
    '2885-2',   # Protein
    '2093-3',   # Cholesterol
    '2571-8',   # Triglycerides
    '2085-9',   # HDL
    '2089-1',   # LDL
    '6301-6',   # INR
    '5902-2',   # PT
    '5895-7',   # PTT
    '4548-4',   # HbA1c
    '2339-0',   # Glucose
    '2157-6',   # Creatinine clearance
    '33914-3',  # Estimated GFR
    '2965-2',   # Specific gravity
    '5811-5',   # pH
    '5767-9',   # Appearance
    '5778-6',   # Color
    '5804-0',   # Protein (urine)
    '5794-3',   # Ketones
    '5802-4',   # Nitrite
    '5799-9',   # Leukocytes
    '20454-5',  # Protein/creatinine ratio
    '14959-1',  # Microalbumin
    '14958-3',  # Microalbumin/creatinine ratio
    '2349-9',   # Glucose (urine)
    '2514-8',   # Ketones (urine)
    '5799-9',   # Leukocytes (urine)
    '5794-3',   # Hemoglobin (urine)
    '25428-4',  # Glucose tolerance test
    '14647-2',  # Cholesterol/HDL ratio
}

def process_observations(observations_csv: str, force_reprocess: bool = False, batch_size: int = 1000, 
                      measurement_batch_size: int = 50000, observation_batch_size: int = 50000, 
                      truncate_tables: bool = False) -> bool:
    """
    Process Synthea observations into OMOP measurement and observation tables with detailed progress tracking.
    
    Args:
        observations_csv: Path to the observations CSV file
        force_reprocess: Whether to force reprocessing even if already completed
        
    Returns:
        bool: True if processing was successful, False otherwise
    """
    step_name = "process_observations"
    if is_step_completed(step_name, force_reprocess):
        logging.info(ColoredFormatter.info("✅ Observations were previously processed. Skipping."))
        return True
    
    logging.info(ColoredFormatter.info("\n🔍 Processing observations data..."))
    
    # Make sure the connection pool is initialized properly
    try:
        if connection_pool is None:
            logging.info("Connection pool not initialized, initializing now...")
            init_db_connection_pool()
        
        # Verify the connection pool with a test query
        test_conn = get_connection()
        test_conn.autocommit = True
        with test_conn.cursor() as cur:
            cur.execute("SELECT 1")
            result = cur.fetchone()
            if result and result[0] == 1:
                logging.debug("Connection pool verified with test connection")
            else:
                logging.error("Connection pool verification failed")
                return False
        release_connection(test_conn)
    except Exception as e:
        logging.error(f"Failed to verify connection pool: {e}")
        # Initialize directly with environment variables instead of failing
        logging.info("Attempting to initialize connection pool using environment variables...")
        try:
            init_db_connection_pool(
                host=os.getenv('DB_HOST', 'localhost'),
                port=os.getenv('DB_PORT', '5432'),
                database=os.getenv('DB_NAME', 'ohdsi'),
                user=os.getenv('DB_USER', 'postgres'),
                password=os.getenv('DB_PASSWORD', 'acumenus')
            )
            # Verify again
            test_conn = get_connection()
            with test_conn.cursor() as cur:
                cur.execute("SELECT 1")
            release_connection(test_conn)
            logging.info("Successfully initialized connection pool with environment variables")
        except Exception as e2:
            logging.error(f"Final connection pool initialization failed: {e2}")
            return False
    
    # Initialize progress tracker with the database configuration
    db_config = {
        'dbname': os.getenv('DB_NAME', 'ohdsi'),
        'user': os.getenv('DB_USER', 'postgres'),
        'password': os.getenv('DB_PASSWORD', 'acumenus'),
        'host': os.getenv('DB_HOST', 'localhost'),
        'port': os.getenv('DB_PORT', '5432')
    }
    progress_tracker = ETLProgressTracker(db_config)
    progress_tracker.start_step("ETL", step_name, message="Starting observations processing")
    
    # --- Pre-count rows in the CSV ---
    total_rows = count_csv_rows(observations_csv)
    logging.info(f"Found {total_rows:,} observations in CSV (excluding header).")
    
    # Update progress tracker with total rows
    progress_tracker.update_progress("ETL", step_name, 0, total_items=total_rows, 
                                   message=f"Found {total_rows:,} observations in CSV")
    
    # --- Pre-count in the DB for reference ---
    # Get counts directly using a connection to avoid issues with execute_query return format
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM omop.measurement")
            pre_count_measurement = cur.fetchone()[0]
            cur.execute("SELECT COUNT(*) FROM omop.observation")
            pre_count_observation = cur.fetchone()[0]
            logging.info(f"Current measurement rows (before load): {pre_count_measurement:,}")
            logging.info(f"Current observation rows (before load): {pre_count_observation:,}")
    except Exception as e:
        logging.error(f"Error getting pre-counts: {e}")
        pre_count_measurement = 0
        pre_count_observation = 0
    finally:
        release_connection(conn)
    
    # We will do chunk-based loading: read CSV row by row, accumulate in a batch, then insert.
    BATCH_SIZE = batch_size
    inserted_rows = 0
    start_time = time.time()

    # Create persistent staging table for observations
    temp_table = "staging.observations_raw"
    conn = None

    # Create etl_checkpoints table for tracking progress
    try:
        conn = get_connection()
        conn.autocommit = True
        with conn.cursor() as cur:
            cur.execute("""
            CREATE TABLE IF NOT EXISTS staging.etl_checkpoints (
                process_name TEXT PRIMARY KEY,
                last_processed_id BIGINT,
                last_offset BIGINT,
                total_processed BIGINT,
                last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
            """)
    except Exception as e:
        logging.error(f"Error creating checkpoint table: {e}")
    finally:
        if conn:
            release_connection(conn)
    
    # Check if observations_raw table already exists with data
    conn = get_connection()
    table_exists = False
    rows_loaded = 0
    try:
        conn.autocommit = True
        with conn.cursor() as cur:
            cur.execute(f"""
            SELECT to_regclass('staging.observations_raw') IS NOT NULL AS table_exists;
            """)
            table_exists = cur.fetchone()[0]
            
            if table_exists:
                cur.execute(f"SELECT COUNT(*) FROM {temp_table}")
                rows_loaded = cur.fetchone()[0]
                logging.info(f"Found existing observations_raw table with {rows_loaded:,} rows")
    except Exception as e:
        logging.error(f"Error checking for observations_raw table: {e}")
        table_exists = False
        rows_loaded = 0
    finally:
        release_connection(conn)
    
    # Skip CSV loading if observations_raw already exists and has data
    if table_exists and rows_loaded > 0:
        logging.info(ColoredFormatter.info(f"✅ Using existing observations_raw table with {rows_loaded:,} rows"))
        inserted_rows = rows_loaded
    else:
        # Need to create and load the table
        try:
            # Get a single connection for all operations to ensure tables persist
            conn = get_connection()
            conn.autocommit = False  # We'll manage transactions manually
            
            # 1) Create staging schema and table
            with conn.cursor() as cur:
                # Create staging schema if it doesn't exist
                cur.execute("""
                CREATE SCHEMA IF NOT EXISTS staging;
                """)
                
                # Truncate destination tables if requested
                if truncate_tables:
                    logging.info("Truncating measurement, observation, and mapping tables...")
                    cur.execute("""
                    -- Truncate destination tables
                    TRUNCATE TABLE omop.measurement CASCADE;
                    TRUNCATE TABLE omop.observation CASCADE;
                    
                    -- Create mapping tables if they don't exist
                    CREATE TABLE IF NOT EXISTS staging.measurement_map (
                        source_measurement_id TEXT PRIMARY KEY,
                        measurement_id BIGINT NOT NULL
                    );
                    
                    CREATE TABLE IF NOT EXISTS staging.observation_map (
                        source_observation_id TEXT PRIMARY KEY,
                        observation_id BIGINT NOT NULL
                    );
                    
                    -- Truncate mapping tables to ensure clean state
                    TRUNCATE TABLE staging.measurement_map CASCADE;
                    TRUNCATE TABLE staging.observation_map CASCADE;
                    """)
                    logging.info("Tables truncated and mapping tables prepared successfully")
                
                # Create persistent staging table
                cur.execute(f"""
                DROP TABLE IF EXISTS {temp_table};
                CREATE TABLE {temp_table} (
                    id TEXT,
                    patient_id TEXT,
                    encounter_id TEXT,
                    observation_type VARCHAR(50),
                    code VARCHAR(20),
                    description TEXT,
                    value_as_string TEXT,
                    timestamp TIMESTAMP,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );
                
                -- Create indexes for better performance
                CREATE INDEX idx_observations_raw_patient_id ON {temp_table}(patient_id);
                CREATE INDEX idx_observations_raw_encounter_id ON {temp_table}(encounter_id);
                """)
                conn.commit()
            
            # 2) Insert rows in small batches
            try:
                with conn.cursor() as cur, open(observations_csv, 'r', newline='') as f:
                    reader = csv.DictReader(f)
                    batch = []
                    
                    # Create progress bar
                    progress_bar = create_progress_bar(total_rows, "Loading Observations")
                    
                    for row_idx, row in enumerate(reader, start=1):
                        # Convert row to tuple in same col order as the staging table
                        batch.append((
                            row.get("ID", ""),              # id
                            row.get("PATIENT", ""),         # patient_id
                            row.get("ENCOUNTER", ""),       # encounter_id
                            row.get("TYPE", ""),            # observation_type
                            row.get("CODE", ""),            # code
                            row.get("DESCRIPTION", ""),     # description
                            row.get("VALUE", ""),           # value_as_string
                            row.get("DATE", "")             # timestamp
                        ))
                        
                        # If batch is large enough, insert
                        if len(batch) >= BATCH_SIZE:
                            _insert_observation_batch(cur, batch, temp_table)
                            inserted_rows += len(batch)
                            batch.clear()
                            
                            # Update progress bar and tracker
                            update_progress_bar(progress_bar, BATCH_SIZE)
                            progress_tracker.update_progress("ETL", step_name, inserted_rows, total_items=total_rows,
                                                          message=f"Loaded {inserted_rows:,} of {total_rows:,} observations")
                            
                            # Commit after each batch to ensure data is persisted even if interrupted
                            conn.commit()
                    
                    # leftover batch
                    if batch:
                        _insert_observation_batch(cur, batch, temp_table)
                        inserted_rows += len(batch)
                        update_progress_bar(progress_bar, len(batch))
                        progress_tracker.update_progress("ETL", step_name, inserted_rows, total_items=total_rows,
                                                      message=f"Loaded {inserted_rows:,} of {total_rows:,} observations")
                        # Commit final batch
                        conn.commit()
                    close_progress_bar(progress_bar)
                    
            except Exception as e:
                conn.rollback()
                error_msg = f"Error loading observations: {e}"
                logging.error(error_msg)
                progress_tracker.complete_step("ETL", step_name, False, error_msg)
                release_connection(conn)
                return False
            finally:
                release_connection(conn)
            
            csv_load_time = time.time() - start_time
            logging.info(f"Inserted {inserted_rows:,} rows into {temp_table} in {csv_load_time:.2f} sec.")
        
        except Exception as e:
            error_msg = f"Error setting up observations processing: {e}"
            logging.error(ColoredFormatter.error(f"❌ {error_msg}"))
            progress_tracker.complete_step("ETL", step_name, False, error_msg)
            return False

    # 3) Create sequences and mapping tables if they don't exist
    execute_query("""
    -- Create sequences for generating integer IDs
    CREATE SEQUENCE IF NOT EXISTS staging.measurement_seq;
    CREATE SEQUENCE IF NOT EXISTS staging.observation_seq;
    
    -- Create mapping tables for observation IDs
    CREATE TABLE IF NOT EXISTS staging.measurement_map (
        source_measurement_id TEXT PRIMARY KEY,
        measurement_id BIGINT NOT NULL
    );
    
    CREATE TABLE IF NOT EXISTS staging.observation_map (
        source_observation_id TEXT PRIMARY KEY,
        observation_id BIGINT NOT NULL
    );
    
    -- Create indexes on the mapping tables
    CREATE INDEX IF NOT EXISTS idx_measurement_map_source_id ON staging.measurement_map(source_measurement_id);
    CREATE INDEX IF NOT EXISTS idx_observation_map_source_id ON staging.observation_map(source_observation_id);
    """)
    
    # Debug: Print sample data to understand observation types
    debug_conn = get_connection()
    try:
        with debug_conn.cursor() as cur:
            cur.execute(f"""
            SELECT o.code, o.timestamp, o.patient_id, o.encounter_id, o.value_as_string, '', o.observation_type
            FROM {temp_table} o
            LIMIT 10
            """)
            sample_rows = cur.fetchall()
            
            logging.info("Sample data from observations table:")
            for row in sample_rows:
                code, timestamp, patient_id, encounter_id, value_as_string, _, observation_type = row
                is_numeric = observation_type == 'numeric' or (value_as_string and value_as_string.replace('.', '', 1).replace('-', '', 1).isdigit())
                logging.info(f"Code: {code}, Value: {value_as_string}, Type: {observation_type}, Is Numeric: {is_numeric}")
    finally:
        release_connection(debug_conn)
    
    # 4) Process measurements (quantitative observations) using chunked approach
    progress_tracker.update_progress("ETL", step_name, inserted_rows, total_items=total_rows,
                                  message=f"Creating measurement records")
    
    logging.info("Inserting into measurement table in batches...")
    
    # Use a single connection for all related operations
    conn = get_connection()
    conn.autocommit = False  # Manage transactions manually
    
    try:
        # Check if we have a checkpoint for measurement processing
        with conn.cursor() as cur:
            cur.execute("""
            SELECT last_processed_id, total_processed 
            FROM staging.etl_checkpoints 
            WHERE process_name = 'measurement_processing'
            """)
            result = cur.fetchone()
            
            if result:
                last_id, already_processed = result
                logging.info(f"Resuming measurement processing from ID {last_id}, already processed {already_processed:,} records")
            else:
                last_id = 0
                already_processed = 0
                # Initialize checkpoint
                cur.execute("""
                INSERT INTO staging.etl_checkpoints 
                (process_name, last_processed_id, total_processed, last_offset)
                VALUES ('measurement_processing', 0, 0, 0)
                """)
                conn.commit()

        # Get count of measurements to process
        with conn.cursor() as cur:
            cur.execute(f"""
            SELECT COUNT(*) 
            FROM {temp_table} o
            WHERE o.observation_type = 'numeric' OR o.value_as_string ~ '^[-]?[0-9]+([.][0-9]+)?([eE][-+]?[0-9]+)?$'
            """)
            measurement_records_to_process = cur.fetchone()[0]
            
        logging.info(f"Found {measurement_records_to_process:,} observations to insert into measurement table")
        
        # Check if we already have these measurements in the database
        with conn.cursor() as cur:
            cur.execute(f"""
            SELECT COUNT(*) 
            FROM {temp_table} o
            JOIN staging.person_map pm ON pm.source_patient_id = o.patient_id
            LEFT JOIN staging.visit_map vm ON vm.source_visit_id = o.encounter_id
            WHERE (o.observation_type = 'numeric' OR o.value_as_string ~ '^[-]?[0-9]+([.][0-9]+)?([eE][-+]?[0-9]+)?$')
            AND EXISTS (
                SELECT 1 
                FROM omop.measurement m
                JOIN staging.person_map pm2 ON pm2.person_id = m.person_id
                LEFT JOIN staging.visit_map vm2 ON vm2.visit_occurrence_id = m.visit_occurrence_id
                WHERE pm2.source_patient_id = o.patient_id
                AND (vm2.source_visit_id = o.encounter_id OR (o.encounter_id IS NULL AND m.visit_occurrence_id IS NULL))
                AND m.measurement_source_value = o.code
                AND m.measurement_date = o.timestamp::date
                AND m.value_source_value = o.value_as_string
            )
            """)
            existing_measurements = cur.fetchone()[0]
        
        records_to_insert = measurement_records_to_process - existing_measurements
        remaining_to_insert = records_to_insert - already_processed
        logging.info(f"Found {existing_measurements:,} measurements already in database, need to insert {remaining_to_insert:,}")
        
        if remaining_to_insert <= 0:
            logging.info("No new measurements to insert")
        else:
            # Create progress bar for this operation
            progress_bar = create_progress_bar(remaining_to_insert, "Inserting Measurements")
            
            # Setup batch processing
            chunk_size = 50000  # Adjust based on memory constraints
            processed_this_run = 0
            
            # Process in chunks until completion
            while True:
                # Get next chunk of records based on ID
                with conn.cursor() as cur:
                    cur.execute(f"""
                    SELECT o.id, o.code, o.timestamp, o.patient_id, o.encounter_id, o.value_as_string, '', o.observation_type
                    FROM {temp_table} o
                    JOIN staging.person_map pm ON pm.source_patient_id = o.patient_id
                    LEFT JOIN staging.visit_map vm ON vm.source_visit_id = o.encounter_id
                    WHERE (o.observation_type = 'numeric' OR o.value_as_string ~ '^[-]?[0-9]+([.][0-9]+)?([eE][-+]?[0-9]+)?$')
                    AND o.id::bigint > {last_id}
                    AND NOT EXISTS (
                        SELECT 1 
                        FROM omop.measurement m
                        JOIN staging.person_map pm2 ON pm2.person_id = m.person_id
                        LEFT JOIN staging.visit_map vm2 ON vm2.visit_occurrence_id = m.visit_occurrence_id
                        WHERE pm2.source_patient_id = o.patient_id
                        AND (vm2.source_visit_id = o.encounter_id OR (o.encounter_id IS NULL AND m.visit_occurrence_id IS NULL))
                        AND m.measurement_source_value = o.code
                        AND m.measurement_date = o.timestamp::date
                        AND m.value_source_value = o.value_as_string
                    )
                    ORDER BY o.id
                    LIMIT {chunk_size}
                    """)
                    batch = cur.fetchall()
                
                # Exit loop if no more records to process
                if not batch:
                    break
                
                # Process this batch
                if batch:
                    # Track the last ID for checkpointing
                    last_id = batch[-1][0]  # First column is the ID
                    
                    # Extract the data fields needed for processing
                    process_batch = [row[1:] for row in batch]  # Skip the ID column
                    
                    # Process the batch
                    _process_measurement_batch(conn, process_batch, progress_tracker, step_name, 
                                            progress_bar, already_processed + processed_this_run, records_to_insert)
                    
                    # Update counters
                    processed_this_run += len(batch)
                    
                    # Update checkpoint
                    with conn.cursor() as cur:
                        cur.execute("""
                        UPDATE staging.etl_checkpoints 
                        SET last_processed_id = %s, total_processed = %s, last_updated = CURRENT_TIMESTAMP
                        WHERE process_name = 'measurement_processing'
                        """, (last_id, already_processed + processed_this_run))
                    
                    # Commit after each batch
                    conn.commit()
                    
                    # Update progress bar
                    update_progress_bar(progress_bar, len(batch))
            
            # Close progress bar
            close_progress_bar(progress_bar)
            
            logging.info(f"Successfully processed {processed_this_run:,} measurement records in this run")
    except Exception as e:
        error_msg = f"❌
