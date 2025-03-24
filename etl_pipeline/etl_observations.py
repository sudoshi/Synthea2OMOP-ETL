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
                
                # leftover batch
                if batch:
                    _insert_observation_batch(cur, batch, temp_table)
                    inserted_rows += len(batch)
                    update_progress_bar(progress_bar, len(batch))
                    progress_tracker.update_progress("ETL", step_name, inserted_rows, total_items=total_rows,
                                                  message=f"Loaded {inserted_rows:,} of {total_rows:,} observations")
                
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
        
        # 4) Process measurements (quantitative observations)
        progress_tracker.update_progress("ETL", step_name, inserted_rows, total_items=total_rows,
                                      message=f"Creating measurement records")
        
        logging.info("Inserting into measurement table in batches...")
        
        # Use a single connection for all related operations
        conn = get_connection()
        conn.autocommit = False  # Manage transactions manually
        try:
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
            logging.info(f"Found {existing_measurements:,} measurements already in database, need to insert {records_to_insert:,}")
            
            if records_to_insert == 0:
                logging.info("No new measurements to insert")
            else:
                # Create progress bar for this operation
                progress_bar = create_progress_bar(records_to_insert, "Inserting Measurements")
                
                # Process in batches
                MEASUREMENT_BATCH_SIZE = measurement_batch_size
                processed_records = 0
                
                # Use a cursor with server-side processing to avoid loading all IDs into memory
                with conn.cursor(name='measurement_cursor') as cur:
                    # Get all observation IDs that need to be processed
                    cur.execute(f"""
                    SELECT o.code, o.timestamp, o.patient_id, o.encounter_id, o.value_as_string, '', o.observation_type
                    FROM {temp_table} o
                    JOIN staging.person_map pm ON pm.source_patient_id = o.patient_id
                    LEFT JOIN staging.visit_map vm ON vm.source_visit_id = o.encounter_id
                    WHERE (o.observation_type = 'numeric' OR o.value_as_string ~ '^[-]?[0-9]+([.][0-9]+)?([eE][-+]?[0-9]+)?$')
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
                    """)
                    
                    # Process in batches
                    batch = []
                    for row in cur:
                        batch.append(row)
                        
                        # When we've collected a full batch, process it
                        if len(batch) >= MEASUREMENT_BATCH_SIZE:
                            # Process this batch
                            _process_measurement_batch(conn, batch, progress_tracker, step_name, 
                                                    progress_bar, processed_records, records_to_insert)
                            
                            # Update counters
                            processed_records += len(batch)
                            batch = []
                            
                            # Commit after each batch
                            conn.commit()
                    
                    # Process any remaining records
                    if batch:
                        _process_measurement_batch(conn, batch, progress_tracker, step_name, 
                                                progress_bar, processed_records, records_to_insert)
                        processed_records += len(batch)
                        conn.commit()
                
                # Close progress bar
                close_progress_bar(progress_bar)
        finally:
            release_connection(conn)
        
        # 5) Process observations (non-quantitative observations)
        progress_tracker.update_progress("ETL", step_name, inserted_rows, total_items=total_rows,
                                      message=f"Creating observation records")
        
        logging.info("Inserting into observation table in batches...")
        
        # Use a single connection for all related operations
        conn = get_connection()
        conn.autocommit = False  # Manage transactions manually
        try:
            with conn.cursor() as cur:
                cur.execute(f"""
                SELECT COUNT(*) 
                FROM {temp_table} o
                WHERE NOT (o.observation_type = 'numeric' OR o.value_as_string ~ '^[-]?[0-9]+([.][0-9]+)?([eE][-+]?[0-9]+)?$')
                """)
                observation_records_to_process = cur.fetchone()[0]
                
            logging.info(f"Found {observation_records_to_process:,} observations to insert into observation table")
            
            # Check if we already have these observations in the database
            with conn.cursor() as cur:
                cur.execute(f"""
                SELECT COUNT(*) 
                FROM {temp_table} o
                JOIN staging.person_map pm ON pm.source_patient_id = o.patient_id
                LEFT JOIN staging.visit_map vm ON vm.source_visit_id = o.encounter_id
                WHERE NOT (o.observation_type = 'numeric' OR o.value_as_string ~ '^[-]?[0-9]+([.][0-9]+)?([eE][-+]?[0-9]+)?$')
                AND EXISTS (
                    SELECT 1 
                    FROM omop.observation obs
                    JOIN staging.person_map pm2 ON pm2.person_id = obs.person_id
                    LEFT JOIN staging.visit_map vm2 ON vm2.visit_occurrence_id = obs.visit_occurrence_id
                    WHERE pm2.source_patient_id = o.patient_id
                    AND (vm2.source_visit_id = o.encounter_id OR (o.encounter_id IS NULL AND obs.visit_occurrence_id IS NULL))
                    AND obs.observation_source_value = o.code
                    AND obs.observation_date = o.timestamp::date
                    AND obs.value_source_value = o.value_as_string
                )
                """)
                existing_observations = cur.fetchone()[0]
            
            records_to_insert = observation_records_to_process - existing_observations
            logging.info(f"Found {existing_observations:,} observations already in database, need to insert {records_to_insert:,}")
            
            if records_to_insert == 0:
                logging.info("No new observations to insert")
            else:
                # Create progress bar for this operation
                progress_bar = create_progress_bar(records_to_insert, "Inserting Observations")
                
                # Process in batches
                OBSERVATION_BATCH_SIZE = observation_batch_size
                processed_records = 0
                
                # Use a cursor with server-side processing to avoid loading all IDs into memory
                with conn.cursor(name='observation_cursor') as cur:
                    # Get all observation IDs that need to be processed
                    cur.execute(f"""
                    SELECT o.code, o.timestamp, o.patient_id, o.encounter_id, o.value_as_string, '', o.observation_type
                    FROM {temp_table} o
                    JOIN staging.person_map pm ON pm.source_patient_id = o.patient_id
                    LEFT JOIN staging.visit_map vm ON vm.source_visit_id = o.encounter_id
                    WHERE NOT (o.observation_type = 'numeric' OR o.value_as_string ~ '^[-]?[0-9]+([.][0-9]+)?([eE][-+]?[0-9]+)?$')
                    AND NOT EXISTS (
                        SELECT 1 
                        FROM omop.observation obs
                        JOIN staging.person_map pm2 ON pm2.person_id = obs.person_id
                        LEFT JOIN staging.visit_map vm2 ON vm2.visit_occurrence_id = obs.visit_occurrence_id
                        WHERE pm2.source_patient_id = o.patient_id
                        AND (vm2.source_visit_id = o.encounter_id OR (o.encounter_id IS NULL AND obs.visit_occurrence_id IS NULL))
                        AND obs.observation_source_value = o.code
                        AND obs.observation_date = o.timestamp::date
                        AND obs.value_source_value = o.value_as_string
                    )
                    """)
                    
                    # Process in batches
                    batch = []
                    for row in cur:
                        batch.append(row)
                        
                        # When we've collected a full batch, process it
                        if len(batch) >= OBSERVATION_BATCH_SIZE:
                            # Process this batch
                            _process_observation_batch(conn, batch, progress_tracker, step_name, 
                                                    progress_bar, processed_records, records_to_insert)
                            
                            # Update counters
                            processed_records += len(batch)
                            batch = []
                            
                            # Commit after each batch
                            conn.commit()
                    
                    # Process any remaining records
                    if batch:
                        _process_observation_batch(conn, batch, progress_tracker, step_name, 
                                                progress_bar, processed_records, records_to_insert)
                        processed_records += len(batch)
                        conn.commit()
                
                # Close progress bar
                close_progress_bar(progress_bar)
        finally:
            release_connection(conn)
        
        # Post-count in DB - use direct connection to avoid issues with execute_query return format
        conn = get_connection()
        try:
            with conn.cursor() as cur:
                cur.execute("SELECT COUNT(*) FROM omop.measurement")
                post_count_measurement = cur.fetchone()[0]
                cur.execute("SELECT COUNT(*) FROM omop.observation")
                post_count_observation = cur.fetchone()[0]
                new_measurements = post_count_measurement - pre_count_measurement
                new_observations = post_count_observation - pre_count_observation
        except Exception as e:
            logging.error(f"Error getting post-counts: {e}")
            post_count_measurement = pre_count_measurement
            post_count_observation = pre_count_observation
            new_measurements = 0
            new_observations = 0
        finally:
            release_connection(conn)
        
        end_time = time.time()
        total_time = end_time - start_time
        
        logging.info(ColoredFormatter.success(
            f"✅ Successfully processed {inserted_rows:,} observations into " +
            f"{new_measurements:,} measurements and {new_observations:,} observations " +
            f"in {total_time:.2f} sec"
        ))
        
        # Mark completion in checkpoint system
        mark_step_completed(step_name, {
            "csv_rows": total_rows,
            "inserted_rows": inserted_rows,
            "new_measurements": new_measurements,
            "new_observations": new_observations,
            "processing_time_sec": total_time
        })
        
        # Update ETL progress tracker with completion status
        progress_tracker.complete_step("ETL", step_name, True, 
                                    f"Successfully processed {inserted_rows:,} observations")
        
        return True
        
    except Exception as e:
        error_msg = f"Error processing observations: {e}"
        logging.error(ColoredFormatter.error(f"❌ {error_msg}"))
        
        # Update ETL progress tracker with error
        progress_tracker.complete_step("ETL", step_name, False, error_msg)
        
        return False

def _insert_observation_batch(cur, batch, table_name: str) -> None:
    """
    Helper to do a parameterized INSERT for a batch into the staging table.
    This uses the standard psycopg2 executemany approach.
    """
    insert_sql = f"""
    INSERT INTO {table_name} (id, patient_id, encounter_id, observation_type, code, description, 
                            value_as_string, timestamp)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
    """
    cur.executemany(insert_sql, batch)

def _process_measurement_batch(conn, batch, progress_tracker, step_name, 
                           progress_bar, processed_records, total_records):
    """
    Helper method to process a batch of measurement records
    """
    # Convert batch to parameters for the query
    batch_params = []
    src_ids = []
    for code, timestamp, patient_id, encounter_id, value_as_string, _, obs_type in batch:
        batch_params.append((code, timestamp, patient_id, encounter_id, value_as_string, ''))
        # Create a unique source ID for mapping
        encounter_part = encounter_id if encounter_id else 'NULL'
        src_ids.append(f"{patient_id}_{encounter_part}_{code}_{timestamp}_{value_as_string}")
    
    # Build the placeholders for the IN clause
    placeholders = ", ".join(["(%s, %s, %s, %s, %s, %s)" for _ in range(len(batch_params))])
    
    with conn.cursor() as cur:
        # First, create or update measurement mapping records for each observation
        for src_id in src_ids:
            cur.execute("""
            INSERT INTO staging.measurement_map (source_measurement_id, measurement_id)
            VALUES (%s, nextval('staging.measurement_seq'))
            ON CONFLICT (source_measurement_id) DO NOTHING
            """, (src_id,))
        
        # Then insert this batch using the mapped IDs
        cur.execute(f"""
        INSERT INTO omop.measurement (
            measurement_id,
            person_id,
            measurement_concept_id,
            measurement_date,
            measurement_datetime,
            measurement_time,
            measurement_type_concept_id,
            operator_concept_id,
            value_as_number,
            value_as_concept_id,
            unit_concept_id,
            range_low,
            range_high,
            provider_id,
            visit_occurrence_id,
            visit_detail_id,
            measurement_source_value,
            measurement_source_concept_id,
            unit_source_value,
            value_source_value
        )
        SELECT
            mm.measurement_id,
            pm.person_id,
            0, -- Will be mapped in concept mapping step
            p.date::date,
            p.date::timestamp,
            NULL,
            32817, -- EHR
            0,
            CASE 
                WHEN p.value ~ '^[-]?[0-9]+([.][0-9]+)?([eE][-+]?[0-9]+)?$' THEN p.value::numeric
                ELSE NULL
            END,
            0,
            0, -- Will be mapped in concept mapping step
            NULL,
            NULL,
            NULL,
            vm.visit_occurrence_id,
            NULL,
            p.code,
            0,
            p.units,
            p.value
        FROM (
            VALUES {placeholders}
        ) AS p(code, date, patient, encounter, value, units)
        JOIN staging.person_map pm ON pm.source_patient_id = p.patient
        LEFT JOIN staging.visit_map vm ON vm.source_visit_id = p.encounter
        JOIN staging.measurement_map mm ON mm.source_measurement_id = 
            p.patient || '_' || COALESCE(p.encounter,'NULL') || '_' || p.code || '_' || p.date || '_' || p.value
        """, sum(batch_params, ()))
        
        # Get number of rows inserted in this batch
        rows_inserted = cur.rowcount
        
        # Commit transaction after batch to prevent log buildup
        conn.commit()
        
        # Update progress
        current_processed = processed_records + rows_inserted
        update_progress_bar(progress_bar, rows_inserted)
        progress_tracker.update_progress("ETL", step_name, current_processed, total_items=total_records,
                                      message=f"Inserted {current_processed:,} of {total_records:,} measurements")

def _process_observation_batch(conn, batch, progress_tracker, step_name, 
                            progress_bar, processed_records, total_records):
    """
    Helper method to process a batch of observation records
    """
    if not batch:
        logging.warning("Empty batch passed to _process_observation_batch")
        return 0
    
    rows_inserted = 0    
    try:
        # Convert batch to parameters for the query
        batch_params = []
        src_ids = []
        for code, timestamp, patient_id, encounter_id, value_as_string, _, obs_type in batch:
            batch_params.append((code, timestamp, patient_id, encounter_id, value_as_string, ''))
            # Create a unique source ID for mapping
            encounter_part = encounter_id if encounter_id else 'NULL'
            src_ids.append(f"{patient_id}_{encounter_part}_{code}_{timestamp}_{value_as_string}")
        
        # Build the placeholders for the IN clause
        placeholders = ", ".join(["(%s, %s, %s, %s, %s, %s)" for _ in range(len(batch_params))])
        
        with conn.cursor() as cur:
            # First, create or update observation mapping records for each observation
            for src_id in src_ids:
                cur.execute("""
                INSERT INTO staging.observation_map (source_observation_id, observation_id)
                VALUES (%s, nextval('staging.observation_seq'))
                ON CONFLICT (source_observation_id) DO NOTHING
                """, (src_id,))
            
            # Then insert this batch using the mapped IDs
            cur.execute(f"""
            INSERT INTO omop.observation (
                observation_id,
                person_id,
                observation_concept_id,
                observation_date,
                observation_datetime,
                observation_type_concept_id,
                value_as_number,
                value_as_string,
                value_as_concept_id,
                qualifier_concept_id,
                unit_concept_id,
                provider_id,
                visit_occurrence_id,
                visit_detail_id,
                observation_source_value,
                observation_source_concept_id,
                unit_source_value,
                qualifier_source_value,
                value_source_value,
                observation_event_id,
                obs_event_field_concept_id
            )
            SELECT
                om.observation_id,
                pm.person_id,
                0, -- Will be mapped in concept mapping step
                p.date::date,
                p.date::timestamp,
                32817, -- EHR
                CASE 
                    WHEN p.value ~ '^[-]?[0-9]+([.][0-9]+)?([eE][-+]?[0-9]+)?$' THEN p.value::numeric
                    ELSE NULL
                END,
                CASE 
                    WHEN p.value !~ '^[-]?[0-9]+([.][0-9]+)?([eE][-+]?[0-9]+)?$' THEN p.value
                    ELSE NULL
                END,
                0,
                0,
                0, -- Will be mapped in concept mapping step
                NULL,
                vm.visit_occurrence_id,
                NULL,
                p.code,
                0,
                p.units,
                NULL,
                p.value,
                NULL,
                NULL
            FROM (
                VALUES {placeholders}
            ) AS p(code, date, patient, encounter, value, units)
            JOIN staging.person_map pm ON pm.source_patient_id = p.patient
            LEFT JOIN staging.visit_map vm ON vm.source_visit_id = p.encounter
            JOIN staging.observation_map om ON om.source_observation_id = 
                p.patient || '_' || COALESCE(p.encounter,'NULL') || '_' || p.code || '_' || p.date || '_' || p.value
            """, sum(batch_params, ()))
            
            # Get number of rows inserted in this batch
            rows_inserted = cur.rowcount
            
            # Commit the transaction to prevent log buildup
            conn.commit()
            
            # Update progress
            current_processed = processed_records + rows_inserted
            update_progress_bar(progress_bar, rows_inserted)
            progress_tracker.update_progress("ETL", step_name, current_processed, total_items=total_records,
                                          message=f"Inserted {current_processed:,} of {total_records:,} observations")
    except Exception as e:
        logging.error(f"Error processing observation batch: {e}")
        conn.rollback()
        rows_inserted = 0
    
    return rows_inserted

if __name__ == "__main__":
    # This allows the module to be run directly for testing
    import argparse
    import os
    from dotenv import load_dotenv
    
    # Load environment variables from .env file
    load_dotenv()
    
    parser = argparse.ArgumentParser(description="Process Synthea observations into OMOP CDM")
    parser.add_argument("--observations-csv", required=True, help="Path to observations.csv file")
    parser.add_argument("--force", action="store_true", help="Force reprocessing even if already completed")
    parser.add_argument("--debug", action="store_true", help="Enable debug logging")
    parser.add_argument("--batch-size", type=int, default=1000, help="Batch size for CSV loading")
    parser.add_argument("--measurement-batch-size", type=int, default=50000, 
                        help="Batch size for measurement processing")
    parser.add_argument("--observation-batch-size", type=int, default=50000, 
                        help="Batch size for observation processing")
    parser.add_argument("--truncate-tables", action="store_true", 
                        help="Truncate measurement and observation tables before processing")
    
    args = parser.parse_args()
    
    from etl_pipeline.etl_setup import init_logging, init_db_connection_pool
    
    # Initialize logging
    init_logging(debug=args.debug)
    
    # Initialize database connection pool
    db_host = os.getenv('DB_HOST', 'localhost')
    db_port = os.getenv('DB_PORT', '5432')
    db_name = os.getenv('DB_NAME', 'ohdsi')
    db_user = os.getenv('DB_USER', 'postgres')
    db_password = os.getenv('DB_PASSWORD', 'acumenus')
    
    logging.info(f"Connecting to database {db_host}:{db_port}/{db_name} as {db_user}")
    
    # Create a dictionary with the database configuration
    db_config = {
        'host': db_host,
        'port': db_port,
        'database': db_name,
        'user': db_user,
        'password': db_password
    }
    
    # Initialize the connection pool with the database configuration
    init_db_connection_pool(**db_config)
    
    try:
        success = process_observations(
        args.observations_csv, 
        args.force,
        batch_size=args.batch_size,
        measurement_batch_size=args.measurement_batch_size,
        observation_batch_size=args.observation_batch_size,
        truncate_tables=args.truncate_tables
    )
        sys.exit(0 if success else 1)
    except Exception as e:
        logging.error(f"Unhandled exception in observations ETL: {e}")
        import traceback
        logging.error(traceback.format_exc())
        sys.exit(1)
