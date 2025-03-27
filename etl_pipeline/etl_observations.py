#!/usr/bin/env python3
"""
etl_observations.py - Process observations data from population.observations into OMOP measurement and observation tables.
With batch processing, progress reporting and pre/post row counts.
"""

import os
import logging
import time
import sys
import json
from pathlib import Path
from typing import List, Dict, Any, Optional, Set, Tuple

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
}

def process_observations(force_reprocess: bool = False, batch_size: int = 1000, 
                       measurement_batch_size: int = 50000, observation_batch_size: int = 50000, 
                       truncate_tables: bool = False) -> bool:
    """
    Process observations from population.observations into OMOP measurement and observation tables with detailed progress tracking.
    
    Args:
        force_reprocess: Whether to force reprocessing even if already completed
        batch_size: Size of batches for initial data loading
        measurement_batch_size: Size of batches for measurement processing
        observation_batch_size: Size of batches for observation processing
        truncate_tables: Whether to truncate destination tables before processing
        
    Returns:
        bool: True if processing was successful, False otherwise
    """
    step_name = "process_observations"
    if is_step_completed(step_name, force_reprocess):
        logging.info(ColoredFormatter.info("✅ Observations were previously processed. Skipping."))
        return True
    
    logging.info(ColoredFormatter.info("\n🔍 Processing observations data..."))
    
    # Initialize memory monitoring
    try:
        import psutil
        memory_monitoring = True
        process = psutil.Process(os.getpid())
    except ImportError:
        logging.warning("psutil not installed. Memory monitoring will be disabled.")
        memory_monitoring = False
    
    # Get database connection
    conn = None
    try:
        conn = get_connection()
        conn.autocommit = True
    except Exception as e:
        logging.error(f"Failed to get database connection: {e}")
        return False
    
    # Start progress tracking
    progress_tracker = ETLProgressTracker()
    
    # --- Count total rows in the population.observations table ---
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM population.observations")
            total_rows = cur.fetchone()[0]
            logging.info(f"Found {total_rows:,} observations in population.observations table.")
            
            # Get counts for pre-processing reference
            cur.execute("SELECT COUNT(*) FROM omop.measurement")
            pre_count_measurement = cur.fetchone()[0]
            cur.execute("SELECT COUNT(*) FROM omop.observation")
            pre_count_observation = cur.fetchone()[0]
            logging.info(f"Current measurement rows (before load): {pre_count_measurement:,}")
            logging.info(f"Current observation rows (before load): {pre_count_observation:,}")
    except Exception as e:
        logging.error(f"Error getting row counts: {e}")
        release_connection(conn)
        return False
    
    # Initialize progress tracker with total rows
    progress_tracker.start_step("ETL", step_name, total_items=total_rows, 
                              message=f"Found {total_rows:,} observations to process")
    
    # We will do chunk-based loading with cursor-based pagination for better performance
    inserted_rows = 0
    start_time = time.time()
    
    # Create etl_checkpoints table for tracking progress
    try:
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
        release_connection(conn)
        return False
    
    # Truncate tables if requested
    if truncate_tables:
        logging.info("Truncating destination tables")
        execute_query("TRUNCATE TABLE omop.measurement CASCADE")
        execute_query("TRUNCATE TABLE omop.observation CASCADE")
        execute_query("TRUNCATE TABLE staging.measurement_map CASCADE")
        execute_query("TRUNCATE TABLE staging.observation_map CASCADE")
        execute_query("ALTER SEQUENCE staging.measurement_seq RESTART WITH 1")
        execute_query("ALTER SEQUENCE staging.observation_seq RESTART WITH 1")
    
    # Create staging schema if it doesn't exist
    try:
        with conn.cursor() as cur:
            cur.execute("CREATE SCHEMA IF NOT EXISTS staging;")
    except Exception as e:
        logging.error(f"Error creating staging schema: {e}")
        release_connection(conn)
        return False
    
    # Create temporary tables for processing
    temp_table = "staging.observations_raw"
    
    # Check if observations_raw table already exists with data
    table_exists = False
    rows_loaded = 0
    try:
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
    
    # Skip loading if observations_raw already exists and has data
    if table_exists and rows_loaded > 0:
        logging.info(ColoredFormatter.info(f"✅ Using existing observations_raw table with {rows_loaded:,} rows"))
        inserted_rows = rows_loaded
    else:
        # Need to create and load the table
        try:
            # Create persistent staging table
            with conn.cursor() as cur:
                cur.execute(f"""
                DROP TABLE IF EXISTS {temp_table};
                CREATE TABLE {temp_table} (
                    id SERIAL PRIMARY KEY,
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
            
            # Implement cursor-based pagination for loading data
            # This is more efficient than offset-based pagination for large datasets
            last_date = None
            last_patient = None
            last_encounter = None
            batch_size_adaptive = batch_size  # Start with the provided batch size
            min_batch_size = 100  # Minimum batch size to prevent too small batches
            max_batch_size = 10000  # Maximum batch size to prevent memory issues
            
            # Create progress bar for initial data loading
            progress_bar = create_progress_bar(total_rows, "Loading Observations")
            
            # Track performance metrics for adaptive batch sizing
            batch_times = []
            memory_usages = []
            
            while True:
                batch_start_time = time.time()
                
                # Monitor memory usage if psutil is available
                if memory_monitoring:
                    current_memory = process.memory_info().rss / 1024 / 1024  # MB
                    memory_usages.append(current_memory)
                    logging.debug(f"Current memory usage: {current_memory:.2f} MB")
                
                # Fetch next batch using cursor-based pagination
                with conn.cursor() as cur:
                    if last_date is None:
                        # First batch
                        cur.execute(f"""
                        SELECT "DATE", "PATIENT", "ENCOUNTER", "TYPE", "CODE", "DESCRIPTION", "VALUE", "CATEGORY"
                        FROM population.observations
                        ORDER BY "DATE", "PATIENT", "ENCOUNTER"
                        LIMIT {batch_size_adaptive}
                        """)
                    else:
                        # Subsequent batches - use cursor-based pagination
                        cur.execute(f"""
                        SELECT "DATE", "PATIENT", "ENCOUNTER", "TYPE", "CODE", "DESCRIPTION", "VALUE", "CATEGORY"
                        FROM population.observations
                        WHERE ("DATE", "PATIENT", "ENCOUNTER") > (%s, %s, %s)
                        ORDER BY "DATE", "PATIENT", "ENCOUNTER"
                        LIMIT {batch_size_adaptive}
                        """, (last_date, last_patient, last_encounter))
                    
                    batch = cur.fetchall()
                
                # If no more rows, we're done
                if not batch:
                    break
                
                # Insert batch into staging table
                with conn.cursor() as cur:
                    args_str = ','.join(cur.mogrify("(%s,%s,%s,%s,%s,%s,%s,%s::timestamp)", (i, row[1], row[2], row[3], row[4], row[5], row[6], row[0])).decode('utf-8') for i, row in enumerate(batch, start=1))
                    cur.execute(f"""
                    INSERT INTO {temp_table} (id, patient_id, encounter_id, observation_type, code, description, value_as_string, timestamp)
                    VALUES {args_str}
                    """)
                
                # Update pagination values for next batch
                last_date = batch[-1][0]      # Last row's DATE
                last_patient = batch[-1][1]    # Last row's PATIENT
                last_encounter = batch[-1][2]  # Last row's ENCOUNTER
                
                # Update progress
                inserted_rows += len(batch)
                update_progress_bar(progress_bar, len(batch))
                progress_tracker.update_progress("ETL", step_name, inserted_rows, total_items=total_rows,
                                               message=f"Loaded {inserted_rows:,} of {total_rows:,} observations")
                
                # Update checkpoint
                with conn.cursor() as cur:
                    # Use a numeric ID for the checkpoint since the schema expects a bigint
                    # We'll use the number of processed rows as the ID
                    cur.execute("""
                    INSERT INTO staging.etl_checkpoints 
                    (process_name, last_processed_id, last_offset, total_processed, last_updated)
                    VALUES (%s, %s, %s, %s, CURRENT_TIMESTAMP)
                    ON CONFLICT (process_name) DO UPDATE
                    SET last_processed_id = EXCLUDED.last_processed_id,
                        last_offset = EXCLUDED.last_offset,
                        total_processed = EXCLUDED.total_processed,
                        last_updated = EXCLUDED.last_updated
                    """, (step_name, inserted_rows, inserted_rows, inserted_rows))
                
                # Commit after each batch
                conn.commit()
                
                # Calculate batch processing time for adaptive sizing
                batch_time = time.time() - batch_start_time
                batch_times.append(batch_time)
                
                # Calculate throughput (rows/sec)
                throughput = len(batch) / batch_time if batch_time > 0 else 0
                
                # Log performance metrics every 10 batches
                if len(batch_times) % 10 == 0:
                    avg_time = sum(batch_times[-10:]) / 10
                    avg_throughput = batch_size_adaptive / avg_time if avg_time > 0 else 0
                    logging.info(f"Batch performance: {avg_throughput:.2f} rows/sec, batch size: {batch_size_adaptive}")
                    
                    # Calculate ETA
                    remaining_rows = total_rows - inserted_rows
                    eta_seconds = remaining_rows / avg_throughput if avg_throughput > 0 else 0
                    eta_minutes = eta_seconds / 60
                    logging.info(f"Estimated time remaining: {eta_minutes:.2f} minutes")
                
                # Adaptive batch sizing based on performance and memory usage
                if len(batch_times) >= 3:
                    # If processing is fast, increase batch size
                    if batch_time < 1.0 and (not memory_monitoring or current_memory < 1000):  # Less than 1 second and under 1GB
                        batch_size_adaptive = min(batch_size_adaptive * 1.2, max_batch_size)
                    # If processing is slow or memory usage is high, decrease batch size
                    elif batch_time > 5.0 or (memory_monitoring and current_memory > 2000):  # More than 5 seconds or over 2GB
                        batch_size_adaptive = max(batch_size_adaptive * 0.8, min_batch_size)
                    
                    batch_size_adaptive = int(batch_size_adaptive)  # Ensure it's an integer
            
            # Close progress bar
            close_progress_bar(progress_bar)
            
            # Log final statistics
            total_time = time.time() - start_time
            logging.info(f"Loaded {inserted_rows:,} observations in {total_time:.2f} seconds")
            logging.info(f"Average throughput: {inserted_rows/total_time:.2f} rows/sec")
            
        except Exception as e:
            conn.rollback()
            error_msg = f"Error loading observations: {e}"
            logging.error(ColoredFormatter.error(f"❌ {error_msg}"))
            progress_tracker.complete_step("ETL", step_name, False, error_msg)
            release_connection(conn)
            return False
    
    # Initialize counters for tracking
    processed_count = 0
    measurement_count = 0
    observation_count = 0
    
    try:
        # Create temporary tables for processing measurements and observations
        with conn.cursor() as cur:
            # Create temporary tables for batch processing
            cur.execute("""
            CREATE TEMP TABLE IF NOT EXISTS temp_measurements (
                id TEXT,
                code TEXT,
                measurement_date TIMESTAMP,
                patient_id TEXT,
                encounter_id TEXT,
                value_as_string TEXT,
                units TEXT,
                observation_type TEXT
            ) ON COMMIT PRESERVE ROWS;
            
            CREATE TEMP TABLE IF NOT EXISTS temp_observations (
                id TEXT,
                code TEXT,
                observation_date TIMESTAMP,
                patient_id TEXT,
                encounter_id TEXT,
                value_as_string TEXT,
                units TEXT,
                observation_type TEXT
            ) ON COMMIT PRESERVE ROWS;
            
            -- Set work_mem for better performance with large batches
            SET work_mem = '1GB';
            SET maintenance_work_mem = '2GB';
            SET statement_timeout = '3600000';
            """)
        
        # Process measurements and observations separately using cursor-based pagination
        # First, process measurements (lab tests, vitals, etc.)
        logging.info("Processing measurements from observations data...")
        
        # Initialize variables for adaptive batch sizing
        measurement_batch_size_adaptive = measurement_batch_size
        min_batch_size = 1000
        max_batch_size = 100000
        batch_times = []
        last_id = None
        
        # Create progress bar for measurements
        measurement_progress = create_progress_bar(total_rows, "Processing Measurements")
        
        # Get count of measurements to process (those with LOINC codes in our measurement set)
        with conn.cursor() as cur:
            cur.execute(f"""
            SELECT COUNT(*) FROM {temp_table} 
            WHERE code IN ({','.join(['%s'] * len(MEASUREMENT_LOINC_CODES))})
            """, tuple(MEASUREMENT_LOINC_CODES))
            measurement_total = cur.fetchone()[0]
            logging.info(f"Found {measurement_total:,} measurements to process")
        
        # Process measurements in batches with cursor-based pagination
        while True:
            batch_start_time = time.time()
            
            # Monitor memory usage if psutil is available
            if memory_monitoring:
                current_memory = process.memory_info().rss / 1024 / 1024  # MB
                logging.debug(f"Current memory usage: {current_memory:.2f} MB")
            
            # Fetch next batch of measurements using cursor-based pagination
            with conn.cursor() as cur:
                if last_id is None:
                    # First batch
                    cur.execute(f"""
                    SELECT id, code, timestamp, patient_id, encounter_id, value_as_string, '', observation_type
                    FROM {temp_table}
                    WHERE code IN ({','.join(['%s'] * len(MEASUREMENT_LOINC_CODES))})
                    ORDER BY id
                    LIMIT {measurement_batch_size_adaptive}
                    """, tuple(MEASUREMENT_LOINC_CODES))
                else:
                    # Subsequent batches - use cursor-based pagination
                    cur.execute(f"""
                    SELECT id, code, timestamp, patient_id, encounter_id, value_as_string, '', observation_type
                    FROM {temp_table}
                    WHERE code IN ({','.join(['%s'] * len(MEASUREMENT_LOINC_CODES))})
                    AND id > %s
                    ORDER BY id
                    LIMIT {measurement_batch_size_adaptive}
                    """, tuple(list(MEASUREMENT_LOINC_CODES) + [last_id]))
                
                batch = cur.fetchall()
            
            # If no more rows, we're done with measurements
            if not batch:
                break
            
            # Process this batch of measurements
            batch_size_this_iteration = len(batch)
            
            # Insert batch into temp_measurements table
            with conn.cursor() as cur:
                args_str = ','.join(cur.mogrify("(%s,%s,%s,%s,%s,%s,%s,%s)", row).decode('utf-8') for row in batch)
                cur.execute(f"""
                TRUNCATE TABLE temp_measurements;
                INSERT INTO temp_measurements (id, code, measurement_date, patient_id, encounter_id, value_as_string, units, observation_type)
                VALUES {args_str}
                """)
                
                # Process the measurements and insert into OMOP measurement table
                measurement_count += _process_measurement_batch(cur, measurement_batch_size_adaptive)
            
            # Update pagination values for next batch
            last_date = batch[-1][0]      # Last row's DATE
            last_patient = batch[-1][1]    # Last row's PATIENT
            last_encounter = batch[-1][2]  # Last row's ENCOUNTER
            
            # Update progress
            processed_count += batch_size_this_iteration
            update_progress_bar(measurement_progress, batch_size_this_iteration)
            progress_tracker.update_progress("ETL", step_name, processed_count, total_items=total_rows,
                                          message=f"Processed {processed_count:,} of {total_rows:,} observations ({measurement_count:,} measurements)")
            
            # Commit after each batch
            conn.commit()
            
            # Calculate batch processing time for adaptive sizing
            batch_time = time.time() - batch_start_time
            batch_times.append(batch_time)
            
            # Calculate throughput (rows/sec)
            throughput = batch_size_this_iteration / batch_time if batch_time > 0 else 0
            
            # Log performance metrics every 5 batches
            if len(batch_times) % 5 == 0:
                avg_time = sum(batch_times[-5:]) / 5
                avg_throughput = measurement_batch_size_adaptive / avg_time if avg_time > 0 else 0
                logging.info(f"Measurement batch performance: {avg_throughput:.2f} rows/sec, batch size: {measurement_batch_size_adaptive}")
                
                # Calculate ETA for measurements
                remaining_rows = measurement_total - measurement_count
                eta_seconds = remaining_rows / avg_throughput if avg_throughput > 0 else 0
                eta_minutes = eta_seconds / 60
                logging.info(f"Estimated time remaining for measurements: {eta_minutes:.2f} minutes")
            
            # Adaptive batch sizing based on performance and memory usage
            if len(batch_times) >= 3:
                # If processing is fast, increase batch size
                if batch_time < 2.0 and (not memory_monitoring or current_memory < 1000):  # Less than 2 seconds and under 1GB
                    measurement_batch_size_adaptive = min(int(measurement_batch_size_adaptive * 1.2), max_batch_size)
                # If processing is slow or memory usage is high, decrease batch size
                elif batch_time > 10.0 or (memory_monitoring and current_memory > 2000):  # More than 10 seconds or over 2GB
                    measurement_batch_size_adaptive = max(int(measurement_batch_size_adaptive * 0.8), min_batch_size)
        
        # Close measurement progress bar
        close_progress_bar(measurement_progress)
        
        # Now process observations (non-measurement records)
        logging.info("Processing observations (non-measurements) from observations data...")
        
        # Reset variables for observations processing
        observation_batch_size_adaptive = observation_batch_size
        batch_times = []
        last_id = None
        
        # Create progress bar for observations
        observation_progress = create_progress_bar(total_rows - measurement_total, "Processing Observations")
        
        # Process observations in batches with cursor-based pagination
        while True:
            batch_start_time = time.time()
            
            # Monitor memory usage if psutil is available
            if memory_monitoring:
                current_memory = process.memory_info().rss / 1024 / 1024  # MB
                logging.debug(f"Current memory usage: {current_memory:.2f} MB")
            
            # Fetch next batch of observations using cursor-based pagination
            with conn.cursor() as cur:
                if last_id is None:
                    # First batch
                    cur.execute(f"""
                    SELECT id, code, timestamp, patient_id, encounter_id, value_as_string, '', observation_type
                    FROM {temp_table}
                    WHERE code NOT IN ({','.join(['%s'] * len(MEASUREMENT_LOINC_CODES))})
                    ORDER BY id
                    LIMIT {observation_batch_size_adaptive}
                    """, tuple(MEASUREMENT_LOINC_CODES))
                else:
                    # Subsequent batches - use cursor-based pagination
                    cur.execute(f"""
                    SELECT id, code, timestamp, patient_id, encounter_id, value_as_string, '', observation_type
                    FROM {temp_table}
                    WHERE code NOT IN ({','.join(['%s'] * len(MEASUREMENT_LOINC_CODES))})
                    AND id > %s
                    ORDER BY id
                    LIMIT {observation_batch_size_adaptive}
                    """, tuple(list(MEASUREMENT_LOINC_CODES) + [last_id]))
                
                batch = cur.fetchall()
            
            # If no more rows, we're done with observations
            if not batch:
                break
            
            # Process this batch of observations
            batch_size_this_iteration = len(batch)
            
            # Insert batch into temp_observations table
            with conn.cursor() as cur:
                args_str = ','.join(cur.mogrify("(%s,%s,%s,%s,%s,%s,%s,%s)", row).decode('utf-8') for row in batch)
                cur.execute(f"""
                TRUNCATE TABLE temp_observations;
                INSERT INTO temp_observations (id, code, observation_date, patient_id, encounter_id, value_as_string, units, observation_type)
                VALUES {args_str}
                """)
                
                # Process the observations and insert into OMOP observation table
                observation_count += _process_observation_batch(cur, observation_batch_size_adaptive)
            
            # Update pagination values for next batch
            last_date = batch[-1][0]      # Last row's DATE
            last_patient = batch[-1][1]    # Last row's PATIENT
            last_encounter = batch[-1][2]  # Last row's ENCOUNTER
            
            # Update progress
            processed_count += batch_size_this_iteration
            update_progress_bar(observation_progress, batch_size_this_iteration)
            progress_tracker.update_progress("ETL", step_name, processed_count, total_items=total_rows,
                                          message=f"Processed {processed_count:,} of {total_rows:,} observations ({observation_count:,} observations)")
            
            # Commit after each batch
            conn.commit()
            
            # Calculate batch processing time for adaptive sizing
            batch_time = time.time() - batch_start_time
            batch_times.append(batch_time)
            
            # Calculate throughput (rows/sec)
            throughput = batch_size_this_iteration / batch_time if batch_time > 0 else 0
            
            # Log performance metrics every 5 batches
            if len(batch_times) % 5 == 0:
                avg_time = sum(batch_times[-5:]) / 5
                avg_throughput = observation_batch_size_adaptive / avg_time if avg_time > 0 else 0
                logging.info(f"Observation batch performance: {avg_throughput:.2f} rows/sec, batch size: {observation_batch_size_adaptive}")
                
                # Calculate ETA for observations
                remaining_rows = (total_rows - measurement_total) - observation_count
                eta_seconds = remaining_rows / avg_throughput if avg_throughput > 0 else 0
                eta_minutes = eta_seconds / 60
                logging.info(f"Estimated time remaining for observations: {eta_minutes:.2f} minutes")
            
            # Adaptive batch sizing based on performance and memory usage
            if len(batch_times) >= 3:
                # If processing is fast, increase batch size
                if batch_time < 2.0 and (not memory_monitoring or current_memory < 1000):  # Less than 2 seconds and under 1GB
                    observation_batch_size_adaptive = min(int(observation_batch_size_adaptive * 1.2), max_batch_size)
                # If processing is slow or memory usage is high, decrease batch size
                elif batch_time > 10.0 or (memory_monitoring and current_memory > 2000):  # More than 10 seconds or over 2GB
                    observation_batch_size_adaptive = max(int(observation_batch_size_adaptive * 0.8), min_batch_size)
        
        # Close observation progress bar
        close_progress_bar(observation_progress)
        
        # Log final statistics
        total_time = time.time() - start_time
        logging.info(f"Processed {processed_count:,} observations in {total_time:.2f} seconds")
        logging.info(f"Created {measurement_count:,} measurements and {observation_count:,} observations")
        logging.info(f"Average throughput: {processed_count/total_time:.2f} rows/sec")
        
        # Get final counts from the database for verification
        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM omop.measurement")
            post_count_measurement = cur.fetchone()[0]
            cur.execute("SELECT COUNT(*) FROM omop.observation")
            post_count_observation = cur.fetchone()[0]
            
            # Calculate the difference
            measurement_diff = post_count_measurement - pre_count_measurement
            observation_diff = post_count_observation - pre_count_observation
            
            logging.info(f"Added {measurement_diff:,} new measurements to omop.measurement table")
            logging.info(f"Added {observation_diff:,} new observations to omop.observation table")
    
    except Exception as e:
        conn.rollback()
        error_msg = f"Error processing observations: {e}"
        logging.error(ColoredFormatter.error(f"❌ {error_msg}"))
        progress_tracker.complete_step("ETL", step_name, False, error_msg)
        if conn:
            release_connection(conn)
        return False
    finally:
        if conn:
            release_connection(conn)
    
    # Mark step as completed
    mark_step_completed(step_name)
    progress_tracker.complete_step("ETL", step_name, True, "Observations processed successfully")
    logging.info(ColoredFormatter.info("✅ Observations processed successfully"))
    return True


def _process_measurement_batch(cur, batch_size):
    """
    Process a batch of measurements from the temp_measurements table
    
    Args:
        cur: Database cursor
        batch_size: Size of batch to process
        
    Returns:
        int: Number of measurements processed
    """
    # Process measurements in temp_measurements table
    cur.execute(f"""
    INSERT INTO omop.measurement (
        measurement_id, person_id, measurement_concept_id, measurement_date, 
        measurement_datetime, measurement_type_concept_id, value_as_number, 
        value_source_value, unit_concept_id, visit_occurrence_id
    )
    SELECT 
        nextval('staging.measurement_seq'), 
        p.person_id, 
        COALESCE(c.target_concept_id, 0), 
        tm.measurement_date, 
        tm.measurement_date, 
        32817, -- EHR
        CASE WHEN tm.value_as_string ~ '^[0-9]+(\\.[0-9]+)?$' THEN tm.value_as_string::numeric ELSE NULL END, 
        tm.value_as_string, 
        COALESCE(u.target_concept_id, 0), 
        v.visit_occurrence_id
    FROM temp_measurements tm
    JOIN omop.person p ON tm.patient_id = p.person_source_value
    LEFT JOIN omop.visit_occurrence v ON tm.encounter_id = v.visit_source_value AND p.person_id = v.person_id
    LEFT JOIN staging.concept_map c ON tm.code = c.source_code AND c.source_vocabulary_id = 'LOINC'
    LEFT JOIN staging.concept_map u ON tm.units = u.source_code AND u.source_vocabulary_id = 'UCUM'
    LIMIT {batch_size}
    """)    
    # Get number of rows inserted
    row_count = cur.rowcount
    
    # Insert into measurement_map for tracking
    cur.execute(f"""
    INSERT INTO staging.measurement_map (measurement_id, source_id)
    SELECT m.measurement_id, tm.id
    FROM omop.measurement m
    JOIN temp_measurements tm ON 
        m.person_id = (SELECT person_id FROM omop.person WHERE person_source_value = tm.patient_id) AND
        m.measurement_date = tm.measurement_date AND
        (m.visit_occurrence_id = (SELECT visit_occurrence_id FROM omop.visit_occurrence WHERE visit_source_value = tm.encounter_id) OR 
         (m.visit_occurrence_id IS NULL AND tm.encounter_id IS NULL))
    WHERE NOT EXISTS (SELECT 1 FROM staging.measurement_map WHERE source_id = tm.id)
    LIMIT {batch_size}
    """)
    
    return row_count


def _process_observation_batch(cur, batch_size):
    """
    Process a batch of observations from the temp_observations table
    
    Args:
        cur: Database cursor
        batch_size: Size of batch to process
        
    Returns:
        int: Number of observations processed
    """
    # Process observations in temp_observations table
    cur.execute(f"""
    INSERT INTO omop.observation (
        observation_id, person_id, observation_concept_id, observation_date, 
        observation_datetime, observation_type_concept_id, value_as_string, 
        visit_occurrence_id
    )
    SELECT 
        nextval('staging.observation_seq'), 
        p.person_id, 
        COALESCE(c.target_concept_id, 0), 
        to.observation_date, 
        to.observation_date, 
        32817, -- EHR
        to.value_as_string, 
        v.visit_occurrence_id
    FROM temp_observations to
    JOIN omop.person p ON to.patient_id = p.person_source_value
    LEFT JOIN omop.visit_occurrence v ON to.encounter_id = v.visit_source_value AND p.person_id = v.person_id
    LEFT JOIN staging.concept_map c ON to.code = c.source_code AND c.source_vocabulary_id = 'LOINC'
    LIMIT {batch_size}
    """)
    
    # Get number of rows inserted
    row_count = cur.rowcount
    
    # Insert into observation_map for tracking
    cur.execute(f"""
    INSERT INTO staging.observation_map (observation_id, source_id)
    SELECT o.observation_id, to.id
    FROM omop.observation o
    JOIN temp_observations to ON 
        o.person_id = (SELECT person_id FROM omop.person WHERE person_source_value = to.patient_id) AND
        o.observation_date = to.observation_date AND
        (o.visit_occurrence_id = (SELECT visit_occurrence_id FROM omop.visit_occurrence WHERE visit_source_value = to.encounter_id) OR 
         (o.visit_occurrence_id IS NULL AND to.encounter_id IS NULL))
    WHERE NOT EXISTS (SELECT 1 FROM staging.observation_map WHERE source_id = to.id)
    LIMIT {batch_size}
    """)
    
    return row_count
