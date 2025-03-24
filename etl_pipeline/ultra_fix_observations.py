#!/usr/bin/env python3
"""
ultra_fix_observations.py - An optimized script to process remaining observations 
with significantly improved throughput.
"""

import os
import logging
import time
import sys
from pathlib import Path
import psycopg2
from psycopg2.extras import execute_values

# Add parent directory to path to import from etl_setup
sys.path.append(str(Path(__file__).parent.parent))
from etl_pipeline.etl_setup import (
    execute_query,
    get_connection,
    release_connection,
    create_progress_bar,
    update_progress_bar,
    close_progress_bar,
    ColoredFormatter,
    ETLProgressTracker,
    init_logging,
    init_db_connection_pool
)

def process_measurement_batch_optimized(conn, batch, progress_tracker, step_name, 
                           progress_bar, processed_records, total_records):
    """
    Optimized method to process a large batch of measurement records
    using bulk operations for maximum performance
    """
    if not batch:
        return 0
    
    try:
        # First, bulk insert into map table and use direct insert with actual sequence expression
        with conn.cursor() as cur:
            # Extract the source IDs for mapping
            source_ids = []
            for row in batch:
                if len(row) < 7:  # Ensure we have at least 7 fields
                    continue
                    
                code, timestamp, patient_id, encounter_id, value_as_string = row[0], row[1], row[2], row[3], row[4]
                
                # Create a unique source ID for mapping
                encounter_part = encounter_id if encounter_id else 'NULL'
                src_id = f"{patient_id}_{encounter_part}_{code}_{timestamp}_{value_as_string}"
                source_ids.append(src_id)
            
            # Bulk insert directly using direct SQL rather than execute_values
            # This handles the sequence in the SQL rather than as a parameter
            placeholders = ",".join(["(%s, nextval('staging.measurement_seq'))" for _ in source_ids])
            
            if placeholders:  # Only execute if we have placeholders
                cur.execute(f"""
                INSERT INTO staging.measurement_map (source_measurement_id, measurement_id)
                VALUES {placeholders}
                ON CONFLICT (source_measurement_id) DO NOTHING
                """, source_ids)
                
            # Use a more optimized direct SQL approach for the main insert
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
                o.timestamp::date,
                o.timestamp::timestamp,
                NULL,
                32817, -- EHR
                0,
                CASE 
                    WHEN o.value_as_string ~ '^[-]?[0-9]+([.][0-9]+)?([eE][-+]?[0-9]+)?$' THEN o.value_as_string::numeric
                    ELSE NULL
                END,
                0,
                0, -- Will be mapped in concept mapping step
                NULL,
                NULL,
                NULL,
                vm.visit_occurrence_id,
                NULL,
                o.code,
                0,
                NULL,
                o.value_as_string
            FROM (
                SELECT 
                    patient_id || '_' || 
                    COALESCE(encounter_id,'NULL') || '_' || 
                    code || '_' || 
                    timestamp || '_' || 
                    value_as_string AS source_id,
                    * 
                FROM staging.observations_raw 
                WHERE id IN (
                    SELECT unnest(%s::text[])
                )
            ) o
            JOIN staging.person_map pm ON pm.source_patient_id = o.patient_id
            LEFT JOIN staging.visit_map vm ON vm.source_visit_id = o.encounter_id
            JOIN staging.measurement_map mm ON mm.source_measurement_id = o.source_id
            WHERE NOT EXISTS (
                SELECT 1 
                FROM omop.measurement m
                WHERE m.measurement_source_value = o.code
                AND m.measurement_date = o.timestamp::date
                AND m.value_source_value = o.value_as_string
                AND m.person_id = pm.person_id
                AND (
                    m.visit_occurrence_id = vm.visit_occurrence_id OR 
                    (o.encounter_id IS NULL AND m.visit_occurrence_id IS NULL)
                )
            )
            """, ([row[0] for row in batch],))  # Pass the list of IDs
            
            # Get number of rows inserted in this batch
            rows_inserted = cur.rowcount
            
            # Update progress
            current_processed = processed_records + rows_inserted
            update_progress_bar(progress_bar, rows_inserted)
            if progress_tracker:
                progress_tracker.update_progress("ETL", step_name, current_processed, total_items=total_records,
                                              message=f"Inserted {current_processed:,} of {total_records:,} measurements")
            
            return rows_inserted
    except Exception as e:
        logging.error(f"Error in process_measurement_batch_optimized: {e}")
        import traceback
        logging.error(traceback.format_exc())
        conn.rollback()
        return 0

def process_observation_batch_optimized(conn, batch, progress_tracker, step_name, 
                            progress_bar, processed_records, total_records):
    """
    Optimized method to process a large batch of observation records
    using bulk operations for maximum performance
    """
    if not batch:
        return 0
    
    try:
        with conn.cursor() as cur:
            # Extract the source IDs for mapping
            source_ids = []
            for row in batch:
                if len(row) < 7:  # Ensure we have at least 7 fields
                    continue
                    
                code, timestamp, patient_id, encounter_id, value_as_string = row[0], row[1], row[2], row[3], row[4]
                
                # Create a unique source ID for mapping
                encounter_part = encounter_id if encounter_id else 'NULL'
                src_id = f"{patient_id}_{encounter_part}_{code}_{timestamp}_{value_as_string}"
                source_ids.append(src_id)
            
            # Bulk insert directly using direct SQL rather than execute_values
            # This handles the sequence in the SQL rather than as a parameter
            placeholders = ",".join(["(%s, nextval('staging.observation_seq'))" for _ in source_ids])
            
            if placeholders:  # Only execute if we have placeholders
                cur.execute(f"""
                INSERT INTO staging.observation_map (source_observation_id, observation_id)
                VALUES {placeholders}
                ON CONFLICT (source_observation_id) DO NOTHING
                """, source_ids)
            
            # Use a more optimized direct SQL approach for the main insert
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
                o.timestamp::date,
                o.timestamp::timestamp,
                32817, -- EHR
                CASE 
                    WHEN o.value_as_string ~ '^[-]?[0-9]+([.][0-9]+)?([eE][-+]?[0-9]+)?$' THEN o.value_as_string::numeric
                    ELSE NULL
                END,
                CASE 
                    WHEN o.value_as_string !~ '^[-]?[0-9]+([.][0-9]+)?([eE][-+]?[0-9]+)?$' THEN o.value_as_string
                    ELSE NULL
                END,
                0,
                0,
                0, -- Will be mapped in concept mapping step
                NULL,
                vm.visit_occurrence_id,
                NULL,
                o.code,
                0,
                NULL,
                NULL,
                o.value_as_string,
                NULL,
                NULL
            FROM (
                SELECT 
                    patient_id || '_' || 
                    COALESCE(encounter_id,'NULL') || '_' || 
                    code || '_' || 
                    timestamp || '_' || 
                    value_as_string AS source_id,
                    * 
                FROM staging.observations_raw 
                WHERE id IN (
                    SELECT unnest(%s::text[])
                )
            ) o
            JOIN staging.person_map pm ON pm.source_patient_id = o.patient_id
            LEFT JOIN staging.visit_map vm ON vm.source_visit_id = o.encounter_id
            JOIN staging.observation_map om ON om.source_observation_id = o.source_id
            WHERE NOT EXISTS (
                SELECT 1 
                FROM omop.observation obs
                WHERE obs.observation_source_value = o.code
                AND obs.observation_date = o.timestamp::date
                AND obs.value_source_value = o.value_as_string
                AND obs.person_id = pm.person_id
                AND (
                    obs.visit_occurrence_id = vm.visit_occurrence_id OR 
                    (o.encounter_id IS NULL AND obs.visit_occurrence_id IS NULL)
                )
            )
            """, ([row[0] for row in batch],))  # Pass the list of IDs
            
            # Get number of rows inserted in this batch
            rows_inserted = cur.rowcount
            
            # Update progress
            current_processed = processed_records + rows_inserted
            update_progress_bar(progress_bar, rows_inserted)
            if progress_tracker:
                progress_tracker.update_progress("ETL", step_name, current_processed, total_items=total_records,
                                              message=f"Inserted {current_processed:,} of {total_records:,} observations")
            
            return rows_inserted
    except Exception as e:
        logging.error(f"Error in process_observation_batch_optimized: {e}")
        import traceback
        logging.error(traceback.format_exc())
        conn.rollback()
        return 0

def resume_measurement_processing():
    """
    Resume the measurement processing from the last checkpoint
    """
    # Create etl_checkpoints table if it doesn't exist
    execute_query("""
    CREATE TABLE IF NOT EXISTS staging.etl_checkpoints (
        process_name TEXT PRIMARY KEY,
        last_processed_id BIGINT,
        last_offset BIGINT,
        total_processed BIGINT,
        last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
    """)
    
    temp_table = "staging.observations_raw"
    conn = get_connection()
    conn.autocommit = False
    
    try:
        # Check if we have a checkpoint for measurement processing
        with conn.cursor() as cur:
            cur.execute("""
            SELECT last_processed_id, total_processed, last_offset
            FROM staging.etl_checkpoints 
            WHERE process_name = 'measurement_processing'
            """)
            result = cur.fetchone()
            
            if result:
                last_id, already_processed, offset = result
                logging.info(f"Resuming measurement processing from offset {offset}, already processed {already_processed:,} records")
            else:
                last_id = 0
                already_processed = 0
                offset = 0
                # Initialize checkpoint
                cur.execute("""
                INSERT INTO staging.etl_checkpoints 
                (process_name, last_processed_id, total_processed, last_offset)
                VALUES ('measurement_processing', 0, 0, 0)
                """)
                conn.commit()

        # Add index for ID if not exists
        try:
            with conn.cursor() as cur:
                cur.execute(f"CREATE INDEX IF NOT EXISTS idx_{temp_table.replace('.', '_')}_id ON {temp_table}(id);")
                conn.commit()
                logging.info("Added ID index for better query performance")
        except Exception as e:
            logging.warning(f"Could not create ID index: {e}")
            conn.rollback()

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
            return True
        
        # Create progress bar for this operation
        progress_bar = create_progress_bar(remaining_to_insert, "Inserting Measurements")
        
        # Setup batch processing
        chunk_size = 50000  # Somewhat smaller batch size for stability
        processed_this_run = 0
        
        # Process in chunks using OFFSET-based pagination
        while True:
            # Get next chunk of records
            with conn.cursor() as cur:
                cur.execute(f"""
                SELECT o.id, o.code, o.timestamp, o.patient_id, o.encounter_id, o.value_as_string, o.observation_type
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
                ORDER BY o.id
                LIMIT {chunk_size} OFFSET {offset}
                """)
                batch = cur.fetchall()
            
            # Exit loop if no more records to process
            if not batch:
                logging.info("No more measurement records to process")
                break
            
            # Process this batch
            if batch:
                # Extract the ID for tracking purposes only
                last_id_str = batch[-1][0]
                # Handle empty string IDs safely
                last_id = 0 if not last_id_str else int(last_id_str) if last_id_str.isdigit() else 0
                
                # Process the batch (using optimized version)
                start = time.time()
                rows_processed = process_measurement_batch_optimized(conn, batch, None, "process_observations", 
                                        progress_bar, already_processed + processed_this_run, records_to_insert)
                end = time.time()
                
                # Update counters
                processed_this_run += rows_processed
                offset += len(batch)
                
                # Update checkpoint
                with conn.cursor() as cur:
                    cur.execute("""
                    UPDATE staging.etl_checkpoints 
                    SET last_processed_id = %s, total_processed = %s, last_offset = %s, last_updated = CURRENT_TIMESTAMP
                    WHERE process_name = 'measurement_processing'
                    """, (last_id, already_processed + processed_this_run, offset))
                
                # Commit after each batch
                conn.commit()
                
                # Calculate and log throughput
                duration = end - start
                if duration > 0:
                    throughput = rows_processed / duration
                    logging.info(f"Processed {rows_processed:,} records in {duration:.2f}s ({throughput:.2f} records/sec)")
                else:
                    logging.info(f"Processed {rows_processed:,} records instantly")
                
                # Update progress bar
                update_progress_bar(progress_bar, rows_processed)
                
                logging.info(f"Processed {processed_this_run:,} measurement records so far (offset: {offset})")
        
        # Close progress bar
        close_progress_bar(progress_bar)
        
        logging.info(f"Successfully processed {processed_this_run:,} measurement records in this run")
        return True
        
    except Exception as e:
        error_msg = f"Error processing measurements: {e}"
        logging.error(f"❌ {error_msg}")
        import traceback
        logging.error(traceback.format_exc())
        return False
    finally:
        release_connection(conn)

def resume_observation_processing():
    """
    Resume the observation processing from the last checkpoint
    """
    # Create etl_checkpoints table if it doesn't exist
    execute_query("""
    CREATE TABLE IF NOT EXISTS staging.etl_checkpoints (
        process_name TEXT PRIMARY KEY,
        last_processed_id BIGINT,
        last_offset BIGINT,
        total_processed BIGINT,
        last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
    """)
    
    temp_table = "staging.observations_raw"
    conn = get_connection()
    conn.autocommit = False
    
    try:
        # Check if we have a checkpoint for observation processing
        with conn.cursor() as cur:
            cur.execute("""
            SELECT last_processed_id, total_processed, last_offset 
            FROM staging.etl_checkpoints 
            WHERE process_name = 'observation_processing'
            """)
            result = cur.fetchone()
            
            if result:
                last_id, already_processed, offset = result
                logging.info(f"Resuming observation processing from offset {offset}, already processed {already_processed:,} records")
            else:
                last_id = 0
                already_processed = 0
                offset = 0
                # Initialize checkpoint
                cur.execute("""
                INSERT INTO staging.etl_checkpoints 
                (process_name, last_processed_id, total_processed, last_offset)
                VALUES ('observation_processing', 0, 0, 0)
                """)
                conn.commit()

        # Get count of observations to process
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
        remaining_to_insert = records_to_insert - already_processed
        logging.info(f"Found {existing_observations:,} observations already in database, need to insert {remaining_to_insert:,}")
        
        if remaining_to_insert <= 0:
            logging.info("No new observations to insert")
            return True
        
        # Create progress bar for this operation
        progress_bar = create_progress_bar(remaining_to_insert, "Inserting Observations")
        
        # Setup batch processing
        chunk_size = 50000  # Somewhat smaller batch size for stability
        processed_this_run = 0
        
        # Process in chunks using OFFSET-based pagination
        while True:
            # Get next chunk of records
            with conn.cursor() as cur:
                cur.execute(f"""
                SELECT o.id, o.code, o.timestamp, o.patient_id, o.encounter_id, o.value_as_string, o.observation_type
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
                ORDER BY o.id
                LIMIT {chunk_size} OFFSET {offset}
                """)
                batch = cur.fetchall()
            
            # Exit loop if no more records to process
            if not batch:
                logging.info("No more observation records to process")
                break
            
            # Process this batch
            if batch:
                # Extract the ID for tracking purposes only
                last_id_str = batch[-1][0]
                # Handle empty string IDs safely
                last_id = 0 if not last_id_str else int(last_id_str) if last_id_str.isdigit() else 0
                
                # Process the batch (using optimized version)
                start = time.time()
                rows_processed = process_observation_batch_optimized(conn, batch, None, "process_observations", 
                                        progress_bar, already_processed + processed_this_run, records_to_insert)
                end = time.time()
                
                # Update counters
                processed_this_run += rows_processed
                offset += len(batch)
                
                # Update checkpoint
                with conn.cursor() as cur:
                    cur.execute("""
                    UPDATE staging.etl_checkpoints 
                    SET last_processed_id = %s, total_processed = %s, last_offset = %s, last_updated = CURRENT_TIMESTAMP
                    WHERE process_name = 'observation_processing'
                    """, (last_id, already_processed + processed_this_run, offset))
                
                # Commit after each batch
                conn.commit()
                
                # Calculate and log throughput
                duration = end - start
                if duration > 0:
                    throughput = rows_processed / duration
                    logging.info(f"Processed {rows_processed:,} records in {duration:.2f}s ({throughput:.2f} records/sec)")
                else:
                    logging.info(f"Processed {rows_processed:,} records instantly")
                
                # Update progress bar
                update_progress_bar(progress_bar, rows_processed)
                
                logging.info(f"Processed {processed_this_run:,} observation records so far (offset: {offset})")
        
        # Close progress bar
        close_progress_bar(progress_bar)
        
        logging.info(f"Successfully processed {processed_this_run:,} observation records in this run")
        return True
        
    except Exception as e:
        error_msg = f"Error processing observations: {e}"
        logging.error(f"❌ {error_msg}")
        import traceback
        logging.error(traceback.format_exc())
        return False
    finally:
        release_connection(conn)

if __name__ == "__main__":
    # Initialize logging
    init_logging(debug=True)
    
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
        logging.info("Starting measurement processing...")
        measurement_success = resume_measurement_processing()
        
        if measurement_success:
            logging.info("Measurements processed successfully, starting observation processing...")
            observation_success = resume_observation_processing()
            
            if observation_success:
                logging.info("✅ Successfully processed all observations!")
                sys.exit(0)
            else:
                logging.error("❌ Failed to process observations")
                sys.exit(1)
        else:
            logging.error("❌ Failed to process measurements")
            sys.exit(1)
    except Exception as e:
        logging.error(f"❌ Unhandled exception: {e}")
        import traceback
        logging.error(traceback.format_exc())
        sys.exit(1)
