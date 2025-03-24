#!/usr/bin/env python3
"""
simple_observations_etl.py - A simplified ETL script to process observations data
with immediate feedback and progress tracking.
"""

import os
import sys
import time
import logging
import psycopg2
from pathlib import Path
from tqdm import tqdm
from datetime import datetime

# Add parent directory to path
sys.path.append(str(Path(__file__).parent.parent))
from etl_pipeline.etl_setup import (
    init_logging
)

# Constants
CHUNK_SIZE = 100000  # Start with smaller chunks for immediate feedback

# Setup logging with colors in terminal
def setup_logging():
    """Initialize logging with colors"""
    init_logging(debug=True)
    
    # Add a console handler that's visible immediately
    console = logging.StreamHandler()
    console.setLevel(logging.INFO)
    
    # Add the handler to the root logger
    logging.getLogger('').addHandler(console)

def get_db_connection():
    """Create a database connection using environment variables"""
    db_host = os.getenv('DB_HOST', 'localhost')
    db_port = os.getenv('DB_PORT', '5432')
    db_name = os.getenv('DB_NAME', 'ohdsi')
    db_user = os.getenv('DB_USER', 'postgres')
    db_password = os.getenv('DB_PASSWORD', 'acumenus')
    
    logging.info(f"Connecting to database {db_host}:{db_port}/{db_name} as {db_user}")
    
    return psycopg2.connect(
        host=db_host,
        port=db_port,
        dbname=db_name,
        user=db_user,
        password=db_password
    )

def setup_basic_structures(conn):
    """Create minimal required sequences"""
    logging.info("Setting up basic database structures...")
    
    with conn.cursor() as cursor:
        # Create sequences if they don't exist
        cursor.execute("""
        CREATE SEQUENCE IF NOT EXISTS staging.observation_seq START 1 INCREMENT 1;
        CREATE SEQUENCE IF NOT EXISTS staging.measurement_seq START 1 INCREMENT 1;
        """)
        conn.commit()
        
        # Get current record counts
        cursor.execute("SELECT COUNT(*) FROM omop.measurement")
        current_measurements = cursor.fetchone()[0]
        
        cursor.execute("SELECT COUNT(*) FROM omop.observation")
        current_observations = cursor.fetchone()[0]
        
        logging.info(f"Current measurements: {current_measurements:,}")
        logging.info(f"Current observations: {current_observations:,}")
        
        return current_measurements, current_observations

def count_records_to_process(conn):
    """Count how many records need to be processed"""
    logging.info("Counting records to process (this might take a moment)...")
    
    with conn.cursor() as cursor:
        # First check if we can get a quick count using table statistics
        cursor.execute("""
        SELECT reltuples::bigint AS estimate
        FROM pg_class
        WHERE relname = 'observations_raw';
        """)
        est_count = cursor.fetchone()[0]
        logging.info(f"Estimated total records: ~{est_count:,} (from table statistics)")
        
        # Count measurements (numerical) to process
        cursor.execute("""
        SELECT COUNT(*) 
        FROM staging.observations_raw 
        WHERE observation_type = 'numeric' OR value_as_string ~ '^[-]?[0-9]+([.][0-9]+)?([eE][-+]?[0-9]+)?$'
        LIMIT 1000000;  -- Limit the count to not lock up the database
        """)
        # This will either return the actual count (if under 1M) or 1M
        measurement_count = cursor.fetchone()[0]
        if measurement_count == 1000000:
            logging.info("More than 1M measurements to process, using estimate")
            measurement_count = int(est_count * 0.6)  # Estimate 60% are measurements
        
        # Count observations (non-numerical) to process
        cursor.execute("""
        SELECT COUNT(*) 
        FROM staging.observations_raw 
        WHERE NOT (observation_type = 'numeric' OR value_as_string ~ '^[-]?[0-9]+([.][0-9]+)?([eE][-+]?[0-9]+)?$')
        LIMIT 1000000;  -- Limit the count to not lock up the database
        """)
        observation_count = cursor.fetchone()[0]
        if observation_count == 1000000:
            logging.info("More than 1M observations to process, using estimate")
            observation_count = int(est_count * 0.4)  # Estimate 40% are observations
        
        logging.info(f"Measurements to process: ~{measurement_count:,}")
        logging.info(f"Observations to process: ~{observation_count:,}")
        
        return measurement_count, observation_count

def process_measurements_in_chunks(conn, total_to_process, current_count):
    """Process all measurements in chunks with a progress bar"""
    logging.info("Starting measurement processing...")
    
    total_processed = 0
    offset = 0
    
    # Create a progress bar
    with tqdm(total=total_to_process, 
              initial=0,
              desc="Processing Measurements", 
              unit=" records",
              bar_format="{desc}: {percentage:3.0f}%|{bar}| {n_fmt}/{total_fmt} [{elapsed}<{remaining}, {rate_fmt}{postfix}]") as pbar:
        
        # Keep processing chunks until we've processed everything
        while total_processed < total_to_process:
            with conn.cursor() as cursor:
                # Get the time for throughput calculation
                start_time = time.time()
                
                # Process the next chunk
                cursor.execute("""
                WITH measurement_data AS (
                    SELECT 
                        o.id, 
                        o.code, 
                        o.timestamp, 
                        o.patient_id, 
                        o.encounter_id, 
                        o.value_as_string
                    FROM 
                        staging.observations_raw o
                    JOIN 
                        staging.person_map pm ON pm.source_patient_id = o.patient_id
                    LEFT JOIN 
                        staging.visit_map vm ON vm.source_visit_id = o.encounter_id
                    WHERE
                        (o.observation_type = 'numeric' OR o.value_as_string ~ '^[-]?[0-9]+([.][0-9]+)?([eE][-+]?[0-9]+)?$')
                    ORDER BY o.id
                    LIMIT %s OFFSET %s
                ),
                inserted AS (
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
                        NEXTVAL('staging.measurement_seq') AS measurement_id,
                        pm.person_id,
                        0 AS measurement_concept_id,
                        CAST(o.timestamp AS DATE) AS measurement_date,
                        o.timestamp AS measurement_datetime,
                        NULL AS measurement_time,
                        32817 AS measurement_type_concept_id, -- "EHR"
                        0 AS operator_concept_id,
                        CASE 
                            WHEN o.value_as_string ~ '^[-]?[0-9]+([.][0-9]+)?([eE][-+]?[0-9]+)?$' THEN o.value_as_string::numeric
                            ELSE NULL
                        END AS value_as_number,
                        0 AS value_as_concept_id,
                        0 AS unit_concept_id,
                        NULL AS range_low,
                        NULL AS range_high,
                        NULL AS provider_id,
                        vm.visit_occurrence_id,
                        NULL AS visit_detail_id,
                        o.code AS measurement_source_value,
                        0 AS measurement_source_concept_id,
                        NULL AS unit_source_value,
                        o.value_as_string AS value_source_value
                    FROM 
                        measurement_data o
                    JOIN 
                        staging.person_map pm ON pm.source_patient_id = o.patient_id
                    LEFT JOIN 
                        staging.visit_map vm ON vm.source_visit_id = o.encounter_id
                    WHERE NOT EXISTS (
                        SELECT 1 FROM omop.measurement m
                        JOIN staging.person_map pm2 ON pm2.person_id = m.person_id
                        LEFT JOIN staging.visit_map vm2 ON vm2.visit_occurrence_id = m.visit_occurrence_id
                        WHERE pm2.source_patient_id = o.patient_id
                        AND (vm2.source_visit_id = o.encounter_id OR (o.encounter_id IS NULL AND m.visit_occurrence_id IS NULL))
                        AND m.measurement_source_value = o.code
                        AND m.measurement_date = o.timestamp::date
                        AND m.value_source_value = o.value_as_string
                    )
                    RETURNING 1
                )
                SELECT COUNT(*) FROM inserted
                """, (CHUNK_SIZE, offset))
                
                rows_inserted = cursor.fetchone()[0]
                
                # Commit this chunk
                conn.commit()
                
                # Update counters and progress bar
                total_processed += rows_inserted
                offset += CHUNK_SIZE
                pbar.update(rows_inserted)
                
                # Calculate and show throughput
                duration = time.time() - start_time
                if duration > 0:
                    throughput = rows_inserted / duration
                    pbar.set_postfix({
                        "rate": f"{throughput:.2f} rec/s", 
                        "offset": offset
                    })
                
                # Log progress every 10 chunks
                if offset % (CHUNK_SIZE * 10) == 0:
                    logging.info(f"Processed {total_processed:,} measurements so far (offset: {offset:,})")
                
                # If we didn't get any rows in this chunk, we're done
                if rows_inserted == 0:
                    logging.info("No more measurements to process")
                    break
                
                # Adjust chunk size for better throughput based on performance
                if throughput > 10000:
                    CHUNK_SIZE = min(CHUNK_SIZE * 2, 1000000)
                    logging.info(f"Increased chunk size to {CHUNK_SIZE:,}")
    
    # Return the total actually processed
    return total_processed

def process_observations_in_chunks(conn, total_to_process, current_count):
    """Process all observations in chunks with a progress bar"""
    logging.info("Starting observation processing...")
    
    total_processed = 0
    offset = 0
    
    # Create a progress bar
    with tqdm(total=total_to_process, 
              initial=0,
              desc="Processing Observations", 
              unit=" records",
              bar_format="{desc}: {percentage:3.0f}%|{bar}| {n_fmt}/{total_fmt} [{elapsed}<{remaining}, {rate_fmt}{postfix}]") as pbar:
        
        # Keep processing chunks until we've processed everything
        while total_processed < total_to_process:
            with conn.cursor() as cursor:
                # Get the time for throughput calculation
                start_time = time.time()
                
                # Process the next chunk
                cursor.execute("""
                WITH observation_data AS (
                    SELECT 
                        o.id, 
                        o.code, 
                        o.timestamp, 
                        o.patient_id, 
                        o.encounter_id, 
                        o.value_as_string
                    FROM 
                        staging.observations_raw o
                    JOIN 
                        staging.person_map pm ON pm.source_patient_id = o.patient_id
                    LEFT JOIN 
                        staging.visit_map vm ON vm.source_visit_id = o.encounter_id
                    WHERE
                        NOT (o.observation_type = 'numeric' OR o.value_as_string ~ '^[-]?[0-9]+([.][0-9]+)?([eE][-+]?[0-9]+)?$')
                    ORDER BY o.id
                    LIMIT %s OFFSET %s
                ),
                inserted AS (
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
                        NEXTVAL('staging.observation_seq') AS observation_id,
                        pm.person_id,
                        0 AS observation_concept_id,
                        CAST(o.timestamp AS DATE) AS observation_date,
                        o.timestamp AS observation_datetime,
                        32817 AS observation_type_concept_id, -- "EHR"
                        NULL AS value_as_number,
                        o.value_as_string AS value_as_string,
                        0 AS value_as_concept_id,
                        0 AS qualifier_concept_id,
                        0 AS unit_concept_id,
                        NULL AS provider_id,
                        vm.visit_occurrence_id,
                        NULL AS visit_detail_id,
                        o.code AS observation_source_value,
                        0 AS observation_source_concept_id,
                        NULL AS unit_source_value,
                        NULL AS qualifier_source_value,
                        o.value_as_string AS value_source_value,
                        NULL AS observation_event_id,
                        NULL AS obs_event_field_concept_id
                    FROM 
                        observation_data o
                    JOIN 
                        staging.person_map pm ON pm.source_patient_id = o.patient_id
                    LEFT JOIN 
                        staging.visit_map vm ON vm.source_visit_id = o.encounter_id
                    WHERE NOT EXISTS (
                        SELECT 1 FROM omop.observation obs
                        JOIN staging.person_map pm2 ON pm2.person_id = obs.person_id
                        LEFT JOIN staging.visit_map vm2 ON vm2.visit_occurrence_id = obs.visit_occurrence_id
                        WHERE pm2.source_patient_id = o.patient_id
                        AND (vm2.source_visit_id = o.encounter_id OR (o.encounter_id IS NULL AND obs.visit_occurrence_id IS NULL))
                        AND obs.observation_source_value = o.code
                        AND obs.observation_date = o.timestamp::date
                        AND obs.value_source_value = o.value_as_string
                    )
                    RETURNING 1
                )
                SELECT COUNT(*) FROM inserted
                """, (CHUNK_SIZE, offset))
                
                rows_inserted = cursor.fetchone()[0]
                
                # Commit this chunk
                conn.commit()
                
                # Update counters and progress bar
                total_processed += rows_inserted
                offset += CHUNK_SIZE
                pbar.update(rows_inserted)
                
                # Calculate and show throughput
                duration = time.time() - start_time
                if duration > 0:
                    throughput = rows_inserted / duration
                    pbar.set_postfix({
                        "rate": f"{throughput:.2f} rec/s", 
                        "offset": offset
                    })
                
                # Log progress every 10 chunks
                if offset % (CHUNK_SIZE * 10) == 0:
                    logging.info(f"Processed {total_processed:,} observations so far (offset: {offset:,})")
                
                # If we didn't get any rows in this chunk, we're done
                if rows_inserted == 0:
                    logging.info("No more observations to process")
                    break
                
                # Adjust chunk size for better throughput based on performance
                if throughput > 10000:
                    CHUNK_SIZE = min(CHUNK_SIZE * 2, 1000000)
                    logging.info(f"Increased chunk size to {CHUNK_SIZE:,}")
    
    # Return the total actually processed
    return total_processed

def log_final_statistics(conn, measurements_processed, observations_processed):
    """Log final statistics about the ETL process"""
    logging.info("Generating final ETL statistics...")
    
    with conn.cursor() as cursor:
        # Get current record counts
        cursor.execute("SELECT COUNT(*) FROM omop.measurement")
        current_measurements = cursor.fetchone()[0]
        
        cursor.execute("SELECT COUNT(*) FROM omop.observation")
        current_observations = cursor.fetchone()[0]
        
        # Log statistics
        logging.info("=== ETL STATISTICS ===")
        logging.info(f"Measurements processed: {measurements_processed:,}")
        logging.info(f"Observations processed: {observations_processed:,}")
        logging.info(f"Total records processed: {measurements_processed + observations_processed:,}")
        
        logging.info("=== FINAL TABLE COUNTS ===")
        logging.info(f"Total measurements: {current_measurements:,}")
        logging.info(f"Total observations: {current_observations:,}")

def main():
    """Main entry point for the ETL process"""
    # Initialize logging
    setup_logging()
    
    # Start the ETL process
    logging.info("Starting simplified observations ETL process...")
    start_time = time.time()
    
    conn = None
    try:
        # Connect to the database
        conn = get_db_connection()
        
        # Setup basic database structures
        current_measurements, current_observations = setup_basic_structures(conn)
        
        # Count records to process (or estimate)
        measurements_to_process, observations_to_process = count_records_to_process(conn)
        
        # Process measurements
        measurements_processed = process_measurements_in_chunks(conn, measurements_to_process, current_measurements)
        
        # Process observations
        observations_processed = process_observations_in_chunks(conn, observations_to_process, current_observations)
        
        # Log final statistics
        log_final_statistics(conn, measurements_processed, observations_processed)
        
        # Log total time
        total_time = time.time() - start_time
        logging.info(f"ETL process completed in {total_time:.2f} seconds")
        logging.info(f"Overall throughput: {(measurements_processed + observations_processed) / total_time:.2f} records/second")
        
        logging.info("✅ ETL process completed successfully!")
        
    except Exception as e:
        logging.error(f"❌ Error during ETL process: {e}")
        import traceback
        logging.error(traceback.format_exc())
        sys.exit(1)
        
    finally:
        if conn:
            conn.close()

if __name__ == "__main__":
    main()
