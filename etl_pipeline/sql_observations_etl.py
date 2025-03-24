#!/usr/bin/env python3
"""
sql_observations_etl.py - A SQL-driven ETL script to process observations data
with detailed progress tracking.

This script uses direct PostgreSQL batch processing to efficiently transform
data from staging.observations_raw into omop.measurement and omop.observation tables.
"""

import os
import sys
import time
import logging
import psycopg2
from pathlib import Path
from tqdm import tqdm
from datetime import datetime, timedelta

# Add parent directory to path
sys.path.append(str(Path(__file__).parent.parent))
from etl_pipeline.etl_setup import (
    init_logging,
    ColoredFormatter
)

# Constants for batch size
DEFAULT_BATCH_SIZE = 500000

# SQL queries for tracking and setup
CREATE_TRACKER_TABLE = """
CREATE TABLE IF NOT EXISTS staging.observation_batch_tracker (
    batch_id SERIAL PRIMARY KEY,
    batch_type VARCHAR(20) NOT NULL,  -- 'measurement' or 'observation'
    min_id BIGINT NOT NULL,
    max_id BIGINT NOT NULL,
    processed BOOLEAN DEFAULT FALSE,
    started_at TIMESTAMP,
    completed_at TIMESTAMP,
    record_count BIGINT DEFAULT 0
);
"""

CREATE_CHECKPOINT_TABLE = """
CREATE TABLE IF NOT EXISTS staging.etl_checkpoints (
    process_name TEXT PRIMARY KEY,
    last_processed_id BIGINT,
    last_offset BIGINT,
    total_processed BIGINT,
    last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
"""

CREATE_SEQUENCES = """
-- Create sequences for observation_id and measurement_id if they don't exist
CREATE SEQUENCE IF NOT EXISTS staging.observation_seq START 1 INCREMENT 1;
CREATE SEQUENCE IF NOT EXISTS staging.measurement_seq START 1 INCREMENT 1;
"""

# Query to calculate batches needed
CALCULATE_MEASUREMENT_BATCHES = """
INSERT INTO staging.observation_batch_tracker (batch_type, min_id, max_id)
SELECT 
    'measurement',
    min_id + (batch_num - 1) * batch_size,
    LEAST(min_id + batch_num * batch_size - 1, max_id)
FROM (
    SELECT 
        MIN(id) AS min_id,
        MAX(id) AS max_id,
        CEIL((MAX(id) - MIN(id) + 1)::FLOAT / %s) AS num_batches,
        %s AS batch_size
    FROM staging.observations_raw
    WHERE observation_type = 'numeric' OR value_as_string ~ '^[-]?[0-9]+([.][0-9]+)?([eE][-+]?[0-9]+)?$'
) AS batch_info
CROSS JOIN generate_series(1, batch_info.num_batches::INTEGER) AS batch_num
WHERE NOT EXISTS (
    SELECT 1 FROM staging.observation_batch_tracker 
    WHERE batch_type = 'measurement'
)
RETURNING batch_id, min_id, max_id;
"""

CALCULATE_OBSERVATION_BATCHES = """
INSERT INTO staging.observation_batch_tracker (batch_type, min_id, max_id)
SELECT 
    'observation',
    min_id + (batch_num - 1) * batch_size,
    LEAST(min_id + batch_num * batch_size - 1, max_id)
FROM (
    SELECT 
        MIN(id) AS min_id,
        MAX(id) AS max_id,
        CEIL((MAX(id) - MIN(id) + 1)::FLOAT / %s) AS num_batches,
        %s AS batch_size
    FROM staging.observations_raw
    WHERE NOT (observation_type = 'numeric' OR value_as_string ~ '^[-]?[0-9]+([.][0-9]+)?([eE][-+]?[0-9]+)?$')
) AS batch_info
CROSS JOIN generate_series(1, batch_info.num_batches::INTEGER) AS batch_num
WHERE NOT EXISTS (
    SELECT 1 FROM staging.observation_batch_tracker 
    WHERE batch_type = 'observation'
)
RETURNING batch_id, min_id, max_id;
"""

# Queries to count records
COUNT_MEASUREMENT_RECORDS = """
SELECT COUNT(*)
FROM staging.observations_raw 
WHERE observation_type = 'numeric' OR value_as_string ~ '^[-]?[0-9]+([.][0-9]+)?([eE][-+]?[0-9]+)?$'
"""

COUNT_OBSERVATION_RECORDS = """
SELECT COUNT(*)
FROM staging.observations_raw 
WHERE NOT (observation_type = 'numeric' OR value_as_string ~ '^[-]?[0-9]+([.][0-9]+)?([eE][-+]?[0-9]+)?$')
"""

# Get the pending batches (not yet processed)
GET_PENDING_MEASUREMENT_BATCHES = """
SELECT batch_id, min_id, max_id
FROM staging.observation_batch_tracker
WHERE batch_type = 'measurement' AND NOT processed
ORDER BY batch_id
"""

GET_PENDING_OBSERVATION_BATCHES = """
SELECT batch_id, min_id, max_id
FROM staging.observation_batch_tracker
WHERE batch_type = 'observation' AND NOT processed
ORDER BY batch_id
"""

# Process a batch of measurements
PROCESS_MEASUREMENT_BATCH = """
WITH inserted AS (
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
        staging.observations_raw o
    JOIN 
        staging.person_map pm ON pm.source_patient_id = o.patient_id
    LEFT JOIN 
        staging.visit_map vm ON vm.source_visit_id = o.encounter_id
    WHERE
        (o.observation_type = 'numeric' OR o.value_as_string ~ '^[-]?[0-9]+([.][0-9]+)?([eE][-+]?[0-9]+)?$')
        AND o.id BETWEEN %s AND %s
        AND NOT EXISTS (
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
"""

PROCESS_OBSERVATION_BATCH = """
WITH inserted AS (
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
        staging.observations_raw o
    JOIN 
        staging.person_map pm ON pm.source_patient_id = o.patient_id
    LEFT JOIN 
        staging.visit_map vm ON vm.source_visit_id = o.encounter_id
    WHERE
        NOT (o.observation_type = 'numeric' OR o.value_as_string ~ '^[-]?[0-9]+([.][0-9]+)?([eE][-+]?[0-9]+)?$')
        AND o.id BETWEEN %s AND %s
        AND NOT EXISTS (
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
"""

# Mark batch as processed
MARK_BATCH_PROCESSED = """
UPDATE staging.observation_batch_tracker
SET processed = TRUE, completed_at = %s, record_count = %s
WHERE batch_id = %s
"""

MARK_BATCH_STARTED = """
UPDATE staging.observation_batch_tracker
SET started_at = %s
WHERE batch_id = %s
"""

# Get batch statistics
GET_BATCH_STATISTICS = """
SELECT 
    batch_type,
    COUNT(*) AS total_batches,
    COUNT(*) FILTER (WHERE processed) AS completed_batches,
    COUNT(*) FILTER (WHERE NOT processed) AS pending_batches,
    SUM(record_count) AS total_records_inserted,
    CASE WHEN COUNT(*) FILTER (WHERE processed) > 0 THEN
        EXTRACT(EPOCH FROM SUM(completed_at - started_at) FILTER (WHERE processed)) / 
        COUNT(*) FILTER (WHERE processed)
    ELSE NULL END AS avg_seconds_per_batch,
    CASE WHEN SUM(record_count) FILTER (WHERE processed) > 0 AND 
              SUM(EXTRACT(EPOCH FROM (completed_at - started_at))) FILTER (WHERE processed) > 0 THEN
        SUM(record_count) FILTER (WHERE processed) / 
        SUM(EXTRACT(EPOCH FROM (completed_at - started_at))) FILTER (WHERE processed)
    ELSE NULL END AS avg_records_per_second
FROM 
    staging.observation_batch_tracker
GROUP BY 
    batch_type
"""

# Count current records in measurement and observation tables
COUNT_CURRENT_MEASUREMENTS = "SELECT COUNT(*) FROM omop.measurement"
COUNT_CURRENT_OBSERVATIONS = "SELECT COUNT(*) FROM omop.observation"

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

def setup_database(conn, batch_size):
    """Initialize database structures needed for ETL"""
    logging.info("Setting up database structures...")
    
    with conn.cursor() as cursor:
        # Create necessary tables and sequences
        cursor.execute(CREATE_TRACKER_TABLE)
        cursor.execute(CREATE_CHECKPOINT_TABLE)
        cursor.execute(CREATE_SEQUENCES)
        
        # Get current record counts
        cursor.execute(COUNT_CURRENT_MEASUREMENTS)
        current_measurements = cursor.fetchone()[0]
        
        cursor.execute(COUNT_CURRENT_OBSERVATIONS)
        current_observations = cursor.fetchone()[0]
        
        logging.info(f"Current measurement count: {current_measurements:,}")
        logging.info(f"Current observation count: {current_observations:,}")
        
        # Create batches for measurements if needed
        cursor.execute(CALCULATE_MEASUREMENT_BATCHES, (batch_size, batch_size))
        measurement_batches = cursor.fetchall()
        
        # Create batches for observations if needed
        cursor.execute(CALCULATE_OBSERVATION_BATCHES, (batch_size, batch_size))
        observation_batches = cursor.fetchall()
        
        # Count total records to process
        cursor.execute(COUNT_MEASUREMENT_RECORDS)
        measurement_records = cursor.fetchone()[0]
        
        cursor.execute(COUNT_OBSERVATION_RECORDS)
        observation_records = cursor.fetchone()[0]
        
        # Commit changes
        conn.commit()
        
        # Log batch information
        if measurement_batches:
            logging.info(f"Created {len(measurement_batches)} measurement batches")
            logging.info(f"Measurement batch range: {measurement_batches[0][1]} to {measurement_batches[-1][2]}")
        
        if observation_batches:
            logging.info(f"Created {len(observation_batches)} observation batches")
            logging.info(f"Observation batch range: {observation_batches[0][1]} to {observation_batches[-1][2]}")
        
        return measurement_records, observation_records

def process_measurement_batches(conn, total_measurements):
    """Process all pending measurement batches"""
    logging.info("Starting measurement batch processing...")
    
    with conn.cursor() as cursor:
        # Get all pending measurement batches
        cursor.execute(GET_PENDING_MEASUREMENT_BATCHES)
        pending_batches = cursor.fetchall()
        
        if not pending_batches:
            logging.info("No pending measurement batches to process")
            return 0
        
        logging.info(f"Processing {len(pending_batches)} measurement batches")
        
        # Initialize tracking variables
        total_processed = 0
        start_time = time.time()
        
        # Create progress bar
        with tqdm(total=total_measurements, 
                  desc="Processing Measurements", 
                  unit="record",
                  bar_format="{desc}: {percentage:3.0f}%|{bar}| {n_fmt}/{total_fmt} [{elapsed}<{remaining}, {rate_fmt}{postfix}]") as pbar:
            
            # Process each batch
            for batch_id, min_id, max_id in pending_batches:
                batch_start = time.time()
                
                # Mark batch as started
                cursor.execute(MARK_BATCH_STARTED, (datetime.now(), batch_id))
                
                # Process this batch
                cursor.execute(PROCESS_MEASUREMENT_BATCH, (min_id, max_id))
                rows_inserted = cursor.fetchone()[0]
                
                # Mark batch as processed
                batch_end = datetime.now()
                cursor.execute(MARK_BATCH_PROCESSED, (batch_end, rows_inserted, batch_id))
                
                # Update counters
                total_processed += rows_inserted
                
                # Commit transaction
                conn.commit()
                
                # Update progress bar
                pbar.update(rows_inserted)
                
                # Calculate batch stats
                batch_duration = time.time() - batch_start
                if batch_duration > 0:
                    batch_rate = rows_inserted / batch_duration
                    pbar.set_postfix({"batch": batch_id, "rate": f"{batch_rate:.2f} rec/s"})
                
                # Log progress
                logging.info(f"Processed measurement batch {batch_id} ({min_id}-{max_id}): {rows_inserted:,} records in {batch_duration:.2f}s")
        
        # Log final statistics
        total_duration = time.time() - start_time
        overall_rate = total_processed / total_duration if total_duration > 0 else 0
        logging.info(f"Measurement processing completed: {total_processed:,} records in {total_duration:.2f}s ({overall_rate:.2f} records/sec)")
        
        return total_processed

def process_observation_batches(conn, total_observations):
    """Process all pending observation batches"""
    logging.info("Starting observation batch processing...")
    
    with conn.cursor() as cursor:
        # Get all pending observation batches
        cursor.execute(GET_PENDING_OBSERVATION_BATCHES)
        pending_batches = cursor.fetchall()
        
        if not pending_batches:
            logging.info("No pending observation batches to process")
            return 0
        
        logging.info(f"Processing {len(pending_batches)} observation batches")
        
        # Initialize tracking variables
        total_processed = 0
        start_time = time.time()
        
        # Create progress bar
        with tqdm(total=total_observations, 
                  desc="Processing Observations", 
                  unit="record",
                  bar_format="{desc}: {percentage:3.0f}%|{bar}| {n_fmt}/{total_fmt} [{elapsed}<{remaining}, {rate_fmt}{postfix}]") as pbar:
            
            # Process each batch
            for batch_id, min_id, max_id in pending_batches:
                batch_start = time.time()
                
                # Mark batch as started
                cursor.execute(MARK_BATCH_STARTED, (datetime.now(), batch_id))
                
                # Process this batch
                cursor.execute(PROCESS_OBSERVATION_BATCH, (min_id, max_id))
                rows_inserted = cursor.fetchone()[0]
                
                # Mark batch as processed
                batch_end = datetime.now()
                cursor.execute(MARK_BATCH_PROCESSED, (batch_end, rows_inserted, batch_id))
                
                # Update counters
                total_processed += rows_inserted
                
                # Commit transaction
                conn.commit()
                
                # Update progress bar
                pbar.update(rows_inserted)
                
                # Calculate batch stats
                batch_duration = time.time() - batch_start
                if batch_duration > 0:
                    batch_rate = rows_inserted / batch_duration
                    pbar.set_postfix({"batch": batch_id, "rate": f"{batch_rate:.2f} rec/s"})
                
                # Log progress
                logging.info(f"Processed observation batch {batch_id} ({min_id}-{max_id}): {rows_inserted:,} records in {batch_duration:.2f}s")
        
        # Log final statistics
        total_duration = time.time() - start_time
        overall_rate = total_processed / total_duration if total_duration > 0 else 0
        logging.info(f"Observation processing completed: {total_processed:,} records in {total_duration:.2f}s ({overall_rate:.2f} records/sec)")
        
        return total_processed

def log_final_statistics(conn):
    """Log final ETL statistics"""
    logging.info("Generating final ETL statistics...")
    
    with conn.cursor() as cursor:
        # Get batch statistics
        cursor.execute(GET_BATCH_STATISTICS)
        stats = cursor.fetchall()
        
        # Get current record counts
        cursor.execute(COUNT_CURRENT_MEASUREMENTS)
        current_measurements = cursor.fetchone()[0]
        
        cursor.execute(COUNT_CURRENT_OBSERVATIONS)
        current_observations = cursor.fetchone()[0]
        
        # Display statistics
        for row in stats:
            (batch_type, total_batches, completed_batches, pending_batches, 
             total_records, avg_seconds, avg_rate) = row
            
            logging.info(f"=== {batch_type.upper()} STATISTICS ===")
            logging.info(f"Total batches: {total_batches}")
            logging.info(f"Completed batches: {completed_batches}")
            logging.info(f"Pending batches: {pending_batches}")
            logging.info(f"Records inserted: {total_records:,}")
            
            if avg_seconds:
                logging.info(f"Average batch duration: {timedelta(seconds=avg_seconds)}")
            
            if avg_rate:
                logging.info(f"Average processing rate: {avg_rate:.2f} records/second")
        
        logging.info("=== FINAL TABLE COUNTS ===")
        logging.info(f"Total measurements: {current_measurements:,}")
        logging.info(f"Total observations: {current_observations:,}")

def main():
    """Main entry point"""
    init_logging(debug=True)
    
    # Get batch size from environment or use default
    batch_size = int(os.getenv('BATCH_SIZE', DEFAULT_BATCH_SIZE))
    logging.info(f"Using batch size: {batch_size:,}")
    
    conn = None
    try:
        # Get database connection
        conn = get_db_connection()
        
        # Setup database and get record counts
        measurement_records, observation_records = setup_database(conn, batch_size)
        
        # First process measurement batches
        process_measurement_batches(conn, measurement_records)
        
        # Then process observation batches
        process_observation_batches(conn, observation_records)
        
        # Log final statistics
        log_final_statistics(conn)
        
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
