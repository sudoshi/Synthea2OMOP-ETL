#!/usr/bin/env python3
"""
load_population_observations.py - Load data from population.observation to omop.observation
with progress tracking and efficient batch processing.
"""

import os
import sys
import time
import logging
import psycopg2
from pathlib import Path
from tqdm import tqdm

# Add parent directory to path
sys.path.append(str(Path(__file__).parent.parent))
from etl_pipeline.etl_setup import (
    init_logging
)

# Constants
CHUNK_SIZE = 10000  # Start with a smaller chunk size due to the large dataset

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

def ensure_sequence_exists(conn):
    """Ensure the observation sequence exists"""
    logging.info("Ensuring observation sequence exists...")
    
    with conn.cursor() as cursor:
        cursor.execute("""
        CREATE SEQUENCE IF NOT EXISTS staging.observation_seq START 1 INCREMENT 1;
        """)
        conn.commit()

def estimate_records_to_process(conn):
    """Estimate how many records need to be processed from population.observations
    using a more efficient approach for very large datasets"""
    logging.info("Estimating records to process from population.observations...")
    
    with conn.cursor() as cursor:
        # Get an estimate of total records using table statistics
        # This is much faster than a full COUNT(*)
        cursor.execute("""
        SELECT reltuples::bigint AS estimate
        FROM pg_class
        WHERE relname = 'observations' AND relnamespace = (SELECT oid FROM pg_namespace WHERE nspname = 'population');
        """)
        result = cursor.fetchone()
        estimated_total = result[0] if result and result[0] else 0
        
        # Check if we already have records in omop.observation
        cursor.execute("""
        SELECT COUNT(*) FROM omop.observation LIMIT 1;
        """)
        has_existing = cursor.fetchone()[0] > 0
        
        # If we have a very large dataset, use a sampling approach to estimate completion
        if estimated_total > 10000000:  # 10 million records
            logging.info(f"Large dataset detected (~{estimated_total:,} records). Using sampling for estimation.")
            
            # Sample a small number of records to check if they exist in omop.observation
            cursor.execute("""
            WITH sample AS (
                SELECT po."PATIENT", po."ENCOUNTER", po."CODE", po."DATE"::date, po."VALUE"
                FROM population.observations po
                TABLESAMPLE SYSTEM(0.01)  -- Sample 0.01% of records
                LIMIT 1000
            )
            SELECT COUNT(*) FROM sample s
            JOIN staging.person_map pm ON pm.source_patient_id = s."PATIENT"
            LEFT JOIN staging.visit_map vm ON vm.source_visit_id = s."ENCOUNTER"
            WHERE EXISTS (
                SELECT 1 FROM omop.observation obs
                JOIN staging.person_map pm2 ON pm2.person_id = obs.person_id
                LEFT JOIN staging.visit_map vm2 ON vm2.visit_occurrence_id = obs.visit_occurrence_id
                WHERE pm2.source_patient_id = s."PATIENT"
                AND (vm2.source_visit_id = s."ENCOUNTER" OR (s."ENCOUNTER" IS NULL AND obs.visit_occurrence_id IS NULL))
                AND obs.observation_source_value = s."CODE"
                AND obs.observation_date = s."DATE"
                AND COALESCE(obs.value_source_value, '') = COALESCE(s."VALUE", '')
            );
            """)
            sample_existing = cursor.fetchone()[0]
            sample_size = 1000
            estimated_completion_pct = (sample_existing / sample_size) if sample_size > 0 else 0
            estimated_existing = int(estimated_total * estimated_completion_pct)
            
            logging.info(f"Estimated total records in population.observations: ~{estimated_total:,}")
            logging.info(f"Estimated records already in omop.observation: ~{estimated_existing:,} ({estimated_completion_pct:.2%})")
            logging.info(f"Estimated records to process: ~{estimated_total - estimated_existing:,}")
            
            return estimated_total - estimated_existing
        else:
            # For smaller datasets, we can do a full count
            cursor.execute("""
            SELECT COUNT(*) FROM population.observations;
            """)
            total_records = cursor.fetchone()[0]
            
            if has_existing:
                # Count records already in omop.observation that match population.observations
                cursor.execute("""
                SELECT COUNT(*) 
                FROM omop.observation;
                """)
                existing_records = cursor.fetchone()[0]
            else:
                existing_records = 0
            
            to_process = total_records - existing_records
            
            logging.info(f"Total records in population.observations: {total_records:,}")
            logging.info(f"Records already in omop.observation: {existing_records:,}")
            logging.info(f"Records to process: {to_process:,}")
            
            return to_process

def process_observations_in_chunks(conn, estimated_total):
    """Process all observations in chunks with a progress bar
    Uses an efficient approach for very large datasets with cursor-based pagination
    and batch processing to prevent memory issues"""
    if estimated_total <= 0:
        logging.info("No observations to process")
        return 0
    
    logging.info("Starting observation processing...")
    logging.info(f"Using initial chunk size of {CHUNK_SIZE:,}")
    
    # Initialize counters and tracking variables
    total_processed = 0
    chunk_size = CHUNK_SIZE
    max_retries = 3
    last_id_processed = None
    overall_start_time = time.time()
    batch_start_time = time.time()
    
    # For very large datasets, we'll use a cursor-based approach instead of offset
    # This is much more efficient for PostgreSQL
    
    # Create a progress bar with an estimated total
    with tqdm(total=estimated_total, 
              initial=0,
              desc="Processing Observations", 
              unit=" records",
              bar_format="{desc}: {n_fmt}/{total_fmt} [{elapsed}, {rate_fmt}{postfix}]") as pbar:
        
        # Keep processing chunks until we've processed everything
        # For large datasets, we'll continue until we get an empty batch
        while True:
            with conn.cursor() as cursor:
                # Get the time for throughput calculation
                start_time = time.time()
                
                # Process the next chunk using a cursor-based approach for better performance
                # with large datasets
                # For large datasets, we need a different approach to handle the temporary tables
                # First, create a temporary table for this batch of observations
                create_temp_table_sql = """
                CREATE TEMP TABLE temp_observation_batch AS
                SELECT 
                    po."PATIENT" as patient_id, 
                    po."ENCOUNTER" as encounter_id,
                    0 as observation_concept_id,
                    po."CODE" as observation_source_value,
                    po."DATE"::date as observation_date,
                    po."DATE"::timestamp as observation_datetime,
                    CASE 
                        WHEN po."TYPE" = 'numeric' OR po."VALUE" ~ '^[-]?[0-9]+([.][0-9]+)?([eE][-+]?[0-9]+)?$' 
                        THEN po."VALUE"::numeric 
                        ELSE NULL 
                    END as value_as_number,
                    po."VALUE" as value_as_string,
                    0 as value_as_concept_id,
                    0 as qualifier_concept_id,
                    0 as unit_concept_id,
                    NULL as provider_id,
                    32817 as observation_type_concept_id,
                    0 as observation_source_concept_id,
                    po."UNITS" as unit_source_value,
                    NULL as qualifier_source_value,
                    po."VALUE" as value_source_value,
                    (po."PATIENT" || '-' || po."DATE" || '-' || po."CODE") as record_id
                FROM population.observations po
                """
                
                # Add filtering based on last processed ID
                if last_id_processed is None:
                    # First chunk - no cursor condition
                    create_temp_table_sql += f"ORDER BY po.\"PATIENT\", po.\"DATE\", po.\"CODE\" LIMIT {chunk_size}"
                else:
                    # Subsequent chunks - use cursor-based pagination
                    create_temp_table_sql += f"WHERE (po.\"PATIENT\" || '-' || po.\"DATE\" || '-' || po.\"CODE\") > '{last_id_processed}' "
                    create_temp_table_sql += f"ORDER BY po.\"PATIENT\", po.\"DATE\", po.\"CODE\" LIMIT {chunk_size}"
                
                # Execute the temp table creation
                cursor.execute(create_temp_table_sql)
                
                # Create a table to store the filtered records that will be inserted
                cursor.execute("""
                CREATE TEMP TABLE temp_observation_filtered AS
                SELECT 
                    t.*,
                    t.record_id
                FROM temp_observation_batch t
                JOIN staging.person_map pm ON pm.source_patient_id = t.patient_id
                LEFT JOIN staging.visit_map vm ON vm.source_visit_id = t.encounter_id
                WHERE NOT EXISTS (
                    SELECT 1 FROM omop.observation obs
                    JOIN staging.person_map pm2 ON pm2.person_id = obs.person_id
                    LEFT JOIN staging.visit_map vm2 ON vm2.visit_occurrence_id = obs.visit_occurrence_id
                    WHERE pm2.source_patient_id = t.patient_id
                    AND (vm2.source_visit_id = t.encounter_id OR (t.encounter_id IS NULL AND obs.visit_occurrence_id IS NULL))
                    AND obs.observation_source_value = t.observation_source_value
                    AND obs.observation_date = t.observation_date
                    AND COALESCE(obs.value_source_value, '') = COALESCE(t.value_source_value, '')
                )
                """)
                
                # Get the last record ID for pagination
                cursor.execute("SELECT MAX(record_id) FROM temp_observation_filtered")
                result = cursor.fetchone()
                if result and result[0]:
                    last_id_processed = result[0]
                
                # Insert the filtered records into the final table
                insert_sql = """
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
                    COALESCE(t.observation_concept_id, 0) AS observation_concept_id,
                    t.observation_date,
                    t.observation_datetime,
                    COALESCE(t.observation_type_concept_id, 32817) AS observation_type_concept_id,
                    t.value_as_number,
                    t.value_as_string,
                    COALESCE(t.value_as_concept_id, 0) AS value_as_concept_id,
                    COALESCE(t.qualifier_concept_id, 0) AS qualifier_concept_id,
                    COALESCE(t.unit_concept_id, 0) AS unit_concept_id,
                    NULL AS provider_id,
                    vm.visit_occurrence_id,
                    NULL AS visit_detail_id,
                    t.observation_source_value,
                    COALESCE(t.observation_source_concept_id, 0) AS observation_source_concept_id,
                    t.unit_source_value,
                    t.qualifier_source_value,
                    t.value_source_value,
                    NULL AS observation_event_id,
                    NULL AS obs_event_field_concept_id
                FROM temp_observation_filtered t
                JOIN staging.person_map pm ON pm.source_patient_id = t.patient_id
                LEFT JOIN staging.visit_map vm ON vm.source_visit_id = t.encounter_id
                """
                
                # Execute the insert and get the count of inserted rows
                cursor.execute(insert_sql)
                
                # Get count of inserted rows
                cursor.execute("SELECT COUNT(*) FROM temp_observation_filtered")
                rows_inserted = cursor.fetchone()[0]
                
                # Log progress for this batch
                if rows_inserted > 0:
                    batch_elapsed = time.time() - batch_start_time
                    overall_elapsed = time.time() - overall_start_time
                    batch_rate = rows_inserted / batch_elapsed if batch_elapsed > 0 else 0
                    overall_rate = total_processed / overall_elapsed if overall_elapsed > 0 else 0
                    
                    logging.info(f"Inserted {rows_inserted} observations in this batch (rate: {batch_rate:.2f} rec/s)")
                    logging.info(f"Last processed ID: {last_id_processed}")
                    logging.info(f"Total processed: {total_processed:,} of ~{estimated_total:,} ({total_processed/estimated_total*100:.2f}%)")
                    logging.info(f"Overall rate: {overall_rate:.2f} rec/s, ETA: {(estimated_total-total_processed)/overall_rate/60:.1f} minutes")
                    
                    # Adaptive chunk size based on performance
                    # For large datasets, we need to be careful with memory usage
                    if batch_rate > 5000 and chunk_size < 100000:
                        chunk_size = min(int(chunk_size * 1.5), 100000)
                        logging.info(f"Increasing chunk size to {chunk_size:,}")
                    elif batch_rate < 500 and chunk_size > 1000:
                        chunk_size = max(int(chunk_size / 1.5), 1000)
                        logging.info(f"Decreasing chunk size to {chunk_size:,}")
                
                # Clean up temporary tables
                cursor.execute("DROP TABLE IF EXISTS temp_observation_filtered")
                cursor.execute("DROP TABLE IF EXISTS temp_observation_batch")
                
                # Commit this chunk to prevent transaction log buildup
                conn.commit()
                
                # Reset the batch start time for the next chunk
                batch_start_time = time.time()
                
                # Update counters and progress bar
                total_processed += rows_inserted
                pbar.update(rows_inserted)
                
                # Calculate and show throughput for progress bar
                overall_elapsed = time.time() - overall_start_time
                overall_rate = total_processed / overall_elapsed if overall_elapsed > 0 else 0
                eta_minutes = (estimated_total - total_processed) / overall_rate / 60 if overall_rate > 0 else 0
                
                pbar.set_postfix({
                    "rate": f"{overall_rate:.2f} rec/s", 
                    "processed": total_processed,
                    "eta": f"{eta_minutes:.1f}m"
                })
                
                # If we didn't get any rows in this chunk, we're done
                if rows_inserted == 0:
                    logging.info("No more observations to process")
                    break
                
                # Periodically log memory usage to monitor for potential issues
                if total_processed % (chunk_size * 20) < chunk_size:
                    try:
                        import psutil
                        process = psutil.Process()
                        memory_info = process.memory_info()
                        logging.info(f"Memory usage: {memory_info.rss / (1024 * 1024):.1f} MB")
                    except ImportError:
                        # psutil not available, skip memory logging
                        pass
    
    # Return the total actually processed
    return total_processed

def log_final_statistics(conn, observations_processed):
    """Log final statistics about the ETL process"""
    with conn.cursor() as cursor:
        cursor.execute("SELECT COUNT(*) FROM omop.observation")
        total_observations = cursor.fetchone()[0]
        
        logging.info("=" * 80)
        logging.info("ETL PROCESS COMPLETE")
        logging.info("=" * 80)
        logging.info(f"Observations processed in this run: {observations_processed:,}")
        logging.info(f"Total observations in OMOP CDM: {total_observations:,}")
        logging.info("=" * 80)

def main():
    """Main entry point for the ETL process"""
    # Setup logging
    setup_logging()
    
    logging.info("=" * 80)
    logging.info("STARTING POPULATION.OBSERVATIONS TO OMOP.OBSERVATION ETL")
    logging.info("=" * 80)
    
    start_time = time.time()
    conn = None
    
    try:
        # Get database connection - use a single connection for the entire process
        # to ensure temporary tables persist and transactions are properly managed
        conn = get_db_connection()
        
        # Set a reasonable statement timeout to prevent long-running queries
        with conn.cursor() as cursor:
            cursor.execute("SET statement_timeout = '3600000'")
            # 1 hour timeout
            
            # Optimize work_mem for better performance with large datasets
            cursor.execute("SET work_mem = '256MB'")
            
            # Use a larger maintenance_work_mem for better index creation performance
            cursor.execute("SET maintenance_work_mem = '1GB'")
        
        # Ensure sequences exist
        ensure_sequence_exists(conn)
        
        # Estimate how many records we need to process
        estimated_total = estimate_records_to_process(conn)
        
        # Process observations in chunks with a single connection
        observations_processed = process_observations_in_chunks(conn, estimated_total)
        
        # Log final statistics
        log_final_statistics(conn, observations_processed)
        
        # Calculate and log overall performance
        total_time = time.time() - start_time
        if total_time > 0 and observations_processed > 0:
            rate = observations_processed / total_time
            logging.info(f"Overall processing rate: {rate:.2f} records/second")
            logging.info(f"Total processing time: {total_time:.2f} seconds ({total_time/60:.2f} minutes)")
        
        return True
        
    except Exception as e:
        logging.error(f"Error in ETL process: {e}")
        # Rollback any pending transaction
        if conn is not None:
            try:
                conn.rollback()
                logging.info("Transaction rolled back due to error")
            except Exception as rollback_error:
                logging.error(f"Error rolling back transaction: {rollback_error}")
        return False
        
    finally:
        # Always close the connection in the finally block to ensure it happens
        if conn is not None:
            try:
                conn.close()
                logging.info("Database connection closed")
            except Exception as close_error:
                logging.error(f"Error closing database connection: {close_error}")

if __name__ == "__main__":
    main()
