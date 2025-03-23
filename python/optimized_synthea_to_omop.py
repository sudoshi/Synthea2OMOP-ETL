#!/usr/bin/env python3
"""
optimized_synthea_to_omop.py - Direct and optimized import of Synthea data to OMOP CDM

This script implements:
1. Direct import from Synthea CSV files to OMOP tables
2. Parallel processing of independent ETL steps
3. Optimized SQL operations
4. Combined measurement/observation handling
"""

import argparse
import concurrent.futures
import csv
import logging
import os
import sys
import time
import psycopg2
import psycopg2.extras
from psycopg2 import pool

# Try to import ETL progress tracker
try:
    from etl_progress_tracking import ETLProgressTracker
    progress_tracker_available = True
except ImportError:
    progress_tracker_available = False
from typing import Dict, List, Any, Tuple, Optional
from utils.config_loader import ConfigLoader

# Set up logging
log_dir = "logs"
os.makedirs(log_dir, exist_ok=True)
log_file = os.path.join(log_dir, f"optimized_etl_{time.strftime('%Y%m%d_%H%M%S')}.log")
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler(log_file),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

# Global variables
connection_pool = None
config = None
progress_tracker = None

def parse_arguments():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(description='Optimized Synthea to OMOP ETL process')
    parser.add_argument('--synthea-dir', type=str, default='./synthea-output',
                        help='Directory containing Synthea output files (default: ./synthea-output)')
    parser.add_argument('--max-workers', type=int, default=4,
                        help='Maximum number of parallel workers (default: 4)')
    parser.add_argument('--skip-optimization', action='store_true',
                        help='Skip PostgreSQL optimization')
    parser.add_argument('--skip-validation', action='store_true',
                        help='Skip validation steps')
    parser.add_argument('--debug', action='store_true',
                        help='Enable debug logging')
    parser.add_argument('--track-progress', action='store_true',
                        help='Enable progress tracking')
    return parser.parse_args()

def setup_logging(debug=False):
    """Set up logging with appropriate level."""
    if debug:
        logging.getLogger().setLevel(logging.DEBUG)
        logger.debug("Debug logging enabled")
    else:
        logging.getLogger().setLevel(logging.INFO)

def initialize_database_connection():
    """Initialize the database connection pool."""
    global connection_pool, config, progress_tracker
    
    try:
        # Load configuration
        config_loader = ConfigLoader()
        config = {
            'host': config_loader.get_env('DB_HOST', 'localhost'),
            'port': config_loader.get_env('DB_PORT', '5432'),
            'database': config_loader.get_env('DB_NAME', 'ohdsi'),
            'user': config_loader.get_env('DB_USER', 'postgres'),
            'password': config_loader.get_env('DB_PASSWORD', 'acumenus')
        }
        
        # Create connection pool
        connection_pool = pool.ThreadedConnectionPool(
            minconn=1,
            maxconn=20,  # Adjust based on max_workers and expected DB load
            **config
        )
        
        # Initialize progress tracker if available
        if progress_tracker_available and args.track_progress:
            progress_tracker = ETLProgressTracker({
                'host': config['host'],
                'port': config['port'],
                'dbname': config['database'],
                'user': config['user'],
                'password': config['password']
            })
            logger.info("ETL progress tracking initialized")
        
        logger.info(f"Database connection pool initialized: {config['host']}:{config['port']}/{config['database']}")
        return True
    except Exception as e:
        logger.error(f"Failed to initialize database connection: {e}")
        return False

def get_connection():
    """Get a connection from the pool."""
    global connection_pool
    try:
        return connection_pool.getconn()
    except Exception as e:
        logger.error(f"Failed to get database connection: {e}")
        raise

def release_connection(conn):
    """Release a connection back to the pool."""
    global connection_pool
    try:
        connection_pool.putconn(conn)
    except Exception as e:
        logger.error(f"Failed to release database connection: {e}")

def execute_query(query, params=None, fetch=False):
    """Execute a SQL query and optionally fetch results."""
    conn = None
    try:
        conn = get_connection()
        with conn.cursor() as cursor:
            cursor.execute(query, params)
            if fetch:
                return cursor.fetchall()
            conn.commit()
            return True
    except Exception as e:
        if conn:
            conn.rollback()
        logger.error(f"Query execution failed: {e}")
        logger.debug(f"Failed query: {query}")
        if params:
            logger.debug(f"Parameters: {params}")
        raise
    finally:
        if conn:
            release_connection(conn)

def optimize_postgres_config():
    """Apply PostgreSQL configuration optimizations."""
    logger.info("Applying PostgreSQL configuration optimizations")
    
    try:
        # Execute optimization queries
        execute_query("""
        SET work_mem = '256MB';
        SET maintenance_work_mem = '512MB';
        SET max_parallel_workers_per_gather = 4;
        SET max_parallel_workers = 8;
        SET max_worker_processes = 8;
        SET synchronous_commit = OFF;
        """)
        
        logger.info("PostgreSQL configuration optimized for ETL")
        return True
    except Exception as e:
        logger.error(f"Failed to optimize PostgreSQL configuration: {e}")
        logger.warning("Continuing with default PostgreSQL settings")
        return False

def identify_csv_files(synthea_dir):
    """Identify and validate Synthea CSV files."""
    logger.info(f"Identifying CSV files in {synthea_dir}")
    
    csv_files = {}
    expected_files = [
        'patients.csv', 'encounters.csv', 'conditions.csv', 
        'observations.csv', 'procedures.csv', 'medications.csv',
        'immunizations.csv', 'allergies.csv', 'careplans.csv'
    ]
    
    try:
        # Check if directory exists
        if not os.path.isdir(synthea_dir):
            logger.error(f"Directory not found: {synthea_dir}")
            return None
        
        # List all CSV files
        for filename in os.listdir(synthea_dir):
            if filename.endswith('.csv'):
                file_path = os.path.join(synthea_dir, filename)
                csv_files[filename] = file_path
                logger.debug(f"Found CSV file: {filename}")
        
        # Check for required files
        missing_files = [f for f in expected_files if f not in csv_files]
        if missing_files:
            logger.warning(f"Missing expected files: {', '.join(missing_files)}")
        
        logger.info(f"Found {len(csv_files)} CSV files")
        return csv_files
    except Exception as e:
        logger.error(f"Error identifying CSV files: {e}")
        return None

def analyze_csv_header(csv_file):
    """Analyze CSV header to determine column types."""
    try:
        with open(csv_file, 'r') as f:
            reader = csv.reader(f)
            header = next(reader)
            
            # Sample a few rows to determine types
            sample_rows = []
            for _ in range(10):
                try:
                    sample_rows.append(next(reader))
                except StopIteration:
                    break
            
            # Determine column types
            column_types = []
            for i, column in enumerate(header):
                # Check if column might be numeric
                numeric = True
                date = True
                has_values = False

                for row in sample_rows:
                    if i < len(row) and row[i]:
                        has_values = True
                        # Check if value could be numeric
                        try:
                            float(row[i])
                        except ValueError:
                            numeric = False

                        # Check if value could be a date
                        try:
                            # Simple date format check (YYYY-MM-DD)
                            if not (len(row[i]) == 10 and row[i][4] == '-' and row[i][7] == '-'):
                                date = False
                        except:
                            date = False
                
                # If we didn't see any values in the sample, assume TEXT to be safe
                if not has_values:
                    numeric = False
                    date = False
                
                # Determine type based on analysis
                if numeric:
                    column_types.append('NUMERIC')
                elif date:
                    column_types.append('DATE')
                else:
                    column_types.append('TEXT')
            
            return header, column_types
    except Exception as e:
        logger.error(f"Error analyzing CSV header for {csv_file}: {e}")
        return None, None

def create_temp_table(table_name, columns, column_types):
    """Create a temporary table with appropriate column types."""
    try:
        # Build CREATE TABLE statement
        create_sql = f"CREATE TEMPORARY TABLE {table_name} ("
        for i, column in enumerate(columns):
            create_sql += f"\"{column}\" {column_types[i]}, "
        create_sql = create_sql[:-2] + ")"
        
        # Execute the statement
        execute_query(create_sql)
        logger.debug(f"Created temporary table: {table_name}")
        return True
    except Exception as e:
        logger.error(f"Failed to create temporary table {table_name}: {e}")
        return False

def bulk_load_csv(csv_file, table_name):
    """Bulk load CSV data into a table using COPY command."""
    conn = None
    try:
        conn = get_connection()
        with conn.cursor() as cursor:
            with open(csv_file, 'r') as f:
                # Skip header
                next(f)
                # Use COPY command for bulk loading
                cursor.copy_expert(
                    f"COPY {table_name} FROM STDIN WITH CSV",
                    f
                )
            conn.commit()
            
            # Get row count
            cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
            count = cursor.fetchone()[0]
            logger.info(f"Loaded {count} rows into {table_name} from {os.path.basename(csv_file)}")
            return count
    except Exception as e:
        if conn:
            conn.rollback()
        logger.error(f"Failed to bulk load CSV data: {e}")
        return 0
    finally:
        if conn:
            release_connection(conn)

def is_numeric(value):
    """Check if a value is numeric."""
    if value is None or value == '':
        return False
    try:
        float(value)
        return True
    except ValueError:
        return False

def process_patients(patients_csv, omop_schema='omop'):
    """Process Synthea patients directly to OMOP person table."""
    logger.info("Processing patients data")
    
    try:
        # Analyze CSV header
        header, column_types = analyze_csv_header(patients_csv)
        if not header:
            return False
        
        # Create temporary table
        temp_table = "temp_patients"
        if not create_temp_table(temp_table, header, column_types):
            return False
        
        # Bulk load data
        row_count = bulk_load_csv(patients_csv, temp_table)
        if row_count == 0:
            return False
        
        # Transform and load to OMOP person table
        logger.info("Transforming patients to OMOP person table")
        transform_sql = f"""
        INSERT INTO {omop_schema}.person (
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
            -- Generate a sequential ID instead of using the UUID
            ROW_NUMBER() OVER (ORDER BY p."Id") AS person_id,
            CASE
                WHEN p."GENDER" = 'M' THEN 8507
                WHEN p."GENDER" = 'F' THEN 8532
                ELSE 0
            END AS gender_concept_id,
            EXTRACT(YEAR FROM p."BIRTHDATE"::date) AS year_of_birth,
            EXTRACT(MONTH FROM p."BIRTHDATE"::date) AS month_of_birth,
            EXTRACT(DAY FROM p."BIRTHDATE"::date) AS day_of_birth,
            p."BIRTHDATE"::timestamp AS birth_datetime,
            CASE
                WHEN p."RACE" = 'WHITE' THEN 8527
                WHEN p."RACE" = 'BLACK' THEN 8516
                WHEN p."RACE" = 'ASIAN' THEN 8515
                ELSE 0
            END AS race_concept_id,
            CASE
                WHEN p."ETHNICITY" = 'HISPANIC' THEN 38003563
                WHEN p."ETHNICITY" = 'NONHISPANIC' THEN 38003564
                ELSE 0
            END AS ethnicity_concept_id,
            NULL AS location_id,
            NULL AS provider_id,
            NULL AS care_site_id,
            p."Id" AS person_source_value,
            p."GENDER" AS gender_source_value,
            0 AS gender_source_concept_id,
            p."RACE" AS race_source_value,
            0 AS race_source_concept_id,
            p."ETHNICITY" AS ethnicity_source_value,
            0 AS ethnicity_source_concept_id
        FROM
            {temp_table} p
        """
        
        execute_query(transform_sql)
        
        # Get count of inserted records
        person_count = execute_query(f"SELECT COUNT(*) FROM {omop_schema}.person", fetch=True)[0][0]
        logger.info(f"Inserted {person_count} records into {omop_schema}.person table")
        
        # Log progress
        execute_query(
            "SELECT staging.log_progress('Transform Person', 'complete', %s)",
            (person_count,)
        )
        
        return True
    except Exception as e:
        logger.error(f"Error processing patients data: {e}")
        return False

def process_encounters(encounters_csv, omop_schema='omop'):
    """Process Synthea encounters directly to OMOP visit_occurrence table."""
    logger.info("Processing encounters data")
    process_name = "ETL_Pipeline"
    step_name = "process_encounters"
    
    try:
        # Track progress if available
        if progress_tracker and progress_tracker_available:
            # Get file size to estimate work
            file_size = os.path.getsize(encounters_csv)
            progress_tracker.start_step(process_name, step_name, file_size, "Starting encounters processing")
        
        # Analyze CSV header
        header, column_types = analyze_csv_header(encounters_csv)
        if not header:
            if progress_tracker and progress_tracker_available:
                progress_tracker.complete_step(process_name, step_name, False, "Failed to analyze CSV header")
            return False
        
        # Create temporary table
        temp_table = "temp_encounters"
        if not create_temp_table(temp_table, header, column_types):
            if progress_tracker and progress_tracker_available:
                progress_tracker.complete_step(process_name, step_name, False, "Failed to create temporary table")
            return False
        
        # Track progress update
        if progress_tracker and progress_tracker_available:
            progress_tracker.update_progress(process_name, step_name, file_size * 0.1, "Created temporary table")
        
        # Bulk load data
        row_count = bulk_load_csv(encounters_csv, temp_table)
        if row_count == 0:
            if progress_tracker and progress_tracker_available:
                progress_tracker.complete_step(process_name, step_name, False, "Failed to load data")
            return False
            
        # Track progress update
        if progress_tracker and progress_tracker_available:
            progress_tracker.update_progress(process_name, step_name, file_size * 0.5, f"Loaded {row_count} encounters")
        
        # Create visit_map table if it doesn't exist
        execute_query("""
        CREATE TABLE IF NOT EXISTS staging.visit_map (
          source_visit_id      TEXT PRIMARY KEY,
          visit_occurrence_id  INTEGER NOT NULL UNIQUE,
          person_id            INTEGER,
          created_at           TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
          updated_at           TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
        );
        
        -- Create a sequence for visit IDs if it doesn't exist
        CREATE SEQUENCE IF NOT EXISTS staging.visit_occurrence_seq START 1 INCREMENT 1;
        """)
        
        # Track progress update
        if progress_tracker and progress_tracker_available:
            progress_tracker.update_progress(process_name, step_name, file_size * 0.6, "Created visit_map table")
        
        # Populate visit_map for new encounters
        populate_map_sql = """
        INSERT INTO staging.visit_map (source_visit_id, visit_occurrence_id, person_id)
        SELECT 
            e."Id" AS source_visit_id,
            nextval('staging.visit_occurrence_seq') AS visit_occurrence_id,
            p.person_id
        FROM 
            temp_encounters e
        JOIN 
            omop.person p ON p.person_source_value = e."PATIENT"
        WHERE 
            e."Id" NOT IN (SELECT source_visit_id FROM staging.visit_map)
        ON CONFLICT (source_visit_id) DO NOTHING;
        """
        execute_query(populate_map_sql)
        
        # Track progress update
        if progress_tracker and progress_tracker_available:
            progress_tracker.update_progress(process_name, step_name, file_size * 0.8, "Populated visit_map table")
        
        # Transform and load to OMOP visit_occurrence table
        logger.info("Transforming encounters to OMOP visit_occurrence table")
        transform_sql = f"""
        INSERT INTO {omop_schema}.visit_occurrence (
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
            discharged_to_source_value,
            preceding_visit_occurrence_id
        )
        SELECT
            vm.visit_occurrence_id,
            -- Join to person table to get the person_id
            p.person_id,
            CASE
                WHEN e."ENCOUNTERCLASS" = 'ambulatory' THEN 9202
                WHEN e."ENCOUNTERCLASS" = 'emergency' THEN 9203
                WHEN e."ENCOUNTERCLASS" = 'inpatient' THEN 9201
                WHEN e."ENCOUNTERCLASS" = 'wellness' THEN 9202
                WHEN e."ENCOUNTERCLASS" = 'urgentcare' THEN 9203
                WHEN e."ENCOUNTERCLASS" = 'outpatient' THEN 9202
                ELSE 0
            END AS visit_concept_id,
            e."START"::date AS visit_start_date,
            e."START"::timestamp AS visit_start_datetime,
            e."STOP"::date AS visit_end_date,
            e."STOP"::timestamp AS visit_end_datetime,
            32817 AS visit_type_concept_id, -- EHR
            NULL AS provider_id,
            NULL AS care_site_id,
            e."Id" AS visit_source_value,
            0 AS visit_source_concept_id,
            0 AS admitted_from_concept_id,
            NULL AS admitted_from_source_value,
            0 AS discharged_to_concept_id,
            NULL AS discharged_to_source_value,
            NULL AS preceding_visit_occurrence_id
        FROM
            {temp_table} e
        JOIN 
            omop.person p ON p.person_source_value = e."PATIENT"
        JOIN 
            staging.visit_map vm ON vm.source_visit_id = e."Id"
        WHERE 
            vm.visit_occurrence_id NOT IN (
                SELECT visit_occurrence_id
                FROM {omop_schema}.visit_occurrence
            )
        """
        
        execute_query(transform_sql)
        
        # Get count of inserted records
        visit_count = execute_query(f"SELECT COUNT(*) FROM {omop_schema}.visit_occurrence", fetch=True)[0][0]
        logger.info(f"Inserted {visit_count} records into {omop_schema}.visit_occurrence table")
        
        # Log progress
        execute_query(
            "SELECT staging.log_progress('Transform Visit Occurrence', 'complete', %s)",
            (visit_count,)
        )
        
        return True
    except Exception as e:
        logger.error(f"Error processing encounters data: {e}")
        return False

def process_observations(observations_csv, omop_schema='omop'):
    """Process Synthea observations with direct routing to measurement or observation."""
    logger.info("Processing observations data with direct routing")
    
    try:
        # Analyze CSV header
        header, column_types = analyze_csv_header(observations_csv)
        if not header:
            return False
        
        # Create temporary table
        temp_table = "temp_observations"
        if not create_temp_table(temp_table, header, column_types):
            return False
        
        # Bulk load data
        row_count = bulk_load_csv(observations_csv, temp_table)
        if row_count == 0:
            return False
        
        # Create sequences if they don't exist
        execute_query("CREATE SEQUENCE IF NOT EXISTS staging.measurement_seq START 1 INCREMENT 1")
        execute_query("CREATE SEQUENCE IF NOT EXISTS staging.observation_seq START 1 INCREMENT 1")
        
        # Transform and load numeric values to OMOP measurement table
        logger.info("Transforming numeric observations to OMOP measurement table")
        measurement_sql = f"""
        INSERT INTO {omop_schema}.measurement (
            measurement_id,
            person_id,
            measurement_concept_id,
            measurement_date,
            measurement_datetime,
            measurement_type_concept_id,
            operator_concept_id,
            value_as_number,
            value_as_concept_id,
            unit_concept_id,
            provider_id,
            visit_occurrence_id,
            measurement_source_value,
            measurement_source_concept_id,
            unit_source_value,
            value_source_value
        )
        SELECT
            nextval('staging.measurement_seq') AS measurement_id,
            (SELECT person_id FROM omop.person p WHERE p.person_source_value = o."PATIENT") AS person_id,
            0 AS measurement_concept_id, -- Will be mapped later
            o."DATE"::date AS measurement_date,
            o."DATE"::timestamp AS measurement_datetime,
            32817 AS measurement_type_concept_id, -- EHR
            0 AS operator_concept_id,
            CASE
                WHEN o."VALUE" ~ '^[0-9]+(\\.[0-9]+)?$' THEN o."VALUE"::numeric
                ELSE NULL
            END AS value_as_number,
            0 AS value_as_concept_id,
            0 AS unit_concept_id,
            NULL AS provider_id,
            o."ENCOUNTER"::bigint AS visit_occurrence_id,
            o."CODE" AS measurement_source_value,
            0 AS measurement_source_concept_id,
            o."UNITS" AS unit_source_value,
            o."VALUE" AS value_source_value
        FROM
            {temp_table} o
        WHERE
            o."VALUE" ~ '^[0-9]+(\\.[0-9]+)?$' OR o."VALUE" ~ '^[0-9]+$'
        """
        
        execute_query(measurement_sql)
        
        # Transform and load non-numeric values to OMOP observation table
        logger.info("Transforming non-numeric observations to OMOP observation table")
        observation_sql = f"""
        INSERT INTO {omop_schema}.observation (
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
            observation_source_value,
            observation_source_concept_id,
            unit_source_value,
            qualifier_source_value,
            value_source_value
        )
        SELECT
            nextval('staging.observation_seq') AS observation_id,
            (SELECT person_id FROM omop.person p WHERE p.person_source_value = o."PATIENT") AS person_id,
            0 AS observation_concept_id, -- Will be mapped later
            o."DATE"::date AS observation_date,
            o."DATE"::timestamp AS observation_datetime,
            32817 AS observation_type_concept_id, -- EHR
            NULL AS value_as_number,
            o."VALUE" AS value_as_string,
            0 AS value_as_concept_id,
            0 AS qualifier_concept_id,
            0 AS unit_concept_id,
            NULL AS provider_id,
            o."ENCOUNTER"::bigint AS visit_occurrence_id,
            o."CODE" AS observation_source_value,
            0 AS observation_source_concept_id,
            o."UNITS" AS unit_source_value,
            NULL AS qualifier_source_value,
            o."VALUE" AS value_source_value
        FROM
            {temp_table} o
        WHERE
            NOT (o."VALUE" ~ '^[0-9]+(\\.[0-9]+)?$' OR o."VALUE" ~ '^[0-9]+$')
        """
        
        execute_query(observation_sql)
        
        # Get counts of inserted records
        measurement_count = execute_query(f"SELECT COUNT(*) FROM {omop_schema}.measurement", fetch=True)[0][0]
        observation_count = execute_query(f"SELECT COUNT(*) FROM {omop_schema}.observation", fetch=True)[0][0]
        
        logger.info(f"Inserted {measurement_count} records into {omop_schema}.measurement table")
        logger.info(f"Inserted {observation_count} records into {omop_schema}.observation table")
        
        # Log progress
        execute_query(
            "SELECT staging.log_progress('Transform Measurement and Observation', 'complete', %s)",
            (measurement_count + observation_count,)
        )
        
        return True
    except Exception as e:
        logger.error(f"Error processing observations data: {e}")
        return False

def process_conditions(conditions_csv, omop_schema='omop'):
    """Process Synthea conditions directly to OMOP condition_occurrence table."""
    logger.info("Processing conditions data")
    
    try:
        # Analyze CSV header
        header, column_types = analyze_csv_header(conditions_csv)
        if not header:
            return False
        
        # Create temporary table
        temp_table = "temp_conditions"
        if not create_temp_table(temp_table, header, column_types):
            return False
        
        # Bulk load data
        row_count = bulk_load_csv(conditions_csv, temp_table)
        if row_count == 0:
            return False
        
        # Create sequence if it doesn't exist
        execute_query("CREATE SEQUENCE IF NOT EXISTS staging.condition_occurrence_seq START 1 INCREMENT 1")
        
        # Transform and load to OMOP condition_occurrence table
        logger.info("Transforming conditions to OMOP condition_occurrence table")
        transform_sql = f"""
        INSERT INTO {omop_schema}.condition_occurrence (
            condition_occurrence_id,
            person_id,
            condition_concept_id,
            condition_start_date,
            condition_start_datetime,
            condition_end_date,
            condition_end_datetime,
            condition_type_concept_id,
            stop_reason,
            provider_id,
            visit_occurrence_id,
            visit_detail_id,
            condition_source_value,
            condition_source_concept_id,
            condition_status_source_value,
            condition_status_concept_id
        )
        SELECT
            nextval('staging.condition_occurrence_seq') AS condition_occurrence_id,
            (SELECT person_id FROM omop.person p WHERE p.person_source_value = c."PATIENT") AS person_id,
            0 AS condition_concept_id, -- Will be mapped later
            c."START"::date AS condition_start_date,
            c."START"::timestamp AS condition_start_datetime,
            c."STOP"::date AS condition_end_date,
            c."STOP"::timestamp AS condition_end_datetime,
            32817 AS condition_type_concept_id, -- EHR
            NULL AS stop_reason,
            NULL AS provider_id,
            c."ENCOUNTER"::bigint AS visit_occurrence_id,
            NULL AS visit_detail_id,
            c."CODE" AS condition_source_value,
            0 AS condition_source_concept_id,
            NULL AS condition_status_source_value,
            0 AS condition_status_concept_id
        FROM
            {temp_table} c
        """
        
        execute_query(transform_sql)
        
        # Get count of inserted records
        condition_count = execute_query(f"SELECT COUNT(*) FROM {omop_schema}.condition_occurrence", fetch=True)[0][0]
        logger.info(f"Inserted {condition_count} records into {omop_schema}.condition_occurrence table")
        
        # Log progress
        execute_query(
            "SELECT staging.log_progress('Transform Condition Occurrence', 'complete', %s)",
            (condition_count,)
        )
        
        return True
    except Exception as e:
        logger.error(f"Error processing conditions data: {e}")
        return False

def process_medications(medications_csv, omop_schema='omop'):
    """Process Synthea medications directly to OMOP drug_exposure table."""
    logger.info("Processing medications data")
    
    try:
        # Analyze CSV header
        header, column_types = analyze_csv_header(medications_csv)
        if not header:
            return False
        
        # Create temporary table
        temp_table = "temp_medications"
        if not create_temp_table(temp_table, header, column_types):
            return False
        
        # Bulk load data
        row_count = bulk_load_csv(medications_csv, temp_table)
        if row_count == 0:
            return False
        
        # Create sequence if it doesn't exist
        execute_query("CREATE SEQUENCE IF NOT EXISTS staging.drug_exposure_seq START 1 INCREMENT 1")
        
        # Transform and load to OMOP drug_exposure table
        logger.info("Transforming medications to OMOP drug_exposure table")
        transform_sql = f"""
        INSERT INTO {omop_schema}.drug_exposure (
            drug_exposure_id,
            person_id,
            drug_concept_id,
            drug_exposure_start_date,
            drug_exposure_start_datetime,
            drug_exposure_end_date,
            drug_exposure_end_datetime,
            verbatim_end_date,
            drug_type_concept_id,
            stop_reason,
            refills,
            quantity,
            days_supply,
            sig,
            route_concept_id,
            lot_number,
            provider_id,
            visit_occurrence_id,
            visit_detail_id,
            drug_source_value,
            drug_source_concept_id,
            route_source_value,
            dose_unit_source_value
        )
        SELECT
            nextval('staging.drug_exposure_seq') AS drug_exposure_id,
            (SELECT person_id FROM omop.person p WHERE p.person_source_value = m."PATIENT") AS person_id,
            0 AS drug_concept_id, -- Will be mapped later
            m."START"::date AS drug_exposure_start_date,
            m."START"::timestamp AS drug_exposure_start_datetime,
            m."STOP"::date AS drug_exposure_end_date,
            m."STOP"::timestamp AS drug_exposure_end_datetime,
            m."STOP"::date AS verbatim_end_date,
            32817 AS drug_type_concept_id, -- EHR
            NULL AS stop_reason,
            0 AS refills,
            CASE
                WHEN m."DISPENSES" ~ '^[0-9]+(\\.[0-9]+)?$' THEN m."DISPENSES"::numeric
                ELSE NULL
            END AS quantity,
            CASE
                WHEN m."DAYS_SUPPLY" ~ '^[0-9]+(\\.[0-9]+)?$' THEN m."DAYS_SUPPLY"::numeric
                ELSE NULL
            END AS days_supply,
            NULL AS sig,
            0 AS route_concept_id,
            NULL AS lot_number,
            NULL AS provider_id,
            m."ENCOUNTER"::bigint AS visit_occurrence_id,
            NULL AS visit_detail_id,
            m."CODE" AS drug_source_value,
            0 AS drug_source_concept_id,
            NULL AS route_source_value,
            NULL AS dose_unit_source_value
        FROM
            {temp_table} m
        """
        
        execute_query(transform_sql)
        
        # Get count of inserted records
        drug_count = execute_query(f"SELECT COUNT(*) FROM {omop_schema}.drug_exposure", fetch=True)[0][0]
        logger.info(f"Inserted {drug_count} records into {omop_schema}.drug_exposure table")
        
        # Log progress
        execute_query(
            "SELECT staging.log_progress('Transform Drug Exposure', 'complete', %s)",
            (drug_count,)
        )
        
        return True
    except Exception as e:
        logger.error(f"Error processing medications data: {e}")
        return False

def process_procedures(procedures_csv, omop_schema='omop'):
    """Process Synthea procedures directly to OMOP procedure_occurrence table."""
    logger.info("Processing procedures data")
    
    try:
        # Analyze CSV header
        header, column_types = analyze_csv_header(procedures_csv)
        if not header:
            return False
        
        # Create temporary table
        temp_table = "temp_procedures"
        if not create_temp_table(temp_table, header, column_types):
            return False
        
        # Bulk load data
        row_count = bulk_load_csv(procedures_csv, temp_table)
        if row_count == 0:
            return False
        
        # Create sequence if it doesn't exist
        execute_query("CREATE SEQUENCE IF NOT EXISTS staging.procedure_occurrence_seq START 1 INCREMENT 1")
        
        # Transform and load to OMOP procedure_occurrence table
        logger.info("Transforming procedures to OMOP procedure_occurrence table")
        transform_sql = f"""
        INSERT INTO {omop_schema}.procedure_occurrence (
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
            nextval('staging.procedure_occurrence_seq') AS procedure_occurrence_id,
            (SELECT person_id FROM omop.person p2 WHERE p2.person_source_value = p."PATIENT") AS person_id,
            0 AS procedure_concept_id, -- Will be mapped later
            p."DATE"::date AS procedure_date,
            p."DATE"::timestamp AS procedure_datetime,
            32817 AS procedure_type_concept_id, -- EHR
            0 AS modifier_concept_id,
            NULL AS quantity,
            NULL AS provider_id,
            p."ENCOUNTER"::bigint AS visit_occurrence_id,
            NULL AS visit_detail_id,
            p."CODE" AS procedure_source_value,
            0 AS procedure_source_concept_id,
            NULL AS modifier_source_value
        FROM
            {temp_table} p
        """
        
        execute_query(transform_sql)
        
        # Get count of inserted records
        procedure_count = execute_query(f"SELECT COUNT(*) FROM {omop_schema}.procedure_occurrence", fetch=True)[0][0]
        logger.info(f"Inserted {procedure_count} records into {omop_schema}.procedure_occurrence table")
        
        # Log progress
        execute_query(
            "SELECT staging.log_progress('Transform Procedure Occurrence', 'complete', %s)",
            (procedure_count,)
        )
        
        return True
    except Exception as e:
        logger.error(f"Error processing procedures data: {e}")
        return False

def create_observation_periods(omop_schema='omop'):
    """Create observation periods for patients."""
    logger.info("Creating observation periods")
    
    try:
        # Create sequence if it doesn't exist
        execute_query("CREATE SEQUENCE IF NOT EXISTS staging.observation_period_seq START 1 INCREMENT 1")
        
        # Create observation periods
        transform_sql = f"""
        INSERT INTO {omop_schema}.observation_period (
            observation_period_id,
            person_id,
            observation_period_start_date,
            observation_period_end_date,
            period_type_concept_id
        )
        SELECT
            nextval('staging.observation_period_seq') AS observation_period_id,
            person_id,
            MIN(observation_start_date) AS observation_period_start_date,
            MAX(observation_end_date) AS observation_period_end_date,
            32817 AS period_type_concept_id -- EHR
        FROM (
            -- Visits
            SELECT
                person_id,
                visit_start_date AS observation_start_date,
                visit_end_date AS observation_end_date
            FROM
                {omop_schema}.visit_occurrence
            UNION ALL
            -- Conditions
            SELECT
                person_id,
                condition_start_date AS observation_start_date,
                COALESCE(condition_end_date, condition_start_date) AS observation_end_date
            FROM
                {omop_schema}.condition_occurrence
            UNION ALL
            -- Drugs
            SELECT
                person_id,
                drug_exposure_start_date AS observation_start_date,
                COALESCE(drug_exposure_end_date, drug_exposure_start_date) AS observation_end_date
            FROM
                {omop_schema}.drug_exposure
            UNION ALL
            -- Procedures
            SELECT
                person_id,
                procedure_date AS observation_start_date,
                procedure_date AS observation_end_date
            FROM
                {omop_schema}.procedure_occurrence
            UNION ALL
            -- Measurements
            SELECT
                person_id,
                measurement_date AS observation_start_date,
                measurement_date AS observation_end_date
            FROM
                {omop_schema}.measurement
            UNION ALL
            -- Observations
            SELECT
                person_id,
                observation_date AS observation_start_date,
                observation_date AS observation_end_date
            FROM
                {omop_schema}.observation
        ) AS all_dates
        GROUP BY
            person_id
        """
        
        execute_query(transform_sql)
        
        # Get count of inserted records
        period_count = execute_query(f"SELECT COUNT(*) FROM {omop_schema}.observation_period", fetch=True)[0][0]
        logger.info(f"Created {period_count} observation periods")
        
        # Log progress
        execute_query(
            "SELECT staging.log_progress('Create Observation Period', 'complete', %s)",
            (period_count,)
        )
        
        return True
    except Exception as e:
        logger.error(f"Error creating observation periods: {e}")
        return False

def map_concepts(omop_schema='omop'):
    """Map source concepts to standard concepts."""
    logger.info("Mapping source concepts to standard concepts")
    
    try:
        # Map person concepts
        logger.info("Mapping person concepts")
        execute_query(f"""
        UPDATE {omop_schema}.person p
        SET gender_concept_id = COALESCE(c.target_concept_id, 0)
        FROM staging.local_to_omop_concept_map c
        WHERE p.gender_source_value = c.source_code
        AND c.source_vocabulary = 'Gender'
        """)
        
        # Map condition concepts
        logger.info("Mapping condition concepts")
        execute_query(f"""
        UPDATE {omop_schema}.condition_occurrence co
        SET condition_concept_id = COALESCE(c.target_concept_id, 0)
        FROM staging.local_to_omop_concept_map c
        WHERE co.condition_source_value = c.source_code
        AND c.domain_id = 'Condition'
        """)
        
        # Map drug concepts
        logger.info("Mapping drug concepts")
        execute_query(f"""
        UPDATE {omop_schema}.drug_exposure de
        SET drug_concept_id = COALESCE(c.target_concept_id, 0)
        FROM staging.local_to_omop_concept_map c
        WHERE de.drug_source_value = c.source_code
        AND c.domain_id = 'Drug'
        """)
        
        # Map procedure concepts
        logger.info("Mapping procedure concepts")
        execute_query(f"""
        UPDATE {omop_schema}.procedure_occurrence po
        SET procedure_concept_id = COALESCE(c.target_concept_id, 0)
        FROM staging.local_to_omop_concept_map c
        WHERE po.procedure_source_value = c.source_code
        AND c.domain_id = 'Procedure'
        """)
        
        # Map measurement concepts
        logger.info("Mapping measurement concepts")
        execute_query(f"""
        UPDATE {omop_schema}.measurement m
        SET measurement_concept_id = COALESCE(c.target_concept_id, 0)
        FROM staging.local_to_omop_concept_map c
        WHERE m.measurement_source_value = c.source_code
        AND c.domain_id = 'Measurement'
        """)
        
        # Map observation concepts
        logger.info("Mapping observation concepts")
        execute_query(f"""
        UPDATE {omop_schema}.observation o
        SET observation_concept_id = COALESCE(c.target_concept_id, 0)
        FROM staging.local_to_omop_concept_map c
        WHERE o.observation_source_value = c.source_code
        AND c.domain_id = 'Observation'
        """)
        
        logger.info("Concept mapping completed")
        
        # Log progress
        execute_query(
            "SELECT staging.log_progress('Map Concepts', 'complete', NULL)"
        )
        
        return True
    except Exception as e:
        logger.error(f"Error mapping concepts: {e}")
        return False

def analyze_tables(omop_schema='omop'):
    """Analyze tables for query optimization."""
    logger.info("Analyzing tables for query optimization")
    
    try:
        tables = [
            'person', 'observation_period', 'visit_occurrence', 
            'condition_occurrence', 'drug_exposure', 'procedure_occurrence',
            'measurement', 'observation', 'death', 'cost'
        ]
        
        for table in tables:
            logger.info(f"Analyzing table {omop_schema}.{table}")
            execute_query(f"ANALYZE {omop_schema}.{table}")
        
        logger.info("Table analysis completed")
        
        # Log progress
        execute_query(
            "SELECT staging.log_progress('Analyze Tables', 'complete', NULL)"
        )
        
        return True
    except Exception as e:
        logger.error(f"Error analyzing tables: {e}")
        return False

def validate_data(omop_schema='omop'):
    """Validate the transformed data."""
    logger.info("Validating transformed data")
    
    try:
        # Check record counts
        tables = [
            'person', 'observation_period', 'visit_occurrence', 
            'condition_occurrence', 'drug_exposure', 'procedure_occurrence',
            'measurement', 'observation'
        ]
        
        for table in tables:
            count = execute_query(f"SELECT COUNT(*) FROM {omop_schema}.{table}", fetch=True)[0][0]
            logger.info(f"Table {omop_schema}.{table}: {count} records")
        
        # Check for unmapped concepts
        unmapped_counts = execute_query(f"""
        SELECT 
            'condition_occurrence' AS table_name,
            COUNT(*) AS total_count,
            SUM(CASE WHEN condition_concept_id = 0 THEN 1 ELSE 0 END) AS unmapped_count
        FROM 
            {omop_schema}.condition_occurrence
        UNION ALL
        SELECT 
            'drug_exposure' AS table_name,
            COUNT(*) AS total_count,
            SUM(CASE WHEN drug_concept_id = 0 THEN 1 ELSE 0 END) AS unmapped_count
        FROM 
            {omop_schema}.drug_exposure
        UNION ALL
        SELECT 
            'procedure_occurrence' AS table_name,
            COUNT(*) AS total_count,
            SUM(CASE WHEN procedure_concept_id = 0 THEN 1 ELSE 0 END) AS unmapped_count
        FROM 
            {omop_schema}.procedure_occurrence
        UNION ALL
        SELECT 
            'measurement' AS table_name,
            COUNT(*) AS total_count,
            SUM(CASE WHEN measurement_concept_id = 0 THEN 1 ELSE 0 END) AS unmapped_count
        FROM 
            {omop_schema}.measurement
        UNION ALL
        SELECT 
            'observation' AS table_name,
            COUNT(*) AS total_count,
            SUM(CASE WHEN observation_concept_id = 0 THEN 1 ELSE 0 END) AS unmapped_count
        FROM 
            {omop_schema}.observation
        """, fetch=True)
        
        for row in unmapped_counts:
            table_name, total_count, unmapped_count = row
            unmapped_percent = (unmapped_count / total_count * 100) if total_count > 0 else 0
            logger.info(f"Table {table_name}: {unmapped_count} unmapped concepts ({unmapped_percent:.2f}%)")
        
        # Check date ranges
        date_ranges = execute_query(f"""
        SELECT 
            MIN(observation_period_start_date) AS min_start_date,
            MAX(observation_period_end_date) AS max_end_date
        FROM 
            {omop_schema}.observation_period
        """, fetch=True)[0]
        
        min_date, max_date = date_ranges
        logger.info(f"Observation period date range: {min_date} to {max_date}")
        
        # Log progress
        execute_query(
            "SELECT staging.log_progress('Validate Data', 'complete', NULL)"
        )
        
        return True
    except Exception as e:
        logger.error(f"Error validating data: {e}")
        return False

def run_parallel_etl(csv_files, max_workers=4, omop_schema='omop'):
    """Run ETL steps in parallel where possible."""
    logger.info(f"Running parallel ETL with {max_workers} workers")
    
    try:
        # Step 1: Process person data (dependency for all other steps)
        if not process_patients(csv_files.get('patients.csv'), omop_schema):
            logger.error("Failed to process patients data")
            return False
        
        # Step 2: Process encounters (dependency for clinical data)
        if not process_encounters(csv_files.get('encounters.csv'), omop_schema):
            logger.error("Failed to process encounters data")
            return False
        
        # Step 3: Process clinical data in parallel
        clinical_tasks = []
        
        if 'conditions.csv' in csv_files:
            clinical_tasks.append(('conditions', lambda: process_conditions(csv_files['conditions.csv'], omop_schema)))
        
        if 'medications.csv' in csv_files:
            clinical_tasks.append(('medications', lambda: process_medications(csv_files['medications.csv'], omop_schema)))
        
        if 'procedures.csv' in csv_files:
            clinical_tasks.append(('procedures', lambda: process_procedures(csv_files['procedures.csv'], omop_schema)))
        
        if 'observations.csv' in csv_files:
            clinical_tasks.append(('observations', lambda: process_observations(csv_files['observations.csv'], omop_schema)))
        
        # Execute clinical tasks in parallel
        with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
            future_to_task = {executor.submit(task): name for name, task in clinical_tasks}
            for future in concurrent.futures.as_completed(future_to_task):
                task_name = future_to_task[future]
                try:
                    result = future.result()
                    if result:
                        logger.info(f"Task '{task_name}' completed successfully")
                    else:
                        logger.error(f"Task '{task_name}' failed")
                except Exception as e:
                    logger.error(f"Task '{task_name}' raised an exception: {e}")
        
        # Step 4: Create observation periods
        if not create_observation_periods(omop_schema):
            logger.error("Failed to create observation periods")
            return False
        
        # Step 5: Map concepts
        if not map_concepts(omop_schema):
            logger.error("Failed to map concepts")
            return False
        
        # Step 6: Analyze tables
        if not analyze_tables(omop_schema):
            logger.error("Failed to analyze tables")
            return False
        
        # Step 7: Validate data
        if not validate_data(omop_schema):
            logger.error("Failed to validate data")
            return False
        
        logger.info("Parallel ETL completed successfully")
        return True
    except Exception as e:
        logger.error(f"Error in parallel ETL: {e}")
        return False

def direct_import_pipeline(synthea_dir, max_workers=4, skip_optimization=False, skip_validation=False):
    """Main function implementing the direct import pipeline."""
    logger.info(f"Starting direct import pipeline from {synthea_dir}")
    
    try:
        # Step 1: Initialize database connection
        if not initialize_database_connection():
            logger.error("Failed to initialize database connection")
            return False
        
        # Step 2: Optimize PostgreSQL configuration if not skipped
        if not skip_optimization:
            optimize_postgres_config()
        
        # Step 3: Identify CSV files
        csv_files = identify_csv_files(synthea_dir)
        if not csv_files:
            logger.error("Failed to identify CSV files")
            return False
        
        # Step 4: Reset OMOP tables
        logger.info("Resetting OMOP tables")
        execute_query("""
        DO $$
        DECLARE
            tables TEXT[] := ARRAY[
                'person', 'observation_period', 'visit_occurrence', 
                'condition_occurrence', 'drug_exposure', 'procedure_occurrence',
                'measurement', 'observation', 'death', 'cost'
            ];
            t TEXT;
        BEGIN
            FOREACH t IN ARRAY tables
            LOOP
                EXECUTE 'TRUNCATE TABLE omop.' || t || ' CASCADE';
                EXECUTE 'ALTER SEQUENCE IF EXISTS staging.' || t || '_seq RESTART WITH 1';
            END LOOP;
        END $$;
        """)
        
        # Step 5: Log ETL start
        execute_query(
            "SELECT staging.log_progress('Direct Import Pipeline', 'start', NULL)"
        )
        
        # Step 6: Run parallel ETL
        if not run_parallel_etl(csv_files, max_workers):
            logger.error("Parallel ETL failed")
            return False
        
        # Step 7: Log ETL completion
        execute_query(
            "SELECT staging.log_progress('Direct Import Pipeline', 'complete', NULL)"
        )
        
        logger.info("Direct import pipeline completed successfully")
        return True
    except Exception as e:
        logger.error(f"Error in direct import pipeline: {e}")
        return False
    finally:
        # Close connection pool
        if connection_pool:
            connection_pool.closeall()
            logger.info("Database connection pool closed")

def main():
    """Main function to run the optimized ETL process."""
    global args
    start_time = time.time()
    
    # Parse command line arguments
    args = parse_arguments()
    
    # Set up logging
    setup_logging(args.debug)
    
    logger.info("Starting optimized Synthea to OMOP ETL process")
    
    # Check if progress tracking is requested but not available
    if getattr(args, 'track_progress', False) and not progress_tracker_available:
        logger.warning("Progress tracking requested but etl_progress_tracking.py not found. Continuing without progress tracking.")
    
    # Run the direct import pipeline
    success = direct_import_pipeline(
        args.synthea_dir,
        max_workers=args.max_workers,
        skip_optimization=args.skip_optimization,
        skip_validation=args.skip_validation
    )
    
    end_time = time.time()
    duration = end_time - start_time
    
    if success:
        logger.info(f"ETL process completed successfully in {duration:.2f} seconds")
        return 0
    else:
        logger.error(f"ETL process failed after {duration:.2f} seconds")
        return 1

if __name__ == "__main__":
    sys.exit(main())
