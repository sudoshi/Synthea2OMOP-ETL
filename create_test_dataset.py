#!/usr/bin/env python3
"""
Create a small test dataset for testing the ETL process.

This script creates a small subset of the data in the population schema
to allow for faster testing of the ETL process.
"""

import os
import sys
import argparse
import logging
import datetime
import psycopg2
from utils.config_loader import ConfigLoader

# Set up logging
log_dir = "logs"
os.makedirs(log_dir, exist_ok=True)
log_file = os.path.join(log_dir, f"create_test_dataset_{datetime.datetime.now().strftime('%Y%m%d_%H%M%S')}.log")
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler(log_file),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

def get_db_connection(config_loader):
    """Create a database connection using configuration."""
    connection_string = f"postgresql://{config_loader.get_env('DB_USER', 'postgres')}:{config_loader.get_env('DB_PASSWORD', '')}@{config_loader.get_env('DB_HOST', 'localhost')}:{config_loader.get_env('DB_PORT', '5432')}/{config_loader.get_env('DB_NAME', 'ohdsi')}"
    return psycopg2.connect(connection_string)

def create_test_schema(conn, schema_name="population_test"):
    """Create a test schema for the test dataset."""
    cursor = conn.cursor()
    try:
        logger.info(f"Creating schema {schema_name}")
        cursor.execute(f"DROP SCHEMA IF EXISTS {schema_name} CASCADE")
        cursor.execute(f"CREATE SCHEMA {schema_name}")
        conn.commit()
        logger.info(f"Schema {schema_name} created successfully")
        return True
    except Exception as e:
        conn.rollback()
        logger.error(f"Error creating schema {schema_name}: {e}")
        return False
    finally:
        cursor.close()

def create_test_tables(conn, source_schema="population", target_schema="population_test"):
    """Create test tables with the same structure as the source tables."""
    cursor = conn.cursor()
    try:
        # Get list of tables in the source schema
        cursor.execute(f"""
            SELECT table_name
            FROM information_schema.tables
            WHERE table_schema = '{source_schema}'
            AND table_type = 'BASE TABLE'
        """)
        tables = [row[0] for row in cursor.fetchall()]
        
        # Create tables in the target schema
        for table in tables:
            logger.info(f"Creating table {target_schema}.{table}")
            cursor.execute(f"""
                CREATE TABLE {target_schema}.{table} (LIKE {source_schema}.{table} INCLUDING ALL)
            """)
        
        conn.commit()
        logger.info(f"Tables created successfully in schema {target_schema}")
        return True
    except Exception as e:
        conn.rollback()
        logger.error(f"Error creating tables in schema {target_schema}: {e}")
        return False
    finally:
        cursor.close()

def populate_test_tables(conn, source_schema="population", target_schema="population_test", limit=1000):
    """Populate test tables with a subset of data from the source tables."""
    cursor = conn.cursor()
    try:
        # Get list of tables in the source schema
        cursor.execute(f"""
            SELECT table_name
            FROM information_schema.tables
            WHERE table_schema = '{source_schema}'
            AND table_type = 'BASE TABLE'
        """)
        tables = [row[0] for row in cursor.fetchall()]
        
        # First, get a subset of patients
        logger.info(f"Selecting {limit} patients for the test dataset")
        cursor.execute(f"""
            INSERT INTO {target_schema}.patients_typed
            SELECT *
            FROM {source_schema}.patients_typed
            LIMIT {limit}
        """)
        
        # Get the patient IDs
        cursor.execute(f"""
            SELECT patient_id
            FROM {target_schema}.patients_typed
        """)
        patient_ids = [row[0] for row in cursor.fetchall()]
        
        if not patient_ids:
            logger.error("No patients selected for the test dataset")
            conn.rollback()
            return False
        
        # Format patient IDs for SQL IN clause
        patient_ids_str = "', '".join(str(pid) for pid in patient_ids)
        
        # Populate other tables with data for the selected patients
        for table in tables:
            if table == 'patients_typed':
                # Already populated
                continue
            
            logger.info(f"Populating table {target_schema}.{table}")
            
            # Check if the table has a patient column
            cursor.execute(f"""
                SELECT column_name
                FROM information_schema.columns
                WHERE table_schema = '{source_schema}'
                AND table_name = '{table}'
                AND column_name = 'patient'
            """)
            
            if cursor.fetchone():
                # Table has a patient column, filter by patient
                cursor.execute(f"""
                    INSERT INTO {target_schema}.{table}
                    SELECT *
                    FROM {source_schema}.{table}
                    WHERE patient IN ('{patient_ids_str}')
                """)
            else:
                # Table doesn't have a patient column, just take a subset
                cursor.execute(f"""
                    INSERT INTO {target_schema}.{table}
                    SELECT *
                    FROM {source_schema}.{table}
                    LIMIT {limit}
                """)
        
        conn.commit()
        logger.info(f"Tables populated successfully in schema {target_schema}")
        return True
    except Exception as e:
        conn.rollback()
        logger.error(f"Error populating tables in schema {target_schema}: {e}")
        return False
    finally:
        cursor.close()

def create_indexes(conn, schema_name="population_test"):
    """Create indexes on the test tables."""
    cursor = conn.cursor()
    try:
        logger.info(f"Creating indexes in schema {schema_name}")
        
        # Create indexes on patient_id and patient columns
        cursor.execute(f"""
            CREATE INDEX IF NOT EXISTS idx_{schema_name}_patients_typed_patient_id 
            ON {schema_name}.patients_typed (patient_id)
        """)
        
        # Create indexes on patient and encounter columns for other tables
        cursor.execute(f"""
            SELECT table_name
            FROM information_schema.tables
            WHERE table_schema = '{schema_name}'
            AND table_type = 'BASE TABLE'
            AND table_name != 'patients_typed'
        """)
        tables = [row[0] for row in cursor.fetchall()]
        
        for table in tables:
            # Check if the table has a patient column
            cursor.execute(f"""
                SELECT column_name
                FROM information_schema.columns
                WHERE table_schema = '{schema_name}'
                AND table_name = '{table}'
                AND column_name = 'patient'
            """)
            
            if cursor.fetchone():
                cursor.execute(f"""
                    CREATE INDEX IF NOT EXISTS idx_{schema_name}_{table}_patient 
                    ON {schema_name}.{table} (patient)
                """)
            
            # Check if the table has an encounter column
            cursor.execute(f"""
                SELECT column_name
                FROM information_schema.columns
                WHERE table_schema = '{schema_name}'
                AND table_name = '{table}'
                AND column_name = 'encounter'
            """)
            
            if cursor.fetchone():
                cursor.execute(f"""
                    CREATE INDEX IF NOT EXISTS idx_{schema_name}_{table}_encounter 
                    ON {schema_name}.{table} (encounter)
                """)
            
            # Check if the table has a code column
            cursor.execute(f"""
                SELECT column_name
                FROM information_schema.columns
                WHERE table_schema = '{schema_name}'
                AND table_name = '{table}'
                AND column_name = 'code'
            """)
            
            if cursor.fetchone():
                cursor.execute(f"""
                    CREATE INDEX IF NOT EXISTS idx_{schema_name}_{table}_code 
                    ON {schema_name}.{table} (code)
                """)
            
            # Check if the table has a category column
            cursor.execute(f"""
                SELECT column_name
                FROM information_schema.columns
                WHERE table_schema = '{schema_name}'
                AND table_name = '{table}'
                AND column_name = 'category'
            """)
            
            if cursor.fetchone():
                cursor.execute(f"""
                    CREATE INDEX IF NOT EXISTS idx_{schema_name}_{table}_category 
                    ON {schema_name}.{table} (category)
                """)
        
        conn.commit()
        logger.info(f"Indexes created successfully in schema {schema_name}")
        return True
    except Exception as e:
        conn.rollback()
        logger.error(f"Error creating indexes in schema {schema_name}: {e}")
        return False
    finally:
        cursor.close()

def analyze_tables(conn, schema_name="population_test"):
    """Analyze tables to update statistics."""
    cursor = conn.cursor()
    try:
        logger.info(f"Analyzing tables in schema {schema_name}")
        
        cursor.execute(f"""
            SELECT table_name
            FROM information_schema.tables
            WHERE table_schema = '{schema_name}'
            AND table_type = 'BASE TABLE'
        """)
        tables = [row[0] for row in cursor.fetchall()]
        
        for table in tables:
            logger.info(f"Analyzing table {schema_name}.{table}")
            cursor.execute(f"ANALYZE {schema_name}.{table}")
        
        conn.commit()
        logger.info(f"Tables analyzed successfully in schema {schema_name}")
        return True
    except Exception as e:
        conn.rollback()
        logger.error(f"Error analyzing tables in schema {schema_name}: {e}")
        return False
    finally:
        cursor.close()

def print_table_counts(conn, schema_name="population_test"):
    """Print row counts for tables in the schema."""
    cursor = conn.cursor()
    try:
        logger.info(f"Row counts for tables in schema {schema_name}:")
        
        cursor.execute(f"""
            SELECT table_name
            FROM information_schema.tables
            WHERE table_schema = '{schema_name}'
            AND table_type = 'BASE TABLE'
        """)
        tables = [row[0] for row in cursor.fetchall()]
        
        for table in tables:
            cursor.execute(f"SELECT COUNT(*) FROM {schema_name}.{table}")
            count = cursor.fetchone()[0]
            logger.info(f"  {table}: {count:,} rows")
        
        return True
    except Exception as e:
        logger.error(f"Error getting row counts for tables in schema {schema_name}: {e}")
        return False
    finally:
        cursor.close()

def main():
    """Main function."""
    parser = argparse.ArgumentParser(description="Create a small test dataset for testing the ETL process")
    parser.add_argument("--schema", default="population_test", help="Name of the test schema (default: population_test)")
    parser.add_argument("--source", default="population", help="Name of the source schema (default: population)")
    parser.add_argument("--limit", type=int, default=1000, help="Number of patients to include in the test dataset (default: 1000)")
    args = parser.parse_args()
    
    logger.info("Starting test dataset creation")
    
    # Load configuration
    try:
        config_loader = ConfigLoader()
    except Exception as e:
        logger.error(f"Error loading configuration: {e}")
        return 1
    
    # Connect to the database
    try:
        conn = get_db_connection(config_loader)
    except Exception as e:
        logger.error(f"Error connecting to the database: {e}")
        return 1
    
    try:
        # Create test schema
        if not create_test_schema(conn, args.schema):
            return 1
        
        # Create test tables
        if not create_test_tables(conn, args.source, args.schema):
            return 1
        
        # Populate test tables
        if not populate_test_tables(conn, args.source, args.schema, args.limit):
            return 1
        
        # Create indexes
        if not create_indexes(conn, args.schema):
            return 1
        
        # Analyze tables
        if not analyze_tables(conn, args.schema):
            return 1
        
        # Print table counts
        if not print_table_counts(conn, args.schema):
            return 1
        
        logger.info(f"Test dataset created successfully in schema {args.schema}")
        logger.info(f"To use this test dataset with the ETL process, modify the .env file to set POPULATION_SCHEMA={args.schema}")
        
        return 0
    except Exception as e:
        logger.error(f"Error creating test dataset: {e}")
        return 1
    finally:
        conn.close()

if __name__ == "__main__":
    sys.exit(main())
