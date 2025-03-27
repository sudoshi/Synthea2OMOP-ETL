#!/usr/bin/env python3
"""
test_observations.py - Test script for processing a small sample of observations data
"""

import logging
import time
import psutil
from tqdm import tqdm
from etl_pipeline.etl_setup import init_db_connection_pool, get_connection, release_connection

def test_process_observations(sample_size=100):
    """
    Process a small sample of observations from population.observations
    
    Args:
        sample_size: Number of records to process
    """
    logging.info("\n🔍 Testing observations processing with sample data...")
    
    # Initialize connection
    init_db_connection_pool()
    conn = get_connection()
    
    try:
        # Count total rows in the sample
        with conn.cursor() as cur:
            # Create a temporary table with sample data
            cur.execute(f"""
            CREATE TEMP TABLE temp_observations AS 
            SELECT * FROM population.observations 
            ORDER BY "DATE", "PATIENT", "ENCOUNTER"
            LIMIT {sample_size}
            """)
            
            cur.execute("SELECT COUNT(*) FROM temp_observations")
            total_rows = cur.fetchone()[0]
            logging.info(f"Created temporary table with {total_rows} sample observations")
            
            # Get column information
            cur.execute("""
            SELECT column_name, data_type 
            FROM information_schema.columns 
            WHERE table_name = 'temp_observations'
            ORDER BY ordinal_position
            """)
            columns = cur.fetchall()
            logging.info("Column structure:")
            for col in columns:
                logging.info(f"  - {col[0]}: {col[1]}")
            
            # Get a sample row to examine the data
            cur.execute("SELECT * FROM temp_observations LIMIT 1")
            sample_row = cur.fetchone()
            logging.info(f"Sample row: {sample_row}")
            
            # Create a staging table for processing
            cur.execute("""
            CREATE TEMP TABLE staging_observations (
                id SERIAL PRIMARY KEY,
                patient_id TEXT,
                encounter_id TEXT,
                observation_type VARCHAR(50),
                code VARCHAR(20),
                description TEXT,
                value_as_string TEXT,
                timestamp TIMESTAMP,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
            """)
            
            # Insert the sample data into the staging table
            cur.execute("""
            INSERT INTO staging_observations 
            (patient_id, encounter_id, observation_type, code, description, value_as_string, timestamp)
            SELECT 
                "PATIENT", 
                "ENCOUNTER", 
                "TYPE", 
                "CODE", 
                "DESCRIPTION", 
                "VALUE", 
                "DATE"::timestamp
            FROM temp_observations
            """)
            
            # Check how many rows were inserted
            cur.execute("SELECT COUNT(*) FROM staging_observations")
            inserted_count = cur.fetchone()[0]
            logging.info(f"Successfully inserted {inserted_count} rows into staging table")
            
            # Get sample of processed data
            cur.execute("SELECT * FROM staging_observations LIMIT 5")
            processed_rows = cur.fetchall()
            logging.info("Sample of processed data:")
            for row in processed_rows:
                logging.info(f"  - {row}")
                
        # Commit the transaction
        conn.commit()
        logging.info("✅ Test completed successfully")
        return True
        
    except Exception as e:
        logging.error(f"❌ Error in test: {e}")
        conn.rollback()
        return False
        
    finally:
        release_connection(conn)

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    test_process_observations()
