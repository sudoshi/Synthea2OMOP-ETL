Overview of the New Structure
We’ll create a small directory of multiple Python scripts, each focusing on a distinct piece of the ETL. Then we’ll have a master script (etl_main.py) that orchestrates them in order. The directory could look like this:

etl_pipeline/
  ├─ etl_main.py
  ├─ etl_setup.py
  ├─ etl_patients.py
  ├─ etl_encounters.py
  ├─ etl_conditions.py
  ├─ etl_medications.py
  ├─ etl_procedures.py
  ├─ etl_observations.py
  ├─ etl_observation_periods.py
  ├─ etl_map_concepts.py
  ├─ etl_analyze.py
  ├─ etl_validate.py
  └─ (optional) etl_progress_tracking.py
Note:

You can add or remove modules depending on your needs (e.g., if you combine conditions/procedures, etc.).

Each module can handle its own "checkpoint" logic, or you can share a global checkpoint.

If your environment uses a package structure (e.g., etl_pipeline/ as a Python package), you can import them via from etl_pipeline.etl_patients import process_patients etc.

Below is an illustration using chunk-based insertion for row-by-row (or small-batch) progress in the encounters step. You can apply the same pattern to other steps if you want that granular progress. The other steps below show simpler structures. All code samples assume you have a shared file or module where you do:

Database connection (get_connection, release_connection, etc.)

Logging setup

Checkpoint handling

Possibly a shared progress tracker

We'll call that shared file etl_setup.py (though you can rename it) to house common utilities. Then each step script will import from it. Finally, an etl_main.py orchestrates them in sequence.

1. Shared Setup and Utilities (etl_setup.py)
python
Copy
#!/usr/bin/env python3
"""
etl_setup.py - Shared utilities for database connections, logging, checkpointing, etc.
"""

import os
import sys
import json
import logging
import time
import psycopg2
from psycopg2 import pool
from datetime import datetime
from typing import Any, Dict, Optional, Tuple

# GLOBALS
connection_pool: Optional[pool.ThreadedConnectionPool] = None
CHECKPOINT_FILE = ".synthea_etl_checkpoint.json"

# Default config (override as needed)
db_config = {
    'host': 'localhost',
    'port': '5432',
    'database': 'ohdsi',
    'user': 'postgres',
    'password': 'acumenus'
}

def init_logging(debug: bool=False) -> None:
    """Initialize logging, optionally with debug level."""
    level = logging.DEBUG if debug else logging.INFO
    logging.basicConfig(
        level=level,
        format="%(asctime)s [%(levelname)s] %(message)s",
        handlers=[logging.StreamHandler(sys.stdout)]
    )
    logging.info("Logging initialized.")
    
def init_db_connection_pool(
    host: str = db_config['host'],
    port: str = db_config['port'],
    database: str = db_config['database'],
    user: str = db_config['user'],
    password: str = db_config['password'],
    minconn: int = 1,
    maxconn: int = 10
) -> None:
    """Create a global connection pool for Postgres."""
    global connection_pool
    logging.info("Initializing database connection pool...")
    try:
        connection_pool = pool.ThreadedConnectionPool(
            minconn=minconn,
            maxconn=maxconn,
            host=host,
            port=port,
            database=database,
            user=user,
            password=password
        )
        logging.info(f"Database connection pool initialized: {host}:{port}/{database}")
    except Exception as e:
        logging.error(f"Failed to initialize DB connection pool: {e}")
        sys.exit(1)

def get_connection() -> psycopg2.extensions.connection:
    """Get a database connection from the pool."""
    global connection_pool
    if not connection_pool:
        raise RuntimeError("Connection pool not initialized. Call init_db_connection_pool first.")
    return connection_pool.getconn()

def release_connection(conn: psycopg2.extensions.connection) -> None:
    """Release a connection back to the pool."""
    global connection_pool
    if connection_pool:
        connection_pool.putconn(conn)

def execute_query(query: str, params: Tuple[Any, ...] = (), fetch: bool=False) -> Any:
    """Helper to execute a query within a borrowed connection."""
    conn = None
    try:
        conn = get_connection()
        with conn.cursor() as cur:
            cur.execute(query, params)
            if fetch:
                result = cur.fetchall()
                conn.commit()
                return result
            conn.commit()
            return True
    except Exception as e:
        if conn:
            conn.rollback()
        logging.error(f"Error executing query: {e}")
        logging.debug(f"Query was: {query}")
        raise
    finally:
        if conn:
            release_connection(conn)

def load_checkpoint() -> Dict[str, Any]:
    """Load checkpoint from file, or return empty structure."""
    if os.path.exists(CHECKPOINT_FILE):
        try:
            with open(CHECKPOINT_FILE, 'r') as f:
                return json.load(f)
        except Exception as e:
            logging.warning(f"Failed to load checkpoint file: {e}")
    return {"completed_steps": [], "stats": {}}

def save_checkpoint(checkpoint: Dict[str, Any]) -> None:
    """Save checkpoint to file."""
    checkpoint["last_updated"] = datetime.now().isoformat()
    try:
        with open(CHECKPOINT_FILE, "w") as f:
            json.dump(checkpoint, f, indent=2)
    except Exception as e:
        logging.warning(f"Failed to save checkpoint: {e}")

def mark_step_completed(step_name: str, stats: Dict[str, Any] = None) -> None:
    """Mark a step as completed in the checkpoint."""
    cp = load_checkpoint()
    if step_name not in cp["completed_steps"]:
        cp["completed_steps"].append(step_name)
    if stats:
        cp["stats"][step_name] = stats
    save_checkpoint(cp)
    logging.debug(f"Step completed: {step_name}")

def is_step_completed(step_name: str) -> bool:
    """Check if step is completed in the checkpoint."""
    cp = load_checkpoint()
    return (step_name in cp["completed_steps"])

######################################
# Additional shared helpers go here...
######################################
This file provides:

Logging: init_logging(debug)

DB Connection: init_db_connection_pool(), get_connection(), release_connection(conn), execute_query(...)

Checkpoint: load_checkpoint(), save_checkpoint(), mark_step_completed(), is_step_completed(...)

You can add any other utility functions (like reading CSV in chunks, or progress bars) here too.

2. Schema Setup / Lookup Table Population (etl_setup.py or new module)
You might choose to put your schema creation / table creation logic in the same file above or a separate file. If separate, call it something like etl_schema.py. For brevity, let's assume we keep them in one file called etl_schema.py, and we import from it:

python
Copy
#!/usr/bin/env python3
"""
etl_schema.py - Create OMOP schemas, staging tables, lookup tables, etc.
"""

import logging
from etl_setup import execute_query, mark_step_completed, is_step_completed

def ensure_schemas_exist():
    step_name = "ensure_schemas_exist"
    if is_step_completed(step_name):
        logging.info("Schemas already exist. Skipping.")
        return
    
    logging.info("Ensuring required schemas exist...")
    # CREATE SCHEMA, CREATE TABLE, CREATE SEQUENCE, etc.
    # ...
    
    mark_step_completed(step_name)

def populate_lookup_tables():
    step_name = "populate_lookup_tables"
    if is_step_completed(step_name):
        logging.info("Lookup tables already populated. Skipping.")
        return
    
    logging.info("Populating lookup tables...")
    # Do INSERTs into staging.gender_lookup, race_lookup, etc.
    # ...
    
    mark_step_completed(step_name)
3. An Example Step with Row-by-Row or Chunk Progress: etl_encounters.py
Below is a sample of how you might do the Encounters ETL with:

Pre-count of rows in CSV

Read row by row (or in small chunks)

Insert them individually or in batch to staging (or directly to your final table).

Track progress using a simple progress bar or the tqdm library.

Post-count in the database to confirm how many actually loaded

python
Copy
#!/usr/bin/env python3
"""
etl_encounters.py - Process Synthea 'encounters.csv' data into OMOP visit_occurrence.
With row-by-row or small-batch progress reporting and pre/post row counts.
"""

import os
import logging
import time
import csv
from typing import List, Dict, Any
from etl_setup import (
    execute_query,
    mark_step_completed,
    is_step_completed,
    get_connection,
    release_connection,
)
# If you have tqdm installed, you can use it; otherwise, we do a manual approach
try:
    from tqdm import tqdm
    HAS_TQDM = True
except ImportError:
    HAS_TQDM = False

def process_encounters(encounters_csv: str) -> bool:
    step_name = "process_encounters"
    if is_step_completed(step_name):
        logging.info("Encounters step already completed. Skipping.")
        return True
    
    logging.info(f"Processing Encounters from {encounters_csv} ...")

    # --- Pre-count rows in the CSV ---
    total_rows = 0
    with open(encounters_csv, 'r') as f:
        next(f)  # Skip header
        for _ in f:
            total_rows += 1
    logging.info(f"Found {total_rows} encounters in CSV (excluding header).")
    
    # --- Pre-count in the DB for reference ---
    pre_count_result = execute_query("SELECT COUNT(*) FROM omop.visit_occurrence", fetch=True)
    pre_count_db = pre_count_result[0][0] if pre_count_result else 0
    logging.info(f"Current visit_occurrence rows (before load): {pre_count_db}")
    
    # We will do chunk-based loading: read CSV row by row, accumulate in a batch, then insert.
    BATCH_SIZE = 1000
    inserted_rows = 0
    start_time = time.time()

    # We'll insert into a TEMP table or directly into the final table. Let's do a staging table approach:
    # 1) Create temp table
    tmp_table_name = "temp_encounters"
    create_sql = """
    CREATE TEMP TABLE IF NOT EXISTS temp_encounters (
        "Id" TEXT,
        "START" TEXT,
        "STOP" TEXT,
        "PATIENT" TEXT,
        "ENCOUNTERCLASS" TEXT,
        "CODE" TEXT,
        "DESCRIPTION" TEXT,
        "BASE_ENCOUNTER_COST" TEXT,
        "TOTAL_CLAIM_COST" TEXT,
        "PAYER_COVERAGE" TEXT,
        "REASONCODE" TEXT,
        "REASONDESCRIPTION" TEXT,
        "PROVIDERID" TEXT
    );
    """
    execute_query(create_sql)
    
    # 2) Insert rows in small batches
    conn = get_connection()
    try:
        with conn.cursor() as cur, open(encounters_csv, 'r', newline='') as f:
            reader = csv.DictReader(f)
            batch = []
            header = reader.fieldnames
            
            if HAS_TQDM:
                pbar = tqdm(total=total_rows, desc="Loading Encounters", unit="row")
            else:
                pbar = None
            
            for row_idx, row in enumerate(reader, start=1):
                # Convert row to tuple in same col order as temp table
                batch.append((
                    row["Id"],
                    row["START"],
                    row["STOP"],
                    row["PATIENT"],
                    row["ENCOUNTERCLASS"],
                    row["CODE"],
                    row["DESCRIPTION"],
                    row["BASE_ENCOUNTER_COST"],
                    row["TOTAL_CLAIM_COST"],
                    row["PAYER_COVERAGE"],
                    row["REASONCODE"],
                    row["REASONDESCRIPTION"],
                    row["PROVIDERID"]
                ))
                
                # If batch is large enough, insert
                if len(batch) >= BATCH_SIZE:
                    _insert_encounter_batch(cur, batch, tmp_table_name)
                    inserted_rows += len(batch)
                    batch.clear()
                    if pbar:
                        pbar.update(BATCH_SIZE)
            
            # leftover batch
            if batch:
                _insert_encounter_batch(cur, batch, tmp_table_name)
                inserted_rows += len(batch)
                if pbar:
                    pbar.update(len(batch))
            
            conn.commit()
            if pbar:
                pbar.close()
    except Exception as e:
        conn.rollback()
        logging.error(f"Error loading encounters: {e}")
        release_connection(conn)
        return False
    finally:
        release_connection(conn)
    
    end_time = time.time()
    logging.info(f"Inserted {inserted_rows} rows into temp_encounters in {(end_time - start_time):.2f} sec.")
    
    # Now we map from temp_encounters -> staging.visit_map -> omop.visit_occurrence
    # 1) Create or update staging.visit_map
    # ...
    # 2) Insert into omop.visit_occurrence
    # ...
    
    # Post-count in DB
    post_count_result = execute_query("SELECT COUNT(*) FROM omop.visit_occurrence", fetch=True)
    post_count_db = post_count_result[0][0] if post_count_result else 0
    new_records = post_count_db - pre_count_db
    logging.info(f"After insertion, visit_occurrence has {post_count_db} rows (added {new_records}).")
    
    # Mark step completed
    mark_step_completed(step_name, {
        "csv_rows": total_rows,
        "inserted_rows": inserted_rows,
        "db_new_records": new_records,
    })
    logging.info("Encounters processing complete.")
    return True

def _insert_encounter_batch(cur, batch, table_name: str) -> None:
    """
    Helper to do a parameterized INSERT for a batch into the temp table.
    This uses the standard psycopg2 mogrify approach or executemany.
    """
    insert_sql = f"""
    INSERT INTO {table_name} ("Id","START","STOP","PATIENT","ENCOUNTERCLASS","CODE",
            "DESCRIPTION","BASE_ENCOUNTER_COST","TOTAL_CLAIM_COST","PAYER_COVERAGE",
            "REASONCODE","REASONDESCRIPTION","PROVIDERID")
    VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
    """
    cur.executemany(insert_sql, batch)
Key points:

Pre-Count the CSV lines (total_rows).

Pre-Count the target table rows (pre_count_db).

Insert in chunks, updating a progress bar (tqdm or manual).

Time the insertion step.

Post-Count the target table and compare.

Mark step as completed in checkpoint with stats (how many you inserted, how many ended up in the DB, etc.).

You’d repeat a similar pattern in the other step scripts (etl_patients.py, etl_conditions.py, etc.), adapting column names, logic, and final target tables.

4. A Master Orchestrator (etl_main.py)
Finally, we have a main script that:

Parses arguments.

Initializes logging.

Initializes DB.

Runs each ETL script in the correct sequence.

For example:

python
Copy
#!/usr/bin/env python3
"""
etl_main.py - Master script that orchestrates the entire Synthea-to-OMOP pipeline
"""

import argparse
import logging
import os
import sys

# Import from your local modules
from etl_setup import init_logging, init_db_connection_pool
from etl_setup import is_step_completed, mark_step_completed
from etl_schema import ensure_schemas_exist, populate_lookup_tables
from etl_patients import process_patients
from etl_encounters import process_encounters
from etl_conditions import process_conditions
from etl_medications import process_medications
from etl_procedures import process_procedures
from etl_observations import process_observations
from etl_observation_periods import create_observation_periods
from etl_map_concepts import map_source_to_standard_concepts
from etl_analyze import analyze_tables
from etl_validate import validate_etl_results

def parse_args():
    parser = argparse.ArgumentParser(description="Master ETL Orchestrator")
    parser.add_argument("--debug", action="store_true", help="Enable debug logging")
    parser.add_argument("--synthea-dir", required=True, help="Directory with Synthea CSVs")
    parser.add_argument("--reset-tables", action="store_true", help="Reset OMOP tables first")
    parser.add_argument("--skip-concept-mapping", action="store_true", help="Skip concept mapping step")
    parser.add_argument("--skip-validation", action="store_true", help="Skip validation step")
    # Add more arguments if needed, e.g., DB credentials
    return parser.parse_args()

def main():
    args = parse_args()
    init_logging(debug=args.debug)
    
    # 1) Initialize DB connections
    # (In real usage, might pass DB creds from CLI or environment)
    init_db_connection_pool()
    
    # 2) Possibly reset tables
    if args.reset_tables:
        # Call your reset logic here
        # e.g. reset_omop_tables()
        pass
    
    # 3) Ensure schemas, lookups
    ensure_schemas_exist()
    populate_lookup_tables()
    
    # 4) ETL each domain
    synthea_dir = args.synthea_dir
    
    patients_csv = os.path.join(synthea_dir, "patients.csv")
    encounters_csv = os.path.join(synthea_dir, "encounters.csv")
    conditions_csv = os.path.join(synthea_dir, "conditions.csv")
    medications_csv = os.path.join(synthea_dir, "medications.csv")
    procedures_csv = os.path.join(synthea_dir, "procedures.csv")
    observations_csv = os.path.join(synthea_dir, "observations.csv")
    
    process_patients(patients_csv)
    process_encounters(encounters_csv)
    process_conditions(conditions_csv)
    process_medications(medications_csv)
    process_procedures(procedures_csv)
    process_observations(observations_csv)
    
    # 5) Create observation periods
    create_observation_periods()
    
    # 6) Concept mapping (if not skipped)
    if not args.skip_concept_mapping:
        map_source_to_standard_concepts()
    
    # 7) Analyze
    analyze_tables()
    
    # 8) Validate results (unless skipped)
    if not args.skip_validation:
        validate_etl_results()
    
    logging.info("ETL pipeline completed successfully.")

if __name__ == "__main__":
    main()
With this structure:

etl_setup.py holds shared logic (logging, DB connections, checkpointing).

etl_schema.py (or etl_reset.py etc.) can hold schema creation.

Each domain (patients, encounters, conditions, medications, procedures, observations) can have its own script that loads data from CSV, optionally row by row or chunk by chunk, and logs progress.

The final “glue” is etl_main.py, which calls each step in turn.

5. Handling the Encounter Failure and Observations Issue
Encounter Step Failing
If your encounter step is causing the database to “go into recovery mode,” that typically indicates a severe error: possibly out-of-memory, a malformed statement, or an extremely large transaction. Breaking the load into smaller chunks (like BATCH_SIZE = 1000) can help avoid giant transactions. Also, watch for:

Data Type mismatches (e.g., an invalid date that Postgres can’t parse).

Invalid foreign keys if you have constraints.

Potential deadlocks if you run multiple threads.

With chunk-based inserts, you can catch errors earlier. If, for example, a certain row has an invalid date format, your script can log which batch or row triggered the failure.

Observations Table is Empty
Your query that inserts “non-numeric” observations might be skipping everything. Common causes:

Regex Mismatch: Check that WHERE NOT (o."VALUE" ~ '^[0-9]+(\\.[0-9]+)?$') truly matches your non-numeric data. If Synthea data sometimes has empty strings, you might want to treat them as numeric or skip them.

Join: If the join to staging.visit_map fails (no matching ENCOUNTER), no rows will load. Check that the CSV column is spelled "ENCOUNTER" in the script and “ENCOUNTER” in the actual CSV.

No Insert: If you do an INSERT ... WHERE NOT EXISTS(...), ensure that the logic isn’t over-restricting the insert.

Printing out the first few lines of your CSV and stepping through the logic helps to confirm everything lines up.

6. Conclusion
Using multiple smaller scripts with clear entry points can make debugging much easier, especially if you run into a catastrophic failure (like the database going into recovery). Each script can:

Pre-count CSV rows.

Load into a staging table in small batches, with partial progress logs.

Check the resulting row counts in the DB.

Insert into final OMOP tables.

Log final counts and time taken.

Then your etl_main.py calls them in a known sequence. This approach:

Prevents re-running the entire pipeline if only “encounters” step is failing.

Makes row-by-row or chunk-based progress simpler to implement.

Allows you to store (or ignore) partial results in staging.

Tracks insertion performance with start/end times for each chunk.

With that in place, you should be able to see:

Exactly how many rows each step attempts to load.

A progress bar or line-by-line logging.

The time each chunk or step takes.

The post-insert row count in the database.

