#!/usr/bin/env python3
"""
etl_concept_mapping.py - Map source concepts to standard OMOP concepts.
This module maps source codes (SNOMED, LOINC, RxNorm, etc.) to standard OMOP concepts
for conditions, procedures, medications, observations, and measurements.
"""

import os
import logging
import time
import sys
from pathlib import Path
from typing import Dict, Any, Optional, List, Tuple

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

def map_source_to_standard_concepts(force_reprocess: bool = False) -> bool:
    """
    Map source concepts to standard OMOP concepts for all relevant tables.
    
    Args:
        force_reprocess: Whether to force reprocessing even if already completed
        
    Returns:
        bool: True if processing was successful, False otherwise
    """
    step_name = "map_source_to_standard_concepts"
    if is_step_completed(step_name, force_reprocess):
        logging.info(ColoredFormatter.info("✅ Concept mapping was previously completed. Skipping."))
        return True
    
    logging.info(ColoredFormatter.info("\n🔍 Mapping source concepts to standard concepts..."))
    
    # Initialize progress tracker
    progress_tracker = ETLProgressTracker()
    progress_tracker.start_step("ETL", step_name, message="Starting concept mapping")
    
    start_time = time.time()
    
    # Define the tables and their mapping fields
    mapping_tasks = [
        {
            "name": "condition_occurrence",
            "source_field": "condition_source_value",
            "source_concept_field": "condition_source_concept_id",
            "target_concept_field": "condition_concept_id",
            "vocabulary": "SNOMED",
            "domain": "Condition"
        },
        {
            "name": "procedure_occurrence",
            "source_field": "procedure_source_value",
            "source_concept_field": "procedure_source_concept_id",
            "target_concept_field": "procedure_concept_id",
            "vocabulary": "SNOMED",
            "domain": "Procedure"
        },
        {
            "name": "drug_exposure",
            "source_field": "drug_source_value",
            "source_concept_field": "drug_source_concept_id",
            "target_concept_field": "drug_concept_id",
            "vocabulary": "RxNorm",
            "domain": "Drug"
        },
        {
            "name": "measurement",
            "source_field": "measurement_source_value",
            "source_concept_field": "measurement_source_concept_id",
            "target_concept_field": "measurement_concept_id",
            "vocabulary": "LOINC",
            "domain": "Measurement"
        },
        {
            "name": "observation",
            "source_field": "observation_source_value",
            "source_concept_field": "observation_source_concept_id",
            "target_concept_field": "observation_concept_id",
            "vocabulary": "LOINC",
            "domain": "Observation"
        }
    ]
    
    # Get total number of records to process
    total_records = 0
    for task in mapping_tasks:
        count_result = execute_query(f"SELECT COUNT(*) FROM omop.{task['name']}", fetch=True)
        task['count'] = count_result[0][0] if count_result else 0
        total_records += task['count']
    
    logging.info(f"Found {total_records:,} total records to map across {len(mapping_tasks)} tables.")
    
    # Update progress tracker with total records
    progress_tracker.update_progress("ETL", step_name, 0, total_items=total_records, 
                                   message=f"Found {total_records:,} records to map")
    
    processed_records = 0
    success = True
    
    try:
        # Process each table
        for task in mapping_tasks:
            table_name = task['name']
            source_field = task['source_field']
            source_concept_field = task['source_concept_field']
            target_concept_field = task['target_concept_field']
            vocabulary = task['vocabulary']
            domain = task['domain']
            count = task['count']
            
            if count == 0:
                logging.info(f"No records in {table_name} to map. Skipping.")
                continue
            
            logging.info(f"Mapping {count:,} records in {table_name}...")
            progress_tracker.update_progress("ETL", step_name, processed_records, total_items=total_records, 
                                          message=f"Mapping {table_name} ({count:,} records)")
            
            # Create progress bar
            progress_bar = create_progress_bar(count, f"Mapping {table_name}")
            
            # 1) First, map source concepts
            source_mapping_sql = f"""
            UPDATE omop.{table_name} t
            SET {source_concept_field} = COALESCE(c.concept_id, 0)
            FROM (
                SELECT DISTINCT {source_field}
                FROM omop.{table_name}
                WHERE {source_concept_field} = 0
            ) AS sources
            LEFT JOIN omop.concept c ON 
                c.concept_code = sources.{source_field} AND
                c.vocabulary_id = '{vocabulary}'
            WHERE t.{source_field} = sources.{source_field};
            """
            
            execute_query(source_mapping_sql)
            
            # 2) Then, map to standard concepts
            standard_mapping_sql = f"""
            UPDATE omop.{table_name} t
            SET {target_concept_field} = COALESCE(c.concept_id, 0)
            FROM omop.concept_relationship cr
            JOIN omop.concept c ON cr.concept_id_2 = c.concept_id
            WHERE 
                t.{source_concept_field} = cr.concept_id_1 AND
                cr.relationship_id = 'Maps to' AND
                c.standard_concept = 'S' AND
                c.domain_id = '{domain}';
            """
            
            execute_query(standard_mapping_sql)
            
            # 3) For records that didn't map, try direct mapping
            direct_mapping_sql = f"""
            UPDATE omop.{table_name} t
            SET {target_concept_field} = COALESCE(c.concept_id, 0)
            FROM omop.concept c
            WHERE 
                t.{target_concept_field} = 0 AND
                c.concept_code = t.{source_field} AND
                c.vocabulary_id = '{vocabulary}' AND
                c.standard_concept = 'S' AND
                c.domain_id = '{domain}';
            """
            
            execute_query(direct_mapping_sql)
            
            # Get mapping stats
            mapping_stats = execute_query(f"""
            SELECT 
                COUNT(*) as total,
                SUM(CASE WHEN {target_concept_field} > 0 THEN 1 ELSE 0 END) as mapped,
                SUM(CASE WHEN {target_concept_field} = 0 THEN 1 ELSE 0 END) as unmapped
            FROM omop.{table_name}
            """, fetch=True)
            
            total = mapping_stats[0][0] if mapping_stats else 0
            mapped = mapping_stats[0][1] if mapping_stats else 0
            unmapped = mapping_stats[0][2] if mapping_stats else 0
            mapping_rate = (mapped / total * 100) if total > 0 else 0
            
            logging.info(f"Mapped {mapped:,} of {total:,} {table_name} records ({mapping_rate:.1f}%)")
            
            # Update progress
            processed_records += count
            update_progress_bar(progress_bar, count)
            close_progress_bar(progress_bar)
            
            progress_tracker.update_progress("ETL", step_name, processed_records, total_items=total_records, 
                                          message=f"Completed mapping {table_name} ({mapping_rate:.1f}% mapped)")
        
        # Map units for measurements
        logging.info("Mapping measurement units...")
        progress_tracker.update_progress("ETL", step_name, processed_records, total_items=total_records, 
                                      message="Mapping measurement units")
        
        execute_query("""
        UPDATE omop.measurement m
        SET unit_concept_id = COALESCE(c.concept_id, 0)
        FROM (
            SELECT DISTINCT unit_source_value
            FROM omop.measurement
            WHERE unit_concept_id = 0 AND unit_source_value IS NOT NULL AND unit_source_value != ''
        ) AS sources
        LEFT JOIN omop.concept c ON 
            LOWER(c.concept_name) = LOWER(sources.unit_source_value) AND
            c.domain_id = 'Unit'
        WHERE LOWER(m.unit_source_value) = LOWER(sources.unit_source_value);
        """)
        
        # Map value_as_concept_id for observations with categorical values
        logging.info("Mapping observation categorical values...")
        progress_tracker.update_progress("ETL", step_name, processed_records, total_items=total_records, 
                                      message="Mapping observation categorical values")
        
        execute_query("""
        UPDATE omop.observation o
        SET value_as_concept_id = COALESCE(c.concept_id, 0)
        FROM (
            SELECT DISTINCT value_as_string
            FROM omop.observation
            WHERE value_as_concept_id = 0 AND value_as_string IS NOT NULL AND value_as_string != ''
        ) AS sources
        LEFT JOIN omop.concept c ON 
            LOWER(c.concept_name) = LOWER(sources.value_as_string) AND
            c.domain_id = 'Meas Value'
        WHERE LOWER(o.value_as_string) = LOWER(sources.value_as_string);
        """)
        
        end_time = time.time()
        total_time = end_time - start_time
        
        logging.info(ColoredFormatter.success(
            f"✅ Successfully mapped source concepts to standard concepts " +
            f"for {total_records:,} records in {total_time:.2f} sec"
        ))
        
        # Mark completion in checkpoint system
        mark_step_completed(step_name, {
            "total_records": total_records,
            "processing_time_sec": total_time
        })
        
        # Update ETL progress tracker with completion status
        progress_tracker.complete_step("ETL", step_name, True, 
                                    f"Successfully mapped {total_records:,} records")
        
        return True
        
    except Exception as e:
        error_msg = f"Error mapping concepts: {e}"
        logging.error(ColoredFormatter.error(f"❌ {error_msg}"))
        
        # Update ETL progress tracker with error
        progress_tracker.complete_step("ETL", step_name, False, error_msg)
        
        return False

if __name__ == "__main__":
    # This allows the module to be run directly for testing
    import argparse
    
    parser = argparse.ArgumentParser(description="Map source concepts to standard OMOP concepts")
    parser.add_argument("--force", action="store_true", help="Force reprocessing even if already completed")
    parser.add_argument("--debug", action="store_true", help="Enable debug logging")
    
    args = parser.parse_args()
    
    from etl_setup import init_logging, init_db_connection_pool
    
    init_logging(debug=args.debug)
    init_db_connection_pool()
    
    success = map_source_to_standard_concepts(args.force)
    sys.exit(0 if success else 1)
