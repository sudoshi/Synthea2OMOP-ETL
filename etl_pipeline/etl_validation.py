#!/usr/bin/env python3
"""
etl_validation.py - Validate the ETL results in OMOP CDM.
This module performs various validation checks on the OMOP CDM data to ensure data quality.
"""

import os
import logging
import time
import sys
from pathlib import Path
from typing import Dict, Any, Optional, List, Tuple
import json

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

def validate_etl_results(force_revalidation: bool = False) -> bool:
    """
    Validate the ETL results by performing various checks on the OMOP CDM data.
    
    Args:
        force_revalidation: Whether to force revalidation even if already completed
        
    Returns:
        bool: True if validation was successful, False otherwise
    """
    step_name = "validate_etl_results"
    if is_step_completed(step_name, force_revalidation):
        logging.info(ColoredFormatter.info("✅ ETL validation was previously completed. Skipping."))
        return True
    
    logging.info(ColoredFormatter.info("\n🔍 Validating ETL results..."))
    
    # Initialize progress tracker
    progress_tracker = ETLProgressTracker()
    progress_tracker.start_step("ETL", step_name, message="Starting ETL validation")
    
    start_time = time.time()
    
    # Define validation checks
    validation_checks = [
        {
            "name": "record_counts",
            "description": "Check record counts in all tables",
            "weight": 1
        },
        {
            "name": "referential_integrity",
            "description": "Check referential integrity between tables",
            "weight": 2
        },
        {
            "name": "date_ranges",
            "description": "Check date ranges in all tables",
            "weight": 1
        },
        {
            "name": "demographics",
            "description": "Check demographic data consistency",
            "weight": 1
        },
        {
            "name": "concept_mapping",
            "description": "Check concept mapping completeness",
            "weight": 2
        }
    ]
    
    total_weight = sum(check["weight"] for check in validation_checks)
    logging.info(f"Running {len(validation_checks)} validation checks...")
    
    # Update progress tracker with total checks
    progress_tracker.update_progress("ETL", step_name, 0, total_items=total_weight, 
                                   message=f"Running {len(validation_checks)} validation checks")
    
    validation_results = {}
    current_progress = 0
    all_checks_passed = True
    
    try:
        # 1) Check record counts
        logging.info("Checking record counts...")
        progress_tracker.update_progress("ETL", step_name, current_progress, total_items=total_weight, 
                                      message="Checking record counts")
        
        record_counts = execute_query("""
        SELECT 'person' as table_name, COUNT(*) as record_count FROM omop.person
        UNION ALL
        SELECT 'visit_occurrence', COUNT(*) FROM omop.visit_occurrence
        UNION ALL
        SELECT 'condition_occurrence', COUNT(*) FROM omop.condition_occurrence
        UNION ALL
        SELECT 'procedure_occurrence', COUNT(*) FROM omop.procedure_occurrence
        UNION ALL
        SELECT 'drug_exposure', COUNT(*) FROM omop.drug_exposure
        UNION ALL
        SELECT 'measurement', COUNT(*) FROM omop.measurement
        UNION ALL
        SELECT 'observation', COUNT(*) FROM omop.observation
        UNION ALL
        SELECT 'observation_period', COUNT(*) FROM omop.observation_period
        """, fetch=True)
        
        count_results = {row[0]: row[1] for row in record_counts}
        
        # Check if any tables have zero records
        empty_tables = [table for table, count in count_results.items() if count == 0]
        
        if empty_tables:
            logging.warning(ColoredFormatter.warning(
                f"⚠️ The following tables have zero records: {', '.join(empty_tables)}"
            ))
            validation_results["record_counts"] = {
                "status": "WARNING",
                "message": f"The following tables have zero records: {', '.join(empty_tables)}",
                "details": count_results
            }
        else:
            logging.info(ColoredFormatter.success(
                f"✅ All tables have records. Person count: {count_results.get('person', 0):,}"
            ))
            validation_results["record_counts"] = {
                "status": "PASS",
                "message": "All tables have records",
                "details": count_results
            }
        
        current_progress += validation_checks[0]["weight"]
        progress_tracker.update_progress("ETL", step_name, current_progress, total_items=total_weight, 
                                      message="Completed record count check")
        
        # 2) Check referential integrity
        logging.info("Checking referential integrity...")
        progress_tracker.update_progress("ETL", step_name, current_progress, total_items=total_weight, 
                                      message="Checking referential integrity")
        
        ref_integrity_checks = [
            {
                "child_table": "visit_occurrence",
                "child_column": "person_id",
                "parent_table": "person",
                "parent_column": "person_id"
            },
            {
                "child_table": "condition_occurrence",
                "child_column": "person_id",
                "parent_table": "person",
                "parent_column": "person_id"
            },
            {
                "child_table": "condition_occurrence",
                "child_column": "visit_occurrence_id",
                "parent_table": "visit_occurrence",
                "parent_column": "visit_occurrence_id"
            },
            {
                "child_table": "procedure_occurrence",
                "child_column": "person_id",
                "parent_table": "person",
                "parent_column": "person_id"
            },
            {
                "child_table": "procedure_occurrence",
                "child_column": "visit_occurrence_id",
                "parent_table": "visit_occurrence",
                "parent_column": "visit_occurrence_id"
            },
            {
                "child_table": "drug_exposure",
                "child_column": "person_id",
                "parent_table": "person",
                "parent_column": "person_id"
            },
            {
                "child_table": "drug_exposure",
                "child_column": "visit_occurrence_id",
                "parent_table": "visit_occurrence",
                "parent_column": "visit_occurrence_id"
            },
            {
                "child_table": "measurement",
                "child_column": "person_id",
                "parent_table": "person",
                "parent_column": "person_id"
            },
            {
                "child_table": "measurement",
                "child_column": "visit_occurrence_id",
                "parent_table": "visit_occurrence",
                "parent_column": "visit_occurrence_id"
            },
            {
                "child_table": "observation",
                "child_column": "person_id",
                "parent_table": "person",
                "parent_column": "person_id"
            },
            {
                "child_table": "observation",
                "child_column": "visit_occurrence_id",
                "parent_table": "visit_occurrence",
                "parent_column": "visit_occurrence_id"
            },
            {
                "child_table": "observation_period",
                "child_column": "person_id",
                "parent_table": "person",
                "parent_column": "person_id"
            }
        ]
        
        integrity_issues = []
        
        for check in ref_integrity_checks:
            child_table = check["child_table"]
            child_column = check["child_column"]
            parent_table = check["parent_table"]
            parent_column = check["parent_column"]
            
            # Skip visit_occurrence_id checks for tables that might have NULL values
            if child_column == "visit_occurrence_id":
                sql = f"""
                SELECT COUNT(*) 
                FROM omop.{child_table} child
                LEFT JOIN omop.{parent_table} parent ON child.{child_column} = parent.{parent_column}
                WHERE child.{child_column} IS NOT NULL AND parent.{parent_column} IS NULL
                """
            else:
                sql = f"""
                SELECT COUNT(*) 
                FROM omop.{child_table} child
                LEFT JOIN omop.{parent_table} parent ON child.{child_column} = parent.{parent_column}
                WHERE parent.{parent_column} IS NULL
                """
            
            result = execute_query(sql, fetch=True)
            orphan_count = result[0][0] if result else 0
            
            if orphan_count > 0:
                issue = f"{orphan_count:,} records in {child_table} have invalid {child_column} references"
                integrity_issues.append(issue)
                logging.warning(ColoredFormatter.warning(f"⚠️ {issue}"))
        
        if integrity_issues:
            validation_results["referential_integrity"] = {
                "status": "FAIL",
                "message": "Referential integrity issues found",
                "details": integrity_issues
            }
            all_checks_passed = False
        else:
            logging.info(ColoredFormatter.success("✅ All referential integrity checks passed"))
            validation_results["referential_integrity"] = {
                "status": "PASS",
                "message": "All referential integrity checks passed",
                "details": []
            }
        
        current_progress += validation_checks[1]["weight"]
        progress_tracker.update_progress("ETL", step_name, current_progress, total_items=total_weight, 
                                      message="Completed referential integrity check")
        
        # 3) Check date ranges
        logging.info("Checking date ranges...")
        progress_tracker.update_progress("ETL", step_name, current_progress, total_items=total_weight, 
                                      message="Checking date ranges")
        
        date_range_checks = [
            {
                "table": "visit_occurrence",
                "start_date": "visit_start_date",
                "end_date": "visit_end_date"
            },
            {
                "table": "condition_occurrence",
                "start_date": "condition_start_date",
                "end_date": "condition_end_date"
            },
            {
                "table": "drug_exposure",
                "start_date": "drug_exposure_start_date",
                "end_date": "drug_exposure_end_date"
            },
            {
                "table": "observation_period",
                "start_date": "observation_period_start_date",
                "end_date": "observation_period_end_date"
            }
        ]
        
        date_issues = []
        
        for check in date_range_checks:
            table = check["table"]
            start_date = check["start_date"]
            end_date = check["end_date"]
            
            # Check for end dates before start dates
            sql = f"""
            SELECT COUNT(*) 
            FROM omop.{table}
            WHERE {end_date} IS NOT NULL AND {end_date} < {start_date}
            """
            
            result = execute_query(sql, fetch=True)
            invalid_count = result[0][0] if result else 0
            
            if invalid_count > 0:
                issue = f"{invalid_count:,} records in {table} have end dates before start dates"
                date_issues.append(issue)
                logging.warning(ColoredFormatter.warning(f"⚠️ {issue}"))
            
            # Check for dates in the future
            sql = f"""
            SELECT COUNT(*) 
            FROM omop.{table}
            WHERE {start_date} > CURRENT_DATE OR ({end_date} IS NOT NULL AND {end_date} > CURRENT_DATE)
            """
            
            result = execute_query(sql, fetch=True)
            future_count = result[0][0] if result else 0
            
            if future_count > 0:
                issue = f"{future_count:,} records in {table} have dates in the future"
                date_issues.append(issue)
                logging.warning(ColoredFormatter.warning(f"⚠️ {issue}"))
        
        if date_issues:
            validation_results["date_ranges"] = {
                "status": "FAIL",
                "message": "Date range issues found",
                "details": date_issues
            }
            all_checks_passed = False
        else:
            logging.info(ColoredFormatter.success("✅ All date range checks passed"))
            validation_results["date_ranges"] = {
                "status": "PASS",
                "message": "All date range checks passed",
                "details": []
            }
        
        current_progress += validation_checks[2]["weight"]
        progress_tracker.update_progress("ETL", step_name, current_progress, total_items=total_weight, 
                                      message="Completed date range check")
        
        # 4) Check demographics
        logging.info("Checking demographics...")
        progress_tracker.update_progress("ETL", step_name, current_progress, total_items=total_weight, 
                                      message="Checking demographics")
        
        demographic_issues = []
        
        # Check for invalid gender
        gender_sql = """
        SELECT COUNT(*) 
        FROM omop.person
        WHERE gender_concept_id NOT IN (8507, 8532) -- Male, Female
        """
        
        result = execute_query(gender_sql, fetch=True)
        invalid_gender_count = result[0][0] if result else 0
        
        if invalid_gender_count > 0:
            issue = f"{invalid_gender_count:,} persons have invalid gender concepts"
            demographic_issues.append(issue)
            logging.warning(ColoredFormatter.warning(f"⚠️ {issue}"))
        
        # Check for invalid year of birth
        birth_year_sql = """
        SELECT COUNT(*) 
        FROM omop.person
        WHERE year_of_birth < 1900 OR year_of_birth > EXTRACT(YEAR FROM CURRENT_DATE)
        """
        
        result = execute_query(birth_year_sql, fetch=True)
        invalid_birth_year_count = result[0][0] if result else 0
        
        if invalid_birth_year_count > 0:
            issue = f"{invalid_birth_year_count:,} persons have invalid years of birth"
            demographic_issues.append(issue)
            logging.warning(ColoredFormatter.warning(f"⚠️ {issue}"))
        
        # Check for missing race or ethnicity
        race_ethnicity_sql = """
        SELECT COUNT(*) 
        FROM omop.person
        WHERE race_concept_id = 0 OR ethnicity_concept_id = 0
        """
        
        result = execute_query(race_ethnicity_sql, fetch=True)
        missing_race_ethnicity_count = result[0][0] if result else 0
        
        if missing_race_ethnicity_count > 0:
            issue = f"{missing_race_ethnicity_count:,} persons have missing race or ethnicity"
            demographic_issues.append(issue)
            logging.warning(ColoredFormatter.warning(f"⚠️ {issue}"))
        
        if demographic_issues:
            validation_results["demographics"] = {
                "status": "WARNING",
                "message": "Demographic issues found",
                "details": demographic_issues
            }
        else:
            logging.info(ColoredFormatter.success("✅ All demographic checks passed"))
            validation_results["demographics"] = {
                "status": "PASS",
                "message": "All demographic checks passed",
                "details": []
            }
        
        current_progress += validation_checks[3]["weight"]
        progress_tracker.update_progress("ETL", step_name, current_progress, total_items=total_weight, 
                                      message="Completed demographics check")
        
        # 5) Check concept mapping
        logging.info("Checking concept mapping...")
        progress_tracker.update_progress("ETL", step_name, current_progress, total_items=total_weight, 
                                      message="Checking concept mapping")
        
        concept_mapping_checks = [
            {
                "table": "condition_occurrence",
                "concept_column": "condition_concept_id"
            },
            {
                "table": "procedure_occurrence",
                "concept_column": "procedure_concept_id"
            },
            {
                "table": "drug_exposure",
                "concept_column": "drug_concept_id"
            },
            {
                "table": "measurement",
                "concept_column": "measurement_concept_id"
            },
            {
                "table": "observation",
                "concept_column": "observation_concept_id"
            }
        ]
        
        mapping_issues = []
        
        for check in concept_mapping_checks:
            table = check["table"]
            concept_column = check["concept_column"]
            
            # Check for unmapped concepts (concept_id = 0)
            sql = f"""
            SELECT COUNT(*) as total,
                   SUM(CASE WHEN {concept_column} = 0 THEN 1 ELSE 0 END) as unmapped
            FROM omop.{table}
            """
            
            result = execute_query(sql, fetch=True)
            total = result[0][0] if result else 0
            unmapped = result[0][1] if result else 0
            
            if total > 0:
                unmapped_percent = (unmapped / total) * 100
                
                if unmapped_percent > 20:  # More than 20% unmapped is a failure
                    issue = f"{unmapped:,} of {total:,} records ({unmapped_percent:.1f}%) in {table} have unmapped {concept_column}"
                    mapping_issues.append(issue)
                    logging.warning(ColoredFormatter.warning(f"⚠️ {issue}"))
        
        if mapping_issues:
            validation_results["concept_mapping"] = {
                "status": "FAIL",
                "message": "Concept mapping issues found",
                "details": mapping_issues
            }
            all_checks_passed = False
        else:
            logging.info(ColoredFormatter.success("✅ All concept mapping checks passed"))
            validation_results["concept_mapping"] = {
                "status": "PASS",
                "message": "All concept mapping checks passed",
                "details": []
            }
        
        current_progress += validation_checks[4]["weight"]
        progress_tracker.update_progress("ETL", step_name, current_progress, total_items=total_weight, 
                                      message="Completed concept mapping check")
        
        # Save validation results to a file
        validation_dir = Path(__file__).parent / "validation_results"
        validation_dir.mkdir(exist_ok=True)
        
        timestamp = time.strftime("%Y%m%d_%H%M%S")
        validation_file = validation_dir / f"validation_results_{timestamp}.json"
        
        with open(validation_file, 'w') as f:
            json.dump(validation_results, f, indent=2)
        
        end_time = time.time()
        total_time = end_time - start_time
        
        if all_checks_passed:
            logging.info(ColoredFormatter.success(
                f"✅ All validation checks passed in {total_time:.2f} sec. " +
                f"Results saved to {validation_file}"
            ))
        else:
            logging.warning(ColoredFormatter.warning(
                f"⚠️ Some validation checks failed in {total_time:.2f} sec. " +
                f"Results saved to {validation_file}"
            ))
        
        # Mark completion in checkpoint system
        mark_step_completed(step_name, {
            "validation_results": validation_results,
            "all_checks_passed": all_checks_passed,
            "processing_time_sec": total_time
        })
        
        # Update ETL progress tracker with completion status
        progress_tracker.complete_step("ETL", step_name, all_checks_passed, 
                                    f"Validation {'successful' if all_checks_passed else 'completed with issues'}")
        
        return all_checks_passed
        
    except Exception as e:
        error_msg = f"Error validating ETL results: {e}"
        logging.error(ColoredFormatter.error(f"❌ {error_msg}"))
        
        # Update ETL progress tracker with error
        progress_tracker.complete_step("ETL", step_name, False, error_msg)
        
        return False

if __name__ == "__main__":
    # This allows the module to be run directly for testing
    import argparse
    
    parser = argparse.ArgumentParser(description="Validate ETL results in OMOP CDM")
    parser.add_argument("--force", action="store_true", help="Force revalidation even if already completed")
    parser.add_argument("--debug", action="store_true", help="Enable debug logging")
    
    args = parser.parse_args()
    
    from etl_setup import init_logging, init_db_connection_pool
    
    init_logging(debug=args.debug)
    init_db_connection_pool()
    
    success = validate_etl_results(args.force)
    sys.exit(0 if success else 1)
