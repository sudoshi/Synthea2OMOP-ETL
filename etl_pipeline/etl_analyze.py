#!/usr/bin/env python3
"""
etl_analyze.py - Analyze OMOP tables to update PostgreSQL statistics.
This module runs ANALYZE on OMOP tables to ensure the query planner has up-to-date statistics.
"""

import os
import logging
import time
import sys
from pathlib import Path
from typing import Dict, Any, Optional, List

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

def analyze_tables(force_reprocess: bool = False) -> bool:
    """
    Analyze OMOP tables to update PostgreSQL statistics.
    
    Args:
        force_reprocess: Whether to force reprocessing even if already completed
        
    Returns:
        bool: True if processing was successful, False otherwise
    """
    step_name = "analyze_tables"
    if is_step_completed(step_name, force_reprocess):
        logging.info(ColoredFormatter.info("✅ Tables were previously analyzed. Skipping."))
        return True
    
    logging.info(ColoredFormatter.info("\n🔍 Analyzing OMOP tables..."))
    
    # Initialize progress tracker
    progress_tracker = ETLProgressTracker()
    progress_tracker.start_step("ETL", step_name, message="Starting table analysis")
    
    # List of tables to analyze
    tables_to_analyze = [
        "person",
        "visit_occurrence",
        "condition_occurrence",
        "procedure_occurrence",
        "drug_exposure",
        "measurement",
        "observation",
        "observation_period"
    ]
    
    total_tables = len(tables_to_analyze)
    logging.info(f"Analyzing {total_tables} tables...")
    
    # Update progress tracker with total tables
    progress_tracker.update_progress("ETL", step_name, 0, total_items=total_tables, 
                                   message=f"Analyzing {total_tables} tables")
    
    start_time = time.time()
    
    try:
        # Create progress bar
        progress_bar = create_progress_bar(total_tables, "Analyzing Tables")
        
        # Analyze each table
        for i, table in enumerate(tables_to_analyze):
            table_name = f"omop.{table}"
            logging.info(f"Analyzing {table_name}...")
            
            # Run ANALYZE on the table
            execute_query(f"ANALYZE {table_name}")
            
            # Update progress
            update_progress_bar(progress_bar, 1)
            progress_tracker.update_progress("ETL", step_name, i + 1, total_items=total_tables, 
                                          message=f"Analyzed {i + 1} of {total_tables} tables")
        
        close_progress_bar(progress_bar)
        
        # Get table statistics
        table_stats = []
        for table in tables_to_analyze:
            table_name = f"omop.{table}"
            stats_query = f"""
            SELECT 
                relname AS table_name,
                n_live_tup AS row_count,
                pg_size_pretty(pg_total_relation_size('{table_name}')) AS total_size
            FROM pg_stat_user_tables
            WHERE relname = '{table}'
            """
            
            result = execute_query(stats_query, fetch=True)
            if result:
                table_stats.append({
                    "table_name": result[0][0],
                    "row_count": result[0][1],
                    "total_size": result[0][2]
                })
        
        end_time = time.time()
        total_time = end_time - start_time
        
        # Format table statistics for logging
        stats_str = "\n".join([
            f"  - {stat['table_name']}: {stat['row_count']:,} rows, {stat['total_size']}"
            for stat in table_stats
        ])
        
        logging.info(ColoredFormatter.success(
            f"✅ Successfully analyzed {total_tables} tables in {total_time:.2f} sec\n"
            f"Table statistics:\n{stats_str}"
        ))
        
        # Mark completion in checkpoint system
        mark_step_completed(step_name, {
            "tables_analyzed": total_tables,
            "table_stats": table_stats,
            "processing_time_sec": total_time
        })
        
        # Update ETL progress tracker with completion status
        progress_tracker.complete_step("ETL", step_name, True, 
                                    f"Successfully analyzed {total_tables} tables")
        
        return True
        
    except Exception as e:
        error_msg = f"Error analyzing tables: {e}"
        logging.error(ColoredFormatter.error(f"❌ {error_msg}"))
        
        # Update ETL progress tracker with error
        progress_tracker.complete_step("ETL", step_name, False, error_msg)
        
        return False

if __name__ == "__main__":
    # This allows the module to be run directly for testing
    import argparse
    
    parser = argparse.ArgumentParser(description="Analyze OMOP tables to update PostgreSQL statistics")
    parser.add_argument("--force", action="store_true", help="Force reprocessing even if already completed")
    parser.add_argument("--debug", action="store_true", help="Enable debug logging")
    
    args = parser.parse_args()
    
    from etl_setup import init_logging, init_db_connection_pool
    
    init_logging(debug=args.debug)
    init_db_connection_pool()
    
    success = analyze_tables(args.force)
    sys.exit(0 if success else 1)
