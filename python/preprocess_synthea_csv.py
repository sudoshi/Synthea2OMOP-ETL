#!/usr/bin/env python3
"""
preprocess_synthea_csv.py - Preprocess Synthea CSV files for proper CSV formatting

This script fixes Synthea's malformed CSV files by:
1. Reading the header row to identify columns
2. Processing data rows that have values concatenated without separators
3. Writing properly formatted CSV files that can be loaded into the database

Usage:
  python preprocess_synthea_csv.py [--input-dir INPUT_DIR] [--output-dir OUTPUT_DIR] [--file FILE]
  
Options:
  --input-dir INPUT_DIR    Directory containing Synthea output files (default: ./synthea-output)
  --output-dir OUTPUT_DIR  Directory to write processed files (default: ./synthea-processed)
  --file FILE              Process only a specific file (optional)
  --overwrite              Overwrite existing processed files
  --debug                  Enable debug logging
  --no-progress-bar        Disable progress bars
  --no-fast                Disable fast mode (use exact line counting)
"""

import argparse
import csv
import logging
import os
import re
import sys
import time
import subprocess
from typing import List, Dict, Optional, Tuple
from tqdm import tqdm

# Set up logging
log_dir = "logs"
os.makedirs(log_dir, exist_ok=True)
log_file = os.path.join(log_dir, f"preprocess_synthea_{time.strftime('%Y%m%d_%H%M%S')}.log")
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler(log_file),
        # Only log to file, not to console (tqdm will handle console output)
    ]
)
logger = logging.getLogger(__name__)

# Global variable to track overall progress
overall_progress = None
current_file_progress = None

def parse_args():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(description='Preprocess Synthea CSV files')
    parser.add_argument('--input-dir', type=str, default='./synthea-output',
                        help='Directory containing Synthea output files (default: ./synthea-output)')
    parser.add_argument('--output-dir', type=str, default='./synthea-processed',
                        help='Directory to write processed files (default: ./synthea-processed)')
    parser.add_argument('--file', type=str, help='Process only a specific file')
    parser.add_argument('--overwrite', action='store_true', help='Overwrite existing processed files')
    parser.add_argument('--debug', action='store_true', help='Enable debug logging')
    parser.add_argument('--no-progress-bar', action='store_true', help='Disable progress bars')
    parser.add_argument('--no-fast', action='store_true', help='Disable fast mode (use exact line counting)')
    
    return parser.parse_args()

def setup_logging(debug=False):
    """Set up logging with appropriate level."""
    if debug:
        logging.getLogger().setLevel(logging.DEBUG)
        logger.debug("Debug logging enabled")
    else:
        logging.getLogger().setLevel(logging.INFO)

def count_lines_in_file_fast(file_path):
    """Count the number of lines in a file using wc -l (much faster for large files)."""
    try:
        # Use wc -l to count lines instead of reading the whole file
        result = subprocess.run(['wc', '-l', file_path], 
                                capture_output=True, text=True, check=True)
        # Extract the number from the result
        line_count = int(result.stdout.strip().split()[0])
        return line_count
    except (subprocess.SubprocessError, ValueError, IndexError):
        # Fall back to slower method if subprocess fails
        try:
            with open(file_path, 'r') as f:
                # Use a buffer-based counting approach
                bufgen = takewhile(lambda x: x, (f.read(1024*1024) for _ in repeat(None)))
                line_count = sum(buf.count('\n') for buf in bufgen)
                return line_count
        except Exception as e:
            logger.error(f"Error counting lines in {file_path}: {e}")
            # Provide an estimate based on file size (1KB ~ 20 lines for typical CSV)
            try:
                file_size = os.path.getsize(file_path)
                estimated_lines = file_size // 50  # Rough estimate
                logger.warning(f"Using estimated line count: ~{estimated_lines}")
                return estimated_lines
            except:
                # If all else fails, return a default value
                return 1000

def identify_csv_files(input_dir, specific_file=None):
    """Identify CSV files in the input directory."""
    logger.info(f"Identifying CSV files in {input_dir}")

    csv_files = {}

    try:
        # Check if directory exists
        if not os.path.isdir(input_dir):
            logger.error(f"Directory not found: {input_dir}")
            return None

        # List all CSV files
        for filename in os.listdir(input_dir):
            if specific_file and filename != specific_file:
                continue

            if filename.endswith('.csv'):
                file_path = os.path.join(input_dir, filename)
                csv_files[filename] = file_path
                logger.debug(f"Found CSV file: {filename}")

        if not csv_files:
            logger.warning(f"No CSV files found in {input_dir}")
            return None

        logger.info(f"Found {len(csv_files)} CSV files to process")
        return csv_files
    except Exception as e:
        logger.error(f"Error identifying CSV files: {e}")
        return None

def analyze_csv_header(csv_file):
    """
    Analyze CSV header to get column names and expected data types.
    Returns: (header_line, column_names)
    """
    try:
        with open(csv_file, 'r') as f:
            # Read the first line as header
            header_line = f.readline().strip()

            # Split by commas to get column names
            column_names = header_line.split(',')

            logger.debug(f"Analyzed header with {len(column_names)} columns")
            return header_line, column_names
    except Exception as e:
        logger.error(f"Error analyzing CSV header for {csv_file}: {e}")
        return None, None

def parse_malformed_row(row_text, column_count):
    """
    Parse a malformed row where values are not properly comma-separated.
    This is the core logic to handle Synthea's special format.
    
    Strategy:
    1. For rows that have proper commas, just use normal CSV parsing
    2. For malformed rows, try various heuristics:
       a. Look for common field patterns (UUIDs, ISO dates)
       b. Try to split the row based on expected field lengths
       c. Use regex pattern matching for known field formats
    """
    # If the row has the expected number of commas, it might be properly formatted
    commas_count = row_text.count(',')
    if commas_count == column_count - 1:
        return row_text.split(',')
    
    # If almost no commas, assume it's entirely malformed
    if commas_count < column_count / 3:
        # Start with some basic patterns we can identify
        parsed_fields = []
        remaining_text = row_text
        
        # First field is typically a UUID (36 chars + optional quotes)
        uuid_match = re.match(r'^([0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12})', remaining_text)
        if uuid_match:
            parsed_fields.append(uuid_match.group(1))
            remaining_text = remaining_text[len(uuid_match.group(1)):]
        
        # If we couldn't parse properly, use a simpler approach - divide evenly
        if len(parsed_fields) < column_count:
            # Calculate average field length for the remaining text
            remaining_length = len(remaining_text)
            remaining_fields = column_count - len(parsed_fields)
            avg_field_length = remaining_length // remaining_fields if remaining_fields > 0 else 0
            
            for i in range(remaining_fields):
                if i == remaining_fields - 1:
                    # Last field gets all remaining text
                    parsed_fields.append(remaining_text)
                else:
                    # Each field gets avg_field_length characters
                    field = remaining_text[:avg_field_length] if remaining_length > 0 else ""
                    parsed_fields.append(field)
                    remaining_text = remaining_text[avg_field_length:] if remaining_length > avg_field_length else ""
        
        return parsed_fields
    
    # If partial commas, try to parse as much as possible
    parts = row_text.split(',')
    if len(parts) < column_count:
        # Need to split the last part further
        last_part = parts[-1]
        remaining_fields = column_count - len(parts) + 1
        
        # Similar to above, divide evenly
        if remaining_fields > 1:
            remaining_length = len(last_part)
            avg_field_length = remaining_length // remaining_fields if remaining_fields > 0 else 0
            
            new_parts = []
            for i in range(remaining_fields):
                if i == remaining_fields - 1:
                    # Last field gets all remaining text
                    new_parts.append(last_part)
                else:
                    # Each field gets avg_field_length characters
                    field = last_part[:avg_field_length] if remaining_length > 0 else ""
                    new_parts.append(field)
                    last_part = last_part[avg_field_length:] if len(last_part) > avg_field_length else ""
            
            # Replace the last part with the new parts
            parts = parts[:-1] + new_parts
        
    return parts

def estimate_lines(file_path):
    """Estimate the number of lines in a file based on file size and sample lines"""
    try:
        # Get file size
        file_size = os.path.getsize(file_path)
        
        # Read a sample of the first 1000 lines to get average line length
        with open(file_path, 'r') as f:
            sample_lines = []
            for _ in range(min(1000, file_size // 50)):  # Read up to 1000 lines or fewer for small files
                line = f.readline()
                if not line:
                    break
                sample_lines.append(line)
        
        # Calculate average line length from non-empty lines
        non_empty_lines = [line for line in sample_lines if line.strip()]
        if non_empty_lines:
            avg_line_length = sum(len(line) for line in non_empty_lines) / len(non_empty_lines)
            # Estimate total lines
            estimated_total = max(len(sample_lines), int(file_size / max(1, avg_line_length)))
            return estimated_total
        else:
            # If sample is empty, make a conservative estimate
            return max(1000, file_size // 50)  # Assume 50 bytes per line as fallback
    except Exception as e:
        logger.error(f"Error estimating lines in {file_path}: {e}")
        # Default fallback estimate based on file size
        return max(1000, os.path.getsize(file_path) // 50)

def process_csv_file(input_file, output_file, overwrite=False, use_progress_bar=True, use_fast_mode=True):
    """Process a single CSV file, fixing formatting issues."""
    global current_file_progress
    
    try:
        # Check if output file already exists
        if os.path.exists(output_file) and not overwrite:
            logger.info(f"Output file {output_file} already exists, skipping (use --overwrite to force)")
            if overall_progress:
                overall_progress.write(f"Skipping {os.path.basename(input_file)} (already exists)")
            return False
        
        # Create output directory if it doesn't exist
        output_dir = os.path.dirname(output_file)
        os.makedirs(output_dir, exist_ok=True)
        
        # Analyze header
        header_line, column_names = analyze_csv_header(input_file)
        if not header_line or not column_names:
            logger.error(f"Failed to analyze header for {input_file}")
            return False
        
        # Get file size for progress reporting
        file_size = os.path.getsize(input_file)
        
        # Count or estimate lines for progress bar
        if use_fast_mode:
            # Use faster file size based estimation
            total_rows = estimate_lines(input_file)
            logger.info(f"Estimated {total_rows} rows in {os.path.basename(input_file)} ({file_size} bytes)")
        else:
            try:
                # Try fast line counting using wc -l
                total_rows = count_lines_in_file_fast(input_file)
                logger.info(f"Counted {total_rows} rows in {os.path.basename(input_file)}")
            except Exception as e:
                # Fall back to estimation if counting fails
                logger.warning(f"Line counting failed: {e}, using estimation")
                total_rows = estimate_lines(input_file)
                logger.info(f"Estimated {total_rows} rows in {os.path.basename(input_file)} ({file_size} bytes)")
        
        # Adjust for header
        data_rows = max(0, total_rows - 1)
        column_count = len(column_names)
        
        if overall_progress:
            overall_progress.write(f"Processing {os.path.basename(input_file)} with {column_count} columns, ~{data_rows} rows")
        
        # Create a progress bar for this file
        if use_progress_bar:
            file_basename = os.path.basename(input_file)
            current_file_progress = tqdm(
                total=data_rows,  # -1 for header
                desc=f"Processing {file_basename}",
                unit="rows",
                leave=False,
                position=1
            )
        
        # Process the file
        with open(input_file, 'r') as in_file, open(output_file, 'w', newline='') as out_file:
            csv_writer = csv.writer(out_file, quoting=csv.QUOTE_MINIMAL)
            
            # Write header row
            csv_writer.writerow(column_names)
            
            # Process data rows
            line_count = 0
            processed_count = 0
            last_progress_update = time.time()
            
            for line in in_file:
                line_count += 1
                
                # Skip header row
                if line_count == 1:
                    continue
                
                # Parse and write row
                row_data = parse_malformed_row(line.strip(), column_count)
                
                # Ensure we have the right number of columns
                if len(row_data) != column_count:
                    # Try to fix by padding or truncating
                    if len(row_data) < column_count:
                        # Pad with empty strings
                        row_data.extend([''] * (column_count - len(row_data)))
                    else:
                        # Truncate
                        row_data = row_data[:column_count]
                
                csv_writer.writerow(row_data)
                processed_count += 1
                
                # Update progress
                if current_file_progress:
                    current_file_progress.update(1)
                    
                    # Add a description to help shell script parse progress
                    # Only update at most every 2 seconds to avoid flooding stderr
                    current_time = time.time()
                    if current_time - last_progress_update > 2 or processed_count % 100000 == 0:
                        current_file_progress.set_postfix(progress=f"{processed_count}/{data_rows}")
                        # Write progress info that can be captured by the shell wrapper
                        print(f"progress: {processed_count}/{data_rows} Processing {os.path.basename(input_file)}", 
                              file=sys.stderr, flush=True)
                        last_progress_update = current_time
                
                # Also update overall progress
                if overall_progress:
                    overall_progress.update(1)
        
        if current_file_progress:
            current_file_progress.close()
            current_file_progress = None
            
        logger.info(f"Successfully processed {processed_count} rows from {os.path.basename(input_file)}")
        return True
    except Exception as e:
        logger.error(f"Error processing {input_file}: {e}")
        import traceback
        logger.error(traceback.format_exc())
        if current_file_progress:
            current_file_progress.close()
            current_file_progress = None
        return False

def main():
    """Main function."""
    global overall_progress
    
    # Parse arguments
    args = parse_args()
    
    # Set up logging
    setup_logging(args.debug)
    
    logger.info("Starting Synthea CSV preprocessing")
    
    # Get input files
    csv_files = identify_csv_files(args.input_dir, args.file)
    if not csv_files:
        logger.error("No CSV files found to process")
        return 1
    
    show_progress = not args.no_progress_bar
    use_fast_mode = not args.no_fast  # Fast mode is now default
    
    # Create overall progress bar
    if show_progress:
        # Calculate an approximate total by summing file sizes and dividing by average line size
        total_size = sum(os.path.getsize(fp) for fp in csv_files.values())
        estimated_total = total_size // 100  # Rough estimate assuming average line length of 100 bytes
        
        print(f"Using estimated total of approximately {estimated_total} rows", file=sys.stderr)
        
        overall_progress = tqdm(
            total=estimated_total,
            desc="Overall progress",
            unit="rows",
            position=0
        )
    
    # Process files
    success_count = 0
    failure_count = 0
    
    start_time = time.time()
    
    for filename, file_path in csv_files.items():
        output_path = os.path.join(args.output_dir, filename)
        
        if show_progress:
            # Update description before processing file
            overall_progress.set_description(f"Processing {filename}")
        
        success = process_csv_file(file_path, output_path, args.overwrite, show_progress, use_fast_mode)
        
        if success:
            success_count += 1
        else:
            failure_count += 1
    
    # Close overall progress bar
    if overall_progress:
        overall_progress.close()
    
    # Log summary
    end_time = time.time()
    duration = end_time - start_time
    
    summary = [
        "="*80,
        f"Preprocessing complete in {duration:.2f} seconds",
        f"Files processed successfully: {success_count}",
        f"Files failed: {failure_count}",
        f"Processed files are in: {args.output_dir}",
        "="*80
    ]
    
    # Log to file
    for line in summary:
        logger.info(line)
    
    # Print summary to console
    print("\n".join(summary), file=sys.stderr)
    
    return 0 if failure_count == 0 else 1

if __name__ == "__main__":
    # Add imports for fallback line counting method
    from itertools import takewhile, repeat
    sys.exit(main())
