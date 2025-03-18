#!/usr/bin/env python3
import os
import csv
import re
import sys
from pathlib import Path
import shutil
from tqdm import tqdm

def fix_header(file_path):
    """
    Fix the header of a vocabulary file by adding tab separators between column names.
    
    Args:
        file_path: Path to the vocabulary file
    
    Returns:
        True if the header was fixed, False otherwise
    """
    with open(file_path, 'r', encoding='utf-8', errors='replace') as f:
        first_line = f.readline().strip()
    
    # Check if the header needs fixing (no tabs)
    if '\t' not in first_line:
        # Try to identify column names based on common OMOP vocabulary patterns
        # First try to match snake_case patterns like domain_id, domain_name, etc.
        snake_case_pattern = r'([a-z]+_[a-z_]+)'
        snake_case_columns = re.findall(snake_case_pattern, first_line)
        
        # If we found snake_case columns, use them
        if snake_case_columns:
            columns = snake_case_columns
        else:
            # Otherwise, try the original pattern for camelCase
            columns = re.findall(r'([a-z_]+)(?=[A-Z]|$)', first_line)
        
        # If we found columns with either method
        if columns:
            # Read the entire file
            with open(file_path, 'r', encoding='utf-8', errors='replace') as f:
                content = f.read()
            
            # Replace the header
            new_header = '\t'.join(columns)
            new_content = content.replace(first_line, new_header, 1)
            
            # Write the file back
            with open(file_path, 'w', encoding='utf-8') as f:
                f.write(new_content)
            
            return True
        
        # If we couldn't identify columns automatically, try a manual approach for known files
        known_headers = {
            'DOMAIN.csv': "domain_id\tdomain_name\tdomain_concept_id",
            'CONCEPT_CLASS.csv': "concept_class_id\tconcept_class_name\tconcept_class_concept_id",
            'VOCABULARY.csv': "vocabulary_id\tvocabulary_name\tvocabulary_reference\tvocabulary_version\tvocabulary_concept_id",
            'RELATIONSHIP.csv': "relationship_id\trelationship_name\tis_hierarchical\tdefines_ancestry\treverse_relationship_id\trelationship_concept_id",
            'CONCEPT.csv': "concept_id\tconcept_name\tdomain_id\tvocabulary_id\tconcept_class_id\tstandard_concept\tconcept_code\tvalid_start_date\tvalid_end_date\tinvalid_reason",
            'CONCEPT_RELATIONSHIP.csv': "concept_id_1\tconcept_id_2\trelationship_id\tvalid_start_date\tvalid_end_date\tinvalid_reason",
            'CONCEPT_SYNONYM.csv': "concept_id\tconcept_synonym_name\tlanguage_concept_id",
            'CONCEPT_ANCESTOR.csv': "ancestor_concept_id\tdescendant_concept_id\tmin_levels_of_separation\tmax_levels_of_separation",
            'DRUG_STRENGTH.csv': "drug_concept_id\tingredient_concept_id\tamount_value\tamount_unit_concept_id\tnumerator_value\tnumerator_unit_concept_id\tdenominator_value\tdenominator_unit_concept_id\tbox_size\tvalid_start_date\tvalid_end_date\tinvalid_reason"
        }
        
        filename = os.path.basename(file_path)
        if filename in known_headers:
            with open(file_path, 'r', encoding='utf-8', errors='replace') as f:
                content = f.read()
            
            # Manually set the header for the known file
            new_header = known_headers[filename]
            new_content = content.replace(first_line, new_header, 1)
            
            with open(file_path, 'w', encoding='utf-8') as f:
                f.write(new_content)
            
            return True
    
    return False

def clean_vocabulary_file(input_file, output_file):
    """
    Clean and prepare a vocabulary CSV file for PostgreSQL import.
    
    Args:
        input_file: Path to the original vocabulary file
        output_file: Path where the cleaned file will be saved
    """
    print(f"Processing {os.path.basename(input_file)}...")
    
    # Create a temporary copy to work with
    temp_file = f"{input_file}.temp"
    shutil.copy2(input_file, temp_file)
    
    # Fix the header if needed
    fix_header(temp_file)
    
    # Count lines for progress bar
    line_count = sum(1 for _ in open(temp_file, 'r', encoding='utf-8', errors='replace'))
    
    # Process the file line by line
    with open(temp_file, 'r', encoding='utf-8', errors='replace') as infile, \
         open(output_file, 'w', encoding='utf-8', newline='') as outfile:
        
        # Read the header
        header = infile.readline().strip()
        outfile.write(header + '\n')
        
        # Process each line with a progress bar
        for line in tqdm(infile, total=line_count-1, desc=f"Cleaning {os.path.basename(input_file)}"):
            if not line.strip():
                continue
            
            # Replace problematic characters
            cleaned_line = line
            
            # 1. Handle inch marks (e.g., 22")
            cleaned_line = re.sub(r'(\d+)"', r'\1 inch', cleaned_line)
            
            # 2. Replace unescaped quotes with escaped quotes
            # This regex looks for quotes that are not at the beginning or end of a field
            cleaned_line = re.sub(r'(?<!\t)"(?!\t|\n|$)', "'", cleaned_line)
            
            # 3. Remove any carriage returns or newlines within fields, but preserve the ending newline
            cleaned_line = cleaned_line.replace('\r', ' ')
            
            # Make sure the line ends with a newline
            if not cleaned_line.endswith('\n'):
                cleaned_line = cleaned_line + '\n'
            
            outfile.write(cleaned_line)
    
    # Clean up the temporary file
    os.remove(temp_file)
    
    print(f"Completed processing {os.path.basename(input_file)} -> {os.path.basename(output_file)}")

def process_all_vocabulary_files(input_dir, output_dir):
    """Process all vocabulary CSV files in the input directory."""
    os.makedirs(output_dir, exist_ok=True)
    
    # Get all CSV files
    csv_files = [f for f in os.listdir(input_dir) if f.endswith('.csv')]
    
    print(f"Found {len(csv_files)} CSV files to process")
    
    for filename in csv_files:
        input_path = os.path.join(input_dir, filename)
        output_path = os.path.join(output_dir, filename)
        
        # Skip if the output file already exists and is newer than the input file
        if os.path.exists(output_path) and os.path.getmtime(output_path) > os.path.getmtime(input_path):
            print(f"Skipping {filename} (already processed)")
            continue
        
        clean_vocabulary_file(input_path, output_path)
    
    # Copy non-CSV files
    for filename in os.listdir(input_dir):
        if not filename.endswith('.csv'):
            src = os.path.join(input_dir, filename)
            dst = os.path.join(output_dir, filename)
            if os.path.isfile(src):
                shutil.copy2(src, dst)
                print(f"Copied {filename}")

def main():
    if len(sys.argv) < 3:
        print("Usage: python clean_vocab.py <input_dir> <output_dir>")
        sys.exit(1)
    
    input_dir = sys.argv[1]
    output_dir = sys.argv[2]
    
    if not os.path.isdir(input_dir):
        print(f"Error: Input directory '{input_dir}' does not exist")
        sys.exit(1)
    
    process_all_vocabulary_files(input_dir, output_dir)
    print("All vocabulary files processed successfully")

if __name__ == "__main__":
    main()
