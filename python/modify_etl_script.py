#!/usr/bin/env python3
"""
modify_etl_script.py - Helper script to modify run_simplified_etl.sh for domain selection

This script modifies the run_simplified_etl.sh script to add domain selection capabilities,
allowing the ETL process to selectively process specific data domains.
"""

import os
import sys
import re
import shutil
from pathlib import Path

# Define project root
PROJECT_ROOT = Path(__file__).parent.parent
ORIGINAL_SCRIPT = PROJECT_ROOT / "bash" / "run_simplified_etl.sh"
MODIFIED_SCRIPT = PROJECT_ROOT / "bash" / "run_domain_specific_etl.sh"

# Available domains in the ETL process
DOMAINS = [
    "patients",
    "encounters",
    "conditions",
    "medications",
    "procedures",
    "observations",
    "observation_periods",
    "concept_mapping"
]

def backup_original_script():
    """Create a backup of the original script."""
    backup_path = ORIGINAL_SCRIPT.with_suffix(".sh.bak")
    shutil.copy2(ORIGINAL_SCRIPT, backup_path)
    print(f"Backup created at {backup_path}")

def add_domain_options(content):
    """Add domain selection options to the script."""
    # Define new command line options for domain selection
    domain_options = """
# Domain selection options
PROCESS_PATIENTS=true
PROCESS_ENCOUNTERS=true
PROCESS_CONDITIONS=true
PROCESS_MEDICATIONS=true
PROCESS_PROCEDURES=true
PROCESS_OBSERVATIONS=true
PROCESS_OBSERVATION_PERIODS=true
PROCESS_CONCEPT_MAPPING=true

"""
    
    # Add domain options after the existing options
    options_section = re.search(r"# Default values for command line arguments.*?# Parse command line arguments", 
                               content, re.DOTALL)
    
    if options_section:
        start, end = options_section.span()
        content = content[:end-1] + domain_options + content[end-1:]
    
    return content

def add_domain_parsing(content):
    """Add command line parsing for domain options."""
    # Define new command line parsing for domain options
    domain_parsing = """
    # Domain selection options
    --patients)
        PROCESS_PATIENTS=true
        shift
        ;;
    --no-patients)
        PROCESS_PATIENTS=false
        shift
        ;;
    --encounters)
        PROCESS_ENCOUNTERS=true
        shift
        ;;
    --no-encounters)
        PROCESS_ENCOUNTERS=false
        shift
        ;;
    --conditions)
        PROCESS_CONDITIONS=true
        shift
        ;;
    --no-conditions)
        PROCESS_CONDITIONS=false
        shift
        ;;
    --medications)
        PROCESS_MEDICATIONS=true
        shift
        ;;
    --no-medications)
        PROCESS_MEDICATIONS=false
        shift
        ;;
    --procedures)
        PROCESS_PROCEDURES=true
        shift
        ;;
    --no-procedures)
        PROCESS_PROCEDURES=false
        shift
        ;;
    --observations)
        PROCESS_OBSERVATIONS=true
        shift
        ;;
    --no-observations)
        PROCESS_OBSERVATIONS=false
        shift
        ;;
    --observation-periods)
        PROCESS_OBSERVATION_PERIODS=true
        shift
        ;;
    --no-observation-periods)
        PROCESS_OBSERVATION_PERIODS=false
        shift
        ;;
    --concept-mapping)
        PROCESS_CONCEPT_MAPPING=true
        shift
        ;;
    --no-concept-mapping)
        PROCESS_CONCEPT_MAPPING=false
        shift
        ;;
    --all-domains)
        PROCESS_PATIENTS=true
        PROCESS_ENCOUNTERS=true
        PROCESS_CONDITIONS=true
        PROCESS_MEDICATIONS=true
        PROCESS_PROCEDURES=true
        PROCESS_OBSERVATIONS=true
        PROCESS_OBSERVATION_PERIODS=true
        PROCESS_CONCEPT_MAPPING=true
        shift
        ;;
    --core-domains)
        PROCESS_PATIENTS=true
        PROCESS_ENCOUNTERS=true
        PROCESS_CONDITIONS=true
        PROCESS_MEDICATIONS=false
        PROCESS_PROCEDURES=false
        PROCESS_OBSERVATIONS=false
        PROCESS_OBSERVATION_PERIODS=false
        PROCESS_CONCEPT_MAPPING=false
        shift
        ;;
    --no-domains)
        PROCESS_PATIENTS=false
        PROCESS_ENCOUNTERS=false
        PROCESS_CONDITIONS=false
        PROCESS_MEDICATIONS=false
        PROCESS_PROCEDURES=false
        PROCESS_OBSERVATIONS=false
        PROCESS_OBSERVATION_PERIODS=false
        PROCESS_CONCEPT_MAPPING=false
        shift
        ;;"""
    
    # Find the end of the existing option parsing
    parsing_section = re.search(r"while \[ \$# -gt 0 \]; do.*?esac", content, re.DOTALL)
    
    if parsing_section:
        # Find the position of the last option before 'esac'
        last_option_pos = content.rfind(";;", 0, parsing_section.end())
        if last_option_pos > 0:
            # Insert new options after the last existing option
            content = content[:last_option_pos+2] + domain_parsing + content[last_option_pos+2:]
    
    return content

def add_domain_conditionals(content):
    """Add conditional processing for each domain."""
    # Add conditional processing for patients
    patients_section = re.search(r"# Step \d+: Process patients.*?fi\n\n", content, re.DOTALL)
    if patients_section:
        start, end = patients_section.span()
        modified_section = f"# Step X: Process patients\nif [ \"$PROCESS_PATIENTS\" = true ]; then\n{content[start+len('# Step X: Process patients'):end]}"
        content = content[:start] + modified_section + content[end:]
    
    # Add conditional processing for encounters
    encounters_section = re.search(r"# Step \d+: Process encounters.*?fi\n\n", content, re.DOTALL)
    if encounters_section:
        start, end = encounters_section.span()
        modified_section = f"# Step X: Process encounters\nif [ \"$PROCESS_ENCOUNTERS\" = true ]; then\n{content[start+len('# Step X: Process encounters'):end]}"
        content = content[:start] + modified_section + content[end:]
    
    # Add conditional processing for conditions
    conditions_section = re.search(r"# Step \d+: Process conditions.*?fi\n\n", content, re.DOTALL)
    if conditions_section:
        start, end = conditions_section.span()
        modified_section = f"# Step X: Process conditions\nif [ \"$PROCESS_CONDITIONS\" = true ]; then\n{content[start+len('# Step X: Process conditions'):end]}"
        content = content[:start] + modified_section + content[end:]
    
    # Add conditional processing for medications
    medications_section = re.search(r"# Step \d+: Process medications.*?fi\n\n", content, re.DOTALL)
    if medications_section:
        start, end = medications_section.span()
        modified_section = f"# Step X: Process medications\nif [ \"$PROCESS_MEDICATIONS\" = true ]; then\n{content[start+len('# Step X: Process medications'):end]}"
        content = content[:start] + modified_section + content[end:]
    
    # Add conditional processing for procedures
    procedures_section = re.search(r"# Step \d+: Process procedures.*?fi\n\n", content, re.DOTALL)
    if procedures_section:
        start, end = procedures_section.span()
        modified_section = f"# Step X: Process procedures\nif [ \"$PROCESS_PROCEDURES\" = true ]; then\n{content[start+len('# Step X: Process procedures'):end]}"
        content = content[:start] + modified_section + content[end:]
    
    # Add conditional processing for observations
    observations_section = re.search(r"# Step \d+: Process observations.*?fi\n\n", content, re.DOTALL)
    if observations_section:
        start, end = observations_section.span()
        modified_section = f"# Step X: Process observations\nif [ \"$PROCESS_OBSERVATIONS\" = true ]; then\n{content[start+len('# Step X: Process observations'):end]}"
        content = content[:start] + modified_section + content[end:]
    
    # Add conditional processing for observation periods
    observation_periods_section = re.search(r"# Step \d+: Create observation periods.*?fi\n\n", content, re.DOTALL)
    if observation_periods_section:
        start, end = observation_periods_section.span()
        modified_section = f"# Step X: Create observation periods\nif [ \"$PROCESS_OBSERVATION_PERIODS\" = true ]; then\n{content[start+len('# Step X: Create observation periods'):end]}"
        content = content[:start] + modified_section + content[end:]
    
    # Add conditional processing for concept mapping
    concept_mapping_section = re.search(r"# Step \d+: Map source to standard concepts.*?fi\n\n", content, re.DOTALL)
    if concept_mapping_section:
        start, end = concept_mapping_section.span()
        modified_section = f"# Step X: Map source to standard concepts\nif [ \"$PROCESS_CONCEPT_MAPPING\" = true ]; then\n{content[start+len('# Step X: Map source to standard concepts'):end]}"
        content = content[:start] + modified_section + content[end:]
    
    return content

def update_help_text(content):
    """Update the help text to include domain selection options."""
    help_text = """  Domain selection options:
    --patients                Process patient data (default: true)
    --no-patients             Skip patient data processing
    --encounters              Process encounter data (default: true)
    --no-encounters           Skip encounter data processing
    --conditions              Process condition data (default: true)
    --no-conditions           Skip condition data processing
    --medications             Process medication data (default: true)
    --no-medications          Skip medication data processing
    --procedures              Process procedure data (default: true)
    --no-procedures           Skip procedure data processing
    --observations            Process observation data (default: true)
    --no-observations         Skip observation data processing
    --observation-periods     Create observation periods (default: true)
    --no-observation-periods  Skip observation period creation
    --concept-mapping         Perform concept mapping (default: true)
    --no-concept-mapping      Skip concept mapping
    --all-domains             Process all domains (default)
    --core-domains            Process only core domains (patients, encounters, conditions)
    --no-domains              Skip all domains (useful with --force-restart to reset checkpoints)
"""
    
    # Find the help text section
    help_section = re.search(r"print_help\(\) \{.*?}", content, re.DOTALL)
    
    if help_section:
        # Find the position to insert the new help text (before the closing brace)
        help_end = content.rfind("}", 0, help_section.end())
        if help_end > 0:
            # Insert the new help text
            content = content[:help_end] + help_text + content[help_end:]
    
    return content

def update_script_header(content):
    """Update the script header to indicate domain selection capability."""
    header = """#!/bin/bash
#
# run_domain_specific_etl.sh - Domain-specific ETL script for Synthea to OMOP
#
# This script is a modified version of run_simplified_etl.sh that supports
# selective processing of specific data domains.
#
# Usage: ./run_domain_specific_etl.sh [options]
#
# See --help for available options
#
"""
    
    # Replace the existing header
    header_end = content.find("# Default values for command line arguments")
    if header_end > 0:
        content = header + content[header_end:]
    
    return content

def modify_script():
    """Modify the ETL script to add domain selection capabilities."""
    # Read the original script
    with open(ORIGINAL_SCRIPT, 'r') as f:
        content = f.read()
    
    # Make modifications
    content = update_script_header(content)
    content = add_domain_options(content)
    content = add_domain_parsing(content)
    content = update_help_text(content)
    content = add_domain_conditionals(content)
    
    # Write the modified script
    with open(MODIFIED_SCRIPT, 'w') as f:
        f.write(content)
    
    # Make the new script executable
    os.chmod(MODIFIED_SCRIPT, 0o755)
    
    print(f"Modified script created at {MODIFIED_SCRIPT}")

def main():
    """Main function."""
    print("Modifying ETL script to add domain selection capabilities...")
    
    # Check if the original script exists
    if not ORIGINAL_SCRIPT.exists():
        print(f"Error: Original script not found at {ORIGINAL_SCRIPT}")
        return 1
    
    # Create a backup of the original script
    backup_original_script()
    
    # Modify the script
    modify_script()
    
    print("Script modification completed successfully!")
    print(f"You can now use {MODIFIED_SCRIPT} with domain selection options.")
    print("Example usage:")
    print(f"  {MODIFIED_SCRIPT} --core-domains")
    print(f"  {MODIFIED_SCRIPT} --no-medications --no-procedures")
    
    return 0

if __name__ == "__main__":
    sys.exit(main())
