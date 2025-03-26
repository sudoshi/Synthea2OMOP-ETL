#!/usr/bin/env python3
"""
manage_etl.py - Flexible management script for Synthea to OMOP ETL

This script provides a flexible interface to:
1. Select specific data domains to transform (patients, encounters, conditions, etc.)
2. Configure batch processing parameters for large datasets
3. Control the ETL process with detailed options
4. Monitor progress with detailed statistics
5. Manage database connections efficiently for large datasets

Usage:
  python manage_etl.py --help
  python manage_etl.py --interactive
  python manage_etl.py --domains patients,encounters,conditions --batch-size 50000
"""

import argparse
import logging
import os
import sys
import time
import json
import subprocess
from pathlib import Path
from typing import List, Dict, Any, Optional, Set
import configparser
import shutil
from datetime import datetime

# Set up logging
log_dir = "logs"
os.makedirs(log_dir, exist_ok=True)
log_file = os.path.join(log_dir, f"manage_etl_{time.strftime('%Y%m%d_%H%M%S')}.log")
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler(log_file),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

# Define project root for proper path references
PROJECT_ROOT = Path(__file__).parent.parent
CHECKPOINT_FILE = PROJECT_ROOT / ".etl_domain_checkpoint"

# Available data domains
AVAILABLE_DOMAINS = {
    "patients": {
        "description": "Patient demographic data",
        "source_file": "patients.csv",
        "target_table": "person",
        "dependencies": []
    },
    "encounters": {
        "description": "Healthcare encounters/visits",
        "source_file": "encounters.csv",
        "target_table": "visit_occurrence",
        "dependencies": ["patients"]
    },
    "conditions": {
        "description": "Patient conditions/diagnoses",
        "source_file": "conditions.csv",
        "target_table": "condition_occurrence",
        "dependencies": ["patients", "encounters"]
    },
    "medications": {
        "description": "Medication prescriptions and administrations",
        "source_file": "medications.csv",
        "target_table": "drug_exposure",
        "dependencies": ["patients", "encounters"]
    },
    "procedures": {
        "description": "Medical procedures performed",
        "source_file": "procedures.csv",
        "target_table": "procedure_occurrence",
        "dependencies": ["patients", "encounters"]
    },
    "observations": {
        "description": "Clinical observations and measurements",
        "source_file": "observations.csv",
        "target_table": "observation,measurement",
        "dependencies": ["patients", "encounters"]
    },
    "observation_periods": {
        "description": "Patient observation periods",
        "source_file": None,  # Generated from other data
        "target_table": "observation_period",
        "dependencies": ["patients", "encounters", "conditions", "medications", "procedures", "observations"]
    },
    "concept_mapping": {
        "description": "Map source concepts to standard concepts",
        "source_file": None,  # Uses vocabulary tables
        "target_table": "Multiple tables",
        "dependencies": ["patients", "encounters", "conditions", "medications", "procedures", "observations"]
    }
}

# ANSI color codes for terminal output
class Colors:
    HEADER = '\033[95m'
    BLUE = '\033[94m'
    GREEN = '\033[92m'
    YELLOW = '\033[93m'
    RED = '\033[91m'
    BOLD = '\033[1m'
    UNDERLINE = '\033[4m'
    END = '\033[0m'

def print_header(message: str):
    """Print a formatted header message."""
    print(f"\n{Colors.BOLD}{Colors.BLUE}{'=' * 70}{Colors.END}")
    print(f"{Colors.BOLD}{Colors.BLUE}{message.center(70)}{Colors.END}")
    print(f"{Colors.BOLD}{Colors.BLUE}{'=' * 70}{Colors.END}\n")

def print_success(message: str):
    """Print a success message."""
    print(f"{Colors.GREEN}{message}{Colors.END}")

def print_warning(message: str):
    """Print a warning message."""
    print(f"{Colors.YELLOW}{message}{Colors.END}")

def print_error(message: str):
    """Print an error message."""
    print(f"{Colors.RED}{message}{Colors.END}")

def parse_arguments():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(description='Flexible Synthea to OMOP ETL Management')
    
    # Domain selection options
    domain_help = "Comma-separated list of domains to process: " + ", ".join(AVAILABLE_DOMAINS.keys())
    parser.add_argument('--domains', type=str,
                        help=domain_help)
    parser.add_argument('--skip-domains', type=str,
                        help="Comma-separated list of domains to skip")
    parser.add_argument('--list-domains', action='store_true',
                        help='List available data domains and exit')
    parser.add_argument('--interactive', action='store_true',
                        help='Run in interactive mode to select domains')
    
    # Processing options
    parser.add_argument('--batch-size', type=int, default=50000,
                        help='Batch size for loading data (default: 50000)')
    parser.add_argument('--max-workers', type=int, default=4,
                        help='Maximum number of parallel workers (default: 4)')
    parser.add_argument('--single-connection', action='store_true',
                        help='Use a single database connection for related operations')
    parser.add_argument('--commit-frequency', type=int, default=10000,
                        help='How often to commit during batch operations (default: 10000)')
    
    # Directory options
    parser.add_argument('--synthea-dir', type=str, default=str(PROJECT_ROOT / 'synthea-output'),
                        help='Directory containing Synthea output files')
    parser.add_argument('--processed-dir', type=str, default=str(PROJECT_ROOT / 'synthea-processed'),
                        help='Directory to store processed Synthea files')
    
    # Control options
    parser.add_argument('--force-restart', action='store_true',
                        help='Force restart of ETL process, ignoring checkpoints')
    parser.add_argument('--force-load', action='store_true',
                        help='Force reload of staging data even if already loaded')
    parser.add_argument('--skip-preprocessing', action='store_true',
                        help='Skip preprocessing of Synthea CSV files')
    parser.add_argument('--skip-validation', action='store_true',
                        help='Skip validation steps')
    parser.add_argument('--disable-progress-bars', action='store_true',
                        help='Disable progress bars')
    parser.add_argument('--debug', action='store_true',
                        help='Enable debug logging')
    parser.add_argument('--dry-run', action='store_true',
                        help='Show what would be done without executing')
    
    return parser.parse_args()

def setup_logging(debug: bool = False):
    """Set up logging with appropriate level."""
    level = logging.DEBUG if debug else logging.INFO
    logger.setLevel(level)
    for handler in logger.handlers:
        handler.setLevel(level)

def list_available_domains():
    """List available data domains with descriptions."""
    print_header("AVAILABLE DATA DOMAINS")
    
    for domain, info in AVAILABLE_DOMAINS.items():
        deps = ", ".join(info["dependencies"]) if info["dependencies"] else "None"
        source = info["source_file"] if info["source_file"] else "Generated"
        print(f"{Colors.BOLD}{domain}{Colors.END}")
        print(f"  Description: {info['description']}")
        print(f"  Source file: {source}")
        print(f"  Target OMOP table(s): {info['target_table']}")
        print(f"  Dependencies: {deps}")
        print()

def interactive_domain_selection() -> List[str]:
    """Interactive selection of domains to process."""
    print_header("INTERACTIVE DOMAIN SELECTION")
    
    print("Select the domains you want to process:")
    selected_domains = []
    
    # First, show all available domains
    for i, (domain, info) in enumerate(AVAILABLE_DOMAINS.items(), 1):
        print(f"{i}. {domain} - {info['description']}")
    
    print("\nEnter domain numbers separated by commas, or:")
    print("- 'all' to select all domains")
    print("- 'core' for essential domains (patients, encounters, conditions)")
    print("- 'none' to cancel")
    
    choice = input("\nYour selection: ").strip().lower()
    
    if choice == 'all':
        selected_domains = list(AVAILABLE_DOMAINS.keys())
    elif choice == 'core':
        selected_domains = ['patients', 'encounters', 'conditions']
    elif choice == 'none':
        return []
    else:
        try:
            # Parse numbers and convert to domain names
            domain_indices = [int(x.strip()) for x in choice.split(',')]
            domain_list = list(AVAILABLE_DOMAINS.keys())
            for idx in domain_indices:
                if 1 <= idx <= len(domain_list):
                    selected_domains.append(domain_list[idx-1])
        except ValueError:
            print_error("Invalid selection. Please enter numbers separated by commas.")
            return interactive_domain_selection()
    
    # Show selected domains and confirm
    print("\nYou selected the following domains:")
    for domain in selected_domains:
        print(f"- {domain}")
    
    confirm = input("\nConfirm selection? (y/n): ").strip().lower()
    if confirm != 'y':
        return interactive_domain_selection()
    
    return selected_domains

def resolve_dependencies(selected_domains: List[str]) -> List[str]:
    """Resolve dependencies for selected domains and return ordered list."""
    resolved = []
    selected_set = set(selected_domains)
    
    # Helper function to add a domain and its dependencies
    def add_with_deps(domain):
        if domain in resolved:
            return  # Already processed
        
        # First add dependencies
        for dep in AVAILABLE_DOMAINS[domain]["dependencies"]:
            if dep not in resolved:
                # If dependency wasn't explicitly selected, warn user
                if dep not in selected_set:
                    print_warning(f"Adding required dependency '{dep}' for '{domain}'")
                    selected_set.add(dep)
                add_with_deps(dep)
        
        # Then add the domain itself
        resolved.append(domain)
    
    # Process each selected domain
    for domain in selected_domains:
        add_with_deps(domain)
    
    return resolved

def load_checkpoint() -> Dict[str, Any]:
    """Load checkpoint data from file."""
    if not CHECKPOINT_FILE.exists():
        return {"domains_completed": []}
    
    try:
        with open(CHECKPOINT_FILE, 'r') as f:
            return json.load(f)
    except (json.JSONDecodeError, IOError) as e:
        logger.warning(f"Error loading checkpoint file: {e}")
        return {"domains_completed": []}

def save_checkpoint(checkpoint_data: Dict[str, Any]):
    """Save checkpoint data to file."""
    try:
        with open(CHECKPOINT_FILE, 'w') as f:
            json.dump(checkpoint_data, f, indent=2)
    except IOError as e:
        logger.error(f"Error saving checkpoint file: {e}")

def mark_domain_completed(domain: str):
    """Mark a domain as completed in the checkpoint file."""
    checkpoint = load_checkpoint()
    if domain not in checkpoint["domains_completed"]:
        checkpoint["domains_completed"].append(domain)
        checkpoint["last_updated"] = datetime.now().isoformat()
        save_checkpoint(checkpoint)

def is_domain_completed(domain: str) -> bool:
    """Check if a domain is already completed."""
    checkpoint = load_checkpoint()
    return domain in checkpoint["domains_completed"]

def run_preprocessing(args):
    """Run preprocessing of Synthea CSV files."""
    if args.skip_preprocessing:
        print_warning("Skipping preprocessing step as requested")
        return True
    
    print_header("PREPROCESSING SYNTHEA CSV FILES")
    
    cmd = [
        "python", 
        str(PROJECT_ROOT / "python" / "preprocess_synthea_csv.py"),
        "--input-dir", args.synthea_dir,
        "--output-dir", args.processed_dir
    ]
    
    if args.disable_progress_bars:
        cmd.append("--no-progress-bar")
    
    if args.debug:
        cmd.append("--debug")
    
    if args.dry_run:
        print(f"Would run: {' '.join(cmd)}")
        return True
    
    print(f"Running: {' '.join(cmd)}")
    result = subprocess.run(cmd, capture_output=True, text=True)
    
    if result.returncode == 0:
        print_success("Preprocessing completed successfully")
        return True
    else:
        print_error(f"Preprocessing failed with code {result.returncode}")
        print(result.stderr)
        return False

def generate_domain_specific_script(domains: List[str], args):
    """Generate a domain-specific ETL script."""
    script_path = PROJECT_ROOT / "bash" / "run_domain_specific_etl.sh"
    
    # Read the original script
    original_script_path = PROJECT_ROOT / "bash" / "run_simplified_etl.sh"
    with open(original_script_path, 'r') as f:
        script_content = f.read()
    
    # Modify the script to include only selected domains
    # This is a simplified approach - in a real implementation, you would
    # need to parse the script more carefully
    
    # Add domain selection variables at the top of the script
    domain_vars = "\n# Domain selection (auto-generated)\n"
    for domain in AVAILABLE_DOMAINS.keys():
        if domain in domains:
            domain_vars += f"PROCESS_{domain.upper()}=true\n"
        else:
            domain_vars += f"PROCESS_{domain.upper()}=false\n"
    
    # Insert after the variable declarations
    script_content = script_content.replace(
        "# Default values for command line arguments",
        "# Default values for command line arguments\n" + domain_vars
    )
    
    # Modify each domain processing section to check the domain variable
    for domain in AVAILABLE_DOMAINS.keys():
        if domain in ["observation_periods", "concept_mapping"]:
            # Special handling for derived domains
            continue
            
        # Find the section for this domain and add a condition
        domain_section = f"# Process {domain}"
        if domain_section in script_content:
            condition = f'if [ "$PROCESS_{domain.upper()}" = true ]; then\n'
            end_condition = 'fi\n'
            
            # Split the script at the domain section
            parts = script_content.split(domain_section, 1)
            if len(parts) == 2:
                # Find the end of this section (next domain or end of script)
                next_domain_markers = [f"# Process {d}" for d in AVAILABLE_DOMAINS.keys() 
                                      if d != domain and f"# Process {d}" in parts[1]]
                if next_domain_markers:
                    # Find the closest next domain marker
                    next_marker_pos = min(parts[1].find(marker) for marker in next_domain_markers 
                                         if parts[1].find(marker) >= 0)
                    if next_marker_pos >= 0:
                        # Insert condition at the beginning and end of this section
                        parts[1] = (parts[1][:next_marker_pos] + end_condition + 
                                   parts[1][next_marker_pos:])
                        script_content = parts[0] + domain_section + "\n" + condition + parts[1]
    
    # Add batch size and other parameters
    script_content = script_content.replace(
        "--batch-size 50000",
        f"--batch-size {args.batch_size}"
    )
    
    script_content = script_content.replace(
        "--commit-frequency 10000",
        f"--commit-frequency {args.commit_frequency}"
    )
    
    # Write the modified script
    with open(script_path, 'w') as f:
        f.write(script_content)
    
    # Make it executable
    os.chmod(script_path, 0o755)
    
    return script_path

def run_domain_specific_etl(domains: List[str], args):
    """Run ETL for specific domains."""
    print_header("RUNNING DOMAIN-SPECIFIC ETL")
    
    # Generate the domain-specific script
    script_path = generate_domain_specific_script(domains, args)
    
    # Build the command
    cmd = [str(script_path)]
    
    if args.force_restart:
        cmd.append("--force-restart")
    
    if args.force_load:
        cmd.append("--force-load")
    
    if args.disable_progress_bars:
        cmd.append("--disable-progress-bars")
    
    if args.debug:
        cmd.append("--verbose")
    
    if args.synthea_dir:
        cmd.extend(["--synthea-dir", args.synthea_dir])
    
    if args.processed_dir:
        cmd.extend(["--processed-dir", args.processed_dir])
    
    if args.max_workers:
        cmd.extend(["--max-workers", str(args.max_workers)])
    
    if args.dry_run:
        print(f"Would run: {' '.join(cmd)}")
        return True
    
    print(f"Running: {' '.join(cmd)}")
    
    # Execute the script
    process = subprocess.Popen(
        cmd, 
        stdout=subprocess.PIPE, 
        stderr=subprocess.PIPE,
        universal_newlines=True,
        bufsize=1
    )
    
    # Stream output in real-time
    for line in process.stdout:
        print(line, end='')
    
    # Wait for process to complete
    process.wait()
    
    if process.returncode == 0:
        print_success("ETL completed successfully")
        return True
    else:
        print_error(f"ETL failed with code {process.returncode}")
        for line in process.stderr:
            print(line, end='')
        return False

def main():
    """Main function."""
    args = parse_arguments()
    setup_logging(args.debug)
    
    # If list-domains flag is set, just list domains and exit
    if args.list_domains:
        list_available_domains()
        return 0
    
    # If interactive mode is selected, prompt for domain selection
    if args.interactive:
        selected_domains = interactive_domain_selection()
        if not selected_domains:
            print_warning("No domains selected. Exiting.")
            return 0
    else:
        # Otherwise, use command line arguments
        if args.domains:
            selected_domains = [d.strip() for d in args.domains.split(',')]
        else:
            # Default to all domains if none specified
            selected_domains = list(AVAILABLE_DOMAINS.keys())
        
        # Remove skipped domains if specified
        if args.skip_domains:
            skip_domains = [d.strip() for d in args.skip_domains.split(',')]
            selected_domains = [d for d in selected_domains if d not in skip_domains]
    
    # Validate selected domains
    invalid_domains = [d for d in selected_domains if d not in AVAILABLE_DOMAINS]
    if invalid_domains:
        print_error(f"Invalid domains specified: {', '.join(invalid_domains)}")
        print("Use --list-domains to see available options")
        return 1
    
    # Resolve dependencies
    resolved_domains = resolve_dependencies(selected_domains)
    
    print_header("ETL EXECUTION PLAN")
    print(f"Data source directory: {args.synthea_dir}")
    print(f"Processed data directory: {args.processed_dir}")
    print(f"Batch size: {args.batch_size}")
    print(f"Max workers: {args.max_workers}")
    print(f"Force restart: {args.force_restart}")
    print(f"Skip preprocessing: {args.skip_preprocessing}")
    print()
    
    print("Domains to process (in order):")
    for i, domain in enumerate(resolved_domains, 1):
        print(f"{i}. {domain} - {AVAILABLE_DOMAINS[domain]['description']}")
    
    if args.dry_run:
        print_warning("\nDRY RUN MODE: No changes will be made")
    
    # Confirm execution
    if not args.dry_run and not args.interactive:
        confirm = input("\nProceed with ETL execution? (y/n): ").strip().lower()
        if confirm != 'y':
            print_warning("Execution cancelled.")
            return 0
    
    # Run preprocessing
    if not run_preprocessing(args):
        return 1
    
    # Run domain-specific ETL
    if not run_domain_specific_etl(resolved_domains, args):
        return 1
    
    print_success("\nETL process completed successfully!")
    return 0

if __name__ == "__main__":
    sys.exit(main())
