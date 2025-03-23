#!/usr/bin/env python3
"""
init_project.py

A script to initialize the project structure, create necessary directories,
and set up configuration files.
"""

import os
import sys
import shutil
import logging
from pathlib import Path

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def create_directory(path: Path) -> None:
    """
    Create a directory if it doesn't exist.
    
    Args:
        path: Path to the directory to create
    """
    if not path.exists():
        path.mkdir(parents=True)
        logger.info(f"Created directory: {path}")
    else:
        logger.info(f"Directory already exists: {path}")

def copy_file(source: Path, destination: Path) -> None:
    """
    Copy a file if the destination doesn't exist.
    
    Args:
        source: Path to the source file
        destination: Path to the destination file
    """
    if not destination.exists():
        shutil.copy2(source, destination)
        logger.info(f"Copied {source} to {destination}")
    else:
        logger.info(f"File already exists: {destination}")

def initialize_project() -> None:
    """Initialize the project structure and configuration files."""
    # Get project root directory
    project_root = Path(__file__).parent.absolute()
    
    # Create necessary directories
    directories = [
        project_root / "vocabulary",
        project_root / "synthea_data",
        project_root / "logs",
        project_root / "output",
        project_root / "utils",
        project_root / "docs"
    ]
    
    for directory in directories:
        create_directory(directory)
    
    # Copy .env.example to .env if it doesn't exist
    env_example = project_root / ".env.example"
    env_file = project_root / ".env"
    
    if env_example.exists():
        copy_file(env_example, env_file)
    else:
        logger.warning(f".env.example not found in {project_root}")
    
    # Create requirements.txt if it doesn't exist
    requirements_file = project_root / "requirements.txt"
    if not requirements_file.exists():
        with open(requirements_file, 'w') as f:
            f.write("""# Python dependencies for Synthea2OMOP-ETL
python-dotenv>=0.19.0
psycopg2-binary>=2.9.1
pandas>=1.3.0
sqlalchemy>=1.4.0
tqdm>=4.62.0
pyyaml>=6.0
pytest>=6.2.5
"""
            )
        logger.info(f"Created requirements.txt")
    
    # Create README.md if it doesn't exist
    readme_file = project_root / "README.md"
    if not readme_file.exists():
        with open(readme_file, 'w') as f:
            f.write("""# Enhanced Synthea2OMOP-ETL

An enhanced ETL pipeline for converting Synthea synthetic healthcare data to the OMOP Common Data Model.

## Setup

1. Clone this repository
2. Run `python init_project.py` to initialize the project structure
3. Copy your Synthea CSV files to the `synthea_data` directory
4. Copy your OMOP vocabulary files to the `vocabulary` directory
5. Edit the `.env` file with your database connection details
6. Install dependencies: `pip install -r requirements.txt`
7. Run the ETL process: `python run_etl.py`

## Configuration

- `.env`: Environment-specific configuration (database credentials, etc.)
- `config.json`: Project-wide settings and mappings

## Directory Structure

- `scripts/`: Shell scripts for data loading and processing
- `sql/`: SQL scripts for ETL operations
- `utils/`: Utility modules and helper functions
- `vocabulary/`: OMOP vocabulary files
- `synthea_data/`: Synthea CSV files
- `logs/`: Log files
- `output/`: Output files and reports
- `docs/`: Documentation

## Documentation

See the `docs/` directory for detailed documentation.
"""
            )
        logger.info(f"Created README.md")
    
    logger.info("Project initialization complete!")
    logger.info("Next steps:")
    logger.info("1. Edit the .env file with your database connection details")
    logger.info("2. Install dependencies: pip install -r requirements.txt")
    logger.info("3. Copy Synthea CSV files to the synthea_data directory")
    logger.info("4. Copy OMOP vocabulary files to the vocabulary directory")

if __name__ == "__main__":
    try:
        initialize_project()
    except Exception as e:
        logger.error(f"Error initializing project: {e}")
        sys.exit(1)
