#!/usr/bin/env python3
"""
config_loader.py

A module for loading and accessing configuration settings from both
environment variables (.env file) and the config.json file.
"""

import os
import json
import logging
from pathlib import Path
from typing import Any, Dict, Optional, Union
from dotenv import load_dotenv

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class ConfigLoader:
    """
    A class to load and provide access to configuration settings from
    environment variables and JSON configuration files.
    """
    
    def __init__(self, env_file: str = '.env', config_file: str = 'config.json'):
        """
        Initialize the ConfigLoader.
        
        Args:
            env_file: Path to the .env file (default: '.env')
            config_file: Path to the config.json file (default: 'config.json')
        """
        self.project_root = Path(__file__).parent.parent.absolute()
        self.env_file = self.project_root / env_file
        self.config_file = self.project_root / config_file
        self.config_data = {}
        
        # Load configuration
        self._load_env_vars()
        self._load_config_json()
        
        logger.info(f"Configuration loaded from {env_file} and {config_file}")
    
    def _load_env_vars(self) -> None:
        """Load environment variables from .env file."""
        if self.env_file.exists():
            load_dotenv(dotenv_path=str(self.env_file))
            logger.info(f"Loaded environment variables from {self.env_file}")
        else:
            logger.warning(f"Environment file {self.env_file} not found")
    
    def _load_config_json(self) -> None:
        """Load configuration from config.json file."""
        if self.config_file.exists():
            try:
                with open(self.config_file, 'r') as f:
                    self.config_data = json.load(f)
                logger.info(f"Loaded configuration from {self.config_file}")
            except json.JSONDecodeError as e:
                logger.error(f"Error parsing {self.config_file}: {e}")
                self.config_data = {}
        else:
            logger.warning(f"Configuration file {self.config_file} not found")
    
    def get_env(self, key: str, default: Any = None) -> str:
        """
        Get a value from environment variables.
        
        Args:
            key: The environment variable name
            default: Default value if the key is not found
            
        Returns:
            The value of the environment variable or the default
        """
        return os.environ.get(key, default)
    
    def get_config(self, path: str, default: Any = None) -> Any:
        """
        Get a value from the config.json using dot notation.
        
        Args:
            path: Path to the config value using dot notation (e.g., 'database.host')
            default: Default value if the path is not found
            
        Returns:
            The configuration value or the default
        """
        parts = path.split('.')
        value = self.config_data
        
        for part in parts:
            if isinstance(value, dict) and part in value:
                value = value[part]
            else:
                return default
        
        return value
    
    def get_db_config(self) -> Dict[str, str]:
        """
        Get database configuration as a dictionary.
        
        Returns:
            Dictionary with database configuration
        """
        return {
            'host': self.get_env('DB_HOST', 'localhost'),
            'port': self.get_env('DB_PORT', '5432'),
            'dbname': self.get_env('DB_NAME', 'synthea'),
            'user': self.get_env('DB_USER', 'postgres'),
            'password': self.get_env('DB_PASSWORD', '')
        }
    
    def get_schema_names(self) -> Dict[str, str]:
        """
        Get schema names as a dictionary.
        
        Returns:
            Dictionary with schema names
        """
        return {
            'omop': self.get_env('OMOP_SCHEMA', 'omop'),
            'staging': self.get_env('STAGING_SCHEMA', 'staging'),
            'population': self.get_env('POPULATION_SCHEMA', 'population')
        }
    
    def get_file_paths(self) -> Dict[str, str]:
        """
        Get file paths as a dictionary.
        
        Returns:
            Dictionary with file paths
        """
        return {
            'vocab_dir': self.get_env('VOCAB_DIR', str(self.project_root / 'vocabulary')),
            'synthea_data_dir': self.get_env('SYNTHEA_DATA_DIR', str(self.project_root / 'synthea_data'))
        }
    
    def get_processing_options(self) -> Dict[str, Union[bool, int]]:
        """
        Get processing options as a dictionary.
        
        Returns:
            Dictionary with processing options
        """
        return {
            'with_header': self.get_env('WITH_HEADER', 'true').lower() == 'true',
            'parallel_jobs': int(self.get_env('PARALLEL_JOBS', '4')),
            'batch_size': int(self.get_config('etl.batch_size', 10000)),
            'enable_logging': self.get_config('etl.enable_logging', True),
            'truncate_target_tables': self.get_config('etl.truncate_target_tables', True)
        }
    
    def get_concept_id(self, category: str, code: str) -> Optional[int]:
        """
        Get a concept ID from the mapping configuration.
        
        Args:
            category: The mapping category (e.g., 'gender', 'race')
            code: The source code
            
        Returns:
            The concept ID or None if not found
        """
        mapping = self.get_config(f'mapping.{category}', {})
        return mapping.get(code)


# Create a singleton instance
config = ConfigLoader()

# Example usage
if __name__ == "__main__":
    # Print some configuration values for testing
    print(f"Database config: {config.get_db_config()}")
    print(f"Schema names: {config.get_schema_names()}")
    print(f"File paths: {config.get_file_paths()}")
    print(f"Processing options: {config.get_processing_options()}")
    print(f"Gender concept ID for 'M': {config.get_concept_id('gender', 'M')}")
