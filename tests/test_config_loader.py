#!/usr/bin/env python3
"""
test_config_loader.py

Unit tests for the configuration loader module.
"""

import os
import json
import tempfile
from pathlib import Path
import unittest
from unittest.mock import patch

# Add the parent directory to the Python path
import sys
sys.path.insert(0, str(Path(__file__).parent.parent))

from utils.config_loader import ConfigLoader

class TestConfigLoader(unittest.TestCase):
    """Test cases for the ConfigLoader class."""
    
    def setUp(self):
        """Set up test fixtures."""
        # Create a temporary directory for test files
        self.temp_dir = tempfile.TemporaryDirectory()
        self.temp_path = Path(self.temp_dir.name)
        
        # Create a test .env file
        self.env_file = self.temp_path / '.env'
        with open(self.env_file, 'w') as f:
            f.write("""
DB_HOST=testhost
DB_PORT=5433
DB_NAME=testdb
DB_USER=testuser
DB_PASSWORD=testpass
OMOP_SCHEMA=test_omop
STAGING_SCHEMA=test_staging
POPULATION_SCHEMA=test_population
VOCAB_DIR=/test/vocab
SYNTHEA_DATA_DIR=/test/synthea
WITH_HEADER=false
PARALLEL_JOBS=2
""")
        
        # Create a test config.json file
        self.config_file = self.temp_path / 'config.json'
        with open(self.config_file, 'w') as f:
            f.write("""
{
  "project": {
    "name": "Test Project",
    "version": "0.1.0"
  },
  "database": {
    "connection_timeout": 15,
    "max_connections": 5
  },
  "etl": {
    "batch_size": 5000,
    "enable_logging": false
  },
  "mapping": {
    "gender": {
      "M": 1,
      "F": 2
    },
    "race": {
      "white": 10,
      "black": 11
    }
  }
}
""")
    
    def tearDown(self):
        """Tear down test fixtures."""
        self.temp_dir.cleanup()
    
    @patch('utils.config_loader.Path')
    def test_get_env(self, mock_path):
        """Test getting environment variables."""
        # Mock the path to use our test files
        mock_path.return_value.parent.parent.absolute.return_value = self.temp_path
        mock_path.return_value.parent.parent.absolute.return_value.__truediv__.return_value = self.env_file
        
        # Create a config loader with our test files
        with patch.object(Path, 'exists', return_value=True):
            with patch('utils.config_loader.load_dotenv'):
                with patch.dict(os.environ, {
                    'DB_HOST': 'testhost',
                    'DB_PORT': '5433'
                }):
                    config = ConfigLoader(env_file=str(self.env_file), config_file=str(self.config_file))
                    
                    # Test getting environment variables
                    self.assertEqual(config.get_env('DB_HOST'), 'testhost')
                    self.assertEqual(config.get_env('DB_PORT'), '5433')
                    self.assertEqual(config.get_env('NONEXISTENT', 'default'), 'default')
    
    @patch('utils.config_loader.Path')
    def test_get_config(self, mock_path):
        """Test getting configuration values."""
        # Mock the path to use our test files
        mock_path.return_value.parent.parent.absolute.return_value = self.temp_path
        mock_path.return_value.parent.parent.absolute.return_value.__truediv__.return_value = self.config_file
        
        # Create a config loader with our test files
        with patch.object(Path, 'exists', return_value=True):
            with patch('utils.config_loader.load_dotenv'):
                # Load the test config.json content
                with open(self.config_file, 'r') as f:
                    config_data = json.load(f)
                
                # Mock the config_data attribute
                config = ConfigLoader(env_file=str(self.env_file), config_file=str(self.config_file))
                config.config_data = config_data
                
                # Test getting configuration values
                self.assertEqual(config.get_config('project.name'), 'Test Project')
                self.assertEqual(config.get_config('project.version'), '0.1.0')
                self.assertEqual(config.get_config('database.connection_timeout'), 15)
                self.assertEqual(config.get_config('etl.batch_size'), 5000)
                self.assertEqual(config.get_config('nonexistent.path', 'default'), 'default')
    
    @patch('utils.config_loader.Path')
    def test_get_db_config(self, mock_path):
        """Test getting database configuration."""
        # Mock the path to use our test files
        mock_path.return_value.parent.parent.absolute.return_value = self.temp_path
        mock_path.return_value.parent.parent.absolute.return_value.__truediv__.return_value = self.env_file
        
        # Create a config loader with our test files
        with patch.object(Path, 'exists', return_value=True):
            with patch('utils.config_loader.load_dotenv'):
                with patch.dict(os.environ, {
                    'DB_HOST': 'testhost',
                    'DB_PORT': '5433',
                    'DB_NAME': 'testdb',
                    'DB_USER': 'testuser',
                    'DB_PASSWORD': 'testpass'
                }):
                    config = ConfigLoader(env_file=str(self.env_file), config_file=str(self.config_file))
                    
                    # Test getting database configuration
                    db_config = config.get_db_config()
                    self.assertEqual(db_config['host'], 'testhost')
                    self.assertEqual(db_config['port'], '5433')
                    self.assertEqual(db_config['dbname'], 'testdb')
                    self.assertEqual(db_config['user'], 'testuser')
                    self.assertEqual(db_config['password'], 'testpass')
    
    @patch('utils.config_loader.Path')
    def test_get_concept_id(self, mock_path):
        """Test getting concept IDs from mapping."""
        # Mock the path to use our test files
        mock_path.return_value.parent.parent.absolute.return_value = self.temp_path
        mock_path.return_value.parent.parent.absolute.return_value.__truediv__.return_value = self.config_file
        
        # Create a config loader with our test files
        with patch.object(Path, 'exists', return_value=True):
            with patch('utils.config_loader.load_dotenv'):
                # Load the test config.json content
                with open(self.config_file, 'r') as f:
                    config_data = json.load(f)
                
                # Mock the config_data attribute
                config = ConfigLoader(env_file=str(self.env_file), config_file=str(self.config_file))
                config.config_data = config_data
                
                # Test getting concept IDs
                self.assertEqual(config.get_concept_id('gender', 'M'), 1)
                self.assertEqual(config.get_concept_id('gender', 'F'), 2)
                self.assertEqual(config.get_concept_id('race', 'white'), 10)
                self.assertEqual(config.get_concept_id('race', 'black'), 11)
                self.assertIsNone(config.get_concept_id('gender', 'X'))
                self.assertIsNone(config.get_concept_id('nonexistent', 'code'))

if __name__ == '__main__':
    unittest.main()
