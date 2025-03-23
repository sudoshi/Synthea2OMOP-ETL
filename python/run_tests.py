#!/usr/bin/env python3
"""
run_tests.py

A script to run the test suite for the Synthea2OMOP-ETL project.
"""

import os
import sys
import unittest
import argparse
import logging
from pathlib import Path

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def discover_and_run_tests(test_path: str = 'tests', pattern: str = 'test_*.py', verbosity: int = 2) -> bool:
    """
    Discover and run tests in the specified path.
    
    Args:
        test_path: Path to the test directory
        pattern: Pattern to match test files
        verbosity: Verbosity level for test output
        
    Returns:
        True if all tests passed, False otherwise
    """
    logger.info(f"Discovering tests in {test_path} with pattern {pattern}")
    
    # Ensure the test path exists
    if not os.path.exists(test_path):
        logger.error(f"Test path {test_path} does not exist")
        return False
    
    # Discover and run tests
    test_loader = unittest.TestLoader()
    test_suite = test_loader.discover(test_path, pattern=pattern)
    
    test_runner = unittest.TextTestRunner(verbosity=verbosity)
    result = test_runner.run(test_suite)
    
    # Return True if all tests passed
    return result.wasSuccessful()

def main(args=None) -> int:
    """
    Main entry point for running tests.
    
    Args:
        args: Command line arguments
        
    Returns:
        Exit code (0 for success, non-zero for failure)
    """
    parser = argparse.ArgumentParser(description="Run the test suite for the Synthea2OMOP-ETL project")
    parser.add_argument('--path', default='tests', help='Path to the test directory')
    parser.add_argument('--pattern', default='test_*.py', help='Pattern to match test files')
    parser.add_argument('--verbosity', type=int, default=2, help='Verbosity level for test output')
    
    args = parser.parse_args(args)
    
    # Run tests
    success = discover_and_run_tests(args.path, args.pattern, args.verbosity)
    
    # Return exit code
    return 0 if success else 1

if __name__ == '__main__':
    sys.exit(main())
