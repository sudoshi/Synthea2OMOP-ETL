#!/bin/bash

# Set environment variables
export DB_HOST=localhost
export DB_PORT=5432
export DB_NAME=synthea
export DB_USER=postgres
export DB_PASSWORD=acumenus
export PORT=5081

# Install dependencies if needed
pip install -r requirements-api.txt

# Run the API server
python etl_api.py
