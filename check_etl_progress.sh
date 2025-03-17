#!/bin/bash

# Set the API URL
API_URL="http://localhost:5001/api/etl/status"

# Make the API request and format the output
echo "Checking ETL progress..."
echo "------------------------"
curl -s $API_URL | python -m json.tool

echo ""
echo "------------------------"
echo "ETL progress check complete."
