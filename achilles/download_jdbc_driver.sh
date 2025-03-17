#!/bin/bash

# Script to download PostgreSQL JDBC driver

DRIVER_VERSION="42.2.23"
DRIVER_URL="https://jdbc.postgresql.org/download/postgresql-${DRIVER_VERSION}.jar"
OUTPUT_DIR="./achilles/drivers"

echo "Downloading PostgreSQL JDBC driver version ${DRIVER_VERSION}..."
curl -L -o "${OUTPUT_DIR}/postgresql-${DRIVER_VERSION}.jar" "${DRIVER_URL}"

if [ $? -eq 0 ]; then
    echo "Download successful. Driver saved to ${OUTPUT_DIR}/postgresql-${DRIVER_VERSION}.jar"
else
    echo "Download failed. Please check your internet connection and try again."
    exit 1
fi
