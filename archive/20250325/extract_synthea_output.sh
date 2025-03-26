#!/bin/bash
#
# extract_synthea_output.sh
#
# Script to extract Synthea output files from the Docker volume to a local directory.

set -euo pipefail

# Default output directory
OUTPUT_DIR="./synthea_output"

# Parse command line arguments
while [[ $# -gt 0 ]]; do
    case "$1" in
        -o|--output)
            OUTPUT_DIR="$2"
            shift 2
            ;;
        -h|--help)
            echo "Usage: $0 [options]"
            echo "Options:"
            echo "  -o, --output <directory>   Output directory (default: ./synthea_output)"
            echo "  -h, --help                 Display this help message"
            exit 0
            ;;
        *)
            echo "Unknown option: $1"
            echo "Use --help for usage information"
            exit 1
            ;;
    esac
done

# Create output directory if it doesn't exist
mkdir -p "$OUTPUT_DIR"
echo "Extracting Synthea output files to $OUTPUT_DIR..."

# Extract files from Docker volume to local directory
docker run --rm -v synthea2omop-etl_synthea-output:/output -v "$(pwd)/$OUTPUT_DIR":/extract alpine:latest sh -c "cp -r /output/* /extract/"

# Count files
FILE_COUNT=$(find "$OUTPUT_DIR" -type f | wc -l)
echo "Extracted $FILE_COUNT files to $OUTPUT_DIR"

# List directories
echo "Directories in $OUTPUT_DIR:"
find "$OUTPUT_DIR" -type d -maxdepth 1 | sort

echo "Done! You can now inspect the files in $OUTPUT_DIR"
