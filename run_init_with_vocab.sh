#!/bin/bash
#
# run_init_with_vocab.sh
#
# Script to initialize the database with schemas, tables, and vocabulary.
# This is a wrapper for the init_database_with_vocab.py script.

set -euo pipefail

# Get project root directory
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$SCRIPT_DIR"

# Load environment variables from .env file if it exists
if [ -f "$PROJECT_ROOT/.env" ]; then
    echo "Loading configuration from $PROJECT_ROOT/.env"
    set -a  # automatically export all variables
    source "$PROJECT_ROOT/.env"
    set +a
else
    echo "Warning: .env file not found in $PROJECT_ROOT"
    echo "Using default configuration values"
fi

# Default values
VOCAB_DIR="${VOCAB_DIR:-$PROJECT_ROOT/vocabulary}"
PROCESSED_VOCAB_DIR="${PROCESSED_VOCAB_DIR:-$PROJECT_ROOT/vocabulary_processed}"
MAX_WORKERS="${MAX_WORKERS:-4}"
BATCH_SIZE="${BATCH_SIZE:-1000000}"
SKIP_INIT=false
SKIP_VOCAB=false
DEBUG=false

# Function to display usage information
usage() {
    echo "Usage: $0 [options]"
    echo "Options:"
    echo "  -v, --vocab-dir <directory>           Directory containing vocabulary files (default: ./vocabulary)"
    echo "  -p, --processed-vocab-dir <directory> Directory for processed vocabulary files (default: ./vocabulary_processed)"
    echo "  -w, --max-workers <number>            Maximum number of parallel workers (default: 4)"
    echo "  -b, --batch-size <number>             Batch size for processing large files (default: 1000000)"
    echo "  -i, --skip-init                       Skip database initialization"
    echo "  -s, --skip-vocab                      Skip vocabulary loading"
    echo "  -d, --debug                           Enable debug logging"
    echo "  -h, --help                            Display this help message"
    exit 1
}

# Parse command line arguments
while [[ $# -gt 0 ]]; do
    case "$1" in
        -v|--vocab-dir)
            VOCAB_DIR="$2"
            shift 2
            ;;
        -p|--processed-vocab-dir)
            PROCESSED_VOCAB_DIR="$2"
            shift 2
            ;;
        -w|--max-workers)
            MAX_WORKERS="$2"
            shift 2
            ;;
        -b|--batch-size)
            BATCH_SIZE="$2"
            shift 2
            ;;
        -i|--skip-init)
            SKIP_INIT=true
            shift
            ;;
        -s|--skip-vocab)
            SKIP_VOCAB=true
            shift
            ;;
        -d|--debug)
            DEBUG=true
            shift
            ;;
        -h|--help)
            usage
            ;;
        *)
            echo "Unknown option: $1"
            usage
            ;;
    esac
done

# Build command arguments
ARGS=("--vocab-dir" "$VOCAB_DIR" "--processed-vocab-dir" "$PROCESSED_VOCAB_DIR" "--max-workers" "$MAX_WORKERS" "--batch-size" "$BATCH_SIZE")

if [ "$SKIP_INIT" = true ]; then
    ARGS+=("--skip-init")
fi

if [ "$SKIP_VOCAB" = true ]; then
    ARGS+=("--skip-vocab")
fi

if [ "$DEBUG" = true ]; then
    ARGS+=("--debug")
fi

# Display header
echo "===== Starting Database Initialization and Vocabulary Loading ====="
echo "Date: $(date)"
echo "Vocabulary Directory: $VOCAB_DIR"
echo "Processed Vocabulary Directory: $PROCESSED_VOCAB_DIR"
echo "Max Workers: $MAX_WORKERS"
echo "Batch Size: $BATCH_SIZE"
echo "Skip Init: $SKIP_INIT"
echo "Skip Vocab: $SKIP_VOCAB"
echo "Debug Mode: $DEBUG"
echo ""

# Execute the Python script with timing
echo "Executing initialization script..."
start_time=$(date +%s)
python3 "$PROJECT_ROOT/init_database_with_vocab.py" "${ARGS[@]}"
exit_code=$?
end_time=$(date +%s)

# Calculate duration
duration=$((end_time - start_time))
hours=$((duration / 3600))
minutes=$(( (duration % 3600) / 60 ))
seconds=$((duration % 60))

# Display completion message
echo ""
echo "===== Initialization Process Completed ====="
echo "Exit Code: $exit_code"
echo "Total duration: ${hours}h ${minutes}m ${seconds}s"
echo "Date: $(date)"

# Display record counts if successful and vocabulary was loaded
if [ $exit_code -eq 0 ] && [ "$SKIP_VOCAB" = false ]; then
    echo ""
    echo "===== Vocabulary Record Counts ====="
    PGPASSWORD=${DB_PASSWORD:-acumenus} psql -h ${DB_HOST:-localhost} -U ${DB_USER:-postgres} -d ${DB_NAME:-ohdsi} -c "
    SELECT 'concept' as table_name, COUNT(*) as row_count FROM omop.concept
    UNION ALL
    SELECT 'vocabulary', COUNT(*) FROM omop.vocabulary
    UNION ALL
    SELECT 'domain', COUNT(*) FROM omop.domain
    UNION ALL
    SELECT 'concept_class', COUNT(*) FROM omop.concept_class
    UNION ALL
    SELECT 'relationship', COUNT(*) FROM omop.relationship
    UNION ALL
    SELECT 'concept_relationship', COUNT(*) FROM omop.concept_relationship
    UNION ALL
    SELECT 'concept_synonym', COUNT(*) FROM omop.concept_synonym
    UNION ALL
    SELECT 'concept_ancestor', COUNT(*) FROM omop.concept_ancestor
    UNION ALL
    SELECT 'drug_strength', COUNT(*) FROM omop.drug_strength
    UNION ALL
    SELECT 'source_to_concept_map', COUNT(*) FROM omop.source_to_concept_map
    ORDER BY table_name;"
fi

exit $exit_code
