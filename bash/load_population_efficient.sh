#!/bin/bash

# Default settings
DEFAULT_DB_HOST="localhost"
DEFAULT_DB_PORT="5432"
DEFAULT_DB_NAME="ohdsi"
DEFAULT_DB_USER="postgres"
DEFAULT_DB_SCHEMA="population"

# Load environment variables
if [ -f .env ]; then
    source .env
fi

# Set database connection parameters
DB_HOST="${DB_HOST:-$DEFAULT_DB_HOST}"
DB_PORT="${DB_PORT:-$DEFAULT_DB_PORT}"
DB_NAME="${DB_NAME:-$DEFAULT_DB_NAME}"
DB_USER="${DB_USER:-$DEFAULT_DB_USER}"
DB_SCHEMA="${DB_SCHEMA:-$DEFAULT_DB_SCHEMA}"

# Export password for psql commands if provided
if [ -n "$DB_PASSWORD" ]; then
    export PGPASSWORD="$DB_PASSWORD"
fi

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

# Function to log messages
log() {
    echo -e "$(date '+%Y-%m-%d %H:%M:%S') - $1"
}

# Function to create table from CSV header
create_table_from_csv() {
    local csv_file="$1"
    local table_name="$2"
    local schema="$3"

    # Extract header and create table
    header=$(head -n 1 "$csv_file")
    create_sql="CREATE TABLE IF NOT EXISTS \"$schema\".\"$table_name\" ("
    IFS=',' read -ra columns <<< "$header"
    
    for i in "${!columns[@]}"; do
        col_name="${columns[$i]}"
        col_name="${col_name//[$'\t\r\n ']}"  # Remove whitespace
        col_name="${col_name//\"}"            # Remove quotes
        
        if [[ $i -gt 0 ]]; then
            create_sql+=", "
        fi
        create_sql+="\"$col_name\" TEXT"
    done
    create_sql+=");"
    
    psql -h "$DB_HOST" -p "$DB_PORT" -U "$DB_USER" -d "$DB_NAME" -c "$create_sql"
}

# Function to display progress bar
show_progress() {
    local current=$1
    local total=$2
    local percent=$((current * 100 / total))
    local completed=$((percent / 2))
    local remaining=$((50 - completed))
    
    printf "\r["
    printf "%${completed}s" | tr ' ' '='
    printf ">"
    printf "%${remaining}s" | tr ' ' ' '
    printf "] %3d%% (%'d/%'d)" $percent $current $total
}

# Function to load CSV file efficiently with enhanced batch processing
load_csv() {
    local csv_file="$1"
    local force="$2"
    
    # Extract table name from file name (remove .csv extension)
    local table_name="${csv_file%.csv}"
    
    log "${BLUE}Processing $csv_file => $DB_SCHEMA.$table_name${NC}"
    
    # Check if table exists and has data
    if psql -h "$DB_HOST" -p "$DB_PORT" -U "$DB_USER" -d "$DB_NAME" -t -c "
        SELECT EXISTS (
            SELECT 1 FROM \"$DB_SCHEMA\".\"$table_name\" LIMIT 1
        );
    " | grep -q 't'; then
        if [ "$force" != "--force" ]; then
            log "${YELLOW}Table $DB_SCHEMA.$table_name already has data. Use --force to overwrite.${NC}"
            return 0
        fi
    fi

    # Create or recreate table
    if [ "$force" = "--force" ]; then
        log "Dropping existing table $DB_SCHEMA.$table_name"
        psql -h "$DB_HOST" -p "$DB_PORT" -U "$DB_USER" -d "$DB_NAME" -c "DROP TABLE IF EXISTS \"$DB_SCHEMA\".\"$table_name\";"
    fi

    # Create table from CSV header
    create_table_from_csv "$csv_file" "$table_name" "$DB_SCHEMA"

    # Count total rows (excluding header)
    total_rows=$(($(wc -l < "$csv_file") - 1))
    log "Loading $total_rows rows into $DB_SCHEMA.$table_name..."

    # Get file size in MB
    file_size_mb=$(( $(stat --format=%s "$csv_file") / 1024 / 1024 ))
    
    # Set optimized parameters
    log "Optimizing PostgreSQL settings for bulk load..."
    psql -h "$DB_HOST" -p "$DB_PORT" -U "$DB_USER" -d "$DB_NAME" -c "ALTER SYSTEM SET work_mem = '2GB';"
    psql -h "$DB_HOST" -p "$DB_PORT" -U "$DB_USER" -d "$DB_NAME" -c "ALTER SYSTEM SET maintenance_work_mem = '4GB';"
    psql -h "$DB_HOST" -p "$DB_PORT" -U "$DB_USER" -d "$DB_NAME" -c "ALTER SYSTEM SET temp_buffers = '2GB';"
    psql -h "$DB_HOST" -p "$DB_PORT" -U "$DB_USER" -d "$DB_NAME" -c "ALTER SYSTEM SET checkpoint_timeout = '30min';"
    psql -h "$DB_HOST" -p "$DB_PORT" -U "$DB_USER" -d "$DB_NAME" -c "ALTER SYSTEM SET max_wal_size = '8GB';"
    psql -h "$DB_HOST" -p "$DB_PORT" -U "$DB_USER" -d "$DB_NAME" -c "SELECT pg_reload_conf();"
    
    # Check available disk space in /tmp (in MB)
    available_space_mb=$(df -m /tmp | awk 'NR==2 {print $4}')
    log "Available space in /tmp: $available_space_mb MB"
    
    # For very large files, use direct loading with psql's \copy command
    if [ "$file_size_mb" -gt 10000 ] || [ "$available_space_mb" -lt "$file_size_mb" ]; then
        log "${YELLOW}File is too large for batch processing with available disk space. Using direct psql COPY...${NC}"
        
        # Use psql's \copy command which reads directly from the file system
        # This avoids the need for temporary files or pipes
        log "Starting direct load using psql's \\copy command..."
        
        # Set up a progress monitoring mechanism
        total_lines=$(wc -l < "$csv_file")
        log "Total lines in file: $total_lines"
        
        # Ensure temporary directory exists
        if [ ! -d "$tmp_dir" ]; then
            mkdir -p "$tmp_dir"
            log "Created temporary directory: $tmp_dir"
        fi
        
        # Create a temporary psql script with COPY command
        psql_script="$tmp_dir/copy_script.sql"
        echo "\timing on" > "$psql_script"
        echo "BEGIN;" >> "$psql_script"
        echo "\echo 'Starting COPY operation...'" >> "$psql_script"
        echo "\copy \"$DB_SCHEMA\".\"$table_name\" FROM '$csv_file' WITH (FORMAT csv, HEADER true);" >> "$psql_script"
        echo "COMMIT;" >> "$psql_script"
        echo "\echo 'COPY completed successfully'" >> "$psql_script"
        echo "ANALYZE \"$DB_SCHEMA\".\"$table_name\";" >> "$psql_script"
        echo "SELECT COUNT(*) FROM \"$DB_SCHEMA\".\"$table_name\";" >> "$psql_script"
        
        # Execute the script
        log "Executing direct COPY command. This may take a while for large files..."
        if psql -h "$DB_HOST" -p "$DB_PORT" -U "$DB_USER" -d "$DB_NAME" -f "$psql_script"; then
            log "${GREEN}Successfully completed direct COPY operation${NC}"
        else
            log "${RED}Error during direct COPY operation${NC}"
            return 1
        fi
        
        # Get count from the database
        count=$(psql -h "$DB_HOST" -p "$DB_PORT" -U "$DB_USER" -d "$DB_NAME" -t -c "SELECT COUNT(*) FROM \"$DB_SCHEMA\".\"$table_name\"" | tr -d ' ')
        log "${GREEN}Successfully loaded $(printf "%'d" $count) rows into $DB_SCHEMA.$table_name${NC}"
        
        return 0
    fi
    
    # For other files, use batch processing with temp files
    log "Using batch processing for optimal performance and monitoring..."
    
    # Use a directory with more space if possible
    if [ -d "/data" ] && [ -w "/data" ]; then
        tmp_dir="/data/tmp_$$.$(date +%s)"
    elif [ -d "/mnt" ] && [ -w "/mnt" ]; then
        tmp_dir="/mnt/tmp_$$.$(date +%s)"
    else
        tmp_dir="/tmp/csv_load_$$.$(date +%s)"
    fi
    
    # Ensure the temporary directory exists and is writable
    mkdir -p "$tmp_dir" || {
        log "${RED}Error: Cannot create temporary directory $tmp_dir${NC}"
        # Try an alternative location as fallback
        tmp_dir="/tmp/csv_load_fallback_$$.$(date +%s)"
        mkdir -p "$tmp_dir" || {
            log "${RED}Fatal error: Cannot create any temporary directory${NC}"
            return 1
        }
    }
    
    log "Using temporary directory: $tmp_dir"
    trap 'rm -rf "$tmp_dir"' EXIT
    
    # Determine batch size based on file size and available space
    batch_size=1000000  # Default 1 million rows per batch
    if [ "$file_size_mb" -gt 10000 ]; then
        batch_size=250000  # For extremely large files (>10GB), use smaller batches
    elif [ "$file_size_mb" -lt 100 ]; then
        batch_size=5000000  # For small files (<100MB), use larger batches
    fi
    
    # Split the file, preserving the header
    log "Preparing batches with $batch_size rows per batch..."
    head -n1 "$csv_file" > "$tmp_dir/header" || {
        log "${RED}Error creating header file. Check disk space.${NC}"
        df -h /tmp
        return 1
    }
    
    # Use a more efficient approach for splitting large files
    if [ "$file_size_mb" -gt 5000 ]; then
        log "Large file detected. Using streaming split approach..."
        # Calculate approximate number of batches
        estimated_batches=$((total_rows / batch_size + 1))
        log "Estimated number of batches: $estimated_batches"
        
        # Process in streaming mode without creating all split files at once
        current_batch=0
        rows_loaded=0
        row_count=0
        batch_file="$tmp_dir/current_batch"
        
        # Process the file in chunks without storing all splits
        tail -n+2 "$csv_file" | while IFS= read -r line; do
            # Add line to current batch
            echo "$line" >> "$batch_file"
            ((row_count++))
            
            # If we've reached batch size, process this batch
            if [ "$row_count" -ge "$batch_size" ]; then
                ((current_batch++))
                log "Processing streaming batch $current_batch (approx. $row_count rows)..."
                
                # Prepend header to batch file
                cat "$tmp_dir/header" "$batch_file" > "$tmp_dir/with_header"
                mv "$tmp_dir/with_header" "$batch_file"
                
                # Load the batch
                if psql -h "$DB_HOST" -p "$DB_PORT" -U "$DB_USER" -d "$DB_NAME" -c "\copy $DB_SCHEMA.$table_name FROM STDIN WITH (FORMAT csv, HEADER true)" < "$batch_file"; then
                    # Add batch rows to total
                    rows_loaded=$((rows_loaded + row_count))
                    
                    # Calculate and show progress
                    percent=$((rows_loaded * 100 / total_rows))
                    show_progress $rows_loaded $total_rows
                    echo ""  # New line after progress bar
                    
                    # Commit after each batch to prevent transaction log buildup
                    psql -h "$DB_HOST" -p "$DB_PORT" -U "$DB_USER" -d "$DB_NAME" -c "COMMIT;"
                else
                    log "${RED}Error loading streaming batch $current_batch${NC}"
                fi
                
                # Reset for next batch
                rm -f "$batch_file"
                row_count=0
            fi
        done
        
        # Process any remaining rows
        if [ -f "$batch_file" ] && [ "$row_count" -gt 0 ]; then
            ((current_batch++))
            log "Processing final streaming batch $current_batch (approx. $row_count rows)..."
            
            # Prepend header to batch file
            cat "$tmp_dir/header" "$batch_file" > "$tmp_dir/with_header"
            mv "$tmp_dir/with_header" "$batch_file"
            
            # Load the batch
            if psql -h "$DB_HOST" -p "$DB_PORT" -U "$DB_USER" -d "$DB_NAME" -c "\copy $DB_SCHEMA.$table_name FROM STDIN WITH (FORMAT csv, HEADER true)" < "$batch_file"; then
                # Add batch rows to total
                rows_loaded=$((rows_loaded + row_count))
                
                # Calculate and show progress
                percent=$((rows_loaded * 100 / total_rows))
                show_progress $rows_loaded $total_rows
                echo ""  # New line after progress bar
                
                # Commit after each batch
                psql -h "$DB_HOST" -p "$DB_PORT" -U "$DB_USER" -d "$DB_NAME" -c "COMMIT;"
            else
                log "${RED}Error loading final streaming batch${NC}"
            fi
        fi
        
        # Skip the regular batch processing
        total_batches=0
    else
        # For smaller files, use the regular split approach
        tail -n+2 "$csv_file" | split -l $batch_size - "$tmp_dir/split_" || {
            log "${RED}Error splitting file. Check disk space.${NC}"
            df -h
            return 1
        }
        
        # Count total batches
        total_batches=$(ls "$tmp_dir/split_"* 2>/dev/null | wc -l)
        
        if [ "$total_batches" -eq 0 ]; then
            # Handle empty files or files with only header
            log "${YELLOW}File contains only header or is empty. Creating empty table.${NC}"
            return 0
        fi
    fi
    
    log "File will be processed in $total_batches batches"
    current_batch=0
    rows_loaded=0
    
    # Process each batch
    for batch in "$tmp_dir"/split_*; do
        ((current_batch++))
        
        # Combine header with current batch
        cat "$tmp_dir/header" "$batch" > "$tmp_dir/current_batch"
        
        # Get batch row count (excluding header)
        batch_rows=$(($(wc -l < "$tmp_dir/current_batch") - 1))
        
        # Show batch info with progress
        log "Processing batch $current_batch of $total_batches ($batch_rows rows)..."
        
        # Load the batch
        if psql -h "$DB_HOST" -p "$DB_PORT" -U "$DB_USER" -d "$DB_NAME" -c "\copy $DB_SCHEMA.$table_name FROM STDIN WITH (FORMAT csv, HEADER true)" < "$tmp_dir/current_batch"; then
            # Add batch rows to total
            rows_loaded=$((rows_loaded + batch_rows))
            
            # Calculate and show progress
            percent=$((rows_loaded * 100 / total_rows))
            show_progress $rows_loaded $total_rows
            echo ""  # New line after progress bar
            
            # Commit after each batch to prevent transaction log buildup
            psql -h "$DB_HOST" -p "$DB_PORT" -U "$DB_USER" -d "$DB_NAME" -c "COMMIT;"
        else
            log "${RED}Error loading batch $current_batch${NC}"
        fi
    done
    
    # Clean up
    rm -rf "$tmp_dir"
    
    # Get final count
    count=$(psql -h "$DB_HOST" -p "$DB_PORT" -U "$DB_USER" -d "$DB_NAME" -t -c "SELECT COUNT(*) FROM \"$DB_SCHEMA\".\"$table_name\"" | tr -d ' ')
    
    # Calculate final percentage
    percent=$((count * 100 / total_rows))
    
    log "${GREEN}Successfully loaded $(printf "%'d" $count) rows ($percent%) into $DB_SCHEMA.$table_name${NC}"
    
    # Analyze table for better query performance
    log "Analyzing table for optimal query performance..."
    psql -h "$DB_HOST" -p "$DB_PORT" -U "$DB_USER" -d "$DB_NAME" -c "ANALYZE \"$DB_SCHEMA\".\"$table_name\";"
    
    return 0
}

# Main script
if [ -z "$SYNTHEA_DATA_DIR" ]; then
    log "${RED}Error: SYNTHEA_DATA_DIR environment variable not set${NC}"
    exit 1
fi

# Create schema if it doesn't exist
psql -h "$DB_HOST" -p "$DB_PORT" -U "$DB_USER" -d "$DB_NAME" -c "CREATE SCHEMA IF NOT EXISTS \"$DB_SCHEMA\";"

# Process arguments
force_flag=""
specific_files=()

for arg in "$@"; do
    if [ "$arg" = "--force" ]; then
        force_flag="--force"
    elif [[ "$arg" == *.csv ]]; then
        specific_files+=("$arg")
    fi
done

# Function to estimate time remaining
estimate_time_remaining() {
    local elapsed_time=$1
    local progress_percent=$2
    
    if [ "$progress_percent" -eq 0 ]; then
        echo "Calculating..."
        return
    fi
    
    local total_estimated_time=$((elapsed_time * 100 / progress_percent))
    local remaining_time=$((total_estimated_time - elapsed_time))
    
    # Format time
    local hours=$((remaining_time / 3600))
    local minutes=$(((remaining_time % 3600) / 60))
    local seconds=$((remaining_time % 60))
    
    printf "%02d:%02d:%02d" $hours $minutes $seconds
}

# Process files
if [ ${#specific_files[@]} -eq 0 ]; then
    # Process all CSV files in size order (smallest to largest for faster feedback)
    log "${BLUE}Finding all CSV files and sorting by size...${NC}"
    cd "$SYNTHEA_DATA_DIR" || exit 1
    readarray -t files < <(find . -maxdepth 1 -name "*.csv" -type f -printf "%s %p\n" | sort -n | cut -d' ' -f2-)
    cd - > /dev/null || exit 1
    
    # Fix file paths to be relative
    for i in "${!files[@]}"; do
        files[$i]="$(basename "${files[$i]}")"
    done
    
    total_files=${#files[@]}
    log "${GREEN}Found $total_files CSV files to process${NC}"
    
    # Display file list with sizes
    log "Files to process (in order):"
    cd "$SYNTHEA_DATA_DIR" || exit 1
    for file in "${files[@]}"; do
        size_mb=$(( $(stat --format=%s "$file") / 1024 / 1024 ))
        printf "  - %-30s %'8d MB\n" "$file" "$size_mb"
    done
    
    # Start processing timer
    start_time=$(date +%s)
    
    # Process each file
    for ((i=0; i<${#files[@]}; i++)); do
        file="${files[$i]}"
        
        # Calculate overall progress
        overall_percent=$(( (i * 100) / total_files ))
        elapsed_time=$(($(date +%s) - start_time))
        remaining_time=$(estimate_time_remaining "$elapsed_time" "$overall_percent")
        
        # Display overall progress header
        printf "\n${BLUE}========== OVERALL PROGRESS: [%3d%%] File %d of %d ===========${NC}\n" "$overall_percent" "$((i+1))" "$total_files"
        printf "${YELLOW}Elapsed: %s | Remaining: %s${NC}\n\n" "$(printf '%02d:%02d:%02d' $((elapsed_time/3600)) $(((elapsed_time%3600)/60)) $((elapsed_time%60)))" "$remaining_time"
        
        # Process the file
        load_csv "$file" "$force_flag"
        
        # Check if we need to continue
        if [ $? -ne 0 ]; then
            log "${RED}Error processing $file. Continuing with next file...${NC}"
        fi
    done
    
    # Calculate total time
    total_time=$(($(date +%s) - start_time))
    hours=$((total_time / 3600))
    minutes=$(((total_time % 3600) / 60))
    seconds=$((total_time % 60))
    
    log "${GREEN}All CSV files processed successfully in $(printf '%02d:%02d:%02d' $hours $minutes $seconds)${NC}"
else
    # Process specific files
    cd "$SYNTHEA_DATA_DIR" || exit 1
    
    total_files=${#specific_files[@]}
    log "${BLUE}Processing $total_files specific CSV files${NC}"
    
    # Start processing timer
    start_time=$(date +%s)
    
    for ((i=0; i<${#specific_files[@]}; i++)); do
        file="${specific_files[$i]}"
        
        # Calculate overall progress
        overall_percent=$(( (i * 100) / total_files ))
        elapsed_time=$(($(date +%s) - start_time))
        remaining_time=$(estimate_time_remaining "$elapsed_time" "$overall_percent")
        
        # Display overall progress header
        printf "\n${BLUE}========== OVERALL PROGRESS: [%3d%%] File %d of %d ===========${NC}\n" "$overall_percent" "$((i+1))" "$total_files"
        printf "${YELLOW}Elapsed: %s | Remaining: %s${NC}\n\n" "$(printf '%02d:%02d:%02d' $((elapsed_time/3600)) $(((elapsed_time%3600)/60)) $((elapsed_time%60)))" "$remaining_time"
        
        # Process the file
        load_csv "$file" "$force_flag"
        
        # Check if we need to continue
        if [ $? -ne 0 ]; then
            log "${RED}Error processing $file. Continuing with next file...${NC}"
        fi
    done
    
    # Calculate total time
    total_time=$(($(date +%s) - start_time))
    hours=$((total_time / 3600))
    minutes=$(((total_time % 3600) / 60))
    seconds=$((total_time % 60))
    
    log "${GREEN}All specified CSV files processed successfully in $(printf '%02d:%02d:%02d' $hours $minutes $seconds)${NC}"
fi

log "${GREEN}All CSV files processed successfully${NC}"
