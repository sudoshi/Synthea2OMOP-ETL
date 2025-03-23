#!/bin/bash
# monitor_etl_progress.sh - Shell script to monitor ETL progress

# Default values
INTERVAL=5
CONTINUOUS=true
PROCESS_NAME=""

# Parse command line arguments
while [[ $# -gt 0 ]]; do
  case $1 in
    --interval|-i)
      INTERVAL="$2"
      shift 2
      ;;
    --process|-p)
      PROCESS_NAME="$2"
      shift 2
      ;;
    --once|-o)
      CONTINUOUS=false
      shift
      ;;
    --help|-h)
      echo "Usage: $0 [OPTIONS]"
      echo "Monitor ETL progress"
      echo ""
      echo "Options:"
      echo "  -i, --interval SECONDS   Update interval in seconds (default: 5)"
      echo "  -p, --process NAME       Filter by process name"
      echo "  -o, --once               Show progress once and exit (don't continuously monitor)"
      echo "  -h, --help               Show this help message"
      exit 0
      ;;
    *)
      echo "Unknown option: $1"
      echo "Use --help for usage information"
      exit 1
      ;;
  esac
done

# Build command
CMD="python3 etl_progress_tracking.py"

if [ -n "$PROCESS_NAME" ]; then
  CMD="$CMD --process \"$PROCESS_NAME\""
fi

CMD="$CMD --interval $INTERVAL"

if [ "$CONTINUOUS" = true ]; then
  CMD="$CMD --continuous"
fi

# Run command
eval $CMD

