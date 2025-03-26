#!/bin/bash
# run_unified_pipeline.sh - Shell script wrapper for the unified Synthea to OMOP ETL pipeline

# Define default values
TRACK_PROGRESS=false
MONITOR=false

# Parse arguments to check for progress tracking
for arg in "$@"; do
  case $arg in
    --track-progress)
      TRACK_PROGRESS=true
      ;;
    --monitor)
      MONITOR=true
      ;;
  esac
done

# Make script executable if it's not already
if [ ! -x "run_unified_pipeline.py" ]; then
  chmod +x run_unified_pipeline.py
fi

# Add progress tracking to command if requested
if [ "$TRACK_PROGRESS" = true ]; then
  # Create progress tracking table if it doesn't exist
  echo "Setting up progress tracking..."
  
  # Check if the progress tracking script exists
  if [ ! -f "etl_progress_tracking.py" ]; then
    echo "⚠️ Warning: etl_progress_tracking.py not found. Progress tracking will not be available."
    TRACK_PROGRESS=false
  fi
fi

# Start monitoring in background if requested
if [ "$MONITOR" = true ] && [ "$TRACK_PROGRESS" = true ] && [ -f "monitor_etl_progress.sh" ]; then
  echo "Starting progress monitor in a new terminal..."
  # Try different terminal emulators
  if command -v gnome-terminal &> /dev/null; then
    gnome-terminal -- bash -c "./monitor_etl_progress.sh; echo 'Press Enter to close'; read"
  elif command -v xterm &> /dev/null; then
    xterm -e "./monitor_etl_progress.sh; echo 'Press Enter to close'; read" &
  elif command -v konsole &> /dev/null; then
    konsole --new-tab -e "./monitor_etl_progress.sh; echo 'Press Enter to close'; read" &
  else
    echo "Could not find a suitable terminal emulator. Please run ./monitor_etl_progress.sh manually in another terminal."
  fi
fi

# Pass all arguments to the Python script
python3 run_unified_pipeline.py "$@"

# Get exit code
EXIT_CODE=$?

# Display completion message
if [ $EXIT_CODE -eq 0 ]; then
  echo "✅ Unified pipeline completed successfully."
  
  # Remind about progress tracking
  if [ "$TRACK_PROGRESS" = true ]; then
    echo ""
    echo "Progress tracking information is available in the database."
    echo "Run ./monitor_etl_progress.sh to view the progress summary."
  fi
else
  echo "❌ Unified pipeline failed with exit code $EXIT_CODE."
  
  # Suggest checking progress for debugging
  if [ "$TRACK_PROGRESS" = true ]; then
    echo ""
    echo "Check the progress tracking information for more details:"
    echo "Run ./monitor_etl_progress.sh --once to view the progress summary."
  fi
fi

exit $EXIT_CODE
