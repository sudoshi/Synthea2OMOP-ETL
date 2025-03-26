#!/bin/bash
# run_interactive_pipeline.sh - Shell script wrapper for the interactive Synthea to OMOP ETL pipeline

# Make script executable if it's not already
if [ ! -x "interactive_unified_pipeline.py" ]; then
  chmod +x interactive_unified_pipeline.py
fi

# The Python script will check for required packages and offer to install them

# Display welcome message
echo "╔════════════════════════════════════════════════════════════════╗"
echo "║                                                                ║"
echo "║  Interactive Synthea to OMOP ETL Pipeline                      ║"
echo "║                                                                ║"
echo "╚════════════════════════════════════════════════════════════════╝"
echo ""
echo "This script will guide you through the process of converting"
echo "Synthea synthetic patient data to the OMOP Common Data Model format."
echo ""
echo "For more information, see INTERACTIVE_PIPELINE_README.md"
echo ""

# Pass all arguments to the Python script
python3 interactive_unified_pipeline.py "$@"

# Get exit code
EXIT_CODE=$?

# Display completion message
if [ $EXIT_CODE -eq 0 ]; then
  echo "✅ Interactive pipeline completed successfully."
else
  echo "❌ Interactive pipeline failed with exit code $EXIT_CODE."
  echo "   Check the logs for more information."
fi

exit $EXIT_CODE
