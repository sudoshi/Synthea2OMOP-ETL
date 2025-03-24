# Checkpointed Synthea to OMOP ETL Pipeline

## Overview

This document explains how to use the checkpointed ETL pipeline for converting Synthea output files into OMOP CDM format. The process consists of four major steps, with built-in checkpointing to resume from interruptions.

## The Problem

ETL processes for large datasets can take a long time to complete, and interruptions (server restarts, errors, etc.) can force you to start over. The checkpointed pipeline addresses this issue by:

1. Keeping track of completed steps in a JSON checkpoint file
2. Allowing resumption from the last successful step
3. Avoiding redundant work when restarting the pipeline
4. Providing statistics on the duration of each step

## Using the Checkpointed Pipeline

```bash
# Run the complete pipeline with checkpointing
bash/run_complete_synthea_etl_with_checkpoints.sh

# Run with custom options
bash/run_complete_synthea_etl_with_checkpoints.sh --input-dir ./synthea-output \
                                                 --processed-dir ./synthea-processed \
                                                 --force-load \
                                                 --debug
```

## Checkpoint-Specific Options

In addition to the standard pipeline options, the checkpointed version adds:

- `--checkpoint-file FILE`: Path to the checkpoint file (default: `.synthea_etl_checkpoint.json`)
- `--force-restart`: Ignore existing checkpoints and start from the beginning

## How Checkpointing Works

The checkpointing system works as follows:

1. Before running a step, the script checks if it's already marked as completed in the checkpoint file
2. If a step is completed, it's skipped (unless `--force-restart` is used)
3. After each step completes successfully, it's marked in the checkpoint file
4. The checkpoint file includes statistics about each step, such as duration

## Checkpoint File Format

The checkpoint file uses a simple JSON format:

```json
{
  "completed_steps": ["preprocessing", "staging_load", "typing_transform"],
  "last_updated": "2025-03-24T14:10:45-04:00",
  "stats": {
    "preprocessing": {"duration_seconds": 120},
    "staging_load": {"duration_seconds": 305},
    "typing_transform": {"duration_seconds": 187}
  }
}
```

This allows the pipeline to know which steps have been completed and provides valuable performance metrics.

## Resuming After Interruptions

If the pipeline is interrupted (due to errors, server restarts, etc.):

1. Simply run the same command again
2. The pipeline will automatically detect which steps have been completed
3. Only the remaining steps will be executed
4. Use `--debug` for detailed information about which steps are being skipped

Example of resuming:

```bash
# First run (gets interrupted during OMOP transform)
bash/run_complete_synthea_etl_with_checkpoints.sh --debug

# Later, resume the pipeline
bash/run_complete_synthea_etl_with_checkpoints.sh --debug
# Output will show:
#   STEP 1: [ALREADY COMPLETED] Preprocessing Synthea CSV files
#   STEP 2: [ALREADY COMPLETED] Loading processed data into staging schema
#   STEP 3: [ALREADY COMPLETED] Transforming staging data to properly typed tables
#   STEP 4: Transforming typed data to OMOP CDM format
```

## Custom Checkpoint Files

You can use custom checkpoint files for different environments or datasets:

```bash
# Run with a custom checkpoint file
bash/run_complete_synthea_etl_with_checkpoints.sh --checkpoint-file ./checkpoints/production_etl.json

# Run development ETL with a different checkpoint file
bash/run_complete_synthea_etl_with_checkpoints.sh --checkpoint-file ./checkpoints/dev_etl.json
```

## Troubleshooting

### Common Issues

1. **Checkpoint Not Being Recognized**: 
   - Check file permissions on the checkpoint file
   - Validate that the JSON format is correct

2. **Forced Restart Not Working**:
   - Use both `--force-restart` and `--debug` to see detailed logging

3. **Steps Being Repeated Despite Checkpoints**:
   - Check if the checkpoint file path is correctly specified
   - Examine the checkpoint file to ensure steps are properly recorded

## Integration with Existing Systems

The checkpoint file can be used by other systems to monitor ETL progress:

- Monitoring dashboards can parse the JSON to display status
- Alerts can be triggered based on duration statistics
- Automation tools can check for completion without running the pipeline

## Performance Implications

The checkpoint functionality adds minimal overhead to the ETL process:

- File operations for reading/writing checkpoints are negligible
- Step skipping can dramatically reduce processing time during restarts
- Duration statistics help identify bottlenecks in the pipeline
