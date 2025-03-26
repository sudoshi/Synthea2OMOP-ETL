#!/usr/bin/env Rscript

# Load required libraries
library(SqlRender)
library(DatabaseConnector)
library(Achilles)
library(jsonlite)

# Parse command line arguments
args <- commandArgs(trailingOnly = TRUE)
config_file <- args[1]

# Log function
log_message <- function(message) {
  timestamp <- format(Sys.time(), "%Y-%m-%d %H:%M:%S")
  cat(paste0("[", timestamp, "] ", message, "\n"))
}

log_message("Starting Achilles analysis")
log_message(paste("Using config file:", config_file))

# Read configuration
if (file.exists(config_file)) {
  config <- fromJSON(config_file)
  log_message("Configuration loaded successfully")
} else {
  stop(paste("Config file not found:", config_file))
}

# Setup progress tracking
progress_file <- config$progressFile
if (is.null(progress_file)) {
  progress_file <- "/app/progress.json"
}

results_file <- config$resultsFile
if (is.null(results_file)) {
  results_file <- "/app/results.json"
}

# Initialize progress file
write("", file = progress_file)

# Simple progress tracking
log_message("Using simplified progress tracking (no callback)")

# Setup connection
log_message("Setting up database connection")
connectionDetails <- createConnectionDetails(
  dbms = config$dbms,
  server = config$server,
  port = config$port,
  user = config$user,
  password = config$password,
  pathToDriver = config$pathToDriver
)

# Run Achilles
log_message("Starting Achilles execution")
tryCatch({
  results <- achilles(
    connectionDetails = connectionDetails,
    cdmDatabaseSchema = config$cdmDatabaseSchema,
    resultsDatabaseSchema = config$resultsDatabaseSchema,
    vocabDatabaseSchema = config$vocabDatabaseSchema,
    sourceName = config$sourceName,
    createTable = config$createTable,
    smallCellCount = config$smallCellCount,
    cdmVersion = config$cdmVersion,
    createIndices = config$createIndices,
    numThreads = config$numThreads,
    tempAchillesPrefix = config$tempAchillesPrefix,
    dropScratchTables = config$dropScratchTables,
    sqlOnly = config$sqlOnly,
    outputFolder = config$outputFolder,
    verboseMode = config$verboseMode,
    optimizeAtlasCache = config$optimizeAtlasCache,
    defaultAnalysesOnly = config$defaultAnalysesOnly,
    updateGivenAnalysesOnly = config$updateGivenAnalysesOnly,
    excludeAnalysisIds = config$excludeAnalysisIds,
    sqlDialect = config$sqlDialect
  )
  
  # Save results summary
  log_message("Achilles execution completed successfully")
  log_message(paste("Saving results to", results_file))
  
  # Convert results to a format that can be serialized to JSON
  results_json <- list(
    status = "completed",
    timestamp = format(Sys.time(), "%Y-%m-%d %H:%M:%S"),
    summary = list(
      analyses_performed = length(results$analysesPerformed),
      execution_time = results$executionTime,
      source_name = results$sourceName
    )
  )
  
  write(
    toJSON(results_json, auto_unbox = TRUE, pretty = TRUE), 
    file = results_file
  )
  
  log_message("Results saved successfully")
  
}, error = function(e) {
  # Log error
  error_message <- paste("Error in Achilles execution:", e$message)
  log_message(error_message)
  
  # Save error to results file
  error_json <- list(
    status = "error",
    timestamp = format(Sys.time(), "%Y-%m-%d %H:%M:%S"),
    error = error_message
  )
  
  write(
    toJSON(error_json, auto_unbox = TRUE, pretty = TRUE), 
    file = results_file
  )
  
  # Exit with error
  stop(error_message)
})

log_message("Achilles process completed")
