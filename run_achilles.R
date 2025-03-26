#!/usr/bin/env Rscript

# Run Achilles analysis on OMOP CDM database
# This script uses the configuration in achilles_config.json

library(Achilles)
library(jsonlite)

# Load configuration
config_file <- "/home/acumenus/GitHub/Synthea2OMOP-ETL/achilles_config.json"
config <- fromJSON(config_file)

# Print configuration
cat("Running Achilles with the following configuration:\n")
cat(paste("  Database:", config$server, "\n"))
cat(paste("  CDM Schema:", config$cdmDatabaseSchema, "\n"))
cat(paste("  Results Schema:", config$resultsDatabaseSchema, "\n"))
cat(paste("  Threads:", config$numThreads, "\n"))
cat("\n")

# Create connection details
connectionDetails <- DatabaseConnector::createConnectionDetails(
  dbms = config$dbms,
  server = config$server,
  port = config$port,
  user = config$user,
  password = config$password,
  pathToDriver = config$pathToDriver
)

# Run Achilles
cat("Starting Achilles analysis...\n")
start_time <- Sys.time()

# Run Achilles
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

end_time <- Sys.time()
execution_time <- difftime(end_time, start_time, units = "mins")

cat("\nAchilles analysis completed!\n")
cat(paste("Execution time:", round(execution_time, 2), "minutes\n"))
cat(paste("Results are available in the", config$resultsDatabaseSchema, "schema\n"))

# Print summary
cat("\nSummary:\n")
cat(paste("  Analyses performed:", length(results$analysesPerformed), "\n"))
cat(paste("  Execution time:", results$executionTime, "\n"))
cat(paste("  Source name:", results$sourceName, "\n"))
