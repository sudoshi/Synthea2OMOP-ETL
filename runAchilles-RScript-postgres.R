#load Libraries

library(SqlRender)
library(DatabaseConnector)
library(Achilles)

#Db Connection

path_to_driver <- "/home/acumenus/GitHub/drivers"

connectionDetails <- createConnectionDetails(
  dbms = "postgresql",
  server = "localhost/omop",
  port = "5440",
  user = "postgres",
  password = "acumenus",
  pathToDriver = path_to_driver
)

#Generate OHDSI Achilles analysis
results <- achilles(
  connectionDetails,
  cdmDatabaseSchema = "synpuf",
  resultsDatabaseSchema = "synpuf_results",
  vocabDatabaseSchema = "synpuf",
  sourceName = "omop",
  createTable = TRUE,
  smallCellCount = 5,
  cdmVersion = "5.4",
  createIndices = TRUE,
  numThreads = 1,
  tempAchillesPrefix = "tmpach",
  dropScratchTables = TRUE,
  sqlOnly = FALSE,
  outputFolder = "output",
  verboseMode = TRUE,
  optimizeAtlasCache = TRUE,
  defaultAnalysesOnly = TRUE,
  updateGivenAnalysesOnly = FALSE,
  excludeAnalysisIds = FALSE,
  sqlDialect = "postgresql"
)

