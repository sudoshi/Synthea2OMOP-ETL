# Synthea2OMOP ETL Dashboard

A web-based dashboard for monitoring and managing the Synthea to OMOP ETL (Extract, Transform, Load) process, with integrated database maintenance tools and OHDSI Achilles data quality analysis.

## Overview

This project provides a comprehensive dashboard for monitoring the progress of the Synthea to OMOP ETL process. It includes:

- Real-time ETL progress monitoring
- System resource usage tracking
- Data exploration and visualization
- ETL configuration management
- Database maintenance tools
- OHDSI Achilles data quality analysis

## Architecture

The project consists of seven main components:

1. **Frontend**: A React application with Material-UI for the user interface
2. **Backend**: An Express.js API server that interfaces with the database
3. **Python API**: A Flask API that provides ETL progress data and database operations
4. **Database**: A PostgreSQL database that stores the ETL data
5. **Synthea**: A containerized version of the Synthea patient generator
6. **Synthea API**: A Flask API for controlling the Synthea container
7. **Achilles R Service**: An R service for running OHDSI Achilles analyses

## Getting Started

### Prerequisites

- Docker and Docker Compose
- Node.js (for local development)
- PostgreSQL (for local development)

### Installation

1. Clone the repository:
   ```
   git clone https://github.com/acumenus/Synthea2OMOP-ETL.git
   cd Synthea2OMOP-ETL
   ```

2. Start the application using Docker Compose:
   ```
   docker-compose up
   ```

3. Access the dashboard at http://localhost:3080

### Running the Dashboard

#### Using Docker Compose (Recommended)

1. Start all services:
   ```
   ./run_dashboard.sh
   ```

2. Access the dashboard at http://localhost:3080

3. To stop all services:
   ```
   ./stop_dashboard.sh
   ```

### Generating Synthea Data

You can generate Synthea data in two ways:

1. **Using the Dashboard**: Navigate to the Synthea Configuration tab in the dashboard and configure the parameters for data generation. Click the "Generate Data" button to start the process.

2. **Using the Command Line**: Use the provided script to generate data and run the ETL process:
   ```
   ./run_synthea_and_etl.sh --population 1000 --state "Massachusetts" --city "Bedford"
   ```

   Available options:
   - `-p, --population <number>`: Number of patients to generate (default: 1000)
   - `-s, --seed <number>`: Random seed for reproducibility (default: 1)
   - `-S, --state <name>`: State name (default: Massachusetts)
   - `-c, --city <name>`: City name (default: Bedford)
   - `-g, --gender <M|F>`: Gender filter (default: both)
   - `-a, --age <range>`: Age range (default: all)
   - `-m, --module <name>`: Module to run (default: all)

### Database Initialization and Vocabulary Loading

Before running the ETL process, you need to initialize the database with the OMOP CDM schema and load the vocabulary files. You can do this in several ways:

1. **Using the Unified Pipeline (Recommended)**: Run the entire process (database initialization, vocabulary loading, and ETL) with a single command:
   ```
   ./run_unified_pipeline.sh
   ```

2. **Using the Database Initialization Script**: Initialize the database and load vocabulary separately:
   ```
   ./run_init_with_vocab.sh
   ```

The optimized database initialization and vocabulary loading process includes several improvements:

- **Parallel Processing**: Processes vocabulary files in parallel
- **Batch Processing**: Processes large files in batches to reduce memory usage
- **Direct Loading**: Uses PostgreSQL's COPY command for bulk loading
- **Special Handling**: Implements special handling for tables with long text values
- **Dependency Checks**: Ensures that vocabulary tables exist and contain required concept IDs

For more detailed information about the database initialization and vocabulary loading process, see the [VOCABULARY_LOADING_README.md](VOCABULARY_LOADING_README.md) file.

### Running the ETL Process

The ETL (Extract, Transform, Load) process converts Synthea data to the OMOP Common Data Model format. You can run the ETL process in several ways:

1. **Using the Unified Pipeline (Recommended)**: Run the entire process (database initialization, vocabulary loading, and ETL) with a single command:
   ```
   ./run_unified_pipeline.sh
   ```

2. **Using the Dashboard**: Navigate to the ETL Pipeline tab in the dashboard and click the "Run ETL" button.

3. **Using the Standard ETL Process**: Use the provided script to run the standard ETL process:
   ```
   ./run_etl.sh
   ```

4. **Using the Optimized ETL Process**: Use the optimized ETL process for faster performance:
   ```
   ./run_optimized_import.sh
   ```

#### Standard ETL Process

The standard ETL process consists of several steps:
1. Adding missing indexes for better performance
2. Transforming person data
3. Adding indexes to population and staging tables
4. Populating concept map
5. Creating observation period data
6. Transforming visit data
7. Transforming condition data
8. Transforming medication data
9. Transforming procedure data
10. Transforming measurement and observation data
11. Transferring non-numeric measurements to observation
12. Transforming death data
13. Transforming cost data
14. Verifying record counts
15. Checking for unmapped source codes
16. Verifying date ranges
17. Verifying demographics
18. Analyzing tables for query optimization

For more detailed information about the standard ETL process, see the [ETL_README.md](ETL_README.md) file.

#### Optimized ETL Process

The optimized ETL process implements several improvements:

- **Direct Import Pipeline**: Eliminates intermediate staging tables
- **Parallel Processing**: Executes independent ETL steps simultaneously
- **SQL Optimization**: Uses bulk operations and optimized queries
- **Combined Measurement/Observation Handling**: Directly routes data to appropriate tables

Benefits of the optimized process:
- Significantly faster processing time (up to 80% faster)
- Reduced complexity with fewer steps
- Better resource utilization
- Improved reliability with comprehensive error handling

For more detailed information about the optimized ETL process, see the [OPTIMIZED_ETL_README.md](OPTIMIZED_ETL_README.md) file.

#### Resetting OMOP Tables

If you need to reset the OMOP tables for a fresh ETL run, use the provided script:
```
./reset_omop_tables.sh
```

This script will truncate all OMOP tables and reset sequences, allowing you to start the ETL process from scratch.

### Running Achilles Analysis

You can run OHDSI Achilles data quality analysis in two ways:

1. **Using the Dashboard**: Navigate to the Achilles Configuration tab in the dashboard, configure the parameters, and click the "Run Analysis" button.

2. **Using the Command Line**: Use the provided script:
   ```
   ./run_achilles.sh
   ```

### Database Maintenance

The project includes tools for PostgreSQL database maintenance:

1. **Using the Dashboard**: Navigate to the Database Maintenance tab to access maintenance tools.

2. **Using the Command Line**: Use the provided script:
   ```
   ./run_db_maintenance.sh
   ```

   This script provides interactive prompts to:
   - Terminate idle transactions
   - Resolve blocking chains
   - Cancel long-running ETL processes
   - Monitor system recovery
   - Optimize PostgreSQL configuration

### Local Development

#### Frontend

1. Navigate to the frontend directory:
   ```
   cd frontend
   ```

2. Install dependencies:
   ```
   npm install
   ```

3. Start the development server:
   ```
   npm start
   ```

4. Access the frontend at http://localhost:3000

#### Backend

1. Navigate to the backend directory:
   ```
   cd backend
   ```

2. Install dependencies:
   ```
   npm install
   ```

3. Start the development server:
   ```
   npm run dev
   ```

4. The API will be available at http://localhost:5080

#### Python API

1. Run the Python API:
   ```
   ./run_api.sh
   ```

2. The API will be available at http://localhost:5081

## Features

### ETL Monitoring

- Real-time progress tracking
- Table-level progress visualization
- Step-by-step ETL process monitoring
- System resource usage monitoring
- Enhanced monitoring with colorized output and detailed metrics

### Data Exploration

- Browse data in OMOP tables
- Execute custom SQL queries
- View data quality metrics
- Explore Achilles analysis results

### Configuration

- Configure database connections
- Set ETL parameters
- Manage ETL process
- Configure Synthea data generation parameters
- Configure and run Achilles analyses

### Database Maintenance

- Identify and terminate idle transactions
- Resolve blocking chains
- Cancel long-running ETL processes
- Monitor system recovery
- Optimize PostgreSQL configuration
- Implement batch processing for ETL operations

### Achilles Analysis

- Configure and run OHDSI Achilles analyses
- Monitor analysis progress in real-time
- View and explore Achilles results
- Export results data for further analysis

## Project Structure

```
.
├── api/                  # Python API
│   └── Dockerfile        # Dockerfile for Python API
├── backend/              # Express.js backend
│   ├── src/              # Source code
│   ├── .env              # Environment variables
│   └── Dockerfile        # Dockerfile for backend
├── frontend/             # React frontend
│   ├── public/           # Static files
│   ├── src/              # Source code
│   └── Dockerfile        # Dockerfile for frontend
├── achilles/             # Achilles R service
│   ├── scripts/          # R scripts
│   ├── drivers/          # JDBC drivers
│   ├── output/           # Output directory
│   └── Dockerfile        # Dockerfile for R service
├── sql/                  # SQL scripts
│   ├── db_maintenance/   # Database maintenance scripts
│   ├── etl/              # ETL scripts
│   │   ├── direct_transfer.sql      # Direct SQL approach for transferring non-numeric measurements
│   │   ├── run_all_etl.sql          # Main ETL script
│   │   ├── transfer_non_numeric_to_observation.sql # Original transfer script
│   │   └── redundant/               # Redundant scripts kept for reference
│   ├── init/             # Initialization scripts
│   ├── omop_ddl/         # OMOP CDM DDL scripts
│   ├── staging/          # Staging scripts
│   └── synthea_typing/   # Synthea typing scripts
├── synthea/              # Synthea container
│   ├── api/              # Synthea API
│   └── Dockerfile        # Dockerfile for Synthea
├── scripts/              # Utility scripts
├── utils/                # Python utilities
├── backup/               # Backup of iteration files
│   ├── scripts/          # Backup of Python and shell scripts
│   └── sql/              # Backup of SQL scripts
├── docker-compose.yml    # Docker Compose configuration
├── ETL_README.md         # Detailed documentation of the ETL process
├── ETL_SUMMARY.md        # Summary of the ETL improvements
├── run_dashboard.sh      # Script to run all services
├── run_api.sh            # Script to run Python API
├── run_achilles.sh       # Script to run Achilles directly
├── run_db_maintenance.sh # Script to run database maintenance
├── run_direct_transfer.sh # Script to run the direct transfer SQL
├── run_etl.sh            # Script to run the ETL process
├── run_synthea_and_etl.sh # Script to run Synthea and ETL
├── reset_omop_tables.sh  # Script to reset OMOP tables
└── stop_dashboard.sh     # Script to stop all services
```

## License

This project is licensed under the MIT License - see the LICENSE file for details.

## Acknowledgements

- [Synthea](https://github.com/synthetichealth/synthea) - Synthetic patient generator
- [OMOP CDM](https://www.ohdsi.org/data-standardization/the-common-data-model/) - Common Data Model for standardizing healthcare data
- [OHDSI Achilles](https://github.com/OHDSI/Achilles) - Data quality assessment tool
- [Material-UI](https://mui.com/) - React UI framework
