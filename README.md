# Synthea2OMOP ETL Dashboard

A web-based dashboard for monitoring and managing the Synthea to OMOP ETL (Extract, Transform, Load) process.

## Overview

This project provides a comprehensive dashboard for monitoring the progress of the Synthea to OMOP ETL process. It includes:

- Real-time ETL progress monitoring
- System resource usage tracking
- Data exploration and visualization
- ETL configuration management

## Architecture

The project consists of five main components:

1. **Frontend**: A React application with Material-UI for the user interface
2. **Backend**: An Express.js API server that interfaces with the database
3. **Database**: A PostgreSQL database that stores the ETL data
4. **Synthea**: A containerized version of the Synthea patient generator
5. **Synthea API**: A Flask API for controlling the Synthea container

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

4. The API will be available at http://localhost:5000

## Features

### ETL Monitoring

- Real-time progress tracking
- Table-level progress visualization
- Step-by-step ETL process monitoring
- System resource usage monitoring

### Data Exploration

- Browse data in OMOP tables
- Execute custom SQL queries
- View data quality metrics

### Configuration

- Configure database connections
- Set ETL parameters
- Manage ETL process
- Configure Synthea data generation parameters

## License

This project is licensed under the MIT License - see the LICENSE file for details.

## Acknowledgements

- [Synthea](https://github.com/synthetichealth/synthea) - Synthetic patient generator
- [OMOP CDM](https://www.ohdsi.org/data-standardization/the-common-data-model/) - Common Data Model for standardizing healthcare data
- [Material-UI](https://mui.com/) - React UI framework
