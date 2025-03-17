# Synthea2OMOP ETL Monitoring Dashboard

This dashboard provides a comprehensive interface for monitoring the progress of the Synthea to OMOP ETL (Extract, Transform, Load) process and running OHDSI Achilles data quality analyses.

## Components

The dashboard consists of the following components:

1. **Frontend**: A React application with Material-UI for the user interface
2. **Backend**: An Express.js API server that interfaces with the database
3. **Python API**: A Flask API that provides ETL progress data
4. **Achilles R Service**: An R service for running OHDSI Achilles analyses
5. **Database**: A PostgreSQL database that stores the ETL data

## Getting Started

### Prerequisites

- Docker and Docker Compose
- Node.js (for local development)
- Python 3.9+ (for local development)
- PostgreSQL (for local development)

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

#### Running Components Individually

##### Python API

1. Run the Python API:
   ```
   ./run_api.sh
   ```

2. The API will be available at http://localhost:5081

3. Check ETL progress:
   ```
   ./check_etl_progress.sh
   ```

##### Backend

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

##### Frontend

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

4. Access the frontend at http://localhost:3080

## API Endpoints

### Python API

- `GET /api/etl/status`: Get ETL status
- `GET /api/db/tables?schema=<schema>`: Get tables in a schema
- `GET /api/db/data?schema=<schema>&table=<table>&limit=<limit>&offset=<offset>`: Get table data
- `POST /api/db/query`: Execute SQL query

### Express.js Backend

- `GET /api/etl/status`: Get ETL status
- `GET /api/db/tables?schema=<schema>`: Get tables in a schema
- `GET /api/db/data?schema=<schema>&table=<table>&limit=<limit>&offset=<offset>`: Get table data
- `POST /api/db/query`: Execute SQL query

## Dashboard Features

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
├── docker-compose.yml    # Docker Compose configuration
├── etl_api.py            # Python API for ETL monitoring
├── requirements-api.txt  # Python dependencies
├── run_api.sh            # Script to run Python API
├── run_dashboard.sh      # Script to run all services
├── run_achilles.sh       # Script to run Achilles directly
├── check_etl_progress.sh # Script to check ETL progress
└── stop_dashboard.sh     # Script to stop all services
```

## License

This project is licensed under the MIT License - see the LICENSE file for details.
