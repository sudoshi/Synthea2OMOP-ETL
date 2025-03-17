const express = require('express');
const cors = require('cors');
const { Pool } = require('pg');
const dotenv = require('dotenv');

// Load environment variables
dotenv.config();

const app = express();
const port = process.env.PORT || 5000;

// Middleware
app.use(cors());
app.use(express.json());

// Database connection
const pool = new Pool({
  host: process.env.DB_HOST || 'localhost',
  port: process.env.DB_PORT || 5432,
  database: process.env.DB_NAME || 'synthea',
  user: process.env.DB_USER || 'postgres',
  password: process.env.DB_PASSWORD || 'acumenus',
});

// Test database connection
pool.query('SELECT NOW()', (err, res) => {
  if (err) {
    console.error('Database connection error:', err.stack);
  } else {
    console.log('Database connected successfully');
  }
});

// Routes
app.get('/', (req, res) => {
  res.json({ message: 'Welcome to Synthea2OMOP ETL API' });
});

// Get ETL status
app.get('/api/etl/status', async (req, res) => {
  try {
    // In a real implementation, this would query the database for ETL status
    // For now, we'll return mock data
    const status = {
      isRunning: true,
      overallProgress: 20.13,
      elapsedTime: '0h 32m 47s',
      estimatedTimeRemaining: '2h 10m',
      currentQuery: 'CREATE INDEX IF NOT EXISTS idx_observations_typed_code_category ON population.observations_typed (code, category)',
      queryStartTime: '2025-03-17 03:20:35',
      queryDuration: '0:02:33',
      systemResources: {
        cpuUsage: 18.9,
        memoryUsage: 36.7,
        diskUsage: 39.2,
      },
      tableProgress: [
        { sourceTable: 'patients_typed', sourceCount: 888463, targetTable: 'person', targetCount: 5823, progress: 0.66 },
        { sourceTable: 'encounters_typed', sourceCount: 67018822, targetTable: 'visit_occurrence', targetCount: 67018822, progress: 100 },
        { sourceTable: 'conditions_typed', sourceCount: 37624858, targetTable: 'condition_occurrence', targetCount: 0, progress: 0 },
        { sourceTable: 'medications_typed', sourceCount: 64535201, targetTable: 'drug_exposure', targetCount: 0, progress: 0 },
        { sourceTable: 'procedures_typed', sourceCount: 172916715, targetTable: 'procedure_occurrence', targetCount: 0, progress: 0 },
        { sourceTable: 'observations_typed', sourceCount: 896011741, targetTable: 'measurement', targetCount: 0, progress: '-' },
        { sourceTable: '', sourceCount: 0, targetTable: 'observation', targetCount: 0, progress: '-' },
      ],
      etlSteps: [
        { step: 'Conditions (SNOMED-CT)', started: '02:40:16', completed: '02:40:16', duration: '0m 0s', status: 'Completed', rows: 251, error: null },
        { step: 'Medications (RxNorm)', started: '02:40:49', completed: '02:40:49', duration: '0m 0s', status: 'Completed', rows: 417, error: null },
        { step: 'Procedures (SNOMED-CT)', started: '02:42:43', completed: '02:42:43', duration: '0m 0s', status: 'Completed', rows: 254, error: null },
        { step: 'Observations - Measurement (LOINC)', started: '02:45:00', completed: '02:45:00', duration: '0m 0s', status: 'Completed', rows: null, error: null },
        { step: 'Observations - Observation (LOINC)', started: '02:45:00', completed: '02:45:00', duration: '0m 0s', status: 'Completed', rows: null, error: null },
        { step: 'Unmapped conditions', started: '02:45:00', completed: '02:45:00', duration: '0m 0s', status: 'Completed', rows: null, error: null },
        { step: 'Unmapped medications', started: '02:45:07', completed: '02:45:07', duration: '0m 0s', status: 'Completed', rows: null, error: null },
        { step: 'Unmapped procedures', started: '02:45:28', completed: '02:45:28', duration: '0m 0s', status: 'Completed', rows: 2, error: null },
        { step: 'Unmapped observations - Measurement', started: '02:46:21', completed: '02:46:21', duration: '0m 0s', status: 'Completed', rows: null, error: null },
        { step: 'Unmapped observations - Observation', started: '02:46:21', completed: '02:46:21', duration: '0m 0s', status: 'Completed', rows: null, error: null },
      ],
    };

    res.json(status);
  } catch (error) {
    console.error('Error fetching ETL status:', error);
    res.status(500).json({ error: 'Failed to fetch ETL status' });
  }
});

// Get database tables
app.get('/api/db/tables', async (req, res) => {
  try {
    const schema = req.query.schema || 'omop';
    
    // In a real implementation, this would query the database for tables
    // For now, we'll return mock data
    const tables = {
      omop: ['person', 'visit_occurrence', 'condition_occurrence', 'drug_exposure', 'procedure_occurrence', 'measurement', 'observation'],
      staging: ['staging_person', 'staging_visit', 'staging_condition', 'staging_drug', 'staging_procedure', 'staging_observation'],
      population: ['patients_typed', 'encounters_typed', 'conditions_typed', 'medications_typed', 'procedures_typed', 'observations_typed'],
    };

    res.json(tables[schema] || []);
  } catch (error) {
    console.error('Error fetching tables:', error);
    res.status(500).json({ error: 'Failed to fetch tables' });
  }
});

// Get table data
app.get('/api/db/data', async (req, res) => {
  try {
    const { schema, table, limit = 10, offset = 0 } = req.query;
    
    // In a real implementation, this would query the database for table data
    // For now, we'll return mock data
    const mockData = {
      person: [
        { person_id: 1, gender_concept_id: 8507, year_of_birth: 1975, race_concept_id: 8527, ethnicity_concept_id: 38003564 },
        { person_id: 2, gender_concept_id: 8532, year_of_birth: 1982, race_concept_id: 8516, ethnicity_concept_id: 38003564 },
        { person_id: 3, gender_concept_id: 8507, year_of_birth: 1990, race_concept_id: 8527, ethnicity_concept_id: 38003563 },
        { person_id: 4, gender_concept_id: 8532, year_of_birth: 1965, race_concept_id: 8516, ethnicity_concept_id: 38003564 },
        { person_id: 5, gender_concept_id: 8507, year_of_birth: 1945, race_concept_id: 8527, ethnicity_concept_id: 38003564 },
      ],
      visit_occurrence: [
        { visit_occurrence_id: 1, person_id: 1, visit_concept_id: 9201, visit_start_date: '2020-01-01', visit_end_date: '2020-01-03' },
        { visit_occurrence_id: 2, person_id: 1, visit_concept_id: 9202, visit_start_date: '2020-02-15', visit_end_date: '2020-02-15' },
        { visit_occurrence_id: 3, person_id: 2, visit_concept_id: 9203, visit_start_date: '2020-01-10', visit_end_date: '2020-01-10' },
        { visit_occurrence_id: 4, person_id: 3, visit_concept_id: 9201, visit_start_date: '2020-03-20', visit_end_date: '2020-03-25' },
        { visit_occurrence_id: 5, person_id: 4, visit_concept_id: 9202, visit_start_date: '2020-04-05', visit_end_date: '2020-04-05' },
      ],
    };

    const data = mockData[table] || [];
    const total = data.length;
    const result = data.slice(offset, offset + limit);

    res.json({
      data: result,
      total,
      limit: parseInt(limit),
      offset: parseInt(offset),
    });
  } catch (error) {
    console.error('Error fetching table data:', error);
    res.status(500).json({ error: 'Failed to fetch table data' });
  }
});

// Execute SQL query
app.post('/api/db/query', async (req, res) => {
  try {
    const { sql } = req.body;
    
    // In a real implementation, this would execute the SQL query
    // For now, we'll return mock data
    res.json({
      data: [
        { person_id: 1, gender_concept_id: 8507, year_of_birth: 1975 },
        { person_id: 2, gender_concept_id: 8532, year_of_birth: 1982 },
        { person_id: 3, gender_concept_id: 8507, year_of_birth: 1990 },
      ],
      rowCount: 3,
      fields: [
        { name: 'person_id', dataType: 'integer' },
        { name: 'gender_concept_id', dataType: 'integer' },
        { name: 'year_of_birth', dataType: 'integer' },
      ],
    });
  } catch (error) {
    console.error('Error executing query:', error);
    res.status(500).json({ error: 'Failed to execute query' });
  }
});

// Start server
app.listen(port, () => {
  console.log(`Server is running on port ${port}`);
});
