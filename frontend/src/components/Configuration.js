import React, { useState } from 'react';
import {
  Container,
  Typography,
  Box,
  TextField,
  Button,
  Grid,
  Paper,
  FormControl,
  InputLabel,
  Select,
  MenuItem,
  Switch,
  FormControlLabel,
  Divider,
} from '@mui/material';

function Configuration() {
  const [config, setConfig] = useState({
    database: {
      host: 'localhost',
      port: '5432',
      name: 'ohdsi',
      user: 'postgres',
      password: 'acumenus',
    },
    schemas: {
      omop: 'omop',
      staging: 'staging',
      population: 'population',
    },
    paths: {
      vocabDir: '/path/to/vocabulary/files',
      syntheaDir: '/path/to/synthea/output',
    },
    options: {
      withHeader: true,
      parallelJobs: 4,
      truncateTargetTables: true,
      batchSize: 10000,
      enableLogging: true,
      logLevel: 'INFO',
    },
  });

  const handleDatabaseChange = (e) => {
    setConfig({
      ...config,
      database: {
        ...config.database,
        [e.target.name]: e.target.value,
      },
    });
  };

  const handleSchemasChange = (e) => {
    setConfig({
      ...config,
      schemas: {
        ...config.schemas,
        [e.target.name]: e.target.value,
      },
    });
  };

  const handlePathsChange = (e) => {
    setConfig({
      ...config,
      paths: {
        ...config.paths,
        [e.target.name]: e.target.value,
      },
    });
  };

  const handleOptionsChange = (e) => {
    const value = e.target.type === 'checkbox' ? e.target.checked : e.target.value;
    setConfig({
      ...config,
      options: {
        ...config.options,
        [e.target.name]: value,
      },
    });
  };

  const handleSave = () => {
    // In a real app, this would save to the backend
    console.log('Saving configuration:', config);
    alert('Configuration saved successfully!');
  };

  const handleTest = () => {
    // In a real app, this would test the database connection
    console.log('Testing connection with:', config.database);
    alert('Database connection successful!');
  };

  return (
    <Container maxWidth="lg" sx={{ mt: 4, mb: 4 }}>
      <Typography variant="h4" gutterBottom component="h2">
        ETL Configuration
      </Typography>
      <Paper sx={{ p: 2, display: 'flex', flexDirection: 'column' }}>
        <Box component="form" noValidate autoComplete="off">
          {/* Database Configuration */}
          <Typography variant="h6" gutterBottom>
            Database Connection
          </Typography>
          <Grid container spacing={3}>
            <Grid item xs={12} sm={6} md={4}>
              <TextField
                fullWidth
                label="Host"
                name="host"
                value={config.database.host}
                onChange={handleDatabaseChange}
              />
            </Grid>
            <Grid item xs={12} sm={6} md={2}>
              <TextField
                fullWidth
                label="Port"
                name="port"
                value={config.database.port}
                onChange={handleDatabaseChange}
              />
            </Grid>
            <Grid item xs={12} sm={6} md={6}>
              <TextField
                fullWidth
                label="Database Name"
                name="name"
                value={config.database.name}
                onChange={handleDatabaseChange}
              />
            </Grid>
            <Grid item xs={12} sm={6} md={6}>
              <TextField
                fullWidth
                label="Username"
                name="user"
                value={config.database.user}
                onChange={handleDatabaseChange}
              />
            </Grid>
            <Grid item xs={12} sm={6} md={6}>
              <TextField
                fullWidth
                label="Password"
                name="password"
                type="password"
                value={config.database.password}
                onChange={handleDatabaseChange}
              />
            </Grid>
            <Grid item xs={12}>
              <Button variant="outlined" onClick={handleTest}>
                Test Connection
              </Button>
            </Grid>
          </Grid>

          <Divider sx={{ my: 3 }} />

          {/* Schema Configuration */}
          <Typography variant="h6" gutterBottom>
            Schema Names
          </Typography>
          <Grid container spacing={3}>
            <Grid item xs={12} sm={4}>
              <TextField
                fullWidth
                label="OMOP Schema"
                name="omop"
                value={config.schemas.omop}
                onChange={handleSchemasChange}
              />
            </Grid>
            <Grid item xs={12} sm={4}>
              <TextField
                fullWidth
                label="Staging Schema"
                name="staging"
                value={config.schemas.staging}
                onChange={handleSchemasChange}
              />
            </Grid>
            <Grid item xs={12} sm={4}>
              <TextField
                fullWidth
                label="Population Schema"
                name="population"
                value={config.schemas.population}
                onChange={handleSchemasChange}
              />
            </Grid>
          </Grid>

          <Divider sx={{ my: 3 }} />

          {/* File Paths */}
          <Typography variant="h6" gutterBottom>
            File Paths
          </Typography>
          <Grid container spacing={3}>
            <Grid item xs={12} sm={6}>
              <TextField
                fullWidth
                label="Vocabulary Directory"
                name="vocabDir"
                value={config.paths.vocabDir}
                onChange={handlePathsChange}
              />
            </Grid>
            <Grid item xs={12} sm={6}>
              <TextField
                fullWidth
                label="Synthea Data Directory"
                name="syntheaDir"
                value={config.paths.syntheaDir}
                onChange={handlePathsChange}
              />
            </Grid>
          </Grid>

          <Divider sx={{ my: 3 }} />

          {/* Processing Options */}
          <Typography variant="h6" gutterBottom>
            Processing Options
          </Typography>
          <Grid container spacing={3}>
            <Grid item xs={12} sm={6} md={3}>
              <FormControlLabel
                control={
                  <Switch
                    checked={config.options.withHeader}
                    onChange={handleOptionsChange}
                    name="withHeader"
                  />
                }
                label="Files Have Headers"
              />
            </Grid>
            <Grid item xs={12} sm={6} md={3}>
              <TextField
                fullWidth
                label="Parallel Jobs"
                name="parallelJobs"
                type="number"
                value={config.options.parallelJobs}
                onChange={handleOptionsChange}
              />
            </Grid>
            <Grid item xs={12} sm={6} md={3}>
              <TextField
                fullWidth
                label="Batch Size"
                name="batchSize"
                type="number"
                value={config.options.batchSize}
                onChange={handleOptionsChange}
              />
            </Grid>
            <Grid item xs={12} sm={6} md={3}>
              <FormControl fullWidth>
                <InputLabel>Log Level</InputLabel>
                <Select
                  name="logLevel"
                  value={config.options.logLevel}
                  label="Log Level"
                  onChange={handleOptionsChange}
                >
                  <MenuItem value="DEBUG">DEBUG</MenuItem>
                  <MenuItem value="INFO">INFO</MenuItem>
                  <MenuItem value="WARNING">WARNING</MenuItem>
                  <MenuItem value="ERROR">ERROR</MenuItem>
                </Select>
              </FormControl>
            </Grid>
            <Grid item xs={12} sm={6}>
              <FormControlLabel
                control={
                  <Switch
                    checked={config.options.truncateTargetTables}
                    onChange={handleOptionsChange}
                    name="truncateTargetTables"
                  />
                }
                label="Truncate Target Tables"
              />
            </Grid>
            <Grid item xs={12} sm={6}>
              <FormControlLabel
                control={
                  <Switch
                    checked={config.options.enableLogging}
                    onChange={handleOptionsChange}
                    name="enableLogging"
                  />
                }
                label="Enable Logging"
              />
            </Grid>
          </Grid>

          <Box sx={{ mt: 3, display: 'flex', justifyContent: 'flex-end' }}>
            <Button variant="contained" color="primary" onClick={handleSave}>
              Save Configuration
            </Button>
          </Box>
        </Box>
      </Paper>
    </Container>
  );
}

export default Configuration;
