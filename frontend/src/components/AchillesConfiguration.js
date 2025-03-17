import React, { useState, useEffect } from 'react';
import {
  Box,
  Button,
  Card,
  CardContent,
  CardHeader,
  Checkbox,
  Divider,
  FormControl,
  FormControlLabel,
  Grid,
  InputLabel,
  MenuItem,
  Select,
  TextField,
  Typography,
  Paper,
  Alert,
  AlertTitle,
} from '@mui/material';
import PlayArrowIcon from '@mui/icons-material/PlayArrow';
import SettingsIcon from '@mui/icons-material/Settings';

const AchillesConfiguration = ({ onRunAchilles }) => {
  const [config, setConfig] = useState({
    dbms: 'postgresql',
    server: 'postgres/synthea',
    port: '5432',
    user: 'postgres',
    password: 'acumenus',
    pathToDriver: '/drivers',
    cdmDatabaseSchema: 'omop',
    resultsDatabaseSchema: 'achilles_results',
    vocabDatabaseSchema: 'omop',
    sourceName: 'Synthea',
    createTable: true,
    smallCellCount: 5,
    cdmVersion: '5.4',
    createIndices: true,
    numThreads: 4,
    tempAchillesPrefix: 'tmpach',
    dropScratchTables: true,
    sqlOnly: false,
    outputFolder: '/app/output',
    verboseMode: true,
    optimizeAtlasCache: true,
    defaultAnalysesOnly: true,
    updateGivenAnalysesOnly: false,
    excludeAnalysisIds: false,
    sqlDialect: 'postgresql'
  });

  const [loading, setLoading] = useState(false);
  const [error, setError] = useState(null);

  useEffect(() => {
    // Fetch default configuration from API
    fetch('http://localhost:5081/api/achilles/config')
      .then(response => response.json())
      .then(data => {
        setConfig(data);
      })
      .catch(err => {
        console.error('Error fetching Achilles configuration:', err);
        setError('Failed to load default configuration');
      });
  }, []);

  const handleChange = (e) => {
    const { name, value, type, checked } = e.target;
    setConfig({
      ...config,
      [name]: type === 'checkbox' ? checked : value
    });
  };

  const handleNumberChange = (e) => {
    const { name, value } = e.target;
    setConfig({
      ...config,
      [name]: parseInt(value, 10)
    });
  };

  const handleSubmit = (e) => {
    e.preventDefault();
    setLoading(true);
    setError(null);
    
    // Call the parent component's function to run Achilles
    onRunAchilles(config)
      .catch(err => {
        console.error('Error starting Achilles:', err);
        setError('Failed to start Achilles analysis');
      })
      .finally(() => {
        setLoading(false);
      });
  };

  return (
    <Box component="form" onSubmit={handleSubmit} sx={{ mt: 3 }}>
      <Card>
        <CardHeader 
          title="Achilles Configuration" 
          subheader="Configure and run Achilles analysis"
          avatar={<SettingsIcon />}
        />
        <Divider />
        <CardContent>
          {error && (
            <Alert severity="error" sx={{ mb: 3 }}>
              <AlertTitle>Error</AlertTitle>
              {error}
            </Alert>
          )}
          
          <Typography variant="h6" gutterBottom>
            Database Connection
          </Typography>
          <Grid container spacing={3}>
            <Grid item xs={12} md={6}>
              <TextField
                fullWidth
                label="DBMS"
                name="dbms"
                value={config.dbms}
                onChange={handleChange}
                margin="normal"
                variant="outlined"
              />
            </Grid>
            <Grid item xs={12} md={6}>
              <TextField
                fullWidth
                label="Server"
                name="server"
                value={config.server}
                onChange={handleChange}
                margin="normal"
                variant="outlined"
              />
            </Grid>
            <Grid item xs={12} md={4}>
              <TextField
                fullWidth
                label="Port"
                name="port"
                value={config.port}
                onChange={handleChange}
                margin="normal"
                variant="outlined"
              />
            </Grid>
            <Grid item xs={12} md={4}>
              <TextField
                fullWidth
                label="User"
                name="user"
                value={config.user}
                onChange={handleChange}
                margin="normal"
                variant="outlined"
              />
            </Grid>
            <Grid item xs={12} md={4}>
              <TextField
                fullWidth
                label="Password"
                name="password"
                type="password"
                value={config.password}
                onChange={handleChange}
                margin="normal"
                variant="outlined"
              />
            </Grid>
          </Grid>

          <Divider sx={{ my: 3 }} />
          
          <Typography variant="h6" gutterBottom>
            Schema Configuration
          </Typography>
          <Grid container spacing={3}>
            <Grid item xs={12} md={4}>
              <TextField
                fullWidth
                label="CDM Schema"
                name="cdmDatabaseSchema"
                value={config.cdmDatabaseSchema}
                onChange={handleChange}
                margin="normal"
                variant="outlined"
              />
            </Grid>
            <Grid item xs={12} md={4}>
              <TextField
                fullWidth
                label="Results Schema"
                name="resultsDatabaseSchema"
                value={config.resultsDatabaseSchema}
                onChange={handleChange}
                margin="normal"
                variant="outlined"
              />
            </Grid>
            <Grid item xs={12} md={4}>
              <TextField
                fullWidth
                label="Vocabulary Schema"
                name="vocabDatabaseSchema"
                value={config.vocabDatabaseSchema}
                onChange={handleChange}
                margin="normal"
                variant="outlined"
              />
            </Grid>
          </Grid>

          <Divider sx={{ my: 3 }} />
          
          <Typography variant="h6" gutterBottom>
            Analysis Options
          </Typography>
          <Grid container spacing={3}>
            <Grid item xs={12} md={6}>
              <TextField
                fullWidth
                label="Source Name"
                name="sourceName"
                value={config.sourceName}
                onChange={handleChange}
                margin="normal"
                variant="outlined"
              />
            </Grid>
            <Grid item xs={12} md={6}>
              <FormControl fullWidth margin="normal" variant="outlined">
                <InputLabel>CDM Version</InputLabel>
                <Select
                  name="cdmVersion"
                  value={config.cdmVersion}
                  onChange={handleChange}
                  label="CDM Version"
                >
                  <MenuItem value="5.3">5.3</MenuItem>
                  <MenuItem value="5.4">5.4</MenuItem>
                </Select>
              </FormControl>
            </Grid>
            <Grid item xs={12} md={4}>
              <TextField
                fullWidth
                label="Small Cell Count"
                name="smallCellCount"
                type="number"
                value={config.smallCellCount}
                onChange={handleNumberChange}
                margin="normal"
                variant="outlined"
              />
            </Grid>
            <Grid item xs={12} md={4}>
              <TextField
                fullWidth
                label="Number of Threads"
                name="numThreads"
                type="number"
                value={config.numThreads}
                onChange={handleNumberChange}
                margin="normal"
                variant="outlined"
              />
            </Grid>
            <Grid item xs={12} md={4}>
              <TextField
                fullWidth
                label="Temp Achilles Prefix"
                name="tempAchillesPrefix"
                value={config.tempAchillesPrefix}
                onChange={handleChange}
                margin="normal"
                variant="outlined"
              />
            </Grid>
          </Grid>

          <Grid container spacing={3} sx={{ mt: 1 }}>
            <Grid item xs={12} md={6}>
              <Paper variant="outlined" sx={{ p: 2 }}>
                <Typography variant="subtitle1" gutterBottom>
                  Table Options
                </Typography>
                <FormControlLabel
                  control={
                    <Checkbox
                      checked={config.createTable}
                      onChange={handleChange}
                      name="createTable"
                    />
                  }
                  label="Create Tables"
                />
                <FormControlLabel
                  control={
                    <Checkbox
                      checked={config.createIndices}
                      onChange={handleChange}
                      name="createIndices"
                    />
                  }
                  label="Create Indices"
                />
                <FormControlLabel
                  control={
                    <Checkbox
                      checked={config.dropScratchTables}
                      onChange={handleChange}
                      name="dropScratchTables"
                    />
                  }
                  label="Drop Scratch Tables"
                />
              </Paper>
            </Grid>
            <Grid item xs={12} md={6}>
              <Paper variant="outlined" sx={{ p: 2 }}>
                <Typography variant="subtitle1" gutterBottom>
                  Analysis Options
                </Typography>
                <FormControlLabel
                  control={
                    <Checkbox
                      checked={config.verboseMode}
                      onChange={handleChange}
                      name="verboseMode"
                    />
                  }
                  label="Verbose Mode"
                />
                <FormControlLabel
                  control={
                    <Checkbox
                      checked={config.optimizeAtlasCache}
                      onChange={handleChange}
                      name="optimizeAtlasCache"
                    />
                  }
                  label="Optimize Atlas Cache"
                />
                <FormControlLabel
                  control={
                    <Checkbox
                      checked={config.defaultAnalysesOnly}
                      onChange={handleChange}
                      name="defaultAnalysesOnly"
                    />
                  }
                  label="Default Analyses Only"
                />
              </Paper>
            </Grid>
          </Grid>
        </CardContent>
        <Divider />
        <Box sx={{ display: 'flex', justifyContent: 'flex-end', p: 2 }}>
          <Button
            type="submit"
            variant="contained"
            color="primary"
            startIcon={<PlayArrowIcon />}
            disabled={loading}
          >
            {loading ? 'Running...' : 'Run Achilles'}
          </Button>
        </Box>
      </Card>
    </Box>
  );
};

export default AchillesConfiguration;
