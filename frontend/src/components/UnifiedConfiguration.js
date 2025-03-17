import React, { useState, useEffect } from 'react';
import {
  Box,
  Container,
  Typography,
  Paper,
  TextField,
  Button,
  Grid,
  FormControl,
  InputLabel,
  Select,
  MenuItem,
  Switch,
  FormControlLabel,
  Divider,
  Tabs,
  Tab,
  Card,
  CardContent,
  CardHeader,
  Alert,
  AlertTitle,
  Accordion,
  AccordionSummary,
  AccordionDetails,
  Slider,
  Chip,
  LinearProgress
} from '@mui/material';
import { styled } from '@mui/material/styles';
import PlayArrowIcon from '@mui/icons-material/PlayArrow';
import SettingsIcon from '@mui/icons-material/Settings';
import {
  ChevronDownIcon,
  PersonIcon,
  GlobeIcon,
  ClockIcon,
  HeartIcon,
  InfoCircledIcon,
  DownloadIcon,
  ReloadIcon,
  CheckCircledIcon,
  GearIcon,
  BarChartIcon
} from '@radix-ui/react-icons';

// Glassmorphism styled components
const GlassCard = styled(Paper)(({ theme }) => ({
  backdropFilter: 'blur(10px)',
  WebkitBackdropFilter: 'blur(10px)',
  background: 'rgba(30, 30, 40, 0.25)',
  borderRadius: '10px',
  border: '1px solid rgba(255, 255, 255, 0.05)',
  boxShadow: '0 8px 32px 0 rgba(0, 0, 0, 0.37)',
  padding: theme.spacing(3),
  height: '100%',
  display: 'flex',
  flexDirection: 'column',
  '&:hover': {
    background: 'rgba(30, 30, 40, 0.35)',
  },
}));

const GlassAccordion = styled(Accordion)(({ theme }) => ({
  backdropFilter: 'blur(10px)',
  WebkitBackdropFilter: 'blur(10px)',
  background: 'rgba(30, 30, 40, 0.25)',
  borderRadius: '10px !important',
  border: '1px solid rgba(255, 255, 255, 0.05)',
  boxShadow: '0 8px 32px 0 rgba(0, 0, 0, 0.37)',
  marginBottom: theme.spacing(2),
  '&:before': {
    display: 'none',
  },
  '&.Mui-expanded': {
    margin: `0 0 ${theme.spacing(2)} 0`,
  }
}));

const GlassAccordionSummary = styled(AccordionSummary)(({ theme }) => ({
  borderRadius: '10px',
  '&.Mui-expanded': {
    borderBottomLeftRadius: 0,
    borderBottomRightRadius: 0,
  }
}));

const ControlButton = styled(Button)(({ theme }) => ({
  backdropFilter: 'blur(10px)',
  WebkitBackdropFilter: 'blur(10px)',
  background: 'rgba(30, 30, 40, 0.25)',
  border: '1px solid rgba(255, 255, 255, 0.05)',
  borderRadius: '8px',
  padding: theme.spacing(1, 2),
  color: '#90caf9',
  textTransform: 'none',
  fontWeight: 500,
  '&:hover': {
    background: 'rgba(144, 202, 249, 0.15)',
  },
  '& svg': {
    color: '#90caf9'
  }
}));

function UnifiedConfiguration() {
  const [activeTab, setActiveTab] = useState(0);

  const handleTabChange = (event, newValue) => {
    setActiveTab(newValue);
  };

  return (
    <Container maxWidth="lg" sx={{ mt: 4, mb: 4 }}>
      <Typography variant="h4" gutterBottom component="h2">
        Configuration
      </Typography>

      <Box sx={{ borderBottom: 1, borderColor: 'divider', mb: 3 }}>
        <Tabs value={activeTab} onChange={handleTabChange} aria-label="configuration tabs">
          <Tab label="ETL Configuration" icon={<GearIcon />} iconPosition="start" />
          <Tab label="Achilles Configuration" icon={<BarChartIcon />} iconPosition="start" />
          <Tab label="Synthea Configuration" icon={<PersonIcon />} iconPosition="start" />
        </Tabs>
      </Box>

      {/* ETL Configuration Tab */}
      {activeTab === 0 && <ETLConfigTab />}

      {/* Achilles Configuration Tab */}
      {activeTab === 1 && <AchillesConfigTab />}

      {/* Synthea Configuration Tab */}
      {activeTab === 2 && <SyntheaConfigTab />}
    </Container>
  );
}

// ETL Configuration Tab
function ETLConfigTab() {
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
  );
}

// Achilles Configuration Tab
function AchillesConfigTab() {
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
    // In a real app, this would fetch from the backend
    // For now, we'll just use the default state
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
    
    // Simulate running Achilles
    setTimeout(() => {
      setLoading(false);
      alert('Achilles analysis started successfully!');
    }, 1000);
  };

  return (
    <Box component="form" onSubmit={handleSubmit} sx={{ mt: 3 }}>
      <Card>
        <CardHeader 
          title="Achilles Configuration" 
          subheader="Configure and run Achilles analysis"
          avatar={<BarChartIcon width={24} height={24} />}
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
                    <Switch
                      checked={config.createTable}
                      onChange={handleChange}
                      name="createTable"
                    />
                  }
                  label="Create Tables"
                />
                <FormControlLabel
                  control={
                    <Switch
                      checked={config.createIndices}
                      onChange={handleChange}
                      name="createIndices"
                    />
                  }
                  label="Create Indices"
                />
                <FormControlLabel
                  control={
                    <Switch
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
                    <Switch
                      checked={config.verboseMode}
                      onChange={handleChange}
                      name="verboseMode"
                    />
                  }
                  label="Verbose Mode"
                />
                <FormControlLabel
                  control={
                    <Switch
                      checked={config.optimizeAtlasCache}
                      onChange={handleChange}
                      name="optimizeAtlasCache"
                    />
                  }
                  label="Optimize Atlas Cache"
                />
                <FormControlLabel
                  control={
                    <Switch
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
}

// Synthea Configuration Tab
function SyntheaConfigTab() {
  // State for configuration
  const [config, setConfig] = useState({
    population: 1000,
    state: 'MA',
    city: 'Bedford',
    ageDistribution: {
      pediatric: 20,
      adult: 60,
      geriatric: 20
    },
    genderRatio: 50, // % female
    raceEthnicity: {
      white: 70,
      black: 13,
      hispanic: 10,
      asian: 5,
      other: 2
    },
    yearsOfHistory: 5,
    endDate: new Date().toISOString().split('T')[0],
    diseases: [],
    advancedOptions: {
      seed: 1,
      referenceDate: new Date().toISOString().split('T')[0],
      appendData: false,
      onlyAlivePatients: true,
      includeMultipleRecords: true
    }
  });

  // State for generation status
  const [generationStatus, setGenerationStatus] = useState({
    isGenerating: false,
    progress: 0,
    message: '',
    error: null,
    completed: false
  });

  // Handle population change
  const handlePopulationChange = (event) => {
    setConfig({
      ...config,
      population: Number(event.target.value)
    });
  };

  // Handle state change
  const handleStateChange = (event) => {
    setConfig({
      ...config,
      state: event.target.value
    });
  };

  // Handle city change
  const handleCityChange = (event) => {
    setConfig({
      ...config,
      city: event.target.value
    });
  };

  // Handle generate button click
  const handleGenerate = () => {
    setGenerationStatus({
      isGenerating: true,
      progress: 0,
      message: 'Initializing Synthea...',
      error: null,
      completed: false
    });
    
    // Simulate generation progress
    const interval = setInterval(() => {
      setGenerationStatus(prevStatus => {
        if (prevStatus.progress >= 100) {
          clearInterval(interval);
          return {
            isGenerating: false,
            progress: 100,
            message: 'Generation completed successfully!',
            error: null,
            completed: true
          };
        }
        
        const newProgress = prevStatus.progress + Math.random() * 5;
        let message = 'Generating synthetic patients...';
        
        if (newProgress < 10) {
          message = 'Initializing Synthea...';
        } else if (newProgress < 30) {
          message = 'Generating patient demographics...';
        } else if (newProgress < 50) {
          message = 'Generating patient medical histories...';
        } else if (newProgress < 70) {
          message = 'Generating encounters and observations...';
        } else if (newProgress < 90) {
          message = 'Generating claims and medications...';
        } else {
          message = 'Finalizing data generation...';
        }
        
        return {
          ...prevStatus,
          progress: newProgress,
          message
        };
      });
    }, 500);
  };

  return (
    <Box>
      {/* Basic Configuration */}
      <GlassAccordion defaultExpanded>
        <GlassAccordionSummary expandIcon={<ChevronDownIcon />}>
          <Box sx={{ display: 'flex', alignItems: 'center' }}>
            <PersonIcon style={{ marginRight: 8, color: '#90caf9' }} />
            <Typography variant="h6">Basic Configuration</Typography>
          </Box>
        </GlassAccordionSummary>
        <AccordionDetails>
          <Grid container spacing={3}>
            <Grid item xs={12} md={4}>
              <TextField
                fullWidth
                label="Population Size"
                type="number"
                value={config.population}
                onChange={handlePopulationChange}
                InputProps={{
                  inputProps: { min: 1 }
                }}
              />
            </Grid>
            
            <Grid item xs={12} md={4}>
              <FormControl fullWidth>
                <InputLabel>State</InputLabel>
                <Select
                  value={config.state}
                  label="State"
                  onChange={handleStateChange}
                >
                  <MenuItem value="MA">Massachusetts (MA)</MenuItem>
                  <MenuItem value="NY">New York (NY)</MenuItem>
                  <MenuItem value="CA">California (CA)</MenuItem>
                </Select>
              </FormControl>
            </Grid>
            
            <Grid item xs={12} md={4}>
              <TextField
                fullWidth
                label="City"
                value={config.city}
                onChange={handleCityChange}
              />
            </Grid>
          </Grid>
        </AccordionDetails>
      </GlassAccordion>
      
      {/* Generation Status */}
      {generationStatus.isGenerating && (
        <GlassCard sx={{ mt: 3 }}>
          <Typography variant="h6" gutterBottom>
            Generation Status
          </Typography>
          <Typography variant="body1" gutterBottom>
            {generationStatus.message}
          </Typography>
          <LinearProgress 
            variant="determinate" 
            value={generationStatus.progress} 
            sx={{ height: 10, borderRadius: 5, mb: 2 }}
          />
          <Typography variant="body2">
            {Math.round(generationStatus.progress)}% Complete
          </Typography>
        </GlassCard>
      )}
      
      {generationStatus.completed && (
        <Alert 
          severity="success" 
          sx={{ mt: 3, backdropFilter: 'blur(10px)', background: 'rgba(76, 175, 80, 0.1)' }}
        >
          Data generation completed successfully!
        </Alert>
      )}
      
      {/* Generate Button */}
      <Box sx={{ mt: 3, display: 'flex', justifyContent: 'flex-end' }}>
        <ControlButton
          variant="contained"
          onClick={handleGenerate}
          disabled={generationStatus.isGenerating}
          startIcon={<DownloadIcon />}
        >
          Generate Data
        </ControlButton>
      </Box>
    </Box>
  );
}

export default UnifiedConfiguration;
