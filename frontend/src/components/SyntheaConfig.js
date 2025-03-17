import React, { useState } from 'react';
import {
  Box,
  Paper,
  Typography,
  Grid,
  TextField,
  Slider,
  FormControl,
  InputLabel,
  Select,
  MenuItem,
  Chip,
  Button,
  Divider,
  Accordion,
  AccordionSummary,
  AccordionDetails,
  Switch,
  FormControlLabel,
  Alert,
  LinearProgress
} from '@mui/material';
import { styled } from '@mui/material/styles';
import {
  ChevronDownIcon,
  PersonIcon,
  GlobeIcon,
  ClockIcon,
  HeartIcon,
  InfoCircledIcon,
  DownloadIcon,
  ReloadIcon,
  CheckCircledIcon
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

function SyntheaConfig() {
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
      <Typography variant="h5" gutterBottom sx={{ fontWeight: 600 }}>
        Synthea Configuration
      </Typography>
      
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

export default SyntheaConfig;
