import React, { useState } from 'react';
import {
  Box,
  Stepper,
  Step,
  StepLabel,
  Button,
  Typography,
  Paper,
  Divider,
  Alert,
  AlertTitle,
} from '@mui/material';
import AchillesConfiguration from './AchillesConfiguration';
import AchillesMonitor from './AchillesMonitor';
import AchillesResults from './AchillesResults';

const steps = ['Configure', 'Run Analysis', 'View Results'];

const AchillesTab = () => {
  const [activeStep, setActiveStep] = useState(0);
  const [processId, setProcessId] = useState(null);
  const [analysisComplete, setAnalysisComplete] = useState(false);
  const [error, setError] = useState(null);

  const handleRunAchilles = async (config) => {
    try {
      const response = await fetch('http://localhost:5081/api/achilles/run', {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
        },
        body: JSON.stringify(config),
      });
      
      if (!response.ok) {
        throw new Error(`HTTP error ${response.status}`);
      }
      
      const data = await response.json();
      setProcessId(data.process_id);
      setActiveStep(1); // Move to monitoring step
      return data;
    } catch (err) {
      console.error('Error starting Achilles:', err);
      setError(`Failed to start Achilles analysis: ${err.message}`);
      throw err;
    }
  };

  const handleAnalysisComplete = (result) => {
    if (result.status === 'completed') {
      setAnalysisComplete(true);
    } else {
      setError(`Analysis failed: ${result.stderr || 'Unknown error'}`);
    }
  };

  const handleNext = () => {
    setActiveStep((prevActiveStep) => prevActiveStep + 1);
  };

  const handleBack = () => {
    setActiveStep((prevActiveStep) => prevActiveStep - 1);
  };

  const handleReset = () => {
    setActiveStep(0);
    setProcessId(null);
    setAnalysisComplete(false);
    setError(null);
  };

  return (
    <Box sx={{ width: '100%' }}>
      <Stepper activeStep={activeStep} sx={{ pt: 3, pb: 5 }}>
        {steps.map((label) => (
          <Step key={label}>
            <StepLabel>{label}</StepLabel>
          </Step>
        ))}
      </Stepper>
      
      {error && (
        <Alert severity="error" sx={{ mb: 3 }}>
          <AlertTitle>Error</AlertTitle>
          {error}
        </Alert>
      )}
      
      <Paper variant="outlined" sx={{ p: 3, mb: 3 }}>
        {activeStep === 0 && (
          <>
            <Typography variant="h6" gutterBottom>
              Configure Achilles Analysis
            </Typography>
            <Typography variant="body2" color="text.secondary" paragraph>
              Configure the parameters for the Achilles analysis. Achilles is a data quality assessment tool for OMOP CDM databases.
              It generates descriptive statistics and data quality metrics that can be used to evaluate the quality of your data.
            </Typography>
            <Divider sx={{ my: 2 }} />
            <AchillesConfiguration onRunAchilles={handleRunAchilles} />
          </>
        )}
        
        {activeStep === 1 && (
          <>
            <Typography variant="h6" gutterBottom>
              Achilles Analysis Progress
            </Typography>
            <Typography variant="body2" color="text.secondary" paragraph>
              Monitor the progress of the Achilles analysis. This process may take some time depending on the size of your database.
            </Typography>
            <Divider sx={{ my: 2 }} />
            {processId ? (
              <AchillesMonitor processId={processId} onComplete={handleAnalysisComplete} />
            ) : (
              <Alert severity="warning">
                <AlertTitle>No Active Process</AlertTitle>
                No Achilles analysis is currently running. Please go back and start an analysis.
              </Alert>
            )}
          </>
        )}
        
        {activeStep === 2 && (
          <>
            <Typography variant="h6" gutterBottom>
              Achilles Analysis Results
            </Typography>
            <Typography variant="body2" color="text.secondary" paragraph>
              View and explore the results of the Achilles analysis. You can browse through the generated tables and export data.
            </Typography>
            <Divider sx={{ my: 2 }} />
            <AchillesResults schema="achilles_results" />
          </>
        )}
      </Paper>
      
      <Box sx={{ display: 'flex', justifyContent: 'space-between' }}>
        <Button
          disabled={activeStep === 0}
          onClick={handleBack}
          sx={{ mr: 1 }}
        >
          Back
        </Button>
        <Box sx={{ flex: '1 1 auto' }} />
        {activeStep === steps.length - 1 ? (
          <Button onClick={handleReset}>
            Start New Analysis
          </Button>
        ) : (
          <Button
            variant="contained"
            onClick={handleNext}
            disabled={(activeStep === 1 && !analysisComplete) || (activeStep === 0 && processId === null)}
          >
            {activeStep === steps.length - 2 ? 'View Results' : 'Next'}
          </Button>
        )}
      </Box>
    </Box>
  );
};

export default AchillesTab;
