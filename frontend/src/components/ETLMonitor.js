import React, { useState, useEffect } from 'react';
import {
  Container,
  Typography,
  Box,
  Paper,
  Grid,
  LinearProgress,
  Table,
  TableBody,
  TableCell,
  TableContainer,
  TableHead,
  TableRow,
  Button,
  Chip,
} from '@mui/material';
import PlayArrowIcon from '@mui/icons-material/PlayArrow';
import PauseIcon from '@mui/icons-material/Pause';
import StopIcon from '@mui/icons-material/Stop';

function ETLMonitor() {
  // In a real app, this would be fetched from the backend
  const [etlStatus, setEtlStatus] = useState({
    isRunning: true,
    overallProgress: 20.13,
    elapsedTime: '0h 22m 36s',
    estimatedTimeRemaining: '1h 29m',
    currentQuery: 'CREATE INDEX IF NOT EXISTS idx_observations_typed_code_category ON population.observations_typed (code, category)',
    queryStartTime: '2025-03-17 03:20:35',
    queryDuration: '0:02:33',
    systemResources: {
      cpuUsage: 3.1,
      memoryUsage: 35.5,
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
  });

  // Simulate real-time updates
  useEffect(() => {
    const timer = setInterval(() => {
      // In a real app, this would fetch the latest status from the backend
      // For now, we'll just increment the elapsed time
      setEtlStatus((prevStatus) => ({
        ...prevStatus,
        elapsedTime: incrementTime(prevStatus.elapsedTime),
      }));
    }, 1000);

    return () => clearInterval(timer);
  }, []);

  const incrementTime = (timeStr) => {
    // Simple function to increment the time string by 1 second
    // In a real app, this would be handled by the backend
    const [hours, minutes, seconds] = timeStr.replace(/[h|m|s]/g, '').split(' ').map(Number);
    let newSeconds = seconds + 1;
    let newMinutes = minutes;
    let newHours = hours;

    if (newSeconds >= 60) {
      newSeconds = 0;
      newMinutes += 1;
    }

    if (newMinutes >= 60) {
      newMinutes = 0;
      newHours += 1;
    }

    return `${newHours}h ${newMinutes}m ${newSeconds}s`;
  };

  const handlePause = () => {
    // In a real app, this would send a request to pause the ETL process
    setEtlStatus({ ...etlStatus, isRunning: false });
  };

  const handleResume = () => {
    // In a real app, this would send a request to resume the ETL process
    setEtlStatus({ ...etlStatus, isRunning: true });
  };

  const handleStop = () => {
    // In a real app, this would send a request to stop the ETL process
    if (window.confirm('Are you sure you want to stop the ETL process? This cannot be undone.')) {
      setEtlStatus({ ...etlStatus, isRunning: false });
    }
  };

  return (
    <Container maxWidth="lg" sx={{ mt: 4, mb: 4 }}>
      <Typography variant="h4" gutterBottom component="h2">
        ETL Monitor
      </Typography>

      {/* Control Panel */}
      <Paper sx={{ p: 2, mb: 3, display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
        <Box>
          <Typography variant="h6" component="span" sx={{ mr: 2 }}>
            Status: 
          </Typography>
          <Chip 
            label={etlStatus.isRunning ? "Running" : "Paused"} 
            color={etlStatus.isRunning ? "success" : "warning"} 
            sx={{ fontWeight: 'bold' }}
          />
        </Box>
        <Box>
          {etlStatus.isRunning ? (
            <Button 
              variant="contained" 
              color="warning" 
              startIcon={<PauseIcon />}
              onClick={handlePause}
              sx={{ mr: 1 }}
            >
              Pause
            </Button>
          ) : (
            <Button 
              variant="contained" 
              color="success" 
              startIcon={<PlayArrowIcon />}
              onClick={handleResume}
              sx={{ mr: 1 }}
            >
              Resume
            </Button>
          )}
          <Button 
            variant="contained" 
            color="error" 
            startIcon={<StopIcon />}
            onClick={handleStop}
          >
            Stop
          </Button>
        </Box>
      </Paper>

      {/* Overall Progress */}
      <Paper sx={{ p: 2, mb: 3 }}>
        <Typography variant="h6" gutterBottom>
          Overall Progress: {etlStatus.overallProgress.toFixed(2)}%
        </Typography>
        <LinearProgress 
          variant="determinate" 
          value={etlStatus.overallProgress} 
          sx={{ height: 10, mb: 2 }}
        />
        <Grid container spacing={2}>
          <Grid item xs={12} sm={4}>
            <Typography variant="body1">
              Elapsed Time: {etlStatus.elapsedTime}
            </Typography>
          </Grid>
          <Grid item xs={12} sm={8}>
            <Typography variant="body1">
              Estimated Time Remaining: {etlStatus.estimatedTimeRemaining}
            </Typography>
          </Grid>
        </Grid>
      </Paper>

      {/* Current Query */}
      <Paper sx={{ p: 2, mb: 3 }}>
        <Typography variant="h6" gutterBottom>
          Current Query
        </Typography>
        <Box sx={{ bgcolor: '#f5f5f5', p: 2, borderRadius: 1, fontFamily: 'monospace', overflow: 'auto' }}>
          {etlStatus.currentQuery}
        </Box>
        <Grid container spacing={2} sx={{ mt: 1 }}>
          <Grid item xs={12} sm={6}>
            <Typography variant="body2">
              Started at: {etlStatus.queryStartTime}
            </Typography>
          </Grid>
          <Grid item xs={12} sm={6}>
            <Typography variant="body2">
              Duration: {etlStatus.queryDuration}
            </Typography>
          </Grid>
        </Grid>
      </Paper>

      {/* System Resources */}
      <Paper sx={{ p: 2, mb: 3 }}>
        <Typography variant="h6" gutterBottom>
          System Resources
        </Typography>
        <Grid container spacing={2}>
          <Grid item xs={12} sm={4}>
            <Typography variant="body1" gutterBottom>
              CPU Usage
            </Typography>
            <LinearProgress 
              variant="determinate" 
              value={etlStatus.systemResources.cpuUsage} 
              sx={{ height: 10, mb: 1 }}
            />
            <Typography variant="body2">
              {etlStatus.systemResources.cpuUsage.toFixed(1)}%
            </Typography>
          </Grid>
          <Grid item xs={12} sm={4}>
            <Typography variant="body1" gutterBottom>
              Memory Usage
            </Typography>
            <LinearProgress 
              variant="determinate" 
              value={etlStatus.systemResources.memoryUsage} 
              sx={{ height: 10, mb: 1 }}
            />
            <Typography variant="body2">
              {etlStatus.systemResources.memoryUsage.toFixed(1)}%
            </Typography>
          </Grid>
          <Grid item xs={12} sm={4}>
            <Typography variant="body1" gutterBottom>
              Disk Usage
            </Typography>
            <LinearProgress 
              variant="determinate" 
              value={etlStatus.systemResources.diskUsage} 
              sx={{ height: 10, mb: 1 }}
            />
            <Typography variant="body2">
              {etlStatus.systemResources.diskUsage.toFixed(1)}%
            </Typography>
          </Grid>
        </Grid>
      </Paper>

      {/* Table Progress */}
      <Paper sx={{ p: 2, mb: 3 }}>
        <Typography variant="h6" gutterBottom>
          Table Progress
        </Typography>
        <TableContainer>
          <Table size="small">
            <TableHead>
              <TableRow>
                <TableCell>Source Table</TableCell>
                <TableCell align="right">Source Count</TableCell>
                <TableCell>Target Table</TableCell>
                <TableCell align="right">Target Count</TableCell>
                <TableCell align="right">Progress</TableCell>
              </TableRow>
            </TableHead>
            <TableBody>
              {etlStatus.tableProgress.map((row, index) => (
                <TableRow key={index}>
                  <TableCell>{row.sourceTable}</TableCell>
                  <TableCell align="right">{row.sourceCount.toLocaleString()}</TableCell>
                  <TableCell>{row.targetTable}</TableCell>
                  <TableCell align="right">{row.targetCount.toLocaleString()}</TableCell>
                  <TableCell align="right">
                    {row.progress === '-' ? '-' : `${row.progress.toFixed(2)}%`}
                  </TableCell>
                </TableRow>
              ))}
            </TableBody>
          </Table>
        </TableContainer>
      </Paper>

      {/* ETL Steps */}
      <Paper sx={{ p: 2 }}>
        <Typography variant="h6" gutterBottom>
          ETL Steps Progress
        </Typography>
        <TableContainer>
          <Table size="small">
            <TableHead>
              <TableRow>
                <TableCell>Step</TableCell>
                <TableCell>Started</TableCell>
                <TableCell>Completed</TableCell>
                <TableCell>Duration</TableCell>
                <TableCell>Status</TableCell>
                <TableCell align="right">Rows</TableCell>
                <TableCell>Error</TableCell>
              </TableRow>
            </TableHead>
            <TableBody>
              {etlStatus.etlSteps.map((step, index) => (
                <TableRow key={index}>
                  <TableCell>{step.step}</TableCell>
                  <TableCell>{step.started}</TableCell>
                  <TableCell>{step.completed}</TableCell>
                  <TableCell>{step.duration}</TableCell>
                  <TableCell>
                    <Chip 
                      label={step.status} 
                      color={
                        step.status === 'Completed' ? 'success' : 
                        step.status === 'In Progress' ? 'info' : 
                        step.status === 'Error' ? 'error' : 
                        'default'
                      }
                      size="small"
                    />
                  </TableCell>
                  <TableCell align="right">{step.rows || '-'}</TableCell>
                  <TableCell>{step.error || '-'}</TableCell>
                </TableRow>
              ))}
            </TableBody>
          </Table>
        </TableContainer>
      </Paper>
    </Container>
  );
}

export default ETLMonitor;
