import React, { useState, useEffect } from 'react';
import {
  Box,
  Card,
  CardContent,
  CardHeader,
  Divider,
  LinearProgress,
  Typography,
  List,
  ListItem,
  ListItemText,
  Paper,
  Grid,
  Alert,
  AlertTitle,
  Chip,
} from '@mui/material';
import AssessmentIcon from '@mui/icons-material/Assessment';
import TimerIcon from '@mui/icons-material/Timer';
import CheckCircleIcon from '@mui/icons-material/CheckCircle';
import ErrorIcon from '@mui/icons-material/Error';
import HourglassEmptyIcon from '@mui/icons-material/HourglassEmpty';

const AchillesMonitor = ({ processId, onComplete }) => {
  const [status, setStatus] = useState({
    status: 'running',
    progress: [],
    current_progress: 0,
    current_stage: '',
    start_time: '',
    elapsed_time: '',
  });
  const [error, setError] = useState(null);
  const [loading, setLoading] = useState(true);
  const [pollingInterval, setPollingInterval] = useState(null);

  useEffect(() => {
    // Start polling for status updates
    const interval = setInterval(() => {
      fetchStatus();
    }, 5000); // Poll every 5 seconds
    
    setPollingInterval(interval);
    
    // Initial fetch
    fetchStatus();
    
    // Cleanup on unmount
    return () => {
      if (pollingInterval) {
        clearInterval(pollingInterval);
      }
    };
  }, [processId]);

  const fetchStatus = () => {
    setLoading(true);
    fetch(`http://localhost:5081/api/achilles/status/${processId}`)
      .then(response => {
        if (!response.ok) {
          throw new Error(`HTTP error ${response.status}`);
        }
        return response.json();
      })
      .then(data => {
        setStatus(data);
        setLoading(false);
        
        // If process is completed or failed, stop polling and notify parent
        if (data.status === 'completed' || data.status === 'failed') {
          if (pollingInterval) {
            clearInterval(pollingInterval);
            setPollingInterval(null);
          }
          
          if (onComplete) {
            onComplete(data);
          }
        }
      })
      .catch(err => {
        console.error('Error fetching Achilles status:', err);
        setError(`Failed to fetch status: ${err.message}`);
        setLoading(false);
      });
  };

  const renderProgressChip = () => {
    if (status.status === 'completed') {
      return <Chip icon={<CheckCircleIcon />} label="Completed" color="success" />;
    } else if (status.status === 'failed') {
      return <Chip icon={<ErrorIcon />} label="Failed" color="error" />;
    } else {
      return <Chip icon={<HourglassEmptyIcon />} label="Running" color="primary" />;
    }
  };

  return (
    <Box sx={{ mt: 3 }}>
      <Card>
        <CardHeader 
          title="Achilles Analysis Progress" 
          subheader="Monitor the progress of the Achilles analysis"
          avatar={<AssessmentIcon />}
          action={renderProgressChip()}
        />
        <Divider />
        <CardContent>
          {error && (
            <Alert severity="error" sx={{ mb: 3 }}>
              <AlertTitle>Error</AlertTitle>
              {error}
            </Alert>
          )}
          
          <Grid container spacing={3}>
            <Grid item xs={12} md={6}>
              <Paper variant="outlined" sx={{ p: 2 }}>
                <Typography variant="subtitle1" gutterBottom>
                  <TimerIcon sx={{ mr: 1, verticalAlign: 'middle' }} />
                  Timing Information
                </Typography>
                <List dense>
                  <ListItem>
                    <ListItemText 
                      primary="Start Time" 
                      secondary={status.start_time ? new Date(status.start_time).toLocaleString() : 'N/A'} 
                    />
                  </ListItem>
                  <ListItem>
                    <ListItemText 
                      primary="Elapsed Time" 
                      secondary={status.elapsed_time || 'N/A'} 
                    />
                  </ListItem>
                  <ListItem>
                    <ListItemText 
                      primary="Current Stage" 
                      secondary={status.current_stage || 'Initializing...'} 
                    />
                  </ListItem>
                </List>
              </Paper>
            </Grid>
            
            <Grid item xs={12} md={6}>
              <Paper variant="outlined" sx={{ p: 2, height: '100%' }}>
                <Typography variant="subtitle1" gutterBottom>
                  Overall Progress
                </Typography>
                <Box sx={{ display: 'flex', alignItems: 'center', mt: 2 }}>
                  <Box sx={{ width: '100%', mr: 1 }}>
                    <LinearProgress 
                      variant="determinate" 
                      value={status.current_progress || 0} 
                      sx={{ height: 10, borderRadius: 5 }}
                    />
                  </Box>
                  <Box sx={{ minWidth: 35 }}>
                    <Typography variant="body2" color="text.secondary">
                      {`${Math.round(status.current_progress || 0)}%`}
                    </Typography>
                  </Box>
                </Box>
              </Paper>
            </Grid>
          </Grid>
          
          <Paper variant="outlined" sx={{ p: 2, mt: 3 }}>
            <Typography variant="subtitle1" gutterBottom>
              Progress Log
            </Typography>
            <List dense sx={{ maxHeight: 300, overflow: 'auto' }}>
              {status.progress && status.progress.length > 0 ? (
                status.progress.map((item, index) => (
                  <ListItem key={index} divider={index < status.progress.length - 1}>
                    <ListItemText
                      primary={item.stage}
                      secondary={
                        <>
                          <Typography component="span" variant="body2" color="text.primary">
                            {`Progress: ${Math.round(item.progress * 100)}%`}
                          </Typography>
                          {` — ${item.detail}`}
                          <Typography component="span" variant="body2" color="text.secondary" sx={{ display: 'block' }}>
                            {new Date(item.timestamp).toLocaleString()}
                          </Typography>
                        </>
                      }
                    />
                  </ListItem>
                ))
              ) : (
                <ListItem>
                  <ListItemText primary="No progress updates yet" />
                </ListItem>
              )}
            </List>
          </Paper>
          
          {status.status === 'failed' && status.stderr && (
            <Alert severity="error" sx={{ mt: 3 }}>
              <AlertTitle>Execution Error</AlertTitle>
              <Typography variant="body2" component="pre" sx={{ whiteSpace: 'pre-wrap', mt: 1 }}>
                {status.stderr}
              </Typography>
            </Alert>
          )}
          
          {status.status === 'completed' && (
            <Alert severity="success" sx={{ mt: 3 }}>
              <AlertTitle>Analysis Completed Successfully</AlertTitle>
              <Typography variant="body2">
                The Achilles analysis has completed successfully. You can now view the results.
              </Typography>
            </Alert>
          )}
        </CardContent>
      </Card>
    </Box>
  );
};

export default AchillesMonitor;
