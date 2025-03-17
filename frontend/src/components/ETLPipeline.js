import React from 'react';
import { 
  Box, 
  Paper, 
  Typography, 
  Grid, 
  LinearProgress, 
  Chip,
  Button,
  Divider
} from '@mui/material';
import { styled } from '@mui/material/styles';
import { 
  RocketIcon, 
  PersonIcon, 
  TableIcon, 
  ArchiveIcon, 
  MixerHorizontalIcon,
  CheckCircledIcon,
  CrossCircledIcon,
  UpdateIcon,
  PlayIcon,
  PauseIcon,
  StopIcon
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

const PhaseCard = styled(Paper)(({ theme, $active, $completed, $error }) => ({
  backdropFilter: 'blur(10px)',
  WebkitBackdropFilter: 'blur(10px)',
  background: $active 
    ? 'rgba(144, 202, 249, 0.15)' 
    : $completed 
      ? 'rgba(76, 175, 80, 0.15)' 
      : $error 
        ? 'rgba(244, 67, 54, 0.15)' 
        : 'rgba(30, 30, 40, 0.25)',
  borderRadius: '10px',
  border: '1px solid rgba(255, 255, 255, 0.05)',
  boxShadow: '0 8px 32px 0 rgba(0, 0, 0, 0.37)',
  padding: theme.spacing(2),
  height: '100%',
  display: 'flex',
  flexDirection: 'column',
}));

const ProgressContainer = styled(Box)(({ theme }) => ({
  width: '100%',
  height: '10px',
  background: 'rgba(30, 30, 40, 0.5)',
  borderRadius: '5px',
  overflow: 'hidden',
  margin: '10px 0',
}));

const ProgressBar = styled(Box)(({ $width, theme }) => ({
  height: '100%',
  width: `${$width}%`,
  background: 'linear-gradient(90deg, rgba(144, 202, 249, 0.7) 0%, rgba(66, 165, 245, 0.7) 100%)',
  borderRadius: '5px',
  boxShadow: '0 0 10px rgba(144, 202, 249, 0.5)',
}));

const ControlButton = styled(Button)(({ theme }) => ({
  backdropFilter: 'blur(10px)',
  WebkitBackdropFilter: 'blur(10px)',
  background: 'rgba(30, 30, 40, 0.25)',
  border: '1px solid rgba(255, 255, 255, 0.05)',
  borderRadius: '8px',
  padding: theme.spacing(1),
  color: '#90caf9',
  textTransform: 'none',
  fontWeight: 500,
  minWidth: 'auto',
  '&:hover': {
    background: 'rgba(144, 202, 249, 0.15)',
  },
  '& svg': {
    color: '#90caf9'
  }
}));

const ConnectorLine = styled(Box)(({ theme, $direction = 'right', $active }) => ({
  position: 'relative',
  height: $direction === 'right' || $direction === 'left' ? '2px' : '100%',
  width: $direction === 'right' || $direction === 'left' ? '100%' : '2px',
  background: $active ? 'rgba(144, 202, 249, 0.7)' : 'rgba(255, 255, 255, 0.1)',
  margin: $direction === 'right' ? '0 0 0 auto' : $direction === 'left' ? '0 auto 0 0' : '0 auto',
}));

// Mock data for demonstration
const mockETLPhases = [
  {
    id: 'schema-setup',
    name: 'Database Schema Setup',
    icon: <TableIcon width={24} height={24} style={{ color: '#90caf9' }} />,
    status: 'completed',
    progress: 100,
    steps: [
      { name: 'Create OMOP schema', status: 'completed', progress: 100 },
      { name: 'Create population schema', status: 'completed', progress: 100 },
      { name: 'Create staging schema', status: 'completed', progress: 100 },
      { name: 'Set up tables and sequences', status: 'completed', progress: 100 }
    ],
    startTime: '2025-03-17 04:15:00',
    endTime: '2025-03-17 04:20:00',
    duration: '5m 0s'
  },
  {
    id: 'synthea-generation',
    name: 'Synthea Data Generation',
    icon: <PersonIcon width={24} height={24} style={{ color: '#90caf9' }} />,
    status: 'completed',
    progress: 100,
    steps: [
      { name: 'Configure population parameters', status: 'completed', progress: 100 },
      { name: 'Generate synthetic patient data', status: 'completed', progress: 100 },
      { name: 'Output CSV files', status: 'completed', progress: 100 }
    ],
    startTime: '2025-03-17 04:20:00',
    endTime: '2025-03-17 04:25:00',
    duration: '5m 0s'
  },
  {
    id: 'data-loading',
    name: 'Loading Data into Staging',
    icon: <ArchiveIcon width={24} height={24} style={{ color: '#90caf9' }} />,
    status: 'completed',
    progress: 100,
    steps: [
      { name: 'Load Synthea CSV files', status: 'completed', progress: 100 },
      { name: 'Create typed tables', status: 'completed', progress: 100 }
    ],
    startTime: '2025-03-17 04:25:00',
    endTime: '2025-03-17 04:30:00',
    duration: '5m 0s'
  },
  {
    id: 'vocabulary-loading',
    name: 'Vocabulary Loading',
    icon: <MixerHorizontalIcon width={24} height={24} style={{ color: '#90caf9' }} />,
    status: 'in-progress',
    progress: 60,
    steps: [
      { name: 'Download OMOP vocabulary files', status: 'completed', progress: 100 },
      { name: 'Load vocabulary into OMOP schema', status: 'in-progress', progress: 40 }
    ],
    startTime: '2025-03-17 04:30:00',
    endTime: null,
    duration: '10m 12s (ongoing)'
  },
  {
    id: 'etl-transformation',
    name: 'ETL Transformation',
    icon: <RocketIcon width={24} height={24} style={{ color: '#90caf9' }} />,
    status: 'not-started',
    progress: 0,
    steps: [
      { name: 'Create observation periods', status: 'not-started', progress: 0 },
      { name: 'Transform visit data', status: 'not-started', progress: 0 },
      { name: 'Transform condition data', status: 'not-started', progress: 0 },
      { name: 'Transform medication data', status: 'not-started', progress: 0 },
      { name: 'Transform procedure data', status: 'not-started', progress: 0 },
      { name: 'Transform observation data', status: 'not-started', progress: 0 },
      { name: 'Transform death data', status: 'not-started', progress: 0 },
      { name: 'Transform cost data', status: 'not-started', progress: 0 }
    ],
    startTime: null,
    endTime: null,
    duration: 'Not started'
  },
  {
    id: 'validation',
    name: 'Validation & Optimization',
    icon: <CheckCircledIcon width={24} height={24} style={{ color: '#90caf9' }} />,
    status: 'not-started',
    progress: 0,
    steps: [
      { name: 'Verify record counts', status: 'not-started', progress: 0 },
      { name: 'Check referential integrity', status: 'not-started', progress: 0 },
      { name: 'Create additional indexes', status: 'not-started', progress: 0 },
      { name: 'Analyze tables', status: 'not-started', progress: 0 }
    ],
    startTime: null,
    endTime: null,
    duration: 'Not started'
  },
  {
    id: 'achilles',
    name: 'Achilles Analysis',
    icon: <UpdateIcon width={24} height={24} style={{ color: '#90caf9' }} />,
    status: 'not-started',
    progress: 0,
    steps: [
      { name: 'Run data quality checks', status: 'not-started', progress: 0 },
      { name: 'Generate reports', status: 'not-started', progress: 0 }
    ],
    startTime: null,
    endTime: null,
    duration: 'Not started'
  }
];

const getStatusChip = (status) => {
  switch (status) {
    case 'completed':
      return <Chip 
        label="Completed" 
        size="small" 
        icon={<CheckCircledIcon width={16} height={16} />}
        sx={{ 
          backgroundColor: 'rgba(76, 175, 80, 0.15)', 
          color: '#4caf50',
          '& .MuiChip-icon': { color: '#4caf50' }
        }} 
      />;
    case 'in-progress':
      return <Chip 
        label="In Progress" 
        size="small" 
        icon={<UpdateIcon width={16} height={16} />}
        sx={{ 
          backgroundColor: 'rgba(144, 202, 249, 0.15)', 
          color: '#90caf9',
          '& .MuiChip-icon': { color: '#90caf9' }
        }} 
      />;
    case 'error':
      return <Chip 
        label="Error" 
        size="small" 
        icon={<CrossCircledIcon width={16} height={16} />}
        sx={{ 
          backgroundColor: 'rgba(244, 67, 54, 0.15)', 
          color: '#f44336',
          '& .MuiChip-icon': { color: '#f44336' }
        }} 
      />;
    default:
      return <Chip 
        label="Not Started" 
        size="small" 
        sx={{ 
          backgroundColor: 'rgba(255, 255, 255, 0.05)', 
          color: '#aaa' 
        }} 
      />;
  }
};

const PhaseDetails = ({ phase }) => {
  const isActive = phase.status === 'in-progress';
  const isCompleted = phase.status === 'completed';
  const isError = phase.status === 'error';
  
  return (
    <PhaseCard $active={isActive} $completed={isCompleted} $error={isError}>
      <Box sx={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', mb: 1 }}>
        <Box sx={{ display: 'flex', alignItems: 'center' }}>
          {phase.icon}
          <Typography variant="subtitle1" sx={{ ml: 1, fontWeight: 600 }}>
            {phase.name}
          </Typography>
        </Box>
        {getStatusChip(phase.status)}
      </Box>
      
      <ProgressContainer>
        <ProgressBar $width={phase.progress} />
      </ProgressContainer>
      
      <Typography variant="body2" sx={{ mt: 1, mb: 2 }}>
        {phase.progress}% complete
      </Typography>
      
      <Divider sx={{ my: 1, borderColor: 'rgba(255, 255, 255, 0.05)' }} />
      
      <Typography variant="body2" sx={{ mt: 1, fontWeight: 600 }}>
        Steps:
      </Typography>
      
      {phase.steps.map((step, index) => (
        <Box key={index} sx={{ mt: 1 }}>
          <Box sx={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
            <Typography variant="body2">{step.name}</Typography>
            {getStatusChip(step.status)}
          </Box>
          {step.status === 'in-progress' && (
            <LinearProgress 
              variant="determinate" 
              value={step.progress} 
              sx={{ 
                mt: 0.5, 
                height: 4, 
                borderRadius: 2,
                backgroundColor: 'rgba(30, 30, 40, 0.5)',
                '& .MuiLinearProgress-bar': {
                  backgroundColor: '#90caf9'
                }
              }} 
            />
          )}
        </Box>
      ))}
      
      <Box sx={{ mt: 'auto', pt: 2 }}>
        <Typography variant="body2" sx={{ opacity: 0.7 }}>
          {phase.startTime ? `Started: ${phase.startTime}` : 'Not started yet'}
        </Typography>
        <Typography variant="body2" sx={{ opacity: 0.7 }}>
          {phase.endTime ? `Completed: ${phase.endTime}` : ''}
        </Typography>
        <Typography variant="body2" sx={{ opacity: 0.7 }}>
          Duration: {phase.duration}
        </Typography>
      </Box>
      
      {phase.status === 'in-progress' && (
        <Box sx={{ display: 'flex', justifyContent: 'flex-end', mt: 2, gap: 1 }}>
          <ControlButton size="small">
            <PauseIcon width={16} height={16} />
          </ControlButton>
          <ControlButton size="small">
            <StopIcon width={16} height={16} />
          </ControlButton>
        </Box>
      )}
      
      {phase.status === 'not-started' && phase.id !== 'schema-setup' && (
        <Box sx={{ display: 'flex', justifyContent: 'flex-end', mt: 2 }}>
          <ControlButton size="small" disabled={
            // Disable if previous phase is not completed
            mockETLPhases.findIndex(p => p.id === phase.id) > 0 && 
            mockETLPhases[mockETLPhases.findIndex(p => p.id === phase.id) - 1].status !== 'completed'
          }>
            <PlayIcon width={16} height={16} />
          </ControlButton>
        </Box>
      )}
    </PhaseCard>
  );
};

function ETLPipeline() {
  // In a real implementation, this would fetch data from the API
  const etlPhases = mockETLPhases;
  
  // Calculate overall progress
  const overallProgress = etlPhases.reduce((acc, phase) => acc + phase.progress, 0) / etlPhases.length;
  
  return (
    <Box>
      <Typography variant="h5" gutterBottom sx={{ fontWeight: 600 }}>
        ETL Pipeline
      </Typography>
      
      <GlassCard sx={{ mb: 3 }}>
        <Box sx={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', mb: 2 }}>
          <Typography variant="h6">Overall Progress</Typography>
          <Typography variant="h6">{overallProgress.toFixed(2)}%</Typography>
        </Box>
        
        <ProgressContainer>
          <ProgressBar $width={overallProgress} />
        </ProgressContainer>
        
        <Box sx={{ display: 'flex', justifyContent: 'flex-end', mt: 2, gap: 1 }}>
          <ControlButton 
            variant="contained" 
            size="small"
            disabled={etlPhases.every(phase => phase.status === 'completed')}
          >
            <PauseIcon width={16} height={16} />
            <Typography sx={{ ml: 1 }}>Pause All</Typography>
          </ControlButton>
          
          <ControlButton 
            variant="contained" 
            size="small"
            disabled={etlPhases.every(phase => phase.status === 'completed')}
          >
            <StopIcon width={16} height={16} />
            <Typography sx={{ ml: 1 }}>Stop All</Typography>
          </ControlButton>
        </Box>
      </GlassCard>
      
      <Grid container spacing={3}>
        {etlPhases.map((phase, index) => (
          <Grid item xs={12} md={6} lg={4} key={phase.id}>
            <PhaseDetails phase={phase} />
          </Grid>
        ))}
      </Grid>
    </Box>
  );
}

export default ETLPipeline;
