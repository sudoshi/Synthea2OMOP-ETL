import React from 'react';
import { Container, Grid, Paper, Typography, Box, Button } from '@mui/material';
import { styled } from '@mui/material/styles';
import { Link } from 'react-router-dom';
import { 
  RocketIcon, 
  LightningBoltIcon, 
  BarChartIcon, 
  TableIcon, 
  GearIcon, 
  DesktopIcon, 
  FileTextIcon 
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

const QuickLinkButton = styled(Button)(({ theme }) => ({
  backdropFilter: 'blur(10px)',
  WebkitBackdropFilter: 'blur(10px)',
  background: 'rgba(30, 30, 40, 0.25)',
  border: '1px solid rgba(255, 255, 255, 0.05)',
  borderRadius: '8px',
  padding: theme.spacing(1.5),
  color: '#90caf9', // Updated to match theme primary color
  textTransform: 'none',
  fontWeight: 500,
  height: '100%',
  display: 'flex',
  flexDirection: 'column',
  alignItems: 'center',
  justifyContent: 'center',
  gap: '8px',
  '&:hover': {
    background: 'rgba(144, 202, 249, 0.15)',
  },
  '& svg': {
    color: '#90caf9' // Ensure icons also use the theme color
  }
}));

const GradientText = styled(Typography)(({ theme }) => ({
  fontWeight: 600,
  background: 'linear-gradient(90deg, #90caf9, #f48fb1)',
  WebkitBackgroundClip: 'text',
  WebkitTextFillColor: 'transparent',
  textShadow: '0 0 20px rgba(144, 202, 249, 0.3)',
}));

function Dashboard() {
  return (
    <Container maxWidth="lg" sx={{ mt: 4, mb: 4 }}>
      <GradientText variant="h4" gutterBottom component="h2" className="mb-6">
        Synthea2OMOP ETL Dashboard
      </GradientText>
      
      <Grid container spacing={3}>
        {/* ETL Status */}
        <Grid item xs={12} md={8} lg={9}>
          <GlassCard className="glass-card">
            <Box sx={{ display: 'flex', alignItems: 'center', mb: 2 }}>
              <RocketIcon width={24} height={24} className="mr-2" style={{ color: '#90caf9' }} />
              <Typography component="h2" variant="h6" color="primary" fontWeight={600}>
                ETL Status
              </Typography>
            </Box>
            
            <Typography component="p" variant="h4" sx={{ mb: 1 }}>
              <span style={{ color: '#90caf9' }}>In Progress</span>
            </Typography>
            
            <Typography sx={{ mb: 2 }}>
              Overall Progress: 20.13%
            </Typography>
            
            <ProgressContainer>
              <ProgressBar $width={20.13} />
            </ProgressContainer>
            
            <Typography variant="body2" sx={{ mt: 2, opacity: 0.7 }}>
              Started: 2025-03-17 04:30:12 • Estimated completion: 2025-03-17 06:45:00
            </Typography>
          </GlassCard>
        </Grid>
        
        {/* System Resources */}
        <Grid item xs={12} md={4} lg={3}>
          <GlassCard className="glass-card">
            <Box sx={{ display: 'flex', alignItems: 'center', mb: 2 }}>
              <LightningBoltIcon width={24} height={24} className="mr-2" style={{ color: '#90caf9' }} />
              <Typography component="h2" variant="h6" color="primary" fontWeight={600}>
                System Resources
              </Typography>
            </Box>
            
            <Box sx={{ display: 'flex', flexDirection: 'column', gap: 2 }}>
              <Box>
                <Typography variant="body2" sx={{ mb: 0.5 }}>Memory Usage</Typography>
                <ProgressContainer>
                  <ProgressBar $width={34.7} />
                </ProgressContainer>
                <Typography variant="body1" fontWeight={500}>34.7%</Typography>
              </Box>
              
              <Box>
                <Typography variant="body2" sx={{ mb: 0.5 }}>CPU Usage</Typography>
                <ProgressContainer>
                  <ProgressBar $width={5.9} />
                </ProgressContainer>
                <Typography variant="body1" fontWeight={500}>5.9%</Typography>
              </Box>
            </Box>
          </GlassCard>
        </Grid>
        
        {/* Table Progress */}
        <Grid item xs={12}>
          <GlassCard className="glass-card">
            <Box sx={{ display: 'flex', alignItems: 'center', mb: 3 }}>
              <TableIcon width={24} height={24} className="mr-2" style={{ color: '#90caf9' }} />
              <Typography component="h2" variant="h6" color="primary" fontWeight={600}>
                Table Progress
              </Typography>
            </Box>
            
            <Grid container spacing={2}>
              <Grid item xs={12}>
                <Box sx={{ mb: 2 }}>
                  <Box sx={{ display: 'flex', justifyContent: 'space-between', mb: 0.5 }}>
                    <Typography variant="body2">Visit Occurrence</Typography>
                    <Typography variant="body2" fontWeight={600}>100%</Typography>
                  </Box>
                  <ProgressContainer>
                    <ProgressBar $width={100} />
                  </ProgressContainer>
                </Box>
              </Grid>
              
              <Grid item xs={12}>
                <Box sx={{ mb: 2 }}>
                  <Box sx={{ display: 'flex', justifyContent: 'space-between', mb: 0.5 }}>
                    <Typography variant="body2">Person</Typography>
                    <Typography variant="body2" fontWeight={600}>0.66%</Typography>
                  </Box>
                  <ProgressContainer>
                    <ProgressBar $width={0.66} />
                  </ProgressContainer>
                </Box>
              </Grid>
              
              <Grid item xs={12}>
                <Box sx={{ mb: 2 }}>
                  <Box sx={{ display: 'flex', justifyContent: 'space-between', mb: 0.5 }}>
                    <Typography variant="body2">Condition Occurrence</Typography>
                    <Typography variant="body2" fontWeight={600}>0%</Typography>
                  </Box>
                  <ProgressContainer>
                    <ProgressBar $width={0} />
                  </ProgressContainer>
                </Box>
              </Grid>
              
              <Grid item xs={12}>
                <Box sx={{ mb: 2 }}>
                  <Box sx={{ display: 'flex', justifyContent: 'space-between', mb: 0.5 }}>
                    <Typography variant="body2">Drug Exposure</Typography>
                    <Typography variant="body2" fontWeight={600}>0%</Typography>
                  </Box>
                  <ProgressContainer>
                    <ProgressBar $width={0} />
                  </ProgressContainer>
                </Box>
              </Grid>
              
              <Grid item xs={12}>
                <Box sx={{ mb: 2 }}>
                  <Box sx={{ display: 'flex', justifyContent: 'space-between', mb: 0.5 }}>
                    <Typography variant="body2">Procedure Occurrence</Typography>
                    <Typography variant="body2" fontWeight={600}>0%</Typography>
                  </Box>
                  <ProgressContainer>
                    <ProgressBar $width={0} />
                  </ProgressContainer>
                </Box>
              </Grid>
            </Grid>
          </GlassCard>
        </Grid>
        
        {/* Quick Links */}
        <Grid item xs={12}>
          <GlassCard className="glass-card">
            <Box sx={{ display: 'flex', alignItems: 'center', mb: 3 }}>
              <BarChartIcon width={24} height={24} className="mr-2" style={{ color: '#90caf9' }} />
              <Typography component="h2" variant="h6" color="primary" fontWeight={600}>
                Quick Links
              </Typography>
            </Box>
            
            <Grid container spacing={2}>
              <Grid item xs={12} sm={6} md={3}>
                <QuickLinkButton component={Link} to="/config" fullWidth>
                  <GearIcon width={24} height={24} />
                  <Typography variant="body1">Configure ETL</Typography>
                </QuickLinkButton>
              </Grid>
              
              <Grid item xs={12} sm={6} md={3}>
                <QuickLinkButton component={Link} to="/monitor" fullWidth>
                  <DesktopIcon width={24} height={24} />
                  <Typography variant="body1">Monitor Progress</Typography>
                </QuickLinkButton>
              </Grid>
              
              <Grid item xs={12} sm={6} md={3}>
                <QuickLinkButton component={Link} to="/data" fullWidth>
                  <TableIcon width={24} height={24} />
                  <Typography variant="body1">View Data</Typography>
                </QuickLinkButton>
              </Grid>
              
              <Grid item xs={12} sm={6} md={3}>
                <QuickLinkButton component="a" href="https://github.com/acumenus/Synthea2OMOP-ETL" target="_blank" fullWidth>
                  <FileTextIcon width={24} height={24} />
                  <Typography variant="body1">Documentation</Typography>
                </QuickLinkButton>
              </Grid>
            </Grid>
          </GlassCard>
        </Grid>
      </Grid>
    </Container>
  );
}

export default Dashboard;
