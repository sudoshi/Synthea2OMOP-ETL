import React from 'react';
import { styled } from '@mui/material/styles';
import MuiAppBar from '@mui/material/AppBar';
import Toolbar from '@mui/material/Toolbar';
import Typography from '@mui/material/Typography';
import IconButton from '@mui/material/IconButton';
import { GearIcon } from '@radix-ui/react-icons';

const AppBar = styled(MuiAppBar)(({ theme }) => ({
  zIndex: theme.zIndex.drawer + 1,
  width: '100%',
  position: 'fixed',
  backdropFilter: 'blur(10px)',
  WebkitBackdropFilter: 'blur(10px)',
  background: 'rgba(30, 30, 40, 0.75)',
  boxShadow: '0 8px 32px 0 rgba(0, 0, 0, 0.37)',
  borderBottom: '1px solid rgba(255, 255, 255, 0.05)',
  borderRadius: 0, // Remove rounded corners from header
}));

// Custom styled button with glassmorphism effect
const GlassIconButton = styled(IconButton)(({ theme }) => ({
  backdropFilter: 'blur(10px)',
  WebkitBackdropFilter: 'blur(10px)',
  background: 'rgba(255, 255, 255, 0.05)',
  border: '1px solid rgba(255, 255, 255, 0.05)',
  borderRadius: '8px',
  '&:hover': {
    background: 'rgba(255, 255, 255, 0.1)',
  },
}));

function Header() {
  return (
    <AppBar position="fixed" className="glass">
      <Toolbar
        sx={{
          pr: '24px', // keep right padding when drawer closed
        }}
      >
        <Typography
          component="h1"
          variant="h6"
          color="inherit"
          noWrap
          sx={{ 
            flexGrow: 1,
            fontWeight: 600,
            background: 'linear-gradient(90deg, #90caf9, #f48fb1)',
            WebkitBackgroundClip: 'text',
            WebkitTextFillColor: 'transparent',
            textShadow: '0 0 20px rgba(144, 202, 249, 0.3)'
          }}
        >
          Synthea2OMOP ETL Dashboard
        </Typography>
        
        <GlassIconButton color="inherit" aria-label="settings" sx={{ ml: 1 }}>
          <GearIcon width={20} height={20} />
        </GlassIconButton>
      </Toolbar>
    </AppBar>
  );
}

export default Header;
