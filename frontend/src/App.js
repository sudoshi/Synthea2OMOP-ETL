import React, { useState } from 'react';
import { BrowserRouter as Router, Routes, Route } from 'react-router-dom';
import { ThemeProvider, createTheme } from '@mui/material/styles';
import CssBaseline from '@mui/material/CssBaseline';
import Box from '@mui/material/Box';

// Components
import Header from './components/Header';
import Sidebar from './components/Sidebar';
import Dashboard from './components/Dashboard';
import ETLMonitor from './components/ETLMonitor';
import DataProfiler from './components/DataProfiler';
import ETLPipeline from './components/ETLPipeline';
import UnifiedConfiguration from './components/UnifiedConfiguration';

// Create dark theme with glassmorphism
const theme = createTheme({
  palette: {
    mode: 'dark',
    primary: {
      main: '#90caf9',
    },
    secondary: {
      main: '#f48fb1',
    },
    background: {
      default: 'transparent',
      paper: 'rgba(30, 30, 40, 0.25)',
    },
  },
  components: {
    MuiPaper: {
      styleOverrides: {
        root: {
          backdropFilter: 'blur(10px)',
          WebkitBackdropFilter: 'blur(10px)',
          borderRadius: '10px',
          border: '1px solid rgba(255, 255, 255, 0.05)',
          boxShadow: '0 8px 32px 0 rgba(0, 0, 0, 0.37)',
          background: 'rgba(30, 30, 40, 0.25)',
        },
      },
    },
    MuiCard: {
      styleOverrides: {
        root: {
          backdropFilter: 'blur(10px)',
          WebkitBackdropFilter: 'blur(10px)',
          borderRadius: '10px',
          border: '1px solid rgba(255, 255, 255, 0.05)',
          boxShadow: '0 8px 32px 0 rgba(0, 0, 0, 0.37)',
          background: 'rgba(30, 30, 40, 0.25)',
          '&:hover': {
            background: 'rgba(30, 30, 40, 0.35)',
          },
        },
      },
    },
    MuiButton: {
      styleOverrides: {
        root: {
          borderRadius: '8px',
          textTransform: 'none',
          fontWeight: 500,
          backdropFilter: 'blur(10px)',
          WebkitBackdropFilter: 'blur(10px)',
          border: '1px solid rgba(255, 255, 255, 0.05)',
          background: 'rgba(30, 30, 40, 0.25)',
          '&:hover': {
            background: 'rgba(30, 30, 40, 0.4)',
          },
        },
      },
    },
    MuiAppBar: {
      styleOverrides: {
        root: {
          backdropFilter: 'blur(10px)',
          WebkitBackdropFilter: 'blur(10px)',
          background: 'rgba(30, 30, 40, 0.75)',
          boxShadow: '0 8px 32px 0 rgba(0, 0, 0, 0.37)',
          borderBottom: '1px solid rgba(255, 255, 255, 0.05)',
          borderRadius: 0, // Remove rounded corners from header
        },
      },
    },
    MuiDrawer: {
      styleOverrides: {
        paper: {
          backdropFilter: 'blur(10px)',
          WebkitBackdropFilter: 'blur(10px)',
          background: 'rgba(30, 30, 40, 0.75)',
          boxShadow: '0 8px 32px 0 rgba(0, 0, 0, 0.37)',
          borderRight: '1px solid rgba(255, 255, 255, 0.05)',
          borderRadius: 0, // Remove rounded corners from drawer
        },
      },
    },
    MuiTableContainer: {
      styleOverrides: {
        root: {
          backdropFilter: 'blur(10px)',
          WebkitBackdropFilter: 'blur(10px)',
          background: 'rgba(30, 30, 40, 0.25)',
          borderRadius: '10px',
          border: '1px solid rgba(255, 255, 255, 0.05)',
        },
      },
    },
  },
  typography: {
    fontFamily: [
      '-apple-system',
      'BlinkMacSystemFont',
      '"Segoe UI"',
      'Roboto',
      '"Helvetica Neue"',
      'Arial',
      'sans-serif',
    ].join(','),
    h1: {
      fontSize: '2.5rem',
      fontWeight: 600,
    },
    h2: {
      fontSize: '2rem',
      fontWeight: 600,
    },
    h3: {
      fontSize: '1.75rem',
      fontWeight: 600,
    },
    h4: {
      fontSize: '1.5rem',
      fontWeight: 600,
    },
    h5: {
      fontSize: '1.25rem',
      fontWeight: 600,
    },
    h6: {
      fontSize: '1rem',
      fontWeight: 600,
    },
  },
  shape: {
    borderRadius: 10,
  },
});

function App() {
  return (
    <ThemeProvider theme={theme}>
      <Router>
        <Box sx={{ display: 'flex' }}>
          <CssBaseline />
          <Header />
          <Sidebar />
          <Box
            component="main"
            sx={{
              backgroundColor: 'transparent',
              flexGrow: 1,
              height: '100vh',
              overflow: 'auto',
              pt: 10, // Increased padding to account for the app bar
              px: 3,
              ml: { xs: 0, sm: '20px' }, // Reduced margin to decrease space after sidebar
            }}
            className="main-content"
          >
            <Routes>
              <Route path="/" element={<ETLPipeline />} />
              <Route path="/config" element={<UnifiedConfiguration />} />
              <Route path="/monitor" element={<ETLMonitor />} />
              <Route path="/data-profiler" element={<DataProfiler />} />
            </Routes>
          </Box>
        </Box>
      </Router>
    </ThemeProvider>
  );
}

export default App;
