import React from 'react';
import { Link, useLocation } from 'react-router-dom';
import { styled } from '@mui/material/styles';
import MuiDrawer from '@mui/material/Drawer';
import Toolbar from '@mui/material/Toolbar';
import List from '@mui/material/List';
import Divider from '@mui/material/Divider';
import IconButton from '@mui/material/IconButton';
import ListItemButton from '@mui/material/ListItemButton';
import ListItemIcon from '@mui/material/ListItemIcon';
import ListItemText from '@mui/material/ListItemText';
import Typography from '@mui/material/Typography';
import { 
  ChevronLeftIcon, 
  DashboardIcon, 
  GearIcon, 
  DesktopIcon, 
  TableIcon, 
  BarChartIcon,
  RocketIcon,
  PersonIcon
} from '@radix-ui/react-icons';

const drawerWidth = 240;

const Drawer = styled(MuiDrawer)(({ theme }) => ({
  '& .MuiDrawer-paper': {
    position: 'relative',
    whiteSpace: 'nowrap',
    width: drawerWidth,
    boxSizing: 'border-box',
    backdropFilter: 'blur(10px)',
    WebkitBackdropFilter: 'blur(10px)',
    background: 'rgba(30, 30, 40, 0.75)',
    boxShadow: '0 8px 32px 0 rgba(0, 0, 0, 0.37)',
    borderRight: '1px solid rgba(255, 255, 255, 0.05)',
  },
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

// Custom styled nav item with glassmorphism effect
const GlassNavItem = styled(ListItemButton)(({ theme, $active }) => ({
  margin: '8px 12px',
  borderRadius: '8px',
  backdropFilter: 'blur(10px)',
  WebkitBackdropFilter: 'blur(10px)',
  border: '1px solid rgba(255, 255, 255, 0.05)',
  background: $active ? 'rgba(144, 202, 249, 0.15)' : 'rgba(255, 255, 255, 0.05)',
  '&:hover': {
    background: 'rgba(144, 202, 249, 0.2)',
  },
}));

function Sidebar({ open }) {
  const location = useLocation();
  
  const isActive = (path) => {
    return location.pathname === path;
  };

  return (
    <Drawer variant="permanent" className="glass">
      <Toolbar
        sx={{
          display: 'flex',
          alignItems: 'center',
          justifyContent: 'center',
          px: [1],
        }}
      >
        <Typography 
          variant="subtitle1" 
          sx={{ 
            fontWeight: 600,
            color: '#90caf9'
          }}
        >
          Navigation
        </Typography>
      </Toolbar>
      <Divider sx={{ borderColor: 'rgba(255, 255, 255, 0.05)' }} />
      <List component="nav" sx={{ p: 1 }}>
        <GlassNavItem component={Link} to="/" $active={isActive('/')}>
          <ListItemIcon>
            <RocketIcon width={20} height={20} />
          </ListItemIcon>
          <ListItemText 
            primary="ETL Pipeline" 
            primaryTypographyProps={{ 
              sx: { 
                fontWeight: isActive('/') ? 600 : 400,
                color: isActive('/') ? '#90caf9' : 'inherit'
              } 
            }} 
          />
        </GlassNavItem>
        <GlassNavItem component={Link} to="/config" $active={isActive('/config')}>
          <ListItemIcon>
            <GearIcon width={20} height={20} />
          </ListItemIcon>
          <ListItemText 
            primary="Configuration" 
            primaryTypographyProps={{ 
              sx: { 
                fontWeight: isActive('/config') ? 600 : 400,
                color: isActive('/config') ? '#90caf9' : 'inherit'
              } 
            }} 
          />
        </GlassNavItem>
        <GlassNavItem component={Link} to="/monitor" $active={isActive('/monitor')}>
          <ListItemIcon>
            <DesktopIcon width={20} height={20} />
          </ListItemIcon>
          <ListItemText 
            primary="ETL Monitor" 
            primaryTypographyProps={{ 
              sx: { 
                fontWeight: isActive('/monitor') ? 600 : 400,
                color: isActive('/monitor') ? '#90caf9' : 'inherit'
              } 
            }} 
          />
        </GlassNavItem>
        <GlassNavItem component={Link} to="/data-profiler" $active={isActive('/data-profiler')}>
          <ListItemIcon>
            <TableIcon width={20} height={20} />
          </ListItemIcon>
          <ListItemText 
            primary="Data Profiler" 
            primaryTypographyProps={{ 
              sx: { 
                fontWeight: isActive('/data-profiler') ? 600 : 400,
                color: isActive('/data-profiler') ? '#90caf9' : 'inherit'
              } 
            }} 
          />
        </GlassNavItem>
      </List>
    </Drawer>
  );
}

export default Sidebar;
