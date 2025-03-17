import React, { useState, useEffect } from 'react';
import {
  Box,
  Card,
  CardContent,
  CardHeader,
  Divider,
  Typography,
  Table,
  TableBody,
  TableCell,
  TableContainer,
  TableHead,
  TableRow,
  Paper,
  Grid,
  Alert,
  AlertTitle,
  FormControl,
  InputLabel,
  Select,
  MenuItem,
  LinearProgress,
  Button,
  Tabs,
  Tab,
  Pagination,
} from '@mui/material';
import BarChartIcon from '@mui/icons-material/BarChart';
import TableChartIcon from '@mui/icons-material/TableChart';
import StorageIcon from '@mui/icons-material/Storage';
import DownloadIcon from '@mui/icons-material/Download';

// TabPanel component for the tabs
function TabPanel(props) {
  const { children, value, index, ...other } = props;

  return (
    <div
      role="tabpanel"
      hidden={value !== index}
      id={`achilles-tabpanel-${index}`}
      aria-labelledby={`achilles-tab-${index}`}
      {...other}
    >
      {value === index && (
        <Box sx={{ p: 3 }}>
          {children}
        </Box>
      )}
    </div>
  );
}

const AchillesResults = ({ schema = 'achilles_results' }) => {
  const [tables, setTables] = useState([]);
  const [selectedTable, setSelectedTable] = useState('');
  const [tableData, setTableData] = useState({
    columns: [],
    data: [],
    total: 0
  });
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState(null);
  const [page, setPage] = useState(0);
  const [rowsPerPage, setRowsPerPage] = useState(10);
  const [tabValue, setTabValue] = useState(0);

  // Fetch tables on component mount
  useEffect(() => {
    fetchTables();
  }, [schema]);

  // Fetch table data when selected table changes
  useEffect(() => {
    if (selectedTable) {
      fetchTableData();
    }
  }, [selectedTable, page, rowsPerPage]);

  const fetchTables = () => {
    setLoading(true);
    fetch(`http://localhost:5081/api/achilles/results/${schema}`)
      .then(response => {
        if (!response.ok) {
          throw new Error(`HTTP error ${response.status}`);
        }
        return response.json();
      })
      .then(data => {
        // Convert object to array of { table, count } objects
        const tableArray = Object.entries(data).map(([table, count]) => ({
          table,
          count
        }));
        
        // Sort by table name
        tableArray.sort((a, b) => a.table.localeCompare(b.table));
        
        setTables(tableArray);
        setLoading(false);
        
        // Select first table by default if available
        if (tableArray.length > 0 && !selectedTable) {
          setSelectedTable(tableArray[0].table);
        }
      })
      .catch(err => {
        console.error('Error fetching Achilles tables:', err);
        setError(`Failed to fetch tables: ${err.message}`);
        setLoading(false);
      });
  };

  const fetchTableData = () => {
    setLoading(true);
    fetch(`http://localhost:5081/api/achilles/table/${schema}/${selectedTable}?limit=${rowsPerPage}&offset=${page * rowsPerPage}`)
      .then(response => {
        if (!response.ok) {
          throw new Error(`HTTP error ${response.status}`);
        }
        return response.json();
      })
      .then(data => {
        setTableData({
          columns: data.columns || [],
          data: data.data || [],
          total: data.total || 0
        });
        setLoading(false);
      })
      .catch(err => {
        console.error('Error fetching table data:', err);
        setError(`Failed to fetch table data: ${err.message}`);
        setLoading(false);
      });
  };

  const handleTableChange = (event) => {
    setSelectedTable(event.target.value);
    setPage(0); // Reset to first page when changing tables
  };

  const handleChangePage = (event, newPage) => {
    setPage(newPage);
  };

  const handleChangeRowsPerPage = (event) => {
    setRowsPerPage(parseInt(event.target.value, 10));
    setPage(0);
  };

  const handleTabChange = (event, newValue) => {
    setTabValue(newValue);
  };

  // Function to export table data as CSV
  const exportTableData = () => {
    if (!tableData.data || tableData.data.length === 0) return;
    
    // Get column names
    const columns = tableData.columns.map(col => col.column_name);
    
    // Create CSV header row
    let csv = columns.join(',') + '\n';
    
    // Add data rows
    tableData.data.forEach(row => {
      const rowData = columns.map(col => {
        const value = row[col];
        // Handle values that need quotes (strings with commas, etc.)
        if (value === null || value === undefined) return '';
        if (typeof value === 'string' && (value.includes(',') || value.includes('"') || value.includes('\n'))) {
          return `"${value.replace(/"/g, '""')}"`;
        }
        return value;
      });
      csv += rowData.join(',') + '\n';
    });
    
    // Create download link
    const blob = new Blob([csv], { type: 'text/csv;charset=utf-8;' });
    const url = URL.createObjectURL(blob);
    const link = document.createElement('a');
    link.setAttribute('href', url);
    link.setAttribute('download', `${selectedTable}.csv`);
    link.style.visibility = 'hidden';
    document.body.appendChild(link);
    link.click();
    document.body.removeChild(link);
  };

  return (
    <Box sx={{ mt: 3 }}>
      <Card>
        <CardHeader 
          title="Achilles Results" 
          subheader="View and explore Achilles analysis results"
          avatar={<BarChartIcon />}
        />
        <Divider />
        <CardContent>
          {error && (
            <Alert severity="error" sx={{ mb: 3 }}>
              <AlertTitle>Error</AlertTitle>
              {error}
            </Alert>
          )}
          
          {loading && <LinearProgress sx={{ mb: 3 }} />}
          
          {tables.length === 0 && !loading && !error ? (
            <Alert severity="info">
              <AlertTitle>No Results Available</AlertTitle>
              No Achilles results found in the database. Please run an Achilles analysis first.
            </Alert>
          ) : (
            <>
              <Grid container spacing={3} sx={{ mb: 3 }}>
                <Grid item xs={12} md={6}>
                  <FormControl fullWidth>
                    <InputLabel id="table-select-label">Select Table</InputLabel>
                    <Select
                      labelId="table-select-label"
                      id="table-select"
                      value={selectedTable}
                      label="Select Table"
                      onChange={handleTableChange}
                      startAdornment={<StorageIcon sx={{ mr: 1 }} />}
                    >
                      {tables.map((table) => (
                        <MenuItem key={table.table} value={table.table}>
                          {table.table} ({table.count} rows)
                        </MenuItem>
                      ))}
                    </Select>
                  </FormControl>
                </Grid>
                <Grid item xs={12} md={6} sx={{ display: 'flex', alignItems: 'center', justifyContent: 'flex-end' }}>
                  <Button
                    variant="outlined"
                    startIcon={<DownloadIcon />}
                    onClick={exportTableData}
                    disabled={!selectedTable || tableData.data.length === 0}
                  >
                    Export CSV
                  </Button>
                </Grid>
              </Grid>
              
              <Box sx={{ borderBottom: 1, borderColor: 'divider' }}>
                <Tabs value={tabValue} onChange={handleTabChange} aria-label="achilles results tabs">
                  <Tab icon={<TableChartIcon />} label="Table View" id="achilles-tab-0" aria-controls="achilles-tabpanel-0" />
                  <Tab icon={<BarChartIcon />} label="Visualization" id="achilles-tab-1" aria-controls="achilles-tabpanel-1" disabled />
                </Tabs>
              </Box>
              
              <TabPanel value={tabValue} index={0}>
                {selectedTable && tableData.columns.length > 0 ? (
                  <>
                    <TableContainer component={Paper} variant="outlined">
                      <Table size="small" aria-label="achilles results table">
                        <TableHead>
                          <TableRow>
                            {tableData.columns.map((column) => (
                              <TableCell key={column.column_name}>
                                <Typography variant="subtitle2">
                                  {column.column_name}
                                </Typography>
                                <Typography variant="caption" color="text.secondary">
                                  {column.data_type}
                                </Typography>
                              </TableCell>
                            ))}
                          </TableRow>
                        </TableHead>
                        <TableBody>
                          {tableData.data.map((row, rowIndex) => (
                            <TableRow key={rowIndex}>
                              {tableData.columns.map((column) => (
                                <TableCell key={`${rowIndex}-${column.column_name}`}>
                                  {row[column.column_name] !== null ? row[column.column_name] : 'NULL'}
                                </TableCell>
                              ))}
                            </TableRow>
                          ))}
                        </TableBody>
                      </Table>
                    </TableContainer>
                    
                    <Box sx={{ display: 'flex', justifyContent: 'center', mt: 2 }}>
                      <Pagination
                        count={Math.ceil(tableData.total / rowsPerPage)}
                        page={page + 1}
                        onChange={(e, p) => handleChangePage(e, p - 1)}
                        color="primary"
                      />
                    </Box>
                    
                    <Typography variant="body2" color="text.secondary" sx={{ mt: 1, textAlign: 'center' }}>
                      Showing {Math.min(rowsPerPage, tableData.data.length)} of {tableData.total} rows
                    </Typography>
                  </>
                ) : (
                  <Typography variant="body1" sx={{ p: 2, textAlign: 'center' }}>
                    {selectedTable ? 'No data available for this table' : 'Please select a table to view data'}
                  </Typography>
                )}
              </TabPanel>
              
              <TabPanel value={tabValue} index={1}>
                <Typography variant="body1" sx={{ p: 2, textAlign: 'center' }}>
                  Visualization features will be available in a future update.
                </Typography>
              </TabPanel>
            </>
          )}
        </CardContent>
      </Card>
    </Box>
  );
};

export default AchillesResults;
