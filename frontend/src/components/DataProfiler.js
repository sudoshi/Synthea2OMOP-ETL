import React, { useState } from 'react';
import {
  Container,
  Typography,
  Box,
  Paper,
  TextField,
  Button,
  Table,
  TableBody,
  TableCell,
  TableContainer,
  TableHead,
  TableRow,
  FormControl,
  InputLabel,
  Select,
  MenuItem,
  Tabs,
  Tab,
  Pagination,
} from '@mui/material';
import SearchIcon from '@mui/icons-material/Search';
import DownloadIcon from '@mui/icons-material/Download';

function DataProfiler() {
  const [activeTab, setActiveTab] = useState(0);
  const [schema, setSchema] = useState('omop');
  const [table, setTable] = useState('person');
  const [searchTerm, setSearchTerm] = useState('');
  const [page, setPage] = useState(1);
  const [rowsPerPage, setRowsPerPage] = useState(10);

  // Mock data for demonstration
  const schemas = ['omop', 'staging', 'population'];
  const tables = {
    omop: ['person', 'visit_occurrence', 'condition_occurrence', 'drug_exposure', 'procedure_occurrence', 'measurement', 'observation'],
    staging: ['staging_person', 'staging_visit', 'staging_condition', 'staging_drug', 'staging_procedure', 'staging_observation'],
    population: ['patients_typed', 'encounters_typed', 'conditions_typed', 'medications_typed', 'procedures_typed', 'observations_typed'],
  };

  // Mock data for table content
  const mockData = {
    person: [
      { person_id: 1, gender_concept_id: 8507, year_of_birth: 1975, race_concept_id: 8527, ethnicity_concept_id: 38003564 },
      { person_id: 2, gender_concept_id: 8532, year_of_birth: 1982, race_concept_id: 8516, ethnicity_concept_id: 38003564 },
      { person_id: 3, gender_concept_id: 8507, year_of_birth: 1990, race_concept_id: 8527, ethnicity_concept_id: 38003563 },
      { person_id: 4, gender_concept_id: 8532, year_of_birth: 1965, race_concept_id: 8516, ethnicity_concept_id: 38003564 },
      { person_id: 5, gender_concept_id: 8507, year_of_birth: 1945, race_concept_id: 8527, ethnicity_concept_id: 38003564 },
    ],
    visit_occurrence: [
      { visit_occurrence_id: 1, person_id: 1, visit_concept_id: 9201, visit_start_date: '2020-01-01', visit_end_date: '2020-01-03' },
      { visit_occurrence_id: 2, person_id: 1, visit_concept_id: 9202, visit_start_date: '2020-02-15', visit_end_date: '2020-02-15' },
      { visit_occurrence_id: 3, person_id: 2, visit_concept_id: 9203, visit_start_date: '2020-01-10', visit_end_date: '2020-01-10' },
      { visit_occurrence_id: 4, person_id: 3, visit_concept_id: 9201, visit_start_date: '2020-03-20', visit_end_date: '2020-03-25' },
      { visit_occurrence_id: 5, person_id: 4, visit_concept_id: 9202, visit_start_date: '2020-04-05', visit_end_date: '2020-04-05' },
    ],
  };

  // Get columns for the current table
  const getColumns = () => {
    if (table === 'person' && mockData.person && mockData.person.length > 0) {
      return Object.keys(mockData.person[0]);
    } else if (table === 'visit_occurrence' && mockData.visit_occurrence && mockData.visit_occurrence.length > 0) {
      return Object.keys(mockData.visit_occurrence[0]);
    }
    return [];
  };

  // Get data for the current table
  const getData = () => {
    if (table === 'person' && mockData.person) {
      return mockData.person;
    } else if (table === 'visit_occurrence' && mockData.visit_occurrence) {
      return mockData.visit_occurrence;
    }
    return [];
  };

  const handleTabChange = (event, newValue) => {
    setActiveTab(newValue);
  };

  const handleSchemaChange = (event) => {
    setSchema(event.target.value);
    setTable(tables[event.target.value][0]);
  };

  const handleTableChange = (event) => {
    setTable(event.target.value);
  };

  const handleSearch = () => {
    // In a real app, this would filter the data based on the search term
    console.log('Searching for:', searchTerm);
  };

  const handleExport = () => {
    // In a real app, this would export the data to a CSV file
    console.log('Exporting data');
  };

  const handlePageChange = (event, value) => {
    setPage(value);
  };

  const handleRowsPerPageChange = (event) => {
    setRowsPerPage(event.target.value);
    setPage(1);
  };

  return (
    <Container maxWidth="xl" sx={{ mt: 4, mb: 4 }}>
      <Typography variant="h4" gutterBottom component="h2">
        Data Profiler
      </Typography>

      <Tabs value={activeTab} onChange={handleTabChange} sx={{ mb: 3 }}>
        <Tab label="Browse Data" />
        <Tab label="Run Query" />
        <Tab label="Data Quality" />
      </Tabs>

      {activeTab === 0 && (
        <>
          {/* Data Browser */}
          <Paper sx={{ p: 2, mb: 3 }}>
            <Box sx={{ display: 'flex', flexWrap: 'wrap', gap: 2, mb: 3 }}>
              <FormControl sx={{ minWidth: 200 }}>
                <InputLabel>Schema</InputLabel>
                <Select
                  value={schema}
                  label="Schema"
                  onChange={handleSchemaChange}
                >
                  {schemas.map((s) => (
                    <MenuItem key={s} value={s}>
                      {s}
                    </MenuItem>
                  ))}
                </Select>
              </FormControl>

              <FormControl sx={{ minWidth: 200 }}>
                <InputLabel>Table</InputLabel>
                <Select
                  value={table}
                  label="Table"
                  onChange={handleTableChange}
                >
                  {tables[schema].map((t) => (
                    <MenuItem key={t} value={t}>
                      {t}
                    </MenuItem>
                  ))}
                </Select>
              </FormControl>

              <TextField
                label="Search"
                variant="outlined"
                value={searchTerm}
                onChange={(e) => setSearchTerm(e.target.value)}
                sx={{ flexGrow: 1 }}
              />

              <Button
                variant="contained"
                startIcon={<SearchIcon />}
                onClick={handleSearch}
                sx={{ 
                  color: '#90caf9',
                  background: 'rgba(144, 202, 249, 0.15)',
                  '&:hover': {
                    background: 'rgba(144, 202, 249, 0.25)',
                  }
                }}
              >
                Search
              </Button>

              <Button
                variant="outlined"
                startIcon={<DownloadIcon />}
                onClick={handleExport}
                sx={{ 
                  color: '#90caf9',
                  borderColor: 'rgba(144, 202, 249, 0.5)',
                  '&:hover': {
                    borderColor: '#90caf9',
                    background: 'rgba(144, 202, 249, 0.08)',
                  }
                }}
              >
                Export
              </Button>
            </Box>

            <TableContainer>
              <Table size="small">
                <TableHead>
                  <TableRow>
                    {getColumns().map((column) => (
                      <TableCell key={column}>{column}</TableCell>
                    ))}
                  </TableRow>
                </TableHead>
                <TableBody>
                  {getData()
                    .slice((page - 1) * rowsPerPage, page * rowsPerPage)
                    .map((row, index) => (
                      <TableRow key={index}>
                        {getColumns().map((column) => (
                          <TableCell key={column}>{row[column]}</TableCell>
                        ))}
                      </TableRow>
                    ))}
                </TableBody>
              </Table>
            </TableContainer>

            <Box sx={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', mt: 2 }}>
              <FormControl sx={{ minWidth: 120 }}>
                <InputLabel>Rows per page</InputLabel>
                <Select
                  value={rowsPerPage}
                  label="Rows per page"
                  onChange={handleRowsPerPageChange}
                >
                  <MenuItem value={5}>5</MenuItem>
                  <MenuItem value={10}>10</MenuItem>
                  <MenuItem value={25}>25</MenuItem>
                  <MenuItem value={50}>50</MenuItem>
                </Select>
              </FormControl>

              <Pagination
                count={Math.ceil(getData().length / rowsPerPage)}
                page={page}
                onChange={handlePageChange}
              />
            </Box>
          </Paper>
        </>
      )}

      {activeTab === 1 && (
        <>
          {/* SQL Query */}
          <Paper sx={{ p: 2, mb: 3 }}>
            <Typography variant="h6" gutterBottom>
              Run SQL Query
            </Typography>
            <TextField
              label="SQL Query"
              multiline
              rows={6}
              fullWidth
              variant="outlined"
              placeholder="SELECT * FROM omop.person LIMIT 10;"
              sx={{ mb: 2 }}
            />
            <Button 
              variant="contained" 
              sx={{ 
                color: '#90caf9',
                background: 'rgba(144, 202, 249, 0.15)',
                '&:hover': {
                  background: 'rgba(144, 202, 249, 0.25)',
                }
              }}
            >
              Run Query
            </Button>
          </Paper>

          {/* Query Results */}
          <Paper sx={{ p: 2 }}>
            <Typography variant="h6" gutterBottom>
              Query Results
            </Typography>
            <Typography variant="body2" color="text.secondary" sx={{ mb: 2 }}>
              Run a query to see results
            </Typography>
          </Paper>
        </>
      )}

      {activeTab === 2 && (
        <>
          {/* Data Quality */}
          <Paper sx={{ p: 2 }}>
            <Typography variant="h6" gutterBottom>
              Data Quality Metrics
            </Typography>
            <TableContainer>
              <Table size="small">
                <TableHead>
                  <TableRow>
                    <TableCell>Table</TableCell>
                    <TableCell>Row Count</TableCell>
                    <TableCell>Null Values</TableCell>
                    <TableCell>Duplicates</TableCell>
                    <TableCell>Completeness</TableCell>
                  </TableRow>
                </TableHead>
                <TableBody>
                  <TableRow>
                    <TableCell>person</TableCell>
                    <TableCell>5,823</TableCell>
                    <TableCell>0</TableCell>
                    <TableCell>0</TableCell>
                    <TableCell>100%</TableCell>
                  </TableRow>
                  <TableRow>
                    <TableCell>visit_occurrence</TableCell>
                    <TableCell>67,018,822</TableCell>
                    <TableCell>0</TableCell>
                    <TableCell>0</TableCell>
                    <TableCell>100%</TableCell>
                  </TableRow>
                  <TableRow>
                    <TableCell>condition_occurrence</TableCell>
                    <TableCell>0</TableCell>
                    <TableCell>-</TableCell>
                    <TableCell>-</TableCell>
                    <TableCell>0%</TableCell>
                  </TableRow>
                  <TableRow>
                    <TableCell>drug_exposure</TableCell>
                    <TableCell>0</TableCell>
                    <TableCell>-</TableCell>
                    <TableCell>-</TableCell>
                    <TableCell>0%</TableCell>
                  </TableRow>
                  <TableRow>
                    <TableCell>procedure_occurrence</TableCell>
                    <TableCell>0</TableCell>
                    <TableCell>-</TableCell>
                    <TableCell>-</TableCell>
                    <TableCell>0%</TableCell>
                  </TableRow>
                </TableBody>
              </Table>
            </TableContainer>
          </Paper>
        </>
      )}
    </Container>
  );
}

export default DataProfiler;
