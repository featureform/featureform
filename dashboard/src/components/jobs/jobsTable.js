import RefreshIcon from '@mui/icons-material/Refresh';
import SearchIcon from '@mui/icons-material/Search';
import {
  Button,
  Chip,
  FormControl,
  IconButton,
  InputAdornment,
  InputLabel,
  MenuItem,
  Paper,
  Select,
  Table,
  TableBody,
  TableCell,
  TableContainer,
  TableHead,
  TableRow,
  TextField,
  Typography,
} from '@mui/material';
import React, { useState } from 'react';

export default function JobsTable({ jobsList = [] }) {
  let dummyJob = {
    name: 'agg1',
    type: 'Source',
    provider: 'Spark',
    resource: 'perc_balance',
    variant: 'v1',
    status: 'Pending',
    lastRuntime: '2024-06-15',
    triggeredBy: 'On Apply',
  };
  jobsList.push(dummyJob, dummyJob, dummyJob, dummyJob, dummyJob);

  const STATUS_ALL = 'ALL';
  const STATUS_ACTIVE = 'ACTIVE';
  const STATUS_COMPLETE = 'COMPLETE';
  const SORT_FAILED = 'FAILED';
  const SORT_PENDING = 'PENDING';
  const SORT_SUCCESSFUL = 'SUCCESSFUL';
  const ENTER_KEY = 'Enter';
  const [statusFilter, setStatusFilter] = useState(STATUS_ALL);
  const [statusSort, setStatusSort] = useState('');
  const [searchText, setSearchText] = useState('');

  const handleStatusBtnSelect = (statusType = STATUS_ALL) => {
    setStatusFilter(statusType);
  };

  const handleSortBy = (event) => {
    setStatusSort(event.target.value);
  };

  const handleRowSelect = (jobName) => {
    console.log('clicked on jobs row', jobName);
  };

  const handleSearch = (event) => {
    event.preventDefault();
    console.log('searching with:', event.target.value);
  };

  return (
    <>
      <div style={{ paddingBottom: '25px' }}>
        <Button
          variant='outlined'
          style={
            statusFilter === STATUS_ALL
              ? { color: 'white', background: '#FC195C' }
              : { color: 'black' }
          }
          onClick={() => handleStatusBtnSelect(STATUS_ALL)}
        >
          <Typography
            variant='button'
            style={{ textTransform: 'none', paddingRight: '10px' }}
          >
            All
          </Typography>
          <Chip
            label={56}
            style={
              statusFilter === STATUS_ALL
                ? { color: 'black', background: 'white' }
                : { color: 'white', background: '#FC195C' }
            }
          />
        </Button>
        <Button
          variant='outlined'
          style={
            statusFilter === STATUS_ACTIVE
              ? { color: 'white', background: '#FC195C' }
              : { color: 'black' }
          }
          onClick={() => handleStatusBtnSelect(STATUS_ACTIVE)}
        >
          <Typography
            variant='button'
            style={{ textTransform: 'none', paddingRight: '10px' }}
          >
            Active
          </Typography>
          <Chip
            label={32}
            style={
              statusFilter === STATUS_ACTIVE
                ? { color: 'black', background: 'white' }
                : { color: 'white', background: '#FC195C' }
            }
          />
        </Button>
        <Button
          variant='outlined'
          style={
            statusFilter === STATUS_COMPLETE
              ? { color: 'white', background: '#FC195C' }
              : { color: 'black' }
          }
          onClick={() => handleStatusBtnSelect(STATUS_COMPLETE)}
        >
          <Typography
            variant='button'
            style={{ textTransform: 'none', paddingRight: '10px' }}
          >
            Complete
          </Typography>
          <Chip
            label={24}
            style={
              statusFilter === STATUS_COMPLETE
                ? { color: 'black', background: 'white' }
                : { color: 'white', background: '#FC195C' }
            }
          />
        </Button>

        <span style={{ float: 'right' }}>
          <FormControl style={{ paddingRight: '15px' }}>
            <InputLabel id='sortId'>Sort By</InputLabel>
            <Select
              value={statusSort}
              onChange={handleSortBy}
              label='Sort By'
              style={{ minWidth: '200px' }}
            >
              <MenuItem value={SORT_SUCCESSFUL}>Successful</MenuItem>
              <MenuItem value={SORT_PENDING}>Pending</MenuItem>
              <MenuItem value={SORT_FAILED}>Failed</MenuItem>
            </Select>
          </FormControl>
          <FormControl>
            <TextField
              placeholder='Search Jobs...'
              onChange={(event) => {
                const rawText = event.target.value;
                if (rawText === '') {
                  // user is deleting the text field. allow this and clear out state
                  setSearchText(rawText);
                  return;
                }
                const searchText = event.target.value ?? '';
                if (searchText.trim()) {
                  setSearchText(searchText);
                }
              }}
              value={searchText}
              onKeyDown={(event) => {
                if (event.key === ENTER_KEY && searchText) {
                  handleSearch(event);
                }
              }}
              InputProps={{
                endAdornment: (
                  <InputAdornment position='end'>
                    <IconButton>
                      <SearchIcon />
                    </IconButton>
                  </InputAdornment>
                ),
              }}
              inputProps={{
                'aria-label': 'search',
                'data-testid': 'searchInputId',
              }}
            />
          </FormControl>
          <IconButton variant='' size='large'>
            <RefreshIcon />
          </IconButton>
        </span>
      </div>

      <TableContainer component={Paper}>
        <Table sx={{ minWidth: 300 }} aria-label='Job Runs'>
          <TableHead>
            <TableRow>
              <TableCell>Name</TableCell>
              <TableCell align='right'>Type</TableCell>
              <TableCell align='right'>Provider</TableCell>
              <TableCell align='right'>Resource</TableCell>
              <TableCell align='right'>Variant</TableCell>
              <TableCell align='right'>Status</TableCell>
              <TableCell align='right'>Last Runtime</TableCell>
              <TableCell align='right'>Triggered By</TableCell>
            </TableRow>
          </TableHead>
          <TableBody>
            {jobsList?.map((job, index) => {
              return (
                <TableRow
                  key={index}
                  onClick={() => handleRowSelect(job.name)}
                  style={{ cursor: 'pointer' }}
                  hover
                >
                  <TableCell>{job.name}</TableCell>
                  <TableCell align='right'>{job.type}</TableCell>
                  <TableCell align='right'>{job.provider}</TableCell>
                  <TableCell align='right'>{job.resource}</TableCell>
                  <TableCell align='right'>{job.variant}</TableCell>
                  <TableCell align='right'>{job.status}</TableCell>
                  <TableCell align='right'>{job.lastRuntime}</TableCell>
                  <TableCell align='right'>{job.triggeredBy}</TableCell>
                </TableRow>
              );
            })}
          </TableBody>
        </Table>
      </TableContainer>
    </>
  );
}
