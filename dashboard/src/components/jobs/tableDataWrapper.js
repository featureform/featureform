import NotInterestedIcon from '@mui/icons-material/NotInterested';
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
  Select,
  TextField,
  Typography,
} from '@mui/material';
import React, { useEffect, useState } from 'react';
import { useDataAPI } from '../../hooks/dataAPI';
import JobsTable from './jobsTable';

export default function TableDataWrapper() {
  const dataAPI = useDataAPI();

  const STATUS_ALL = 'ALL';
  const STATUS_ACTIVE = 'ACTIVE';
  const STATUS_COMPLETE = 'COMPLETE';
  const SORT_FAILED = 'FAILED';
  const SORT_PENDING = 'PENDING';
  const SORT_SUCCESSFUL = 'SUCCESSFUL';
  const ENTER_KEY = 'Enter';
  const [searchParams, setSearchParams] = useState({
    status: STATUS_ALL,
    sortBy: '',
    searchText: '',
  });
  const [searchQuery, setSearchQuery] = useState('');
  const [jobsList, setJobsList] = useState([]);

  const handleStatusBtnSelect = (statusType = STATUS_ALL) => {
    setSearchParams({ ...searchParams, status: statusType });
  };

  const handleSortBy = (event) => {
    let value = event?.target?.value ?? '';
    setSearchParams({ ...searchParams, sortBy: value });
  };

  const handleSearch = (searchArg = '') => {
    setSearchParams({ ...searchParams, searchText: searchArg });
  };

  const clearInputs = () => {
    setSearchParams({
      status: STATUS_ALL,
      sortBy: '',
      searchText: '',
    });
  };

  useEffect(async () => {
    let data = await dataAPI.getJobs(searchParams);
    console.log(data);
    setJobsList(data);
  }, [searchParams]);

  return (
    <>
      <div style={{ paddingBottom: '25px' }}>
        <Button
          variant='outlined'
          style={
            searchParams.status === STATUS_ALL
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
              searchParams.status === STATUS_ALL
                ? { color: 'black', background: 'white' }
                : { color: 'white', background: '#FC195C' }
            }
          />
        </Button>
        <Button
          variant='outlined'
          style={
            searchParams.status === STATUS_ACTIVE
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
              searchParams.status === STATUS_ACTIVE
                ? { color: 'black', background: 'white' }
                : { color: 'white', background: '#FC195C' }
            }
          />
        </Button>
        <Button
          variant='outlined'
          style={
            searchParams.status === STATUS_COMPLETE
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
              searchParams.status === STATUS_COMPLETE
                ? { color: 'black', background: 'white' }
                : { color: 'white', background: '#FC195C' }
            }
          />
        </Button>

        <span style={{ float: 'right' }}>
          <FormControl style={{ paddingRight: '15px' }}>
            <InputLabel id='sortId'>Sort By</InputLabel>
            <Select
              value={searchParams.sortBy}
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
                  setSearchQuery(rawText);
                  handleSearch('');
                  return;
                }
                const searchText = event.target.value ?? '';
                if (searchText.trim()) {
                  setSearchQuery(searchText);
                }
              }}
              value={searchQuery}
              onKeyDown={(event) => {
                if (event.key === ENTER_KEY && searchQuery) {
                  // todox: odd case since i'm tracking 2 search props.
                  // the one in the searchparams, and also the input's itself.
                  // the searchParams, won't update unless you hit ENTER.
                  // so you can ultimately search with a stale searchParam.searchText value
                  handleSearch(searchQuery);
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
          <IconButton variant='' size='large' onClick={clearInputs}>
            <NotInterestedIcon />
          </IconButton>
        </span>
      </div>
      <JobsTable jobsList={jobsList} />
    </>
  );
}
