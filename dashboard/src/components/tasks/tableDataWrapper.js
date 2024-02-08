import NotInterestedIcon from '@mui/icons-material/NotInterested';
import RefreshIcon from '@mui/icons-material/Refresh';
import SearchIcon from '@mui/icons-material/Search';
import {
  Box,
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
import { useStyles } from './styles';
import TasksTable from './tasksTable';

export default function TableDataWrapper() {
  const classes = useStyles();
  const dataAPI = useDataAPI();
  const STATUS_ALL = 'ALL';
  const STATUS_ACTIVE = 'ACTIVE';
  const STATUS_COMPLETE = 'COMPLETE';
  const SORT_STATUS = 'STATUS';
  const SORT_DATE = 'STATUS_DATE';
  const ENTER_KEY = 'Enter';
  const [searchParams, setSearchParams] = useState({
    status: STATUS_ALL,
    sortBy: '',
    searchText: '',
  });
  const [searchQuery, setSearchQuery] = useState('');
  const [taskList, setTaskList] = useState([]);

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
    setSearchQuery('');
  };

  useEffect(async () => {
    let data = await dataAPI.getTasks(searchParams);
    setTaskList(data);
  }, [searchParams]);

  return (
    <>
      <Box className={classes.intputRow}>
        <Button
          variant='outlined'
          className={
            searchParams.status === STATUS_ALL
              ? classes.activeButton
              : classes.inactiveButton
          }
          onClick={() => handleStatusBtnSelect(STATUS_ALL)}
        >
          <Typography variant='button' className={classes.buttonText}>
            All
          </Typography>
          <Chip
            label={56}
            className={
              searchParams.status === STATUS_ALL
                ? classes.activeChip
                : classes.inactiveChip
            }
          />
        </Button>
        <Button
          variant='outlined'
          className={
            searchParams.status === STATUS_ACTIVE
              ? classes.activeButton
              : classes.inactiveButton
          }
          onClick={() => handleStatusBtnSelect(STATUS_ACTIVE)}
        >
          <Typography variant='button' className={classes.buttonText}>
            Active
          </Typography>
          <Chip
            label={32}
            className={
              searchParams.status === STATUS_ACTIVE
                ? classes.activeChip
                : classes.inactiveChip
            }
          />
        </Button>
        <Button
          variant='outlined'
          className={
            searchParams.status === STATUS_COMPLETE
              ? classes.activeButton
              : classes.inactiveButton
          }
          onClick={() => handleStatusBtnSelect(STATUS_COMPLETE)}
        >
          <Typography variant='button' className={classes.buttonText}>
            Complete
          </Typography>
          <Chip
            label={24}
            className={
              searchParams.status === STATUS_COMPLETE
                ? classes.activeChip
                : classes.inactiveChip
            }
          />
        </Button>

        <Box style={{ float: 'right' }}>
          <FormControl
            className={classes.filterInput}
            style={{ paddingRight: '15px' }}
          >
            <InputLabel id='sortId'>Sort By</InputLabel>
            <Select
              value={searchParams.sortBy}
              onChange={handleSortBy}
              label='Sort By'
              className={classes.filterInput}
            >
              <MenuItem value={SORT_STATUS}>Status</MenuItem>
              <MenuItem value={SORT_DATE}>Date</MenuItem>
            </Select>
          </FormControl>
          <FormControl>
            <TextField
              placeholder='Search Tasks...'
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
              className={classes.filterInput}
              inputProps={{
                'aria-label': 'search',
                'data-testid': 'searchInputId',
              }}
            />
          </FormControl>
          <IconButton size='large'>
            <RefreshIcon />
          </IconButton>
          <IconButton size='large' onClick={clearInputs}>
            <NotInterestedIcon />
          </IconButton>
        </Box>
      </Box>
      <TasksTable taskList={taskList} />
    </>
  );
}
