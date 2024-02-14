import ClearAllIcon from '@mui/icons-material/ClearAll';
import RefreshIcon from '@mui/icons-material/Refresh';
import SearchIcon from '@mui/icons-material/Search';
import {
  Box,
  Button,
  Chip,
  CircularProgress,
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
import TaskRunDataGrid from './taskRunDataGrid';

export default function TableDataWrapper() {
  const classes = useStyles();
  const dataAPI = useDataAPI();
  const FILTER_STATUS_ALL = 'ALL';
  const FILTER_STATUS_ACTIVE = 'ACTIVE';
  const FILTER_STATUS_COMPLETE = 'COMPLETE';
  const JOB_STATUS_RUNNING = 'RUNNING';
  const JOB_STATUS_PENDING = 'PENDING';
  const JOB_STATUS_SUCCESS = 'SUCCESS';
  const JOB_STATUS_FAILED = 'FAILED';
  const SORT_STATUS = 'STATUS';
  const SORT_DATE = 'STATUS_DATE';
  const ENTER_KEY = 'Enter';
  const [searchParams, setSearchParams] = useState({
    status: FILTER_STATUS_ALL,
    sortBy: '',
    searchText: '',
  });
  const [searchQuery, setSearchQuery] = useState('');
  const [taskRunList, setTaskRunList] = useState([]);
  const [loading, setLoading] = useState(true);
  const [allCount, setAllCount] = useState(0);
  const [activeCount, setActiveCount] = useState(0);
  const [completeCount, setCompleteCount] = useState(0);

  useEffect(async () => {
    let data = await dataAPI.getTasks(searchParams);
    //if the search are in all state. run the counts again
    if (
      !searchParams.searchText &&
      !searchParams.sortBy &&
      searchParams.status == FILTER_STATUS_ALL
    ) {
      if (data?.length) {
        setAllCount(data.length);
        setActiveCount(
          data.filter((q) =>
            [JOB_STATUS_PENDING, JOB_STATUS_RUNNING].includes(q?.status)
          )?.length ?? 0
        );
        setCompleteCount(
          data.filter((q) =>
            [JOB_STATUS_FAILED, JOB_STATUS_SUCCESS].includes(q?.status)
          )?.length ?? 0
        );
      } else {
        setAllCount(0);
        setActiveCount(0);
        setCompleteCount(0);
      }
    }
    setTaskRunList(data);
    setTimeout(() => {
      setLoading(false);
    }, 750);
  }, [searchParams, loading]);

  const handleStatusBtnSelect = (statusType = FILTER_STATUS_ALL) => {
    setSearchParams({ ...searchParams, status: statusType });
  };

  const handleSortBy = (event) => {
    let value = event?.target?.value ?? '';
    setSearchParams({ ...searchParams, sortBy: value });
  };

  const handleSearch = (searchArg = '') => {
    setSearchParams({ ...searchParams, searchText: searchArg });
  };

  const handleReloadRequest = () => {
    if (!loading) {
      setLoading(true);
    }
  };

  const clearInputs = () => {
    setSearchParams({
      status: FILTER_STATUS_ALL,
      sortBy: '',
      searchText: '',
    });
    setSearchQuery('');
  };

  return (
    <>
      <Box className={classes.intputRow}>
        <Button
          variant='outlined'
          className={
            searchParams.status === FILTER_STATUS_ALL
              ? classes.activeButton
              : classes.inactiveButton
          }
          onClick={() => handleStatusBtnSelect(FILTER_STATUS_ALL)}
        >
          <Typography variant='button' className={classes.buttonText}>
            All
          </Typography>
          <Chip
            label={allCount}
            className={
              searchParams.status === FILTER_STATUS_ALL
                ? classes.activeChip
                : classes.inactiveChip
            }
          />
        </Button>
        <Button
          variant='outlined'
          className={
            searchParams.status === FILTER_STATUS_ACTIVE
              ? classes.activeButton
              : classes.inactiveButton
          }
          onClick={() => handleStatusBtnSelect(FILTER_STATUS_ACTIVE)}
        >
          <Typography variant='button' className={classes.buttonText}>
            Active
          </Typography>
          <Chip
            label={activeCount}
            className={
              searchParams.status === FILTER_STATUS_ACTIVE
                ? classes.activeChip
                : classes.inactiveChip
            }
          />
        </Button>
        <Button
          variant='outlined'
          className={
            searchParams.status === FILTER_STATUS_COMPLETE
              ? classes.activeButton
              : classes.inactiveButton
          }
          onClick={() => handleStatusBtnSelect(FILTER_STATUS_COMPLETE)}
        >
          <Typography variant='button' className={classes.buttonText}>
            Complete
          </Typography>
          <Chip
            label={completeCount}
            className={
              searchParams.status === FILTER_STATUS_COMPLETE
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
          <IconButton size='large' onClick={handleReloadRequest}>
            {loading ? <CircularProgress size={'.85em'} /> : <RefreshIcon />}
          </IconButton>
          <IconButton size='large' onClick={clearInputs}>
            <ClearAllIcon />
          </IconButton>
        </Box>
      </Box>
      <TaskRunDataGrid taskRunList={taskRunList} />
    </>
  );
}
