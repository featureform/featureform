import FilterAltOffIcon from '@mui/icons-material/FilterAltOff';
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
  Tooltip,
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
  const SORT_STATUS = 'STATUS';
  const SORT_DATE = 'STATUS_DATE';
  const ENTER_KEY = 'Enter';

  const STATUS_MAP = {
    NO_STATUS: 0,
    CREATED: 1,
    PENDING: 2,
    READY: 3,
    FAILED: 4,
    RUNNING: 5,
  };
  const [searchParams, setSearchParams] = useState({
    status: FILTER_STATUS_ALL,
    sortBy: SORT_DATE,
    searchText: '',
  });
  const [searchQuery, setSearchQuery] = useState('');
  const [taskRunList, setTaskRunList] = useState([]);
  const [loading, setLoading] = useState(true);
  const [allCount, setAllCount] = useState(0);
  const [activeCount, setActiveCount] = useState(0);
  const [completeCount, setCompleteCount] = useState(0);

  useEffect(async () => {
    if (loading) {
      let data = await dataAPI.getTaskRuns(searchParams);
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
              [STATUS_MAP.PENDING, STATUS_MAP.RUNNING].includes(
                q?.taskRun?.status
              )
            )?.length ?? 0
          );
          setCompleteCount(
            data.filter((q) =>
              [STATUS_MAP.FAILED, STATUS_MAP.READY].includes(q?.taskRun?.status)
            )?.length ?? 0
          );
        } else {
          setAllCount(0);
          setActiveCount(0);
          setCompleteCount(0);
        }
      }
      setTaskRunList(data);
      const timeout = setTimeout(() => {
        setLoading(false);
      }, 750);
      return () => {
        if (timeout) {
          clearTimeout(timeout);
        }
      };
    }
  }, [searchParams, loading]);

  const handleStatusBtnSelect = (statusType = FILTER_STATUS_ALL) => {
    setSearchParams({ ...searchParams, status: statusType });
    setLoading(true);
  };

  const handleSortBy = (event) => {
    let value = event?.target?.value ?? '';
    setSearchParams({ ...searchParams, sortBy: value });
    setLoading(true);
  };

  const handleSearch = (searchArg = '') => {
    setSearchParams({ ...searchParams, searchText: searchArg });
    setLoading(true);
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
    setLoading(true);
  };

  return (
    <>
      <Box className={classes.inputRow}>
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
            data-testid='allId'
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
            data-testid='activeId'
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
            data-testid='completeId'
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
                'data-testid': 'searcInputId',
              }}
            />
          </FormControl>
          <Tooltip title='Refresh table' placement='top'>
            <IconButton size='large' onClick={handleReloadRequest}>
              {loading ? (
                <CircularProgress
                  size={'.85em'}
                  data-testid='circularProgressId'
                />
              ) : (
                <RefreshIcon data-testid='refreshIcon' />
              )}
            </IconButton>
          </Tooltip>
          <Tooltip title='Clear filter inputs' placement='top'>
            <IconButton size='large' onClick={clearInputs}>
              <FilterAltOffIcon data-testid='clearIcon' />
            </IconButton>
          </Tooltip>
        </Box>
      </Box>
      <TaskRunDataGrid
        taskRunList={taskRunList?.map(function (object) {
          return {
            ...object,
            id: object.taskRun.taskId + '.' + object.taskRun.runId,
          };
        })}
      />
    </>
  );
}
